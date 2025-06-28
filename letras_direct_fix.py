#!/usr/bin/env python3
"""
Direct fix for letras.com encoding issues - completely bypassing existing infrastructure.
This script directly fetches lyrics, explicitly handles encoding at every step, 
and saves properly encoded files.
"""
import os
import sys
import re
import time
import logging
import sqlite3
import yaml
import hashlib
import requests
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("letras_direct_fix")

def load_config(config_path="config.yaml"):
    """Load configuration file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return None

def fix_url(url):
    """Fetch and save lyrics directly from a letras.com URL."""
    try:
        logger.info(f"Processing URL: {url}")
        
        # Step 1: Fetch the HTML content
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Step 2: Force UTF-8 encoding
        response.encoding = 'utf-8'
        html = response.text
        
        logger.info(f"Got HTML response: {len(html)} bytes, encoding: {response.encoding}")
        
        # Step 3: Parse HTML with BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        # Step 4: Extract metadata
        metadata = {}
        
        # Extract title
        title_elem = soup.select_one('h1')
        if title_elem:
            metadata['title'] = title_elem.get_text().strip()
            logger.info(f"Title: {metadata['title']}")
        
        # Extract artist
        artist_selectors = [
            'div.cnt-head_title div.cnt-head_artistname a',
            'h2 a',
            'div.header-section-title h2 a',
            'div.artist-name',
            'div.cnt-head h2'
        ]
        
        for selector in artist_selectors:
            artist_elem = soup.select_one(selector)
            if artist_elem:
                metadata['artist'] = artist_elem.get_text().strip()
                logger.info(f"Artist: {metadata['artist']}")
                break
                
        # If artist not found, extract from URL
        if 'artist' not in metadata:
            url_parts = url.strip('/').split('/')
            if len(url_parts) >= 5:
                metadata['artist'] = url_parts[-2].replace('-', ' ').title()
                logger.info(f"Artist from URL: {metadata['artist']}")
        
        # Step 5: Extract lyrics text
        lyrics_div = (
            soup.select_one('div.lyric-original') or 
            soup.select_one('.lyric-original') or
            soup.select_one('div.cnt-letra') or
            soup.select_one('.letra') or
            soup.select_one('.lyric')
        )
        
        if not lyrics_div:
            logger.error("Lyrics container not found")
            return False
            
        # Remove unwanted elements
        for unwanted in lyrics_div.select('script, style, .ads, .banner, .pub'):
            unwanted.extract()
        
        # Process lyrics text
        lyrics_text = ""
        paragraphs = lyrics_div.select('p')
        
        if paragraphs:
            # Process paragraphs
            for p in paragraphs:
                paragraph_text = p.get_text(strip=True)
                if paragraph_text:
                    # Add period if needed
                    if paragraph_text and not paragraph_text[-1] in ['.', '!', '?']:
                        paragraph_text += '.'
                    lyrics_text += paragraph_text + '\n\n'
        else:
            # Direct text extraction
            raw_text = lyrics_div.get_text(separator='\n', strip=True)
            
            # Clean up navigation elements
            navigation_texts = ["Letra", "Traducción", "Significado"]
            for nav_text in navigation_texts:
                raw_text = raw_text.replace(nav_text, "")
            
            # Process lines
            lines = []
            for line in raw_text.split('\n'):
                line = line.strip()
                if line:
                    lines.append(line)
            
            for line in lines:
                if line and not line[-1] in ['.', '!', '?']:
                    line += '.'
                lyrics_text += line + '\n\n'
        
        # Clean up excessive newlines
        lyrics_text = re.sub(r'\n{3,}', '\n\n', lyrics_text.strip())
        
        if not lyrics_text:
            logger.error("Failed to extract lyrics text")
            return False
            
        # Count words and sentences
        words = lyrics_text.split()
        sentences = len(re.split(r'[.!?]+', lyrics_text))
        
        logger.info(f"Extracted {len(words)} words, {sentences} sentences")
        
        # Step 6: Save the lyrics
        output_dir = Path("data/corpus_raw/letras_com")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate unique filename
        date_stamp = datetime.now().strftime('%Y%m%d')
        content_hash = hashlib.md5(lyrics_text.encode('utf-8')).hexdigest()[:16]
        file_path = output_dir / f"letras_com_{date_stamp}_{content_hash}.txt"
        
        # Save with explicit UTF-8 encoding
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(lyrics_text)
            
        logger.info(f"Saved lyrics to {file_path}")
        
        # Step 7: Verify saved file
        with open(file_path, 'r', encoding='utf-8') as f:
            saved_content = f.read()
            
        if saved_content == lyrics_text:
            logger.info("Verification: File content matches extracted lyrics ✓")
            return str(file_path)
        else:
            logger.error("Verification: File content does not match extracted lyrics ✗")
            return False
            
    except Exception as e:
        logger.error(f"Error processing URL: {e}")
        return False

def get_letras_urls():
    """Get all letras.com URLs from the database."""
    try:
        config = load_config()
        if not config:
            return []
            
        db_path = Path(config.get('storage', {}).get('state_dir', './data/state')) / 'scraper_state.db'
        
        if not db_path.exists():
            logger.error(f"Database not found: {db_path}")
            return []
            
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT url_hash, url FROM url_status WHERE url LIKE '%letras.com%'"
        )
        
        urls = []
        for row in cursor.fetchall():
            urls.append({
                'url': row['url'],
                'url_hash': row['url_hash']
            })
            
        conn.close()
        logger.info(f"Found {len(urls)} letras.com URLs in database")
        return urls
        
    except Exception as e:
        logger.error(f"Error getting letras.com URLs: {e}")
        return []

def update_url_status(url_hash, status, file_path=None):
    """Update URL status in database."""
    try:
        config = load_config()
        if not config:
            return False
            
        db_path = Path(config.get('storage', {}).get('state_dir', './data/state')) / 'scraper_state.db'
        
        if not db_path.exists():
            logger.error(f"Database not found: {db_path}")
            return False
            
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        if status == 'completed' and file_path:
            cursor.execute(
                "UPDATE url_status SET status = ?, updated_at = ?, file_path = ? WHERE url_hash = ?",
                (status, datetime.now().isoformat(), file_path, url_hash)
            )
        else:
            cursor.execute(
                "UPDATE url_status SET status = ?, updated_at = ? WHERE url_hash = ?",
                (status, datetime.now().isoformat(), url_hash)
            )
            
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error updating URL status: {e}")
        return False

def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Direct fix for letras.com encoding issues")
    parser.add_argument("--url", help="Process a single URL")
    parser.add_argument("--all", action="store_true", help="Process all letras.com URLs in database")
    parser.add_argument("--limit", type=int, default=None, help="Limit the number of URLs to process")
    args = parser.parse_args()
    
    if args.url:
        # Process single URL
        file_path = fix_url(args.url)
        if file_path:
            print(f"✅ Successfully processed URL: {args.url}")
            print(f"Saved to: {file_path}")
            return 0
        else:
            print(f"❌ Failed to process URL: {args.url}")
            return 1
    elif args.all:
        # Process all URLs
        urls = get_letras_urls()
        
        if not urls:
            print("No letras.com URLs found in database")
            return 1
            
        # Apply limit if specified
        if args.limit and len(urls) > args.limit:
            urls = urls[:args.limit]
            logger.info(f"Limited to {args.limit} URLs")
            
        successful = 0
        failed = 0
        
        for i, url_data in enumerate(urls):
            url = url_data['url']
            url_hash = url_data['url_hash']
            
            print(f"Processing URL {i+1}/{len(urls)}: {url}")
            
            file_path = fix_url(url)
            if file_path:
                update_url_status(url_hash, 'completed', file_path)
                successful += 1
            else:
                update_url_status(url_hash, 'failed')
                failed += 1
                
            # Be polite
            if i < len(urls) - 1:
                time.sleep(1)
                
        print(f"Processed {len(urls)} URLs: {successful} successful, {failed} failed")
        
        if failed > 0:
            return 1
        return 0
    else:
        parser.print_help()
        return 1

if __name__ == "__main__":
    sys.exit(main())
