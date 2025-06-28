#!/usr/bin/env python3
"""
A specialized tool to fix letras.com encoding issues and process URLs correctly.

This script:
1. Fixes encoding issues directly by patching the letras.com extractor
2. Provides a standalone scraper functionality to scrape letras.com URLs
3. Ensures proper UTF-8 encoding throughout the entire pipeline
"""
import os
import sys
import re
import time
import logging
import sqlite3
import yaml
from pathlib import Path
from typing import Dict, Any, List
import hashlib
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("fixed_letras_processor")

def load_config(config_path="config.yaml"):
    """Load the configuration file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)

def ensure_utf8_text(text):
    """
    Ensure text is valid UTF-8 string.
    Handles both string and bytes input.
    """
    if text is None:
        return ""
    
    try:
        if isinstance(text, bytes):
            return text.decode('utf-8', errors='replace')
        elif isinstance(text, str):
            # Re-encode and decode to clean up any potential encoding issues
            return text.encode('utf-8', errors='replace').decode('utf-8')
        else:
            return str(text)
    except Exception as e:
        logger.error(f"Error ensuring UTF-8: {e}")
        return str(text)

class LetrasFixer:
    """
    A specialized version of the LetrasScraper that ensures proper encoding.
    This works directly without going through the orchestrator pipeline.
    """
    def __init__(self, config):
        self.logger = logging.getLogger("LetrasFixer")
        self.storage_config = config.get('storage', {})
        self.base_url = "https://www.letras.com"
        
    def fetch_page(self, url):
        """Fetch a webpage and return a BeautifulSoup object with proper encoding."""
        try:
            # Use requests directly
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Force UTF-8 encoding
            response.encoding = 'utf-8'
            html_content = response.text
            
            # Ensure we're working with proper UTF-8 text
            if isinstance(html_content, bytes):
                html_content = html_content.decode('utf-8', errors='replace')
                
            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            return soup
            
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return None
    
    def _count_sentences(self, text):
        """Count sentences in lyrics text."""
        if not text:
            return 0
            
        # Split by common sentence-ending punctuation
        sentences = re.split(r'[.!?]+', text)
        
        # Also count paragraph breaks as potential sentence boundaries
        paragraph_count = text.count('\n\n')
        
        # Count non-empty sentences from punctuation splits
        punct_sentence_count = sum(1 for s in sentences if s.strip())
        
        # Return the larger of the two counts as our best estimate, minimum of 2
        return max(punct_sentence_count, paragraph_count + 1, 2)
    
    def extract_lyrics(self, song_url):
        """Extract lyrics and metadata from a song page with proper encoding."""
        result = {
            'success': False,
            'lyrics': None,
            'metadata': {},
            'url': song_url
        }
        
        soup = self.fetch_page(song_url)
        if not soup:
            result['error'] = f"Failed to fetch song page {song_url}"
            return result
            
        try:
            # Extract title
            title_elem = soup.select_one('h1')
            if title_elem:
                result['metadata']['title'] = ensure_utf8_text(title_elem.get_text().strip())
            
            # Extract artist with multiple selector options
            artist_selectors = [
                'div.cnt-head_title div.cnt-head_artistname a',  # New format
                'h2 a',                                        # Old format
                'div.header-section-title h2 a',               # Another variation
                'div.artist-name',                             # Yet another variation
                'div.cnt-head h2'                              # Fallback
            ]
            
            artist_name = None
            for selector in artist_selectors:
                artist_elem = soup.select_one(selector)
                if artist_elem:
                    artist_name = artist_elem.get_text().strip()
                    if artist_name:
                        result['metadata']['artist'] = ensure_utf8_text(artist_name)
                        break
            
            # If we still don't have an artist name, try to extract it from the URL
            if not artist_name and 'artist' not in result['metadata']:
                # Extract from URL path: https://www.letras.com/natanael-cano/sin-ti/
                url_parts = song_url.strip('/').split('/')
                if len(url_parts) >= 5:  # Has enough path components
                    potential_artist = url_parts[-2].replace('-', ' ')
                    result['metadata']['artist'] = ensure_utf8_text(potential_artist.title())
                    self.logger.debug(f"Extracted artist '{potential_artist.title()}' from URL path")
            
            # Extract lyrics with multiple selector options for new and old formats
            lyrics_div = (
                soup.select_one('div.lyric-original') or 
                soup.select_one('.lyric-original') or
                soup.select_one('div.cnt-letra') or
                soup.select_one('.letra') or
                soup.select_one('.lyric')
            )
            
            if lyrics_div:
                # Remove any unwanted elements like scripts, ads, etc.
                for unwanted in lyrics_div.select('script, style, .ads, .banner, .pub'):
                    unwanted.extract()
                
                # Process lyrics text
                lyrics_text = ''
                paragraphs = lyrics_div.select('p')
                
                if paragraphs:
                    for p in paragraphs:
                        paragraph_text = p.get_text(strip=True)
                        if paragraph_text:
                            # Ensure paragraphs are properly separated and end with period if missing
                            if paragraph_text and not paragraph_text[-1] in ['.', '!', '?']:
                                paragraph_text += '.'
                            lyrics_text += paragraph_text + '\n\n'
                else:
                    # If no paragraphs found, try getting text directly
                    raw_text = lyrics_div.get_text(separator='\n', strip=True)
                    
                    # Clean up the text - remove tab navigation elements
                    navigation_texts = ["Letra", "Traducción", "Significado"]
                    for nav_text in navigation_texts:
                        raw_text = raw_text.replace(nav_text, "")
                    
                    # Split into paragraphs
                    lines = []
                    for line in raw_text.split('\n'):
                        line = line.strip()
                        if line:
                            lines.append(line)
                    
                    # Group lines into paragraphs
                    for line in lines:
                        if line and not line[-1] in ['.', '!', '?']:
                            line += '.'
                        lyrics_text += line + '\n\n'
                
                # Clean up any excessive newlines
                lyrics_text = re.sub(r'\n{3,}', '\n\n', lyrics_text.strip())
                
                # Ensure proper UTF-8 encoding of lyrics text
                lyrics_text = ensure_utf8_text(lyrics_text)
                
                result['lyrics'] = lyrics_text
                result['success'] = bool(lyrics_text)
                
                # Add word and sentence count as metadata
                if result['success']:
                    words = lyrics_text.split()
                    sentences = self._count_sentences(lyrics_text)
                    result['metadata']['word_count'] = len(words)
                    result['metadata']['sentence_count'] = sentences
                    self.logger.info(f"Extracted {len(words)} words, {sentences} sentences from {song_url}")
            
            # Additional metadata (if available)
            album_elem = soup.select_one('.letra-info > a')
            if album_elem:
                result['metadata']['album'] = ensure_utf8_text(album_elem.get_text().strip())
            
            # Default to Spanish for Letras.com
            result['metadata']['language'] = 'es'
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error extracting lyrics from {song_url}: {e}")
            result['error'] = str(e)
            return result
    
    def save_lyrics(self, lyrics_result):
        """Save extracted lyrics with proper encoding."""
        try:
            if not lyrics_result['success'] or not lyrics_result['lyrics']:
                return False
                
            source_name = "letras_com"
            url = lyrics_result['url']
            lyrics_text = lyrics_result['lyrics']
            metadata = lyrics_result['metadata']
            
            # Create output directory
            output_dir = Path(self.storage_config.get('output_dir', 'data/corpus_raw')) / source_name
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate filename
            date_stamp = datetime.now().strftime('%Y%m%d')
            
            # Calculate content hash
            content_hash = hashlib.md5(lyrics_text.encode('utf-8')).hexdigest()[:16]
            
            # Construct filename
            file_path = output_dir / f"{source_name}_{date_stamp}_{content_hash}.txt"
            
            # Ensure proper UTF-8 encoding before writing
            lyrics_text = ensure_utf8_text(lyrics_text)
            
            # Write to file with explicit UTF-8 encoding
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(lyrics_text)
                
            self.logger.info(f"Saved lyrics to {file_path}")
            return str(file_path)
            
        except Exception as e:
            self.logger.error(f"Error saving lyrics: {e}")
            return False
    
    def process_url(self, url):
        """Process a single URL: extract lyrics and save to file."""
        try:
            # Only process letras.com URLs
            if 'letras.com' not in url.lower():
                self.logger.warning(f"Not a letras.com URL: {url}")
                return False
            
            self.logger.info(f"Processing URL: {url}")
            
            # Extract lyrics
            lyrics_result = self.extract_lyrics(url)
            if not lyrics_result['success']:
                self.logger.error(f"Failed to extract lyrics: {lyrics_result.get('error', 'Unknown error')}")
                return False
            
            # Save lyrics
            file_path = self.save_lyrics(lyrics_result)
            if not file_path:
                self.logger.error("Failed to save lyrics")
                return False
            
            self.logger.info(f"Successfully processed URL {url}, saved to {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing URL {url}: {e}")
            return False

class StateManager:
    """Simplified version of StateManager to work with the SQLite database."""
    def __init__(self, db_path):
        self.logger = logging.getLogger("StateManager")
        self.db_path = db_path
        
    def get_connection(self):
        """Get SQLite connection."""
        return sqlite3.connect(self.db_path)
        
    def get_letras_urls(self, status=None):
        """Get URLs from letras.com with the given status (or all if None)."""
        try:
            conn = self.get_connection()
            # Set row factory to access columns by name
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            if status:
                cursor.execute(
                    "SELECT url_hash, url, status FROM url_status WHERE url LIKE '%letras.com%' AND status = ?",
                    (status,)
                )
            else:
                cursor.execute(
                    "SELECT url_hash, url, status FROM url_status WHERE url LIKE '%letras.com%'"
                )
                
            urls = []
            for row in cursor.fetchall():
                urls.append({
                    'url': row['url'],
                    'url_hash': row['url_hash'],
                    'source_name': 'letras_com',
                    'status': row['status']
                })
                
            conn.close()
            return urls
            
        except Exception as e:
            self.logger.error(f"Error getting letras.com URLs: {e}")
            return []
            
    def update_url_status(self, url_hash, status, error=None, content_hash=None, file_path=None):
        """Update URL status in database."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Update based on the correct table and column structure
            if error:
                cursor.execute(
                    "UPDATE url_status SET status = ?, error_message = ?, updated_at = ? WHERE url_hash = ?",
                    (status, error, datetime.now().isoformat(), url_hash)
                )
            else:
                # For successful cases with content hash and file path
                cursor.execute(
                    "UPDATE url_status SET status = ?, error_message = NULL, updated_at = ?, file_path = ? WHERE url_hash = ?",
                    (status, datetime.now().isoformat(), file_path, url_hash)
                )
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating URL status: {e}")
            return False

def main():
    """Main function to process letras.com URLs with proper encoding."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Process letras.com URLs with proper encoding")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--url", help="Single URL to process")
    parser.add_argument("--pending-only", action="store_true", help="Process only pending URLs")
    parser.add_argument("--failed-only", action="store_true", help="Process only failed URLs")
    parser.add_argument("--limit", type=int, default=None, help="Limit the number of URLs to process")
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    storage_config = config.get('storage', {})
    
    # Initialize fixer
    fixer = LetrasFixer(config)
    
    if args.url:
        # Process single URL
        success = fixer.process_url(args.url)
        if success:
            logger.info(f"✅ Successfully processed URL: {args.url}")
            return 0
        else:
            logger.error(f"❌ Failed to process URL: {args.url}")
            return 1
    else:
        # Process URLs from database
        db_path = Path(storage_config.get('state_dir', 'data/state')) / "scraper_state.db"
        
        if not db_path.exists():
            logger.error(f"Database not found: {db_path}")
            return 1
            
        state_manager = StateManager(db_path)
        
        # Get URLs to process
        if args.pending_only:
            urls = state_manager.get_letras_urls('pending')
            logger.info(f"Found {len(urls)} pending letras.com URLs")
        elif args.failed_only:
            urls = state_manager.get_letras_urls('failed')
            logger.info(f"Found {len(urls)} failed letras.com URLs")
        else:
            urls = state_manager.get_letras_urls()
            logger.info(f"Found {len(urls)} letras.com URLs in total")
        
        # Apply limit if specified
        if args.limit and len(urls) > args.limit:
            urls = urls[:args.limit]
            logger.info(f"Limited processing to {args.limit} URLs")
        
        # Process URLs
        successful = 0
        failed = 0
        
        for i, url_record in enumerate(urls):
            logger.info(f"Processing URL {i+1}/{len(urls)}: {url_record['url']}")
            url = url_record['url']
            url_hash = url_record['url_hash']
            
            # Process URL
            success = fixer.process_url(url)
            
            # Update status in database
            if success:
                # Read the file content and get its hash
                output_dir = Path(storage_config.get('output_dir', 'data/corpus_raw')) / "letras_com"
                
                # Get the most recently created file
                files = list(output_dir.glob("*.txt"))
                if files:
                    latest_file = max(files, key=lambda f: f.stat().st_mtime)
                    file_path = str(latest_file)
                    
                    # Calculate content hash
                    with open(latest_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                        content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()[:16]
                    
                    # Update status
                    state_manager.update_url_status(url_hash, 'completed', content_hash=content_hash, file_path=file_path)
                    successful += 1
                else:
                    # Something went wrong but we didn't catch it
                    state_manager.update_url_status(url_hash, 'failed', error="File not found after processing")
                    failed += 1
            else:
                state_manager.update_url_status(url_hash, 'failed', error="Failed to process URL")
                failed += 1
            
            # Be polite
            if i < len(urls) - 1:
                time.sleep(2)
        
        logger.info(f"Processed {len(urls)} URLs: {successful} successful, {failed} failed")
        
        if successful > 0:
            return 0
        else:
            return 1

if __name__ == "__main__":
    sys.exit(main())
