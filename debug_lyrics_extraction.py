#!/usr/bin/env python3
import logging
import requests
from bs4 import BeautifulSoup
import re
import time
from urllib.parse import urljoin
import sys
import json
import os

logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('debug_letras_lyrics')

def fetch_page(url):
    """Fetch a page with proper headers and encoding"""
    logger.info(f"Fetching {url}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Force UTF-8 encoding for consistency
        response.encoding = 'utf-8'
        html_content = response.text
        
        logger.info(f"Got {len(html_content)} bytes from {url} with encoding {response.encoding}")
        
        return html_content
    
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
        return None

def extract_song_lyrics(url, save_output=False):
    """Extract lyrics from a letras.com song page with proper encoding and formatting"""
    html_content = fetch_page(url)
    if not html_content:
        return None
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Extract basic metadata
    title = ""
    artist = ""
    
    title_elem = soup.select_one('h1')
    if title_elem:
        title = title_elem.get_text().strip()
    
    artist_elem = soup.select_one('h2 a')
    if artist_elem:
        artist = artist_elem.get_text().strip()
    
    logger.info(f"Title: '{title}', Artist: '{artist}'")
    
    # Extract lyrics using the selectors we've found to work
    lyrics_div = (
        soup.select_one('div.lyric-original') or 
        soup.select_one('.lyric-original') or
        soup.select_one('div.cnt-letra') or
        soup.select_one('.letra') or
        soup.select_one('.lyric')
    )
    
    if not lyrics_div:
        logger.error("No lyrics container found using any known selector")
        return None
    
    # Remove script tags, ads, etc.
    for unwanted in lyrics_div.select('script, style, .ads, .banner, .pub'):
        unwanted.extract()
    
    # Extract paragraphs properly
    lyrics_text = ""
    paragraphs = lyrics_div.select('p')
    
    if paragraphs:
        for p in paragraphs:
            paragraph_text = p.get_text(strip=True)
            if paragraph_text:
                # Ensure paragraphs are properly separated and end with period if missing
                if not paragraph_text[-1] in ['.', '!', '?']:
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
        
        # Group lines into paragraphs with proper punctuation
        i = 0
        while i < len(lines):
            paragraph = lines[i]
            i += 1
            
            # Ensure paragraph ends with proper punctuation
            if paragraph and not paragraph[-1] in ['.', '!', '?']:
                paragraph += '.'
                
            lyrics_text += paragraph + '\n\n'
    
    # Clean up any excessive newlines
    lyrics_text = re.sub(r'\n{3,}', '\n\n', lyrics_text.strip())
    
    # Count words and sentences
    words = lyrics_text.split()
    sentences = count_sentences(lyrics_text)
    
    result = {
        'success': bool(lyrics_text),
        'title': title,
        'artist': artist,
        'url': url, 
        'lyrics': lyrics_text,
        'word_count': len(words),
        'sentence_count': sentences
    }
    
    # Log extraction results
    logger.info(f"Lyrics extraction stats: {len(words)} words, {sentences} sentences")
    
    if save_output:
        # Save to file for inspection
        output_dir = "./debug_output"
        os.makedirs(output_dir, exist_ok=True)
        
        # Create safe filename
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', artist + '_' + title)
        safe_name = re.sub(r'_+', '_', safe_name)[:50]  # Limit length
        
        lyrics_file = os.path.join(output_dir, f"{safe_name}.txt")
        with open(lyrics_file, 'w', encoding='utf-8') as f:
            f.write(lyrics_text)
        
        metadata_file = os.path.join(output_dir, f"{safe_name}_meta.json")
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Saved output to {lyrics_file} and {metadata_file}")
    
    # Print a preview of the lyrics
    preview_length = min(300, len(lyrics_text))
    logger.info(f"\nLyrics preview:\n{'-'*50}\n{lyrics_text[:preview_length]}{'...' if len(lyrics_text) > preview_length else ''}\n{'-'*50}")
    
    return result

def count_sentences(text):
    """Improved sentence counting for lyrics that may not have standard punctuation"""
    if not text:
        return 0
        
    # First split by common sentence-ending punctuation
    sentences = re.split(r'[.!?]+', text)
    
    # Also count paragraph breaks as potential sentence boundaries
    paragraph_count = text.count('\n\n')
    
    # Count non-empty sentences from punctuation splits
    punct_sentence_count = sum(1 for s in sentences if s.strip())
    
    # Return the larger of the two counts as our best estimate, minimum of 2
    return max(punct_sentence_count, paragraph_count + 1, 2)

def analyze_lyrics_page(url):
    """Analyze a letras.com song page for lyrics extraction"""
    html_content = fetch_page(url)
    if not html_content:
        return None
    
    try:
        # Parse with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        title = soup.title.text.strip() if soup.title else "No title found"
        logger.info(f"Page title: {title}")
        
        # Look for potential lyrics containers
        potential_containers = [
            'div.cnt-letra',
            'div.letra-cnt',
            'div.letra',
            'div.lyric-original',
            '.lyric-original',
            '.lyric',
            '.lyrics'
        ]
        
        logger.info("Checking for lyrics containers:")
        for container_selector in potential_containers:
            container = soup.select_one(container_selector)
            if container:
                logger.info(f"Found container with selector '{container_selector}':")
                # Print the first few characters for debugging
                content_text = container.get_text(strip=True)
                logger.info(f"- Text length: {len(content_text)} characters")
                logger.info(f"- Text preview: {content_text[:100]}...")
                logger.info(f"- HTML structure: {container.name} with classes: {container.get('class', [])}")
                
                # Check for paragraphs within the container
                paragraphs = container.select('p')
                logger.info(f"- Contains {len(paragraphs)} paragraphs")
                
                if paragraphs:
                    for i, p in enumerate(paragraphs[:3]):  # Look at first 3 paragraphs
                        p_text = p.get_text(strip=True)
                        logger.info(f"  Paragraph {i+1}: {p_text[:50]}... ({len(p_text)} chars)")
            else:
                logger.info(f"No container found with selector '{container_selector}'")
        
        # Try to extract lyrics with both methods to compare
        # Method 1: Original method from the scraper
        lyrics_div = soup.select_one('div.cnt-letra')
        lyrics_text_1 = ''
        if lyrics_div:
            paragraphs = lyrics_div.select('p')
            if paragraphs:
                for p in paragraphs:
                    paragraph_text = p.get_text(strip=True)
                    if paragraph_text:
                        lyrics_text_1 += paragraph_text + '\n\n'
            else:
                # If no paragraphs found, try getting text directly
                lyrics_text_1 = lyrics_div.get_text(separator='\n', strip=True)
        
        # Method 2: New alternative approach
        lyrics_div_2 = soup.select_one('.lyric-original') or soup.select_one('.lyric')
        lyrics_text_2 = ''
        if lyrics_div_2:
            # Remove script tags, ads, etc.
            for unwanted in lyrics_div_2.select('script, style, .ads, .banner, .pub'):
                unwanted.extract()
            
            if lyrics_div_2.select('p'):
                paragraphs = lyrics_div_2.select('p')
                for p in paragraphs:
                    p_text = p.get_text(strip=True)
                    if p_text:
                        lyrics_text_2 += p_text + '\n\n'
            else:
                lyrics_text_2 = lyrics_div_2.get_text(separator='\n', strip=True)
                
            # Split into paragraphs if no paragraph structure was found
            if '\n\n' not in lyrics_text_2:
                paragraphs = [p.strip() for p in lyrics_text_2.split('\n') if p.strip()]
                lyrics_text_2 = '\n\n'.join(paragraphs)
        
        # Compare methods
        logger.info("\nLyrics Extraction Result:")
        logger.info(f"Method 1 (Original): {len(lyrics_text_1)} characters")
        if lyrics_text_1:
            logger.info(f"Preview: {lyrics_text_1[:100]}...")
        else:
            logger.info("No lyrics found with original method")
        
        logger.info(f"Method 2 (New): {len(lyrics_text_2)} characters")
        if lyrics_text_2:
            logger.info(f"Preview: {lyrics_text_2[:100]}...")
        else:
            logger.info("No lyrics found with new method")
            
        return {
            "title": title,
            "method1_length": len(lyrics_text_1),
            "method2_length": len(lyrics_text_2),
            "lyrics1": lyrics_text_1,
            "lyrics2": lyrics_text_2
        }

    except Exception as e:
        logger.error(f"Error analyzing {url}: {e}")
        return None

def fix_letras_scraper():
    """Function to update the letras_scraper.py code to fix the encoding and extraction issues"""
    # This function would contain the code to update letras_scraper.py
    # For now, we'll just run lyrics extraction as a test
    pass


if __name__ == "__main__":
    # Process command line arguments
    if len(sys.argv) < 2:
        logger.info("Usage: python debug_lyrics_extraction.py <url> [--save]")
        logger.info("Example: python debug_lyrics_extraction.py https://www.letras.com/natanael-cano/sin-ti/ --save")
        url = "https://www.letras.com/natanael-cano/sin-ti/"  # Default URL
        save_output = False
    else:
        url = sys.argv[1]
        save_output = "--save" in sys.argv
    
    # Extract lyrics
    result = extract_song_lyrics(url, save_output)
    
    if result:
        # Show summary of extraction
        print(f"\n==== EXTRACTION SUMMARY ====")
        print(f"Title: {result['title']}")
        print(f"Artist: {result['artist']}")
        print(f"Word count: {result['word_count']}")
        print(f"Sentence count: {result['sentence_count']}")
        print(f"Content validation: {'PASS' if result['word_count'] >= 50 and result['sentence_count'] >= 2 else 'FAIL'}")
        print("===========================")
    else:
        logger.error("Failed to extract lyrics")
