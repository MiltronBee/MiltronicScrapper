#!/usr/bin/env python3
"""
Test script for Letras.com lyrics extraction.
This script tests the updated extraction logic for letras.com URLs.
"""
import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Any, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("letras_test")

# Add parent directory to path to import scraper modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the letras scraper
from corpus_scraper.letras_scraper import LetrasScraper

def setup_output_dir() -> Path:
    """Create output directory for saving extracted lyrics."""
    output_dir = Path("./test_output")
    output_dir.mkdir(exist_ok=True)
    return output_dir

def sanitize_filename(text: str) -> str:
    """Sanitize a string to be used as a filename."""
    # Replace any non-alphanumeric characters with underscores
    return "".join(c if c.isalnum() else "_" for c in text)

def save_lyrics_to_file(output_dir: Path, song_data: Dict[str, Any]) -> None:
    """Save extracted lyrics and metadata to file for inspection."""
    if not song_data.get("success") or not song_data.get("lyrics"):
        logger.warning("No lyrics to save")
        return
    
    # Create a filename from metadata
    title = song_data.get("metadata", {}).get("title", "unknown_song")
    artist = song_data.get("metadata", {}).get("artist", "unknown_artist")
    filename = f"{sanitize_filename(artist)}__{sanitize_filename(title)}.txt"
    
    # Write lyrics to file
    output_path = output_dir / filename
    with open(output_path, "w", encoding="utf-8") as f:
        # Write metadata
        f.write(f"Title: {song_data.get('metadata', {}).get('title', 'Unknown')}\n")
        f.write(f"Artist: {song_data.get('metadata', {}).get('artist', 'Unknown')}\n")
        f.write(f"Album: {song_data.get('metadata', {}).get('album', 'Unknown')}\n")
        f.write(f"Language: {song_data.get('metadata', {}).get('language', 'Unknown')}\n")
        f.write(f"URL: {song_data.get('url', 'Unknown')}\n")
        f.write(f"Word Count: {song_data.get('metadata', {}).get('word_count', 0)}\n")
        f.write(f"Sentence Count: {song_data.get('metadata', {}).get('sentence_count', 0)}\n")
        f.write("\n" + "="*50 + "\n\n")
        
        # Write lyrics
        f.write(song_data.get("lyrics", ""))
    
    logger.info(f"Lyrics saved to {output_path}")

def test_url(url: str) -> Dict[str, Any]:
    """Test lyrics extraction for a single URL."""
    logger.info(f"Testing lyrics extraction for URL: {url}")
    
    scraper = LetrasScraper()
    result = scraper.extract_lyrics(url)
    
    if result.get("success"):
        logger.info(f"Success! Extracted {result.get('metadata', {}).get('word_count', 0)} words")
        # Print a preview of the lyrics
        lyrics_preview = result.get("lyrics", "")[:150].replace("\n", " ")
        logger.info(f"Lyrics preview: {lyrics_preview}...")
    else:
        logger.error(f"Failed to extract lyrics: {result.get('error', 'Unknown error')}")
    
    return result

def test_multiple_urls(urls: List[str]) -> None:
    """Test multiple URLs and report success rate."""
    output_dir = setup_output_dir()
    
    success_count = 0
    for url in urls:
        result = test_url(url)
        if result.get("success"):
            success_count += 1
            save_lyrics_to_file(output_dir, result)
    
    logger.info(f"Testing complete: {success_count}/{len(urls)} URLs processed successfully")

def main():
    parser = argparse.ArgumentParser(description="Test Letras.com lyrics extraction")
    parser.add_argument("urls", nargs="+", help="Song URLs to process")
    
    args = parser.parse_args()
    test_multiple_urls(args.urls)

if __name__ == "__main__":
    main()
