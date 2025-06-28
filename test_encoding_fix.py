#!/usr/bin/env python3
"""
Test script to verify that the encoding fixes are working correctly
for newly scraped letras.com content.
"""
import os
import sys
import logging
import yaml
from pathlib import Path
import shutil
from datetime import datetime
import re

# Add the code directory to the path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import our scraper components
from corpus_scraper.letras_scraper import LetrasScraper
from corpus_scraper.saver import Saver

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("test_encoding_fix")

def load_config(config_path="config.yaml"):
    """Load the configuration file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)

def test_letras_extraction(url):
    """Test the extraction and saving of lyrics from letras.com."""
    try:
        # Initialize the scraper
        scraper = LetrasScraper()
        
        # Extract lyrics
        logger.info(f"Extracting lyrics from {url}")
        result = scraper.extract_lyrics(url)
        
        if not result['success']:
            logger.error(f"Failed to extract lyrics: {result.get('error', 'Unknown error')}")
            return False
            
        # Print lyrics summary
        lyrics = result['lyrics']
        words = result['metadata'].get('word_count', 0)
        sentences = result['metadata'].get('sentence_count', 0)
        title = result['metadata'].get('title', 'Unknown Song')
        artist = result['metadata'].get('artist', 'Unknown Artist')
        
        logger.info(f"Successfully extracted lyrics for '{title}' by {artist}")
        logger.info(f"Word count: {words}, Sentence count: {sentences}")
        
        # Save lyrics to a test file with proper encoding
        test_output = Path('./test_output')
        test_output.mkdir(exist_ok=True)
        
        # Save raw text to file for inspection
        test_file = test_output / f"test_lyrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(test_file, 'w', encoding='utf-8') as f:
            f.write(f"Title: {title}\n")
            f.write(f"Artist: {artist}\n")
            f.write(f"Word count: {words}\n")
            f.write(f"Sentence count: {sentences}\n\n")
            f.write(lyrics)
        
        logger.info(f"Saved test lyrics to {test_file}")
        
        # Also save using the Saver class to test the patched encoding
        config = load_config()
        storage_config = config.get('storage', {})
        saver = Saver(storage_config)
        
        save_result = saver.save_text(
            content=lyrics,
            source_name="letras_com",
            url=url,
            metadata=result['metadata']
        )
        
        if save_result['saved']:
            logger.info(f"Successfully saved lyrics using Saver class to {save_result['file_path']}")
            # Read back the saved file to verify encoding
            verify_file = Path(save_result['file_path'])
            with open(verify_file, 'r', encoding='utf-8') as f:
                verify_content = f.read()
            
            # Check for encoding issues (if we see � characters)
            if '�' in verify_content:
                logger.error("Detected encoding issues in saved file!")
                return False
            else:
                logger.info("Verified saved file has proper encoding")
                return True
        else:
            logger.error(f"Failed to save using Saver: {save_result.get('error', 'Unknown error')}")
            return False
            
    except Exception as e:
        logger.error(f"Error in test: {e}")
        return False

def main():
    """Main function to test letras.com encoding fixes."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test letras.com scraper encoding fixes")
    parser.add_argument("--url", default="https://www.letras.com/natanael-cano/sin-ti/", 
                      help="URL of a letras.com song to test")
    args = parser.parse_args()
    
    success = test_letras_extraction(args.url)
    
    if success:
        logger.info("✅ Encoding test PASSED!")
        return 0
    else:
        logger.error("❌ Encoding test FAILED!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
