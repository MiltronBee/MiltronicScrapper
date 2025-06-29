#!/usr/bin/env python3
"""
Clean existing corpus files with encoding issues.

This script scans all files in data/corpus_raw/, identifies files with encoding
or content quality issues, and either cleans them or removes corrupted files.
"""

import os
import sys
import glob
import shutil
from pathlib import Path
import logging

# Add the corpus_scraper module to path
sys.path.append('/root/MiltronicScrapper')
from corpus_scraper.encoding_validator import EncodingValidator

def setup_logging():
    """Setup logging for the cleanup process."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/root/MiltronicScrapper/data/logs/cleanup.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def backup_file(filepath: str, backup_dir: str) -> str:
    """Create a backup of the file before cleaning."""
    backup_path = os.path.join(backup_dir, os.path.basename(filepath))
    counter = 1
    while os.path.exists(backup_path):
        name, ext = os.path.splitext(os.path.basename(filepath))
        backup_path = os.path.join(backup_dir, f"{name}_{counter}{ext}")
        counter += 1
    
    shutil.copy2(filepath, backup_path)
    return backup_path

def clean_corpus_files():
    """Main cleanup function."""
    logger = setup_logging()
    validator = EncodingValidator()
    
    # Create backup directory
    backup_dir = "/root/MiltronicScrapper/data/corpus_backup"
    os.makedirs(backup_dir, exist_ok=True)
    
    corpus_dir = "/root/MiltronicScrapper/data/corpus_raw"
    
    if not os.path.exists(corpus_dir):
        logger.error(f"Corpus directory not found: {corpus_dir}")
        return
    
    # Statistics
    stats = {
        'total_files': 0,
        'files_with_issues': 0,
        'cleaned_files': 0,
        'deleted_files': 0,
        'clean_files': 0,
        'errors': 0
    }
    
    logger.info("Starting corpus cleanup process...")
    
    # Process all txt files in subdirectories
    pattern = os.path.join(corpus_dir, "**", "*.txt")
    
    for filepath in glob.glob(pattern, recursive=True):
        stats['total_files'] += 1
        
        try:
            logger.debug(f"Processing: {filepath}")
            
            # Validate the file
            validation_result = validator.validate_file_content(filepath)
            
            if validation_result['is_valid']:
                stats['clean_files'] += 1
                logger.debug(f"File is clean: {os.path.basename(filepath)}")
                continue
            
            stats['files_with_issues'] += 1
            recommended_action = validation_result.get('recommended_action', 'review')
            
            logger.info(f"File has issues: {os.path.basename(filepath)} - Action: {recommended_action}")
            logger.info(f"  Issues: {validation_result.get('issues', [])}")
            
            if recommended_action == 'delete':
                # Backup before deleting
                backup_path = backup_file(filepath, backup_dir)
                logger.info(f"  Backed up to: {os.path.basename(backup_path)}")
                
                os.remove(filepath)
                stats['deleted_files'] += 1
                logger.warning(f"  Deleted corrupted file: {os.path.basename(filepath)}")
                
            elif recommended_action in ['clean', 'review']:
                # Try to clean the file
                try:
                    # Read and clean content
                    with open(filepath, 'rb') as f:
                        raw_content = f.read()
                    
                    # Detect encoding and decode
                    detected_encoding, is_valid, validation_info = validator.detect_and_validate_encoding(raw_content)
                    
                    if detected_encoding:
                        text_content = raw_content.decode(detected_encoding, errors='replace')
                    else:
                        text_content = raw_content.decode('utf-8', errors='replace')
                    
                    # Clean and normalize
                    cleaned_content = validator.clean_and_normalize_text(text_content)
                    
                    # Validate cleaned content
                    is_cleaned_valid, cleaned_quality = validator._validate_text_quality(cleaned_content)
                    
                    if is_cleaned_valid and len(cleaned_content.strip()) >= validator.min_text_length:
                        # Backup original
                        backup_path = backup_file(filepath, backup_dir)
                        logger.info(f"  Backed up original to: {os.path.basename(backup_path)}")
                        
                        # Write cleaned content
                        with open(filepath, 'w', encoding='utf-8') as f:
                            f.write(cleaned_content)
                        
                        stats['cleaned_files'] += 1
                        logger.info(f"  Successfully cleaned: {os.path.basename(filepath)}")
                    else:
                        # Cleaning didn't help enough, delete
                        backup_path = backup_file(filepath, backup_dir)
                        logger.info(f"  Backed up to: {os.path.basename(backup_path)}")
                        
                        os.remove(filepath)
                        stats['deleted_files'] += 1
                        logger.warning(f"  Deleted after failed cleaning: {os.path.basename(filepath)}")
                        
                except Exception as clean_error:
                    logger.error(f"  Failed to clean {os.path.basename(filepath)}: {clean_error}")
                    stats['errors'] += 1
            
        except Exception as e:
            logger.error(f"Error processing {filepath}: {e}")
            stats['errors'] += 1
        
        # Progress update every 100 files
        if stats['total_files'] % 100 == 0:
            logger.info(f"Progress: {stats['total_files']} files processed...")
    
    # Final report
    logger.info("\n" + "="*50)
    logger.info("CLEANUP SUMMARY")
    logger.info("="*50)
    logger.info(f"Total files processed: {stats['total_files']}")
    logger.info(f"Clean files (no issues): {stats['clean_files']}")
    logger.info(f"Files with issues found: {stats['files_with_issues']}")
    logger.info(f"Files successfully cleaned: {stats['cleaned_files']}")
    logger.info(f"Files deleted (corrupted): {stats['deleted_files']}")
    logger.info(f"Errors encountered: {stats['errors']}")
    logger.info(f"Backup directory: {backup_dir}")
    logger.info("="*50)
    
    # Show some examples of issues found
    if stats['files_with_issues'] > 0:
        logger.info(f"\nProcessed {stats['files_with_issues']} files with issues.")
        logger.info(f"Check the backup directory for original copies: {backup_dir}")

if __name__ == "__main__":
    clean_corpus_files()