#!/usr/bin/env python3
"""
Utility script to fix the encoding of letras.com files that were saved incorrectly.
Also provides a patch to the Saver class to ensure correct encoding for all future files.
"""
import os
import re
import logging
import sqlite3
import yaml
from pathlib import Path
import shutil
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("fix_letras_encoding")

def load_config(config_path="config.yaml"):
    """Load the configuration file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)

def patch_saver_class():
    """
    Patch the Saver class to ensure it uses UTF-8 encoding when saving files.
    This is a safer approach than modifying the original file directly.
    """
    try:
        # Path to the saver.py file
        saver_path = Path("./corpus_scraper/saver.py")
        
        if not saver_path.exists():
            logger.error(f"Could not find Saver class at {saver_path}")
            return False
            
        # Read the current file
        with open(saver_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Check if we've already patched this file
        if "# PATCHED FOR UTF-8 ENCODING" in content:
            logger.info("Saver class is already patched for UTF-8 encoding.")
            return True
            
        # Create a backup
        backup_path = saver_path.with_suffix(".py.bak")
        shutil.copy2(saver_path, backup_path)
        logger.info(f"Created backup of Saver class at {backup_path}")
        
        # Replace the _atomic_write method with our fixed version
        pattern = re.compile(r'def _atomic_write\(self, file_path: Path, content: str, source_name: str = None\) -> bool:.*?(?=def \w+\()', re.DOTALL)
        replacement = '''def _atomic_write(self, file_path: Path, content: str, source_name: str = None) -> bool:
        """
        Write content to file atomically to prevent corruption.
        
        Args:
            file_path: Target file path
            content: Content to write
            source_name: Optional source name for special encoding handling
            
        Returns:
            True if write was successful, False otherwise
        """
        temp_path = file_path.with_suffix(file_path.suffix + '.tmp')
        
        try:
            # PATCHED FOR UTF-8 ENCODING - Explicitly force UTF-8 for all files
            encoding = 'utf-8'
            
            # For letras.com files, ensure content is properly encoded
            if source_name == 'letras_com':
                # Ensure the content is properly decoded and re-encoded
                try:
                    # First try to ensure we're working with a Unicode string
                    if isinstance(content, bytes):
                        content = content.decode('utf-8', errors='replace')
                        
                    # Re-encode and decode to clean any potential issues
                    content = content.encode('utf-8', errors='replace').decode('utf-8')
                    self.logger.info(f"Successfully sanitized letras_com content encoding")
                except Exception as e:
                    self.logger.warning(f"Error during letras_com encoding sanitization: {e}")
            
            # Write to temporary file with explicit UTF-8 encoding
            with open(temp_path, 'w', encoding=encoding) as f:
                f.write(content)
                f.flush()  # Ensure data is written to disk
                os.fsync(f.fileno())  # Force OS to write to disk
            
            # Atomically rename temporary file to final name
            temp_path.rename(file_path)
            
            self.logger.debug(f"Atomically wrote {len(content)} characters to {file_path} with {encoding} encoding")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write {file_path}: {e}")
            
            # Clean up temporary file if it exists
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except:
                pass
            
            return False
    
    def '''
        
        # Apply the replacement
        new_content = pattern.sub(replacement, content)
        
        # Write the updated file
        with open(saver_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
            
        logger.info("Successfully patched Saver class for proper UTF-8 encoding")
        return True
        
    except Exception as e:
        logger.error(f"Failed to patch Saver class: {e}")
        return False

def fix_saved_letras_files(storage_config):
    """
    Fix previously saved letras.com files with incorrect encoding.
    
    Args:
        storage_config: The storage configuration from the config.yaml file
    
    Returns:
        Number of files fixed
    """
    output_dir = Path(storage_config['output_dir']) / "letras_com"
    
    if not output_dir.exists():
        logger.error(f"Letras.com output directory not found: {output_dir}")
        return 0
        
    # Get all the text files in the directory
    files = list(output_dir.glob("*.txt"))
    logger.info(f"Found {len(files)} letras.com files to check")
    
    fixed_count = 0
    failed_count = 0
    
    # Process each file
    for file_path in files:
        try:
            # Try to read the file with different encodings
            content = None
            encodings = ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1', 'cp1252']
            
            # First try normal reading to see if it's readable
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # If we can read at least some valid text, skip this file
                    if re.search(r'[a-zA-Z]{10,}', content):
                        logger.debug(f"File {file_path.name} appears to be valid UTF-8, skipping")
                        continue
            except UnicodeDecodeError:
                # If we get here, the file is corrupted and needs fixing
                pass
                
            # Try binary reading and detection
            with open(file_path, 'rb') as f:
                raw_content = f.read()
                
            # Try different encodings
            for encoding in encodings:
                try:
                    content = raw_content.decode(encoding, errors='replace')
                    # If this succeeded without too many replacement chars, use it
                    if content.count('�') < len(content) * 0.1:
                        break
                except Exception:
                    continue
                    
            if not content or content.count('�') > len(content) * 0.3:
                # If all attempts failed or too many replacement chars, get the song URL from the state database
                # And reset it to be re-scraped
                logger.warning(f"Could not decode file {file_path.name}, will need to be re-scraped")
                failed_count += 1
                # Rename it to .corrupted extension
                corrupted_path = file_path.with_suffix('.corrupted')
                file_path.rename(corrupted_path)
                continue
                
            # Fix the file by rewriting with correct UTF-8 encoding
            # Create a backup first
            backup_path = file_path.with_suffix('.bak')
            shutil.copy2(file_path, backup_path)
            
            # Write the fixed content
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
                
            logger.info(f"Fixed encoding for file: {file_path.name}")
            fixed_count += 1
            
        except Exception as e:
            logger.error(f"Error processing file {file_path.name}: {e}")
            failed_count += 1
    
    logger.info(f"Fixed encoding for {fixed_count} files, failed to fix {failed_count} files")
    return fixed_count

def main():
    """Main function to fix letras.com encoding issues."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Fix encoding issues with letras.com files and patch the Saver class")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--skip-patch", action="store_true", help="Skip patching the Saver class")
    parser.add_argument("--skip-files", action="store_true", help="Skip fixing already saved files")
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    storage_config = config.get('storage', {})
    
    # Patch the Saver class if not skipped
    if not args.skip_patch:
        success = patch_saver_class()
        if success:
            logger.info("Saver class has been patched to handle encoding properly")
        else:
            logger.warning("Failed to patch Saver class, manual intervention may be required")
    
    # Fix already saved files if not skipped
    if not args.skip_files:
        fixed_count = fix_saved_letras_files(storage_config)
        logger.info(f"Fixed encoding for {fixed_count} previously saved letras.com files")
    
    logger.info("Encoding fix complete!")
    logger.info("You can now reset and re-run the scraper to reprocess any corrupted letras.com URLs")

if __name__ == "__main__":
    main()
