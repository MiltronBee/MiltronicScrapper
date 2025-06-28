#!/usr/bin/env python3
"""
Fix content handling for letras.com scraper in the main orchestration code.
This patches the critical parts of the system responsible for handling content
between extraction and saving to ensure proper UTF-8 encoding.
"""
import os
import re
import logging
import inspect
import importlib
import types
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("fix_letras_content_handler")

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

def patch_orchestrator():
    """
    Patch the Orchestrator class to ensure proper handling of extracted content.
    This is done dynamically rather than modifying the file directly.
    """
    try:
        # Import the orchestrator module
        from corpus_scraper.orchestrator import Orchestrator
        
        # Store original method for reference
        original_process_url = Orchestrator._process_url
        
        # Define patched method
        def patched_process_url(self, url, source_info):
            """Patched version of _process_url that ensures proper encoding."""
            try:
                # Call the original method
                result = original_process_url(self, url, source_info)
                
                # If this is a letras.com URL, ensure the content is properly encoded
                if 'letras.com' in url.lower():
                    if result and 'content' in result and result['content']:
                        # Ensure content is properly UTF-8 encoded
                        result['content'] = ensure_utf8_text(result['content'])
                        logger.info(f"Applied UTF-8 encoding fix to content from {url}")
                
                return result
                
            except Exception as e:
                logger.error(f"Error in patched _process_url: {e}")
                return None
        
        # Apply the patch
        Orchestrator._process_url = patched_process_url
        logger.info("Successfully patched Orchestrator._process_url")
        
        # Also patch the _save_content method to handle encoding
        original_save_content = Orchestrator._save_content
        
        # Define patched save method
        def patched_save_content(self, content, source_name, url, metadata=None):
            """Patched version of _save_content that ensures proper encoding for letras.com."""
            try:
                # If this is a letras.com source, ensure content is properly encoded
                if source_name == 'letras_com' and content:
                    content = ensure_utf8_text(content)
                    logger.info(f"Applied UTF-8 encoding fix before saving content from {url}")
                
                # Call the original method
                return original_save_content(self, content, source_name, url, metadata)
                
            except Exception as e:
                logger.error(f"Error in patched _save_content: {e}")
                return {'saved': False, 'error': str(e)}
        
        # Apply the patch
        Orchestrator._save_content = patched_save_content
        logger.info("Successfully patched Orchestrator._save_content")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to patch Orchestrator: {e}")
        return False

def patch_saver():
    """
    Patch the Saver class to ensure proper handling of text content encoding.
    """
    try:
        # Import the saver module
        from corpus_scraper.saver import Saver
        
        # Store original method for reference
        original_save_text = Saver.save_text
        
        # Define patched method
        def patched_save_text(self, content, source_name, url=None, metadata=None):
            """Patched version of save_text that ensures proper encoding."""
            try:
                # Ensure content is properly UTF-8 encoded, especially for letras.com
                if content and source_name == 'letras_com':
                    content = ensure_utf8_text(content)
                    logger.info(f"Applied UTF-8 encoding fix in save_text for {source_name}")
                
                # Call the original method
                return original_save_text(self, content, source_name, url, metadata)
                
            except Exception as e:
                logger.error(f"Error in patched save_text: {e}")
                return {'saved': False, 'error': str(e)}
        
        # Apply the patch
        Saver.save_text = patched_save_text
        logger.info("Successfully patched Saver.save_text")
        
        # Patch the _atomic_write method as well
        original_atomic_write = Saver._atomic_write
        
        # Define patched atomic write method
        def patched_atomic_write(self, file_path, content, source_name=None):
            """Patched version of _atomic_write that ensures proper encoding."""
            try:
                # Ensure content is properly UTF-8 encoded, especially for letras.com
                if content and source_name == 'letras_com':
                    content = ensure_utf8_text(content)
                    self.logger.info(f"Applied UTF-8 encoding fix in _atomic_write for {source_name}")
                elif content:
                    # For all content, ensure it's a string
                    content = ensure_utf8_text(content)
                
                # Call the original method
                return original_atomic_write(self, file_path, content, source_name)
                
            except Exception as e:
                self.logger.error(f"Error in patched _atomic_write: {e}")
                return False
        
        # Apply the patch
        Saver._atomic_write = patched_atomic_write
        logger.info("Successfully patched Saver._atomic_write")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to patch Saver: {e}")
        return False

def reset_letras_urls():
    """Reset all letras.com URLs to pending status so they can be reprocessed."""
    try:
        # Import the reset module
        sys_path = os.getcwd()
        import sys
        if sys_path not in sys.path:
            sys.path.append(sys_path)
        
        from reset_letras_urls import reset_urls
        
        # Reset all URLs
        count = reset_urls()
        logger.info(f"Reset {count} letras.com URLs")
        
        return count
        
    except Exception as e:
        logger.error(f"Failed to reset URLs: {e}")
        return 0

def main():
    """Apply all patches and reset letras.com URLs."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Fix letras.com content handling")
    parser.add_argument("--skip-reset", action="store_true", help="Skip resetting letras.com URLs")
    args = parser.parse_args()
    
    # Apply patches
    result1 = patch_orchestrator()
    result2 = patch_saver()
    
    if result1 and result2:
        logger.info("✅ Successfully applied all patches to fix letras.com content handling")
    else:
        logger.error("❌ Failed to apply some patches")
        
    # Reset URLs if not skipped
    if not args.skip_reset:
        count = reset_letras_urls()
        if count > 0:
            logger.info(f"Reset {count} letras.com URLs for reprocessing")
        else:
            logger.warning("No letras.com URLs were reset")
    
    logger.info("To process the URLs with fixed handling, run: python main.py")

if __name__ == "__main__":
    main()
