#!/usr/bin/env python3
"""
Utility script to reset the state of letras.com URLs in the database,
allowing them to be reprocessed with the fixed scraper.
"""
import os
import sys
import logging
import yaml
import sqlite3
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("reset_letras")

def load_config(config_path="config.yaml"):
    """Load the configuration file to find the state database path."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        # Extract the database path from the state configuration
        state_dir = Path(config.get('storage', {}).get('state_dir', './data/state'))
        return state_dir
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)

def connect_to_db(state_dir):
    """Connect to the SQLite database."""
    db_path = state_dir / 'scraper_state.db'
    
    if not db_path.exists():
        logger.error(f"Database file not found: {db_path}")
        sys.exit(1)
        
    logger.info(f"Connecting to database: {db_path}")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn

def reset_letras_urls(conn, dry_run=False):
    """
    Reset the state of all letras.com URLs to 'pending'
    so they can be reprocessed with the fixed scraper.
    
    Args:
        conn: SQLite database connection
        dry_run: If True, don't make any changes, just show what would be done
    
    Returns:
        Number of URLs that were reset
    """
    try:
        # Find all letras.com URLs
        cursor = conn.execute("""
            SELECT url_hash, url, status
            FROM url_status
            WHERE url LIKE '%letras.com%'
        """)
        
        letras_urls = list(cursor.fetchall())
        logger.info(f"Found {len(letras_urls)} letras.com URLs")
        
        if not letras_urls:
            logger.info("No letras.com URLs found in the database.")
            return 0
            
        # Group by status
        status_counts = {}
        for row in letras_urls:
            status = row['status']
            if status not in status_counts:
                status_counts[status] = 0
            status_counts[status] += 1
            
        logger.info(f"Status distribution: {status_counts}")
        
        if dry_run:
            logger.info("DRY RUN: No changes will be made")
            logger.info(f"Would reset {len(letras_urls)} letras.com URLs to 'pending'")
            for i, row in enumerate(letras_urls[:5], 1):
                logger.info(f"Example {i}: {row['url']} (currently '{row['status']}')")
            if len(letras_urls) > 5:
                logger.info(f"...and {len(letras_urls) - 5} more")
            return len(letras_urls)
        
        # Reset URLs to 'pending' status
        cursor = conn.execute("""
            UPDATE url_status
            SET status = 'pending', 
                error_message = NULL,
                attempts = 0, 
                updated_at = ?
            WHERE url LIKE '%letras.com%'
        """, (datetime.now().isoformat(),))
        
        reset_count = cursor.rowcount
        conn.commit()
        
        logger.info(f"Reset {reset_count} letras.com URLs to 'pending' status")
        return reset_count
        
    except Exception as e:
        logger.error(f"Error resetting letras.com URLs: {e}")
        conn.rollback()
        return 0

def main():
    """Main function to reset letras.com URLs."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Reset letras.com URLs in the database")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Don't make changes, just show what would be done")
    args = parser.parse_args()
    
    state_dir = load_config(args.config)
    conn = connect_to_db(state_dir)
    
    try:
        reset_count = reset_letras_urls(conn, args.dry_run)
        
        if not args.dry_run:
            logger.info(f"Successfully reset {reset_count} letras.com URLs")
            logger.info("They will be reprocessed with the fixed scraper on the next run")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
