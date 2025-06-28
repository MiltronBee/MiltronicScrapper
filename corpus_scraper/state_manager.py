"""
Robust state management and checkpointing system using SQLite.
Provides crash-resistant URL tracking and job resumption capabilities.
"""

import sqlite3
import logging
import threading
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from contextlib import contextmanager
from .exceptions import StateManagementError


class StateManager:
    """
    Manages persistent state using SQLite for crash-resistant scraping operations.
    Implements thread-safe operations and atomic transactions.
    """
    
    def __init__(self, state_config: Dict[str, Any]):
        self.state_config = state_config
        self.logger = logging.getLogger(__name__)
        
        # Database file path
        state_dir = Path(state_config['state_dir'])
        state_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = state_dir / 'scraper_state.db'
        
        # Thread-local storage for database connections
        self._local = threading.local()
        
        # Initialize database schema
        self._init_database()
        
        self.logger.info(f"StateManager initialized with database: {self.db_path}")
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self._local, 'connection'):
            self._local.connection = sqlite3.Connection(
                str(self.db_path),
                timeout=30.0,  # 30 second timeout for busy database
                check_same_thread=False
            )
            self._local.connection.row_factory = sqlite3.Row  # Enable column access by name
            # Enable WAL mode for better concurrency
            self._local.connection.execute('PRAGMA journal_mode=WAL')
            self._local.connection.execute('PRAGMA synchronous=NORMAL')
        
        return self._local.connection
    
    @contextmanager
    def _transaction(self):
        """Context manager for database transactions."""
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Database transaction failed: {e}")
            raise StateManagementError(f"Transaction failed: {e}")
    
    def _init_database(self):
        """Initialize database schema if it doesn't exist."""
        try:
            with self._transaction() as conn:
                # Create main URL tracking table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS url_status (
                        url_hash TEXT PRIMARY KEY,
                        url TEXT NOT NULL,
                        source TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        last_attempt TIMESTAMP,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        error_message TEXT,
                        content_hash TEXT,
                        file_path TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create index for efficient queries
                conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_status_source 
                    ON url_status(status, source)
                ''')
                
                # Create session tracking table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS scraping_sessions (
                        session_id TEXT PRIMARY KEY,
                        start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        end_time TIMESTAMP,
                        total_urls INTEGER DEFAULT 0,
                        successful_urls INTEGER DEFAULT 0,
                        failed_urls INTEGER DEFAULT 0,
                        config_hash TEXT
                    )
                ''')
                
                self.logger.info("Database schema initialized successfully")
                
        except Exception as e:
            raise StateManagementError(f"Failed to initialize database: {e}")
    
    def _hash_url(self, url: str) -> str:
        """Generate consistent hash for URL."""
        import hashlib
        return hashlib.sha256(url.encode('utf-8')).hexdigest()
    
    def add_urls(self, urls: List[str], source_name: str) -> int:
        """
        Add URLs to the tracking database.
        
        Args:
            urls: List of URLs to add
            source_name: Name of the source these URLs belong to
            
        Returns:
            Number of new URLs added (excluding duplicates)
        """
        if not urls:
            return 0
        
        added_count = 0
        
        try:
            with self._transaction() as conn:
                for url in urls:
                    url_hash = self._hash_url(url)
                    
                    # Use INSERT OR IGNORE to handle duplicates gracefully
                    cursor = conn.execute('''
                        INSERT OR IGNORE INTO url_status (url_hash, url, source, status)
                        VALUES (?, ?, ?, 'pending')
                    ''', (url_hash, url, source_name))
                    
                    if cursor.rowcount > 0:
                        added_count += 1
                
                self.logger.info(f"Added {added_count} new URLs for source '{source_name}'")
                
        except Exception as e:
            raise StateManagementError(f"Failed to add URLs: {e}")
        
        return added_count
    
    def get_pending_urls(self, source_name: Optional[str] = None, 
                        limit: int = 100, max_attempts: int = 3) -> List[Dict[str, Any]]:
        """
        Get URLs that need to be processed.
        
        Args:
            source_name: Filter by source name (optional)
            limit: Maximum number of URLs to return
            max_attempts: Maximum retry attempts for failed URLs
            
        Returns:
            List of URL records ready for processing
        """
        try:
            with self._transaction() as conn:
                # Build query with optional source filter
                base_query = '''
                    SELECT url_hash, url, source, attempts, error_message
                    FROM url_status 
                    WHERE (status = 'pending' OR (status = 'failed' AND attempts < ?))
                '''
                params = [max_attempts]
                
                if source_name:
                    base_query += ' AND source = ?'
                    params.append(source_name)
                
                base_query += ' ORDER BY attempts ASC, created_at ASC LIMIT ?'
                params.append(limit)
                
                cursor = conn.execute(base_query, params)
                results = [dict(row) for row in cursor.fetchall()]
                
                # Mark selected URLs as 'processing' to prevent concurrent processing
                if results:
                    url_hashes = [record['url_hash'] for record in results]
                    placeholders = ','.join('?' * len(url_hashes))
                    conn.execute(f'''
                        UPDATE url_status 
                        SET status = 'processing', last_attempt = CURRENT_TIMESTAMP
                        WHERE url_hash IN ({placeholders})
                    ''', url_hashes)
                
                self.logger.debug(f"Retrieved {len(results)} URLs for processing")
                return results
                
        except Exception as e:
            raise StateManagementError(f"Failed to get pending URLs: {e}")
    
    def update_url_status(self, url_hash: str, status: str, 
                         error_message: Optional[str] = None,
                         content_hash: Optional[str] = None,
                         file_path: Optional[str] = None):
        """
        Update the status of a URL after processing.
        
        Args:
            url_hash: Hash of the URL
            status: New status ('completed', 'failed', 'failed_permanent')
            error_message: Error message if failed
            content_hash: Hash of extracted content if successful
            file_path: Path to saved file if successful
        """
        try:
            with self._transaction() as conn:
                # Increment attempts counter
                conn.execute('''
                    UPDATE url_status 
                    SET attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP
                    WHERE url_hash = ?
                ''', (url_hash,))
                
                # Update status and related fields
                conn.execute('''
                    UPDATE url_status 
                    SET status = ?, error_message = ?, content_hash = ?, 
                        file_path = ?, last_attempt = CURRENT_TIMESTAMP
                    WHERE url_hash = ?
                ''', (status, error_message, content_hash, file_path, url_hash))
                
                self.logger.debug(f"Updated URL {url_hash[:12]}... status to '{status}'")
                
        except Exception as e:
            raise StateManagementError(f"Failed to update URL status: {e}")
    
    def get_progress_stats(self) -> Dict[str, Any]:
        """Get overall progress statistics."""
        try:
            with self._transaction() as conn:
                # Overall statistics
                cursor = conn.execute('''
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                        SUM(CASE WHEN status = 'failed_permanent' THEN 1 ELSE 0 END) as failed_permanent,
                        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                        SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing
                    FROM url_status
                ''')
                overall = dict(cursor.fetchone())
                
                # Per-source statistics
                cursor = conn.execute('''
                    SELECT 
                        source,
                        COUNT(*) as total,
                        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending
                    FROM url_status 
                    GROUP BY source
                ''')
                by_source = {row['source']: dict(row) for row in cursor.fetchall()}
                
                # Calculate completion percentage
                total = overall['total']
                if total > 0:
                    overall['completion_percentage'] = round(
                        (overall['completed'] / total) * 100, 2
                    )
                else:
                    overall['completion_percentage'] = 0
                
                return {
                    'overall': overall,
                    'by_source': by_source,
                    'last_updated': datetime.now().isoformat()
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get progress stats: {e}")
            return {'overall': {}, 'by_source': {}, 'error': str(e)}
    
    def reset_processing_urls(self):
        """
        Reset URLs stuck in 'processing' status back to 'pending'.
        Useful for recovery after unexpected shutdowns.
        """
        try:
            with self._transaction() as conn:
                cursor = conn.execute('''
                    UPDATE url_status 
                    SET status = 'pending' 
                    WHERE status = 'processing'
                ''')
                
                reset_count = cursor.rowcount
                if reset_count > 0:
                    self.logger.info(f"Reset {reset_count} URLs from 'processing' to 'pending'")
                
                return reset_count
                
        except Exception as e:
            raise StateManagementError(f"Failed to reset processing URLs: {e}")
    
    def cleanup_old_sessions(self, days_old: int = 30):
        """Clean up old session records."""
        try:
            with self._transaction() as conn:
                cursor = conn.execute('''
                    DELETE FROM scraping_sessions 
                    WHERE start_time < datetime('now', '-{} days')
                '''.format(days_old))
                
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    self.logger.info(f"Cleaned up {deleted_count} old session records")
                
        except Exception as e:
            self.logger.warning(f"Failed to cleanup old sessions: {e}")
    
    def get_completed_urls(self, urls: List[str], source_name: Optional[str] = None) -> List[str]:
        """
        Obtener las URLs que ya han sido procesadas exitosamente.
        
        Args:
            urls: Lista de URLs a verificar
            source_name: Nombre de la fuente (opcional)
            
        Returns:
            Lista de URLs que ya han sido procesadas
        """
        if not urls:
            return []
            
        try:
            with self._transaction() as conn:
                processed_urls = []
                
                for url in urls:
                    url_hash = self._hash_url(url)
                    
                    query = 'SELECT url FROM url_status WHERE url_hash = ? AND status = "completed"'
                    params = [url_hash]
                    
                    if source_name:
                        query += ' AND source = ?'
                        params.append(source_name)
                    
                    cursor = conn.execute(query, params)
                    result = cursor.fetchone()
                    
                    if result:
                        processed_urls.append(url)
                
                self.logger.debug(f"Found {len(processed_urls)} already processed URLs out of {len(urls)}")
                return processed_urls
                
        except Exception as e:
            self.logger.error(f"Error checking processed URLs: {e}")
            return []  # En caso de error, devolver lista vacía
    
    def close(self):
        """Close database connections."""
        if hasattr(self._local, 'connection'):
            self._local.connection.close()
            self.logger.debug("Database connection closed")