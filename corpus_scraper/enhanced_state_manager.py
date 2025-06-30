"""
Enhanced State Manager with token tracking and advanced metrics.
Extends the existing state manager with high-yield harvesting capabilities.
"""

import sqlite3
import logging
import hashlib
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import json
from .exceptions import ScrapingError


class EnhancedStateManager:
    """
    Enhanced state management with token counting and yield metrics.
    Tracks processing state, performance metrics, and data yield.
    """
    
    def __init__(self, storage_config: Dict[str, Any]):
        self.storage_config = storage_config
        self.logger = logging.getLogger(__name__)
        
        # Database configuration
        self.db_path = Path(storage_config['state_dir']) / 'enhanced_state.db'
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Connection with enhanced settings
        self.conn = None
        self._initialize_database()
        
        # Performance tracking
        self.session_stats = {
            'session_start': datetime.now(),
            'urls_processed': 0,
            'tokens_collected': 0,
            'errors_count': 0
        }
    
    def _initialize_database(self):
        """Initialize SQLite database with enhanced schema."""
        try:
            self.conn = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False,
                timeout=30
            )
            self.conn.row_factory = sqlite3.Row
            
            # Enable WAL mode for better concurrency
            self.conn.execute('PRAGMA journal_mode=WAL')
            self.conn.execute('PRAGMA synchronous=NORMAL')
            self.conn.execute('PRAGMA cache_size=10000')
            self.conn.execute('PRAGMA temp_store=memory')
            
            self._create_enhanced_schema()
            self.logger.info("Enhanced state manager initialized")
            
        except Exception as e:
            raise ScrapingError(f"Failed to initialize enhanced state database: {e}")
    
    def _create_enhanced_schema(self):
        """Create enhanced database schema with token tracking."""
        
        # Enhanced URLs table with token tracking
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url_hash TEXT UNIQUE NOT NULL,
                url TEXT NOT NULL,
                source TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                error_message TEXT,
                content_hash TEXT,
                file_path TEXT,
                
                -- Enhanced fields for high-yield harvesting
                token_count INTEGER DEFAULT 0,
                content_size INTEGER DEFAULT 0,
                mexican_score REAL DEFAULT 0.0,
                extraction_method TEXT,
                processing_time_ms INTEGER DEFAULT 0,
                retry_count INTEGER DEFAULT 0,
                priority_score REAL DEFAULT 1.0,
                discovered_from TEXT,
                content_type TEXT,
                has_comments BOOLEAN DEFAULT FALSE,
                comment_count INTEGER DEFAULT 0,
                link_discovery_count INTEGER DEFAULT 0
            )
        ''')
        
        # Performance metrics table
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metric_name TEXT NOT NULL,
                metric_value REAL NOT NULL,
                source TEXT,
                additional_data TEXT
            )
        ''')
        
        # Session tracking table
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT UNIQUE NOT NULL,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                urls_processed INTEGER DEFAULT 0,
                tokens_collected INTEGER DEFAULT 0,
                success_rate REAL DEFAULT 0.0,
                avg_processing_time_ms REAL DEFAULT 0.0,
                session_config TEXT
            )
        ''')
        
        # Source statistics table
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS source_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_urls INTEGER DEFAULT 0,
                completed_urls INTEGER DEFAULT 0,
                failed_urls INTEGER DEFAULT 0,
                total_tokens INTEGER DEFAULT 0,
                avg_mexican_score REAL DEFAULT 0.0,
                avg_processing_time_ms REAL DEFAULT 0.0,
                success_rate REAL DEFAULT 0.0,
                
                UNIQUE(source_name)
            )
        ''')
        
        # Domain yield tracking
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS domain_yield (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_urls INTEGER DEFAULT 0,
                successful_urls INTEGER DEFAULT 0,
                total_tokens INTEGER DEFAULT 0,
                avg_tokens_per_url REAL DEFAULT 0.0,
                avg_mexican_score REAL DEFAULT 0.0,
                yield_efficiency REAL DEFAULT 0.0,
                
                UNIQUE(domain)
            )
        ''')
        
        # Create indexes for performance
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_urls_status ON urls(status)',
            'CREATE INDEX IF NOT EXISTS idx_urls_source ON urls(source)',
            'CREATE INDEX IF NOT EXISTS idx_urls_updated ON urls(updated_at)',
            'CREATE INDEX IF NOT EXISTS idx_urls_priority ON urls(priority_score DESC)',
            'CREATE INDEX IF NOT EXISTS idx_urls_tokens ON urls(token_count)',
            'CREATE INDEX IF NOT EXISTS idx_performance_timestamp ON performance_metrics(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_performance_metric ON performance_metrics(metric_name)',
            'CREATE INDEX IF NOT EXISTS idx_sessions_start ON sessions(start_time)'
        ]
        
        for index_sql in indexes:
            self.conn.execute(index_sql)
        
        self.conn.commit()
    
    def add_enhanced_url(self, url: str, source: str, priority_score: float = 1.0,
                        discovered_from: str = None, content_type: str = None) -> bool:
        """
        Add URL with enhanced metadata and priority scoring.
        
        Args:
            url: URL to add
            source: Source name
            priority_score: Priority score for processing order
            discovered_from: How this URL was discovered
            content_type: Expected content type
            
        Returns:
            True if URL was added, False if already exists
        """
        try:
            url_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()
            
            self.conn.execute('''
                INSERT OR IGNORE INTO urls (
                    url_hash, url, source, priority_score, 
                    discovered_from, content_type
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (url_hash, url, source, priority_score, discovered_from, content_type))
            
            # Check if it was actually inserted
            if self.conn.total_changes > 0:
                self.conn.commit()
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"Error adding enhanced URL {url}: {e}")
            return False
    
    def add_enhanced_urls(self, urls: List[str], source: str, 
                         discovery_metadata: Dict[str, Any] = None) -> int:
        """
        Add multiple URLs with enhanced metadata in batch.
        
        Args:
            urls: List of URLs to add
            source: Source name
            discovery_metadata: Metadata about how URLs were discovered
            
        Returns:
            Number of new URLs added
        """
        if not urls:
            return 0
        
        try:
            url_data = []
            for url in urls:
                url_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()
                
                # Determine priority based on URL characteristics
                priority_score = self._calculate_url_priority(url, source)
                
                url_data.append((
                    url_hash, url, source, priority_score,
                    discovery_metadata.get('method') if discovery_metadata else None,
                    discovery_metadata.get('content_type') if discovery_metadata else None
                ))
            
            # Batch insert
            self.conn.executemany('''
                INSERT OR IGNORE INTO urls (
                    url_hash, url, source, priority_score,
                    discovered_from, content_type
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', url_data)
            
            added_count = self.conn.total_changes
            self.conn.commit()
            
            # Update source statistics
            self._update_source_stats(source)
            
            if added_count > 0:
                self.logger.info(f"Added {added_count} new URLs for source '{source}'")
            
            return added_count
            
        except Exception as e:
            self.logger.error(f"Error adding enhanced URLs for {source}: {e}")
            return 0
    
    def _calculate_url_priority(self, url: str, source: str) -> float:
        """Calculate priority score for URL based on characteristics."""
        priority = 1.0
        url_lower = url.lower()
        
        # Government and academic content gets higher priority
        if any(domain in url_lower for domain in ['.gob.mx', '.edu.mx', 'unam', 'ipn']):
            priority += 2.0
        
        # News and recent content
        if any(term in url_lower for term in ['noticia', 'articulo', '2024', '2025']):
            priority += 1.0
        
        # PDF documents often have high-quality content
        if url_lower.endswith('.pdf'):
            priority += 1.5
        
        # Reddit and social media (high volume, lower individual priority)
        if 'reddit.com' in url_lower or 'youtube.com' in url_lower:
            priority += 0.5
        
        # Mexican geographic indicators
        mexican_terms = ['mexico', 'mexicano', 'cdmx', 'guadalajara', 'monterrey']
        if any(term in url_lower for term in mexican_terms):
            priority += 0.8
        
        return min(priority, 10.0)  # Cap at 10.0
    
    def update_enhanced_url_status(self, url_hash: str, status: str, 
                                  processing_result: Dict[str, Any] = None) -> bool:
        """
        Update URL status with enhanced processing results.
        
        Args:
            url_hash: URL hash
            status: New status
            processing_result: Enhanced processing results
            
        Returns:
            True if update successful
        """
        try:
            update_data = {
                'status': status,
                'updated_at': datetime.now()
            }
            
            if processing_result:
                # Basic fields
                if 'error' in processing_result:
                    update_data['error_message'] = processing_result['error']
                if 'content_hash' in processing_result:
                    update_data['content_hash'] = processing_result['content_hash']
                if 'file_path' in processing_result:
                    update_data['file_path'] = processing_result['file_path']
                
                # Enhanced fields
                if 'token_count' in processing_result:
                    update_data['token_count'] = processing_result['token_count']
                if 'content_size' in processing_result:
                    update_data['content_size'] = processing_result['content_size']
                if 'mexican_score' in processing_result:
                    update_data['mexican_score'] = processing_result['mexican_score']
                if 'extraction_method' in processing_result:
                    update_data['extraction_method'] = processing_result['extraction_method']
                if 'processing_time_ms' in processing_result:
                    update_data['processing_time_ms'] = processing_result['processing_time_ms']
                if 'comments_count' in processing_result:
                    update_data['has_comments'] = processing_result['comments_count'] > 0
                    update_data['comment_count'] = processing_result['comments_count']
                if 'discovered_links' in processing_result:
                    update_data['link_discovery_count'] = len(processing_result['discovered_links'])
            
            # Build dynamic SQL
            set_clause = ', '.join([f"{key} = ?" for key in update_data.keys()])
            values = list(update_data.values()) + [url_hash]
            
            self.conn.execute(f'''
                UPDATE urls SET {set_clause} WHERE url_hash = ?
            ''', values)
            
            success = self.conn.total_changes > 0
            if success:
                self.conn.commit()
                
                # Update session stats
                self.session_stats['urls_processed'] += 1
                if processing_result and 'token_count' in processing_result:
                    self.session_stats['tokens_collected'] += processing_result['token_count']
                if status == 'failed':
                    self.session_stats['errors_count'] += 1
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error updating enhanced URL status: {e}")
            return False
    
    def get_priority_urls(self, limit: int = 50, min_priority: float = 0.0) -> List[Dict[str, Any]]:
        """
        Get URLs for processing ordered by priority score.
        
        Args:
            limit: Maximum number of URLs to return
            min_priority: Minimum priority score
            
        Returns:
            List of URL records with enhanced metadata
        """
        try:
            cursor = self.conn.execute('''
                SELECT url_hash, url, source, priority_score, discovered_from,
                       content_type, retry_count, created_at
                FROM urls 
                WHERE status = 'pending' AND priority_score >= ?
                ORDER BY priority_score DESC, created_at ASC
                LIMIT ?
            ''', (min_priority, limit))
            
            urls = []
            for row in cursor.fetchall():
                urls.append({
                    'url_hash': row['url_hash'],
                    'url': row['url'],
                    'source': row['source'],
                    'priority_score': row['priority_score'],
                    'discovered_from': row['discovered_from'],
                    'content_type': row['content_type'],
                    'retry_count': row['retry_count'],
                    'created_at': row['created_at']
                })
            
            # Mark as processing
            if urls:
                url_hashes = [url['url_hash'] for url in urls]
                placeholders = ', '.join(['?' for _ in url_hashes])
                self.conn.execute(f'''
                    UPDATE urls SET status = 'processing', updated_at = CURRENT_TIMESTAMP
                    WHERE url_hash IN ({placeholders})
                ''', url_hashes)
                self.conn.commit()
            
            return urls
            
        except Exception as e:
            self.logger.error(f"Error getting priority URLs: {e}")
            return []
    
    def get_enhanced_progress_stats(self) -> Dict[str, Any]:
        """Get enhanced progress statistics with token metrics."""
        try:
            # Overall statistics
            overall_cursor = self.conn.execute('''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status LIKE 'failed%' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status = 'blocked' THEN 1 ELSE 0 END) as blocked,
                    SUM(token_count) as total_tokens,
                    AVG(token_count) as avg_tokens_per_url,
                    SUM(content_size) as total_content_size,
                    AVG(mexican_score) as avg_mexican_score,
                    AVG(processing_time_ms) as avg_processing_time
                FROM urls
            ''')
            
            overall_row = overall_cursor.fetchone()
            
            # Source-wise statistics
            source_cursor = self.conn.execute('''
                SELECT 
                    source,
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status LIKE 'failed%' THEN 1 ELSE 0 END) as failed,
                    SUM(token_count) as tokens,
                    AVG(token_count) as avg_tokens,
                    AVG(mexican_score) as avg_mexican_score,
                    AVG(priority_score) as avg_priority
                FROM urls 
                GROUP BY source
                ORDER BY tokens DESC
            ''')
            
            sources = {}
            for row in source_cursor.fetchall():
                success_rate = (row['completed'] / row['total'] * 100) if row['total'] > 0 else 0
                sources[row['source']] = {
                    'total': row['total'],
                    'completed': row['completed'],
                    'failed': row['failed'],
                    'success_rate': round(success_rate, 1),
                    'tokens': row['tokens'] or 0,
                    'avg_tokens': round(row['avg_tokens'] or 0, 1),
                    'avg_mexican_score': round(row['avg_mexican_score'] or 0, 2),
                    'avg_priority': round(row['avg_priority'] or 0, 2)
                }
            
            # Domain yield statistics
            domain_cursor = self.conn.execute('''
                SELECT 
                    SUBSTR(url, INSTR(url, '://') + 3) as domain_part,
                    COUNT(*) as total_urls,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_urls,
                    SUM(token_count) as total_tokens,
                    AVG(token_count) as avg_tokens_per_url
                FROM urls
                WHERE url LIKE 'http%'
                GROUP BY domain_part
                HAVING total_urls >= 5
                ORDER BY total_tokens DESC
                LIMIT 20
            ''')
            
            domains = {}
            for row in domain_cursor.fetchall():
                domain = row['domain_part'].split('/')[0]  # Extract just domain
                yield_efficiency = (row['total_tokens'] / row['total_urls']) if row['total_urls'] > 0 else 0
                
                domains[domain] = {
                    'total_urls': row['total_urls'],
                    'completed_urls': row['completed_urls'],
                    'total_tokens': row['total_tokens'] or 0,
                    'avg_tokens_per_url': round(row['avg_tokens_per_url'] or 0, 1),
                    'yield_efficiency': round(yield_efficiency, 1)
                }
            
            # Performance metrics
            performance_cursor = self.conn.execute('''
                SELECT 
                    AVG(CASE WHEN token_count > 0 THEN token_count ELSE NULL END) as tokens_per_success,
                    COUNT(CASE WHEN status = 'completed' AND created_at > datetime('now', '-1 hour') THEN 1 END) as recent_successes,
                    COUNT(CASE WHEN status = 'completed' AND created_at > datetime('now', '-1 day') THEN 1 END) as daily_successes
                FROM urls
            ''')
            
            perf_row = performance_cursor.fetchone()
            
            # Calculate progress percentages
            total_tokens = overall_row['total_tokens'] or 0
            target_tokens = 1000000000  # 1 billion target
            progress_pct = (total_tokens / target_tokens) * 100
            
            return {
                'overall': {
                    'total': overall_row['total'],
                    'pending': overall_row['pending'],
                    'processing': overall_row['processing'],
                    'completed': overall_row['completed'],
                    'failed': overall_row['failed'],
                    'blocked': overall_row['blocked'],
                    'success_rate': round((overall_row['completed'] / overall_row['total'] * 100) if overall_row['total'] > 0 else 0, 1)
                },
                'tokens': {
                    'total_tokens': total_tokens,
                    'avg_tokens_per_url': round(overall_row['avg_tokens_per_url'] or 0, 1),
                    'tokens_per_success': round(perf_row['tokens_per_success'] or 0, 1),
                    'target_tokens': target_tokens,
                    'progress_percentage': round(progress_pct, 2)
                },
                'content': {
                    'total_size_bytes': overall_row['total_content_size'] or 0,
                    'total_size_mb': round((overall_row['total_content_size'] or 0) / (1024*1024), 2),
                    'avg_mexican_score': round(overall_row['avg_mexican_score'] or 0, 2),
                    'avg_processing_time_ms': round(overall_row['avg_processing_time'] or 0, 1)
                },
                'performance': {
                    'recent_successes_1h': perf_row['recent_successes'],
                    'daily_successes': perf_row['daily_successes'],
                    'processing_rate_per_hour': perf_row['recent_successes'],
                    'session_stats': self.session_stats
                },
                'sources': sources,
                'top_domains': domains
            }
            
        except Exception as e:
            self.logger.error(f"Error getting enhanced progress stats: {e}")
            return {'error': str(e)}
    
    def _update_source_stats(self, source: str):
        """Update source statistics table."""
        try:
            self.conn.execute('''
                INSERT OR REPLACE INTO source_stats (
                    source_name, last_updated, total_urls, completed_urls, 
                    failed_urls, total_tokens, avg_mexican_score, 
                    avg_processing_time_ms, success_rate
                )
                SELECT 
                    source as source_name,
                    CURRENT_TIMESTAMP as last_updated,
                    COUNT(*) as total_urls,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_urls,
                    SUM(CASE WHEN status LIKE 'failed%' THEN 1 ELSE 0 END) as failed_urls,
                    SUM(token_count) as total_tokens,
                    AVG(mexican_score) as avg_mexican_score,
                    AVG(processing_time_ms) as avg_processing_time_ms,
                    CAST(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS REAL) / COUNT(*) * 100 as success_rate
                FROM urls 
                WHERE source = ?
                GROUP BY source
            ''', (source,))
            
            self.conn.commit()
            
        except Exception as e:
            self.logger.debug(f"Error updating source stats for {source}: {e}")
    
    def record_performance_metric(self, metric_name: str, value: float, 
                                source: str = None, additional_data: Dict = None):
        """Record a performance metric for monitoring."""
        try:
            additional_json = json.dumps(additional_data) if additional_data else None
            
            self.conn.execute('''
                INSERT INTO performance_metrics 
                (metric_name, metric_value, source, additional_data)
                VALUES (?, ?, ?, ?)
            ''', (metric_name, value, source, additional_json))
            
            self.conn.commit()
            
        except Exception as e:
            self.logger.debug(f"Error recording performance metric: {e}")
    
    def cleanup_old_data(self, days_old: int = 30):
        """Clean up old performance metrics and completed URLs."""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_old)
            
            # Clean old performance metrics
            self.conn.execute('''
                DELETE FROM performance_metrics 
                WHERE timestamp < ?
            ''', (cutoff_date,))
            
            # Clean very old completed URLs (keep recent for deduplication)
            self.conn.execute('''
                DELETE FROM urls 
                WHERE status = 'completed' AND updated_at < ?
            ''', (cutoff_date,))
            
            self.conn.commit()
            self.logger.info(f"Cleaned up data older than {days_old} days")
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {e}")
    
    def close(self):
        """Close database connection."""
        if self.conn:
            try:
                self.conn.close()
                self.logger.info("Enhanced state manager closed")
            except Exception as e:
                self.logger.error(f"Error closing enhanced state manager: {e}")