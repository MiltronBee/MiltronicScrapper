"""
High-Yield Orchestrator: Enhanced central controller for aggressive data harvesting.
Coordinates all enhanced components for maximum Mexican Spanish content collection.
"""

import os
import logging
import time
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures

from .config_manager import ConfigManager
from .enhanced_extractor import EnhancedExtractor
from .enhanced_saver import EnhancedSaver
from .enhanced_state_manager import EnhancedStateManager
from .reddit_handler import RedditHandler
from .youtube_handler import YouTubeHandler
from .domain_harvester import DomainHarvester
from .enhanced_scraper import EnhancedScraper
from .dynamic_scraper import DynamicScraperSync
from .exceptions import ScrapingError, RobotsBlockedError, NetworkError


class HighYieldOrchestrator:
    """
    Enhanced orchestrator for high-yield Mexican Spanish content harvesting.
    Integrates all enhanced components for aggressive data collection.
    """
    
    def __init__(self, config_path: str = "config_enhanced.yaml", 
                 sources_path: str = "sources_enhanced.yaml"):
        self.logger = logging.getLogger(__name__)
        
        # Initialize enhanced configuration
        self.config_manager = ConfigManager(config_path, sources_path)
        
        # Initialize enhanced components
        self._initialize_enhanced_components()
        
        # Session tracking
        self.session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.session_start = datetime.now()
        
        # Performance metrics
        self.metrics = {
            'total_processed': 0,
            'total_tokens': 0,
            'total_size_bytes': 0,
            'success_rate': 0.0,
            'processing_rate': 0.0
        }
        
        # Specialized handlers
        self.reddit_handler = None
        self.youtube_handler = None
        self.domain_harvester = None
        
        self._initialize_specialized_handlers()
        
        self.logger.info("High-yield orchestrator initialized successfully")
    
    def _initialize_enhanced_components(self):
        """Initialize all enhanced framework components."""
        try:
            # Get enhanced configurations
            politeness_config = self.config_manager.get_politeness_config()
            extraction_config = self.config_manager.get_extraction_config()
            validation_config = self.config_manager.get_validation_config()
            storage_config = self.config_manager.get_storage_config()
            concurrency_config = self.config_manager.get_concurrency_config()
            
            # Browser automation config
            browser_config = extraction_config.get('browser_automation', {})
            
            # Initialize enhanced components
            self.scraper = EnhancedScraper(politeness_config)
            self.dynamic_scraper = None  # Initialize on demand
            
            self.extractor = EnhancedExtractor(extraction_config, validation_config)
            self.saver = EnhancedSaver(storage_config)
            self.state_manager = EnhancedStateManager(storage_config)
            
            # Concurrency settings
            self.num_threads = concurrency_config.get('num_threads', 16)
            self.browser_workers = concurrency_config.get('browser_workers', 4)
            
            self.logger.info(f"Enhanced components initialized with {self.num_threads} worker threads")
            
        except Exception as e:
            raise ScrapingError(f"Failed to initialize enhanced components: {e}")
    
    def _initialize_specialized_handlers(self):
        """Initialize specialized content handlers."""
        try:
            # Get harvesting configuration
            harvesting_config = self.config_manager.config.get('harvesting', {})
            
            # Reddit handler
            reddit_config = harvesting_config.get('reddit_api', {})
            if reddit_config.get('enabled', False):
                self.reddit_handler = RedditHandler(reddit_config)
            
            # YouTube handler
            youtube_config = harvesting_config.get('youtube_api', {})
            if youtube_config.get('enabled', False):
                self.youtube_handler = YouTubeHandler(youtube_config)
            
            # Domain harvester
            self.domain_harvester = DomainHarvester(harvesting_config)
            
            self.logger.info("Specialized handlers initialized")
            
        except Exception as e:
            self.logger.warning(f"Some specialized handlers failed to initialize: {e}")
    
    def discover_all_content(self) -> Dict[str, List[str]]:
        """
        Comprehensive content discovery using all available methods.
        
        Returns:
            Dictionary mapping source names to lists of URLs
        """
        self.logger.info("Starting comprehensive content discovery")
        all_discovered_urls = {}
        
        # 1. Traditional source discovery
        traditional_urls = self._discover_traditional_sources()
        all_discovered_urls.update(traditional_urls)
        
        # 2. Reddit content discovery
        reddit_urls = self._discover_reddit_content()
        all_discovered_urls.update(reddit_urls)
        
        # 3. YouTube transcript discovery
        youtube_urls = self._discover_youtube_content()
        all_discovered_urls.update(youtube_urls)
        
        # 4. Domain harvesting from Tranco
        harvested_urls = self._discover_tranco_domains()
        all_discovered_urls.update(harvested_urls)
        
        # 5. RSS and fresh content discovery
        fresh_urls = self._discover_fresh_content()
        all_discovered_urls.update(fresh_urls)
        
        total_urls = sum(len(urls) for urls in all_discovered_urls.values())
        self.logger.info(f"Comprehensive discovery complete: {total_urls} total URLs from {len(all_discovered_urls)} sources")
        
        return all_discovered_urls
    
    def _discover_traditional_sources(self) -> Dict[str, List[str]]:
        """Discover URLs from traditional configured sources."""
        self.logger.info("Discovering traditional source URLs")
        
        traditional_urls = {}
        sources = self.config_manager.get_sources()
        
        for source in sources:
            source_name = source['name']
            urls = []
            
            try:
                # Get URLs from source configuration
                if 'urls' in source:
                    urls.extend(source['urls'])
                
                if 'start_urls' in source:
                    urls.extend(source['start_urls'])
                
                # Generate dynamic URLs if enabled
                if source.get('dynamic_dates', False):
                    date_urls = self._generate_date_based_urls(source)
                    urls.extend(date_urls)
                
                # Filter already processed URLs
                if urls:
                    filtered_urls = self._filter_processed_urls(urls, source_name)
                    traditional_urls[source_name] = filtered_urls
                    
            except Exception as e:
                self.logger.error(f"Error discovering URLs for {source_name}: {e}")
                continue
        
        return traditional_urls
    
    def _discover_reddit_content(self) -> Dict[str, List[str]]:
        """Discover content from Reddit using API."""
        if not self.reddit_handler:
            return {}
        
        self.logger.info("Discovering Reddit content")
        
        try:
            reddit_content = self.reddit_handler.discover_content(limit=2000)
            
            # Convert content items to URL format for processing
            reddit_urls = {}
            
            for source_name, content_items in reddit_content.items():
                urls = []
                for item in content_items:
                    # Create pseudo-URLs for Reddit content
                    pseudo_url = f"reddit://{item['type']}/{item.get('subreddit', 'unknown')}/{hash(item['text'])}"
                    urls.append(pseudo_url)
                    
                    # Store content for later retrieval
                    self._store_reddit_content(pseudo_url, item)
                
                reddit_urls[source_name] = urls
            
            return reddit_urls
            
        except Exception as e:
            self.logger.error(f"Reddit content discovery failed: {e}")
            return {}
    
    def _discover_youtube_content(self) -> Dict[str, List[str]]:
        """Discover YouTube transcript content."""
        if not self.youtube_handler:
            return {}
        
        self.logger.info("Discovering YouTube content")
        
        try:
            # Get channel URLs from sources
            youtube_sources = [s for s in self.config_manager.get_sources() 
                             if s.get('type') == 'youtube_transcripts']
            
            all_channel_urls = []
            for source in youtube_sources:
                all_channel_urls.extend(source.get('urls', []))
            
            youtube_content = self.youtube_handler.discover_content(all_channel_urls)
            
            # Convert content items to URL format
            youtube_urls = {}
            
            for source_name, content_items in youtube_content.items():
                urls = []
                for item in content_items:
                    urls.append(item['url'])  # YouTube URLs are real URLs
                    
                    # Store transcript content for later retrieval
                    self._store_youtube_content(item['url'], item)
                
                youtube_urls[source_name] = urls
            
            return youtube_urls
            
        except Exception as e:
            self.logger.error(f"YouTube content discovery failed: {e}")
            return {}
    
    def _discover_tranco_domains(self) -> Dict[str, List[str]]:
        """Discover Mexican domains from Tranco list."""
        if not self.domain_harvester:
            return {}
        
        self.logger.info("Harvesting Mexican domains from Tranco")
        
        try:
            mexican_domains = self.domain_harvester.harvest_mexican_domains()
            
            if not mexican_domains:
                return {}
            
            # Convert domains to source configurations
            source_configs = self.domain_harvester.generate_source_urls(mexican_domains)
            
            # Group by category
            tranco_urls = {}
            for config in source_configs:
                category = config['domain_category']
                source_name = f"tranco_{category}"
                
                if source_name not in tranco_urls:
                    tranco_urls[source_name] = []
                
                tranco_urls[source_name].extend(config['urls'])
            
            return tranco_urls
            
        except Exception as e:
            self.logger.error(f"Tranco domain harvesting failed: {e}")
            return {}
    
    def _discover_fresh_content(self) -> Dict[str, List[str]]:
        """Discover fresh content from RSS feeds and news sources."""
        # Implementation similar to existing RSS discovery
        # but with enhanced source handling
        return {}
    
    def _store_reddit_content(self, pseudo_url: str, content_item: Dict):
        """Store Reddit content for later processing."""
        # Store in a temporary cache for processing
        if not hasattr(self, '_reddit_cache'):
            self._reddit_cache = {}
        self._reddit_cache[pseudo_url] = content_item
    
    def _store_youtube_content(self, url: str, content_item: Dict):
        """Store YouTube content for later processing."""
        # Store in a temporary cache for processing
        if not hasattr(self, '_youtube_cache'):
            self._youtube_cache = {}
        self._youtube_cache[url] = content_item
    
    def _filter_processed_urls(self, urls: List[str], source_name: str) -> List[str]:
        """Filter out already processed URLs."""
        # Use enhanced state manager to filter
        return urls  # Simplified for now
    
    def _generate_date_based_urls(self, source: Dict) -> List[str]:
        """Generate date-based URLs for news sources."""
        # Implementation similar to existing date URL generation
        return []
    
    def populate_enhanced_state(self, discovered_urls: Dict[str, List[str]]) -> int:
        """
        Populate state with discovered URLs using enhanced priority scoring.
        
        Args:
            discovered_urls: Dictionary of source -> URLs
            
        Returns:
            Total number of new URLs added
        """
        total_added = 0
        
        for source_name, urls in discovered_urls.items():
            if not urls:
                continue
            
            # Determine discovery metadata
            discovery_metadata = {
                'method': self._get_discovery_method(source_name),
                'timestamp': datetime.now().isoformat(),
                'content_type': self._infer_content_type(source_name)
            }
            
            # Add URLs with enhanced metadata
            added = self.state_manager.add_enhanced_urls(
                urls, source_name, discovery_metadata
            )
            total_added += added
            
            self.logger.info(f"Added {added} new URLs for enhanced source '{source_name}'")
        
        return total_added
    
    def _get_discovery_method(self, source_name: str) -> str:
        """Determine discovery method from source name."""
        if 'reddit' in source_name.lower():
            return 'reddit_api'
        elif 'youtube' in source_name.lower():
            return 'youtube_api'
        elif 'tranco' in source_name.lower():
            return 'tranco_harvest'
        else:
            return 'traditional_config'
    
    def _infer_content_type(self, source_name: str) -> str:
        """Infer content type from source name."""
        if 'reddit' in source_name.lower():
            return 'social_media'
        elif 'youtube' in source_name.lower():
            return 'video_transcript'
        elif 'gov' in source_name.lower() or 'gob' in source_name.lower():
            return 'government'
        elif 'edu' in source_name.lower() or 'unam' in source_name.lower():
            return 'academic'
        else:
            return 'general_web'
    
    def run_high_yield_session(self, max_duration: int = 7200, 
                              target_tokens: int = 10000000) -> Dict[str, Any]:
        """
        Run high-yield harvesting session with aggressive processing.
        
        Args:
            max_duration: Maximum session duration in seconds (default: 2 hours)
            target_tokens: Target token count for session
            
        Returns:
            Session results with enhanced metrics
        """
        session_start = time.time()
        
        self.logger.info(f"Starting high-yield session (target: {target_tokens:,} tokens)")
        
        # Initialize session tracking
        session_stats = {
            'session_start': datetime.now(),
            'target_tokens': target_tokens,
            'processed_urls': 0,
            'successful_extractions': 0,
            'total_tokens_collected': 0,
            'total_content_size': 0,
            'source_breakdown': {},
            'error_count': 0
        }
        
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            while True:
                # Check session timeout
                elapsed_time = time.time() - session_start
                if elapsed_time > max_duration:
                    self.logger.info(f"Session timeout reached ({max_duration}s)")
                    break
                
                # Check if target achieved
                if session_stats['total_tokens_collected'] >= target_tokens:
                    self.logger.info(f"Target tokens achieved: {session_stats['total_tokens_collected']:,}")
                    break
                
                # Get high-priority URLs for processing
                pending_urls = self.state_manager.get_priority_urls(
                    limit=self.num_threads * 2,  # Double batch size for efficiency
                    min_priority=0.5  # Focus on higher quality sources
                )
                
                if not pending_urls:
                    self.logger.info("No more high-priority URLs available")
                    break
                
                # Process batch with enhanced pipeline
                batch_results = self._process_enhanced_batch(executor, pending_urls)
                
                # Update session statistics
                for result in batch_results:
                    session_stats['processed_urls'] += 1
                    
                    if result['success']:
                        session_stats['successful_extractions'] += 1
                        session_stats['total_tokens_collected'] += result.get('token_count', 0)
                        session_stats['total_content_size'] += result.get('content_size', 0)
                        
                        # Track by source
                        source = result.get('source', 'unknown')
                        if source not in session_stats['source_breakdown']:
                            session_stats['source_breakdown'][source] = {
                                'processed': 0, 'successful': 0, 'tokens': 0
                            }
                        
                        session_stats['source_breakdown'][source]['successful'] += 1
                        session_stats['source_breakdown'][source]['tokens'] += result.get('token_count', 0)
                    else:
                        session_stats['error_count'] += 1
                    
                    # Update source breakdown
                    source = result.get('source', 'unknown')
                    if source not in session_stats['source_breakdown']:
                        session_stats['source_breakdown'][source] = {
                            'processed': 0, 'successful': 0, 'tokens': 0
                        }
                    session_stats['source_breakdown'][source]['processed'] += 1
                
                # Log progress
                progress_pct = (session_stats['total_tokens_collected'] / target_tokens) * 100
                self.logger.info(
                    f"Session progress: {session_stats['total_tokens_collected']:,}/{target_tokens:,} tokens "
                    f"({progress_pct:.1f}%) - {session_stats['processed_urls']} URLs processed"
                )
                
                # Brief pause between batches
                time.sleep(1)
        
        # Final session summary
        session_duration = time.time() - session_start
        session_stats.update({
            'session_duration': session_duration,
            'processing_rate': session_stats['processed_urls'] / session_duration if session_duration > 0 else 0,
            'token_rate': session_stats['total_tokens_collected'] / session_duration if session_duration > 0 else 0,
            'success_rate': (session_stats['successful_extractions'] / session_stats['processed_urls']) * 100 if session_stats['processed_urls'] > 0 else 0
        })
        
        self.logger.info(
            f"High-yield session complete: {session_stats['total_tokens_collected']:,} tokens collected "
            f"in {session_duration/60:.1f} minutes ({session_stats['success_rate']:.1f}% success rate)"
        )
        
        return session_stats
    
    def _process_enhanced_batch(self, executor: ThreadPoolExecutor, 
                               url_records: List[Dict]) -> List[Dict[str, Any]]:
        """Process a batch of URLs with enhanced pipeline."""
        batch_results = []
        
        # Submit all URLs for processing
        future_to_url = {
            executor.submit(self._process_enhanced_single_url, url_record): url_record
            for url_record in url_records
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_url, timeout=300):
            try:
                result = future.result(timeout=120)
                batch_results.append(result)
            except Exception as e:
                url_record = future_to_url[future]
                self.logger.error(f"URL processing failed: {url_record['url']} - {e}")
                batch_results.append({
                    'url': url_record['url'],
                    'source': url_record['source'],
                    'success': False,
                    'error': str(e)
                })
        
        return batch_results
    
    def _process_enhanced_single_url(self, url_record: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single URL through the enhanced pipeline."""
        url = url_record['url']
        source = url_record['source']
        url_hash = url_record['url_hash']
        
        processing_start = time.time()
        
        result = {
            'url': url,
            'url_hash': url_hash,
            'source': source,
            'success': False,
            'error': None
        }
        
        try:
            # Handle special URL types
            if url.startswith('reddit://'):
                # Process Reddit content from cache
                result = self._process_reddit_content(url, source)
            elif 'youtube.com' in url and hasattr(self, '_youtube_cache'):
                # Process YouTube content from cache
                result = self._process_youtube_content(url, source)
            else:
                # Process regular web content
                result = self._process_web_content(url, source, url_record)
            
            # Calculate processing time
            processing_time = (time.time() - processing_start) * 1000  # ms
            result['processing_time_ms'] = processing_time
            
            # Update state with enhanced result
            self.state_manager.update_enhanced_url_status(
                url_hash, 
                'completed' if result['success'] else 'failed',
                result
            )
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"Enhanced processing failed for {url}: {e}")
            
            # Update state as failed
            self.state_manager.update_enhanced_url_status(
                url_hash, 'failed', result
            )
        
        return result
    
    def _process_reddit_content(self, pseudo_url: str, source: str) -> Dict[str, Any]:
        """Process Reddit content from cache."""
        if not hasattr(self, '_reddit_cache') or pseudo_url not in self._reddit_cache:
            return {'success': False, 'error': 'Reddit content not found in cache'}
        
        content_item = self._reddit_cache[pseudo_url]
        
        # Save Reddit content directly
        save_result = self.saver.save_enhanced_content(
            content=content_item['text'],
            source_name=source,
            url=pseudo_url,
            metadata={
                'type': content_item['type'],
                'subreddit': content_item.get('subreddit'),
                'score': content_item.get('score'),
                'created_utc': content_item.get('created_utc').isoformat() if content_item.get('created_utc') else None,
                'mexican_score': content_item.get('mexican_score', 0),
                'extraction_method': 'reddit_api'
            }
        )
        
        if save_result['saved']:
            return {
                'success': True,
                'file_path': save_result['file_path'],
                'token_count': save_result['token_count'],
                'content_size': len(content_item['text']),
                'mexican_score': content_item.get('mexican_score', 0),
                'extraction_method': 'reddit_api'
            }
        else:
            return {'success': False, 'error': save_result.get('error', 'Save failed')}
    
    def _process_youtube_content(self, url: str, source: str) -> Dict[str, Any]:
        """Process YouTube content from cache."""
        if not hasattr(self, '_youtube_cache') or url not in self._youtube_cache:
            return {'success': False, 'error': 'YouTube content not found in cache'}
        
        content_item = self._youtube_cache[url]
        
        # Save YouTube transcript
        save_result = self.saver.save_enhanced_content(
            content=content_item['text'],
            source_name=source,
            url=url,
            metadata={
                'type': content_item['type'],
                'channel': content_item.get('channel'),
                'video_id': content_item.get('video_id'),
                'published_at': content_item.get('published_at').isoformat() if content_item.get('published_at') else None,
                'transcript_length': content_item.get('transcript_length'),
                'mexican_score': content_item.get('mexican_score', 0),
                'extraction_method': 'youtube_transcript'
            }
        )
        
        if save_result['saved']:
            return {
                'success': True,
                'file_path': save_result['file_path'],
                'token_count': save_result['token_count'],
                'content_size': len(content_item['text']),
                'mexican_score': content_item.get('mexican_score', 0),
                'extraction_method': 'youtube_transcript'
            }
        else:
            return {'success': False, 'error': save_result.get('error', 'Save failed')}
    
    def _process_web_content(self, url: str, source: str, url_record: Dict) -> Dict[str, Any]:
        """Process regular web content through enhanced pipeline."""
        # Get source configuration
        source_config = self._get_source_config(source)
        
        # Determine if browser automation is needed
        use_browser = source_config.get('render_js', False)
        
        # Fetch content
        if use_browser:
            if not self.dynamic_scraper:
                self.dynamic_scraper = DynamicScraperSync(self.config_manager.get_politeness_config())
            
            with self.dynamic_scraper as ds:
                html_content = ds.fetch(url)
        else:
            response = self.scraper.fetch(url, source_config)
            html_content = response.text
        
        # Enhanced extraction with comments and link discovery
        extraction_result = self.extractor.extract_with_comments(
            html_content, source_config, url
        )
        
        if not extraction_result['success']:
            return {
                'success': False,
                'error': extraction_result.get('error', 'Extraction failed')
            }
        
        # Save with enhanced features
        save_result = self.saver.save_enhanced_content(
            content=extraction_result['text'],
            source_name=source,
            url=url,
            html_content=html_content,
            metadata=extraction_result.get('metadata', {})
        )
        
        # Process discovered links if dynamic recursion is enabled
        if (source_config.get('dynamic_recursion', False) and 
            extraction_result.get('discovered_links')):
            self._queue_discovered_links(
                extraction_result['discovered_links'], 
                source, 
                url
            )
        
        if save_result['saved']:
            return {
                'success': True,
                'file_path': save_result['file_path'],
                'snapshot_path': save_result.get('snapshot_path'),
                'token_count': save_result['token_count'],
                'content_size': len(extraction_result['text']),
                'mexican_score': extraction_result.get('metadata', {}).get('mexican_score', 0),
                'extraction_method': extraction_result.get('extraction_method', 'enhanced'),
                'comments_count': extraction_result.get('comments_count', 0),
                'discovered_links': len(extraction_result.get('discovered_links', []))
            }
        else:
            return {
                'success': False,
                'error': save_result.get('error', 'Save failed'),
                'duplicate': save_result.get('duplicate', False)
            }
    
    def _get_source_config(self, source_name: str) -> Dict[str, Any]:
        """Get source configuration by name."""
        sources = self.config_manager.get_sources()
        for source in sources:
            if source['name'] == source_name:
                return source
        return {}  # Default empty config
    
    def _queue_discovered_links(self, discovered_links: List[Dict], 
                               parent_source: str, parent_url: str):
        """Queue discovered links for future processing."""
        high_value_links = [
            link for link in discovered_links 
            if link['relevance_score'] > 3.0
        ]
        
        if high_value_links:
            urls = [link['url'] for link in high_value_links]
            discovery_metadata = {
                'method': 'dynamic_recursion',
                'parent_url': parent_url,
                'discovery_count': len(urls)
            }
            
            added = self.state_manager.add_enhanced_urls(
                urls, f"{parent_source}_discovered", discovery_metadata
            )
            
            if added > 0:
                self.logger.info(f"Queued {added} high-value discovered links from {parent_url}")
    
    def run_complete_harvest(self) -> Dict[str, Any]:
        """
        Run complete high-yield harvesting workflow.
        
        Returns:
            Complete workflow results with enhanced metrics
        """
        workflow_start = time.time()
        
        self.logger.info("Starting complete high-yield harvest workflow")
        
        try:
            # Phase 1: Comprehensive content discovery
            self.logger.info("Phase 1: Comprehensive Content Discovery")
            discovered_urls = self.discover_all_content()
            
            # Phase 2: Enhanced state population
            self.logger.info("Phase 2: Enhanced State Population")
            new_urls = self.populate_enhanced_state(discovered_urls)
            
            # Phase 3: High-yield processing session
            self.logger.info("Phase 3: High-Yield Processing Session")
            session_results = self.run_high_yield_session(
                max_duration=7200,  # 2 hours
                target_tokens=50000000  # 50M tokens per session
            )
            
            total_duration = time.time() - workflow_start
            
            # Generate comprehensive report
            final_stats = self.state_manager.get_enhanced_progress_stats()
            corpus_stats = self.saver.get_enhanced_corpus_stats()
            
            return {
                'workflow_duration_seconds': total_duration,
                'urls_discovered': sum(len(urls) for urls in discovered_urls.values()),
                'new_urls_added': new_urls,
                'session_results': session_results,
                'final_progress_stats': final_stats,
                'corpus_stats': corpus_stats,
                'success': True
            }
            
        except Exception as e:
            self.logger.error(f"Complete harvest workflow failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'workflow_duration_seconds': time.time() - workflow_start
            }
    
    def get_harvest_status(self) -> Dict[str, Any]:
        """Get current harvest status with enhanced metrics."""
        return {
            'session_id': self.session_id,
            'session_duration': (datetime.now() - self.session_start).total_seconds(),
            'progress_stats': self.state_manager.get_enhanced_progress_stats(),
            'corpus_stats': self.saver.get_enhanced_corpus_stats(),
            'performance_metrics': self.metrics
        }
    
    def cleanup(self):
        """Clean up all resources."""
        try:
            if self.dynamic_scraper:
                # DynamicScraperSync cleanup is handled by context manager
                pass
            
            self.scraper.close()
            self.state_manager.close()
            
            self.logger.info("High-yield orchestrator cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Cleanup error: {e}")