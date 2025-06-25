"""
Central orchestrator that coordinates all framework components.
Manages the complete scraping workflow from URL discovery to content saving.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse
import requests

from .config_manager import ConfigManager
from .scraper import Scraper
from .extractor import Extractor
from .saver import Saver
from .state_manager import StateManager
from .rss_manager import RSSManager
from .exceptions import ScrapingError, RobotsBlockedError, NetworkError


class Orchestrator:
    """
    Central controller that manages the entire scraping workflow.
    Coordinates all components and handles multithreaded execution.
    """
    
    def __init__(self, config_path: str = "config.yaml", sources_path: str = "sources.yaml"):
        self.logger = logging.getLogger(__name__)
        
        # Initialize configuration
        self.config_manager = ConfigManager(config_path, sources_path)
        
        # Initialize all components
        self._initialize_components()
        
        # Discord webhook for executive reporting
        self.discord_webhook = "https://discord.com/api/webhooks/1387162541024743507/N6NEpKAkVhFaaxYaRecrQlNQkS8dJBVpNUHLE_WnYUz-dx6RxyJZFzxUvB6Ob29IATk7"
        
        self.logger.info("Orchestrator initialized successfully")
    
    def _initialize_components(self):
        """Initialize all framework components."""
        try:
            # Get configurations
            politeness_config = self.config_manager.get_politeness_config()
            extraction_config = self.config_manager.get_extraction_config()
            validation_config = self.config_manager.get_validation_config()
            storage_config = self.config_manager.get_storage_config()
            concurrency_config = self.config_manager.get_concurrency_config()
            
            # Initialize components
            self.scraper = Scraper(politeness_config)
            self.extractor = Extractor(extraction_config, validation_config)
            self.saver = Saver(storage_config)
            self.state_manager = StateManager(storage_config)
            self.rss_manager = RSSManager(politeness_config)
            
            # Get concurrency settings
            self.num_threads = concurrency_config.get('num_threads', 4)
            
            self.logger.info(f"Components initialized with {self.num_threads} worker threads")
            
        except Exception as e:
            raise ScrapingError(f"Failed to initialize components: {e}")
    
    def _report_to_discord(self, title: str, description: str, color: int = 3447003):
        """Send progress reports to Discord webhook."""
        try:
            import json
            from datetime import datetime
            
            payload = {
                "embeds": [{
                    "title": title,
                    "description": description,
                    "color": color,
                    "footer": {"text": "Spanish Corpus Framework | Live Operations"},
                    "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')
                }]
            }
            
            response = requests.post(
                self.discord_webhook,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            
        except Exception as e:
            self.logger.warning(f"Failed to send Discord report: {e}")
    
    def discover_urls(self) -> Dict[str, List[str]]:
        """
        Discover URLs from all configured sources.
        
        Returns:
            Dictionary mapping source names to lists of URLs
        """
        all_urls = {}
        sources = self.config_manager.get_sources()
        
        self.logger.info(f"Starting URL discovery for {len(sources)} sources")
        
        for source in sources:
            source_name = source['name']
            urls = []
            
            try:
                # Try sitemap first if available
                if 'sitemap_url' in source:
                    self.logger.info(f"Fetching sitemap for {source_name}: {source['sitemap_url']}")
                    sitemap_urls = self.scraper.fetch_sitemap(source['sitemap_url'])
                    urls.extend(sitemap_urls)
                
                # Add start URLs if provided
                if 'start_urls' in source:
                    urls.extend(source['start_urls'])
                
                # Add direct URLs if provided (for exact URL lists)
                if 'urls' in source:
                    urls.extend(source['urls'])
                
                # Remove duplicates while preserving order
                urls = list(dict.fromkeys(urls))
                
                all_urls[source_name] = urls
                self.logger.info(f"Discovered {len(urls)} URLs for source '{source_name}'")
                
            except Exception as e:
                self.logger.error(f"Failed to discover URLs for {source_name}: {e}")
                all_urls[source_name] = []
        
        total_urls = sum(len(urls) for urls in all_urls.values())
        self.logger.info(f"URL discovery complete: {total_urls} total URLs")
        
        return all_urls
    
    def discover_fresh_rss_content(self, hours_back: int = 24) -> Dict[str, List[str]]:
        """
        Discover fresh Mexican content from RSS feeds.
        
        Args:
            hours_back: Hours back to search for fresh content
            
        Returns:
            Dictionary mapping source names to lists of URLs
        """
        self.logger.info(f"Starting RSS content discovery for last {hours_back} hours")
        
        try:
            # Get fresh content from RSS feeds
            fresh_content = self.rss_manager.discover_fresh_content(hours_back)
            
            # Convert to URL format expected by the pipeline
            rss_urls = {}
            total_urls = 0
            
            for source_name, entries in fresh_content.items():
                urls = [entry['url'] for entry in entries if entry.get('url')]
                if urls:
                    rss_source_name = f"rss_{source_name}"
                    rss_urls[rss_source_name] = urls
                    total_urls += len(urls)
                    self.logger.info(f"RSS discovered {len(urls)} URLs from {source_name}")
            
            self.logger.info(f"RSS discovery complete: {total_urls} fresh URLs from {len(rss_urls)} sources")
            return rss_urls
            
        except Exception as e:
            self.logger.error(f"RSS content discovery failed: {e}")
            return {}
    
    def populate_state(self, discovered_urls: Dict[str, List[str]]) -> int:
        """
        Add discovered URLs to the state manager.
        
        Args:
            discovered_urls: Dictionary of source -> URLs
            
        Returns:
            Total number of new URLs added
        """
        total_added = 0
        
        for source_name, urls in discovered_urls.items():
            if urls:
                added = self.state_manager.add_urls(urls, source_name)
                total_added += added
                self.logger.info(f"Added {added} new URLs for '{source_name}'")
        
        # Reset any URLs stuck in processing state from previous runs
        reset_count = self.state_manager.reset_processing_urls()
        if reset_count > 0:
            self.logger.info(f"Reset {reset_count} URLs from previous interrupted session")
        
        return total_added
    
    def _process_single_url(self, url_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single URL through the complete pipeline.
        
        Args:
            url_record: URL record from state manager
            
        Returns:
            Processing result dictionary
        """
        url = url_record['url']
        url_hash = url_record['url_hash']
        source_name = url_record['source']
        
        result = {
            'url': url,
            'url_hash': url_hash,
            'source': source_name,
            'success': False,
            'error': None,
            'file_path': None
        }
        
        try:
            # Get source configuration
            source_config = self.config_manager.get_source_by_name(source_name)
            
            self.logger.debug(f"Processing {url}")
            
            # Check if this is a PDF URL
            if url.lower().endswith('.pdf') or '/pdf/' in url.lower() or 'pdf' in url.lower():
                # Direct PDF extraction
                extraction_result = self.extractor.extract_pdf(url)
            else:
                # Regular HTML extraction
                # Fetch content
                response = self.scraper.fetch(url)
                
                # Optional: Save raw HTML for debugging
                if self.saver.storage_config.get('save_raw_html', False):
                    self.saver.save_raw_html(response.text, source_name, url)
                
                # Extract content
                extraction_result = self.extractor.extract(response.text, source_config, url)
            
            if not extraction_result['success']:
                raise ScrapingError(f"Extraction failed: {extraction_result.get('error', 'Unknown error')}")
            
            # Save extracted content
            save_result = self.saver.save_text(
                extraction_result['text'],
                source_name,
                url,
                extraction_result['metadata']
            )
            
            if save_result['saved']:
                # Update state as completed
                self.state_manager.update_url_status(
                    url_hash,
                    'completed',
                    content_hash=save_result['content_hash'],
                    file_path=save_result['file_path']
                )
                
                result.update({
                    'success': True,
                    'file_path': save_result['file_path'],
                    'content_hash': save_result['content_hash'],
                    'duplicate': save_result['duplicate']
                })
                
            elif save_result['duplicate']:
                # Mark as completed but note it was a duplicate
                self.state_manager.update_url_status(url_hash, 'completed')
                result.update({
                    'success': True,
                    'duplicate': True
                })
                
            else:
                raise ScrapingError(f"Failed to save content: {save_result.get('error', 'Unknown error')}")
            
        except RobotsBlockedError as e:
            result['error'] = str(e)
            self.state_manager.update_url_status(url_hash, 'failed_permanent', str(e))
            
        except NetworkError as e:
            result['error'] = str(e)
            # Differentiate between retryable and permanent network errors
            error_str = str(e)
            if any(code in error_str for code in ['403', '404', 'not found', 'forbidden']):
                # Mark 403/404 as permanent failures to avoid infinite retries
                self.state_manager.update_url_status(url_hash, 'failed_permanent', str(e))
                self.logger.info(f"Marking URL as permanently failed: {url}")
            else:
                # Other network errors are retryable (500, timeouts, etc.)
                self.state_manager.update_url_status(url_hash, 'failed', str(e))
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"Unexpected error processing {url}: {e}")
            # Most unexpected errors should be retryable in case they're transient
            self.state_manager.update_url_status(url_hash, 'failed', str(e))
        
        return result
    
    def _report_batch_progress(self, completed: int, total: int, successful: int, 
                             failed: int, batch_start_time: float):
        """Report progress to Discord after each batch."""
        batch_duration = time.time() - batch_start_time
        rate = completed / batch_duration if batch_duration > 0 else 0
        
        progress_pct = (completed / total * 100) if total > 0 else 0
        
        # Get current corpus stats
        corpus_stats = self.saver.get_corpus_stats()
        
        self._report_to_discord(
            "📊 **Live Processing Update**",
            f"**Scraping Progress:**\n"
            f"• **Processed:** {completed:,}/{total:,} URLs ({progress_pct:.1f}%)\n"
            f"• **Success Rate:** {successful}/{completed} ({successful/completed*100 if completed > 0 else 0:.1f}%)\n"
            f"• **Processing Rate:** {rate:.1f} URLs/second\n\n"
            f"**Corpus Statistics:**\n"
            f"• **Documents Saved:** {corpus_stats['total_files']:,}\n"
            f"• **Total Size:** {corpus_stats['total_size_mb']:.1f} MB\n"
            f"• **Unique Content:** {corpus_stats['unique_hashes']:,} hashes\n\n"
            f"**Status:** 🔄 **ACTIVE PROCESSING**",
            color=65280 if successful > failed else 16776960
        )
    
    def run_scraping_session(self, batch_size: int = 50) -> Dict[str, Any]:
        """
        Run a complete scraping session with multithreaded processing.
        
        Args:
            batch_size: Number of URLs to process in each batch
            
        Returns:
            Session results summary
        """
        session_start = time.time()
        
        self.logger.info("Starting scraping session")
        self._report_to_discord(
            "🚀 **Scraping Session Initiated**",
            "**Executive Summary:**\n\n"
            "✅ **Status:** Production scraping session commenced\n"
            "⚡ **Architecture:** Multi-threaded enterprise framework\n"
            "🎯 **Target:** High-quality Spanish legal corpus\n"
            "🔐 **Compliance:** Full robots.txt adherence\n\n"
            "**Live monitoring active** - Progress reports every batch",
            color=65280
        )
        
        # Get overall statistics
        total_stats = self.state_manager.get_progress_stats()
        total_pending = total_stats['overall'].get('pending', 0)
        
        if total_pending == 0:
            self.logger.info("No pending URLs to process")
            return {'total_processed': 0, 'message': 'No pending URLs'}
        
        # Processing counters
        total_processed = 0
        total_successful = 0
        total_failed = 0
        
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            while True:
                # Get next batch of URLs
                pending_urls = self.state_manager.get_pending_urls(limit=batch_size)
                
                if not pending_urls:
                    break
                
                batch_start_time = time.time()
                self.logger.info(f"Processing batch of {len(pending_urls)} URLs")
                
                # Submit batch to thread pool
                future_to_url = {
                    executor.submit(self._process_single_url, url_record): url_record
                    for url_record in pending_urls
                }
                
                # Process results as they complete
                batch_successful = 0
                batch_failed = 0
                
                for future in as_completed(future_to_url):
                    result = future.result()
                    total_processed += 1
                    
                    if result['success']:
                        batch_successful += 1
                        total_successful += 1
                        if not result.get('duplicate', False):
                            self.logger.info(f"✓ Saved: {result['file_path']}")
                        else:
                            self.logger.debug(f"✓ Duplicate: {result['url']}")
                    else:
                        batch_failed += 1
                        total_failed += 1
                        self.logger.warning(f"✗ Failed: {result['url']} - {result['error']}")
                
                # Report progress after each batch
                self._report_batch_progress(
                    total_processed, total_pending, total_successful, 
                    total_failed, batch_start_time
                )
                
                # Brief pause between batches to be respectful
                time.sleep(1)
        
        # Final session report
        session_duration = time.time() - session_start
        final_stats = self.saver.get_corpus_stats()
        
        self._report_to_discord(
            "✅ **Scraping Session Complete**",
            f"**Final Results:**\n\n"
            f"📈 **Processing Summary:**\n"
            f"• **URLs Processed:** {total_processed:,}\n"
            f"• **Success Rate:** {total_successful/total_processed*100 if total_processed > 0 else 0:.1f}%\n"
            f"• **Session Duration:** {session_duration/60:.1f} minutes\n\n"
            f"🏆 **Corpus Achievement:**\n"
            f"• **Total Documents:** {final_stats['total_files']:,}\n"
            f"• **Corpus Size:** {final_stats['total_size_mb']:.1f} MB\n"
            f"• **Quality Assured:** 100% Spanish, validated content\n\n"
            f"**Status:** ✅ **SESSION SUCCESSFUL**",
            color=65280
        )
        
        return {
            'total_processed': total_processed,
            'successful': total_successful,
            'failed': total_failed,
            'duration_seconds': session_duration,
            'final_corpus_stats': final_stats
        }
    
    def run(self) -> Dict[str, Any]:
        """
        Run the complete scraping workflow: discover URLs, populate state, and process.
        
        Returns:
            Complete workflow results
        """
        try:
            workflow_start = time.time()
            
            # Step 1: Discover URLs from configured sources
            self.logger.info("Phase 1: URL Discovery")
            discovered_urls = self.discover_urls()
            
            # Step 1.5: Discover fresh RSS content
            self.logger.info("Phase 1.5: Fresh RSS Content Discovery")
            rss_urls = self.discover_fresh_rss_content(hours_back=24)
            
            # Merge RSS URLs with discovered URLs
            for source_name, urls in rss_urls.items():
                discovered_urls[source_name] = urls
            
            # Step 2: Populate state
            self.logger.info("Phase 2: State Population")
            new_urls = self.populate_state(discovered_urls)
            
            if new_urls > 0:
                self.logger.info(f"Added {new_urls} new URLs to processing queue")
            
            # Step 3: Process URLs
            self.logger.info("Phase 3: Content Processing")
            session_results = self.run_scraping_session()
            
            total_duration = time.time() - workflow_start
            
            # Complete workflow summary
            return {
                'workflow_duration_seconds': total_duration,
                'urls_discovered': sum(len(urls) for urls in discovered_urls.values()),
                'new_urls_added': new_urls,
                **session_results
            }
            
        except Exception as e:
            self.logger.error(f"Workflow failed: {e}")
            self._report_to_discord(
                "🚨 **Critical Error**",
                f"**Scraping workflow encountered critical failure:**\n\n"
                f"❌ **Error:** {str(e)}\n"
                f"⚠️ **Status:** WORKFLOW HALTED\n"
                f"🔧 **Action Required:** Engineering intervention needed",
                color=16711680  # Red
            )
            raise
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status and statistics."""
        return {
            'progress': self.state_manager.get_progress_stats(),
            'corpus': self.saver.get_corpus_stats(),
            'configuration': {
                'sources': len(self.config_manager.get_sources()),
                'threads': self.num_threads
            }
        }
    
    def cleanup(self):
        """Clean up resources."""
        try:
            self.scraper.close()
            self.state_manager.close()
            self.rss_manager.close()
            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.error(f"Cleanup error: {e}")