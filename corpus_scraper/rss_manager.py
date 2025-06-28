"""
RSS Feed Manager for real-time Mexican content discovery.
Handles RSS feeds, news feeds, and content aggregation from Mexican sources.
"""

import logging
import time
import os
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import feedparser
import requests
from urllib.parse import urljoin, urlparse
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from .exceptions import NetworkError
from dotenv import load_dotenv

load_dotenv()


class RSSManager:
    """
    RSS Feed Manager for discovering fresh Mexican Spanish content.
    Integrates with major Mexican news outlets and content sources.
    """
    
    def __init__(self, politeness_config: Dict):
        self.politeness_config = politeness_config
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)
        self.last_request_times = {}
        
        # Discord webhook for notifications
        self.discord_webhook = os.getenv("DISCORD_CHANNEL_WEBHOOK")
        
        # Track sent URLs to prevent duplicates
        self.sent_urls = set()
        
        # Mexican RSS feeds for real-time content discovery
        self.mexican_feeds = {
            # Major Mexican news outlets
            'el_universal': 'https://www.eluniversal.com.mx/rss.xml',
            'la_jornada': 'https://www.jornada.com.mx/rss/',
            'milenio': 'https://www.milenio.com/rss',
            'animal_politico': 'https://www.animalpolitico.com/feed/',
            'proceso': 'https://www.proceso.com.mx/rss/',
            'excelsior': 'https://www.excelsior.com.mx/rss.xml',
            'sin_embargo': 'https://www.sinembargo.mx/feed/',
            'nexos': 'https://www.nexos.com.mx/rss.xml',
            'letras_libres': 'https://www.letraslibres.com/rss.xml',
            'el_deforma': 'https://www.eldeforma.com/feed/',
            'el_norte': 'https://www.elnorte.com/rss/portada.xml',
            'el_siglo_torreon': 'https://www.elsiglodetorreon.com.mx/index.xml',
            'grupo_metropoli': 'https://grupometropoli.net/feed/',
            '8_columnas': 'https://8columnas.com.mx/feed/',
            'diario_yucatan': 'https://www.yucatan.com.mx/feed',
            'vanguardia': 'https://vanguardia.com.mx/rss.xml',
            
            # Heraldo de México feeds
            'heraldo_nacional': 'https://heraldodemexico.com.mx/rss/feed.html?r=4',
            'heraldo_mundo': 'https://heraldodemexico.com.mx/rss/feed.html?r=5',
            'heraldo_economia': 'https://heraldodemexico.com.mx/rss/feed.html?r=6',
            'heraldo_deportes': 'https://heraldodemexico.com.mx/rss/feed.html?r=7',
            'heraldo_espectaculos': 'https://heraldodemexico.com.mx/rss/feed.html?r=1',
            'heraldo_tendencias': 'https://heraldodemexico.com.mx/rss/feed.html?r=8',
            'heraldo_estilo_vida': 'https://heraldodemexico.com.mx/rss/feed.html?r=9',
            'heraldo_cultura': 'https://heraldodemexico.com.mx/rss/feed.html?r=10',
            'heraldo_opinion': 'https://heraldodemexico.com.mx/rss/feed.html?r=11',
            'heraldo_tecnologia': 'https://heraldodemexico.com.mx/rss/feed.html?r=12',
            'heraldo_podcast': 'https://heraldodemexico.com.mx/rss/feed.html?r=2718',
            'heraldo_clases': 'https://heraldodemexico.com.mx/rss/feed.html?r=2',
            'heraldo_usa': 'https://heraldodemexico.com.mx/rss/feed.html?r=3',
            'heraldo_elecciones': 'https://heraldodemexico.com.mx/rss/feed.html?r=2698',
            'heraldo_derecho': 'https://heraldodemexico.com.mx/rss/feed.html?r=2716',
            'heraldo_tv': 'https://heraldodemexico.com.mx/rss/feed.html?r=2677',
            'heraldo_gastrolab': 'https://heraldodemexico.com.mx/rss/feed.html?r=2680',
            'heraldo_suplementos': 'https://heraldodemexico.com.mx/rss/feed.html?r=2683',
            'heraldo_radio': 'https://heraldodemexico.com.mx/rss/feed.html?r=13',
            'heraldo_aguascalientes': 'https://heraldodemexico.com.mx/rss/feed.html?r=2700',
            
            # Government and academic feeds
            'conacyt': 'https://www.conacyt.gob.mx/index.php/rss',
            'unam_gaceta': 'https://www.gaceta.unam.mx/feed/',
        }
        
        self._setup_session()
    
    def _send_discord_url(self, url: str, source_name: str, title: str = None):
        """Send individual URL to Discord for preview and avoid duplicates."""
        if not self.discord_webhook or url in self.sent_urls:
            return
            
        try:
            # Add URL to sent set to prevent duplicates
            self.sent_urls.add(url)
            
            # Create payload with just the URL for Discord preview
            content = f"📰 **{source_name}**"
            if title:
                content += f" - {title}"
            content += f"\n{url}"
            
            payload = {
                "content": content
            }
            
            response = requests.post(
                self.discord_webhook,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            
            # Small delay to avoid rate limiting
            time.sleep(0.5)
            
        except Exception as e:
            self.logger.warning(f"Failed to send Discord URL notification: {e}")
            # Remove from sent set if failed to send
            self.sent_urls.discard(url)
    
    def _setup_session(self):
        """Configure the requests session with realistic headers."""
        self.session.headers.update({
            'User-Agent': 'Mexican Spanish Corpus RSS Reader/1.0 (Academic Research)',
            'Accept': 'application/rss+xml, application/xml, text/xml, application/atom+xml, */*',
            'Accept-Language': 'es-MX,es;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        })
        
        timeout = self.politeness_config.get('timeout', 30)
        self.session.timeout = timeout
        
        # Configure SSL handling
        ssl_verify = self.politeness_config.get('ssl_verify', True)
        if not ssl_verify:
            self.session.verify = False
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.logger.info("RSS Manager: SSL certificate verification disabled")
    
    def _enforce_rate_limit(self, domain: str):
        """Enforce politeness delay between requests to the same domain."""
        current_time = time.time()
        
        if domain in self.last_request_times:
            time_since_last = current_time - self.last_request_times[domain]
            base_delay = self.politeness_config.get('request_delay', 2.0)
            
            if time_since_last < base_delay:
                sleep_time = base_delay - time_since_last
                self.logger.debug(f"RSS rate limiting: sleeping {sleep_time:.2f}s for {domain}")
                time.sleep(sleep_time)
        
        self.last_request_times[domain] = time.time()
    
    def fetch_feed(self, feed_url: str, source_name: str) -> List[Dict[str, Any]]:
        """
        Fetch and parse an RSS feed.
        
        Args:
            feed_url: URL of the RSS feed
            source_name: Name of the source for logging
            
        Returns:
            List of feed entries with metadata
        """
        try:
            domain = urlparse(feed_url).netloc
            self._enforce_rate_limit(domain)
            
            self.logger.info(f"Fetching RSS feed: {source_name} ({feed_url})")
            
            # Configure feedparser with session settings
            feedparser.USER_AGENT = self.session.headers.get('User-Agent', 'feedparser')
            
            # Try to fetch manually first to handle SSL issues
            try:
                response = self.session.get(feed_url, verify=self.session.verify)
                response.raise_for_status()
                
                # Clean up content to fix common XML issues
                content = response.content
                if content:
                    # Try to fix common encoding issues
                    content_str = content.decode('utf-8', errors='ignore')
                    # Remove invalid XML characters
                    import re
                    content_str = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content_str)
                    content = content_str.encode('utf-8')
                
                feed = feedparser.parse(content)
            except Exception as manual_error:
                self.logger.warning(f"Manual fetch failed for {source_name}: {manual_error}")
                # Fall back to feedparser's internal fetching
                try:
                    # Configure feedparser to use custom agent settings
                    feed = feedparser.parse(feed_url, agent=self.session.headers.get('User-Agent'))
                except Exception as fallback_error:
                    self.logger.error(f"Both manual and feedparser methods failed for {source_name}: {fallback_error}")
                    return []
            
            if feed.bozo:
                self.logger.warning(f"Feed parsing issues for {source_name}: {feed.bozo_exception}")
                # Don't return empty if bozo - many feeds work despite warnings
            
            if not hasattr(feed, 'entries') or not feed.entries:
                self.logger.warning(f"No entries found in feed {source_name}")
                # Check if feed object has status attribute for more debugging
                if hasattr(feed, 'status'):
                    self.logger.warning(f"Feed status for {source_name}: {feed.status}")
                if hasattr(feed, 'feed') and hasattr(feed.feed, 'title'):
                    self.logger.info(f"Feed title: {feed.feed.title}")
                return []
            
            entries = []
            for entry in feed.entries:
                processed_entry = self._process_feed_entry(entry, source_name, feed_url)
                if processed_entry:
                    entries.append(processed_entry)
            
            self.logger.info(f"Successfully parsed {len(entries)} entries from {source_name}")
            return entries
            
        except Exception as e:
            self.logger.error(f"Failed to fetch RSS feed {source_name}: {e}")
            return []
    
    def _process_feed_entry(self, entry: Any, source_name: str, feed_url: str) -> Optional[Dict[str, Any]]:
        """Process a single RSS feed entry."""
        try:
            # Extract basic information
            title = getattr(entry, 'title', 'Sin título')
            link = getattr(entry, 'link', '')
            description = getattr(entry, 'description', '') or getattr(entry, 'summary', '')
            
            # Handle publication date
            pub_date = None
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                pub_date = datetime(*entry.published_parsed[:6])
            elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                pub_date = datetime(*entry.updated_parsed[:6])
            
            # Skip entries without links
            if not link:
                return None
            
            # Extract categories/tags if available
            categories = []
            if hasattr(entry, 'tags'):
                categories = [tag.term for tag in entry.tags if hasattr(tag, 'term')]
            elif hasattr(entry, 'category'):
                categories = [entry.category]
            
            return {
                'url': link,
                'title': title,
                'description': description,
                'published_date': pub_date,
                'categories': categories,
                'source': source_name,
                'feed_url': feed_url,
                'entry_id': getattr(entry, 'id', link),
                'discovered_at': datetime.now()
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to process RSS entry from {source_name}: {e}")
            return None
    
    def discover_fresh_content(self, hours_back: int = 24) -> Dict[str, List[Dict[str, Any]]]:
        """
        Discover fresh content from all configured Mexican RSS feeds.
        
        Args:
            hours_back: How many hours back to look for content
            
        Returns:
            Dictionary mapping source names to lists of fresh entries
        """
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        fresh_content = {}
        
        self.logger.info(f"Discovering fresh Mexican content from last {hours_back} hours")
        
        for source_name, feed_url in self.mexican_feeds.items():
            try:
                entries = self.fetch_feed(feed_url, source_name)
                
                # Filter for fresh content
                fresh_entries = []
                for entry in entries:
                    pub_date = entry.get('published_date')
                    if pub_date and pub_date >= cutoff_time:
                        fresh_entries.append(entry)
                    elif not pub_date:
                        # Include entries without dates (might be fresh)
                        fresh_entries.append(entry)
                
                if fresh_entries:
                    fresh_content[source_name] = fresh_entries
                    self.logger.info(f"Found {len(fresh_entries)} fresh entries from {source_name}")
                    # Send individual Discord notifications for each URL
                    for entry in fresh_entries:
                        self._send_discord_url(entry['url'], source_name, entry.get('title'))
                
                # Brief pause between feeds
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"Error processing feed {source_name}: {e}")
                continue
        
        total_fresh = sum(len(entries) for entries in fresh_content.values())
        self.logger.info(f"Discovery complete: {total_fresh} fresh articles from {len(fresh_content)} sources")
        
        return fresh_content
    
    def get_priority_mexican_content(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get high-priority Mexican content based on categories and keywords.
        
        Returns:
            Dictionary of prioritized content by category
        """
        mexican_keywords = {
            'politica': ['méxico', 'mexicano', 'amlo', 'sheinbaum', 'morena', 'congreso', 'senado', 'diputados'],
            'cultura': ['cultura mexicana', 'literatura', 'arte', 'tradición', 'patrimonio'],
            'sociedad': ['sociedad mexicana', 'comunidad', 'familia', 'educación'],
            'economia': ['economía mexicana', 'peso', 'pemex', 'banxico'],
            'judicial': ['suprema corte', 'scjn', 'tribunal', 'justicia'],
        }
        
        fresh_content = self.discover_fresh_content(hours_back=48)
        prioritized_content = {category: [] for category in mexican_keywords.keys()}
        
        for source_name, entries in fresh_content.items():
            for entry in entries:
                title_lower = entry['title'].lower()
                desc_lower = entry['description'].lower()
                text_content = f"{title_lower} {desc_lower}"
                
                # Categorize based on keywords
                for category, keywords in mexican_keywords.items():
                    if any(keyword in text_content for keyword in keywords):
                        prioritized_content[category].append(entry)
                        break
        
        return prioritized_content
    
    def export_urls_for_scraping(self, fresh_content: Dict[str, List[Dict[str, Any]]]) -> List[str]:
        """
        Extract URLs from fresh content for the main scraping pipeline.
        
        Args:
            fresh_content: Fresh content discovered from RSS feeds
            
        Returns:
            List of URLs ready for scraping
        """
        urls = []
        for source_name, entries in fresh_content.items():
            for entry in entries:
                url = entry.get('url')
                if url and url not in urls:
                    urls.append(url)
        
        self.logger.info(f"Exported {len(urls)} unique URLs for scraping")
        return urls
    
    def close(self):
        """Clean up resources."""
        self.session.close()