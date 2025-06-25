"""
Advanced web scraping component with sophisticated politeness protocols.
Handles HTTP requests, robots.txt compliance, and rate limiting.
"""

import time
import random
import logging
import requests
from urllib.parse import urlparse, urljoin
from typing import Dict, Optional, List
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from protego import Protego
from fake_useragent import UserAgent
from .exceptions import RobotsBlockedError, NetworkError


class Scraper:
    """
    Network-facing component responsible for all HTTP/S interactions.
    Implements advanced politeness protocols and resilient request handling.
    """
    
    def __init__(self, politeness_config: Dict):
        self.politeness_config = politeness_config
        self.session = requests.Session()
        self.robots_parsers = {}
        self.last_request_times = {}
        self.user_agent = UserAgent()
        self.logger = logging.getLogger(__name__)
        
        # Configure session with realistic headers
        self._setup_session()
    
    def _setup_session(self):
        """Configure the requests session with realistic browser headers."""
        # Set up realistic browser headers
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Set timeout from config
        timeout = self.politeness_config.get('timeout', 30)
        self.session.timeout = timeout
    
    def _get_robot_parser(self, url: str) -> Protego:
        """Get or create a robots.txt parser for the given URL's domain."""
        domain = urlparse(url).netloc
        
        if domain not in self.robots_parsers:
            robots_url = f"https://{domain}/robots.txt"
            try:
                self.logger.info(f"Fetching robots.txt from {robots_url}")
                response = self.session.get(robots_url, timeout=10)
                response.raise_for_status()
                self.robots_parsers[domain] = Protego.parse(response.text)
            except requests.RequestException as e:
                self.logger.warning(f"Could not fetch robots.txt from {robots_url}: {e}")
                # Create a permissive parser if robots.txt is inaccessible
                self.robots_parsers[domain] = Protego.parse("")
        
        return self.robots_parsers[domain]
    
    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """Check if the URL can be fetched according to robots.txt rules."""
        try:
            parser = self._get_robot_parser(url)
            return parser.can_fetch(url, user_agent)
        except Exception as e:
            self.logger.error(f"Error checking robots.txt for {url}: {e}")
            # Be conservative: if we can't check, assume it's allowed
            return True
    
    def _enforce_rate_limit(self, domain: str):
        """Enforce politeness delay between requests to the same domain."""
        current_time = time.time()
        
        if domain in self.last_request_times:
            time_since_last = current_time - self.last_request_times[domain]
            
            # Calculate required delay with jitter
            base_delay = self.politeness_config.get('request_delay', 2.0)
            jitter = self.politeness_config.get('jitter', 1.0)
            required_delay = base_delay + random.uniform(0, jitter)
            
            if time_since_last < required_delay:
                sleep_time = required_delay - time_since_last
                self.logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s for {domain}")
                time.sleep(sleep_time)
        
        self.last_request_times[domain] = time.time()
    
    def _rotate_user_agent(self):
        """Rotate User-Agent header to avoid detection."""
        try:
            # Get a random modern user agent
            new_ua = self.user_agent.random
            self.session.headers['User-Agent'] = new_ua
            self.logger.debug(f"Rotated User-Agent to: {new_ua[:50]}...")
        except Exception as e:
            self.logger.warning(f"Failed to rotate User-Agent: {e}")
            # Fallback to a generic but realistic User-Agent
            self.session.headers['User-Agent'] = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException, NetworkError)),
        reraise=True
    )
    def fetch(self, url: str, check_robots: bool = True) -> requests.Response:
        """
        Fetch a URL with full politeness protocol enforcement.
        
        Args:
            url: The URL to fetch
            check_robots: Whether to check robots.txt (default: True)
            
        Returns:
            requests.Response object
            
        Raises:
            RobotsBlockedError: If robots.txt blocks the URL
            NetworkError: If network request fails after retries
        """
        domain = urlparse(url).netloc
        
        # Check robots.txt compliance
        if check_robots and not self.can_fetch(url):
            raise RobotsBlockedError(f"URL blocked by robots.txt: {url}")
        
        # Enforce rate limiting
        self._enforce_rate_limit(domain)
        
        # Rotate User-Agent periodically
        if random.random() < 0.1:  # 10% chance to rotate
            self._rotate_user_agent()
        
        try:
            self.logger.info(f"Fetching: {url}")
            response = self.session.get(url)
            
            # Check for rate limiting response
            if response.status_code == 429:
                self.logger.warning(f"Rate limited by {domain}, will retry with backoff")
                raise NetworkError(f"Rate limited by server: {response.status_code}")
            
            # Check for server errors that should trigger retry
            if response.status_code >= 500:
                self.logger.warning(f"Server error {response.status_code} for {url}, will retry")
                raise NetworkError(f"Server error: {response.status_code}")
            
            # Raise for other HTTP errors (4xx)
            response.raise_for_status()
            
            self.logger.info(f"Successfully fetched {url} ({len(response.content)} bytes)")
            return response
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error fetching {url}: {e}")
            raise NetworkError(f"Failed to fetch {url}: {e}")
    
    def fetch_sitemap(self, sitemap_url: str) -> List[str]:
        """
        Fetch and parse a sitemap XML to extract URLs.
        
        Args:
            sitemap_url: URL of the sitemap
            
        Returns:
            List of URLs found in the sitemap
        """
        try:
            response = self.fetch(sitemap_url)
            urls = []
            
            # Simple XML parsing to extract <loc> tags
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.content)
            
            # Handle different sitemap namespaces
            namespaces = {
                'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9',
                'news': 'http://www.google.com/schemas/sitemap-news/0.9'
            }
            
            # Find all <loc> elements
            for loc in root.findall('.//sitemap:loc', namespaces):
                if loc.text:
                    urls.append(loc.text.strip())
            
            # If no namespaced elements found, try without namespace
            if not urls:
                for loc in root.findall('.//loc'):
                    if loc.text:
                        urls.append(loc.text.strip())
            
            self.logger.info(f"Extracted {len(urls)} URLs from sitemap {sitemap_url}")
            return urls
            
        except Exception as e:
            self.logger.error(f"Failed to parse sitemap {sitemap_url}: {e}")
            return []
    
    def close(self):
        """Clean up resources."""
        self.session.close()