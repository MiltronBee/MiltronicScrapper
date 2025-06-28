"""
Advanced web scraping component with sophisticated politeness protocols.
Handles HTTP requests, robots.txt compliance, and rate limiting.
"""

import time
import random
import logging
import os
import requests
from urllib.parse import urlparse, urljoin
from typing import Dict, Optional, List
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from protego import Protego
from fake_useragent import UserAgent
from .exceptions import RobotsBlockedError, NetworkError
from .dynamic_scraper import DynamicScraperSync
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


class Scraper:
    """
    Network-facing component responsible for all HTTP/S interactions.
    Implements advanced politeness protocols and resilient request handling.
    """
    
    def __init__(self, politeness_config: Dict = None):
        """Initialize the scraper with politeness settings."""
        self.politeness_config = politeness_config or {}
        self.session = requests.Session()
        self.robots_parsers = {}
        self.logger = logging.getLogger(__name__)
        self.user_agent = UserAgent()
        self.dynamic_scraper = None
        
        # Discord webhook for notifications
        self.discord_webhook = os.getenv("DISCORD_CHANNEL_WEBHOOK")
        
        # Track domain-specific request times for rate limiting
        self.last_request_times = {}
        self.domain_delays = {
            # Domain-specific minimum delays between requests in seconds
            'opensubtitles.org': 5.0,  # Min 5 seconds between OpenSubtitles requests
            'default': 1.0,            # Default delay for other domains
        }
        
        # Configure session with realistic headers
        self._setup_session()
    
    def _send_discord_notification(self, url: str, message_type: str = "success"):
        """Send Discord notification for successful scraping."""
        if not self.discord_webhook:
            return
            
        try:
            # Only send notifications for certain domains to avoid spam
            domain = urlparse(url).netloc.lower()
            important_domains = [
                'sinembargo.mx', 'milenio.com', 'elpais.com', 
                'animalpolitico.com', 'proceso.com.mx', 'nexos.com.mx',
                'letraslibres.com', 'eluniversal.com.mx'
            ]
            
            # Only notify for important news sources
            if not any(important_domain in domain for important_domain in important_domains):
                return
            
            if message_type == "success":
                title = "✅ Successful RSS Scraping"
                description = (f"**URL:** {url}\n\n"
                              f"📰 **Domain:** {domain}\n"
                              f"🎆 **Status:** Content successfully extracted\n"
                              f"⏱️ **Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                color = 65280  # Green color
            else:
                return
            
            payload = {
                "embeds": [{
                    "title": title,
                    "description": description,
                    "color": color,
                    "footer": {"text": "Spanish Corpus Framework | Main Scraper"},
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
            self.logger.warning(f"Failed to send Discord notification: {e}")
    
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
        
        # Configure SSL handling for sites with certificate issues
        ssl_verify = self.politeness_config.get('ssl_verify', True)
        if not ssl_verify:
            self.session.verify = False
            # Disable SSL warnings when verification is disabled
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.logger.info("SSL certificate verification disabled")
    
    def _get_robot_parser(self, url: str) -> Protego:
        """Get or create a robots.txt parser for the given URL's domain."""
        domain = urlparse(url).netloc
        
        if domain not in self.robots_parsers:
            robots_url = f"https://{domain}/robots.txt"
            try:
                self.logger.info(f"Fetching robots.txt from {robots_url}")
                # Use the session with SSL settings
                response = self.session.get(robots_url, timeout=10, verify=self.session.verify)
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
        """Enforce politeness delay between requests to the same domain.
        
        Applies domain-specific rate limiting, with special handling for domains
        like OpenSubtitles.org that have stricter rate limits.
        
        Args:
            domain: The domain name to enforce rate limiting for
        """
        current_time = time.time()
        
        # Get base delay from config
        base_delay = self.politeness_config.get('request_delay', 2.0)
        jitter = self.politeness_config.get('jitter', 1.0)
        
        # Apply domain-specific delays for known rate-limited sites
        if 'opensubtitles.org' in domain:
            # OpenSubtitles has strict rate limiting - use at least 60 seconds
            # between requests to avoid 429 errors and CAPTCHA
            base_delay = max(base_delay, 60.0)
            # Add more randomness to appear more human-like
            jitter = random.uniform(30, 60)  # 30-60 seconds additional random delay (reduced from 90)
            base_delay += jitter
            # Cap maximum delay at 300 seconds (5 minutes) to prevent excessive waits
            base_delay = min(base_delay, 300.0)
            self.logger.info(f"Using extended delay of {base_delay:.1f}s for OpenSubtitles")
        
        # Add random jitter to avoid patterns
        required_delay = base_delay + random.uniform(0, jitter)
        
        # Check when we last accessed this domain
        if domain in self.last_request_times:
            time_since_last = current_time - self.last_request_times[domain]
            
            if time_since_last < required_delay:
                sleep_time = required_delay - time_since_last
                self.logger.info(f"Rate limiting: waiting {sleep_time:.2f}s before accessing {domain}")
                time.sleep(sleep_time)
        
        # Update the last request time for this domain
        self.last_request_times[domain] = time.time()
    
    def _rotate_user_agent(self):
        """Rotate User-Agent header to avoid detection."""
        try:
            # Get a random modern user agent - prefer browsers
            desktop_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_5_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
            ]
            # 50% chance to use our predefined desktop agents
            if random.random() < 0.5:
                new_ua = random.choice(desktop_agents)
            else:
                new_ua = self.user_agent.random
                
            self.session.headers['User-Agent'] = new_ua
            
            # Add realistic headers that browsers typically include
            self.session.headers.update({
                'Accept-Language': 'en-US,en;q=0.9,es;q=0.8',
                'Referer': 'https://www.opensubtitles.org/es/search',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
            })
            self.logger.info(f"Rotated User-Agent to: {new_ua[:50]}...")
        except Exception as e:
            self.logger.warning(f"Failed to rotate User-Agent: {e}")
            # Fallback to a generic but realistic User-Agent
            self.session.headers['User-Agent'] = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=8),
        retry=retry_if_exception_type((requests.exceptions.RequestException, NetworkError)),
        reraise=True
    )
    def _get_dynamic_scraper(self):
        """Get or create dynamic scraper instance."""
        if self.dynamic_scraper is None:
            try:
                self.dynamic_scraper = DynamicScraperSync(self.politeness_config)
                self.dynamic_scraper.__enter__()
                self.logger.info("Dynamic scraper initialized")
            except Exception as e:
                self.logger.warning(f"Failed to initialize dynamic scraper: {e}")
                self.dynamic_scraper = None
        return self.dynamic_scraper
    
    def fetch(self, url: str, check_robots: bool = True, force_dynamic: bool = False) -> requests.Response:
        """
        Fetch a URL with full politeness protocol enforcement.
        
        Args:
            url: The URL to fetch
            check_robots: Whether to check robots.txt (default: True)
            force_dynamic: Force use of dynamic scraper (default: False)
            
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
        
        # Check if dynamic scraping is needed
        dynamic_scraper = self._get_dynamic_scraper()
        needs_js = (force_dynamic or 
                   (dynamic_scraper and dynamic_scraper.requires_js_rendering(url)))
        
        if needs_js and dynamic_scraper:
            self.logger.info(f"Using dynamic scraper for: {url}")
            
            # Enforce rate limiting
            self._enforce_rate_limit(domain)
            
            try:
                html_content = dynamic_scraper.fetch(url)
                
                # Create a mock response object
                mock_response = type('MockResponse', (), {})()
                mock_response.text = html_content
                mock_response.content = html_content.encode('utf-8')
                mock_response.status_code = 200
                mock_response.headers = {'content-type': 'text/html; charset=utf-8'}
                mock_response.url = url
                
                def raise_for_status():
                    if mock_response.status_code >= 400:
                        raise requests.exceptions.HTTPError(f"{mock_response.status_code}")
                
                mock_response.raise_for_status = raise_for_status
                
                self.logger.info(f"Successfully fetched with dynamic scraper: {url}")
                return mock_response
                
            except Exception as e:
                self.logger.warning(f"Dynamic scraper failed for {url}: {e}")
                # Fall back to regular scraping
        
        # Regular HTTP scraping
        # Enforce rate limiting
        self._enforce_rate_limit(domain)
        
        # Rotate User-Agent periodically
        if random.random() < 0.1:  # 10% chance to rotate
            self._rotate_user_agent()
        
        try:
            self.logger.info(f"Fetching: {url}")
            response = self.session.get(url, timeout=self.session.timeout)
            
            # Check for rate limiting response
            if response.status_code == 429:
                self.logger.warning(f"Rate limited by {domain}, will retry with backoff")
                raise NetworkError(f"Rate limited by server: {response.status_code}")
            
            # Check for CAPTCHA redirections in OpenSubtitles
            if 'opensubtitles.org' in domain and response.url != url and '/captcha/' in response.url:
                self.logger.warning(f"CAPTCHA detected for {url}, redirected to {response.url}")
                # Mark for extended cooldown
                self.last_request_times[domain] = time.time()
                # Add an extra very long pause for OpenSubtitles on CAPTCHA detection
                captcha_delay = 300 + random.uniform(60, 180)  # 5-8 minutes
                self.logger.warning(f"CAPTCHA cooldown: waiting {captcha_delay:.1f} seconds")
                time.sleep(captcha_delay)
                raise NetworkError(f"CAPTCHA challenge detected for {url}")
            
            # Check for server errors that should trigger retry
            if response.status_code >= 500:
                self.logger.warning(f"Server error {response.status_code} for {url}, will retry")
                raise NetworkError(f"Server error: {response.status_code}")
            
            # Handle client errors more intelligently
            if response.status_code == 403:
                self.logger.warning(f"Access forbidden (403) for {url} - may be blocked")
                # Don't retry 403 errors - they're likely permanent
                response.raise_for_status()
            elif response.status_code == 404:
                self.logger.warning(f"URL not found (404): {url}")
                # Don't retry 404 errors - they're permanent
                response.raise_for_status()
            elif response.status_code == 429:
                # Handle rate limiting with exponential backoff
                retry_after = response.headers.get('Retry-After')
                
                if retry_after and retry_after.isdigit():
                    # Use server-specified delay if available
                    delay = int(retry_after)
                else:
                    # Use exponential backoff with jitter
                    # Calculate delay based on attempt number (starts with 10 seconds,
                    # doubles each time with some randomness)
                    base_delay = 10 * (2 ** min(attempt-1, 5))  # Cap at 320 seconds max base delay
                    delay = base_delay + random.uniform(1, 5)  # Add jitter
                
                # Check if domain is opensubtitles.org since they're particularly strict
                if 'opensubtitles.org' in url:
                    delay = max(delay, 30)  # Minimum 30 second delay for OpenSubtitles
                
                self.logger.warning(f"Rate limited (429) for {url}. Waiting for {delay:.1f} seconds before retry")
                time.sleep(delay)
                raise NetworkError(f"Rate limited with 429 response, waited {delay:.1f}s")
            elif response.status_code >= 400:
                self.logger.warning(f"Client error {response.status_code} for {url}")
                response.raise_for_status()
            
            # Verify response content is properly decompressed
            content_encoding = response.headers.get('Content-Encoding', '').lower()
            if content_encoding:
                self.logger.debug(f"Response has Content-Encoding: {content_encoding}")
            
            # Check if content appears to be compressed/corrupted binary data
            if len(response.content) > 100:
                # Sample first 100 bytes to check for binary corruption
                sample = response.content[:100]
                # Count non-printable characters (excluding common whitespace)
                non_printable = sum(1 for b in sample if b < 32 and b not in [9, 10, 13])
                corruption_ratio = non_printable / len(sample)
                
                if corruption_ratio > 0.3:  # More than 30% non-printable characters
                    self.logger.error(f"Response content appears corrupted for {url}: {corruption_ratio:.2%} non-printable chars")
                    # Log sample for debugging
                    self.logger.error(f"Content sample (hex): {sample[:50].hex()}")
                    raise NetworkError(f"Received corrupted/compressed content from {url}")
            
            # Asegurar que tengamos una codificación establecida y corregir problemas comunes
            original_encoding = response.encoding
            if not response.encoding:
                response.encoding = response.apparent_encoding or 'utf-8'
            
            # Fix common encoding misdetection for Spanish content
            if response.encoding and response.encoding.lower() in ['iso-8859-1', 'latin-1', 'windows-1252']:
                # These encodings often cause trafilatura to fail
                # Check if content looks like it might be UTF-8 misdetected as ISO-8859-1
                try:
                    # Try to decode as UTF-8 first
                    test_content = response.content.decode('utf-8')
                    # If successful and contains Spanish characters, use UTF-8
                    if any(char in test_content for char in ['ñ', 'á', 'é', 'í', 'ó', 'ú', 'ü']):
                        response.encoding = 'utf-8'
                        self.logger.info(f"Corrected encoding from {original_encoding} to utf-8 for {url}")
                except UnicodeDecodeError:
                    # Keep original encoding if UTF-8 doesn't work
                    pass
            
            # Verify that response.text produces readable content
            try:
                text_sample = response.text[:200] if len(response.text) > 200 else response.text
                # Check if the text contains mostly binary data
                non_text_chars = sum(1 for c in text_sample if ord(c) > 127 and c not in 'áéíóúñü¿¡')
                if len(text_sample) > 50 and non_text_chars / len(text_sample) > 0.3:
                    self.logger.error(f"Response.text contains corrupted content for {url}")
                    self.logger.error(f"Text sample: {repr(text_sample[:100])}")
                    raise NetworkError(f"Response.text is corrupted for {url}")
            except Exception as e:
                self.logger.error(f"Failed to access response.text for {url}: {e}")
                raise NetworkError(f"Cannot access response text for {url}: {e}")
            
            self.logger.debug(f"Encoding detectado para {url}: {response.encoding} (original: {original_encoding})")
            
            self.logger.info(f"Successfully fetched {url} ({len(response.content)} bytes), encoding: {response.encoding}")
            # Send Discord notification for successful scraping
            self._send_discord_notification(url, "success")
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
        if self.dynamic_scraper:
            try:
                self.dynamic_scraper.__exit__(None, None, None)
            except Exception as e:
                self.logger.warning(f"Error closing dynamic scraper: {e}")
            self.dynamic_scraper = None
        
        self.session.close()