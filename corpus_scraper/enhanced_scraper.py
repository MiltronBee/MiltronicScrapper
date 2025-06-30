"""
Enhanced web scraping component with browser automation support.
Combines HTTP requests with Playwright for JavaScript-heavy sites.
"""

import time
import random
import logging
import os
import asyncio
import requests
from urllib.parse import urlparse, urljoin
from typing import Dict, Optional, List, Union
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from protego import Protego
from fake_useragent import UserAgent
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
import concurrent.futures
from threading import Lock

from .exceptions import RobotsBlockedError, NetworkError
from .encoding_validator import EncodingValidator


class BrowserPool:
    """Manages a pool of Playwright browser instances for efficient resource usage."""
    
    def __init__(self, pool_size: int = 4):
        self.pool_size = pool_size
        self.browsers: List[Browser] = []
        self.available_browsers: List[Browser] = []
        self.lock = Lock()
        self.playwright = None
        self.initialized = False
    
    async def initialize(self):
        """Initialize the browser pool."""
        if self.initialized:
            return
            
        self.playwright = await async_playwright().start()
        
        for _ in range(self.pool_size):
            browser = await self.playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-extensions',
                    '--disable-images',  # Speed optimization
                ]
            )
            self.browsers.append(browser)
            self.available_browsers.append(browser)
        
        self.initialized = True
    
    async def get_browser(self) -> Browser:
        """Get an available browser from the pool."""
        while True:
            with self.lock:
                if self.available_browsers:
                    return self.available_browsers.pop()
            # Wait and retry if no browsers available
            await asyncio.sleep(0.1)
    
    def return_browser(self, browser: Browser):
        """Return a browser to the pool."""
        with self.lock:
            if browser in self.browsers:  # Ensure it's one of our browsers
                self.available_browsers.append(browser)
    
    async def close(self):
        """Close all browsers and cleanup."""
        if not self.initialized:
            return
            
        for browser in self.browsers:
            await browser.close()
        
        await self.playwright.stop()
        self.initialized = False


class EnhancedScraper:
    """
    Enhanced network-facing component with browser automation support.
    Combines fast HTTP requests with Playwright for JavaScript-heavy sites.
    """
    
    def __init__(self, politeness_config: Dict = None, browser_config: Dict = None):
        """Initialize the enhanced scraper."""
        self.politeness_config = politeness_config or {}
        self.browser_config = browser_config or {}
        
        # HTTP session setup
        self.session = requests.Session()
        self.robots_parsers = {}
        self.logger = logging.getLogger(__name__)
        
        # User agent management
        self.user_agents = self.politeness_config.get('user_agents', [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ])
        self.current_ua_index = 0
        
        # Browser automation setup
        self.browser_pool = None
        self.browser_enabled = self.browser_config.get('enabled', True)
        
        # Rate limiting
        self.last_request_times = {}
        self.domain_delays = self.politeness_config.get('domain_rate_limits', {
            'reddit.com': 2.0,
            'youtube.com': 1.0,
            'wikipedia.org': 0.5,
            'default': 0.3
        })
        
        # Encoding validator
        self.encoding_validator = EncodingValidator()
        
        self.logger.info(f"Enhanced scraper initialized - Browser automation: {self.browser_enabled}")
    
    async def _ensure_browser_pool(self):
        """Ensure browser pool is initialized."""
        if self.browser_enabled and self.browser_pool is None:
            pool_size = self.browser_config.get('pool_size', 4)
            self.browser_pool = BrowserPool(pool_size)
            await self.browser_pool.initialize()
            self.logger.info(f"Browser pool initialized with {pool_size} instances")
    
    def _get_next_user_agent(self) -> str:
        """Get next user agent in rotation."""
        if self.politeness_config.get('rotate_user_agents', True):
            ua = self.user_agents[self.current_ua_index]
            self.current_ua_index = (self.current_ua_index + 1) % len(self.user_agents)
            return ua
        return self.user_agents[0]
    
    def _should_respect_robots(self, url: str, source_config: Dict = None) -> bool:
        """Determine if robots.txt should be respected for this URL."""
        # Check source-specific setting first
        if source_config and 'respect_robots_txt' in source_config:
            return source_config['respect_robots_txt']
        
        # Check global override
        if not self.politeness_config.get('global_respect_robots', True):
            return False
        
        # Default behavior
        return True
    
    def _get_domain_delay(self, domain: str) -> float:
        """Get delay for specific domain."""
        return self.domain_delays.get(domain, self.domain_delays.get('default', 0.3))
    
    def _apply_rate_limiting(self, url: str):
        """Apply rate limiting based on domain."""
        domain = urlparse(url).netloc
        delay = self._get_domain_delay(domain)
        
        if domain in self.last_request_times:
            elapsed = time.time() - self.last_request_times[domain]
            if elapsed < delay:
                sleep_time = delay - elapsed
                self.logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s for {domain}")
                time.sleep(sleep_time)
        
        self.last_request_times[domain] = time.time()
    
    def check_robots_txt(self, url: str, source_config: Dict = None) -> bool:
        """Check if URL is allowed by robots.txt."""
        if not self._should_respect_robots(url, source_config):
            return True
        
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        
        if domain not in self.robots_parsers:
            robots_url = urljoin(url, '/robots.txt')
            try:
                response = self.session.get(robots_url, timeout=10)
                if response.status_code == 200:
                    self.robots_parsers[domain] = Protego.parse(response.text)
                else:
                    self.robots_parsers[domain] = None  # No robots.txt
            except Exception as e:
                self.logger.debug(f"Could not fetch robots.txt for {domain}: {e}")
                self.robots_parsers[domain] = None
        
        robots_parser = self.robots_parsers[domain]
        if robots_parser:
            user_agent = self._get_next_user_agent()
            return robots_parser.can_fetch(url, user_agent)
        
        return True  # No robots.txt means allowed
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((requests.RequestException, NetworkError))
    )
    def _fetch_http(self, url: str, source_config: Dict = None) -> requests.Response:
        """Fetch URL using HTTP requests with retries."""
        self._apply_rate_limiting(url)
        
        headers = {
            'User-Agent': self._get_next_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-MX,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        timeout = self.politeness_config.get('timeout', 60)
        
        try:
            response = self.session.get(
                url,
                headers=headers,
                timeout=timeout,
                allow_redirects=True,
                verify=self.politeness_config.get('ssl_verify', False)
            )
            response.raise_for_status()
            return response
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [403, 404]:
                raise NetworkError(f"HTTP {e.response.status_code}: {e}")
            else:
                raise NetworkError(f"HTTP error: {e}")
        except requests.exceptions.RequestException as e:
            raise NetworkError(f"Request failed: {e}")
    
    async def _fetch_browser(self, url: str, source_config: Dict = None) -> str:
        """Fetch URL using Playwright browser automation."""
        await self._ensure_browser_pool()
        
        if not self.browser_pool:
            raise NetworkError("Browser automation not available")
        
        browser = await self.browser_pool.get_browser()
        
        try:
            context = await browser.new_context(
                viewport={
                    'width': self.browser_config.get('viewport_width', 1920),
                    'height': self.browser_config.get('viewport_height', 1080)
                },
                user_agent=self._get_next_user_agent(),
                java_script_enabled=True,
                ignore_https_errors=True
            )
            
            # Block images for speed if configured
            if not self.browser_config.get('load_images', False):
                await context.route('**/*.{png,jpg,jpeg,gif,webp,svg}', lambda route: route.abort())
            
            page = await context.new_page()
            
            # Set timeout
            page.set_default_timeout(self.browser_config.get('page_timeout', 30000))
            
            # Navigate to page
            await page.goto(url, wait_until='domcontentloaded')
            
            # Wait for network idle if configured
            if self.browser_config.get('wait_for_network', True):
                try:
                    await page.wait_for_load_state('networkidle', timeout=10000)
                except Exception:
                    self.logger.debug(f"Network idle timeout for {url}")
            
            # Get page content
            content = await page.content()
            
            await context.close()
            return content
            
        except Exception as e:
            self.logger.error(f"Browser fetch failed for {url}: {e}")
            raise NetworkError(f"Browser automation failed: {e}")
        finally:
            self.browser_pool.return_browser(browser)
    
    def _run_browser_fetch(self, url: str, source_config: Dict = None) -> str:
        """Run browser fetch in thread-safe manner."""
        def run_async():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(self._fetch_browser(url, source_config))
            finally:
                loop.close()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(run_async)
            return future.result(timeout=90)  # 90 second timeout
    
    def fetch(self, url: str, source_config: Dict = None, force_browser: bool = False) -> requests.Response:
        """
        Main fetch method that chooses between HTTP and browser automation.
        
        Args:
            url: URL to fetch
            source_config: Source-specific configuration
            force_browser: Force use of browser automation
            
        Returns:
            Response-like object with .text and .status_code attributes
        """
        self.logger.debug(f"Fetching: {url}")
        
        # Check robots.txt if required
        if not self.check_robots_txt(url, source_config):
            raise RobotsBlockedError(f"Blocked by robots.txt: {url}")
        
        # Determine fetch method
        use_browser = force_browser or (source_config and source_config.get('render_js', False))
        
        if use_browser and self.browser_enabled:
            self.logger.debug(f"Using browser automation for {url}")
            try:
                html_content = self._run_browser_fetch(url, source_config)
                
                # Create response-like object
                class BrowserResponse:
                    def __init__(self, text: str, url: str):
                        self.text = text
                        self.content = text.encode('utf-8')
                        self.status_code = 200
                        self.url = url
                        self.headers = {'Content-Type': 'text/html; charset=utf-8'}
                
                return BrowserResponse(html_content, url)
                
            except Exception as e:
                self.logger.warning(f"Browser fetch failed, falling back to HTTP: {e}")
                # Fall back to HTTP
        
        # Use HTTP fetch
        self.logger.debug(f"Using HTTP requests for {url}")
        return self._fetch_http(url, source_config)
    
    def fetch_sitemap(self, sitemap_url: str) -> List[str]:
        """Fetch and parse sitemap for URLs."""
        try:
            response = self._fetch_http(sitemap_url)
            # Parse sitemap XML
            import xml.etree.ElementTree as ET
            
            urls = []
            root = ET.fromstring(response.text)
            
            # Handle different sitemap namespaces
            namespaces = {
                'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'
            }
            
            # Extract URLs from sitemap
            for url_elem in root.findall('.//sitemap:url', namespaces):
                loc_elem = url_elem.find('sitemap:loc', namespaces)
                if loc_elem is not None:
                    urls.append(loc_elem.text)
            
            # If no URLs found, try without namespace
            if not urls:
                for url_elem in root.findall('.//url'):
                    loc_elem = url_elem.find('loc')
                    if loc_elem is not None:
                        urls.append(loc_elem.text)
            
            self.logger.info(f"Sitemap {sitemap_url} contained {len(urls)} URLs")
            return urls
            
        except Exception as e:
            self.logger.error(f"Failed to fetch sitemap {sitemap_url}: {e}")
            return []
    
    async def close(self):
        """Close all resources."""
        if self.browser_pool:
            await self.browser_pool.close()
        self.session.close()
        self.logger.info("Enhanced scraper closed")
    
    def __del__(self):
        """Cleanup on destruction."""
        if self.browser_pool:
            try:
                asyncio.run(self.close())
            except Exception:
                pass  # Best effort cleanup