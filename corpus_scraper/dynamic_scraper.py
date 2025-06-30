"""
Dynamic Content Scraper using Playwright for JavaScript-heavy Mexican websites.
Handles modern news sites, social media content, and dynamic government portals.
"""

import asyncio
import logging
import time
import os
from typing import Dict, Optional, List, Any
from urllib.parse import urlparse
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from .exceptions import NetworkError, RobotsBlockedError
from datetime import datetime
import requests
from dotenv import load_dotenv

load_dotenv()


class DynamicScraper:
    """
    Advanced dynamic content scraper using Playwright for JavaScript-heavy sites.
    Specifically optimized for modern Mexican news and government websites.
    """
    
    def __init__(self, politeness_config: Dict):
        self.politeness_config = politeness_config
        self.logger = logging.getLogger(__name__)
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.playwright = None
        
        # Discord webhook for notifications
        self.discord_webhook = os.getenv("DISCORD_CHANNEL_WEBHOOK")
        
        # Sites that require JavaScript rendering
        self.js_required_domains = {
            'reddit.com',
            'animalpolitico.com',
            'nexos.com.mx',
            'letraslibres.com',
            'milenio.com',
            'eluniversal.com.mx',
            'proceso.com.mx',
            'sinembargo.mx',
            'forbes.com.mx',
            'expansion.mx'
        }
        
        # Anti-bot detection patterns to handle
        self.antibot_patterns = [
            'cloudflare',
            'checking your browser',
            'enable javascript',
            'captcha',
            'robot verification',
            'ddos protection'
        ]
    
    def _send_discord_notification(self, url: str, message_type: str = "anti_bot"):
        """Send Discord notification for scraping events."""
        if not self.discord_webhook:
            return
            
        try:
            if message_type == "anti_bot":
                title = "⚠️ Anti-Bot Protection Detected"
                description = (f"**URL:** {url}\n\n"
                              f"🔒 **Status:** Anti-bot protection encountered\n"
                              f"🔄 **Action:** Attempting bypass procedures\n"
                              f"⏱️ **Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                color = 16776960  # Yellow/orange color
            elif message_type == "success":
                title = "✅ Dynamic Content Successfully Scraped"
                description = (f"**URL:** {url}\n\n"
                              f"🎆 **Status:** Content successfully extracted\n"
                              f"⚙️ **Method:** Dynamic JavaScript rendering\n"
                              f"⏱️ **Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                color = 65280  # Green color
            else:
                return
            
            payload = {
                "embeds": [{
                    "title": title,
                    "description": description,
                    "color": color,
                    "footer": {"text": "Spanish Corpus Framework | Dynamic Scraper"},
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
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def start(self):
        """Initialize Playwright browser."""
        try:
            self.playwright = await async_playwright().start()
            
            # Launch browser with stealth configuration
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-blink-features=AutomationControlled'
                ]
            )
            
            # Create context with realistic settings
            self.context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='es-MX',
                timezone_id='America/Mexico_City',
                geolocation={'latitude': 19.4326, 'longitude': -99.1332},  # Mexico City
                permissions=['geolocation'],
                extra_http_headers={
                    'Accept-Language': 'es-MX,es;q=0.9,en;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'DNT': '1',
                    'Upgrade-Insecure-Requests': '1'
                }
            )
            
            # Add stealth scripts
            await self.context.add_init_script("""
                // Remove webdriver property
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                // Mock plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                
                // Mock languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['es-MX', 'es', 'en'],
                });
                
                // Override permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
            """)
            
            self.logger.info("Playwright browser initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Playwright: {e}")
            raise NetworkError(f"Playwright initialization failed: {e}")
    
    def requires_js_rendering(self, url: str) -> bool:
        """Check if URL requires JavaScript rendering."""
        domain = urlparse(url).netloc.lower()
        return any(js_domain in domain for js_domain in self.js_required_domains)
    
    async def detect_antibot_protection(self, page: Page) -> bool:
        """Detect if page has anti-bot protection."""
        try:
            content = await page.content()
            content_lower = content.lower()
            
            return any(pattern in content_lower for pattern in self.antibot_patterns)
            
        except Exception:
            return False
    
    async def handle_antibot_protection(self, page: Page, url: str) -> bool:
        """Handle anti-bot protection if detected."""
        try:
            self.logger.warning(f"Anti-bot protection detected for {url}")
            # Send Discord notification
            self._send_discord_notification(url, "anti_bot")
            
            # Wait for potential redirects or challenges
            await page.wait_for_timeout(5000)
            
            # Check for Cloudflare challenge
            if await page.locator('text=Checking your browser').count() > 0:
                self.logger.info("Waiting for Cloudflare challenge...")
                await page.wait_for_selector('text=Checking your browser', state='detached', timeout=30000)
            
            # Check for CAPTCHA
            if await page.locator('iframe[src*="captcha"]').count() > 0:
                self.logger.warning("CAPTCHA detected - skipping")
                return False
            
            # Additional wait for any JavaScript-based protections
            await page.wait_for_timeout(3000)
            
            return True
            
        except Exception as e:
            self.logger.warning(f"Failed to handle anti-bot protection: {e}")
            return False
    
    async def fetch_dynamic_content(self, url: str) -> str:
        """
        Fetch content from JavaScript-heavy websites.
        
        Args:
            url: URL to fetch
            
        Returns:
            Rendered HTML content
            
        Raises:
            NetworkError: If fetching fails
        """
        if not self.context:
            raise NetworkError("Browser context not initialized")
        
        page = None
        try:
            # Create new page
            page = await self.context.new_page()
            
            # Set timeout from config
            timeout = self.politeness_config.get('timeout', 30) * 1000  # Convert to ms
            page.set_default_timeout(timeout)
            
            # Block unnecessary resources for faster loading
            await page.route("**/*", lambda route: (
                route.abort() if route.request.resource_type in ["image", "font", "media"] 
                else route.continue_()
            ))
            
            self.logger.info(f"Fetching dynamic content: {url}")
            
            # Navigate to URL
            response = await page.goto(url, wait_until='domcontentloaded')
            
            if not response:
                raise NetworkError(f"Failed to navigate to {url}")
            
            # Check response status
            if response.status >= 400:
                raise NetworkError(f"HTTP {response.status} for {url}")
            
            # Handle anti-bot protection
            if await self.detect_antibot_protection(page):
                if not await self.handle_antibot_protection(page, url):
                    raise NetworkError(f"Unable to bypass anti-bot protection for {url}")
            
            # Wait for critical content to load
            try:
                # Wait for main content selectors
                await page.wait_for_selector('article, main, .content, .post-content, .entry-content', timeout=10000)
            except:
                # If no main content selector found, just wait a bit
                await page.wait_for_timeout(3000)
            
            # Scroll to load lazy content
            await page.evaluate("""
                window.scrollTo(0, document.body.scrollHeight / 2);
            """)
            await page.wait_for_timeout(1000)
            
            # Remove ads and pop-ups
            await page.evaluate("""
                // Remove common ad and popup selectors
                const selectors = [
                    '[class*="ad-"]', '[id*="ad-"]', '[class*="popup"]', 
                    '[class*="modal"]', '[class*="newsletter"]', '[class*="subscription"]',
                    '.advertisement', '.ads', '.ad-banner', '.popup-overlay'
                ];
                
                selectors.forEach(selector => {
                    document.querySelectorAll(selector).forEach(el => el.remove());
                });
                
                // Remove elements with advertising keywords
                document.querySelectorAll('*').forEach(el => {
                    const text = el.textContent.toLowerCase();
                    if (text.includes('publicidad') || text.includes('anuncio') || 
                        text.includes('suscríbete') || text.includes('newsletter')) {
                        if (el.offsetHeight < 200) { // Only remove small elements
                            el.remove();
                        }
                    }
                });
            """)
            
            # Get final content
            content = await page.content()
            
            self.logger.info(f"Successfully fetched dynamic content: {len(content)} chars from {url}")
            # Send Discord notification for successful scraping
            self._send_discord_notification(url, "success")
            return content
            
        except Exception as e:
            self.logger.error(f"Dynamic content fetch failed for {url}: {e}")
            raise NetworkError(f"Failed to fetch dynamic content: {e}")
            
        finally:
            if page:
                await page.close()
    
    async def fetch_multiple_urls(self, urls: List[str]) -> Dict[str, str]:
        """
        Fetch multiple URLs concurrently with rate limiting.
        
        Args:
            urls: List of URLs to fetch
            
        Returns:
            Dictionary mapping URLs to their content
        """
        if not self.context:
            raise NetworkError("Browser context not initialized")
        
        results = {}
        semaphore = asyncio.Semaphore(3)  # Limit concurrent requests
        
        async def fetch_single(url: str) -> None:
            async with semaphore:
                try:
                    # Apply rate limiting
                    await asyncio.sleep(self.politeness_config.get('request_delay', 2.0))
                    content = await self.fetch_dynamic_content(url)
                    results[url] = content
                except Exception as e:
                    self.logger.error(f"Failed to fetch {url}: {e}")
                    results[url] = None
        
        # Execute all fetches concurrently
        tasks = [fetch_single(url) for url in urls]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        return results
    
    async def check_content_freshness(self, url: str) -> Dict[str, Any]:
        """
        Check if content on a page has been recently updated.
        
        Args:
            url: URL to check
            
        Returns:
            Dictionary with freshness information
        """
        if not self.context:
            raise NetworkError("Browser context not initialized")
        
        page = None
        try:
            page = await self.context.new_page()
            await page.goto(url, wait_until='domcontentloaded')
            
            # Extract publication date and last modified info
            freshness_info = await page.evaluate("""
                () => {
                    const now = new Date();
                    const info = {
                        publication_date: null,
                        last_modified: null,
                        has_recent_comments: false,
                        freshness_score: 0
                    };
                    
                    // Look for publication dates
                    const dateSelectors = [
                        'time[datetime]', '[class*="date"]', '[class*="tiempo"]',
                        '[class*="fecha"]', 'meta[property="article:published_time"]'
                    ];
                    
                    for (const selector of dateSelectors) {
                        const elem = document.querySelector(selector);
                        if (elem) {
                            const dateStr = elem.getAttribute('datetime') || elem.textContent;
                            const date = new Date(dateStr);
                            if (!isNaN(date.getTime())) {
                                info.publication_date = date.toISOString();
                                break;
                            }
                        }
                    }
                    
                    // Check for recent comments
                    const comments = document.querySelectorAll('[class*="comment"], [class*="comentario"]');
                    info.has_recent_comments = comments.length > 0;
                    
                    // Calculate freshness score
                    if (info.publication_date) {
                        const pubDate = new Date(info.publication_date);
                        const daysDiff = (now - pubDate) / (1000 * 60 * 60 * 24);
                        info.freshness_score = Math.max(0, 100 - daysDiff * 2);
                    }
                    
                    return info;
                }
            """)
            
            return freshness_info
            
        except Exception as e:
            self.logger.error(f"Failed to check freshness for {url}: {e}")
            return {'freshness_score': 0}
            
        finally:
            if page:
                await page.close()
    
    async def close(self):
        """Clean up resources."""
        try:
            if self.context:
                await self.context.close()
                self.context = None
            
            if self.browser:
                await self.browser.close()
                self.browser = None
            
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
            
            self.logger.info("Dynamic scraper closed successfully")
            
        except Exception as e:
            self.logger.error(f"Error closing dynamic scraper: {e}")


class DynamicScraperSync:
    """Synchronous wrapper for DynamicScraper."""
    
    def __init__(self, politeness_config: Dict):
        self.politeness_config = politeness_config
        self.logger = logging.getLogger(__name__)
        self._loop = None
        self._scraper = None
    
    def __enter__(self):
        try:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._scraper = DynamicScraper(self.politeness_config)
            self._loop.run_until_complete(self._scraper.start())
            return self
        except Exception as e:
            self.logger.error(f"Failed to start dynamic scraper: {e}")
            self._cleanup()
            raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup()
    
    def _cleanup(self):
        """Properly cleanup resources."""
        try:
            if self._scraper:
                if self._loop and not self._loop.is_closed():
                    self._loop.run_until_complete(self._scraper.close())
                self._scraper = None
        except Exception as e:
            self.logger.warning(f"Error closing dynamic scraper: {e}")
        
        try:
            if self._loop and not self._loop.is_closed():
                # Cancel all remaining tasks
                pending_tasks = [task for task in asyncio.all_tasks(self._loop) if not task.done()]
                if pending_tasks:
                    self.logger.info(f"Cancelling {len(pending_tasks)} pending tasks")
                    for task in pending_tasks:
                        task.cancel()
                    
                    # Wait for tasks to be cancelled
                    try:
                        self._loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))
                    except Exception:
                        pass  # Expected when tasks are cancelled
                
                # Close the loop
                self._loop.close()
                self._loop = None
        except Exception as e:
            self.logger.warning(f"Error closing event loop: {e}")
    
    def fetch(self, url: str) -> str:
        """Synchronous fetch method."""
        if not self._scraper or not self._loop:
            raise NetworkError("Dynamic scraper not initialized")
        
        return self._loop.run_until_complete(
            self._scraper.fetch_dynamic_content(url)
        )
    
    def requires_js_rendering(self, url: str) -> bool:
        """Check if URL requires JavaScript rendering."""
        return self._scraper.requires_js_rendering(url) if self._scraper else False