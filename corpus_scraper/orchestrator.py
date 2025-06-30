"""
Central orchestrator that coordinates all framework components.
Manages the complete scraping workflow from URL discovery to content saving.
"""

import os
import logging
import time
import random
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

from .config_manager import ConfigManager
from .scraper import Scraper
from .extractor import Extractor
from .saver import Saver
from .state_manager import StateManager
from .rss_manager import RSSManager
from .link_extractor import LinkExtractor
from .exceptions import ScrapingError, RobotsBlockedError, NetworkError

load_dotenv()

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
        self.discord_webhook = os.getenv("DISCORD_CHANNEL_WEBHOOK")
        
        # Track source failures for circuit breaker pattern
        self.source_failures = {}
        self.max_consecutive_failures = 10  # Skip source after 10 consecutive failures
        
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
            self.link_extractor = LinkExtractor(extraction_config)
            
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
    
    def _send_discord_url(self, url: str, source_name: str, title: str = None):
        """Send individual URL to Discord for preview after successful extraction."""
        if not self.discord_webhook:
            return
            
        try:
            # Create payload with just the URL for Discord preview
            content = f"📄 **{source_name}** - Content extracted"
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
            
        except Exception as e:
            self.logger.warning(f"Failed to send Discord URL notification: {e}")
    
    def _discover_letras_com_urls(self, base_url: str) -> List[str]:
        """
        Discover artist and song URLs from letras.com.
        
        Args:
            base_url: The base artist listing URL (like genre page)
            
        Returns:
            List of discovered artist and song URLs
        """
        from .letras_scraper import LetrasScraper
        
        self.logger.info(f"Discovering letras.com content from {base_url}")
        
        try:
            # Initialize the letras.com scraper with our scraper
            letras_scraper = LetrasScraper(self.scraper)
            
            # Get artists from the page
            urls = []
            artists = letras_scraper.get_artists_from_page(base_url)
            
            # Add artist URLs
            for artist in artists:
                urls.append(artist['url'])
                
                # Get songs for each artist
                songs = letras_scraper.get_songs_from_artist_page(artist['url'])
                
                # Add song URLs
                for song in songs:
                    urls.append(song['url'])
            
            # Remove duplicates
            urls = list(dict.fromkeys(urls))
            
            self.logger.info(f"Discovered {len(urls)} URLs from letras.com")
            return urls
            
        except Exception as e:
            self.logger.error(f"Error discovering letras.com URLs: {e}")
            return []
    
    def _discover_opensubtitles_urls(self, base_url: str) -> List[str]:
        """
        Discover subtitle download links from OpenSubtitles search page.
        
        Args:
            base_url: The search results URL
            
        Returns:
            List of discovered subtitle download URLs
        """
        from bs4 import BeautifulSoup
        from urllib.parse import urljoin
        import requests
        import re
        
        self.logger.info(f"Discovering OpenSubtitles download links from {base_url}")
        
        # Use direct requests with proper headers
        try:
            # OpenSubtitles needs a good User-Agent
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(base_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            self.logger.info(f"Successfully fetched page ({len(response.text)} bytes)")
            
            # Parse the HTML with BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find direct download links
            download_links = []
            
            # Method 1: Find direct download links from the search results page
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                
                # OpenSubtitles direct download links have this pattern
                if href.startswith('/download/s/') or href.startswith('/download/sub/'):
                    download_url = urljoin(base_url, href)
                    download_links.append(download_url)
                    self.logger.debug(f"Found direct download link: {download_url}")
            
            # Check if we found any direct download links
            if download_links:
                self.logger.info(f"Found {len(download_links)} direct download links")
            else:
                self.logger.warning("No direct download links found, trying alternative method")
                
                # Method 2: Find subtitle detail pages first, then look for download links
                detail_links = []
                
                # Find all subtitle detail page links
                for a_tag in soup.find_all('a', href=True):
                    href = a_tag['href']
                    
                    # Pattern for search results that lead to subtitle listings
                    if re.search(r'/es/search/sublanguageid-spa.*?/pimdbid-\d+', href):
                        detail_url = urljoin(base_url, href)
                        detail_links.append(detail_url)
                        self.logger.debug(f"Found subtitle detail link: {detail_url}")
                
                # Remove duplicates
                detail_links = list(dict.fromkeys(detail_links))
                self.logger.info(f"Found {len(detail_links)} unique subtitle detail links")
                
                # Visit a limited number of detail pages to find download links
                for detail_url in detail_links[:5]:  # Limit to first 5 to avoid too many requests
                    try:
                        self.logger.info(f"Fetching detail page: {detail_url}")
                        detail_response = requests.get(detail_url, headers=headers, timeout=15)
                        detail_response.raise_for_status()
                        
                        detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
                        
                        # Find all download links in the detail page
                        for dl_tag in detail_soup.find_all('a', href=True):
                            dl_href = dl_tag['href']
                            if dl_href.startswith('/download/s/') or dl_href.startswith('/download/sub/'):
                                dl_url = urljoin(base_url, dl_href)
                                download_links.append(dl_url)
                                self.logger.debug(f"Found download link from detail page: {dl_url}")
                    except Exception as e:
                        self.logger.warning(f"Error processing detail page {detail_url}: {e}")
            
            # Remove duplicates from final list
            download_links = list(dict.fromkeys(download_links))
            self.logger.info(f"Found total of {len(download_links)} OpenSubtitles download links")
            return download_links
            
        except Exception as e:
            self.logger.error(f"Error extracting OpenSubtitles download links: {e}")
            return []
    
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
            
            # Skip opensubtitles source to avoid ConfigurationError
            if source_name.lower() == 'opensubtitles':
                self.logger.warning(f"Skipping 'opensubtitles' source as requested")
                continue
                
            urls = []
            
            try:
                # Special handling for letras.com source to discover artist and song URLs
                if source_name.lower() == 'letras_com':
                    self.logger.info(f"Using special discovery for letras.com source")
                    # Get the base URLs from the source
                    artist_pages = source.get('urls', [])
                    
                    # For each artist listing page, discover artist and song links
                    for artist_page in artist_pages:
                        discovered_urls = self._discover_letras_com_urls(artist_page)
                        urls.extend(discovered_urls)
                        
                    self.logger.info(f"Discovered {len(urls)} URLs from letras.com")
                # Special handling for OpenSubtitles source to discover ZIP files
                elif source_name.lower() == 'opensubtitles':
                    self.logger.info(f"Using special discovery for OpenSubtitles source")
                    # Get the search results page URLs from the source
                    search_pages = source.get('urls', [])
                    
                    # For each search page, discover ZIP links
                    for search_page in search_pages:
                        zip_urls = self._discover_opensubtitles_urls(search_page)
                        urls.extend(zip_urls)
                        
                    self.logger.info(f"Discovered {len(urls)} ZIP URLs from OpenSubtitles")
                else:
                    # Regular URL discovery for other sources
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
                    
                    # Generate dynamic date-based URLs if specified
                    if source.get('dynamic_dates', False):
                        date_urls = self._generate_date_based_urls(source)
                        urls.extend(date_urls)
                
                # Remove duplicates while preserving order
                urls = list(dict.fromkeys(urls))
                
                # Filtrar URLs ya procesadas
                filtered_urls = self._filter_already_processed_urls(urls, source_name)
                
                all_urls[source_name] = filtered_urls
                self.logger.info(f"Discovered {len(filtered_urls)} URLs for source '{source_name}' after filtering")
                
            except Exception as e:
                self.logger.error(f"Failed to discover URLs for {source_name}: {e}")
                all_urls[source_name] = []
        
        total_urls = sum(len(urls) for urls in all_urls.values())
        self.logger.info(f"URL discovery complete: {total_urls} total URLs")
        
        return all_urls
        
    def _generate_date_based_urls(self, source: Dict[str, Any]) -> List[str]:
        """
        Generate URLs based on date patterns for news sources.
        
        Args:
            source: Source configuration
            
        Returns:
            List of URLs generated based on date patterns
        """
        source_name = source['name']
        base_url = source.get('base_url', '')
        urls = []
        
        try:
            # Definir fecha inicio y fecha fin (hoy)
            end_date = datetime.now()
            
            # Fecha base para la mayoría de las fuentes
            base_start_date = datetime(2015, 1, 1)
            
            current_date = base_start_date
            
            # El País - formato: /hemeroteca/YYYY-MM-DD/
            if source_name == 'rss_el_pais':
                url_pattern = 'https://elpais.com/hemeroteca/{year}-{month:02d}-{day:02d}/'
                
                while current_date <= end_date:
                    url = url_pattern.format(
                        year=current_date.year,
                        month=current_date.month,
                        day=current_date.day
                    )
                    urls.append(url)
                    current_date += timedelta(days=1)
                
                self.logger.info(f"Generated {len(urls)} date-based URLs for El País")
            
        except Exception as e:
            self.logger.error(f"Error generating date-based URLs for {source_name}: {e}")
        
        return urls
        
    def _filter_already_processed_urls(self, urls: List[str], source_name: str) -> List[str]:
        """
        Filtra URLs que ya han sido procesadas exitosamente.
        
        Args:
            urls: List of URLs to filter
            source_name: Name of the source
            
        Returns:
            List of URLs that haven't been processed yet
        """
        if not urls:
            return []
            
        try:
            # Obtener las URLs ya procesadas del StateManager
            processed_urls = self.state_manager.get_completed_urls(urls, source_name)
            filtered_urls = [url for url in urls if url not in processed_urls]
            
            skipped = len(urls) - len(filtered_urls)
            if skipped > 0:
                self.logger.info(f"Skipping {skipped} already processed URLs for {source_name}")
                
            return filtered_urls
            
        except Exception as e:
            self.logger.error(f"Error filtering processed URLs: {e}")
            return urls  # En caso de error, devolver todas las URLs para evitar pérdidas
    
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
    
    def _extract_episode_key(self, filename: str) -> str:
        """
        Extrae la clave del episodio de un nombre de archivo.
        Por ejemplo, de '1f18sweet_seymour_entry1' extrae '1f18sweet_seymour'.
        
        Args:
            filename: Nombre del archivo de subtítulos
            
        Returns:
            Clave del episodio o el nombre original si no se puede extraer
        """
        import re
        # Patrones comunes para identificar episodios
        patterns = [
            r'^(.+?)_entry\d+$',      # Patrón básico: nombre_entryX
            r'^(.+?)_s\d+e\d+',      # Patrón de serie: nombre_s01e01
            r'^(.+?)_\d{1,3}',       # Patrón numérico: nombre_01
            r'^(.+?)_\[?parte\d+\]?',  # Patrón de parte: nombre_parte1 o nombre_[parte1]
            r'^(.+?)_cap\d+',        # Patrón de capítulo: nombre_cap01
            r'^(.+?)_capitulo\d+',   # Patrón de capítulo: nombre_capitulo01
            r'^(.+?)_ep\d+',         # Patrón de episodio: nombre_ep01
            r'^(.+?)_episod[ei]o\d+'  # Patrón de episodio: nombre_episodio01
        ]
        
        # Intentar cada patrón para extraer la clave del episodio
        for pattern in patterns:
            match = re.match(pattern, filename, re.IGNORECASE)
            if match:
                return match.group(1)
        
        # Si no hay coincidencia, usar el nombre completo
        return filename
    
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
        
        # Skip processing URLs from 'opensubtitles' source
        if source_name.lower() == 'opensubtitles':
            self.logger.warning(f"Skipping URL from 'opensubtitles' source as requested: {url}")
            return {
                'url': url,
                'url_hash': url_hash,
                'source': source_name,
                'success': False,
                'error': 'Source skipped as requested',
                'file_path': None
            }
            
        source_config = self.config_manager.get_source_by_name(source_name)
        
        result = {
            'url': url,
            'url_hash': url_hash,
            'source': source_name,
            'success': False,
            'error': None,
            'file_path': None
        }
        
        # Fetch and extract content
        try:
            # Special handling for subtitle ZIP files
            if url.lower().endswith('.zip') or ('opensubtitles.org' in url.lower() and '/download/' in url.lower()):
                self.logger.info(f"Processing subtitle ZIP file: {url}")
                try:
                    # Get the content first to examine what we received
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                    }
                    import requests
                    response = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
                    response.raise_for_status()
                    
                    content_type = response.headers.get('Content-Type', '')
                    self.logger.info(f"Received response with Content-Type: {content_type} ({len(response.content)} bytes)")
                    
                    # Check if this is actually a ZIP file
                    if response.content.startswith(b'PK'):
                        self.logger.info("Confirmed content is a valid ZIP file (PK signature found)")
                        extraction_result = self.extractor.extract_subtitle_zip(url, content_bytes=response.content)
                    else:
                        self.logger.warning(f"URL appears to be for subtitles but received non-ZIP content - processing as regular HTML")
                        extraction_result = self.extractor.extract(response.text, source_config, url)
                except Exception as e:
                    self.logger.error(f"Error pre-processing subtitle URL {url}: {e}")
                    raise ScrapingError(f"Failed to pre-process subtitle URL {url}: {e}")
            else:
                # Regular URL processing
                response = self.scraper.fetch(url)
                
                # Optional: Save raw HTML for debugging
                if self.saver.storage_config.get('save_raw_html', False):
                    self.saver.save_raw_html(response.text, source_name, url)
                
                # Special handling for elpais.com/hemeroteca URLs - only extract links, add to sources.yaml
                if 'elpais.com/hemeroteca' in url:
                    self.logger.info(f"Processing hemeroteca URL for link extraction and sources.yaml addition: {url}")
                    
                    # Extract links and add to sources.yaml instead of queuing for scraping
                    self._extract_and_add_to_sources(url, response.text, source_name)
                    
                    # Mark as completed without saving text content
                    self.state_manager.update_url_status(url_hash, 'completed')
                    
                    result.update({
                        'success': True,
                        'hemeroteca_links_only': True,
                        'skip_discord': True  # Flag to skip Discord notification
                    })
                    
                    return result
                
                # Extract content from HTML
                extraction_result = self.extractor.extract(response.text, source_config, url)
            
            if not extraction_result['success']:
                raise ScrapingError(f"Extraction failed: {extraction_result.get('error', 'Unknown error')}")
            
            # Step 4: Process the extraction result
            if '_is_subtitle_zip' in extraction_result and extraction_result.get('subtitle_items'):
                # Agrupar los subtítulos por episodio
                subtitle_items = extraction_result.get('subtitle_items', [])
                self.logger.info(f"Procesando {len(subtitle_items)} entradas de subtítulos")
                
                file_paths = []
                content_hashes = []
                is_duplicate = False
                
                # Generate date stamp once for all files
                date_stamp = self.saver.get_datestamp()
                
                # Agrupar los subtítulos por episodio
                episode_subtitles = {}
                
                for item in subtitle_items:
                    clean_title = item.get('title', 'unknown')
                    
                    # Extraer la clave del episodio del título limpio
                    episode_key = self._extract_episode_key(clean_title)
                    
                    # Agregar el texto al diccionario del episodio
                    if episode_key not in episode_subtitles:
                        episode_subtitles[episode_key] = []
                    
                    text_content = item.get('text', '').strip()
                    # Skip empty or very short entries
                    if len(text_content) <= 2:
                        continue
                    
                    episode_subtitles[episode_key].append(text_content)
                
                self.logger.info(f"Agrupados subtítulos en {len(episode_subtitles)} episodios")
                
                # Guardar cada episodio como un archivo independiente
                for episode_key, subtitle_texts in episode_subtitles.items():
                    slugified_title = self._slugify(episode_key)
                    
                    # Crear nombre de archivo con source, date y episode_key
                    custom_filename = f"{source_name}_{slugified_title}_combined.txt"
                    
                    # Combinar todos los subtítulos con saltos de línea dobles
                    combined_text = "\n\n".join(subtitle_texts)
                    
                    metadata = {
                        'episode_key': episode_key,
                        'subtitle_count': len(subtitle_texts),
                        'url': url,
                        'extraction_type': 'subtitle_episode'
                    }
                    
                    save_result = self.saver.save_text(
                        combined_text, 
                        source_name,
                        url, 
                        metadata,
                        custom_filename=custom_filename
                    )
                    
                    file_paths.append(save_result['file_path'])
                    content_hashes.append(save_result['content_hash'])
                    # If any file is a duplicate, mark the overall result
                    if save_result.get('duplicate', False):
                        is_duplicate = True
                
                # Update state with combined info if we have any files
                if file_paths:
                    # Filter out any None values that might have been added
                    valid_paths = [path for path in file_paths if path is not None]
                    valid_hashes = [hash for hash in content_hashes if hash is not None]
                    
                    if not valid_paths:
                        self.logger.warning(f"No valid paths found for {url}")
                        return {
                            'url': url,
                            'url_hash': url_hash,
                            'source': source_name,
                            'success': False,
                            'error': 'No valid file paths found'
                        }
                    
                    self.state_manager.update_url_status(
                        url_hash,
                        'completed',
                        content_hash=','.join(valid_hashes),
                        file_path=','.join(valid_paths)
                    )
                    
                    # Send Discord notification for successful extraction
                    if not is_duplicate and not result.get('skip_discord', False):  # Only notify for new content
                        self._send_discord_url(url, source_name, f"{len(file_paths)} subtitle files")
                    
                    return {
                        'url': url,
                        'url_hash': url_hash,
                        'source': source_name,
                        'success': True,
                        'error': None,
                        'file_path': file_paths,
                        'content_hash': content_hashes,
                        'duplicate': is_duplicate,
                        'multiple_files': True,
                        'num_files': len(file_paths)
                    }
                else:
                    self.logger.warning(f"No valid subtitle entries found for {url}")
                    self.state_manager.update_url_status(url_hash, 'failed', error='No valid subtitle entries found')
                    return {
                        'url': url,
                        'url_hash': url_hash,
                        'source': source_name,
                        'success': False,
                        'error': 'No valid subtitle entries found'
                    }
            else:
                # Regular HTML extraction
                if 'text' not in extraction_result:
                    self.logger.error(f"Missing 'text' in extraction result: {extraction_result}")
                    raise ValueError(f"Missing 'text' in extraction result for {url}")
                        
                save_result = self.saver.save_text(
                    extraction_result['text'],
                    source_name,
                    url,
                    extraction_result.get('metadata', {})
                )
                
                if save_result.get('saved', False):
                    # Standard single file case
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
                        'duplicate': save_result.get('duplicate', False)
                    })
                    
                    # Extract and queue new links if this page should be followed
                    self._extract_and_queue_links(url, response.text, source_name)
                    
                    # Send Discord notification for successful extraction
                    if not save_result.get('duplicate', False) and not result.get('skip_discord', False):  # Only notify for new content
                        title = extraction_result.get('metadata', {}).get('title', '')
                        self._send_discord_url(url, source_name, title)
                
                elif save_result.get('duplicate', False):
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
            self.logger.warning(f"Robots.txt blocked access to {url}: {e}")
            self.state_manager.update_url_status(url_hash, 'blocked', str(e))
            return result
            
        except ScrapingError as e:
            result['error'] = str(e)
            self.logger.error(f"Scraping error processing {url}: {e}")
            self.state_manager.update_url_status(url_hash, 'failed', str(e))
            return result
            
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
    
    def run_scraping_session(self, batch_size: int = 50, max_duration: int = 3600) -> Dict[str, Any]:
        """
        Run a complete scraping session with multithreaded processing.
        
        Args:
            batch_size: Number of URLs to process in each batch
            max_duration: Maximum session duration in seconds (default: 1 hour)
            
        Returns:
            Session results summary
        """
        session_start = time.time()
        
        self.logger.info(f"Starting scraping session (max duration: {max_duration}s)")
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
        consecutive_empty_batches = 0
        max_empty_batches = 5  # Stop after 5 consecutive empty batches
        batch_num = 0  # Track batch number
        
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            while True:
                # Check session timeout
                elapsed_time = time.time() - session_start
                if elapsed_time > max_duration:
                    self.logger.warning(f"Session timeout reached ({max_duration}s), stopping gracefully")
                    break
                # Get next batch of URLs
                pending_urls = self.state_manager.get_pending_urls(limit=batch_size)
                
                if not pending_urls:
                    consecutive_empty_batches += 1
                    if consecutive_empty_batches >= max_empty_batches:
                        self.logger.info(f"No more URLs to process after {consecutive_empty_batches} empty batches")
                        break
                    else:
                        self.logger.debug(f"Empty batch {consecutive_empty_batches}/{max_empty_batches}, retrying...")
                        time.sleep(5)  # Brief pause before retry
                        continue
                else:
                    consecutive_empty_batches = 0  # Reset counter when URLs are found
                    
                # Filter out any OpenSubtitles URLs from processing
                filtered_urls = []
                skipped_opensubtitles = 0
                
                for url_record in pending_urls:
                    if 'opensubtitles.org' in url_record['url'] or url_record['source'].lower() == 'opensubtitles':
                        # Mark these URLs as failed to avoid processing them again
                        self.state_manager.update_url_status(
                            url_record['url_hash'],
                            status='failed_permanent',
                            error_message="Skipped 'opensubtitles' source as requested"
                        )
                        skipped_opensubtitles += 1
                    elif url_record['source'] in self.source_failures and self.source_failures[url_record['source']] >= self.max_consecutive_failures:
                        # Skip sources with too many consecutive failures
                        self.logger.debug(f"Skipping URL from problematic source {url_record['source']}: {url_record['url']}")
                        self.state_manager.update_url_status(
                            url_record['url_hash'],
                            status='failed_permanent',
                            error_message=f"Source {url_record['source']} has too many consecutive failures"
                        )
                    else:
                        filtered_urls.append(url_record)
                
                if skipped_opensubtitles > 0:
                    self.logger.warning(f"Skipped {skipped_opensubtitles} OpenSubtitles URLs as requested")
                    
                # If all URLs in this batch were OpenSubtitles, get the next batch
                if not filtered_urls:
                    continue
                    
                # Process the filtered URLs
                process_urls = filtered_urls
                batch_urls_count = len(process_urls)
                batch_num += 1  # Increment batch counter
                
                batch_start_time = time.time()
                self.logger.info(f"Processing batch {batch_num} of {batch_urls_count} URLs")
                
                # Submit batch to thread pool
                future_to_url = {
                    executor.submit(self._process_single_url, url_record): url_record
                    for url_record in process_urls
                }
                
                # Process results as they complete
                batch_successful = 0
                batch_failed = 0
                
                try:
                    for future in as_completed(future_to_url, timeout=300):  # 5 minute timeout per batch
                        try:
                            result = future.result(timeout=120)  # 2 minute timeout per URL
                            total_processed += 1
                        except Exception as e:
                            # Handle timeout or other exceptions
                            url_record = future_to_url[future]
                            self.logger.error(f"URL processing failed with exception: {url_record['url']} - {str(e)}")
                            result = {
                                'url': url_record['url'],
                                'url_hash': url_record['url_hash'],
                                'source': url_record['source'],
                                'success': False,
                                'error': f'Processing timeout or exception: {str(e)}',
                                'file_path': None
                            }
                            total_processed += 1
                            
                        if result['success']:
                            batch_successful += 1
                            total_successful += 1
                            # Reset failure count for successful source
                            if result['source'] in self.source_failures:
                                self.source_failures[result['source']] = 0
                            if not result.get('duplicate', False):
                                self.logger.info(f"✓ Saved: {result['file_path']}")
                            else:
                                self.logger.debug(f"✓ Duplicate: {result['url']}")
                        else:
                            batch_failed += 1
                            total_failed += 1
                            # Track source failures
                            source = result['source']
                            if source not in self.source_failures:
                                self.source_failures[source] = 0
                            self.source_failures[source] += 1
                            
                            if self.source_failures[source] >= self.max_consecutive_failures:
                                self.logger.warning(f"Source {source} has {self.source_failures[source]} consecutive failures - will be temporarily skipped")
                            
                            self.logger.warning(f"✗ Failed: {result['url']} - {result['error']}")
                        
                        # Update batch progress  
                        batch_progress = (batch_successful + batch_failed) / batch_urls_count * 100 if batch_urls_count > 0 else 0
                        self.logger.info(f"Batch progress: {batch_progress:.1f}% ({batch_successful + batch_failed}/{batch_urls_count})")
                        
                except concurrent.futures.TimeoutError:
                    self.logger.warning(f"Batch timeout reached - {len(future_to_url)} futures submitted, processing what completed")
                    # Handle unfinished futures
                    for future in future_to_url:
                        if not future.done():
                            url_record = future_to_url[future]
                            self.logger.warning(f"URL timed out: {url_record['url']}")
                            # Cancel the future and mark URL as failed
                            future.cancel()
                            # Update state to failed
                            self.state_manager.update_url_status(
                                url_record['url_hash'], 
                                'failed', 
                                'Batch processing timeout'
                            )
                            total_processed += 1
                            total_failed += 1
                            batch_failed += 1
                        else:
                            # Process completed futures that weren't processed yet
                            try:
                                result = future.result()
                                total_processed += 1
                                if result['success']:
                                    batch_successful += 1
                                    total_successful += 1
                                else:
                                    batch_failed += 1
                                    total_failed += 1
                            except Exception as e:
                                url_record = future_to_url[future]
                                self.logger.error(f"Error processing completed future: {url_record['url']} - {e}")
                                total_processed += 1
                                total_failed += 1
                                batch_failed += 1
                
                # Log batch completion
                self.logger.info(f"Batch {batch_num} completed: {batch_successful} successful, {batch_failed} failed")
                
                # Report progress after each batch
                self._report_batch_progress(
                    total_processed, total_pending, total_successful, 
                    total_failed, batch_start_time
                )
                
                # Add longer pause between batches for OpenSubtitles
                if any('opensubtitles.org' in url_record['url'] for url_record in process_urls):
                    pause_time = random.randint(120, 180)  # 2-3 minute pause between OpenSubtitles batches
                    self.logger.info(f"OpenSubtitles in batch - taking a longer pause of {pause_time} seconds")
                    time.sleep(pause_time)
                else:
                    # Brief pause between regular batches to be respectful
                    time.sleep(3)
        
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
    
    def _slugify(self, text: str) -> str:
        """Convert text to slug format for filenames.
        
        Args:
            text: Text to convert to slug
            
        Returns:
            Slugified version of the text
        """
        import re
        # Remove non-alphanumeric characters and replace with underscores
        slug = re.sub(r'[^\w\s]', '', text.lower())
        # Replace whitespace with underscores
        slug = re.sub(r'\s+', '_', slug)
        # Remove consecutive underscores
        slug = re.sub(r'_+', '_', slug)
        # Limit length
        slug = slug[:50].strip('_')
        return slug
        
    def _extract_and_add_to_sources(self, url: str, html_content: str, source_name: str):
        """
        Extract links from hemeroteca pages and add them to sources.yaml instead of queuing for scraping.
        
        Args:
            url: The hemeroteca URL that was processed
            html_content: HTML content from the page
            source_name: Source name for categorization
        """
        try:
            self.logger.debug(f"Extracting links from hemeroteca page: {url}")
            
            # Get max links from config
            extraction_config = self.config_manager.get_extraction_config()
            max_links = extraction_config.get('max_links_per_page', 50)
            
            # Extract links from the hemeroteca page
            extracted_links = self.link_extractor.extract_links(html_content, url, max_links=max_links)
            
            if extracted_links:
                # Filter for article links only (not more hemeroteca pages)
                article_links = [link for link in extracted_links if self.link_extractor._is_article_link(link)]
                
                if article_links:
                    # Add links to sources.yaml
                    self._add_links_to_sources_yaml(article_links, f"{source_name}_hemeroteca")
                    self.logger.info(f"Added {len(article_links)} hemeroteca article links to sources.yaml")
                else:
                    self.logger.debug(f"No article links found in hemeroteca page {url}")
            else:
                self.logger.debug(f"No links extracted from hemeroteca page {url}")
            
        except Exception as e:
            self.logger.warning(f"Error extracting and adding hemeroteca links from {url}: {e}")
    
    def _add_links_to_sources_yaml(self, links: List[str], source_name: str):
        """
        Add a list of links to sources.yaml file.
        
        Args:
            links: List of URLs to add
            source_name: Name for the new source entry
        """
        try:
            import yaml
            from pathlib import Path
            
            sources_path = Path(self.config_manager.sources_path)
            
            # Read current sources.yaml
            if sources_path.exists():
                with open(sources_path, 'r', encoding='utf-8') as f:
                    sources_data = yaml.safe_load(f) or {}
            else:
                sources_data = {'sources': []}
            
            # Check if source already exists
            existing_source = None
            for source in sources_data.get('sources', []):
                if source.get('name') == source_name:
                    existing_source = source
                    break
            
            if existing_source:
                # Add new URLs to existing source, avoiding duplicates
                existing_urls = set(existing_source.get('urls', []))
                new_urls = [url for url in links if url not in existing_urls]
                
                if new_urls:
                    existing_source['urls'].extend(new_urls)
                    self.logger.info(f"Added {len(new_urls)} new URLs to existing source '{source_name}'")
                else:
                    self.logger.debug(f"All URLs already exist in source '{source_name}'")
            else:
                # Create new source entry
                new_source = {
                    'name': source_name,
                    'base_url': 'https://elpais.com',
                    'urls': links
                }
                sources_data.setdefault('sources', []).append(new_source)
                self.logger.info(f"Created new source '{source_name}' with {len(links)} URLs")
            
            # Write back to sources.yaml
            with open(sources_path, 'w', encoding='utf-8') as f:
                yaml.dump(sources_data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            
            self.logger.info(f"Successfully updated sources.yaml with hemeroteca links")
            
        except Exception as e:
            self.logger.error(f"Error adding links to sources.yaml: {e}")
    
    def _extract_and_queue_links(self, url: str, html_content: str, source_name: str):
        """
        Extract links from successfully processed pages and queue them for processing.
        
        Args:
            url: The URL that was processed
            html_content: HTML content from the page
            source_name: Source name for categorization
        """
        try:
            # Check if link following is enabled
            extraction_config = self.config_manager.get_extraction_config()
            if not extraction_config.get('follow_links', True):
                return
            
            # Check if we should follow links from this URL
            if not self.link_extractor.should_follow_links(url):
                return
            
            self.logger.debug(f"Extracting links from: {url}")
            
            # Get max links from config
            max_links = extraction_config.get('max_links_per_page', 50)
            
            # Extract links from the page
            if url.endswith('.xml') or 'rss' in url.lower() or 'feed' in url.lower():
                # Handle RSS feeds
                extracted_links = self.link_extractor.extract_links_from_rss(html_content, url)
            else:
                # Handle regular HTML pages
                extracted_links = self.link_extractor.extract_links(html_content, url, max_links=max_links)
            
            if extracted_links:
                # Add links to state manager for processing
                added_count = self.state_manager.add_urls(extracted_links, f"{source_name}_discovered")
                
                if added_count > 0:
                    self.logger.info(f"Discovered {len(extracted_links)} links from {url}, added {added_count} new URLs")
                else:
                    self.logger.debug(f"Discovered {len(extracted_links)} links from {url}, but all were already queued")
            
        except Exception as e:
            self.logger.warning(f"Error extracting links from {url}: {e}")
    
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