"""
Specialized lyrics scraper for Letras.com.
Extracts song lyrics from artists on Letras.com with pagination support.
"""

import logging
import re
import requests
import time
import unicodedata
from typing import Dict, List, Any, Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse


class LetrasScraper:
    """
    Specialized scraper for Letras.com that navigates through artist pages
    and extracts lyrics from individual song pages.
    """
    
    def __init__(self, scraper=None):
        """Initialize with an optional scraper instance from the main framework."""
        self.logger = logging.getLogger(__name__)
        self.scraper = scraper
        self.base_url = "https://www.letras.com"
        
    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch a webpage and return a BeautifulSoup object.
        
        Args:
            url: URL to fetch
            
        Returns:
            BeautifulSoup object or None if fetch failed
        """
        try:
            if self.scraper:
                # Use the framework's scraper
                response = self.scraper.fetch(url)
                # Explicitly force UTF-8 encoding to prevent encoding issues
                response.encoding = 'utf-8'
                html_content = response.text
                
                # Log the encoding and response size for debugging
                self.logger.debug(f"Got HTML response: {len(html_content)} bytes, encoding: {response.encoding}")
                
                return html_content, {}
            else:
                # Fallback to direct requests if no scraper provided
                try:
                    # Define headers para simular un navegador real
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
                        'Accept-Encoding': 'gzip, deflate',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                        'Cache-Control': 'max-age=0'
                    }
                    
                    response = requests.get(url, headers=headers, timeout=30)
                    response.raise_for_status()
                    # Explicitly force UTF-8 encoding to prevent encoding issues
                    response.encoding = 'utf-8'
                    html_content = response.text
                    
                    # Log the encoding and response size for debugging
                    self.logger.debug(f"Got HTML response: {len(html_content)} bytes, encoding: {response.encoding}")
                    
                    # Verificar que obtuvimos un html válido y no un bloqueo o error
                    if len(html_content) < 1000 or 'captcha' in html_content.lower():
                        self.logger.warning(f"Posible bloqueo o respuesta inválida: {len(html_content)} bytes")
                    else:
                        self.logger.debug(f"Respuesta HTML parece válida: {len(html_content)} bytes")
                    
                    
                    return html_content, {}
                except Exception as e:
                    self.logger.error(f"Error fetching page: {e}")
                    raise Exception(f"Error fetching page: {e}")
            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            return soup
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return None
    
    def get_artists_from_page(self, url: str) -> List[Dict[str, str]]:
        """
        Extract artist links from a genre or listing page.
        
        Args:
            url: URL of the artists listing page
            
        Returns:
            List of dictionaries with artist info (name and url)
        """
        artists = []
        soup = None
        
        # First, try regular HTTP fetch
        self.logger.info(f"Fetching page: {url}")
        soup = self.fetch_page(url)
        
        # If Playwright is available, try with JavaScript rendering as fallback
        if not soup or not self._has_artist_links(soup):
            try:
                import requests
                from playwright.sync_api import sync_playwright
                
                self.logger.info(f"Regular fetch didn't find artists, trying with Playwright: {url}")
                try:
                    with sync_playwright() as playwright:
                        browser = playwright.chromium.launch(headless=True)
                        page = browser.new_page()
                        page.goto(url, wait_until='networkidle', timeout=60000)
                        html_content = page.content()
                        soup = BeautifulSoup(html_content, 'html.parser')
                        browser.close()
                except Exception as e:
                    self.logger.warning(f"Playwright navigation failed: {e}")
            except ImportError:
                self.logger.warning("Playwright not available. To install, run: pip install playwright && playwright install")
        
        if not soup:
            self.logger.error(f"Failed to fetch artists from {url}")
            return artists
        
        # Try direct extraction first with the URL we have
        artists = self._extract_artists_from_soup(soup, url)
        
        # If that didn't work and we're on a genre page, try to find and follow links to artist listing pages
        if not artists and '/estilos/' in url:
            self.logger.info("No artists found directly. This appears to be a genre page. Looking for artist listing links.")
            for a_tag in soup.find_all('a', href=True):
                href = a_tag.get('href', '')
                if 'artistas' in href.lower() or 'artists' in href.lower():
                    sub_url = urljoin(self.base_url, href)
                    self.logger.info(f"Following potential artist listing link: {sub_url}")
                    sub_soup = self.fetch_page(sub_url)
                    if sub_soup:
                        sub_artists = self._extract_artists_from_soup(sub_soup, sub_url)
                        artists.extend(sub_artists)
                        if sub_artists:
                            break  # Stop if we found artists on a sub-page
        
        # If still no artists, try fallback approach with hard-coded URLs
        if not artists:
            self.logger.warning(f"No artists found through normal means. Using fallback hard-coded artists for corridos")
            # Add a few popular corrido artists as fallback
            fallback_artists = [
                {'name': 'Chalino Sánchez', 'url': 'https://www.letras.com/chalino-sanchez/'},
                {'name': 'Peso Pluma', 'url': 'https://www.letras.com/peso-pluma/'},
                {'name': 'Junior H', 'url': 'https://www.letras.com/junior-h/'},
                {'name': 'Natanael Cano', 'url': 'https://www.letras.com/natanael-cano/'}
            ]
            artists.extend(fallback_artists)
        
        self.logger.info(f"Extracted {len(artists)} artists from {url}")
        return artists
    
    def _has_artist_links(self, soup):
        """Check if the soup contains any potential artist links."""
        if not soup:
            return False
        
        # Check for various patterns that would indicate artist links
        artist_patterns = ['/artista/', '/artist/']
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if any(pattern in href for pattern in artist_patterns):
                return True
        return False
    
    def _extract_artists_from_soup(self, soup, url):
        """Extract artist information from BeautifulSoup object."""
        artists = []
        if not soup:
            return artists
            
        try:
            # Try multiple potential selectors that might contain artist links
            potential_selectors = [
                'a.artist-name', 
                'ul.cnt-list a', 
                '.artista-box a',
                '.cnt-listaArtistas a',
                '.lista-letras a',
                '.top50-pos a',
                '.cnt-top a',
                'a[href*="/artista/"]',
                'a[href*="/artist/"]'
            ]
            
            # Combine all found links
            artist_links = []
            for selector in potential_selectors:
                found_links = soup.select(selector)
                artist_links.extend(found_links)
                self.logger.debug(f"Found {len(found_links)} links with selector '{selector}'")
            
            # Search more broadly if specific selectors don't work
            if not artist_links:
                self.logger.info("No artists found with specific selectors, looking for any links containing '/artista/' or '/artist/'")
                all_links = soup.find_all('a', href=True)
                artist_links = [link for link in all_links if '/artista/' in link.get('href', '') or '/artist/' in link.get('href', '')]
            
            # Process all found links
            for link in artist_links:
                artist_name = link.get_text().strip()
                artist_url = urljoin(self.base_url, link.get('href', ''))
                
                if artist_name and artist_url and ("/artista/" in artist_url or "/artist/" in artist_url):
                    # Skip duplicates
                    if not any(artist['url'] == artist_url for artist in artists):
                        artists.append({
                            'name': artist_name,
                            'url': artist_url
                        })
                        self.logger.debug(f"Found artist: {artist_name} at {artist_url}")
            
            return artists
        except Exception as e:
            self.logger.error(f"Error extracting artists from {url}: {e}")
            return []
    
    def get_next_page_url(self, current_url: str, soup: BeautifulSoup) -> Optional[str]:
        """
        Find the next page URL from pagination controls.
        
        Args:
            current_url: Current page URL
            soup: BeautifulSoup object of the current page
            
        Returns:
            URL for the next page or None if not found
        """
        try:
            # Look for pagination links
            # This selector needs adjustment based on the actual HTML structure
            pagination_links = soup.select('.cnt-paginacao ul li a')
            
            for link in pagination_links:
                text = link.get_text().strip().lower()
                if "próximo" in text or "siguiente" in text or "next" in text or "›" in text:
                    next_url = link.get('href')
                    if next_url:
                        return urljoin(current_url, next_url)
            
            # Alternative approach: look for the current page number and try next one
            current_page_elem = soup.select_one('.cnt-paginacao ul li.current')
            if current_page_elem:
                current_page = int(current_page_elem.get_text().strip())
                next_page = current_page + 1
                
                # Try to find a template from other pagination links
                for link in pagination_links:
                    href = link.get('href', '')
                    page_match = re.search(r'/(\d+)/?$', href)
                    if page_match:
                        template = href.replace(page_match.group(1), '{page}')
                        next_url = template.format(page=next_page)
                        return urljoin(current_url, next_url)
            
            return None
        except Exception as e:
            self.logger.error(f"Error finding next page from {current_url}: {e}")
            return None
    
    def get_songs_from_artist_page(self, artist_url: str, visited_urls: set = None) -> List[Dict[str, str]]:
        """
        Extract song links from an artist page.
        
        Args:
            artist_url: URL of the artist page to scrape
            visited_urls: Set of already visited URLs to prevent infinite loops
            
        Returns:
            List of song dictionaries with title and url
        """
        # Initialize visited URLs set if not provided
        if visited_urls is None:
            visited_urls = set()
        
        # Check for infinite loop protection
        if artist_url in visited_urls:
            self.logger.warning(f"Cycle detected, skipping already visited URL: {artist_url}")
            return []
        
        # Add current URL to visited set
        visited_urls.add(artist_url)
        
        # Limit recursion depth
        if len(visited_urls) > 50:  # Maximum 50 pages per artist
            self.logger.warning(f"Maximum recursion depth reached for artist pages, stopping at {artist_url}")
            return []
        
        songs = []
        
        # Obtener el HTML y crear el objeto BeautifulSoup
        html_content, _ = self.fetch_page(artist_url)
        if not html_content:
            self.logger.error(f"Failed to fetch artist page {artist_url}")
            return songs
            
        # Crear el objeto BeautifulSoup
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
        except Exception as e:
            self.logger.error(f"Error parsing artist page HTML: {e}")
            return songs
        
        # First look for song rows with data attributes (current site structure)
        song_rows = soup.find_all('li', class_=lambda x: x and 'songList-table-row' in x and '--song' in x)
        self.logger.info(f"Found {len(song_rows)} potential song rows on {artist_url}")
        
        # Extract songs from data attributes (modern approach)
        for row in song_rows:
            try:
                # Extract data from attributes
                artist_dns = row.get('data-dns', '')
                song_url_part = row.get('data-url', '')
                song_name = row.get('data-name', '')
                
                if artist_dns and song_url_part:
                    full_url = f"https://www.letras.com/{artist_dns}/{song_url_part}/"
                    title = song_name if song_name else row.get_text().strip()
                    
                    # Skip duplicates
                    if title and not any(song['url'] == full_url for song in songs):
                        songs.append({
                            'title': title,
                            'url': full_url
                        })
                        self.logger.debug(f"Found song from data attributes: {title} at {full_url}")
            except Exception as e:
                self.logger.debug(f"Error extracting song from row: {e}")
                continue
        
        # If we found songs with the modern approach, we're done
        if songs:
            self.logger.info(f"Extracted {len(songs)} songs from {artist_url} using data attributes")
            return songs
        
        # Legacy approach - try multiple potential selectors for older pages
        self.logger.info(f"No songs found with data attributes on {artist_url}, trying legacy selectors")
        potential_selectors = [
            '.songList-table-songName',  # Current main selector for song links
            'ul.cnt-list li a',         # Legacy selector
            '.cnt-letras a',             # Alternative potential selector
            '.song-name',                # Alternative potential selector
            'a[href*="/cancion/"]',     # Fallback by URL pattern
            'a[href*="/traduccion/"]',   # Fallback by URL pattern
            '.top50-pos a',             # For top songs sections
            '.artista-todos a',          # Alternative artist page links
            '.artista-top a'             # Top songs by artist
        ]
        
        # Combine all found links from traditional selectors
        song_links = []
        for selector in potential_selectors:
            found_links = soup.select(selector)
            song_links.extend(found_links)
            self.logger.debug(f"Found {len(found_links)} song links with selector '{selector}'")
        
        # Process traditional link elements
        for link in song_links:
            song_title = link.get_text().strip()
            song_url = urljoin(self.base_url, link.get('href', ''))
            
            # Skip duplicates
            if song_title and song_url and not any(song['url'] == song_url for song in songs):
                songs.append({
                    'title': song_title,
                    'url': song_url
                })
                self.logger.debug(f"Found song from legacy selector: {song_title} at {song_url}")
        
        # If still no songs, look for any links that could be songs
        if not songs:
            artist_name = artist_url.split('/')[-2] if artist_url.endswith('/') else artist_url.split('/')[-1]
            self.logger.info(f"No songs found with specific selectors, looking for any links containing artist name")
            all_links = soup.find_all('a', href=True)
            
            for link in all_links:
                href = link.get('href', '')
                if f'/{artist_name}/' in href and ('/' in href.replace(f'/{artist_name}/', '')):
                    song_title = link.get_text().strip()
                    song_url = urljoin(self.base_url, href)
                    
                    # Skip duplicates
                    if song_title and not any(song['url'] == song_url for song in songs):
                        songs.append({
                            'title': song_title,
                            'url': song_url
                        })
                        self.logger.debug(f"Found song from generic link: {song_title} at {song_url}")
        
        self.logger.info(f"Extracted {len(songs)} songs from {artist_url}")
        
        # If no songs found, try looking for "all songs" page
        if not songs:
            # Look for links to "todas las canciones" (all songs) or similar
            all_songs_link = None
            for a_tag in soup.find_all('a', href=True):
                href = a_tag.get('href', '')
                text = a_tag.get_text().lower().strip()
                if ('todas' in text and 'canciones' in text) or 'todas+las+canciones' in href:
                    all_songs_link = urljoin(self.base_url, href)
                    break
            
            if all_songs_link:
                self.logger.info(f"Found 'all songs' page, trying: {all_songs_link}")
                html_content, _ = self.fetch_page(all_songs_link)
                if html_content:
                    # Recursively process this page with visited URLs tracking
                    return self.get_songs_from_artist_page(all_songs_link, visited_urls)
                else:
                    return []
        
        # Check for pagination
        next_page_url = self.get_next_page_url(artist_url, soup)
        if next_page_url:
            self.logger.info(f"Found next page for artist: {next_page_url}")
            # Add a small delay to be polite
            time.sleep(2)
            # Get songs from next page and combine with visited URLs tracking
            next_page_songs = self.get_songs_from_artist_page(next_page_url, visited_urls)
            songs.extend(next_page_songs)
        
        return songs
    
    def _count_sentences(self, text: str) -> int:
        """
        Estimated number of sentences
        """
        if not text:
            return 0
            
        # First split by common sentence-ending punctuation
        sentences = re.split(r'[.!?]+', text)
        
        # Also count paragraph breaks as potential sentence boundaries
        paragraph_count = text.count('\n\n')
        
        # Count non-empty sentences from punctuation splits
        punct_sentence_count = sum(1 for s in sentences if s.strip())
        
        # Return the larger of the two counts as our best estimate, minimum of 2
        return max(punct_sentence_count, paragraph_count + 1, 2)
    
    def extract_lyrics(self, song_url: str) -> Dict[str, Any]:
        """
        Extract lyrics and metadata from a song page.
        
        Args:
            song_url: URL of the song page
            
        Returns:
            Dictionary with lyrics and metadata
        """
        result = {
            'success': False,
            'lyrics': None,
            'metadata': {},
            'url': song_url
        }
        
        # Obtener el HTML y crear el objeto BeautifulSoup
        html_content, _ = self.fetch_page(song_url)
        if not html_content:
            result['error'] = f"Failed to fetch song page {song_url}"
            return result
            
        # Crear el objeto BeautifulSoup con el parser HTML correcto
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            self.logger.debug(f"Successfully parsed HTML into BeautifulSoup object")
        except Exception as e:
            self.logger.error(f"Error parsing HTML content: {e}")
            result['error'] = f"Failed to parse HTML from {song_url}: {e}"
            return result
            
        try:
            # Extract title
            title_elem = soup.select_one('h1')
            if title_elem:
                result['metadata']['title'] = title_elem.get_text().strip()
            
            # Extract artist with multiple selector options
            artist_selectors = [
                'div.cnt-head_title div.cnt-head_artistname a',  # New format
                'h2 a',                                        # Old format
                'div.header-section-title h2 a',               # Another variation
                'div.artist-name',                             # Yet another variation
                'div.cnt-head h2'                              # Fallback
            ]
            
            artist_name = None
            for selector in artist_selectors:
                artist_elem = soup.select_one(selector)
                if artist_elem:
                    artist_name = artist_elem.get_text().strip()
                    if artist_name:
                        result['metadata']['artist'] = artist_name
                        break
            
            # If we still don't have an artist name, try to extract it from the URL
            if not artist_name and 'artist' not in result['metadata']:
                # Extract from URL path: https://www.letras.com/natanael-cano/sin-ti/
                url_parts = song_url.strip('/').split('/')
                if len(url_parts) >= 5:  # Has enough path components
                    potential_artist = url_parts[-2].replace('-', ' ')
                    result['metadata']['artist'] = potential_artist.title()  # Capitalize words
                    self.logger.debug(f"Extracted artist '{potential_artist.title()}' from URL path")
            
            artist = result['metadata'].get('artist', 'Unknown')
            self.logger.debug(f"Artist: {artist}")

            
            # Extract lyrics with multiple selector options for new and old formats
            lyrics_div = (
                soup.select_one('div.lyric-original') or 
                soup.select_one('.lyric-original') or
                soup.select_one('div.cnt-letra') or
                soup.select_one('.letra') or
                soup.select_one('.lyric')
            )
            
            if lyrics_div:
                # Remove any unwanted elements like scripts, ads, etc.
                for unwanted in lyrics_div.select('script, style, .ads, .banner, .pub'):
                    unwanted.extract()
                
                # Process lyrics text
                lyrics_text = ''
                paragraphs = lyrics_div.select('p')
                
                if paragraphs:
                    for p in paragraphs:
                        paragraph_text = p.get_text(strip=True)
                        if paragraph_text:
                            # Ensure paragraphs are properly separated and end with period if missing
                            if paragraph_text and not paragraph_text[-1] in ['.', '!', '?']:
                                paragraph_text += '.'
                            lyrics_text += paragraph_text + '\n\n'
                else:
                    # If no paragraphs found, try getting text directly
                    raw_text = lyrics_div.get_text(separator='\n', strip=True)
                    
                    # Clean up the text - remove tab navigation elements
                    navigation_texts = ["Letra", "Traducción", "Significado"]
                    for nav_text in navigation_texts:
                        raw_text = raw_text.replace(nav_text, "")
                    
                    # Split into paragraphs
                    lines = []
                    for line in raw_text.split('\n'):
                        line = line.strip()
                        if line:
                            lines.append(line)
                    
                    # Group lines into paragraphs
                    for line in lines:
                        if line and not line[-1] in ['.', '!', '?']:
                            line += '.'
                        lyrics_text += line + '\n\n'
                
                # Clean up any excessive newlines
                lyrics_text = re.sub(r'\n{3,}', '\n\n', lyrics_text.strip())
                
                # Ensure proper UTF-8 encoding of the lyrics text
                try:
                    # If somehow we got bytes, decode them properly
                    if isinstance(lyrics_text, bytes):
                        lyrics_text = lyrics_text.decode('utf-8', errors='replace')
                    
                    # Apply Unicode normalization to composed form (NFC)
                    lyrics_text = unicodedata.normalize('NFC', lyrics_text)
                    
                    # Re-encode and decode to normalize and clean the text
                    lyrics_text = lyrics_text.encode('utf-8', errors='replace').decode('utf-8')
                    
                    # Log the encoding success for debugging
                    self.logger.debug(f"Successfully normalized encoding for lyrics text ({len(lyrics_text)} chars)")
                except Exception as e:
                    self.logger.warning(f"Encoding normalization error: {e}")
                
                result['lyrics'] = lyrics_text
                
                # Only mark as success if we actually have lyrics content
                result['success'] = bool(lyrics_text)
                
                # Add word and sentence count as metadata
                if result['success']:
                    words = lyrics_text.split()
                    sentences = self._count_sentences(lyrics_text)
                    result['metadata']['word_count'] = len(words)
                    result['metadata']['sentence_count'] = sentences
                    self.logger.info(f"Extracted {len(words)} words, {sentences} sentences from {song_url}")
            
            # Additional metadata (if available)
            album_elem = soup.select_one('.letra-info > a')
            if album_elem:
                result['metadata']['album'] = album_elem.get_text().strip()
            
            # Extract language (if available)
            lang_elem = soup.select_one('[data-language]')
            if lang_elem:
                result['metadata']['language'] = lang_elem.get('data-language')
            else:
                # Default to Spanish for Letras.com
                result['metadata']['language'] = 'es'
            
            # Track view count if available
            views_elem = soup.select_one('.cnt-info')
            if views_elem:
                views_text = views_elem.get_text()
                views_match = re.search(r'(\d[\d,\.]*)\s*views', views_text, re.IGNORECASE)
                if views_match:
                    result['metadata']['views'] = views_match.group(1)
                    
            if not result['success']:
                result['error'] = "No lyrics found on the page"
                    
            return result
        except Exception as e:
            self.logger.error(f"Error extracting lyrics from {song_url}: {e}")
            result['error'] = f"Extraction error: {str(e)}"
            return result
    
    def crawl_artist(self, artist_url: str) -> Dict[str, Any]:
        """
        Crawl an artist page and extract all songs' lyrics.
        
        Args:
            artist_url: URL of the artist page
            
        Returns:
            Dictionary with artist info and songs with lyrics
        """
        result = {
            'artist_url': artist_url,
            'artist_name': None,
            'song_count': 0,
            'successful_extractions': 0,
            'songs': []
        }
        
        try:
            # Get artist name from the URL or page
            soup = self.fetch_page(artist_url)
            if soup:
                artist_name_elem = soup.select_one('h1')
                if artist_name_elem:
                    result['artist_name'] = artist_name_elem.get_text().strip()
            
            # Extract all songs for the artist
            songs = self.get_songs_from_artist_page(artist_url)
            result['song_count'] = len(songs)
            
            # Extract lyrics for each song
            for song in songs:
                self.logger.info(f"Extracting lyrics for '{song['title']}'")
                
                # Be polite with delays between requests
                time.sleep(1.5)
                
                # Extract the lyrics
                extraction_result = self.extract_lyrics(song['url'])
                
                if extraction_result['success']:
                    result['successful_extractions'] += 1
                
                # Add song with lyrics to results
                song_data = {
                    'title': song['title'],
                    'url': song['url'],
                    'lyrics': extraction_result.get('lyrics'),
                    'metadata': extraction_result.get('metadata', {}),
                    'success': extraction_result['success']
                }
                
                result['songs'].append(song_data)
            
            self.logger.info(
                f"Artist '{result['artist_name']}': extracted {result['successful_extractions']} "
                f"out of {result['song_count']} songs"
            )
            return result
            
        except Exception as e:
            self.logger.error(f"Error crawling artist {artist_url}: {e}")
            result['error'] = str(e)
            return result
    
    def crawl_genre(self, genre_url: str, max_artists: int = None) -> Dict[str, Any]:
        """
        Crawl a genre page and extract lyrics from multiple artists.
        
        Args:
            genre_url: URL of the genre page
            max_artists: Maximum number of artists to process (None for all)
            
        Returns:
            Dictionary with genre info and artists with songs
        """
        result = {
            'genre_url': genre_url,
            'artist_count': 0,
            'song_count': 0,
            'artists': []
        }
        
        try:
            current_url = genre_url
            all_artists = []
            
            # Process the first page
            self.logger.info(f"Discovering artists from {current_url}")
            artists_batch = self.get_artists_from_page(current_url)
            all_artists.extend(artists_batch)
            
            # Continue with pagination until we hit the limit or run out of pages
            soup = self.fetch_page(current_url)
            while soup and (max_artists is None or len(all_artists) < max_artists):
                next_page = self.get_next_page_url(current_url, soup)
                if not next_page:
                    break
                    
                # Move to next page
                current_url = next_page
                self.logger.info(f"Discovering artists from {current_url}")
                
                # Be polite with delays between page requests
                time.sleep(2)
                
                soup = self.fetch_page(current_url)
                if not soup:
                    break
                    
                artists_batch = self.get_artists_from_page(current_url)
                all_artists.extend(artists_batch)
                
                # Check if we've hit the maximum
                if max_artists is not None and len(all_artists) >= max_artists:
                    all_artists = all_artists[:max_artists]
                    break
            
            # Limit to max_artists if specified
            if max_artists is not None and len(all_artists) > max_artists:
                all_artists = all_artists[:max_artists]
                
            result['artist_count'] = len(all_artists)
            self.logger.info(f"Discovered {result['artist_count']} artists from {genre_url}")
            
            # Process each artist
            for i, artist in enumerate(all_artists):
                self.logger.info(f"Processing artist {i+1}/{len(all_artists)}: {artist['name']}")
                
                # Be polite with delays between artist requests
                time.sleep(3)
                
                # Crawl the artist's songs
                artist_result = self.crawl_artist(artist['url'])
                
                result['song_count'] += artist_result['song_count']
                result['artists'].append(artist_result)
                
            return result
            
        except Exception as e:
            self.logger.error(f"Error crawling genre {genre_url}: {e}")
            result['error'] = str(e)
            return result
