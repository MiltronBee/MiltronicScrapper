"""
Link extraction and discovery component for expanding corpus collection.
Extracts and filters internal links from web pages to discover more content.
"""

import re
import logging
from typing import Set, List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse, urlunparse
from bs4 import BeautifulSoup
from datetime import datetime


class LinkExtractor:
    """
    Intelligent link extraction for discovering article and content URLs.
    Specializes in news sites, archives, and content aggregation pages.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Patterns for identifying article/content URLs
        self.article_patterns = [
            r'/articul[eo]s?/',
            r'/noticia[s]?/',
            r'/news/',
            r'/content/',
            r'/post[s]?/',
            r'/entrada[s]?/',
            r'/blog/',
            r'/opinion/',
            r'/editorial/',
            r'/reportaje[s]?/',
            r'/cronica[s]?/',
            r'/especial[es]?/',
            r'/investigacion/',
            r'/analisis/',
            r'/columna[s]?/',
            r'/\d{4}/\d{2}/\d{2}/',  # Date-based URLs
            r'/\d{4}/\d{1,2}/',     # Year/month URLs
            r'\.html$',
            r'\.htm$',
        ]
        
        # Patterns for archive/listing pages (hemeroteca, etc.)
        self.archive_patterns = [
            r'/hemeroteca',
            r'/archivo',
            r'/archive',
            r'/historico',
            r'/edicion',
            r'/ediciones',
            r'/portada[s]?',
            r'/anteriores',
            r'/pasadas',
            r'/categoria[s]?/',
            r'/seccion[es]?/',
            r'/tag[s]?/',
            r'/tema[s]?/',
        ]
        
        # Spanish news sites that we want to prioritize
        self.priority_domains = {
            'elpais.com',
            'eluniversal.com.mx',
            'milenio.com',
            'proceso.com.mx',
            'sinembargo.mx',
            'animalpolitico.com',
            'nexos.com.mx',
            'letraslibres.com',
            'jornada.com.mx',
            'excelsior.com.mx',
            'reforma.com',
            'eleconomista.com.mx',
            'forbes.com.mx',
            'expansion.mx',
            'vanguardia.com.mx',
            'heraldo.mx',
            'grupometropoli.net',
        }
        
        # Patterns to exclude (ads, social, etc.)
        self.exclude_patterns = [
            r'/publicidad',
            r'/anuncio[s]?',
            r'/banner[s]?',
            r'/widget[s]?',
            r'/social',
            r'/share',
            r'/compartir',
            r'/facebook',
            r'/twitter',
            r'/instagram',
            r'/youtube',
            r'/linkedin',
            r'/whatsapp',
            r'/telegram',
            r'/rss',
            r'/feed',
            r'/sitemap',
            r'\.xml$',
            r'\.json$',
            r'\.js$',
            r'\.css$',
            r'\.jpg$',
            r'\.jpeg$',
            r'\.png$',
            r'\.gif$',
            r'\.pdf$',
            r'\.doc[x]?$',
            r'\.xls[x]?$',
            r'/admin',
            r'/login',
            r'/registro',
            r'/suscripc',
            r'/newsletter',
            r'/contacto',
            r'/acerca',
            r'/about',
            r'/legal',
            r'/privacidad',
            r'/terminos',
            r'/cookies',
        ]
    
    def extract_links(self, html_content: str, base_url: str, max_links: int = 100) -> List[str]:
        """
        Extract relevant links from HTML content.
        
        Args:
            html_content: HTML content to parse
            base_url: Base URL for resolving relative links
            max_links: Maximum number of links to return
            
        Returns:
            List of absolute URLs
        """
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            base_domain = urlparse(base_url).netloc.lower()
            
            # Remove script and style elements
            for element in soup(['script', 'style', 'nav', 'footer', 'header']):
                element.decompose()
            
            # Find all links
            links = set()
            for link in soup.find_all('a', href=True):
                href = link['href'].strip()
                if not href or href.startswith('#'):
                    continue
                
                # Convert to absolute URL
                absolute_url = urljoin(base_url, href)
                
                # Filter and validate
                if self._is_valid_link(absolute_url, base_domain):
                    links.add(absolute_url)
                    
                    if len(links) >= max_links:
                        break
            
            # Prioritize article links over archive links
            article_links = []
            archive_links = []
            other_links = []
            
            for link in links:
                if self._is_article_link(link):
                    article_links.append(link)
                elif self._is_archive_link(link):
                    archive_links.append(link)
                else:
                    other_links.append(link)
            
            # Return prioritized list
            result = article_links + archive_links + other_links
            return result[:max_links]
            
        except Exception as e:
            self.logger.error(f"Error extracting links from {base_url}: {e}")
            return []
    
    def _is_valid_link(self, url: str, base_domain: str) -> bool:
        """Check if a link is valid for extraction."""
        try:
            parsed = urlparse(url)
            
            # Must have valid scheme and netloc
            if not parsed.scheme or not parsed.netloc:
                return False
            
            # Must be HTTP/HTTPS
            if parsed.scheme not in ('http', 'https'):
                return False
            
            # Must be same domain or priority domain
            link_domain = parsed.netloc.lower()
            if link_domain != base_domain and link_domain not in self.priority_domains:
                return False
            
            # Check exclusion patterns
            full_path = parsed.path + (parsed.query or '') + (parsed.fragment or '')
            for pattern in self.exclude_patterns:
                if re.search(pattern, full_path, re.IGNORECASE):
                    return False
            
            # Must have some path content
            if len(parsed.path) < 2:
                return False
            
            return True
            
        except Exception:
            return False
    
    def _is_article_link(self, url: str) -> bool:
        """Check if URL looks like an article/content link."""
        try:
            parsed = urlparse(url)
            full_path = parsed.path + (parsed.query or '')
            
            # First check if it's an archive/listing page - exclude those
            if self._is_archive_link(url):
                return False
            
            for pattern in self.article_patterns:
                if re.search(pattern, full_path, re.IGNORECASE):
                    return True
            
            return False
            
        except Exception:
            return False
    
    def _is_archive_link(self, url: str) -> bool:
        """Check if URL looks like an archive/listing link."""
        try:
            parsed = urlparse(url)
            full_path = parsed.path + (parsed.query or '')
            
            for pattern in self.archive_patterns:
                if re.search(pattern, full_path, re.IGNORECASE):
                    return True
            
            return False
            
        except Exception:
            return False
    
    def extract_links_from_rss(self, rss_content: str, base_url: str) -> List[str]:
        """
        Extract article links from RSS feed content.
        
        Args:
            rss_content: RSS XML content
            base_url: Base URL for the RSS feed
            
        Returns:
            List of article URLs from RSS entries
        """
        try:
            soup = BeautifulSoup(rss_content, 'xml')
            links = set()
            
            # Look for RSS items
            for item in soup.find_all(['item', 'entry']):
                # Try different link elements
                link_elem = item.find('link')
                if link_elem:
                    if link_elem.get('href'):
                        href = link_elem['href']
                    else:
                        href = link_elem.get_text(strip=True)
                    
                    if href:
                        absolute_url = urljoin(base_url, href)
                        if self._is_valid_link(absolute_url, urlparse(base_url).netloc):
                            links.add(absolute_url)
            
            return list(links)
            
        except Exception as e:
            self.logger.error(f"Error extracting RSS links from {base_url}: {e}")
            return []
    
    def should_follow_links(self, url: str) -> bool:
        """
        Determine if we should extract links from this URL.
        
        Args:
            url: URL to check
            
        Returns:
            True if we should extract links from this page
        """
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            path = parsed.path.lower()
            
            # Always follow links from priority domains
            if domain in self.priority_domains:
                return True
            
            # Follow links from archive/listing pages
            if self._is_archive_link(url):
                return True
            
            # Follow links from RSS feeds
            if 'rss' in path or 'feed' in path or url.endswith('.xml'):
                return True
            
            # Follow links from main pages
            if path in ('/', '/index.html', '/index.htm', '/inicio', '/home'):
                return True
            
            return False
            
        except Exception:
            return False