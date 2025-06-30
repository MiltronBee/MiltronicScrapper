"""
Enhanced multi-layered content extraction engine with comment harvesting and dynamic recursion.
Extends the existing extractor with aggressive content discovery and comment extraction.
"""

import logging
import re
from typing import Dict, Optional, Any, List, Tuple
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import trafilatura
from html_sanitizer import Sanitizer
import fasttext
import spacy

from .exceptions import ExtractionFailedError, LanguageMismatchError, ContentTooShortError
from .geographic_filter import GeographicFilter
from .pdf_extractor import PDFExtractor
from .quality_analyzer import MexicanSpanishQualityAnalyzer
from .specialized_extractors import SpecializedMexicanExtractors


class EnhancedExtractor:
    """
    Enhanced data processing engine with comment extraction and dynamic recursion capabilities.
    Builds on the existing extractor with aggressive content harvesting features.
    """
    
    def __init__(self, extraction_config: Dict, validation_config: Dict):
        self.extraction_config = extraction_config
        self.validation_config = validation_config
        self.logger = logging.getLogger(__name__)
        
        # Initialize HTML sanitizer with extended tag support for comments
        self.sanitizer = Sanitizer({
            'tags': {
                'p', 'div', 'span', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
                'br', 'strong', 'em', 'ul', 'ol', 'li', 'blockquote',
                'article', 'section', 'main', 'header', 'footer',
                # Additional tags for comment extraction
                'comment', 'reply', 'discussion', 'thread'
            },
            'attributes': {},  # Allow class/id for comment selectors
            'empty': set(),
            'separate': {'p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li'},
            'whitespace': {'br'},
        })
        
        # Initialize language detection model
        self._init_language_detector()
        
        # Initialize spaCy for content validation
        self._init_spacy()
        
        # Initialize geographic filter for Mexican content detection
        self.geographic_filter = GeographicFilter()
        
        # Initialize PDF extractor for academic and government documents
        self.pdf_extractor = PDFExtractor({})
        
        # Initialize quality analyzer for Mexican Spanish assessment
        self.quality_analyzer = MexicanSpanishQualityAnalyzer()
        
        # Initialize specialized extractors for Mexican government sites
        self.specialized_extractors = SpecializedMexicanExtractors()
        
        # Comment extraction configuration
        self.comment_config = self.extraction_config.get('comment_extraction', {})
        self.max_comments = self.comment_config.get('max_comments_per_page', 200)
        self.comment_depth = self.comment_config.get('comment_depth', 3)
        self.comment_separator = self.comment_config.get('comment_separator', '\n--- COMENTARIOS ---\n')
        
        # Dynamic recursion configuration
        self.recursion_config = self.extraction_config.get('dynamic_recursion', {})
        self.recursion_enabled = self.recursion_config.get('enabled', True)
        self.max_recursion_depth = self.recursion_config.get('max_recursion_depth', 6)
        self.mexican_threshold = self.recursion_config.get('mexican_content_threshold', 2.0)
        
        # Common Mexican keywords for link analysis
        self.mexican_keywords = {
            'méxico', 'mexicano', 'mexicana', 'mx', 'cdmx', 'guadalajara', 'monterrey',
            'gobierno', 'gob', 'unam', 'ipn', 'scjn', 'conacyt', 'inegi',
            'artículo', 'noticia', 'investigación', 'tesis', 'publicación',
            'cultura', 'historia', 'política', 'economía', 'sociedad'
        }
    
    def _init_language_detector(self):
        """Initialize fasttext language detection model."""
        try:
            self.lang_detector = fasttext.load_model('lid.176.bin')
            self.logger.info("Loaded fasttext language detection model")
        except:
            self.logger.warning("Could not load fasttext model, language detection will be disabled")
            self.lang_detector = None
    
    def _init_spacy(self):
        """Initialize spaCy for content validation with optimized pipeline."""
        try:
            self.nlp = spacy.load(
                "es_core_news_sm",
                disable=["parser", "ner", "tagger", "lemmatizer", "attribute_ruler"]
            )
            self.nlp.add_pipe("sentencizer")
            self.logger.info("Loaded spaCy Spanish model for validation")
        except OSError:
            self.logger.warning("Spanish spaCy model not found, using blank model")
            self.nlp = spacy.blank("es")
            self.nlp.add_pipe("sentencizer")
    
    def extract_comments(self, html_content: str, comment_selector: Optional[str] = None) -> List[str]:
        """
        Extract comments from HTML using CSS selectors.
        
        Args:
            html_content: Raw HTML content
            comment_selector: CSS selector for comments
            
        Returns:
            List of extracted comment texts
        """
        if not self.comment_config.get('enabled', True):
            return []
        
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            comments = []
            
            # Use provided selector or try common comment selectors
            selectors = []
            if comment_selector:
                selectors.append(comment_selector)
            
            # Common comment selectors for Mexican sites
            default_selectors = [
                '.comment-text', '.comment-content', '.comment-body',
                '.comentario', '.comentario-texto', '.comentario-contenido',
                '.reply-text', '.reply-content', '.respuesta',
                '.discussion-text', '.discusion-contenido',
                '.fb-comment', '.disqus-comment', '.livefyre-comment',
                '[class*="comment"]', '[class*="comentario"]',
                '[class*="reply"]', '[class*="respuesta"]'
            ]
            selectors.extend(default_selectors)
            
            comment_count = 0
            for selector in selectors:
                if comment_count >= self.max_comments:
                    break
                    
                try:
                    elements = soup.select(selector)
                    self.logger.debug(f"Found {len(elements)} elements with selector: {selector}")
                    
                    for element in elements:
                        if comment_count >= self.max_comments:
                            break
                        
                        comment_text = element.get_text(strip=True)
                        
                        # Filter out very short or spammy comments
                        if len(comment_text) < 10 or len(comment_text) > 2000:
                            continue
                        
                        # Basic spam detection
                        if self._is_spam_comment(comment_text):
                            continue
                        
                        # Check if it looks like Spanish content
                        if self._is_likely_spanish(comment_text):
                            comments.append(comment_text)
                            comment_count += 1
                
                except Exception as e:
                    self.logger.debug(f"Error with selector {selector}: {e}")
                    continue
            
            # Remove duplicates while preserving order
            unique_comments = []
            seen = set()
            for comment in comments:
                comment_hash = hash(comment)
                if comment_hash not in seen:
                    seen.add(comment_hash)
                    unique_comments.append(comment)
            
            self.logger.info(f"Extracted {len(unique_comments)} unique comments")
            return unique_comments[:self.max_comments]
            
        except Exception as e:
            self.logger.warning(f"Comment extraction failed: {e}")
            return []
    
    def _is_spam_comment(self, text: str) -> bool:
        """Basic spam detection for comments."""
        text_lower = text.lower()
        
        # Common spam patterns
        spam_patterns = [
            r'http[s]?://',  # URLs
            r'www\.',        # Web addresses
            r'\.com',        # Domains
            r'viagra',       # Common spam words
            r'casino',
            r'poker',
            r'bitcoin',
            r'cryptocurrency'
        ]
        
        spam_count = sum(1 for pattern in spam_patterns if re.search(pattern, text_lower))
        
        # Too many links or spam keywords
        if spam_count > 2:
            return True
        
        # Too many repeated characters
        if re.search(r'(.)\1{4,}', text):
            return True
        
        # All caps (likely spam)
        if len(text) > 20 and text.isupper():
            return True
        
        return False
    
    def _is_likely_spanish(self, text: str) -> bool:
        """Quick check if text is likely Spanish."""
        if len(text) < 20:
            return True  # Too short to determine, include it
        
        # Check for Spanish characteristics
        spanish_indicators = ['ñ', 'ü', 'á', 'é', 'í', 'ó', 'ú']
        spanish_words = ['el', 'la', 'los', 'las', 'de', 'del', 'que', 'es', 'en', 'un', 'una']
        
        has_spanish_chars = any(char in text.lower() for char in spanish_indicators)
        has_spanish_words = any(word in text.lower().split() for word in spanish_words)
        
        return has_spanish_chars or has_spanish_words
    
    def discover_links(self, html_content: str, base_url: str, source_config: Dict = None) -> List[Dict[str, Any]]:
        """
        Discover high-value links for dynamic recursion.
        
        Args:
            html_content: HTML content to extract links from
            base_url: Base URL for relative link resolution
            source_config: Source configuration with recursion keywords
            
        Returns:
            List of dictionaries with link information
        """
        if not self.recursion_enabled:
            return []
        
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            discovered_links = []
            
            # Get recursion keywords from source config
            recursion_keywords = set()
            if source_config and 'recursion_keywords' in source_config:
                recursion_keywords.update(source_config['recursion_keywords'])
            recursion_keywords.update(self.mexican_keywords)
            
            # Find all links
            links = soup.find_all('a', href=True)
            self.logger.debug(f"Found {len(links)} total links")
            
            for link in links:
                try:
                    href = link['href']
                    anchor_text = link.get_text(strip=True)
                    
                    # Resolve relative URLs
                    if href.startswith('/'):
                        full_url = urljoin(base_url, href)
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        continue  # Skip javascript:, mailto:, etc.
                    
                    # Skip if same domain and already processed
                    if self._should_skip_link(full_url, base_url):
                        continue
                    
                    # Score link relevance
                    relevance_score = self._score_link_relevance(
                        full_url, anchor_text, recursion_keywords
                    )
                    
                    if relevance_score > 0:
                        discovered_links.append({
                            'url': full_url,
                            'anchor_text': anchor_text,
                            'relevance_score': relevance_score,
                            'link_type': self._classify_link_type(full_url, anchor_text)
                        })
                
                except Exception as e:
                    self.logger.debug(f"Error processing link: {e}")
                    continue
            
            # Sort by relevance and limit results
            discovered_links.sort(key=lambda x: x['relevance_score'], reverse=True)
            max_links = self.extraction_config.get('max_links_per_page', 200)
            
            self.logger.info(f"Discovered {len(discovered_links[:max_links])} high-value links")
            return discovered_links[:max_links]
            
        except Exception as e:
            self.logger.warning(f"Link discovery failed: {e}")
            return []
    
    def _should_skip_link(self, url: str, base_url: str) -> bool:
        """Determine if link should be skipped."""
        # Skip external domains for now (can be configured later)
        base_domain = urlparse(base_url).netloc
        link_domain = urlparse(url).netloc
        
        # Skip if different domain (can be modified for cross-domain crawling)
        if link_domain != base_domain:
            return True
        
        # Skip common non-content URLs
        skip_patterns = [
            r'/contact', r'/contacto', r'/about', r'/acerca',
            r'/privacy', r'/privacidad', r'/terms', r'/terminos',
            r'/subscribe', r'/suscribir', r'/login', r'/register',
            r'/search', r'/buscar', r'\.pdf$', r'\.doc$', r'\.zip$'
        ]
        
        url_lower = url.lower()
        for pattern in skip_patterns:
            if re.search(pattern, url_lower):
                return True
        
        return False
    
    def _score_link_relevance(self, url: str, anchor_text: str, keywords: set) -> float:
        """Score link relevance for Mexican content."""
        score = 0.0
        
        url_lower = url.lower()
        anchor_lower = anchor_text.lower()
        
        # Check for Mexican keywords in URL
        for keyword in keywords:
            if keyword in url_lower:
                score += 2.0
            if keyword in anchor_lower:
                score += 1.5
        
        # Bonus for specific content types
        content_bonuses = {
            'artículo': 3.0, 'noticia': 3.0, 'investigación': 4.0,
            'tesis': 5.0, 'publicación': 4.0, 'documento': 2.0,
            'política': 2.0, 'cultura': 2.0, 'historia': 2.0,
            'economía': 2.0, 'sociedad': 2.0, 'gobierno': 3.0
        }
        
        for term, bonus in content_bonuses.items():
            if term in anchor_lower:
                score += bonus
        
        # Mexican geographic indicators
        geo_terms = {
            'méxico': 4.0, 'mexicano': 4.0, 'cdmx': 3.0,
            'guadalajara': 2.0, 'monterrey': 2.0, 'puebla': 2.0,
            'tijuana': 2.0, 'mérida': 2.0, 'nacional': 1.5
        }
        
        for term, bonus in geo_terms.items():
            if term in url_lower or term in anchor_lower:
                score += bonus
        
        # Penalty for likely spam or low-quality links
        if len(anchor_text) < 3 or anchor_text.isdigit():
            score -= 1.0
        
        return max(0.0, score)
    
    def _classify_link_type(self, url: str, anchor_text: str) -> str:
        """Classify link type for prioritization."""
        url_lower = url.lower()
        anchor_lower = anchor_text.lower()
        
        if any(term in url_lower for term in ['gob.mx', 'gobierno']):
            return 'government'
        elif any(term in url_lower for term in ['unam', 'ipn', 'edu.mx']):
            return 'academic'
        elif any(term in anchor_lower for term in ['noticia', 'artículo']):
            return 'news'
        elif any(term in anchor_lower for term in ['investigación', 'tesis']):
            return 'research'
        elif 'pdf' in url_lower:
            return 'document'
        else:
            return 'general'
    
    def extract_with_comments(self, html_content: str, source_config: Dict[str, Any], url: str = "") -> Dict[str, Any]:
        """
        Enhanced extraction that includes comment extraction and link discovery.
        
        Args:
            html_content: Raw HTML content
            source_config: Source configuration
            url: Source URL
            
        Returns:
            Enhanced extraction results with comments and discovered links
        """
        try:
            # Start with basic extraction (using existing extractor logic)
            result = self._basic_extract(html_content, source_config, url)
            
            if not result['success']:
                return result
            
            main_text = result['text']
            
            # Extract comments if enabled
            comments = []
            if source_config.get('crawl_comments', False):
                comment_selector = source_config.get('comment_selector')
                comments = self.extract_comments(html_content, comment_selector)
            
            # Combine main content with comments
            if comments:
                comment_text = self.comment_separator.join(comments)
                full_text = f"{main_text}{self.comment_separator}{comment_text}"
                self.logger.info(f"Added {len(comments)} comments to content")
            else:
                full_text = main_text
            
            # Discover links for dynamic recursion
            discovered_links = []
            if source_config.get('dynamic_recursion', False):
                discovered_links = self.discover_links(html_content, url, source_config)
            
            # Update result
            result.update({
                'text': full_text,
                'comments_count': len(comments),
                'discovered_links': discovered_links,
                'enhanced_extraction': True
            })
            
            # Update metadata
            if 'metadata' not in result:
                result['metadata'] = {}
            
            result['metadata'].update({
                'comments_extracted': len(comments),
                'links_discovered': len(discovered_links),
                'total_content_length': len(full_text),
                'comment_ratio': len(comments) / (len(comments) + 1) if comments else 0
            })
            
            return result
            
        except Exception as e:
            self.logger.error(f"Enhanced extraction failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'enhanced_extraction': True
            }
    
    def _basic_extract(self, html_content: str, source_config: Dict[str, Any], url: str = "") -> Dict[str, Any]:
        """
        Basic extraction logic (simplified version of existing extractor).
        This would typically call the existing extractor's extract method.
        """
        # This is a simplified version - in practice, you'd call the existing extractor
        try:
            # Use trafilatura as primary method
            text = trafilatura.extract(html_content, include_comments=False, include_tables=True)
            
            if not text or len(text.strip()) < 50:
                # Fallback to BeautifulSoup
                soup = BeautifulSoup(html_content, 'lxml')
                
                # Remove unwanted elements
                for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
                    element.decompose()
                
                # Try to find main content
                main_content = soup.find('main') or soup.find('article') or soup.find('body')
                if main_content:
                    text = main_content.get_text(separator='\n', strip=True)
            
            if not text or len(text.strip()) < 20:
                return {
                    'success': False,
                    'error': 'No content extracted'
                }
            
            return {
                'success': True,
                'text': text.strip(),
                'extraction_method': 'enhanced_basic'
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }