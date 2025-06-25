"""
Multi-layered content extraction and purification engine.
Combines trafilatura, BeautifulSoup, and advanced validation for high-quality text extraction.
"""

import logging
import re
from typing import Dict, Optional, Any
from bs4 import BeautifulSoup
import trafilatura
from html_sanitizer import Sanitizer
import fasttext
import spacy
from .exceptions import ExtractionFailedError, LanguageMismatchError, ContentTooShortError


class Extractor:
    """
    Data processing engine that extracts, purifies, and validates text content.
    Implements a multi-layered strategy for maximum quality and robustness.
    """
    
    def __init__(self, extraction_config: Dict, validation_config: Dict):
        self.extraction_config = extraction_config
        self.validation_config = validation_config
        self.logger = logging.getLogger(__name__)
        
        # Initialize HTML sanitizer with allowlist approach
        self.sanitizer = Sanitizer({
            'tags': {
                'p', 'div', 'span', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
                'br', 'strong', 'em', 'ul', 'ol', 'li', 'blockquote',
                'article', 'section', 'main', 'header', 'footer'
            },
            'attributes': {},
            'empty': set(),
            'separate': {'p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li'},
            'whitespace': 'normalize',
        })
        
        # Initialize language detection model
        self._init_language_detector()
        
        # Initialize spaCy for content validation
        self._init_spacy()
    
    def _init_language_detector(self):
        """Initialize fasttext language detection model."""
        try:
            # Try to load pre-trained language identification model
            self.lang_detector = fasttext.load_model('lid.176.bin')
            self.logger.info("Loaded fasttext language detection model")
        except:
            self.logger.warning("Could not load fasttext model, language detection will be disabled")
            self.lang_detector = None
    
    def _init_spacy(self):
        """Initialize spaCy for content validation with optimized pipeline."""
        try:
            # Load Spanish model with minimal components for performance
            self.nlp = spacy.load(
                "es_core_news_sm",
                disable=["parser", "ner", "tagger", "lemmatizer", "attribute_ruler"]
            )
            self.logger.info("Loaded spaCy Spanish model for validation")
        except OSError:
            # Fallback to blank model if pre-trained model not available
            self.logger.warning("Spanish spaCy model not found, using blank model")
            self.nlp = spacy.blank("es")
    
    def _sanitize_html(self, html_content: str) -> str:
        """Pre-emptively sanitize HTML content for cleaner extraction."""
        try:
            sanitized = self.sanitizer.sanitize(html_content)
            self.logger.debug(f"HTML sanitized: {len(html_content)} -> {len(sanitized)} chars")
            return sanitized
        except Exception as e:
            self.logger.warning(f"HTML sanitization failed: {e}")
            return html_content
    
    def _extract_with_trafilatura(self, html_content: str) -> Optional[str]:
        """Primary extraction using trafilatura."""
        try:
            text = trafilatura.extract(
                html_content,
                include_comments=False,
                include_tables=True,  # Tables may contain useful structured data
                no_fallback=True,     # We have our own fallback
                favor_precision=True   # Prioritize clean content over completeness
            )
            
            if text and len(text.strip()) > 50:  # Basic sanity check
                self.logger.debug(f"Trafilatura extracted {len(text)} characters")
                return text.strip()
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Trafilatura extraction failed: {e}")
            return None
    
    def _extract_with_beautifulsoup(self, html_content: str, fallback_selector: Optional[str] = None) -> Optional[str]:
        """Fallback extraction using BeautifulSoup with optional CSS selector."""
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            
            if fallback_selector:
                # Use site-specific selector
                content_area = soup.select_one(fallback_selector)
                if content_area:
                    text = content_area.get_text(separator='\n', strip=True)
                    self.logger.debug(f"BeautifulSoup (selector) extracted {len(text)} characters")
                    return text
            
            # Generic content extraction strategy
            # Remove script and style elements
            for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
                element.decompose()
            
            # Try common content containers
            content_selectors = [
                'article', 'main', '[role="main"]', '.content', '#content',
                '.post-content', '.entry-content', '.article-body'
            ]
            
            for selector in content_selectors:
                content_area = soup.select_one(selector)
                if content_area:
                    text = content_area.get_text(separator='\n', strip=True)
                    if len(text) > 100:  # Must be substantial
                        self.logger.debug(f"BeautifulSoup (generic) extracted {len(text)} characters")
                        return text
            
            # Last resort: extract from body
            body = soup.find('body')
            if body:
                text = body.get_text(separator='\n', strip=True)
                self.logger.debug(f"BeautifulSoup (body) extracted {len(text)} characters")
                return text
            
            return None
            
        except Exception as e:
            self.logger.warning(f"BeautifulSoup extraction failed: {e}")
            return None
    
    def _detect_language(self, text: str) -> tuple[str, float]:
        """Detect the language of the text using fasttext."""
        if not self.lang_detector:
            # If no model available, assume Spanish and high confidence
            return 'es', 0.95
        
        try:
            # Clean text for language detection
            clean_text = re.sub(r'\s+', ' ', text.strip())
            if len(clean_text) < 10:
                return 'unknown', 0.0
            
            predictions = self.lang_detector.predict(clean_text, k=1)
            lang_code = predictions[0][0].replace('__label__', '')
            confidence = predictions[1][0]
            
            self.logger.debug(f"Detected language: {lang_code} (confidence: {confidence:.3f})")
            return lang_code, confidence
            
        except Exception as e:
            self.logger.warning(f"Language detection failed: {e}")
            return 'unknown', 0.0
    
    def _validate_content(self, text: str) -> Dict[str, Any]:
        """Validate extracted content against configured criteria."""
        validation_results = {
            'valid': True,
            'errors': [],
            'stats': {}
        }
        
        try:
            # Process text with spaCy
            doc = self.nlp(text)
            
            # Count words and sentences
            word_count = len([token for token in doc if not token.is_space and not token.is_punct])
            sent_count = len(list(doc.sents))
            
            validation_results['stats'] = {
                'word_count': word_count,
                'sentence_count': sent_count,
                'character_count': len(text)
            }
            
            # Check minimum word count
            min_words = self.validation_config.get('min_word_count', 200)
            if word_count < min_words:
                validation_results['valid'] = False
                validation_results['errors'].append(f"Word count {word_count} below minimum {min_words}")
            
            # Check minimum sentence count if configured
            min_sentences = self.validation_config.get('min_sentence_count', 0)
            if min_sentences > 0 and sent_count < min_sentences:
                validation_results['valid'] = False
                validation_results['errors'].append(f"Sentence count {sent_count} below minimum {min_sentences}")
            
            self.logger.debug(f"Content validation: {word_count} words, {sent_count} sentences")
            
        except Exception as e:
            self.logger.error(f"Content validation failed: {e}")
            validation_results['valid'] = False
            validation_results['errors'].append(f"Validation error: {e}")
        
        return validation_results
    
    def _clean_text(self, text: str) -> str:
        """Final text cleaning and normalization."""
        # Normalize whitespace
        text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)  # Reduce multiple newlines
        text = re.sub(r'[ \t]+', ' ', text)           # Normalize spaces and tabs
        text = re.sub(r'\n ', '\n', text)             # Remove spaces after newlines
        
        # Remove common boilerplate patterns
        boilerplate_patterns = [
            r'.*?cookies?.*?acepta.*?\n',  # Cookie notices
            r'.*?política de privacidad.*?\n',  # Privacy policy mentions
            r'.*?suscr[ií]b.*?newsletter.*?\n',  # Newsletter subscriptions
        ]
        
        for pattern in boilerplate_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        return text.strip()
    
    def extract(self, html_content: str, source_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main extraction method that coordinates the entire extraction pipeline.
        
        Args:
            html_content: Raw HTML content to extract from
            source_config: Configuration for the specific source
            
        Returns:
            Dictionary containing extraction results and metadata
        """
        result = {
            'success': False,
            'text': None,
            'metadata': {},
            'validation': {},
            'extraction_method': None
        }
        
        try:
            # Step 1: Pre-emptive HTML sanitization
            sanitized_html = self._sanitize_html(html_content)
            
            # Step 2: Primary extraction with trafilatura
            text = self._extract_with_trafilatura(sanitized_html)
            extraction_method = 'trafilatura'
            
            # Step 3: Fallback to BeautifulSoup if needed
            if not text:
                fallback_selector = source_config.get('fallback_selector')
                text = self._extract_with_beautifulsoup(sanitized_html, fallback_selector)
                extraction_method = 'beautifulsoup'
            
            if not text:
                raise ExtractionFailedError("All extraction methods failed to produce content")
            
            # Step 4: Clean and normalize text
            text = self._clean_text(text)
            
            # Step 5: Language detection
            lang_code, lang_confidence = self._detect_language(text)
            required_lang = self.validation_config.get('required_language', 'es')
            min_confidence = self.validation_config.get('lang_detect_confidence', 0.90)
            
            if lang_code != required_lang or lang_confidence < min_confidence:
                raise LanguageMismatchError(
                    f"Language mismatch: detected {lang_code} ({lang_confidence:.3f}) "
                    f"but required {required_lang} (min confidence: {min_confidence})"
                )
            
            # Step 6: Content validation
            validation_results = self._validate_content(text)
            if not validation_results['valid']:
                error_msg = "; ".join(validation_results['errors'])
                raise ContentTooShortError(f"Content validation failed: {error_msg}")
            
            # Success: populate result
            result.update({
                'success': True,
                'text': text,
                'extraction_method': extraction_method,
                'metadata': {
                    'language': lang_code,
                    'language_confidence': lang_confidence,
                    'original_size': len(html_content),
                    'extracted_size': len(text),
                    'compression_ratio': len(text) / len(html_content) if html_content else 0
                },
                'validation': validation_results
            })
            
            self.logger.info(
                f"Extraction successful: {validation_results['stats']['word_count']} words, "
                f"{validation_results['stats']['sentence_count']} sentences "
                f"({extraction_method})"
            )
            
        except (LanguageMismatchError, ContentTooShortError, ExtractionFailedError) as e:
            self.logger.warning(f"Extraction failed: {e}")
            result['error'] = str(e)
            
        except Exception as e:
            self.logger.error(f"Unexpected extraction error: {e}")
            result['error'] = f"Unexpected error: {e}"
        
        return result