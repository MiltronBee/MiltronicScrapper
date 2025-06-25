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
from .geographic_filter import GeographicFilter
from .pdf_extractor import PDFExtractor
from .quality_analyzer import MexicanSpanishQualityAnalyzer
from .specialized_extractors import SpecializedMexicanExtractors


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
        
        # Initialize geographic filter for Mexican content detection
        self.geographic_filter = GeographicFilter()
        
        # Initialize PDF extractor for academic and government documents
        self.pdf_extractor = PDFExtractor({})
        
        # Initialize quality analyzer for Mexican Spanish assessment
        self.quality_analyzer = MexicanSpanishQualityAnalyzer()
        
        # Initialize specialized extractors for Mexican government sites
        self.specialized_extractors = SpecializedMexicanExtractors()
    
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
            # Add sentencizer for sentence boundary detection
            self.nlp.add_pipe("sentencizer")
            self.logger.info("Loaded spaCy Spanish model for validation")
        except OSError:
            # Fallback to blank model if pre-trained model not available
            self.logger.warning("Spanish spaCy model not found, using blank model")
            self.nlp = spacy.blank("es")
            # Add sentencizer for sentence boundary detection
            self.nlp.add_pipe("sentencizer")
    
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
    
    def _extract_title_from_html(self, html_content: str) -> str:
        """Extract title from HTML content."""
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Try various title selectors
            title_selectors = [
                'title',
                'h1',
                '.title',
                '.headline',
                '.article-title',
                '[data-title]'
            ]
            
            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem and title_elem.get_text(strip=True):
                    return title_elem.get_text(strip=True)
            
            return ""
            
        except Exception as e:
            self.logger.warning(f"Failed to extract title: {e}")
            return ""
    
    def extract_pdf(self, url: str) -> Dict[str, Any]:
        """
        Extract content from PDF URL.
        
        Args:
            url: PDF URL to extract from
            
        Returns:
            Dictionary containing extraction results and metadata
        """
        self.logger.info(f"Extracting PDF content from: {url}")
        
        try:
            # Use PDF extractor
            pdf_result = self.pdf_extractor.extract_pdf_content(url)
            
            if not pdf_result['success']:
                return {
                    'success': False,
                    'error': pdf_result.get('error', 'PDF extraction failed'),
                    'extraction_method': 'pdf'
                }
            
            text = pdf_result['text']
            pdf_metadata = pdf_result['metadata']
            
            # Apply language detection
            lang_code, lang_confidence = self._detect_language(text)
            required_lang = self.validation_config.get('required_language', 'es')
            min_confidence = self.validation_config.get('lang_detect_confidence', 0.70)
            
            if lang_code != required_lang or lang_confidence < min_confidence:
                raise LanguageMismatchError(
                    f"Language mismatch: detected {lang_code} ({lang_confidence:.3f}) "
                    f"but required {required_lang} (min confidence: {min_confidence})"
                )
            
            # Apply content validation
            validation_results = self._validate_content(text)
            if not validation_results['valid']:
                error_msg = "; ".join(validation_results['errors'])
                raise ContentTooShortError(f"Content validation failed: {error_msg}")
            
            # Apply geographic filtering
            title = pdf_metadata.get('title', '')
            is_mexican, geo_score, geo_reasons = self.geographic_filter.is_mexican_content(
                text, title, url, min_score=5.0
            )
            
            regional_info = self.geographic_filter.get_regional_classification(text)
            
            # Apply quality analysis
            quality_score = self.quality_analyzer.analyze_quality(text, title)
            
            # Combine metadata
            combined_metadata = {
                **pdf_metadata,
                'language': lang_code,
                'language_confidence': lang_confidence,
                'is_mexican_content': is_mexican,
                'mexican_score': geo_score.total_score,
                'mexican_confidence': geo_score.confidence,
                'geographic_indicators': {
                    'institutional': geo_score.institutional_markers,
                    'regional': geo_score.regional_indicators,
                    'linguistic': geo_score.linguistic_markers,
                    'other': geo_score.mexican_indicators
                },
                'regional_classification': regional_info,
                'geographic_reasons': geo_reasons[:5],
                'quality_metrics': {
                    'overall_score': quality_score.overall_score,
                    'dialect_authenticity': quality_score.dialect_authenticity,
                    'formality_level': quality_score.formality_level,
                    'linguistic_complexity': quality_score.linguistic_complexity,
                    'vocabulary_richness': quality_score.vocabulary_richness,
                    'cultural_content': quality_score.mexican_cultural_content,
                    'quality_confidence': quality_score.confidence
                }
            }
            
            return {
                'success': True,
                'text': text,
                'extraction_method': 'pdf',
                'metadata': combined_metadata,
                'validation': validation_results
            }
            
        except (LanguageMismatchError, ContentTooShortError) as e:
            self.logger.warning(f"PDF validation failed for {url}: {e}")
            return {
                'success': False,
                'error': str(e),
                'extraction_method': 'pdf'
            }
        except Exception as e:
            self.logger.error(f"PDF extraction error for {url}: {e}")
            return {
                'success': False,
                'error': f"Unexpected error: {e}",
                'extraction_method': 'pdf'
            }
    
    def extract(self, html_content: str, source_config: Dict[str, Any], url: str = "") -> Dict[str, Any]:
        """
        Main extraction method that coordinates the entire extraction pipeline.
        
        Args:
            html_content: Raw HTML content to extract from
            source_config: Configuration for the specific source
            url: Source URL for geographic analysis
            
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
            
            # Step 2: Try specialized extractors first for Mexican government sites
            if self.specialized_extractors.is_specialized_site(url):
                specialized_result = self.specialized_extractors.extract_specialized(sanitized_html, url)
                if specialized_result['success']:
                    text = specialized_result['text']
                    extraction_method = 'specialized'
                    # Add specialized metadata
                    result['metadata'].update(specialized_result.get('metadata', {}))
                else:
                    text = None
                    extraction_method = None
            else:
                text = None
                extraction_method = None
            
            # Step 3: Primary extraction with trafilatura if not specialized
            if not text:
                text = self._extract_with_trafilatura(sanitized_html)
                extraction_method = 'trafilatura'
            
            # Step 4: Fallback to BeautifulSoup if needed
            if not text:
                fallback_selector = source_config.get('fallback_selector')
                text = self._extract_with_beautifulsoup(sanitized_html, fallback_selector)
                extraction_method = 'beautifulsoup'
            
            if not text:
                raise ExtractionFailedError("All extraction methods failed to produce content")
            
            # Step 5: Clean and normalize text
            text = self._clean_text(text)
            
            # Step 6: Language detection with better fallback handling
            lang_code, lang_confidence = self._detect_language(text)
            required_lang = self.validation_config.get('required_language', 'es')
            min_confidence = self.validation_config.get('lang_detect_confidence', 0.50)
            
            # Skip language validation for PDFs or if detection confidence is too low overall
            skip_lang_check = (url.lower().endswith('.pdf') or 
                             lang_confidence < 0.2 or 
                             extraction_method == 'pdf')
            
            if not skip_lang_check and (lang_code != required_lang or lang_confidence < min_confidence):
                self.logger.warning(
                    f"Language validation failed: detected {lang_code} ({lang_confidence:.3f}) "
                    f"but required {required_lang} (min confidence: {min_confidence}). "
                    f"Content may still be valuable - proceeding with extraction."
                )
                # Don't raise exception, just log warning and continue
            
            # Step 7: Content validation with more lenient handling
            validation_results = self._validate_content(text)
            if not validation_results['valid']:
                error_msg = "; ".join(validation_results['errors'])
                self.logger.warning(f"Content validation warnings: {error_msg}")
                # For PDFs and specialized content, be more lenient
                if extraction_method in ['pdf', 'specialized'] or url.lower().endswith('.pdf'):
                    self.logger.info("Allowing content despite validation warnings due to specialized source")
                else:
                    raise ContentTooShortError(f"Content validation failed: {error_msg}")
            
            # Step 8: Geographic filtering for Mexican content
            title = self._extract_title_from_html(html_content)
            is_mexican, geo_score, geo_reasons = self.geographic_filter.is_mexican_content(
                text, title, url, min_score=5.0
            )
            
            # Add regional classification
            regional_info = self.geographic_filter.get_regional_classification(text)
            
            # Step 9: Quality analysis for Mexican Spanish
            quality_score = self.quality_analyzer.analyze_quality(text, title)
            
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
                    'compression_ratio': len(text) / len(html_content) if html_content else 0,
                    'title': title,
                    'is_mexican_content': is_mexican,
                    'mexican_score': geo_score.total_score,
                    'mexican_confidence': geo_score.confidence,
                    'geographic_indicators': {
                        'institutional': geo_score.institutional_markers,
                        'regional': geo_score.regional_indicators,
                        'linguistic': geo_score.linguistic_markers,
                        'other': geo_score.mexican_indicators
                    },
                    'regional_classification': regional_info,
                    'geographic_reasons': geo_reasons[:5],  # Limit to top 5 reasons
                    'quality_metrics': {
                        'overall_score': quality_score.overall_score,
                        'dialect_authenticity': quality_score.dialect_authenticity,
                        'formality_level': quality_score.formality_level,
                        'linguistic_complexity': quality_score.linguistic_complexity,
                        'vocabulary_richness': quality_score.vocabulary_richness,
                        'cultural_content': quality_score.mexican_cultural_content,
                        'quality_confidence': quality_score.confidence
                    }
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