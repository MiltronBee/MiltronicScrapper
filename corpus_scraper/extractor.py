"""
Multi-layered content extraction and purification engine.
Combines trafilatura, BeautifulSoup, and advanced validation for high-quality text extraction.
"""

import logging
import re
import signal
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
            # First normalize encoding issues that cause trafilatura to fail
            html_content = self._normalize_html_encoding(html_content)
            
            sanitized = self.sanitizer.sanitize(html_content)
            self.logger.debug(f"HTML sanitized: {len(html_content)} -> {len(sanitized)} chars")
            return sanitized
        except Exception as e:
            self.logger.warning(f"HTML sanitization failed: {e}")
            return html_content
    
    def _normalize_html_encoding(self, html_content: str) -> str:
        """Normalize HTML encoding issues that cause trafilatura to fail."""
        try:
            # If content appears to be binary data or severely corrupted, return empty
            if not isinstance(html_content, str):
                self.logger.warning("HTML content is not a string")
                return ""
            
            # Check for common encoding corruption patterns
            if len(html_content) < 100:
                return html_content
            
            # Remove null bytes and other problematic characters
            html_content = html_content.replace('\x00', '')
            html_content = html_content.replace('\ufffd', '')  # Unicode replacement character
            
            # Fix common encoding issues in HTML
            html_content = html_content.replace('Ã¡', 'á')  # á in ISO-8859-1 -> UTF-8
            html_content = html_content.replace('Ã©', 'é')  # é in ISO-8859-1 -> UTF-8
            html_content = html_content.replace('Ã­', 'í')  # í in ISO-8859-1 -> UTF-8
            html_content = html_content.replace('Ã³', 'ó')  # ó in ISO-8859-1 -> UTF-8
            html_content = html_content.replace('Ãº', 'ú')  # ú in ISO-8859-1 -> UTF-8
            html_content = html_content.replace('Ã±', 'ñ')  # ñ in ISO-8859-1 -> UTF-8
            html_content = html_content.replace('Â', '')    # Common artifact
            
            # Normalize whitespace
            html_content = re.sub(r'\s+', ' ', html_content)
            
            # Ensure we have proper HTML structure
            if not html_content.strip().startswith('<'):
                # Content might be text-only, wrap in basic HTML
                html_content = f"<html><body>{html_content}</body></html>"
            
            return html_content
            
        except Exception as e:
            self.logger.warning(f"HTML encoding normalization failed: {e}")
            return html_content
    
    def _extract_with_trafilatura(self, html_content: str) -> Optional[str]:
        """Primary extraction using trafilatura with timeout protection."""
        try:
            # Validate HTML content before processing
            if not html_content or len(html_content.strip()) < 100:
                self.logger.warning("HTML content too short for trafilatura processing")
                return None
            
            # Check for common encoding issues that cause trafilatura to fail
            if html_content.count('<') < 3 or html_content.count('>') < 3:
                self.logger.warning("HTML content appears malformed (insufficient tags)")
                # Try to extract any text content that might be present
                try:
                    # Remove any HTML tags that are present
                    clean_text = re.sub(r'<[^>]*>', '', html_content)
                    clean_text = clean_text.strip()
                    if len(clean_text) > 50:  # If we got some meaningful text
                        self.logger.info(f"Extracted {len(clean_text)} characters from malformed HTML as plain text")
                        return clean_text
                except Exception as e:
                    self.logger.debug(f"Failed to extract text from malformed HTML: {e}")
                return None
            
            # Use timeout to prevent hanging on problematic content
            def timeout_handler(signum, frame):
                raise TimeoutError("Trafilatura extraction timed out")
            
            # Set up timeout (30 seconds should be more than enough)
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(30)
            
            try:
                text = trafilatura.extract(
                    html_content,
                    include_comments=False,
                    include_tables=True,  # Tables may contain useful structured data
                    no_fallback=True,     # We have our own fallback
                    favor_precision=True   # Prioritize clean content over completeness
                )
            finally:
                # Always clear the alarm and restore the old handler
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
            
            if text and len(text.strip()) > 50:  # Basic sanity check
                self.logger.debug(f"Trafilatura extracted {len(text)} characters")
                return text.strip()
            else:
                self.logger.debug("Trafilatura returned no content or content too short")
            
            return None
            
        except TimeoutError:
            self.logger.warning("Trafilatura extraction timed out, skipping to fallback method")
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
    
    # ------------------------------------------------------------------
    # Subtitle ZIP extraction
    # ------------------------------------------------------------------
    def _clean_subtitle_filename(self, filename: str) -> str:
        """Clean and normalize a subtitle filename to extract a readable title.
        
        Args:
            filename: Raw filename without extension
            
        Returns:
            Cleaned title string
        """
        # Remove common patterns
        patterns = [
            r'\[[^\]]*\]',  # Remove anything in brackets
            r'\([^\)]*\)',  # Remove anything in parentheses
            r'S\d{1,2}E\d{1,2}',  # Remove season/episode markers like S01E01
            r'\d{1,2}x\d{1,2}',  # Remove season/episode markers like 1x01
            r'-\s*\w{2,5}$',  # Remove language codes at the end like -spa
            r'DVDRip|XviD|INTERNAL|720p|1080p|BluRay|WEB-DL',  # Remove quality markers
            r'CD\d+'  # Remove CD1, CD2, etc.
        ]
        
        result = filename
        for pattern in patterns:
            result = re.sub(pattern, '', result, flags=re.IGNORECASE)
        
        # Replace dots, underscores and multiple spaces with single spaces
        result = re.sub(r'[._]+', ' ', result)
        result = re.sub(r'\s+', ' ', result)
        
        return result.strip()
        
    def extract_subtitle_zip(self, url: str, content_bytes: bytes = None) -> Dict[str, Any]:
        """Download a .zip from OpenSubtitles (or similar) and extract plain text from subtitles.
        Each subtitle sentence is placed on its own line. Multiple SRT files are
        concatenated with blank lines between them.
        
        Args:
            url: URL to download the ZIP from
            content_bytes: Optional ZIP content bytes (if already downloaded)
            
        Returns:
            Dictionary with extraction results
        """
        import zipfile, io, chardet, tempfile, os, re, requests
        self.logger.info(f"Processing subtitle ZIP: {url}")
        
        try:
            # If content wasn't provided, download it
            if content_bytes is None:
                # OpenSubtitles requires a browser user-agent
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                
                # For OpenSubtitles, we need to handle redirects properly
                response = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
                response.raise_for_status()
                content_bytes = response.content
                
                content_type = response.headers.get('Content-Type', '')
                self.logger.info(f"Downloaded ZIP content: {len(content_bytes)} bytes, Content-Type: {content_type}")
            else:
                self.logger.info(f"Using provided content bytes: {len(content_bytes)} bytes")
            
            # Verify this is a ZIP file by checking for PK signature
            if not content_bytes.startswith(b'PK'):
                self.logger.error(f"Content does not appear to be a valid ZIP file (missing PK signature)")
                # Try to extract text from it as if it were HTML
                try:
                    text_content = content_bytes.decode('utf-8', errors='replace')
                    return {
                        'success': True,
                        'text': text_content[:2000],  # Limit to first 2000 characters
                        'metadata': {
                            'extraction_method': 'direct_content',
                            'url': url,
                            'warning': 'Content was not a valid ZIP file'
                        }
                    }
                except Exception as decode_err:
                    self.logger.error(f"Failed to decode content as text: {decode_err}")
                    return {'success': False, 'error': f"Content is not a valid ZIP file and could not be decoded as text", 'extraction_method': 'subtitle_zip'}
            
            # Create a temporary file to save the zip content for debugging
            temp_zip_path = None
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_file:
                    temp_zip_path = temp_file.name
                    temp_file.write(content_bytes)
                    self.logger.info(f"Saved ZIP content to temporary file: {temp_zip_path}")
            except Exception as e:
                self.logger.warning(f"Could not save temporary ZIP file: {e}")
            
            # Process the ZIP file
            try:
                zip_bytes = io.BytesIO(content_bytes)
                with zipfile.ZipFile(zip_bytes) as zf:
                    # List all files in ZIP
                    file_list = zf.namelist()
                    self.logger.info(f"ZIP contains {len(file_list)} files: {file_list[:5]} {'...' if len(file_list) > 5 else ''}")
                    
                    sentences = []
                    subtitle_files_found = 0
                    for name in file_list:
                        if not name.lower().endswith('.srt'):
                            continue
                            
                        subtitle_files_found += 1
                        file_bytes = zf.read(name)
                        
                        # Try to detect encoding and decode
                        detected_encoding = chardet.detect(file_bytes)
                        self.logger.info(f"Detected encoding for {name}: {detected_encoding}")
                        
                        # Try multiple encodings in order of likelihood
                        decoded = False
                        for enc in ('utf-8', detected_encoding['encoding'] or '', 'latin-1', 'cp1252', 'iso-8859-1'):
                            if not enc:
                                continue
                            try:
                                srt_text = file_bytes.decode(enc)
                                self.logger.info(f"Successfully decoded {name} using {enc} encoding")
                                decoded = True
                                break
                            except Exception as e:
                                self.logger.debug(f"Could not decode {name} with {enc}: {e}")
                                continue
                        
                        if not decoded:
                            self.logger.warning(f"Could not decode {name} in {url} with any encoding")
                            continue
                        
                        # Parse SRT into sentences
                        buffer = []
                        for line in srt_text.splitlines():
                            line = line.strip('\ufeff').strip()
                            if not line:
                                # blank => flush buffer
                                if buffer:
                                    sentences.append(' '.join(buffer))
                                    buffer = []
                                continue
                            if line.isdigit() or '-->' in line:
                                # sequence number or timestamp
                                continue
                            buffer.append(line)
                        if buffer:
                            sentences.append(' '.join(buffer))
                        # blank line between files
                        sentences.append('')
                    
                    # If no subtitle files were processed, raise an error
                    if subtitle_files_found == 0:
                        raise ValueError(f"ZIP file contained no .srt files. Contents: {file_list}")
                    
                    # Instead of combining all subtitles into one file, return them as separate items
                    subtitle_items = []
                    file_index = 0
                    
                    # Process each subtitle file in the archive
                    for name in file_list:
                        if not name.lower().endswith('.srt'):
                            continue
                            
                        file_bytes = zf.read(name)
                        
                        # Try to detect encoding and decode
                        detected_encoding = chardet.detect(file_bytes)
                        self.logger.info(f"Detected encoding for {name}: {detected_encoding}")
                        
                        # Try multiple encodings in order of likelihood
                        decoded = False
                        srt_text = ""
                        for enc in ('utf-8', detected_encoding['encoding'] or '', 'latin-1', 'cp1252', 'iso-8859-1'):
                            if not enc:
                                continue
                            try:
                                srt_text = file_bytes.decode(enc)
                                self.logger.info(f"Successfully decoded {name} using {enc} encoding")
                                decoded = True
                                break
                            except Exception as e:
                                self.logger.debug(f"Could not decode {name} with {enc}: {e}")
                                continue
                        
                        if not decoded:
                            self.logger.warning(f"Could not decode {name} in {url} with any encoding")
                            continue
                        
                        # Parse SRT into individual subtitle entries (by timeframe)
                        subtitle_entries = []
                        entry_lines = []
                        in_subtitle_block = False
                        
                        for line in srt_text.splitlines():
                            line = line.strip('\ufeff').strip()
                            
                            # Empty line separates subtitle entries
                            if not line:
                                if entry_lines:
                                    # Filter out only text lines (not numbers or timestamps)
                                    text_lines = [
                                        l for l in entry_lines 
                                        if not l.isdigit() and '-->' not in l
                                    ]
                                    if text_lines:
                                        subtitle_entries.append(' '.join(text_lines))
                                    entry_lines = []
                                in_subtitle_block = False
                                continue
                                
                            # Check for subtitle number (start of new entry)
                            if line.isdigit() and not in_subtitle_block:
                                in_subtitle_block = True
                                continue
                                
                            # Skip timestamp lines
                            if '-->' in line:
                                continue
                                
                            # Add text lines
                            entry_lines.append(line)
                        
                        # Don't forget the last entry
                        if entry_lines:
                            text_lines = [l for l in entry_lines if not l.isdigit() and '-->' not in l]
                            if text_lines:
                                subtitle_entries.append(' '.join(text_lines))
                        
                        # Skip if no entries were found
                        if not subtitle_entries:
                            self.logger.warning(f"No subtitle entries found in {name}")
                            continue
                            
                        # Extract movie title or episode info from filename
                        filename_clean = os.path.splitext(name)[0].strip()
                        clean_title = self._clean_subtitle_filename(filename_clean)
                        # Create separate subtitle item for each entry
                        for idx, entry_text in enumerate(subtitle_entries):
                            subtitle_items.append({
                                'text': entry_text,
                                'title': f"{clean_title}_entry{idx+1}",
                                'filename': f"{name.rsplit('.', 1)[0]}_entry{idx+1}",
                                'entry_idx': idx,
                                'parent_file': name
                            })
                        
                        file_index += 1
                    
                    # Clean up the temporary file
                    if temp_zip_path and os.path.exists(temp_zip_path):
                        os.remove(temp_zip_path)
                        
                    if not subtitle_items:
                        raise ValueError("ZIP contained subtitles but none could be processed successfully")
                    
                    return {
                        'success': True,
                        '_is_subtitle_zip': True,  # Flag to indicate subtitle ZIP extraction
                        'subtitle_items': subtitle_items,
                        'metadata': {
                            'extraction_method': 'subtitle_zip',
                            'url': url,
                            'num_files': len(subtitle_items)
                        }
                    }
            except zipfile.BadZipFile as e:
                # Not a valid ZIP file
                self.logger.error(f"Invalid ZIP file received: {e}")
                if temp_zip_path:
                    error_content = 'Binary content (not shown)'
                    try:
                        with open(temp_zip_path, 'rb') as f:
                            sample = f.read(200)  # Read first 200 bytes
                            error_content = f"First 200 bytes: {repr(sample)}" 
                    except Exception:
                        pass
                    
                    self.logger.error(f"Content sample: {error_content}")
                
                raise ValueError(f"Invalid ZIP file received from {url}: {e}")
        except Exception as e:
            self.logger.error(f"Subtitle ZIP extraction failed for {url}: {e}")
            return {'success': False, 'error': str(e), 'extraction_method': 'subtitle_zip'}

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
            # Step 1: Pre-emptive HTML sanitization and encoding validation
            sanitized_html = self._sanitize_html(html_content)
            
            # Log content stats for debugging encoding issues
            self.logger.debug(f"HTML content: {len(html_content)} chars, tags: {html_content.count('<')}, encoding indicators: {'charset=' in html_content.lower()}")
            
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