#!/usr/bin/env python3
"""
Enhanced encoding detection and content validation for MiltronicScrapper.

This module provides robust encoding detection and content quality validation
to ensure scraped text files contain clean, legible Spanish text.
"""

import re
import chardet
import unicodedata
from typing import Optional, Tuple, Dict, Any
import logging

class EncodingValidator:
    """Enhanced encoding detection and content validation."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Common Spanish characters for validation
        self.spanish_chars = set('áéíóúñüÁÉÍÓÚÑÜ¿¡')
        
        # Patterns that indicate binary/corrupted content
        self.binary_patterns = [
            re.compile(r'[£¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿]{3,}'),  # Mixed symbols (lowered threshold)
            re.compile(r'[ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ]{15,}'),  # Extended Latin overuse
            re.compile(r'[⁄¡¢£¤¥¦§¨©ª«¬­®¯°±²³´µ¶·¸¹º»¼½¾¿]{3,}'),  # Mathematical/currency symbols
            re.compile(r'[μÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ]{10,}'),  # Greek/Latin-A
            re.compile(r'[̧̈́̄]{3,}'),  # Combining diacritical marks
            re.compile(r'\s*[^\x20-\x7E\u00A0-\u00FF\u0100-\u017F\u2000-\u206F]{10,}'),  # Non-printable sequences
        ]
        
        # Quality thresholds
        self.min_text_length = 50
        self.max_binary_ratio = 0.15  # 15% max non-text characters
        self.min_spanish_score = 0.1  # Minimum Spanish character presence
        
    def detect_and_validate_encoding(self, content: bytes) -> Tuple[str, bool, Dict[str, Any]]:
        """
        Detect encoding and validate content quality.
        
        Args:
            content: Raw bytes content
            
        Returns:
            Tuple of (detected_encoding, is_valid, validation_info)
        """
        validation_info = {
            'raw_size': len(content),
            'detected_encoding': None,
            'confidence': 0.0,
            'is_binary': False,
            'is_corrupted': False,
            'spanish_score': 0.0,
            'text_quality': 'unknown',
            'issues': []
        }
        
        # Step 1: Detect encoding
        try:
            detection = chardet.detect(content)
            detected_encoding = detection.get('encoding', 'utf-8')
            confidence = detection.get('confidence', 0.0)
            
            validation_info['detected_encoding'] = detected_encoding
            validation_info['confidence'] = confidence
            
            self.logger.debug(f"Detected encoding: {detected_encoding} (confidence: {confidence:.2f})")
            
        except Exception as e:
            self.logger.warning(f"Encoding detection failed: {e}")
            detected_encoding = 'utf-8'
            validation_info['issues'].append(f"detection_error: {str(e)}")
        
        # Step 2: Try to decode and validate content
        text_content = None
        
        # Try multiple encoding strategies
        for encoding in [detected_encoding, 'utf-8', 'latin-1', 'cp1252']:
            if not encoding:
                continue
                
            try:
                text_content = content.decode(encoding, errors='replace')
                break
            except Exception as e:
                validation_info['issues'].append(f"decode_error_{encoding}: {str(e)}")
                continue
        
        if not text_content:
            validation_info['is_corrupted'] = True
            validation_info['text_quality'] = 'corrupted'
            return detected_encoding or 'utf-8', False, validation_info
        
        # Step 3: Validate content quality
        is_valid, quality_info = self._validate_text_quality(text_content)
        validation_info.update(quality_info)
        
        return detected_encoding or 'utf-8', is_valid, validation_info
    
    def _validate_text_quality(self, text: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate text content quality.
        
        Args:
            text: Decoded text content
            
        Returns:
            Tuple of (is_valid, quality_info)
        """
        quality_info = {
            'text_length': len(text),
            'is_binary': False,
            'is_corrupted': False,
            'spanish_score': 0.0,
            'text_quality': 'good',
            'issues': []
        }
        
        # Check minimum length
        if len(text) < self.min_text_length:
            quality_info['issues'].append(f"too_short: {len(text)} < {self.min_text_length}")
            quality_info['text_quality'] = 'too_short'
            return False, quality_info
        
        # Check for binary patterns
        sample = text[:1000]  # Check first 1000 characters
        for i, pattern in enumerate(self.binary_patterns):
            if pattern.search(sample):
                quality_info['is_binary'] = True
                quality_info['issues'].append(f"binary_pattern_{i}_detected")
                quality_info['text_quality'] = 'binary'
                return False, quality_info
        
        # Additional binary detection: excessive non-ASCII special characters
        special_chars = sum(1 for c in sample if ord(c) > 255 or c in '£¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàâãäåæçèêëìîïðòôõö÷øùûüýþÿ⁄μ')
        if len(sample) > 50 and special_chars / len(sample) > 0.2:
            quality_info['is_binary'] = True
            quality_info['issues'].append(f"excessive_special_chars: {special_chars}/{len(sample)} = {special_chars/len(sample):.2%}")
            quality_info['text_quality'] = 'binary'
            return False, quality_info
        
        # Calculate character quality metrics
        printable_chars = sum(1 for c in sample if c.isprintable() or c in '\n\t\r')
        total_chars = len(sample)
        
        if total_chars > 0:
            printable_ratio = printable_chars / total_chars
            if printable_ratio < (1.0 - self.max_binary_ratio):
                quality_info['is_corrupted'] = True
                quality_info['issues'].append(f"low_printable_ratio: {printable_ratio:.2f}")
                quality_info['text_quality'] = 'corrupted'
                return False, quality_info
        
        # Check for Spanish content
        spanish_chars = sum(1 for c in sample if c in self.spanish_chars)
        spanish_score = spanish_chars / total_chars if total_chars > 0 else 0
        quality_info['spanish_score'] = spanish_score
        
        # Check for excessive Unicode replacement characters
        replacement_chars = text.count('�')
        if replacement_chars > 5:
            quality_info['issues'].append(f"replacement_chars: {replacement_chars}")
            if replacement_chars > 20:
                quality_info['text_quality'] = 'corrupted'
                return False, quality_info
        
        # Check for control characters (except whitespace)
        control_chars = sum(1 for c in sample if ord(c) < 32 and c not in '\n\t\r')
        if control_chars > 5:
            quality_info['issues'].append(f"control_chars: {control_chars}")
            quality_info['text_quality'] = 'poor'
        
        # Determine overall quality
        if spanish_score >= self.min_spanish_score:
            quality_info['text_quality'] = 'good'
        elif self._looks_like_spanish_text(sample):
            quality_info['text_quality'] = 'acceptable'
        elif len(quality_info['issues']) > 0:
            quality_info['text_quality'] = 'poor'
        else:
            quality_info['text_quality'] = 'acceptable'
        
        # Consider valid if not corrupted/binary and has reasonable quality
        is_valid = (not quality_info['is_binary'] and 
                   not quality_info['is_corrupted'] and
                   quality_info['text_quality'] in ['good', 'acceptable'])
        
        return is_valid, quality_info
    
    def _looks_like_spanish_text(self, text: str) -> bool:
        """
        Check if text looks like Spanish content using heuristics.
        
        Args:
            text: Text to analyze
            
        Returns:
            True if text appears to be Spanish
        """
        # Common Spanish words and patterns
        spanish_indicators = [
            r'\b(el|la|los|las|un|una|de|del|en|con|por|para|que|se|es|son|está|están)\b',
            r'\b(y|o|pero|sino|como|cuando|donde|porque|si|ya|no|sí|muy|más|menos)\b',
            r'\b(tiene|tienen|hacer|hace|ser|estar|ir|va|van|puede|pueden)\b'
        ]
        
        # Count Spanish indicators
        indicator_count = 0
        text_lower = text.lower()
        
        for pattern in spanish_indicators:
            matches = re.findall(pattern, text_lower)
            indicator_count += len(matches)
        
        # Also check for Spanish characters
        spanish_char_count = sum(1 for c in text if c in self.spanish_chars)
        
        # Consider it Spanish if we have enough indicators
        words = len(text.split())
        if words > 0:
            indicator_ratio = indicator_count / words
            return indicator_ratio > 0.05 or spanish_char_count > 0
        
        return spanish_char_count > 0
    
    def clean_and_normalize_text(self, text: str) -> str:
        """
        Clean and normalize text content.
        
        Args:
            text: Raw text content
            
        Returns:
            Cleaned and normalized text
        """
        if not text:
            return ""
        
        try:
            # Unicode normalization
            text = unicodedata.normalize('NFC', text)
            
            # Remove null bytes and replacement characters
            text = text.replace('\x00', '')
            text = text.replace('\ufffd', '')
            
            # Fix common encoding artifacts
            text = text.replace('Ã¡', 'á')
            text = text.replace('Ã©', 'é')
            text = text.replace('Ã­', 'í')
            text = text.replace('Ã³', 'ó')
            text = text.replace('Ãº', 'ú')
            text = text.replace('Ã±', 'ñ')
            text = text.replace('Ã¼', 'ü')
            text = text.replace('Â¿', '¿')
            text = text.replace('Â¡', '¡')
            
            # Remove excessive control characters
            text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
            
            # Normalize whitespace
            text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)  # Max 2 consecutive newlines
            text = re.sub(r'[ \t]+', ' ', text)  # Normalize spaces
            text = re.sub(r'\r\n', '\n', text)  # Normalize line endings
            
            # Remove leading/trailing whitespace
            text = text.strip()
            
            return text
            
        except Exception as e:
            self.logger.warning(f"Text cleaning failed: {e}")
            return text
    
    def validate_file_content(self, filepath: str) -> Dict[str, Any]:
        """
        Validate content of an existing file.
        
        Args:
            filepath: Path to the file to validate
            
        Returns:
            Validation results dictionary
        """
        try:
            with open(filepath, 'rb') as f:
                content = f.read()
            
            encoding, is_valid, validation_info = self.detect_and_validate_encoding(content)
            
            validation_info.update({
                'filepath': filepath,
                'is_valid': is_valid,
                'recommended_action': self._get_recommended_action(validation_info)
            })
            
            return validation_info
            
        except Exception as e:
            return {
                'filepath': filepath,
                'is_valid': False,
                'error': str(e),
                'recommended_action': 'delete'
            }
    
    def _get_recommended_action(self, validation_info: Dict[str, Any]) -> str:
        """Get recommended action based on validation results."""
        if validation_info.get('is_binary') or validation_info.get('is_corrupted'):
            return 'delete'
        elif validation_info.get('text_quality') == 'poor':
            return 'review'
        elif validation_info.get('issues'):
            return 'clean'
        else:
            return 'keep'