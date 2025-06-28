"""
PDF Content Extractor for Mexican academic papers and government documents.
Handles PDF text extraction, metadata parsing, and quality validation.
"""

import logging
import tempfile
import os
from typing import Dict, Optional, Any, List
from urllib.parse import urlparse
import requests
import PyPDF2
import pdfplumber
from io import BytesIO


class PDFExtractor:
    """
    Advanced PDF content extractor optimized for Mexican academic and government documents.
    Supports multiple extraction methods with fallback mechanisms.
    """
    
    def __init__(self, politeness_config: Dict):
        self.politeness_config = politeness_config
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        
        # Configure session for PDF downloads
        self.session.headers.update({
            'User-Agent': 'Mexican Academic Corpus Builder/1.0 (Research Purpose)',
            'Accept': 'application/pdf,*/*',
            'Accept-Language': 'es-MX,es;q=0.9,en;q=0.8',
        })
        
        # Mexican academic institution patterns
        self.academic_institutions = {
            'unam', 'ipn', 'colmex', 'ciesas', 'cide', 'itesm', 'uam',
            'universidad', 'instituto', 'colegio', 'centro de investigación'
        }
        
        # Government document patterns
        self.government_patterns = {
            'scjn', 'dof', 'diputados', 'senado', 'presidencia', 'secretaría',
            'instituto nacional', 'consejo nacional', 'comisión nacional'
        }
    
    def is_pdf_url(self, url: str) -> bool:
        """Check if URL points to a PDF file."""
        return (url.lower().endswith('.pdf') or 
                'pdf' in url.lower() or
                '/pdf/' in url.lower())
    
    def download_pdf(self, url: str) -> Optional[bytes]:
        """
        Download PDF content from URL.
        
        Args:
            url: PDF URL to download
            
        Returns:
            PDF content as bytes or None if failed
        """
        try:
            self.logger.info(f"Downloading PDF: {url}")
            
            # Apply rate limiting
            timeout = self.politeness_config.get('timeout', 60)  # Longer timeout for PDFs
            
            response = self.session.get(url, timeout=timeout, stream=True)
            response.raise_for_status()
            
            # Check content type
            content_type = response.headers.get('content-type', '').lower()
            if 'pdf' not in content_type and not self.is_pdf_url(url):
                self.logger.warning(f"URL does not appear to be a PDF: {url}")
                return None
            
            # Download content
            pdf_content = response.content
            
            # Basic PDF validation
            if not pdf_content.startswith(b'%PDF-'):
                self.logger.warning(f"Downloaded content is not a valid PDF: {url}")
                return None
            
            self.logger.info(f"Successfully downloaded PDF: {len(pdf_content)} bytes from {url}")
            return pdf_content
            
        except Exception as e:
            self.logger.error(f"Failed to download PDF {url}: {e}")
            return None
    
    def extract_with_pypdf2(self, pdf_content: bytes) -> Optional[Dict[str, Any]]:
        """Extract text using PyPDF2."""
        try:
            pdf_file = BytesIO(pdf_content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text_parts = []
            metadata = {
                'total_pages': len(pdf_reader.pages),
                'method': 'PyPDF2'
            }
            
            # Extract metadata if available
            if pdf_reader.metadata:
                metadata.update({
                    'title': pdf_reader.metadata.get('/Title', ''),
                    'author': pdf_reader.metadata.get('/Author', ''),
                    'subject': pdf_reader.metadata.get('/Subject', ''),
                    'creator': pdf_reader.metadata.get('/Creator', ''),
                    'creation_date': str(pdf_reader.metadata.get('/CreationDate', '')),
                })
            
            # Extract text from all pages
            for page_num, page in enumerate(pdf_reader.pages):
                try:
                    page_text = page.extract_text()
                    if page_text and page_text.strip():
                        text_parts.append(page_text)
                except Exception as e:
                    self.logger.warning(f"Failed to extract page {page_num + 1}: {e}")
                    continue
            
            full_text = '\n\n'.join(text_parts)
            
            if full_text.strip():
                return {
                    'text': full_text,
                    'metadata': metadata,
                    'success': True
                }
            
            return None
            
        except Exception as e:
            self.logger.warning(f"PyPDF2 extraction failed: {e}")
            return None
    
    def extract_with_pdfplumber(self, pdf_content: bytes) -> Optional[Dict[str, Any]]:
        """Extract text using pdfplumber (better for complex layouts)."""
        try:
            pdf_file = BytesIO(pdf_content)
            
            text_parts = []
            tables_found = 0
            
            with pdfplumber.open(pdf_file) as pdf:
                metadata = {
                    'total_pages': len(pdf.pages),
                    'method': 'pdfplumber'
                }
                
                # Extract PDF metadata
                if pdf.metadata:
                    metadata.update({
                        'title': pdf.metadata.get('Title', ''),
                        'author': pdf.metadata.get('Author', ''),
                        'subject': pdf.metadata.get('Subject', ''),
                        'creator': pdf.metadata.get('Creator', ''),
                        'creation_date': str(pdf.metadata.get('CreationDate', '')),
                    })
                
                for page_num, page in enumerate(pdf.pages):
                    try:
                        # Extract text
                        page_text = page.extract_text()
                        if page_text and page_text.strip():
                            text_parts.append(page_text)
                        
                        # Extract tables if present
                        tables = page.extract_tables()
                        if tables:
                            tables_found += len(tables)
                            for table in tables:
                                # Convert table to text
                                table_text = '\n'.join([
                                    '\t'.join([cell or '' for cell in row]) 
                                    for row in table if row
                                ])
                                text_parts.append(f"\n[TABLA]\n{table_text}\n[/TABLA]\n")
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to extract page {page_num + 1}: {e}")
                        continue
            
            metadata['tables_found'] = tables_found
            full_text = '\n\n'.join(text_parts)
            
            if full_text.strip():
                return {
                    'text': full_text,
                    'metadata': metadata,
                    'success': True
                }
            
            return None
            
        except Exception as e:
            self.logger.warning(f"pdfplumber extraction failed: {e}")
            return None
    
    def clean_pdf_text(self, text: str) -> str:
        """Clean and normalize PDF extracted text."""
        import re
        
        # Remove excessive whitespace
        text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)  # Multiple newlines to double
        text = re.sub(r'[ \t]+', ' ', text)            # Multiple spaces to single
        
        # Remove page numbers and headers/footers (common patterns)
        text = re.sub(r'\n\s*\d+\s*\n', '\n', text)   # Standalone page numbers
        text = re.sub(r'\n\s*Página \d+.*?\n', '\n', text, flags=re.IGNORECASE)
        
        # Remove common PDF artifacts
        text = re.sub(r'[●•▪▫■□]', '', text)           # Bullet points
        text = re.sub(r'[-_]{3,}', '', text)           # Long dashes/underscores
        
        # Fix common hyphenation issues
        text = re.sub(r'(\w+)-\s*\n\s*(\w+)', r'\1\2', text)  # Word split across lines
        
        # Normalize quotes
        text = re.sub(r'["""]', '"', text)
        text = re.sub(r"[''']", "'", text)
        
        return text.strip()
    
    def analyze_document_type(self, text: str, metadata: Dict) -> Dict[str, Any]:
        """Analyze the type and characteristics of the PDF document."""
        text_lower = text.lower()
        title = metadata.get('title', '').lower()
        author = metadata.get('author', '').lower()
        
        analysis = {
            'document_type': 'unknown',
            'is_academic': False,
            'is_government': False,
            'language_quality': 'medium',
            'formality_level': 'medium'
        }
        
        # Check for academic document
        academic_indicators = [
            'resumen', 'abstract', 'introducción', 'metodología', 'conclusiones',
            'referencias', 'bibliografía', 'universidad', 'instituto', 'tesis',
            'artículo', 'investigación', 'estudio', 'análisis'
        ]
        
        academic_score = sum(1 for indicator in academic_indicators if indicator in text_lower)
        
        # Check for government document
        government_score = sum(1 for pattern in self.government_patterns if pattern in text_lower)
        
        # Determine document type
        if academic_score >= 3:
            analysis['document_type'] = 'academic'
            analysis['is_academic'] = True
            analysis['formality_level'] = 'high'
        elif government_score >= 2:
            analysis['document_type'] = 'government'
            analysis['is_government'] = True
            analysis['formality_level'] = 'very_high'
        elif any(word in text_lower for word in ['ley', 'decreto', 'reglamento', 'norma']):
            analysis['document_type'] = 'legal'
            analysis['formality_level'] = 'very_high'
        elif any(word in text_lower for word in ['manual', 'guía', 'instructivo']):
            analysis['document_type'] = 'manual'
            analysis['formality_level'] = 'high'
        
        # Assess language quality (formal Mexican Spanish indicators)
        quality_indicators = [
            'asimismo', 'por tanto', 'en consecuencia', 'no obstante',
            'sin embargo', 'por consiguiente', 'en efecto', 'cabe señalar',
            'es menester', 'resulta pertinente', 'en tal sentido'
        ]
        
        quality_score = sum(1 for indicator in quality_indicators if indicator in text_lower)
        
        if quality_score >= 5:
            analysis['language_quality'] = 'very_high'
        elif quality_score >= 3:
            analysis['language_quality'] = 'high'
        elif quality_score >= 1:
            analysis['language_quality'] = 'medium'
        else:
            analysis['language_quality'] = 'basic'
        
        return analysis
    
    def extract_pdf_content(self, url: str) -> Dict[str, Any]:
        """
        Main method to extract content from PDF URL.
        
        Args:
            url: PDF URL to process
            
        Returns:
            Dictionary with extraction results
        """
        result = {
            'success': False,
            'text': None,
            'metadata': {},
            'error': None,
            'extraction_method': None
        }
        
        try:
            # Download PDF
            pdf_content = self.download_pdf(url)
            if not pdf_content:
                result['error'] = "Failed to download PDF"
                return result
            
            # Try pdfplumber first (better for complex layouts)
            extraction_result = self.extract_with_pdfplumber(pdf_content)
            
            # Fall back to PyPDF2 if pdfplumber fails
            if not extraction_result:
                extraction_result = self.extract_with_pypdf2(pdf_content)
            
            if not extraction_result:
                result['error'] = "All PDF extraction methods failed"
                return result
            
            # Clean and process text
            raw_text = extraction_result['text']
            cleaned_text = self.clean_pdf_text(raw_text)
            
            # Analyze document
            doc_analysis = self.analyze_document_type(cleaned_text, extraction_result['metadata'])
            
            # Combine metadata
            combined_metadata = {
                **extraction_result['metadata'],
                **doc_analysis,
                'source_url': url,
                'file_type': 'pdf',
                'raw_length': len(raw_text),
                'cleaned_length': len(cleaned_text)
            }
            
            result.update({
                'success': True,
                'text': cleaned_text,
                'metadata': combined_metadata,
                'extraction_method': extraction_result['metadata']['method']
            })
            
            self.logger.info(
                f"PDF extraction successful: {len(cleaned_text)} chars, "
                f"type: {doc_analysis['document_type']}, "
                f"method: {extraction_result['metadata']['method']}"
            )
            
        except Exception as e:
            self.logger.error(f"PDF extraction failed for {url}: {e}")
            result['error'] = str(e)
        
        return result
    
    def close(self):
        """Clean up resources."""
        self.session.close()