"""
Specialized extractors for Mexican government and institutional websites.
Custom handlers optimized for specific Mexican government portals and academic sites.
"""

import logging
import re
from typing import Dict, Optional, Any, List
from bs4 import BeautifulSoup
from urllib.parse import urlparse


class SpecializedMexicanExtractors:
    """
    Collection of specialized extractors for Mexican government and institutional sites.
    Each extractor is optimized for specific website structures and content types.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Site-specific extraction rules
        self.extraction_rules = {
            # Letras.com - lyrics website
            'letras.com': {
                'selectors': [
                    'div.cnt-letra',  # Main lyrics container
                    '.lyric-original',  # Original lyrics
                    '.letra-l',  # Alternative lyrics container
                ],
                'remove_selectors': [
                    '.cnt-tlix',  # Ads and related content
                    '.banner',  # Banners and ads
                    '.header',  # Header menu
                    '.footer',  # Footer
                ],
                'metadata_selectors': {
                    'title': ['h1', '.cnt-head_title', '.head-title'],
                    'artist': ['h2 a', '.cnt-head_desc a', '.head-subtitle'],
                    'album': ['.letra-info a'],
                    'language': ['[data-language]']
                }
            },
            
            # Government portals
            'gob.mx': {
                'selectors': [
                    '.article-body',
                    '.contenido-articulo',
                    '.field-item',
                    '.content-area',
                    'article.node',
                    '.contenido'
                ],
                'remove_selectors': [
                    '.social-share',
                    '.compartir',
                    '.redes-sociales',
                    '.nav-secondary',
                    '.sidebar',
                    '.widget-area'
                ],
                'metadata_selectors': {
                    'title': ['h1.title', '.article-title', 'h1'],
                    'date': ['time', '.fecha', '.date', '[datetime]'],
                    'author': ['.autor', '.author', '.by-author']
                }
            },
            
            # Supreme Court
            'scjn.gob.mx': {
                'selectors': [
                    '.publicacion-contenido',
                    '.articulo-texto',
                    '.field-item',
                    '.contenido-principal',
                    '.sentencia-texto',
                    '.jurisprudencia-contenido'
                ],
                'remove_selectors': [
                    '.menu-lateral',
                    '.barra-herramientas',
                    '.navegacion-secundaria'
                ],
                'metadata_selectors': {
                    'title': ['.titulo-publicacion', 'h1.title', 'h1'],
                    'date': ['.fecha-publicacion', 'time', '.fecha'],
                    'type': ['.tipo-documento', '.categoria']
                }
            },
            
            # Chamber of Deputies
            'diputados.gob.mx': {
                'selectors': [
                    '.ley-contenido',
                    '.articulo-texto',
                    '.stenographic-version',
                    '.debate-content',
                    '.contenido-principal'
                ],
                'remove_selectors': [
                    '.menu-navegacion',
                    '.herramientas-laterales'
                ],
                'special_processing': 'legislature'
            },
            
            # Congress debates
            'cronica.diputados.gob.mx': {
                'selectors': [
                    '.debate-content',
                    '.stenographic-version',
                    '.contenido-debate',
                    '.transcripcion'
                ],
                'remove_selectors': [
                    '.menu-lateral',
                    '.herramientas'
                ],
                'special_processing': 'debate_transcript'
            },
            
            # UNAM repositories
            'unam.mx': {
                'selectors': [
                    '.article-content',
                    '.contenido-obra',
                    '.field-item',
                    '.contenido-academico',
                    '.texto-completo'
                ],
                'remove_selectors': [
                    '.menu-lateral',
                    '.sidebar-academic',
                    '.herramientas-repositorio'
                ],
                'metadata_selectors': {
                    'title': ['.titulo-obra', '.article-title', 'h1'],
                    'author': ['.autor-obra', '.author', '.investigador'],
                    'institution': ['.institucion', '.departamento']
                }
            },
            
            # DOF (Official Gazette)
            'dof.gob.mx': {
                'selectors': [
                    '.documento-contenido',
                    '.nota-contenido',
                    '.contenido-dof',
                    '.texto-oficial'
                ],
                'remove_selectors': [
                    '.herramientas-dof',
                    '.menu-servicios'
                ],
                'special_processing': 'official_gazette'
            },
            
            # Mexican news sites
            'jornada.com.mx': {
                'selectors': [
                    '.texto',
                    '.article-text',
                    '.contenido-nota'
                ],
                'remove_selectors': [
                    '.publicidad',
                    '.relacionadas',
                    '.redes'
                ]
            },
            
            'eluniversal.com.mx': {
                'selectors': [
                    '.field-item',
                    '.article-body',
                    '.nota-texto'
                ],
                'remove_selectors': [
                    '.ads',
                    '.related-news',
                    '.social-bar'
                ]
            },
            
            'milenio.com': {
                'selectors': [
                    '.nota-texto',
                    '.article-content',
                    '.contenido-nota'
                ],
                'remove_selectors': [
                    '.widget-publicidad',
                    '.notas-relacionadas'
                ]
            }
        }
    
    def get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            return urlparse(url).netloc.lower()
        except:
            return ""
    
    def find_matching_rule(self, url: str) -> Optional[Dict[str, Any]]:
        """Find the best matching extraction rule for a URL."""
        domain = self.get_domain(url)
        
        # Direct domain match
        for rule_domain, rules in self.extraction_rules.items():
            if rule_domain in domain:
                return rules
        
        # Partial matches for subdomains
        for rule_domain, rules in self.extraction_rules.items():
            domain_parts = rule_domain.split('.')
            if len(domain_parts) >= 2:
                main_domain = '.'.join(domain_parts[-2:])
                if main_domain in domain:
                    return rules
        
        return None
    
    def extract_metadata(self, soup: BeautifulSoup, metadata_selectors: Dict[str, List[str]]) -> Dict[str, str]:
        """Extract metadata using specialized selectors."""
        metadata = {}
        
        for field, selectors in metadata_selectors.items():
            for selector in selectors:
                try:
                    element = soup.select_one(selector)
                    if element:
                        # Handle different attribute extractions
                        if field == 'date' and element.has_attr('datetime'):
                            metadata[field] = element['datetime']
                        else:
                            text = element.get_text(strip=True)
                            if text:
                                metadata[field] = text
                        break
                except Exception as e:
                    self.logger.debug(f"Error extracting {field} with selector {selector}: {e}")
                    continue
        
        return metadata
    
    def process_debate_transcript(self, soup: BeautifulSoup) -> str:
        """Special processing for legislative debate transcripts."""
        try:
            # Find all speaker interventions
            interventions = []
            
            # Common patterns for speaker identification
            speaker_patterns = [
                re.compile(r'^(EL|LA)\s+(PRESIDENTE|PRESIDENTA|SECRETARIO|SECRETARIA|DIPUTADO|DIPUTADA)\s+([^:]+):', re.IGNORECASE),
                re.compile(r'^([A-ZÁÉÍÓÚÑ\s]+):\s*', re.IGNORECASE),
                re.compile(r'^\s*-\s*([^-]+)\s*-\s*')
            ]
            
            # Extract text and identify speakers
            text_content = soup.get_text(separator='\n')
            lines = text_content.split('\n')
            
            current_speaker = None
            current_intervention = []
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Check if line contains speaker identification
                speaker_found = False
                for pattern in speaker_patterns:
                    match = pattern.match(line)
                    if match:
                        # Save previous intervention
                        if current_speaker and current_intervention:
                            interventions.append(f"{current_speaker}: {' '.join(current_intervention)}")
                        
                        # Start new intervention
                        current_speaker = match.group(1) if match.lastindex >= 1 else "SPEAKER"
                        current_intervention = [line[match.end():].strip()]
                        speaker_found = True
                        break
                
                if not speaker_found and current_speaker:
                    current_intervention.append(line)
            
            # Add final intervention
            if current_speaker and current_intervention:
                interventions.append(f"{current_speaker}: {' '.join(current_intervention)}")
            
            # Return formatted transcript
            if interventions:
                return '\n\n'.join(interventions)
            else:
                return text_content
                
        except Exception as e:
            self.logger.warning(f"Error processing debate transcript: {e}")
            return soup.get_text(separator='\n', strip=True)
    
    def process_official_gazette(self, soup: BeautifulSoup) -> str:
        """Special processing for DOF (Official Gazette) documents."""
        try:
            # Remove navigation and administrative elements
            for selector in ['.menu-dof', '.herramientas', '.navegacion']:
                for elem in soup.select(selector):
                    elem.decompose()
            
            # Extract main document content
            content_selectors = [
                '.documento-contenido',
                '.contenido-dof',
                '.texto-oficial',
                '.contenido-principal'
            ]
            
            main_content = None
            for selector in content_selectors:
                main_content = soup.select_one(selector)
                if main_content:
                    break
            
            if not main_content:
                main_content = soup.find('body')
            
            if main_content:
                # Clean up common gazette formatting
                text = main_content.get_text(separator='\n', strip=True)
                
                # Remove page numbers and administrative headers
                text = re.sub(r'\n\s*Página\s+\d+.*?\n', '\n', text, flags=re.IGNORECASE)
                text = re.sub(r'\n\s*DIARIO OFICIAL.*?\n', '\n', text, flags=re.IGNORECASE)
                text = re.sub(r'\n\s*\d{1,2}\s+de\s+\w+\s+de\s+\d{4}\s*\n', '\n', text, flags=re.IGNORECASE)
                
                # Normalize whitespace
                text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)
                
                return text.strip()
            
            return soup.get_text(separator='\n', strip=True)
            
        except Exception as e:
            self.logger.warning(f"Error processing official gazette: {e}")
            return soup.get_text(separator='\n', strip=True)
    
    def process_legislature_content(self, soup: BeautifulSoup) -> str:
        """Special processing for legislative content (laws, regulations)."""
        try:
            # Remove navigation and tools
            for selector in ['.menu-legislacion', '.herramientas-ley', '.navegacion']:
                for elem in soup.select(selector):
                    elem.decompose()
            
            # Extract structured legal content
            main_content = soup.select_one('.ley-contenido, .articulo-texto, .contenido-principal')
            
            if main_content:
                # Preserve legal structure
                text = main_content.get_text(separator='\n', strip=True)
                
                # Clean up legal formatting
                text = re.sub(r'\n\s*Artículo\s+(\d+[.-])', r'\n\nArtículo \1', text)
                text = re.sub(r'\n\s*Capítulo\s+([IVX]+)', r'\n\nCapítulo \1', text)
                text = re.sub(r'\n\s*Título\s+([IVX]+)', r'\n\nTítulo \1', text)
                
                return text.strip()
            
            return soup.get_text(separator='\n', strip=True)
            
        except Exception as e:
            self.logger.warning(f"Error processing legislature content: {e}")
            return soup.get_text(separator='\n', strip=True)
    
    def extract_specialized(self, html_content: str, url: str) -> Dict[str, Any]:
        """
        Extract content using specialized rules for Mexican government sites.
        
        Args:
            html_content: Raw HTML content
            url: Source URL
            
        Returns:
            Dictionary with extracted content and metadata
        """
        result = {
            'success': False,
            'text': None,
            'metadata': {},
            'extraction_method': 'specialized',
            'specialized_for': None
        }
        
        try:
            # Find matching extraction rule
            rules = self.find_matching_rule(url)
            if not rules:
                return result
            
            domain = self.get_domain(url)
            result['specialized_for'] = domain
            
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Remove unwanted elements
            if 'remove_selectors' in rules:
                for selector in rules['remove_selectors']:
                    for elem in soup.select(selector):
                        elem.decompose()
            
            # Extract metadata if selectors provided
            metadata = {}
            if 'metadata_selectors' in rules:
                metadata = self.extract_metadata(soup, rules['metadata_selectors'])
            
            # Extract main content
            main_text = None
            
            # Try specialized selectors
            for selector in rules.get('selectors', []):
                content_elem = soup.select_one(selector)
                if content_elem:
                    main_text = content_elem.get_text(separator='\n', strip=True)
                    if main_text and len(main_text) > 100:  # Minimum content threshold
                        break
            
            # Apply special processing if specified
            special_processing = rules.get('special_processing')
            if special_processing and main_text:
                if special_processing == 'debate_transcript':
                    main_text = self.process_debate_transcript(soup)
                elif special_processing == 'official_gazette':
                    main_text = self.process_official_gazette(soup)
                elif special_processing == 'legislature':
                    main_text = self.process_legislature_content(soup)
            
            if main_text and len(main_text.strip()) > 50:
                result.update({
                    'success': True,
                    'text': main_text.strip(),
                    'metadata': {
                        **metadata,
                        'domain': domain,
                        'extraction_rule': special_processing or 'standard',
                        'content_length': len(main_text)
                    }
                })
                
                self.logger.info(f"Specialized extraction successful for {domain}: {len(main_text)} chars")
            else:
                result['error'] = "No substantial content found with specialized extractors"
            
        except Exception as e:
            self.logger.error(f"Specialized extraction failed for {url}: {e}")
            result['error'] = str(e)
        
        return result
    
    def is_specialized_site(self, url: str) -> bool:
        """Check if URL belongs to a site with specialized extraction rules."""
        return self.find_matching_rule(url) is not None
    
    def get_supported_domains(self) -> List[str]:
        """Get list of domains with specialized extraction support."""
        return list(self.extraction_rules.keys())