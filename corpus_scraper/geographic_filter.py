"""
Geographic Content Filter for Mexican Spanish identification.
Detects and prioritizes content from Mexico using linguistic and contextual clues.
"""

import logging
import re
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass


@dataclass
class GeographicScore:
    """Geographic relevance scoring for Mexican content."""
    total_score: float
    mexican_indicators: int
    regional_indicators: int
    linguistic_markers: int
    institutional_markers: int
    confidence: str  # 'high', 'medium', 'low'


class GeographicFilter:
    """
    Advanced geographic filter for identifying authentic Mexican Spanish content.
    Uses linguistic patterns, institutional markers, and regional indicators.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Mexican institutional markers (highest confidence)
        self.mexican_institutions = {
            # Government
            'presidencia de méxico', 'gobierno de méxico', 'gob.mx', 'presidencia.gob.mx',
            'cámara de diputados', 'senado de la república', 'congreso de la unión',
            'suprema corte de justicia', 'scjn', 'tribunal electoral', 'tepjf',
            'instituto nacional electoral', 'ine', 'conacyt', 'conapred',
            'secretaría de', 'semar', 'sedena', 'shcp', 'sre', 'sep',
            
            # Academic
            'universidad nacional autónoma de méxico', 'unam', 'ipn', 'colmex',
            'colegio de méxico', 'itesm', 'tecnológico de monterrey', 'uam',
            'universidad autónoma metropolitana', 'ciesas', 'el colegio de',
            
            # Judicial
            'poder judicial de la federación', 'consejo de la judicatura',
            'tribunal superior de justicia', 'fiscalía general de la república',
            
            # Cultural
            'instituto nacional de bellas artes', 'inba', 'conaculta',
            'consejo nacional para la cultura', 'instituto nacional de antropología',
            'inah', 'fondo de cultura económica', 'fce'
        }
        
        # Mexican states and regions
        self.mexican_states = {
            'aguascalientes', 'baja california', 'baja california sur', 'campeche',
            'chiapas', 'chihuahua', 'coahuila', 'colima', 'durango', 'guanajuato',
            'guerrero', 'hidalgo', 'jalisco', 'méxico', 'michoacán', 'morelos',
            'nayarit', 'nuevo león', 'oaxaca', 'puebla', 'querétaro', 'quintana roo',
            'san luis potosí', 'sinaloa', 'sonora', 'tabasco', 'tamaulipas',
            'tlaxcala', 'veracruz', 'yucatán', 'zacatecas', 'ciudad de méxico',
            'cdmx', 'distrito federal'
        }
        
        # Major Mexican cities
        self.mexican_cities = {
            'ciudad de méxico', 'guadalajara', 'monterrey', 'puebla', 'tijuana',
            'león', 'juárez', 'torreón', 'querétaro', 'san luis potosí',
            'mérida', 'mexicali', 'aguascalientes', 'cuernavaca', 'saltillo',
            'hermosillo', 'culiacán', 'durango', 'toluca', 'morelia',
            'villahermosa', 'tuxtla gutiérrez', 'pachuca', 'xalapa', 'veracruz',
            'cancún', 'acapulco', 'mazatlán', 'puerto vallarta', 'playa del carmen'
        }
        
        # Mexican linguistic markers (vocabulary and expressions)
        self.mexican_vocabulary = {
            # Currency and measures
            'peso mexicano', 'pesos', 'centavos', 'banxico', 'banco de méxico',
            
            # Mexican Spanish vocabulary
            'ahorita', 'órale', 'güey', 'neta', 'chido', 'padre', 'padrísimo',
            'gacho', 'fresa', 'naco', 'chilango', 'defeño', 'regiomontano',
            'tapatío', 'poblano', 'jarocho', 'yucateco', 'norteño', 'sureño',
            'antro', 'cantina', 'pulquería', 'merendero', 'fondita',
            'tianguis', 'mercado sobre ruedas', 'ambulantaje',
            
            # Mexican food and culture
            'tacos', 'tamales', 'pozole', 'mole', 'chiles en nogada', 'cochinita pibil',
            'tequila', 'mezcal', 'pulque', 'tepache', 'horchata', 'agua fresca',
            'día de muertos', 'día de los muertos', 'posadas', 'quinceañera',
            'mariachi', 'jarabe tapatío', 'folklor mexicano', 'lucha libre',
            
            # Mexican politics and society
            'pri', 'pan', 'prd', 'morena', 'mcl', 'amlo', 'lópez obrador',
            'sheinbaum', 'claudia sheinbaum', 'andrés manuel', 'cuarta transformación',
            '4t', 'mañaneras', 'conferencia matutina', 'palacio nacional',
            'los pinos', 'residencia oficial', 'zócalo capitalino', 'templo mayor',
            
            # Historical and cultural references
            'revolución mexicana', 'independencia de méxico', 'benito juárez',
            'miguel hidalgo', 'josé maría morelos', 'emiliano zapata', 'pancho villa',
            'lázaro cárdenas', 'sor juana inés', 'frida kahlo', 'diego rivera',
            'octavio paz', 'carlos fuentes', 'juan rulfo', 'elena poniatowska'
        }
        
        # Mexican domains and URLs (institutional indicators)
        self.mexican_domains = {
            '.mx', '.gob.mx', '.org.mx', '.edu.mx', '.com.mx',
            'unam.mx', 'ipn.mx', 'colmex.mx', 'ciesas.edu.mx',
            'scjn.gob.mx', 'diputados.gob.mx', 'senado.gob.mx',
            'presidencia.gob.mx', 'gob.mx'
        }
        
        # Compile regex patterns for efficiency
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Compile regex patterns for efficient matching."""
        # Mexican phone number patterns
        self.phone_patterns = [
            re.compile(r'\b(?:\+52|52)?\s*(?:\d{2}\s*)?(?:\d{4}\s*\d{4}|\d{3}\s*\d{3}\s*\d{4})\b'),
            re.compile(r'\b(?:55|81|33|222|664|656|614|618|867|844|477|442|461|777|443|686|998|984)\s*\d{3}\s*\d{4}\b')
        ]
        
        # Mexican postal code pattern
        self.postal_pattern = re.compile(r'\b(?:C\.P\.?\s*)?(?:0[1-9]|[1-9]\d)\d{3}\b')
        
        # Mexican currency patterns
        self.currency_patterns = [
            re.compile(r'\$\s*\d+(?:\.\d{2})?\s*(?:pesos?|mxn|mx)', re.IGNORECASE),
            re.compile(r'\d+(?:\.\d{2})?\s*pesos?\s*mexicanos?', re.IGNORECASE)
        ]
    
    def analyze_url_indicators(self, url: str) -> Tuple[int, List[str]]:
        """Analyze URL for Mexican indicators."""
        indicators = []
        score = 0
        
        url_lower = url.lower()
        
        # Check for Mexican domains
        for domain in self.mexican_domains:
            if domain in url_lower:
                indicators.append(f"Mexican domain: {domain}")
                score += 10 if domain == '.gob.mx' else 5
        
        # Check for Mexican institutions in URL
        for institution in self.mexican_institutions:
            if institution.replace(' ', '') in url_lower.replace(' ', ''):
                indicators.append(f"Institution in URL: {institution}")
                score += 8
        
        return score, indicators
    
    def analyze_content_geography(self, text: str, title: str = "") -> GeographicScore:
        """
        Analyze text content for Mexican geographic and linguistic markers.
        
        Args:
            text: Main content text
            title: Optional title text
            
        Returns:
            GeographicScore with detailed analysis
        """
        full_text = f"{title} {text}".lower()
        
        mexican_indicators = 0
        regional_indicators = 0
        linguistic_markers = 0
        institutional_markers = 0
        found_indicators = []
        
        # Check institutional markers (highest weight)
        for institution in self.mexican_institutions:
            if institution in full_text:
                institutional_markers += 1
                found_indicators.append(f"Institution: {institution}")
        
        # Check states and regions
        for state in self.mexican_states:
            if state in full_text:
                regional_indicators += 1
                found_indicators.append(f"State: {state}")
        
        # Check cities
        for city in self.mexican_cities:
            if city in full_text:
                regional_indicators += 1
                found_indicators.append(f"City: {city}")
        
        # Check Mexican vocabulary and expressions
        for vocab in self.mexican_vocabulary:
            if vocab in full_text:
                linguistic_markers += 1
                found_indicators.append(f"Vocabulary: {vocab}")
        
        # Check phone numbers
        for pattern in self.phone_patterns:
            matches = pattern.findall(text)
            if matches:
                mexican_indicators += len(matches)
                found_indicators.extend([f"Phone: {match}" for match in matches[:3]])  # Limit to 3
        
        # Check postal codes
        postal_matches = self.postal_pattern.findall(text)
        if postal_matches:
            mexican_indicators += len(postal_matches)
            found_indicators.extend([f"Postal: {match}" for match in postal_matches[:3]])
        
        # Check currency mentions
        for pattern in self.currency_patterns:
            matches = pattern.findall(text)
            if matches:
                mexican_indicators += len(matches)
                found_indicators.extend([f"Currency: {match}" for match in matches[:3]])
        
        # Calculate total score with weights
        total_score = (
            institutional_markers * 10 +  # Highest weight for institutions
            regional_indicators * 5 +     # Medium weight for geography
            linguistic_markers * 3 +      # Medium weight for vocabulary
            mexican_indicators * 2        # Base weight for other indicators
        )
        
        # Determine confidence level
        if institutional_markers >= 2 or total_score >= 30:
            confidence = 'high'
        elif institutional_markers >= 1 or total_score >= 15:
            confidence = 'medium'
        elif total_score >= 5:
            confidence = 'low'
        else:
            confidence = 'very_low'
        
        self.logger.debug(f"Geographic analysis: score={total_score}, confidence={confidence}")
        self.logger.debug(f"Indicators found: {found_indicators[:10]}")  # Log first 10
        
        return GeographicScore(
            total_score=total_score,
            mexican_indicators=mexican_indicators,
            regional_indicators=regional_indicators,
            linguistic_markers=linguistic_markers,
            institutional_markers=institutional_markers,
            confidence=confidence
        )
    
    def is_mexican_content(self, text: str, title: str = "", url: str = "", 
                          min_score: float = 10.0) -> Tuple[bool, GeographicScore, List[str]]:
        """
        Determine if content is Mexican with detailed scoring.
        
        Args:
            text: Content text
            title: Content title
            url: Source URL
            min_score: Minimum score threshold for Mexican classification
            
        Returns:
            Tuple of (is_mexican, score_details, reasons)
        """
        # Analyze URL indicators
        url_score, url_indicators = self.analyze_url_indicators(url)
        
        # Analyze content geography
        content_score = self.analyze_content_geography(text, title)
        
        # Combine scores
        total_score = content_score.total_score + url_score
        
        # Update the score object
        final_score = GeographicScore(
            total_score=total_score,
            mexican_indicators=content_score.mexican_indicators,
            regional_indicators=content_score.regional_indicators,
            linguistic_markers=content_score.linguistic_markers,
            institutional_markers=content_score.institutional_markers,
            confidence=content_score.confidence
        )
        
        # Determine if Mexican
        is_mexican = total_score >= min_score
        
        # Compile reasons
        reasons = url_indicators.copy()
        if content_score.institutional_markers > 0:
            reasons.append(f"{content_score.institutional_markers} institutional markers")
        if content_score.regional_indicators > 0:
            reasons.append(f"{content_score.regional_indicators} regional indicators")
        if content_score.linguistic_markers > 0:
            reasons.append(f"{content_score.linguistic_markers} linguistic markers")
        
        return is_mexican, final_score, reasons
    
    def get_regional_classification(self, text: str) -> Dict[str, Any]:
        """
        Classify content by Mexican region for dialect analysis.
        
        Returns:
            Dictionary with regional classification details
        """
        text_lower = text.lower()
        
        regional_scores = {
            'centro': 0,      # Central Mexico (CDMX, Estado de México, etc.)
            'norte': 0,       # Northern Mexico (Monterrey, Tijuana, etc.)
            'occidente': 0,   # Western Mexico (Guadalajara, Colima, etc.)
            'sureste': 0,     # Southeast (Yucatán, Quintana Roo, etc.)
            'sur': 0,         # South (Oaxaca, Chiapas, etc.)
            'golfo': 0        # Gulf region (Veracruz, Tabasco, etc.)
        }
        
        # Regional vocabulary and markers
        regional_markers = {
            'centro': ['chilango', 'defeño', 'ciudad de méxico', 'cdmx', 'distrito federal', 
                      'toluca', 'estado de méxico', 'cuernavaca', 'pachuca'],
            'norte': ['regiomontano', 'monterrey', 'nuevo león', 'tijuana', 'chihuahua',
                     'hermosillo', 'saltillo', 'culiacán', 'mexicali', 'norteño'],
            'occidente': ['tapatío', 'guadalajara', 'jalisco', 'colima', 'aguascalientes',
                         'guanajuato', 'león', 'bajío', 'occidental'],
            'sureste': ['yucateco', 'mérida', 'yucatán', 'cancún', 'quintana roo',
                       'campeche', 'península de yucatán', 'maya'],
            'sur': ['oaxaca', 'chiapas', 'guerrero', 'tuxtla gutiérrez', 'acapulco',
                   'oaxaqueño', 'chiapaneco', 'zapoteco', 'mixteco'],
            'golfo': ['jarocho', 'veracruz', 'xalapa', 'villahermosa', 'tabasco',
                     'tampico', 'golfo de méxico', 'veracruzano']
        }
        
        for region, markers in regional_markers.items():
            for marker in markers:
                if marker in text_lower:
                    regional_scores[region] += 1
        
        # Find dominant region
        max_region = max(regional_scores.items(), key=lambda x: x[1])
        
        return {
            'dominant_region': max_region[0] if max_region[1] > 0 else 'unknown',
            'regional_scores': regional_scores,
            'confidence': 'high' if max_region[1] >= 3 else 'medium' if max_region[1] >= 1 else 'low'
        }