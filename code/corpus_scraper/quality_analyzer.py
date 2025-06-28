"""
Advanced Quality Analyzer for Mexican Spanish corpus content.
Evaluates dialect authenticity, formality levels, and linguistic quality.
"""

import logging
import re
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
import spacy
from collections import Counter


@dataclass
class QualityScore:
    """Comprehensive quality scoring for Mexican Spanish content."""
    overall_score: float
    dialect_authenticity: float
    formality_level: float
    linguistic_complexity: float
    vocabulary_richness: float
    mexican_cultural_content: float
    confidence: str


class MexicanSpanishQualityAnalyzer:
    """
    Advanced quality analyzer specifically designed for Mexican Spanish content.
    Evaluates linguistic features, cultural authenticity, and formality levels.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Initialize spaCy for advanced linguistic analysis
        try:
            self.nlp = spacy.load("es_core_news_sm")
            self.logger.info("Loaded spaCy Spanish model for quality analysis")
        except OSError:
            self.logger.warning("Spanish spaCy model not found, using blank model")
            self.nlp = spacy.blank("es")
            self.nlp.add_pipe("sentencizer")
        
        # Mexican Spanish dialect markers
        self.mexican_dialectisms = {
            # Lexical mexicanisms
            'lexical': {
                'ahorita', 'órale', 'güey', 'wey', 'neta', 'chido', 'padre', 'padrísimo',
                'gacho', 'fresa', 'naco', 'antro', 'cantina', 'tianguis', 'quesadilla',
                'torta', 'bolillo', 'chilango', 'defeño', 'regiomontano', 'tapatío',
                'poblano', 'jarocho', 'yucateco', 'norteño', 'sureño', 'bajacaliforniano',
                'camote', 'elote', 'esquite', 'jícama', 'mamey', 'chicozapote',
                'tepache', 'pulque', 'atole', 'champurrado', 'pozole', 'menudo',
                'machaca', 'carnitas', 'barbacoa', 'cochinita', 'salsita', 'picosito',
                'cuate', 'compadre', 'comadre', 'manito', 'carnala', 'carnalito'
            },
            
            # Mexican expressions and phrases
            'expressions': {
                'no manches', 'qué padre', 'está padrísimo', 'qué gacho', 'de volada',
                'al chile', 'está cañón', 'qué pedo', 'ni modo', 'órale pues',
                'ándale pues', 'ahí nos vemos', 'qué onda', 'cómo estás', 'dónde andas',
                'ahorita vengo', 'ya merito', 'luego luego', 'de perdida', 'por si las moscas',
                'dar la vuelta', 'echar la hueva', 'hacer la lucha', 'ponerse trucha',
                'está de pelos', 'está chingón', 'vale madre', 'está dlv', 'ni pedo'
            },
            
            # Diminutive usage (very Mexican)
            'diminutives': {
                'ahorita', 'poquito', 'chiquito', 'rapidito', 'despacito', 'cerquita',
                'lejitos', 'tantito', 'nadita', 'solito', 'quietito', 'calladito'
            }
        }
        
        # Formality level indicators
        self.formality_indicators = {
            'very_high': {
                'asimismo', 'por tanto', 'en consecuencia', 'no obstante', 'sin embargo',
                'por consiguiente', 'en efecto', 'cabe señalar', 'es menester',
                'resulta pertinente', 'en tal sentido', 'dicho esto', 'habida cuenta',
                'a tenor de', 'en virtud de', 'a la luz de', 'dado que', 'puesto que',
                'toda vez que', 'en tanto que', 'por cuanto', 'atento a lo anterior',
                'en este orden de ideas', 'de conformidad con', 'en cumplimiento de'
            },
            'high': {
                'además', 'igualmente', 'también', 'asimismo', 'del mismo modo',
                'por otra parte', 'por un lado', 'por otro lado', 'en primer lugar',
                'en segundo lugar', 'finalmente', 'en conclusión', 'para concluir',
                'en resumen', 'en síntesis', 'dicho de otro modo', 'es decir',
                'esto es', 'por ejemplo', 'tal como', 'así como', 'de esta manera',
                'de este modo', 'por lo tanto', 'así pues', 'entonces'
            },
            'medium': {
                'pero', 'aunque', 'sin embargo', 'mientras', 'cuando', 'donde',
                'porque', 'ya que', 'como', 'si', 'para que', 'con el fin de',
                'a pesar de', 'a través de', 'mediante', 'según', 'conforme',
                'respecto a', 'en cuanto a', 'en relación con', 'acerca de'
            }
        }
        
        # Academic and technical vocabulary
        self.academic_vocabulary = {
            'metodología', 'análisis', 'investigación', 'estudio', 'evaluación',
            'diagnóstico', 'planteamiento', 'hipótesis', 'resultados', 'conclusiones',
            'recomendaciones', 'propuesta', 'implementación', 'desarrollo',
            'contextualización', 'fundamentación', 'conceptualización',
            'sistematización', 'caracterización', 'identificación', 'descripción',
            'interpretación', 'comprensión', 'explicación', 'justificación'
        }
        
        # Mexican cultural and historical references
        self.cultural_references = {
            'history': {
                'revolución mexicana', 'independencia de méxico', 'conquista',
                'virreinato', 'porfiriato', 'cristiada', 'movimiento estudiantil',
                'dos de octubre', 'tlatelolco', 'benito juárez', 'miguel hidalgo',
                'josé maría morelos', 'emiliano zapata', 'pancho villa', 'francisco madero',
                'venustiano carranza', 'álvaro obregón', 'lázaro cárdenas', 'sor juana'
            },
            'culture': {
                'día de muertos', 'día de los muertos', 'posadas', 'quinceañera',
                'grito de independencia', 'cinco de mayo', 'virgen de guadalupe',
                'mariachi', 'jarabe tapatío', 'danza folklórica', 'lucha libre',
                'frida kahlo', 'diego rivera', 'josé clemente orozco', 'david alfaro siqueiros',
                'octavio paz', 'carlos fuentes', 'juan rulfo', 'elena poniatowska',
                'carlos monsiváis', 'josé emilio pacheco', 'jaime sabines'
            },
            'geography': {
                'valle de méxico', 'altiplano', 'sierra madre', 'golfo de méxico',
                'mar de cortés', 'península de yucatán', 'península de baja california',
                'istmo de tehuantepec', 'río bravo', 'lago de chapala', 'popocatépetl',
                'iztaccíhuatl', 'pico de orizaba', 'nevado de toluca', 'cofre de perote'
            }
        }
        
        # Complex sentence patterns (indicating sophistication)
        self.complexity_patterns = [
            re.compile(r'\b(?:aunque|si bien|aun cuando|a pesar de que|pese a que)\b.*?\b(?:sin embargo|no obstante|empero)\b', re.IGNORECASE),
            re.compile(r'\b(?:no solo|no solamente|no únicamente).*?\bsino (?:que )?también\b', re.IGNORECASE),
            re.compile(r'\b(?:tanto|así como|igual que).*?\bcomo\b', re.IGNORECASE),
            re.compile(r'\b(?:por un lado|por una parte).*?\b(?:por otro|por otra parte)\b', re.IGNORECASE),
            re.compile(r'\b(?:en primer lugar|primeramente).*?\b(?:en segundo lugar|posteriormente|finalmente)\b', re.IGNORECASE)
        ]
    
    def analyze_dialect_authenticity(self, text: str) -> Dict[str, Any]:
        """Analyze Mexican Spanish dialect authenticity."""
        text_lower = text.lower()
        
        # Count dialectisms
        lexical_count = sum(1 for word in self.mexican_dialectisms['lexical'] if word in text_lower)
        expressions_count = sum(1 for expr in self.mexican_dialectisms['expressions'] if expr in text_lower)
        diminutives_count = sum(1 for dim in self.mexican_dialectisms['diminutives'] if dim in text_lower)
        
        total_dialectisms = lexical_count + expressions_count + diminutives_count
        
        # Calculate density (dialectisms per 1000 words)
        word_count = len(text.split())
        dialect_density = (total_dialectisms / max(word_count, 1)) * 1000
        
        # Score based on density and variety
        variety_score = min(len({
            word for word in self.mexican_dialectisms['lexical'] if word in text_lower
        }) + len({
            expr for expr in self.mexican_dialectisms['expressions'] if expr in text_lower
        }), 20) / 20 * 100
        
        density_score = min(dialect_density, 10) / 10 * 100
        
        authenticity_score = (variety_score * 0.6 + density_score * 0.4)
        
        return {
            'authenticity_score': authenticity_score,
            'total_dialectisms': total_dialectisms,
            'lexical_mexicanisms': lexical_count,
            'expressions': expressions_count,
            'diminutives': diminutives_count,
            'dialect_density': dialect_density,
            'variety_score': variety_score
        }
    
    def analyze_formality_level(self, text: str) -> Dict[str, Any]:
        """Analyze formality level of the text."""
        text_lower = text.lower()
        
        # Count formality indicators
        very_high_count = sum(1 for indicator in self.formality_indicators['very_high'] if indicator in text_lower)
        high_count = sum(1 for indicator in self.formality_indicators['high'] if indicator in text_lower)
        medium_count = sum(1 for indicator in self.formality_indicators['medium'] if indicator in text_lower)
        
        # Count academic vocabulary
        academic_count = sum(1 for word in self.academic_vocabulary if word in text_lower)
        
        total_formal = very_high_count + high_count + medium_count + academic_count
        word_count = len(text.split())
        formality_density = (total_formal / max(word_count, 1)) * 1000
        
        # Calculate weighted formality score
        weighted_score = (very_high_count * 4 + high_count * 3 + medium_count * 2 + academic_count * 2)
        formality_score = min(weighted_score / max(word_count, 1) * 1000, 100)
        
        # Determine formality level
        if formality_score >= 30:
            level = 'muy_alto'
        elif formality_score >= 20:
            level = 'alto'
        elif formality_score >= 10:
            level = 'medio'
        elif formality_score >= 5:
            level = 'bajo'
        else:
            level = 'muy_bajo'
        
        return {
            'formality_score': formality_score,
            'formality_level': level,
            'very_high_indicators': very_high_count,
            'high_indicators': high_count,
            'medium_indicators': medium_count,
            'academic_vocabulary': academic_count,
            'formality_density': formality_density
        }
    
    def analyze_linguistic_complexity(self, text: str) -> Dict[str, Any]:
        """Analyze linguistic complexity using spaCy."""
        try:
            doc = self.nlp(text)
            
            # Basic metrics
            sentences = list(doc.sents)
            tokens = [token for token in doc if not token.is_space and not token.is_punct]
            
            if not sentences or not tokens:
                return {'complexity_score': 0, 'error': 'No valid sentences found'}
            
            # Average sentence length
            avg_sentence_length = len(tokens) / len(sentences)
            
            # Lexical diversity (Type-Token Ratio)
            unique_words = len(set(token.lemma_.lower() for token in tokens if token.has_vector))
            ttr = unique_words / len(tokens) if tokens else 0
            
            # Complex sentence patterns
            complex_patterns = sum(1 for pattern in self.complexity_patterns if pattern.search(text))
            
            # Subordinate clauses (approximation)
            subordinating_conjunctions = ['que', 'porque', 'aunque', 'cuando', 'donde', 'como', 'si']
            subordinate_clauses = sum(1 for token in tokens if token.lemma_.lower() in subordinating_conjunctions)
            
            # Calculate complexity score
            length_score = min(avg_sentence_length / 25, 1) * 25  # Normalize to 25 words max
            diversity_score = ttr * 30  # TTR contributes up to 30 points
            pattern_score = min(complex_patterns / len(sentences) * 100, 20)  # Pattern density
            subordination_score = min(subordinate_clauses / len(sentences) * 50, 25)  # Subordination density
            
            complexity_score = length_score + diversity_score + pattern_score + subordination_score
            
            return {
                'complexity_score': min(complexity_score, 100),
                'avg_sentence_length': avg_sentence_length,
                'type_token_ratio': ttr,
                'complex_patterns': complex_patterns,
                'subordinate_clauses': subordinate_clauses,
                'unique_words': unique_words,
                'total_tokens': len(tokens),
                'total_sentences': len(sentences)
            }
            
        except Exception as e:
            self.logger.warning(f"Linguistic complexity analysis failed: {e}")
            return {'complexity_score': 0, 'error': str(e)}
    
    def analyze_cultural_content(self, text: str) -> Dict[str, Any]:
        """Analyze Mexican cultural content richness."""
        text_lower = text.lower()
        
        # Count cultural references
        history_count = sum(1 for ref in self.cultural_references['history'] if ref in text_lower)
        culture_count = sum(1 for ref in self.cultural_references['culture'] if ref in text_lower)
        geography_count = sum(1 for ref in self.cultural_references['geography'] if ref in text_lower)
        
        total_cultural = history_count + culture_count + geography_count
        word_count = len(text.split())
        cultural_density = (total_cultural / max(word_count, 1)) * 1000
        
        # Calculate cultural score
        variety_bonus = min(len({
            ref for ref in self.cultural_references['history'] if ref in text_lower
        }) + len({
            ref for ref in self.cultural_references['culture'] if ref in text_lower
        }) + len({
            ref for ref in self.cultural_references['geography'] if ref in text_lower
        }), 15) / 15 * 20  # Variety bonus up to 20 points
        
        density_score = min(cultural_density, 5) / 5 * 60  # Density up to 60 points
        balance_score = 20 if history_count > 0 and culture_count > 0 else 10 if total_cultural > 0 else 0
        
        cultural_score = density_score + variety_bonus + balance_score
        
        return {
            'cultural_score': cultural_score,
            'total_references': total_cultural,
            'history_references': history_count,
            'culture_references': culture_count,
            'geography_references': geography_count,
            'cultural_density': cultural_density,
            'variety_bonus': variety_bonus
        }
    
    def calculate_vocabulary_richness(self, text: str) -> Dict[str, Any]:
        """Calculate vocabulary richness metrics."""
        try:
            doc = self.nlp(text)
            tokens = [token for token in doc if not token.is_space and not token.is_punct and not token.is_stop]
            
            if len(tokens) < 10:
                return {'richness_score': 0, 'error': 'Insufficient content for analysis'}
            
            # Lemma-based analysis for better accuracy
            lemmas = [token.lemma_.lower() for token in tokens if token.has_vector]
            
            if not lemmas:
                return {'richness_score': 0, 'error': 'No valid lemmas found'}
            
            # Type-Token Ratio (TTR)
            unique_lemmas = len(set(lemmas))
            ttr = unique_lemmas / len(lemmas)
            
            # Moving Average Type-Token Ratio (MATTR) - more stable for varying text lengths
            window_size = min(100, len(lemmas))
            if len(lemmas) >= window_size:
                mattr_values = []
                for i in range(len(lemmas) - window_size + 1):
                    window = lemmas[i:i + window_size]
                    window_ttr = len(set(window)) / len(window)
                    mattr_values.append(window_ttr)
                mattr = sum(mattr_values) / len(mattr_values)
            else:
                mattr = ttr
            
            # Hapax Legomena (words appearing only once)
            lemma_counts = Counter(lemmas)
            hapax_count = sum(1 for count in lemma_counts.values() if count == 1)
            hapax_ratio = hapax_count / len(lemmas)
            
            # Calculate richness score (0-100)
            ttr_score = min(ttr * 100, 50)  # TTR contributes up to 50 points
            mattr_score = min(mattr * 80, 30)  # MATTR contributes up to 30 points
            hapax_score = min(hapax_ratio * 100, 20)  # Hapax ratio contributes up to 20 points
            
            richness_score = ttr_score + mattr_score + hapax_score
            
            return {
                'richness_score': richness_score,
                'type_token_ratio': ttr,
                'mattr': mattr,
                'hapax_ratio': hapax_ratio,
                'unique_lemmas': unique_lemmas,
                'total_lemmas': len(lemmas),
                'hapax_count': hapax_count
            }
            
        except Exception as e:
            self.logger.warning(f"Vocabulary richness analysis failed: {e}")
            return {'richness_score': 0, 'error': str(e)}
    
    def analyze_quality(self, text: str, title: str = "") -> QualityScore:
        """
        Comprehensive quality analysis of Mexican Spanish content.
        
        Args:
            text: Main content text
            title: Optional title text
            
        Returns:
            QualityScore with detailed analysis
        """
        full_text = f"{title} {text}" if title else text
        
        # Perform all analyses
        dialect_analysis = self.analyze_dialect_authenticity(full_text)
        formality_analysis = self.analyze_formality_level(full_text)
        complexity_analysis = self.analyze_linguistic_complexity(full_text)
        cultural_analysis = self.analyze_cultural_content(full_text)
        vocabulary_analysis = self.calculate_vocabulary_richness(full_text)
        
        # Extract scores
        dialect_score = dialect_analysis.get('authenticity_score', 0)
        formality_score = formality_analysis.get('formality_score', 0)
        complexity_score = complexity_analysis.get('complexity_score', 0)
        cultural_score = cultural_analysis.get('cultural_score', 0)
        vocabulary_score = vocabulary_analysis.get('richness_score', 0)
        
        # Calculate weighted overall score
        overall_score = (
            dialect_score * 0.15 +      # Mexican dialect authenticity
            formality_score * 0.25 +    # Formality and register
            complexity_score * 0.25 +   # Linguistic complexity
            cultural_score * 0.15 +     # Cultural content
            vocabulary_score * 0.20     # Vocabulary richness
        )
        
        # Determine confidence level
        if overall_score >= 70:
            confidence = 'high'
        elif overall_score >= 50:
            confidence = 'medium'
        elif overall_score >= 30:
            confidence = 'low'
        else:
            confidence = 'very_low'
        
        self.logger.debug(f"Quality analysis complete: overall={overall_score:.1f}, confidence={confidence}")
        
        return QualityScore(
            overall_score=overall_score,
            dialect_authenticity=dialect_score,
            formality_level=formality_score,
            linguistic_complexity=complexity_score,
            vocabulary_richness=vocabulary_score,
            mexican_cultural_content=cultural_score,
            confidence=confidence
        )