"""
Tranco Domain Harvester for .mx domain discovery.
Downloads and processes the Tranco top domains list to find Mexican websites.
"""

import logging
import requests
import csv
import gzip
import io
from typing import List, Dict, Set, Optional
from urllib.parse import urlparse
import time
from .geographic_filter import GeographicFilter


class DomainHarvester:
    """
    Specialized harvester for discovering Mexican domains from the Tranco list.
    Focuses on .mx domains and Mexican content indicators.
    """
    
    def __init__(self, harvesting_config: Dict):
        self.config = harvesting_config
        self.logger = logging.getLogger(__name__)
        self.geographic_filter = GeographicFilter()
        
        # Tranco list URL (updated daily)
        self.tranco_url = "https://tranco-list.eu/top-1m.csv.zip"
        
        # Mexican TLDs and domain patterns
        self.mexican_tlds = {'.mx', '.com.mx', '.org.mx', '.edu.mx', '.gob.mx', '.net.mx'}
        
        # Known Mexican domain keywords
        self.mexican_keywords = {
            'mexico', 'mexicano', 'mx', 'cdmx', 'guadalajara', 'monterrey',
            'unam', 'ipn', 'tec', 'itesm', 'gobierno', 'gob'
        }
        
        # Quality scoring weights
        self.scoring_weights = {
            'mx_tld': 10.0,
            'edu_mx': 15.0,
            'gob_mx': 20.0,
            'mexican_keyword': 5.0,
            'tranco_rank': 2.0  # Higher rank = higher score
        }
        
        # Limits
        self.max_domains = self.config.get('max_domains', 10000)
    
    def harvest_mexican_domains(self) -> List[Dict[str, any]]:
        """
        Harvest Mexican domains from the Tranco list.
        
        Returns:
            List of domain dictionaries with metadata
        """
        self.logger.info("Starting Mexican domain harvest from Tranco list")
        
        try:
            # Download Tranco list
            domains_data = self._download_tranco_list()
            
            if not domains_data:
                self.logger.error("Failed to download Tranco list")
                return []
            
            # Filter for Mexican domains
            mexican_domains = self._filter_mexican_domains(domains_data)
            
            # Score and rank domains
            scored_domains = self._score_domains(mexican_domains)
            
            # Sort by score and limit results
            scored_domains.sort(key=lambda x: x['score'], reverse=True)
            final_domains = scored_domains[:self.max_domains]
            
            self.logger.info(f"Harvested {len(final_domains)} Mexican domains")
            return final_domains
            
        except Exception as e:
            self.logger.error(f"Domain harvesting failed: {e}")
            return []
    
    def _download_tranco_list(self) -> Optional[List[Dict]]:
        """Download and parse the Tranco top domains list."""
        try:
            self.logger.info("Downloading Tranco top domains list...")
            
            response = requests.get(self.tranco_url, timeout=300, stream=True)
            response.raise_for_status()
            
            # The file is a ZIP containing a CSV
            zip_content = response.content
            
            # Extract CSV from ZIP
            import zipfile
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                # Get the CSV file (usually named top-1m.csv)
                csv_filename = None
                for filename in zip_file.namelist():
                    if filename.endswith('.csv'):
                        csv_filename = filename
                        break
                
                if not csv_filename:
                    raise ValueError("No CSV file found in Tranco ZIP")
                
                with zip_file.open(csv_filename) as csv_file:
                    # Parse CSV
                    csv_content = csv_file.read().decode('utf-8')
                    csv_reader = csv.reader(io.StringIO(csv_content))
                    
                    domains_data = []
                    for row_num, row in enumerate(csv_reader, 1):
                        if len(row) >= 2:
                            rank = int(row[0])
                            domain = row[1].lower().strip()
                            
                            domains_data.append({
                                'rank': rank,
                                'domain': domain
                            })
                        
                        # Limit processing to avoid memory issues
                        if row_num >= 1000000:  # Process max 1M domains
                            break
            
            self.logger.info(f"Downloaded and parsed {len(domains_data)} domains from Tranco list")
            return domains_data
            
        except Exception as e:
            self.logger.error(f"Failed to download Tranco list: {e}")
            return None
    
    def _filter_mexican_domains(self, domains_data: List[Dict]) -> List[Dict]:
        """Filter domains for Mexican relevance."""
        mexican_domains = []
        
        for domain_info in domains_data:
            domain = domain_info['domain']
            rank = domain_info['rank']
            
            if self._is_mexican_domain(domain):
                mexican_domains.append({
                    'domain': domain,
                    'rank': rank,
                    'indicators': self._get_mexican_indicators(domain)
                })
        
        self.logger.info(f"Filtered {len(mexican_domains)} Mexican domains from Tranco list")
        return mexican_domains
    
    def _is_mexican_domain(self, domain: str) -> bool:
        """Check if domain is likely Mexican."""
        domain_lower = domain.lower()
        
        # Check for Mexican TLDs
        if any(domain_lower.endswith(tld) for tld in self.mexican_tlds):
            return True
        
        # Check for Mexican keywords in domain
        if any(keyword in domain_lower for keyword in self.mexican_keywords):
            return True
        
        # Check for specific patterns
        mexican_patterns = [
            r'.*mexico.*',
            r'.*mx.*',
            r'.*unam.*',
            r'.*ipn.*',
            r'.*tec.*',
            r'.*cdmx.*'
        ]
        
        import re
        for pattern in mexican_patterns:
            if re.match(pattern, domain_lower):
                return True
        
        return False
    
    def _get_mexican_indicators(self, domain: str) -> List[str]:
        """Get list of Mexican indicators for a domain."""
        indicators = []
        domain_lower = domain.lower()
        
        # TLD indicators
        for tld in self.mexican_tlds:
            if domain_lower.endswith(tld):
                indicators.append(f"tld_{tld.replace('.', '_')}")
        
        # Keyword indicators
        for keyword in self.mexican_keywords:
            if keyword in domain_lower:
                indicators.append(f"keyword_{keyword}")
        
        return indicators
    
    def _score_domains(self, domains: List[Dict]) -> List[Dict]:
        """Score domains based on Mexican relevance and quality."""
        scored_domains = []
        
        for domain_info in domains:
            domain = domain_info['domain']
            rank = domain_info['rank']
            indicators = domain_info['indicators']
            
            score = 0.0
            
            # Base score from Tranco rank (higher rank = higher score)
            if rank <= 1000:
                score += self.scoring_weights['tranco_rank'] * 5
            elif rank <= 10000:
                score += self.scoring_weights['tranco_rank'] * 3
            elif rank <= 100000:
                score += self.scoring_weights['tranco_rank'] * 1
            
            # Score based on indicators
            for indicator in indicators:
                if indicator.startswith('tld_'):
                    if 'gob_mx' in indicator:
                        score += self.scoring_weights['gob_mx']
                    elif 'edu_mx' in indicator:
                        score += self.scoring_weights['edu_mx']
                    elif 'mx' in indicator:
                        score += self.scoring_weights['mx_tld']
                elif indicator.startswith('keyword_'):
                    score += self.scoring_weights['mexican_keyword']
            
            # Additional quality factors
            domain_parts = domain.split('.')
            
            # Prefer shorter, cleaner domains
            if len(domain_parts) <= 3:
                score += 1.0
            
            # Prefer domains without hyphens/numbers (usually higher quality)
            if '-' not in domain and not any(char.isdigit() for char in domain):
                score += 0.5
            
            scored_domains.append({
                'domain': domain,
                'rank': rank,
                'score': score,
                'indicators': indicators,
                'category': self._categorize_domain(domain, indicators)
            })
        
        return scored_domains
    
    def _categorize_domain(self, domain: str, indicators: List[str]) -> str:
        """Categorize domain by type."""
        domain_lower = domain.lower()
        
        # Government
        if any('gob' in indicator for indicator in indicators):
            return 'government'
        
        # Education
        if any('edu' in indicator for indicator in indicators) or 'unam' in domain_lower or 'ipn' in domain_lower:
            return 'education'
        
        # News/Media
        news_keywords = ['noticias', 'news', 'diario', 'periodico', 'radio', 'tv']
        if any(keyword in domain_lower for keyword in news_keywords):
            return 'news'
        
        # Commercial
        if domain_lower.endswith('.com.mx') or domain_lower.endswith('.com'):
            return 'commercial'
        
        # Organization
        if domain_lower.endswith('.org.mx') or domain_lower.endswith('.org'):
            return 'organization'
        
        return 'general'
    
    def generate_source_urls(self, domains: List[Dict]) -> List[Dict]:
        """
        Generate source configurations for discovered domains.
        
        Args:
            domains: List of domain dictionaries
            
        Returns:
            List of source configurations ready for sources.yaml
        """
        source_configs = []
        
        for domain_info in domains:
            domain = domain_info['domain']
            category = domain_info['category']
            score = domain_info['score']
            
            # Generate source configuration
            source_config = {
                'name': f"tranco_{domain.replace('.', '_')}",
                'base_url': f"https://{domain}",
                'type': f'domain_{category}',
                'urls': [f"https://{domain}"],
                
                # Configuration based on category
                'render_js': category in ['news', 'commercial'],
                'crawl_depth': self._get_crawl_depth(category),
                'respect_robots_txt': category in ['government', 'education'],
                'dynamic_recursion': True,
                'recursion_keywords': self._get_recursion_keywords(category),
                
                # Metadata
                'tranco_rank': domain_info['rank'],
                'relevance_score': score,
                'domain_category': category,
                'discovery_method': 'tranco_harvest'
            }
            
            source_configs.append(source_config)
        
        return source_configs
    
    def _get_crawl_depth(self, category: str) -> int:
        """Get appropriate crawl depth for domain category."""
        depth_map = {
            'government': 4,
            'education': 5,
            'news': 3,
            'organization': 3,
            'commercial': 2,
            'general': 2
        }
        return depth_map.get(category, 2)
    
    def _get_recursion_keywords(self, category: str) -> List[str]:
        """Get recursion keywords for domain category."""
        base_keywords = ['méxico', 'mexicano', 'artículo', 'contenido']
        
        category_keywords = {
            'government': ['ley', 'decreto', 'reglamento', 'norma', 'acuerdo'],
            'education': ['investigación', 'tesis', 'publicación', 'artículo', 'revista'],
            'news': ['noticia', 'reportaje', 'editorial', 'opinión', 'columna'],
            'organization': ['proyecto', 'iniciativa', 'programa', 'evento'],
            'commercial': ['producto', 'servicio', 'empresa', 'negocio']
        }
        
        return base_keywords + category_keywords.get(category, [])