"""
Configuration management for the scraping framework.
Handles loading and validation of YAML configuration files.
"""

import os
import yaml
from typing import Dict, Any, List
from .exceptions import ConfigurationError


class ConfigManager:
    """Manages loading and validation of configuration files."""
    
    def __init__(self, config_path: str = "config.yaml", sources_path: str = "sources.yaml"):
        self.config_path = config_path
        self.sources_path = sources_path
        self.config = {}
        self.sources = []
        self._load_configurations()
    
    def _load_configurations(self):
        """Load both configuration files and validate their schemas."""
        self.config = self._load_yaml_file(self.config_path)
        sources_data = self._load_yaml_file(self.sources_path)
        self.sources = sources_data.get('sources', [])
        
        self._validate_config()
        self._validate_sources()
    
    def _load_yaml_file(self, file_path: str) -> Dict[str, Any]:
        """Load and parse a YAML file."""
        if not os.path.exists(file_path):
            raise ConfigurationError(f"Configuration file not found: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file) or {}
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in {file_path}: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error reading {file_path}: {e}")
    
    def _validate_config(self):
        """Validate the main configuration schema."""
        required_sections = ['politeness', 'extraction', 'validation', 'storage', 'concurrency']
        
        for section in required_sections:
            if section not in self.config:
                raise ConfigurationError(f"Missing required section in config: {section}")
        
        # Validate politeness settings
        politeness = self.config['politeness']
        required_politeness = ['request_delay', 'timeout', 'retry_attempts']
        for key in required_politeness:
            if key not in politeness:
                raise ConfigurationError(f"Missing politeness setting: {key}")
        
        # Validate validation settings
        validation = self.config['validation']
        required_validation = ['min_word_count', 'required_language']
        for key in required_validation:
            if key not in validation:
                raise ConfigurationError(f"Missing validation setting: {key}")
        
        # Validate storage settings
        storage = self.config['storage']
        required_storage = ['output_dir', 'log_dir', 'state_dir']
        for key in required_storage:
            if key not in storage:
                raise ConfigurationError(f"Missing storage setting: {key}")
    
    def _validate_sources(self):
        """Validate the sources configuration."""
        if not self.sources:
            raise ConfigurationError("No sources defined in sources.yaml")
        
        for i, source in enumerate(self.sources):
            if 'name' not in source:
                raise ConfigurationError(f"Source {i} missing required 'name' field")
            if 'base_url' not in source:
                raise ConfigurationError(f"Source '{source.get('name', i)}' missing required 'base_url' field")
            
            # Must have either sitemap_url or start_urls
            if 'sitemap_url' not in source and 'start_urls' not in source:
                raise ConfigurationError(
                    f"Source '{source['name']}' must have either 'sitemap_url' or 'start_urls'"
                )
    
    def get_config(self) -> Dict[str, Any]:
        """Get the complete configuration."""
        return self.config.copy()
    
    def get_sources(self) -> List[Dict[str, Any]]:
        """Get the list of configured sources."""
        return self.sources.copy()
    
    def get_politeness_config(self) -> Dict[str, Any]:
        """Get politeness-related configuration."""
        return self.config['politeness'].copy()
    
    def get_extraction_config(self) -> Dict[str, Any]:
        """Get extraction-related configuration."""
        return self.config['extraction'].copy()
    
    def get_validation_config(self) -> Dict[str, Any]:
        """Get validation-related configuration."""
        return self.config['validation'].copy()
    
    def get_storage_config(self) -> Dict[str, Any]:
        """Get storage-related configuration."""
        return self.config['storage'].copy()
    
    def get_concurrency_config(self) -> Dict[str, Any]:
        """Get concurrency-related configuration."""
        return self.config['concurrency'].copy()
    
    def get_source_by_name(self, name: str) -> Dict[str, Any]:
        """Get a specific source configuration by name."""
        for source in self.sources:
            if source['name'] == name:
                return source.copy()
        raise ConfigurationError(f"Source '{name}' not found in configuration")