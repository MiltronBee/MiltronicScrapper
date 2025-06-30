"""
Enhanced data persistence with HTML snapshots and token counting.
Extends the existing saver with high-yield harvesting capabilities.
"""

import os
import hashlib
import logging
import gzip
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
import tiktoken
from .exceptions import ScrapingError
from .encoding_validator import EncodingValidator


class EnhancedSaver:
    """
    Enhanced persistence layer with token counting and HTML snapshot archiving.
    Builds on the existing saver with high-yield data management features.
    """
    
    def __init__(self, storage_config: Dict[str, Any]):
        self.storage_config = storage_config
        self.logger = logging.getLogger(__name__)
        
        # Initialize encoding validator
        self.encoding_validator = EncodingValidator()
        
        # Token counting configuration
        self.token_config = storage_config.get('token_counting', {})
        self.token_enabled = self.token_config.get('enabled', True)
        
        # Initialize tokenizer
        self.tokenizer = None
        if self.token_enabled:
            self._initialize_tokenizer()
        
        # Snapshot configuration
        self.snapshot_config = storage_config.get('snapshots', {})
        self.snapshots_enabled = self.snapshot_config.get('enabled', True)
        
        # HTML storage configuration
        self.save_html = storage_config.get('save_raw_html', True)
        self.compress_html = storage_config.get('compress_html', True)
        
        # Data organization
        self.organize_by_domain = storage_config.get('organize_by_domain', True)
        self.max_files_per_dir = storage_config.get('max_files_per_dir', 10000)
        
        # Ensure directories exist
        self._ensure_directories()
        
        # Track saved files for deduplication
        self.saved_hashes = set()
        self.token_stats = {}
        self._load_existing_data()
    
    def _initialize_tokenizer(self):
        """Initialize tiktoken tokenizer for token counting."""
        try:
            model = self.token_config.get('model', 'gpt-4')
            self.tokenizer = tiktoken.encoding_for_model(model)
            self.logger.info(f"Initialized tiktoken tokenizer for {model}")
        except Exception as e:
            self.logger.warning(f"Failed to initialize tokenizer: {e}")
            self.tokenizer = None
    
    def _ensure_directories(self):
        """Create necessary directories if they don't exist."""
        directories = [
            self.storage_config['output_dir'],
            self.storage_config['log_dir'],
            self.storage_config['state_dir']
        ]
        
        # Add HTML directories
        if self.save_html:
            directories.append(self.storage_config.get('raw_html_dir', '../data/html_raw'))
        
        # Add snapshot directories
        if self.snapshots_enabled:
            directories.append(self._get_snapshot_dir())
        
        for directory in directories:
            try:
                Path(directory).mkdir(parents=True, exist_ok=True)
                self.logger.debug(f"Ensured directory exists: {directory}")
            except Exception as e:
                raise ScrapingError(f"Failed to create directory {directory}: {e}")
    
    def _get_snapshot_dir(self) -> str:
        """Get snapshot directory path."""
        return self.storage_config.get('snapshot_dir', '../data/snapshots')
    
    def _load_existing_data(self):
        """Load existing file hashes and token statistics."""
        output_dir = Path(self.storage_config['output_dir'])
        
        if not output_dir.exists():
            return
        
        try:
            # Load existing hashes and count tokens
            total_files = 0
            total_tokens = 0
            
            for file_path in output_dir.rglob('*.txt'):
                # Extract hash from filename
                filename = file_path.stem
                parts = filename.split('_')
                if len(parts) >= 3:
                    content_hash = parts[-1]
                    self.saved_hashes.add(content_hash)
                
                # Count tokens if file exists and tokenizer available
                if self.tokenizer and file_path.exists():
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            tokens = len(self.tokenizer.encode(content))
                            total_tokens += tokens
                    except Exception as e:
                        self.logger.debug(f"Error counting tokens in {file_path}: {e}")
                
                total_files += 1
            
            self.token_stats = {
                'total_files': total_files,
                'total_tokens': total_tokens,
                'last_updated': datetime.now()
            }
            
            self.logger.info(
                f"Loaded {len(self.saved_hashes)} existing file hashes, "
                f"{total_tokens:,} total tokens"
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to load existing data: {e}")
    
    def count_tokens(self, text: str) -> int:
        """Count tokens in text using tiktoken."""
        if not self.tokenizer:
            # Rough estimation: ~4 characters per token
            return len(text) // 4
        
        try:
            return len(self.tokenizer.encode(text))
        except Exception as e:
            self.logger.debug(f"Token counting failed: {e}")
            return len(text) // 4  # Fallback estimation
    
    def save_html_snapshot(self, html_content: str, source_name: str, url: str, 
                          content_hash: str) -> Optional[str]:
        """
        Save compressed HTML snapshot for future reference.
        
        Args:
            html_content: Original HTML content
            source_name: Source name for organization
            url: Original URL
            content_hash: Content hash for linking
            
        Returns:
            Snapshot file path if saved, None otherwise
        """
        if not self.snapshots_enabled:
            return None
        
        try:
            # Create snapshot directory structure
            snapshot_dir = Path(self._get_snapshot_dir())
            
            if self.organize_by_domain:
                from urllib.parse import urlparse
                domain = urlparse(url).netloc
                source_snapshot_dir = snapshot_dir / source_name / domain
            else:
                source_snapshot_dir = snapshot_dir / source_name
            
            source_snapshot_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate snapshot filename
            date_str = datetime.now().strftime('%Y%m%d')
            if self.compress_html:
                filename = f"{source_name}_{date_str}_{content_hash}.html.gz"
            else:
                filename = f"{source_name}_{date_str}_{content_hash}.html"
            
            snapshot_path = source_snapshot_dir / filename
            
            # Save snapshot
            if self.compress_html:
                with gzip.open(snapshot_path, 'wt', encoding='utf-8') as f:
                    f.write(html_content)
            else:
                with open(snapshot_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
            
            # Save metadata if enabled
            if self.snapshot_config.get('include_metadata', True):
                metadata = {
                    'url': url,
                    'source': source_name,
                    'content_hash': content_hash,
                    'saved_at': datetime.now().isoformat(),
                    'html_size': len(html_content),
                    'compressed': self.compress_html
                }
                
                metadata_path = snapshot_path.with_suffix('.meta.json')
                import json
                with open(metadata_path, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            self.logger.debug(f"Saved HTML snapshot: {snapshot_path}")
            return str(snapshot_path)
            
        except Exception as e:
            self.logger.warning(f"Failed to save HTML snapshot: {e}")
            return None
    
    def _generate_filename(self, source_name: str, content_hash: str, url: str = None,
                          custom_suffix: str = None) -> str:
        """Generate filename with optional custom suffix."""
        date_str = datetime.now().strftime('%Y%m%d')
        
        if custom_suffix:
            return f"{source_name}_{date_str}_{content_hash}_{custom_suffix}.txt"
        else:
            return f"{source_name}_{date_str}_{content_hash}.txt"
    
    def _get_organized_output_path(self, source_name: str, url: str = None) -> Path:
        """Get organized output path based on configuration."""
        output_dir = Path(self.storage_config['output_dir'])
        
        if self.organize_by_domain and url:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc
            # Clean domain for filesystem
            domain_clean = domain.replace(':', '_').replace('/', '_')
            organized_dir = output_dir / source_name / domain_clean
        else:
            organized_dir = output_dir / source_name
        
        organized_dir.mkdir(parents=True, exist_ok=True)
        
        # Check if directory has too many files
        existing_files = list(organized_dir.glob('*.txt'))
        if len(existing_files) >= self.max_files_per_dir:
            # Create subdirectory
            subdir_num = len(existing_files) // self.max_files_per_dir + 1
            organized_dir = organized_dir / f"batch_{subdir_num:03d}"
            organized_dir.mkdir(parents=True, exist_ok=True)
        
        return organized_dir
    
    def save_enhanced_content(self, content: str, source_name: str, url: str,
                            html_content: str = None, metadata: Optional[Dict[str, Any]] = None,
                            custom_filename: Optional[str] = None) -> Dict[str, Any]:
        """
        Enhanced save method with token counting and HTML snapshots.
        
        Args:
            content: Processed text content
            source_name: Source name
            url: Original URL
            html_content: Original HTML for snapshots
            metadata: Additional metadata
            custom_filename: Custom filename if needed
            
        Returns:
            Save result with enhanced information
        """
        result = {
            'saved': False,
            'duplicate': False,
            'file_path': None,
            'snapshot_path': None,
            'content_hash': None,
            'token_count': 0,
            'error': None
        }
        
        try:
            # Calculate content hash
            content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
            result['content_hash'] = content_hash
            
            # Count tokens
            token_count = self.count_tokens(content)
            result['token_count'] = token_count
            
            # Check for duplicates
            if content_hash in self.saved_hashes:
                result['duplicate'] = True
                self.logger.info(f"Skipping duplicate content (hash: {content_hash})")
                return result
            
            # Generate file path
            if custom_filename:
                output_dir = self._get_organized_output_path(source_name, url)
                file_path = output_dir / custom_filename
            else:
                filename = self._generate_filename(source_name, content_hash, url)
                output_dir = self._get_organized_output_path(source_name, url)
                file_path = output_dir / filename
            
            # Prepare enhanced content with metadata header
            full_content = self._prepare_enhanced_content(
                content, url, source_name, metadata, token_count
            )
            
            # Atomic write
            if self._atomic_write_enhanced(file_path, full_content, source_name):
                # Track hash and tokens
                self.saved_hashes.add(content_hash)
                self._update_token_stats(token_count)
                
                result.update({
                    'saved': True,
                    'file_path': str(file_path)
                })
                
                # Save HTML snapshot
                if html_content and self.snapshots_enabled:
                    snapshot_path = self.save_html_snapshot(
                        html_content, source_name, url, content_hash
                    )
                    result['snapshot_path'] = snapshot_path
                
                # Link snapshot to processed file if enabled
                if (result['snapshot_path'] and 
                    self.snapshot_config.get('link_to_processed', True)):
                    self._create_content_link(result['file_path'], result['snapshot_path'])
                
                self.logger.info(
                    f"Saved enhanced content: {file_path} "
                    f"({len(content)} chars, {token_count:,} tokens)"
                )
            else:
                result['error'] = "Failed to write file"
                
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"Error saving enhanced content: {e}")
        
        return result
    
    def _prepare_enhanced_content(self, content: str, url: str, source_name: str,
                                metadata: Optional[Dict[str, Any]], token_count: int) -> str:
        """Prepare content with enhanced metadata header."""
        if not metadata:
            metadata = {}
        
        # Add token count to metadata
        metadata['token_count'] = token_count
        metadata['saved_at'] = datetime.now().isoformat()
        metadata['content_size'] = len(content)
        
        # For now, just return clean content
        # Future enhancement could add YAML frontmatter
        return content
    
    def _atomic_write_enhanced(self, file_path: Path, content: str, source_name: str = None) -> bool:
        """Enhanced atomic write with better error handling."""
        temp_path = file_path.with_suffix(file_path.suffix + '.tmp')
        
        try:
            # Enhanced content validation
            try:
                if isinstance(content, bytes):
                    content = content.decode('utf-8', errors='replace')
                
                # Validate content quality
                is_valid, quality_info = self.encoding_validator._validate_text_quality(content)
                
                if not is_valid and quality_info.get('is_binary'):
                    self.logger.error(f"Refusing to save binary content for {source_name}")
                    return False
                
                # Clean and normalize
                content = self.encoding_validator.clean_and_normalize_text(content)
                
                if len(content.strip()) < 20:
                    self.logger.warning(f"Content too short after cleaning, skipping {source_name}")
                    return False
                
            except Exception as e:
                self.logger.warning(f"Content validation failed for {source_name}: {e}")
                return False
            
            # Write with UTF-8 encoding
            with open(temp_path, 'w', encoding='utf-8') as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            
            # Atomic rename
            temp_path.rename(file_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Enhanced atomic write failed for {file_path}: {e}")
            
            # Cleanup
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except:
                pass
            
            return False
    
    def _update_token_stats(self, new_tokens: int):
        """Update global token statistics."""
        self.token_stats['total_tokens'] = self.token_stats.get('total_tokens', 0) + new_tokens
        self.token_stats['total_files'] = self.token_stats.get('total_files', 0) + 1
        self.token_stats['last_updated'] = datetime.now()
    
    def _create_content_link(self, content_path: str, snapshot_path: str):
        """Create a link file between processed content and HTML snapshot."""
        try:
            link_info = {
                'content_file': content_path,
                'snapshot_file': snapshot_path,
                'linked_at': datetime.now().isoformat()
            }
            
            link_path = Path(content_path).with_suffix('.link.json')
            import json
            with open(link_path, 'w', encoding='utf-8') as f:
                json.dump(link_info, f, indent=2)
                
        except Exception as e:
            self.logger.debug(f"Failed to create content link: {e}")
    
    def get_enhanced_corpus_stats(self) -> Dict[str, Any]:
        """Get enhanced corpus statistics including token counts."""
        stats = {
            'total_files': 0,
            'total_size_bytes': 0,
            'total_tokens': 0,
            'sources': {},
            'domains': {},
            'unique_hashes': len(self.saved_hashes),
            'snapshots': 0
        }
        
        try:
            output_dir = Path(self.storage_config['output_dir'])
            
            if not output_dir.exists():
                return stats
            
            # Scan all text files
            for file_path in output_dir.rglob('*.txt'):
                stats['total_files'] += 1
                file_size = file_path.stat().st_size
                stats['total_size_bytes'] += file_size
                
                # Extract source from path
                relative_path = file_path.relative_to(output_dir)
                source_name = relative_path.parts[0] if relative_path.parts else 'unknown'
                
                if source_name not in stats['sources']:
                    stats['sources'][source_name] = {
                        'files': 0, 'size_bytes': 0, 'tokens': 0
                    }
                
                stats['sources'][source_name]['files'] += 1
                stats['sources'][source_name]['size_bytes'] += file_size
                
                # Count tokens if possible
                if self.tokenizer:
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            tokens = len(self.tokenizer.encode(content))
                            stats['total_tokens'] += tokens
                            stats['sources'][source_name]['tokens'] += tokens
                    except Exception:
                        pass  # Skip token counting for problematic files
                
                # Track domains if organized by domain
                if self.organize_by_domain and len(relative_path.parts) >= 2:
                    domain = relative_path.parts[1]
                    if domain not in stats['domains']:
                        stats['domains'][domain] = {'files': 0, 'size_bytes': 0}
                    stats['domains'][domain]['files'] += 1
                    stats['domains'][domain]['size_bytes'] += file_size
            
            # Count snapshots
            if self.snapshots_enabled:
                snapshot_dir = Path(self._get_snapshot_dir())
                if snapshot_dir.exists():
                    stats['snapshots'] = len(list(snapshot_dir.rglob('*.html*')))
            
            # Convert bytes to MB
            stats['total_size_mb'] = round(stats['total_size_bytes'] / (1024 * 1024), 2)
            stats['total_size_gb'] = round(stats['total_size_bytes'] / (1024 * 1024 * 1024), 3)
            
            for source_stats in stats['sources'].values():
                source_stats['size_mb'] = round(source_stats['size_bytes'] / (1024 * 1024), 2)
            
            # Estimated progress toward goals
            target_tokens = 1000000000  # 1 billion tokens
            stats['progress_percentage'] = (stats['total_tokens'] / target_tokens) * 100
            
        except Exception as e:
            self.logger.error(f"Failed to calculate enhanced corpus stats: {e}")
        
        return stats