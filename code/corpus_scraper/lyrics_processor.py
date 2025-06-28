"""
Lyrics processor for letras.com content.
Converts extracted lyrics to formatted text files with proper sentence separation.
"""

import logging
import os
import re
import json
from pathlib import Path
from typing import Dict, List, Any
import spacy


class LyricsProcessor:
    """
    Processes extracted lyrics into standardized corpus format.
    Implements sentence segmentation and standardized storage.
    """
    
    def __init__(self, output_dir: str = None):
        """Initialize the lyrics processor.
        
        Args:
            output_dir: Directory to save processed lyrics (default: data/corpus_raw/letras_com)
        """
        self.logger = logging.getLogger(__name__)
        
        # Set default output directory if none provided
        if output_dir is None:
            base_path = Path(__file__).parent.parent.parent
            output_dir = os.path.join(base_path, "data", "corpus_raw", "letras_com")
        
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Load Spanish language model for sentence segmentation
        try:
            self.nlp = spacy.load("es_core_news_sm", disable=["parser", "ner", "tagger"])
            # Add sentencizer for sentence boundary detection
            self.nlp.add_pipe("sentencizer")
            self.logger.info("Loaded spaCy Spanish model for sentence segmentation")
        except OSError:
            # Fallback to blank model if pre-trained model not available
            self.logger.warning("Spanish spaCy model not found, using blank model")
            self.nlp = spacy.blank("es")
            # Add sentencizer for sentence boundary detection
            self.nlp.add_pipe("sentencizer")
    
    def _clean_lyrics(self, text: str) -> str:
        """Clean and normalize lyrics text.
        
        Args:
            text: Raw lyrics text
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
            
        # Remove stage directions often in parentheses or brackets
        text = re.sub(r'\[.*?\]', '', text)
        text = re.sub(r'\(.*?\)', '', text)
        
        # Remove timestamps and other non-text elements
        text = re.sub(r'\d{2}:\d{2}', '', text)
        
        # Remove social media mentions
        text = re.sub(r'@\w+', '', text)
        
        # Remove multiple whitespace characters and normalize line endings
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)
        
        # Ensure proper spacing after punctuation
        text = re.sub(r'([.!?])(\w)', r'\1 \2', text)
        
        return text.strip()
    
    def _segment_into_sentences(self, text: str) -> List[str]:
        """Split lyrics into sentences using spaCy.
        
        Args:
            text: Cleaned lyrics text
            
        Returns:
            List of sentences
        """
        if not text:
            return []
            
        # Process text with spaCy
        doc = self.nlp(text)
        
        # Extract sentences
        sentences = []
        for sent in doc.sents:
            sent_text = sent.text.strip()
            if sent_text:  # Only add non-empty sentences
                sentences.append(sent_text)
        
        return sentences
    
    def process_lyrics(self, artist_data: Dict[str, Any], include_metadata: bool = True) -> Dict[str, Any]:
        """Process lyrics data for an artist and save to files.
        
        Args:
            artist_data: Artist data with songs and lyrics
            include_metadata: Whether to save metadata JSON alongside text
            
        Returns:
            Processing statistics
        """
        stats = {
            'artist': artist_data.get('artist_name', 'unknown'),
            'processed_songs': 0,
            'total_sentences': 0,
            'total_words': 0,
            'files_saved': [],
            'errors': []
        }
        
        artist_dir = self._get_sanitized_filename(stats['artist'])
        artist_path = os.path.join(self.output_dir, artist_dir)
        os.makedirs(artist_path, exist_ok=True)
        
        # Process each song
        for song in artist_data.get('songs', []):
            if not song.get('success') or not song.get('lyrics'):
                stats['errors'].append(f"No lyrics for: {song.get('title', 'unknown')}")
                continue
                
            try:
                # Clean lyrics
                clean_text = self._clean_lyrics(song['lyrics'])
                
                # Skip if no valid content after cleaning
                if len(clean_text) < 20:  # Minimum character threshold
                    stats['errors'].append(f"Insufficient content for: {song.get('title', 'unknown')}")
                    continue
                
                # Split into sentences
                sentences = self._segment_into_sentences(clean_text)
                
                # Skip if no valid sentences
                if not sentences:
                    stats['errors'].append(f"No sentences extracted for: {song.get('title', 'unknown')}")
                    continue
                
                # Create output text with one blank line between sentences
                output_text = "\n\n".join(sentences)
                
                # Save to file
                song_filename = self._get_sanitized_filename(song.get('title', f"song_{len(stats['files_saved'])}"))
                text_file = os.path.join(artist_path, f"{song_filename}.txt")
                
                with open(text_file, 'w', encoding='utf-8') as f:
                    f.write(output_text)
                
                # Save metadata if requested
                if include_metadata:
                    metadata = {
                        'title': song.get('title', ''),
                        'artist': stats['artist'],
                        'album': song.get('metadata', {}).get('album', ''),
                        'language': song.get('metadata', {}).get('language', 'es'),
                        'url': song.get('url', ''),
                        'sentence_count': len(sentences),
                        'word_count': sum(len(s.split()) for s in sentences)
                    }
                    
                    meta_file = os.path.join(artist_path, f"{song_filename}_meta.json")
                    with open(meta_file, 'w', encoding='utf-8') as f:
                        json.dump(metadata, f, ensure_ascii=False, indent=2)
                
                # Update stats
                stats['processed_songs'] += 1
                stats['total_sentences'] += len(sentences)
                stats['total_words'] += sum(len(s.split()) for s in sentences)
                stats['files_saved'].append(text_file)
                
            except Exception as e:
                error_msg = f"Error processing {song.get('title', 'unknown')}: {str(e)}"
                self.logger.error(error_msg)
                stats['errors'].append(error_msg)
        
        # Save artist summary
        summary = {
            'artist': stats['artist'],
            'processed_songs': stats['processed_songs'],
            'total_sentences': stats['total_sentences'],
            'total_words': stats['total_words'],
            'songs': [s.get('title', 'unknown') for s in artist_data.get('songs', []) if s.get('success')]
        }
        
        summary_file = os.path.join(artist_path, "_summary.json")
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        self.logger.info(
            f"Processed {stats['processed_songs']} songs with "
            f"{stats['total_sentences']} sentences and {stats['total_words']} words "
            f"for artist '{stats['artist']}'"
        )
        
        return stats
    
    def process_genre_data(self, genre_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process all artists in genre data.
        
        Args:
            genre_data: Genre data with multiple artists
            
        Returns:
            Processing statistics
        """
        stats = {
            'genre_url': genre_data.get('genre_url', ''),
            'artists_processed': 0,
            'songs_processed': 0,
            'total_sentences': 0,
            'total_words': 0,
            'artists': []
        }
        
        # Process each artist
        for artist_data in genre_data.get('artists', []):
            artist_stats = self.process_lyrics(artist_data)
            
            # Update genre stats
            stats['artists_processed'] += 1
            stats['songs_processed'] += artist_stats['processed_songs']
            stats['total_sentences'] += artist_stats['total_sentences']
            stats['total_words'] += artist_stats['total_words']
            stats['artists'].append({
                'name': artist_stats['artist'],
                'songs_processed': artist_stats['processed_songs'],
                'sentences': artist_stats['total_sentences'],
                'words': artist_stats['total_words']
            })
        
        # Save genre summary
        genre_name = self._extract_genre_name(genre_data.get('genre_url', ''))
        summary_file = os.path.join(self.output_dir, f"{genre_name}_summary.json")
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        
        self.logger.info(
            f"Processed {stats['songs_processed']} songs from {stats['artists_processed']} artists "
            f"with {stats['total_sentences']} sentences and {stats['total_words']} words "
            f"for genre '{genre_name}'"
        )
        
        return stats
    
    def _get_sanitized_filename(self, name: str) -> str:
        """Convert name to a valid filename.
        
        Args:
            name: Raw name
            
        Returns:
            Sanitized filename
        """
        # Replace invalid filename characters
        name = re.sub(r'[\\/*?:"<>|]', '', name)
        # Replace spaces with underscores
        name = re.sub(r'\s+', '_', name.strip())
        # Ensure ASCII only
        name = name.encode('ascii', 'ignore').decode('ascii')
        # Limit length
        if len(name) > 50:
            name = name[:50]
        return name.lower()
    
    def _extract_genre_name(self, url: str) -> str:
        """Extract genre name from URL.
        
        Args:
            url: Genre page URL
            
        Returns:
            Genre name
        """
        # Try to extract genre from URL
        match = re.search(r'/([^/]+)/artistas\.html$', url)
        if match:
            return match.group(1)
        
        # Default fallback
        return "unknown_genre"
