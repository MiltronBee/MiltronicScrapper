"""
YouTube Transcript Handler for Mexican Spanish content discovery.
Extracts transcripts from Mexican YouTubers and Spanish-language channels.
"""

import logging
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import requests
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound
from .geographic_filter import GeographicFilter
import re


class YouTubeHandler:
    """
    Specialized handler for YouTube transcript extraction.
    Focuses on Mexican YouTubers and Spanish-language content.
    """
    
    def __init__(self, youtube_config: Dict):
        self.config = youtube_config
        self.logger = logging.getLogger(__name__)
        self.api_key = youtube_config.get('api_key')
        self.geographic_filter = GeographicFilter()
        
        # Mexican YouTube channels (channel IDs would be extracted from URLs)
        self.mexican_channels = {
            'luisitocomunica': 'UCBuJWy2Ebb8IFEOg9TGMOjQ',
            'yuya': 'UCLNGKYAZIl9_Fj4u4GJj_sw', 
            'werevertumorro': 'UCCGgOjjQhE5Xl9f9PIXsIKQ',
            'elrubius': 'UCXazJD2CjJuNNqgOQgks9XQ',
            'facturafx': 'UC6LMNPTqz6Pc-EbP4TGRBAw'
        }
        
        # Content quality thresholds
        self.min_transcript_length = 100  # Minimum transcript length
        self.max_videos_per_channel = self.config.get('max_videos_per_channel', 500)
        
        # API base URL
        self.api_base = "https://www.googleapis.com/youtube/v3"
    
    def discover_content(self, channel_urls: List[str] = None) -> Dict[str, List[Dict]]:
        """
        Discover Mexican Spanish content from YouTube channels.
        
        Args:
            channel_urls: List of YouTube channel URLs
            
        Returns:
            Dictionary mapping channels to lists of transcript content
        """
        if not self.api_key:
            self.logger.error("YouTube API key not provided")
            return {}
        
        all_content = {}
        
        # Use provided channels or default Mexican channels
        channels_to_process = {}
        
        if channel_urls:
            for url in channel_urls:
                channel_id = self._extract_channel_id(url)
                if channel_id:
                    channel_name = self._get_channel_name(channel_id)
                    channels_to_process[channel_name] = channel_id
        else:
            channels_to_process = self.mexican_channels
        
        for channel_name, channel_id in channels_to_process.items():
            try:
                self.logger.info(f"Processing YouTube channel: {channel_name}")
                
                channel_content = self._process_channel(channel_id, channel_name)
                
                if channel_content:
                    all_content[f"youtube_{channel_name}"] = channel_content
                    self.logger.info(f"Collected {len(channel_content)} transcripts from {channel_name}")
                
                # Rate limiting
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Failed to process channel {channel_name}: {e}")
                continue
        
        total_items = sum(len(items) for items in all_content.values())
        self.logger.info(f"YouTube discovery complete: {total_items} total transcripts")
        
        return all_content
    
    def _extract_channel_id(self, url: str) -> Optional[str]:
        """Extract channel ID from YouTube URL."""
        patterns = [
            r'youtube\.com\/channel\/([a-zA-Z0-9_-]+)',
            r'youtube\.com\/c\/([a-zA-Z0-9_-]+)',
            r'youtube\.com\/@([a-zA-Z0-9_-]+)',
            r'youtube\.com\/user\/([a-zA-Z0-9_-]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                username_or_id = match.group(1)
                
                # If it's a username/handle, resolve to channel ID
                if not username_or_id.startswith('UC'):
                    return self._resolve_channel_id(username_or_id)
                else:
                    return username_or_id
        
        return None
    
    def _resolve_channel_id(self, username: str) -> Optional[str]:
        """Resolve username to channel ID using YouTube API."""
        try:
            url = f"{self.api_base}/channels"
            params = {
                'key': self.api_key,
                'forUsername': username,
                'part': 'id'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data.get('items'):
                return data['items'][0]['id']
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error resolving channel ID for {username}: {e}")
            return None
    
    def _get_channel_name(self, channel_id: str) -> str:
        """Get channel name from channel ID."""
        try:
            url = f"{self.api_base}/channels"
            params = {
                'key': self.api_key,
                'id': channel_id,
                'part': 'snippet'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data.get('items'):
                return data['items'][0]['snippet']['title']
            
            return channel_id
            
        except Exception as e:
            self.logger.debug(f"Error getting channel name for {channel_id}: {e}")
            return channel_id
    
    def _process_channel(self, channel_id: str, channel_name: str) -> List[Dict]:
        """Process a single YouTube channel."""
        try:
            videos = self._get_channel_videos(channel_id)
            transcripts = []
            
            for video in videos[:self.max_videos_per_channel]:
                try:
                    transcript_data = self._extract_video_transcript(video, channel_name)
                    if transcript_data:
                        transcripts.append(transcript_data)
                    
                    # Rate limiting
                    time.sleep(0.5)
                    
                except Exception as e:
                    self.logger.debug(f"Error processing video {video['id']}: {e}")
                    continue
            
            return transcripts
            
        except Exception as e:
            self.logger.error(f"Error processing channel {channel_id}: {e}")
            return []
    
    def _get_channel_videos(self, channel_id: str) -> List[Dict]:
        """Get list of videos from a YouTube channel."""
        try:
            # First get the uploads playlist ID
            url = f"{self.api_base}/channels"
            params = {
                'key': self.api_key,
                'id': channel_id,
                'part': 'contentDetails'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if not data.get('items'):
                return []
            
            uploads_playlist_id = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            
            # Get videos from uploads playlist
            videos = []
            next_page_token = None
            max_results = 50
            
            while len(videos) < self.max_videos_per_channel:
                url = f"{self.api_base}/playlistItems"
                params = {
                    'key': self.api_key,
                    'playlistId': uploads_playlist_id,
                    'part': 'snippet',
                    'maxResults': min(max_results, self.max_videos_per_channel - len(videos))
                }
                
                if next_page_token:
                    params['pageToken'] = next_page_token
                
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                
                for item in data.get('items', []):
                    video_data = {
                        'id': item['snippet']['resourceId']['videoId'],
                        'title': item['snippet']['title'],
                        'description': item['snippet']['description'],
                        'published_at': item['snippet']['publishedAt']
                    }
                    videos.append(video_data)
                
                next_page_token = data.get('nextPageToken')
                if not next_page_token:
                    break
                
                time.sleep(0.1)  # Rate limiting
            
            self.logger.info(f"Found {len(videos)} videos for channel {channel_id}")
            return videos
            
        except Exception as e:
            self.logger.error(f"Error getting videos for channel {channel_id}: {e}")
            return []
    
    def _extract_video_transcript(self, video: Dict, channel_name: str) -> Optional[Dict]:
        """Extract transcript from a single video."""
        try:
            video_id = video['id']
            video_title = video['title']
            
            # Try to get Spanish transcript
            transcript_languages = self.config.get('transcript_languages', ['es', 'es-MX'])
            transcript_text = None
            
            for lang in transcript_languages:
                try:
                    transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=[lang])
                    
                    # Combine transcript segments
                    transcript_segments = []
                    for segment in transcript_list:
                        text = segment['text'].strip()
                        # Clean up auto-generated transcript artifacts
                        text = re.sub(r'\[.*?\]', '', text)  # Remove [Music], [Applause], etc.
                        text = re.sub(r'\s+', ' ', text)     # Normalize whitespace
                        if text:
                            transcript_segments.append(text)
                    
                    transcript_text = ' '.join(transcript_segments)
                    break
                    
                except NoTranscriptFound:
                    continue
            
            if not transcript_text or len(transcript_text) < self.min_transcript_length:
                return None
            
            # Check if content is Mexican/Spanish
            is_mexican, geo_score, reasons = self.geographic_filter.is_mexican_content(
                transcript_text, video_title, f"https://youtube.com/watch?v={video_id}", min_score=1.0
            )
            
            # More lenient for video content
            if geo_score.total_score < 0.5:
                # Check for basic Spanish indicators
                if not self._has_spanish_indicators(transcript_text):
                    return None
            
            # Create transcript content item
            content_item = {
                'type': 'youtube_transcript',
                'title': video_title,
                'text': transcript_text,
                'url': f"https://youtube.com/watch?v={video_id}",
                'channel': channel_name,
                'video_id': video_id,
                'published_at': datetime.fromisoformat(video['published_at'].replace('Z', '+00:00')),
                'description': video['description'][:500],  # First 500 chars
                'transcript_length': len(transcript_text),
                'mexican_score': geo_score.total_score
            }
            
            return content_item
            
        except Exception as e:
            self.logger.debug(f"Error extracting transcript for video {video.get('id', 'unknown')}: {e}")
            return None
    
    def _has_spanish_indicators(self, text: str) -> bool:
        """Quick check for Spanish language indicators."""
        spanish_chars = ['ñ', 'ü', 'á', 'é', 'í', 'ó', 'ú']
        spanish_words = ['que', 'pero', 'como', 'para', 'con', 'por', 'este', 'una', 'muy', 'hola', 'gracias']
        
        text_lower = text.lower()
        
        # Check for Spanish characters
        if any(char in text_lower for char in spanish_chars):
            return True
        
        # Check for common Spanish words
        words = text_lower.split()
        spanish_word_count = sum(1 for word in words if word in spanish_words)
        
        # Need higher threshold for transcripts (more words)
        return spanish_word_count >= len(words) * 0.05  # At least 5% Spanish words
    
    def get_trending_mexican_videos(self, region_code: str = 'MX') -> List[Dict]:
        """Get trending videos from Mexico."""
        if not self.api_key:
            return []
        
        try:
            url = f"{self.api_base}/videos"
            params = {
                'key': self.api_key,
                'part': 'snippet,statistics',
                'chart': 'mostPopular',
                'regionCode': region_code,
                'maxResults': 50
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            trending_content = []
            
            for item in data.get('items', []):
                video_id = item['id']
                video_title = item['snippet']['title']
                
                # Try to extract transcript
                transcript_data = self._extract_video_transcript({
                    'id': video_id,
                    'title': video_title,
                    'description': item['snippet']['description'],
                    'published_at': item['snippet']['publishedAt']
                }, 'trending_mx')
                
                if transcript_data:
                    # Add trending metrics
                    transcript_data.update({
                        'view_count': int(item['statistics'].get('viewCount', 0)),
                        'like_count': int(item['statistics'].get('likeCount', 0)),
                        'comment_count': int(item['statistics'].get('commentCount', 0)),
                        'trending_rank': len(trending_content) + 1
                    })
                    trending_content.append(transcript_data)
            
            self.logger.info(f"Found {len(trending_content)} trending Mexican videos with transcripts")
            return trending_content
            
        except Exception as e:
            self.logger.error(f"Error getting trending Mexican videos: {e}")
            return []