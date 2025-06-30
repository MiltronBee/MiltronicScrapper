"""
Reddit API Handler for Mexican Spanish content discovery.
Efficiently extracts posts and comments from Mexican subreddits using PRAW.
"""

import logging
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import praw
import prawcore
from .geographic_filter import GeographicFilter


class RedditHandler:
    """
    Specialized handler for Reddit content using the official API.
    Focuses on Mexican subreddits and Spanish-language content.
    """
    
    def __init__(self, reddit_config: Dict):
        self.config = reddit_config
        self.logger = logging.getLogger(__name__)
        self.reddit = None
        self.geographic_filter = GeographicFilter()
        
        # Mexican subreddits of interest
        self.mexican_subreddits = [
            'mexico',
            'LigaMX',
            'mexicocity', 
            'tijuana',
            'guadalajara',
            'monterrey',
            'Mujico',  # Popular Mexican meme subreddit
            'TecDeMonterrey',
            'UNAM',
            'mexicocirclejerk'
        ]
        
        # Content quality thresholds
        self.min_score = 1  # Minimum upvotes
        self.min_comment_length = 20  # Minimum comment length
        self.max_comment_length = 5000  # Maximum comment length
        
        self._initialize_reddit()
    
    def _initialize_reddit(self):
        """Initialize Reddit API client."""
        try:
            self.reddit = praw.Reddit(
                client_id=self.config.get('client_id'),
                client_secret=self.config.get('client_secret'),
                user_agent=self.config.get('user_agent', 'MexicanCorpusBot/1.0'),
                read_only=True
            )
            
            # Test connection
            self.reddit.user.me()
            self.logger.info("Reddit API initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Reddit API: {e}")
            self.reddit = None
    
    def discover_content(self, limit: int = 1000, time_filter: str = 'week') -> Dict[str, List[Dict]]:
        """
        Discover Mexican Spanish content from Reddit.
        
        Args:
            limit: Maximum posts per subreddit
            time_filter: Time filter ('day', 'week', 'month', 'year', 'all')
            
        Returns:
            Dictionary mapping subreddits to lists of content
        """
        if not self.reddit:
            self.logger.error("Reddit API not initialized")
            return {}
        
        all_content = {}
        
        for subreddit_name in self.mexican_subreddits:
            try:
                self.logger.info(f"Processing subreddit: r/{subreddit_name}")
                
                subreddit_content = self._process_subreddit(
                    subreddit_name, limit, time_filter
                )
                
                if subreddit_content:
                    all_content[f"reddit_{subreddit_name}"] = subreddit_content
                    self.logger.info(f"Collected {len(subreddit_content)} items from r/{subreddit_name}")
                
                # Rate limiting
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Failed to process r/{subreddit_name}: {e}")
                continue
        
        total_items = sum(len(items) for items in all_content.values())
        self.logger.info(f"Reddit discovery complete: {total_items} total items")
        
        return all_content
    
    def _process_subreddit(self, subreddit_name: str, limit: int, time_filter: str) -> List[Dict]:
        """Process a single subreddit."""
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            content_items = []
            
            # Get hot posts
            hot_posts = list(subreddit.hot(limit=limit//2))
            content_items.extend(self._process_posts(hot_posts, 'hot'))
            
            # Get top posts from time period
            top_posts = list(subreddit.top(time_filter=time_filter, limit=limit//2))
            content_items.extend(self._process_posts(top_posts, 'top'))
            
            return content_items
            
        except prawcore.exceptions.NotFound:
            self.logger.warning(f"Subreddit r/{subreddit_name} not found or private")
            return []
        except Exception as e:
            self.logger.error(f"Error processing r/{subreddit_name}: {e}")
            return []
    
    def _process_posts(self, posts: List, post_type: str) -> List[Dict]:
        """Process Reddit posts and their comments."""
        content_items = []
        
        for post in posts:
            try:
                # Skip low-quality posts
                if post.score < self.min_score:
                    continue
                
                # Extract post content
                post_text = self._extract_post_text(post)
                if not post_text:
                    continue
                
                # Check if content is Mexican/Spanish
                is_mexican, geo_score, reasons = self.geographic_filter.is_mexican_content(
                    post_text, post.title, f"https://reddit.com{post.permalink}", min_score=1.0
                )
                
                # More lenient for social media content
                if not is_mexican and geo_score.total_score < 1.0:
                    continue
                
                # Create content item for post
                content_item = {
                    'type': 'reddit_post',
                    'title': post.title,
                    'text': post_text,
                    'url': f"https://reddit.com{post.permalink}",
                    'score': post.score,
                    'created_utc': datetime.fromtimestamp(post.created_utc),
                    'subreddit': str(post.subreddit),
                    'author': str(post.author) if post.author else '[deleted]',
                    'num_comments': post.num_comments,
                    'mexican_score': geo_score.total_score,
                    'post_type': post_type
                }
                
                content_items.append(content_item)
                
                # Extract comments if enabled
                if self.config.get('crawl_comments', True):
                    comments = self._extract_comments(post)
                    content_items.extend(comments)
                
            except Exception as e:
                self.logger.debug(f"Error processing post {post.id}: {e}")
                continue
        
        return content_items
    
    def _extract_post_text(self, post) -> str:
        """Extract text content from a Reddit post."""
        text_parts = []
        
        # Post title
        if post.title:
            text_parts.append(post.title)
        
        # Post body (self text)
        if hasattr(post, 'selftext') and post.selftext:
            text_parts.append(post.selftext)
        
        # Join with newlines
        full_text = '\n\n'.join(text_parts)
        
        # Basic filtering
        if len(full_text.strip()) < 20:
            return ""
        
        return full_text.strip()
    
    def _extract_comments(self, post) -> List[Dict]:
        """Extract comments from a Reddit post."""
        comments = []
        comment_limit = self.config.get('api_limit', 1000) // 10  # Limit comments per post
        
        try:
            # Expand all comments
            post.comments.replace_more(limit=self.config.get('comment_depth', 3))
            
            comment_count = 0
            for comment in post.comments.list():
                if comment_count >= comment_limit:
                    break
                
                try:
                    # Skip deleted/removed comments
                    if not hasattr(comment, 'body') or comment.body in ['[deleted]', '[removed]']:
                        continue
                    
                    # Skip very short or very long comments
                    comment_length = len(comment.body)
                    if comment_length < self.min_comment_length or comment_length > self.max_comment_length:
                        continue
                    
                    # Skip heavily downvoted comments
                    if hasattr(comment, 'score') and comment.score < -5:
                        continue
                    
                    # Check if comment is Spanish/Mexican
                    is_mexican, geo_score, reasons = self.geographic_filter.is_mexican_content(
                        comment.body, "", f"https://reddit.com{post.permalink}", min_score=0.5
                    )
                    
                    # Very lenient for comments
                    if geo_score.total_score < 0.5:
                        # Check for basic Spanish indicators
                        if not self._has_spanish_indicators(comment.body):
                            continue
                    
                    # Create comment content item
                    comment_item = {
                        'type': 'reddit_comment',
                        'title': f"Comentario en: {post.title[:50]}...",
                        'text': comment.body,
                        'url': f"https://reddit.com{post.permalink}#{comment.id}",
                        'score': getattr(comment, 'score', 0),
                        'created_utc': datetime.fromtimestamp(comment.created_utc),
                        'subreddit': str(post.subreddit),
                        'author': str(comment.author) if comment.author else '[deleted]',
                        'parent_post_title': post.title,
                        'mexican_score': geo_score.total_score,
                        'post_type': 'comment'
                    }
                    
                    comments.append(comment_item)
                    comment_count += 1
                    
                except Exception as e:
                    self.logger.debug(f"Error processing comment: {e}")
                    continue
            
        except Exception as e:
            self.logger.warning(f"Error extracting comments from post {post.id}: {e}")
        
        return comments
    
    def _has_spanish_indicators(self, text: str) -> bool:
        """Quick check for Spanish language indicators."""
        spanish_chars = ['ñ', 'ü', 'á', 'é', 'í', 'ó', 'ú']
        spanish_words = ['que', 'pero', 'como', 'para', 'con', 'por', 'este', 'una', 'muy']
        
        text_lower = text.lower()
        
        # Check for Spanish characters
        if any(char in text_lower for char in spanish_chars):
            return True
        
        # Check for common Spanish words
        words = text_lower.split()
        spanish_word_count = sum(1 for word in words if word in spanish_words)
        
        return spanish_word_count >= 2
    
    def get_trending_mexican_content(self, hours_back: int = 24) -> List[Dict]:
        """Get trending Mexican content from the last N hours."""
        if not self.reddit:
            return []
        
        cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
        trending_content = []
        
        for subreddit_name in self.mexican_subreddits[:5]:  # Limit to top 5 subreddits
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                
                # Get hot posts from the last day
                for post in subreddit.hot(limit=50):
                    post_time = datetime.fromtimestamp(post.created_utc)
                    
                    if post_time > cutoff_time and post.score > 10:
                        post_text = self._extract_post_text(post)
                        if post_text:
                            trending_content.append({
                                'type': 'reddit_trending',
                                'title': post.title,
                                'text': post_text,
                                'url': f"https://reddit.com{post.permalink}",
                                'score': post.score,
                                'created_utc': post_time,
                                'subreddit': subreddit_name,
                                'trending_score': post.score / max(1, (datetime.utcnow() - post_time).total_seconds() / 3600)
                            })
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                self.logger.error(f"Error getting trending content from r/{subreddit_name}: {e}")
                continue
        
        # Sort by trending score
        trending_content.sort(key=lambda x: x['trending_score'], reverse=True)
        
        self.logger.info(f"Found {len(trending_content)} trending Mexican posts")
        return trending_content[:100]  # Return top 100