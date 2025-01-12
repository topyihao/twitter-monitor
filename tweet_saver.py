#!/usr/bin/python3

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List

from monitor_base import MonitorBase
from utils import (parse_media_from_tweet, parse_text_from_tweet, 
                  parse_create_time_from_tweet, find_all, find_one, 
                  get_content, convert_html_to_text)

from database_manager import DatabaseManager

def _verify_tweet_user_id(tweet: dict, user_id: str) -> bool:
    user = find_one(tweet, 'user_results')
    return find_one(user, 'rest_id') == user_id


class TweetSaver(MonitorBase):
    monitor_type = 'TweetSaver'

    def __init__(self, username: str, token_config: dict, user_config: dict, cookies_dir: str, storage_type: str='mangodb'):
        super().__init__(monitor_type=self.monitor_type,
                        username=username,
                        token_config=token_config,
                        user_config=user_config,
                        cookies_dir=cookies_dir)
        
        self.storage_type = storage_type.lower()
        if self.storage_type not in ['mongodb', 'json']:
            raise ValueError("storage_type must be either 'mongodb' or 'json'")

        # Create output directory for saved tweets if using JSON storage
        self.output_dir = os.path.join(os.path.dirname(__file__), 'saved_tweets', username)
        if self.storage_type == 'json':
            os.makedirs(self.output_dir, exist_ok=True)
     
        tweet_list = self.get_tweet_list()
        while tweet_list is None:
            self.logger.warning(f"Failed to get tweets for {username}, retrying...")
            tweet_list = self.get_tweet_list()

        self.last_tweet_id = -1
        for tweet in tweet_list:
            if _verify_tweet_user_id(tweet, self.user_id):
                self.last_tweet_id = max(self.last_tweet_id, int(find_one(tweet, 'rest_id')))

        # Initialize database manager only if using MongoDB
        self.db_manager = DatabaseManager() if self.storage_type == 'mongodb' else None

        self.logger.info(f'Initialized tweet saver for {username}. User ID: {self.user_id}, Last tweet: {self.last_tweet_id}')

    def get_tweet_list(self) -> dict:
        api_name = 'UserTweetsAndReplies'
        params = {
            'userId': self.user_id,
            'includePromotedContent': True,
            'withVoice': True,
            'count': 1000
        }
        json_response = self.twitter_watcher.query(api_name, params)
        return find_all(json_response, 'tweet_results') if json_response else None

    # In the save_tweet_to_file method:
    def save_tweet_to_file(self, tweet_data: dict, text: str, photo_urls: list, video_urls: list):
        """Save tweet data optimized for AI analysis"""
        tweet_id = find_one(tweet_data, 'rest_id')
        engagement = get_content(tweet_data).get('legacy', {})
        
        output = {
            # Basic Information
            'tweet_id': tweet_id,
            'username': self.username,
            'created_at': parse_create_time_from_tweet(tweet_data).isoformat(),
            
            # Content
            'cleaned_text': self._clean_text(text),
            'hashtags': self._extract_hashtags(text),
            'mentions': self._extract_mentions(text),
            'urls': self._extract_urls(text),
            
            # Media
            'media': {
                'photos': photo_urls,
                'videos': video_urls,
                'has_media': bool(photo_urls or video_urls)
            },
            
            # Engagement
            'engagement': {
                'retweet_count': engagement.get('retweet_count', 0),
                'reply_count': engagement.get('reply_count', 0),
                'like_count': engagement.get('favorite_count', 0),
                'quote_count': engagement.get('quote_count', 0)
            },
            
            # Context
            'is_retweet': bool(find_one(tweet_data, 'retweeted_status_result')),
            'is_reply': bool(engagement.get('in_reply_to_status_id_str')),
            'is_quote': bool(find_one(tweet_data, 'quoted_status_result')),
            'conversation_id': engagement.get('conversation_id_str'),
            
            # Language and Location
            'language': get_content(tweet_data).get('lang'),
            'location': get_content(tweet_data).get('location'),
        }
        
        if self.storage_type == 'mongodb':
            success = self.db_manager.save_tweet(output)
            if success:
                self.logger.info(f'Saved tweet {tweet_id} to MongoDB')
            else:
                self.logger.error(f'Failed to save tweet {tweet_id} to MongoDB')
        else:  # JSON storage
            try:
                file_path = os.path.join(self.output_dir, f'tweet_{tweet_id}.json')
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(output, f, ensure_ascii=False, indent=2)
                self.logger.info(f'Saved tweet {tweet_id} to JSON file')
            except Exception as e:
                self.logger.error(f'Failed to save tweet {tweet_id} to JSON: {str(e)}')


    def _clean_text(self, text: str) -> str:
        """Remove URLs, mentions, and special characters for better NLP processing"""
        import re
        # Remove URLs
        text = re.sub(r'http\S+|www.\S+', '', text)
        # Remove mentions
        text = re.sub(r'@\w+', '', text)
        # Remove multiple spaces
        text = ' '.join(text.split())
        return text.strip()

    def _extract_hashtags(self, text: str) -> list:
        """Extract hashtags from text"""
        import re
        return re.findall(r'#(\w+)', text)

    def _extract_mentions(self, text: str) -> list:
        """Extract mentions from text"""
        import re
        return re.findall(r'@(\w+)', text)

    def _extract_urls(self, text: str) -> list:
        """Extract URLs from text"""
        import re
        return re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)

    def _determine_tweet_type(self, tweet_data: dict) -> str:
        """Determine the type of tweet"""
        if find_one(tweet_data, 'retweeted_status_result'):
            return 'retweet'
        elif find_one(tweet_data, 'quoted_status_result'):
            return 'quote'
        elif get_content(tweet_data).get('legacy', {}).get('in_reply_to_status_id_str'):
            return 'reply'
        return 'original'

    def _extract_sentiment_indicators(self, text: str) -> dict:
        """Extract basic sentiment indicators"""
        text_lower = text.lower()
        return {
            'has_positive_emoji': any(emoji in text for emoji in ['😊', '👍', '❤️']),
            'has_negative_emoji': any(emoji in text for emoji in ['😢', '👎', '😡']),
            'exclamation_count': text.count('!'),
            'question_count': text.count('?'),
            'capitalized_words': len([word for word in text.split() if word.isupper()])
        }

    def watch(self) -> bool:
        tweet_list = self.get_tweet_list()
        if tweet_list is None:
            self.logger.error("Failed to get tweet list")
            return False

        max_tweet_id = -1
        new_tweet_list = []
        time_threshold = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(minutes=5)
        
        for tweet in tweet_list:
            if not _verify_tweet_user_id(tweet, self.user_id):
                continue
            tweet_id = int(find_one(tweet, 'rest_id'))
            if tweet_id <= self.last_tweet_id:
                continue
            if parse_create_time_from_tweet(tweet) < time_threshold:
                continue

            new_tweet_list.append(tweet)
            max_tweet_id = max(max_tweet_id, tweet_id)

        self.last_tweet_id = max(self.last_tweet_id, max_tweet_id)

        for tweet in reversed(new_tweet_list):
            text = parse_text_from_tweet(tweet)
            retweet = find_one(tweet, 'retweeted_status_result')
            quote = find_one(tweet, 'quoted_status_result')
            
            if retweet:
                photo_url_list, video_url_list = parse_media_from_tweet(retweet)
            else:
                photo_url_list, video_url_list = parse_media_from_tweet(tweet)
                if quote:
                    quote_text = get_content(quote).get('full_text', '')
                    quote_user = find_one(quote, 'user_results')
                    quote_username = get_content(quote_user).get('screen_name', '')
                    text += '\n\nQuote: @{}: {}'.format(quote_username, quote_text)
            
            source = find_one(tweet, 'source')
            text += '\n\nSource: {}'.format(convert_html_to_text(source))
            
            self.save_tweet_to_file(tweet, text, photo_url_list, video_url_list)

        self.update_last_watch_time()
        return True

    def status(self) -> str:
        return f'Last: {self.get_last_watch_time()}, id: {self.last_tweet_id}'