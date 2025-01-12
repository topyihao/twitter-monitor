from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
from config_manager import ConfigManager

class DatabaseManager:
    def __init__(self):
        self.logger = logging.getLogger('database_manager')
        self.config = ConfigManager()

        try:
            self.client = MongoClient(self.config.mongodb_uri)
            self.db = self.client[self.config.database_name]
            
            # Setup collections
            self.tweets = self.db[self.config.collections['tweets']]
            self.user_metrics = self.db[self.config.collections['user_metrics']]
            self.analysis = self.db[self.config.collections['analysis']]
            
            self._setup_indexes()
            self.logger.info(f"Connected to database: {self.config.database_name}")
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise

    def _setup_indexes(self):
        """Setup optimized indexes for AI analysis"""
        # Tweets collection indexes
        tweet_indexes = [
            ("tweet_id", ASCENDING),  # Unique identifier
            ("created_at", DESCENDING),
            ("username", ASCENDING),
            ("hashtags", ASCENDING),  # Hashtag analysis
        ]
        
        for index in tweet_indexes:
            if isinstance(index, tuple):
                self.tweets.create_index([index], unique=(index[0] == "tweet_id"))
            else:
                self.tweets.create_index(index)

    def save_tweet(self, tweet_data: dict) -> bool:
        """Save enriched tweet data for AI analysis"""
        try:
            # Enrich tweet data with AI-friendly features
            enriched_tweet = self._enrich_tweet_data(tweet_data)
            
            # Update or insert tweet
            result = self.tweets.insert_one(enriched_tweet)
            print(result)
            self.logger.info(f"Saved tweet {enriched_tweet['tweet_id']} to database")
            return True
        except Exception as e:
            self.logger.error(f"Error saving tweet: {e}")
            return False

    def _enrich_tweet_data(self, tweet_data: dict) -> dict:
        """Enrich tweet data with AI-relevant features"""
        enriched = {
            # Core Information
            "tweet_id": tweet_data["tweet_id"],
            "username": tweet_data["username"],
            "created_at": datetime.fromisoformat(tweet_data["created_at"]),
            "saved_at": datetime.now(),
            
            # Content Analysis
            "content": {
                "raw_text": tweet_data.get("cleaned_text", ""),
                "hashtags": tweet_data.get("hashtags", []),
                "mentions": tweet_data.get("mentions", []),
                "urls": tweet_data.get("urls", []),
                "language": tweet_data.get("language"),
            },
            
            # Media Information
            "media": tweet_data.get("media", {}),
        }
        return enriched

    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()