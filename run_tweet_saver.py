#!/usr/bin/python3

import json
import logging
import os
import sys
import signal
import atexit
from datetime import datetime
import click
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

from tweet_saver import TweetSaver

class TweetSaverManager:
    def __init__(self):
        self.scheduler = None
        self.savers = []
        self.logger = logging.getLogger('tweet_saver_manager')
        
    def init_logging(self):
        """Initialize logging configuration"""
        log_dir = os.path.join(os.path.dirname(__file__), 'log')
        os.makedirs(log_dir, exist_ok=True)
        
        # Main log file
        log_file = os.path.join(log_dir, 'tweet_saver.log')
        logging.basicConfig(
            filename=log_file,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            level=logging.INFO
        )
        
        # Also log to console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(console_handler)

    def cleanup(self):
        """Cleanup resources and save state"""
        if self.scheduler and self.scheduler.running:
            self.logger.info("Shutting down scheduler...")
            self.scheduler.shutdown(wait=False)
            
        # Save final state for each saver
        for saver in self.savers:
            self.logger.info(f"Saving final state for {saver.username}")
            try:
                state = {
                    'last_tweet_id': saver.last_tweet_id,
                    'last_check': datetime.now().isoformat()
                }
                state_file = os.path.join(saver.output_dir, 'state.json')
                with open(state_file, 'w') as f:
                    json.dump(state, f, indent=2)
            except Exception as e:
                self.logger.error(f"Error saving state for {saver.username}: {e}")
        
        self.logger.info("Cleanup completed")

    def handle_signal(self, signum, frame):
        """Handle termination signals"""
        signal_name = signal.Signals(signum).name
        self.logger.info(f"Received signal {signal_name}")
        self.cleanup()
        sys.exit(0)

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
        atexit.register(self.cleanup)

    def load_configs(self, config_dir):
        """Load configuration files"""
        token_config_path = os.path.join(config_dir, 'token.json')
        user_config_path = os.path.join(config_dir, 'users_to_save.json')
        
        try:
            with open(token_config_path, 'r') as f:
                token_config = json.load(f)
                if not token_config.get('twitter_auth_username_list'):
                    raise ValueError("No Twitter auth usernames found in config")
        except Exception as e:
            self.logger.error(f"Error loading token config: {e}")
            sys.exit(1)
            
        try:
            with open(user_config_path, 'r') as f:
                user_config = json.load(f)
                if not user_config.get('users'):
                    raise ValueError("No users to monitor found in config")
        except Exception as e:
            self.logger.error(f"Error loading user config: {e}")
            sys.exit(1)
            
        return token_config, user_config

    def run(self, config_dir, cookies_dir, interval):
        """Run the tweet saver manager"""
        self.init_logging()
        self.setup_signal_handlers()
        self.logger.info(f"Starting Tweet Saver Manager with {interval}s interval")
        
        token_config, user_config = self.load_configs(config_dir)
        
        # Setup scheduler
        executors = {'default': ThreadPoolExecutor(max_workers=len(user_config['users']))}
        self.scheduler = BlockingScheduler(executors=executors)
        
        # Create tweet savers for each user
        for user in user_config['users']:
            try:
                saver = TweetSaver(
                    username=user['username'],
                    token_config=token_config,
                    user_config=user,
                    cookies_dir=cookies_dir, 
                    storage_type='mongodb'
                )
                self.savers.append(saver)
                
                self.scheduler.add_job(
                    saver.watch,
                    trigger='interval',
                    seconds=interval,
                    max_instances=1,  # Prevent overlapping runs
                    coalesce=True     # Combine missed runs
                )
                self.logger.info(f"Added monitoring job for user: {user['username']}")
                
            except Exception as e:
                self.logger.error(f"Error setting up saver for {user['username']}: {e}")
                continue
        
        try:
            self.logger.info("Starting scheduler...")
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            self.logger.info("Received shutdown signal")
        except Exception as e:
            self.logger.error(f"Error in scheduler: {e}")
        finally:
            self.cleanup()


@click.command(context_settings={'show_default': True})
@click.option('--config_dir', 
              default=os.path.join(sys.path[0], 'config'), 
              help="Directory containing configuration files")
@click.option('--cookies_dir', 
              default=os.path.join(sys.path[0], 'cookies'), 
              help="Directory containing cookie files")
@click.option('--interval', 
              default=15, 
              help="Monitor run interval in seconds")
def main(config_dir, cookies_dir, interval):
    """Run the tweet saver to monitor and save tweets from specified users"""
    manager = TweetSaverManager()
    manager.run(config_dir, cookies_dir, interval)


if __name__ == '__main__':
    main()