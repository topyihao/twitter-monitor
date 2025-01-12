import os
import json
from typing import Dict

class ConfigManager:
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._config is None:
            self.load_config()
    
    def load_config(self):
        """Load configuration from file and environment variables"""
        # Get environment (default to development)
        env = 'development'
        
        # Load base config
        config_path = os.path.join(os.path.dirname(__file__), 'config', 'database.json')
        try:
            with open(config_path, 'r') as f:
                self._config = json.load(f)[env]
        except Exception as e:
            raise Exception(f"Failed to load database config: {e}")
        
        return self
    
    @property
    def mongodb_uri(self) -> str:
        """Get MongoDB URI"""
        return self._config['mongodb_uri']
    
    @property
    def database_name(self) -> str:
        """Get database name"""
        return self._config['database']
    
    @property
    def collections(self) -> Dict:
        """Get collections configuration"""
        return self._config['collections']