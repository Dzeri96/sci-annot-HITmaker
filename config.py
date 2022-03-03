import os
from dotenv import dotenv_values
import json

class Config:
    __conf = {
        **os.environ
    }

    @staticmethod
    def get(name):
        return Config.__conf[name]

    @staticmethod
    def set(name, value):
        Config.__conf[name] = value
    
    @staticmethod
    def parse_env_file(file_path):
        Config.__conf = {
            **dotenv_values(file_path),
            **os.environ
        }

        # Parse JSON and set None if variable does not exist
        if 'active_page_groups' in Config.__conf:
            Config.__conf['active_page_groups'] = json.loads(Config.__conf['active_page_groups'])
        else:
            Config.__conf['active_page_groups'] = None