import os
from dotenv import dotenv_values

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