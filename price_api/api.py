from abc import ABC
import os
from dotenv import load_dotenv

load_dotenv()


class ApiABC(ABC):
    """
    Base API class
    """

    def __init__(self, base_url: str):
        self.base_url: str = base_url

    @staticmethod
    def _get_os_key(key):
        key_str = os.getenv(key)
        if key_str:
            return key_str
        else:
            raise ValueError("Key Unavailable in the ENV")
