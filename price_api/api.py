from abc import ABC


class ApiABC(ABC):
    """
    Base API class
    """

    def __init__(self, base_url: str):
        self.base_url: str = base_url
