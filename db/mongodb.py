from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from py_utils import StrValueEnum
import os


class MongoHostEnum(StrValueEnum):
    PROD = "cluster0.wte93.mongodb.net"


class MongoDB:
    def __init__(
        self,
        user: str = "joaomj1800",
        cluster: str = "Cluster0",
        host: str = MongoHostEnum.PROD,
    ):
        self.user = user
        self.cluster = cluster
        self.host = host
        self.password = self._get_password()
        self.uri = "mongodb+srv://{user}:{pwd}@{host}/?retryWrites=true&w=majority&appName={cluster}".format(
            user=self.user, pwd=self.password, host=self.host, cluster=self.cluster
        )

        self.client: MongoClient = MongoClient(host=self.uri, server_api=ServerApi("1"))

    @staticmethod
    def _get_password():
        password = os.getenv("MONGO_DB_PASSWORD")
        if password:
            return password
        else:
            raise Exception("Unable to fetch MONGO_DB_PASSWORD from env")
