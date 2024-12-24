from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from py_utils import StrValueEnum
from datetime import datetime
import os
import pandas as pd


class MongoHostEnum(StrValueEnum):
    PROD = "cluster0.wte93.mongodb.net"


class MongoDB:
    def __init__(
        self,
        user: str = os.getenv("MONGO_DB_USER", "service_account"),
        cluster: str = "Cluster0",
        host: str = MongoHostEnum.PROD,
    ):
        self.user = user
        self.cluster = cluster
        self.host = host
        self.password = self._get_password()
        self.uri = "mongodb+srv://service_account:{pwd}@{host}/?retryWrites=true&w=majority&appName=Cluster0".format(
            pwd=self._get_password(), host=self.host
        )

        self.client: MongoClient = MongoClient(host=self.uri, server_api=ServerApi("1"))

    @staticmethod
    def _get_password():
        password = os.getenv("MONGO_DB_PASSWORD")
        if password:
            return password
        else:
            raise Exception("Unable to fetch MONGO_DB_PASSWORD from env")

    def save_dataframe_to_collection(
        self, dataframe: pd.DataFrame, database: str, collection: str
    ):
        """
        Save a pandas DataFrame to a MongoDB collection.

        Args:
            dataframe (pd.DataFrame): The DataFrame to save.
            database (str): The name of the MongoDB database.
            collection (str): The name of the MongoDB collection.

        Raises:
            ValueError: If the DataFrame is empty.
        """
        if dataframe.empty:
            raise ValueError("The provided DataFrame is empty and cannot be saved.")

        # Convert DataFrame to a list of dictionaries
        records = dataframe.to_dict(orient="records")

        # Access the specified database and collection
        db = self.client[database]
        coll = db[collection]

        # Insert records into the collection
        result = coll.insert_many(records)
        return result.inserted_ids

    def ping_db(self):
        """
        Ping the MongoDB server to test the connection.

        Returns:
            bool: True if the server is reachable, otherwise raises an exception.
        """
        try:
            self.client.admin.command("ping")
            return True
        except Exception as e:
            raise Exception(f"Failed to connect to the database: {e}")

    def query_timeseries(
        self,
        database: str,
        collection: str,
        start_time: str,
        end_time: str,
        filters: dict | None = None,
    ):
        """
        Query a MongoDB timeseries collection within a specific time range and optional filters.

        Args:
            database (str): The name of the MongoDB database.
            collection (str): The name of the MongoDB collection.
            start_time (str): The start of the time range in ISO 8601 format.
            end_time (str): The end of the time range in ISO 8601 format.
            filters (dict, optional): Additional filters for the query.

        Returns:
            list: A list of documents matching the query.
        """
        if filters is None:
            filters = {}

        # Convert string dates to datetime objects
        try:
            start_time_dt = datetime.fromisoformat(start_time)
            end_time_dt = datetime.fromisoformat(end_time)
        except ValueError as e:
            raise ValueError(f"Invalid date format: {e}")

        # Ensure the time range is included in the query
        time_filter = {"datetime": {"$gte": start_time_dt, "$lte": end_time_dt}}

        # Combine time filter with additional filters
        query = {**filters, **time_filter}

        # Access the specified database and collection
        db = self.client[database]
        coll = db[collection]

        # Perform the query
        results = coll.find(query)

        return list(results)
