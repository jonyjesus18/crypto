from pymongo.mongo_client import MongoClient
from pymongo import InsertOne
from pymongo.server_api import ServerApi
from utils.py_utils import StrValueEnum
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
        if dataframe.empty:
            raise ValueError("The provided DataFrame is empty and cannot be saved.")

        # Convert DataFrame to a list of dictionaries
        records = dataframe.to_dict(orient="records")

        # Create bulk operations
        operations = [InsertOne(record) for record in records]  # type: ignore

        # Access the specified database and collection
        db = self.client[database]
        coll = db[collection]

        # Execute bulk write
        result = coll.bulk_write(operations)
        return result.inserted_count

    def rename_key_in_collection(
        self, db_name: str, collection_name: str, old_key: str, new_key: str, uri: str
    ):
        """
        Rename a key in all documents of a MongoDB collection.

        Args:
            db_name (str): Name of the database.
            collection_name (str): Name of the collection.
            old_key (str): The key to rename.
            new_key (str): The new name for the key.
            uri (str): MongoDB connection string URI.

        Returns:
            dict: A summary of the operation.
        """
        # Connect to MongoDB
        client = MongoClient(uri)
        db = client[db_name]
        collection = db[collection_name]

        # Perform the rename operation
        result = collection.update_many({}, {"$rename": {old_key: new_key}})

        # Return the result summary
        return {
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
        }

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
        end_time: str | None = None,  # Make end_time optional
        filters: dict | None = None,
    ):
        """
        Query a MongoDB timeseries collection within a specific time range and optional filters.

        Args:
            database (str): The name of the MongoDB database.
            collection (str): The name of the MongoDB collection.
            start_time (str): The start of the time range in ISO 8601 format.
            end_time (str, None): The end of the time range in ISO 8601 format (optional).
            filters (dict, None): Additional filters for the query.

        Returns:
            list: A list of documents matching the query.
        """
        if filters is None:
            filters = {}

        # Convert string dates to datetime objects
        try:
            start_time_dt = datetime.fromisoformat(start_time)
        except ValueError as e:
            raise ValueError(f"Invalid start_time format: {e}")

        # If end_time is provided, convert it to datetime object, otherwise use the current time
        if end_time:
            try:
                end_time_dt = datetime.fromisoformat(end_time)
            except ValueError as e:
                raise ValueError(f"Invalid end_time format: {e}")
        else:
            # If no end_time, use the current time
            end_time_dt = datetime.utcnow()

        # Convert the datetime objects to strings in the same format as the records in MongoDB
        start_time_str = start_time_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        end_time_str = end_time_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

        # Ensure the time range is included in the query (as strings)
        # time_filter = {"datetime": {"$gte": start_time_str, "$lte": end_time_str}}
        time_filter = {}
        if start_time_str:
            time_filter["datetime"] = {"$gte": start_time_str}
        if end_time_str:
            if "datetime" in time_filter:
                time_filter["datetime"]["$lte"] = end_time_str
            else:
                time_filter["datetime"] = {"$lte": end_time_str}

        # Combine time filter with additional filters
        query = {**filters, **time_filter}

        # Access the specified database and collection
        db = self.client[database]
        coll = db[collection]

        # Perform the query
        results = coll.find(query)

        return list(results)
