import certifi
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import certifi


class AtlasClient():
    mongodb_client = None
    database = None

    def __init__(self, altas_uri, dbname):
        self.mongodb_client = MongoClient(altas_uri, server_api=ServerApi('1'), tlsCAFile=certifi.where())
        self.database = self.mongodb_client[dbname]

    def ping(self):
        self.mongodb_client.admin.command('ping')

    def get_collection(self, collection_name):
        collection = self.database[collection_name]
        return collection

    def find(self, collection_name, filter={}, limit=10):
        collection = self.database[collection_name]
        items = list(collection.find(filter=filter, limit=limit))
        return items

    # https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-stage/
    def vector_search(self, collection_name, index_name, attr_name, embedding_vector, limit=5):
        collection = self.database[collection_name]
        results = collection.aggregate([
            {
                '$vectorSearch': {
                    "index": index_name,
                    "path": attr_name,
                    "queryVector": embedding_vector,
                    "numCandidates": 50,
                    "limit": limit,
                }
            },
            {
                "$project": {
                    '_id': 1,
                    'title': 1,
                    'plot': 1,
                    'year': 1,
                    "search_score": {"$meta": "vectorSearchScore"}
                }
            }
        ])
        return list(results)

    def close_connection(self):
        self.mongodb_client.close()
