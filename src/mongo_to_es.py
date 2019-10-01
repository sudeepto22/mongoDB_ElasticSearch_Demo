import time
from collections import deque
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
from pymongo import MongoClient
from tqdm import tqdm
from src.string_utility import StrToCamelCase


class MongoElasticSearch:
    def __init__(self, mongo_url="", db_name="", collection_name="", elastic_search_url="",
                 elastic_search_index="", elastic_search_type=""):
        self.mongo_url = mongo_url
        self.db_name = db_name
        self.collection_name = collection_name
        self.elastic_search_url = elastic_search_url
        self.elastic_search_index = elastic_search_index
        self.elastic_search_type = elastic_search_type
        self.my_collection = None
        self.elastic_search_client = None

    def connect_mongo(self):
        """
        Connect MongoDB with the connection details provided

        :return: None
        """
        # Connect to mongoDB with connection URL
        my_client = MongoClient(self.mongo_url)

        # Load mongoDB database to my_database variable
        my_database = my_client[self.db_name]

        # Load mongoDB collection of my_database to my_collection instance variable
        self.my_collection = my_database[self.collection_name]

    def connect_elastic_search(self):
        """
        Connect ElasticSearch with the connection details provided

        :return: None
        """
        # Connect to ElasticSearch and pass it to the instance variable elastic_search_client
        self.elastic_search_client = Elasticsearch(self.elastic_search_url)

    def mongo_to_elastic_search(self):
        """
        Migrate MongoDB to Elastic Search and create index

        :return: None
        """
        # Connect to mongoDB
        self.connect_mongo()

        # Connect to ElasticSearch
        self.connect_elastic_search()

        # Load data from mongoDB to ElasticSearch in bulk of size = 100
        actions = []
        for data in tqdm(self.my_collection.find(), total=self.my_collection.count()):
            data.pop('_id')

            # Convert the column or field names as camelCase
            new_data = dict()
            for key in data.keys():
                new_data[StrToCamelCase(key).change_string()] = data[key]

            # Delete the old list
            del data

            action = {
                "_index": self.elastic_search_index,
                "_type": self.elastic_search_type,
                "_source": new_data
            }
            actions.append(action)

            try:
                # Dump x number of objects at a time
                if len(actions) >= 100:
                    deque(parallel_bulk(self.elastic_search_client, actions), maxlen=0)
                    actions = []
                time.sleep(.01)
            except Exception as e:
                print(e)

        # Dump the remaining objects in list which is equal to x%100 where x is the total number of documents to be
        # loaded in elastic search and 100 is the bulk size
        try:
            deque(parallel_bulk(self.elastic_search_client, actions), maxlen=0)
        except Exception as e:
            print(e)

