from enum import Enum


class ConstantVariables(Enum):
    # Source Files
    SOURCE_DIRECTORY = "../datasets/regional-newspaper-visualisation-metadata/"

    # MongoDB
    MONGO_PATH = "mongodb://localhost:27017/"
    DB_NAME = "master_newspaper"
    COLLECTION_NAME = "newspaper"

    # ElasticSearch
    ES_PATH = "http://localhost:9200/"
    ES_INDEX = "newspaper"
    ES_TYPE = "type_newspaper"

