import os
from src import constants
from src.import_mongo import ImportMongo
from src.mongo_to_es import MongoElasticSearch
import time

if __name__ == '__main__':
    t1 = time.time()

    directory = constants.ConstantVariables.SOURCE_DIRECTORY.value

    for file in os.listdir(directory):
        if file.endswith(".csv"):
            csv_file_path = directory + file
            import_mongo = ImportMongo(csv_file=csv_file_path,
                                       mongo_url=constants.ConstantVariables.MONGO_PATH.value,
                                       db_name=constants.ConstantVariables.DB_NAME.value,
                                       collection_name=constants.ConstantVariables.COLLECTION_NAME.value
                                       )
            import_mongo.insert()
            print("File {0} is loaded in MongoDB".format(file))

    mongo_elastic_search = MongoElasticSearch(mongo_url=constants.ConstantVariables.MONGO_PATH.value,
                                              db_name=constants.ConstantVariables.DB_NAME.value,
                                              collection_name=constants.ConstantVariables.COLLECTION_NAME.value,
                                              elastic_search_url=constants.ConstantVariables.ES_PATH.value,
                                              elastic_search_index=constants.ConstantVariables.ES_INDEX.value,
                                              elastic_search_type=constants.ConstantVariables.ES_TYPE.value)
    mongo_elastic_search.mongo_to_elastic_search()

    print("Time taken from loading data into mongoDB to migrating the data into ElasticSearch and indexing is {0} "
          "seconds.".format(time.time()-t1))

