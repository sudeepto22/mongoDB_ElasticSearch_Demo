import pymongo
import pandas as pd
from src.string_utility import StringManipulate


class ImportMongo:
    def __init__(self, csv_file="", mongo_url="", db_name="", collection_name=""):
        self.csv_file = csv_file
        self.db_name = db_name
        self.collection_name = collection_name
        self.mongo_url = mongo_url
        self.my_collection = None

    def convert_csv_to_dict(self) -> dict:
        """
        Convert the csv to pandas dataframe and then convert to record wise dictionary

        :return: news_list_dict -> dict
        """
        # Read the csv in pandas DataFrame
        df = pd.read_csv(self.csv_file, encoding="latin1")

        # Convert the pandas DataFrame to dictionary
        news_list = df.to_dict(orient='records')

        # Create a new list of dictionaries with changed column names
        news_dict = dict()
        news_dict_list = list()

        for i in news_list:
            for k, v in i.items():
                # key or column names are manipulated
                news_dict[StringManipulate(k).change_string()] = v
            news_dict_list.append(news_dict.copy())

        # Delete the old list
        del news_list

        return news_dict_list

    def connect_mongo(self) -> None:
        """
        Connect MongoDB with the connection details provided

        :return: None
        """
        # Connect to mongoDB with connection URL
        my_client = pymongo.MongoClient(self.mongo_url)

        # Load mongoDB database to my_database variable
        my_database = my_client[self.db_name]

        # Load mongoDB collection of my_database to my_collection instance variable
        self.my_collection = my_database[self.collection_name]

    def insert(self) -> None:
        """
        Inserts data into MongoDB

        :return: None
        """
        # Get the list of source from convert_csv_to_dict() instance function
        news_list = self.convert_csv_to_dict()

        # Connect to MongoDB with the connect_mongo() instance function
        self.connect_mongo()

        # Load documents one by one in mongoDB collection
        for document in news_list:
            self.my_collection.insert_one(document)

