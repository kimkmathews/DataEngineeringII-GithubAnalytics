from pymongo.errors import ConfigurationError, OperationFailure
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from os import getenv
from json import dump


class DatabaseManager:
    """
    Class for database management, whether inserting or retrieving collection documents, or saving the DB data to .json

    Author: Adam Ross
    Copyright: Only for the use of Group 17 for the course 1TD075 Data Engineering II held at Uppsala University in 2023
    """

    DB_URI = "mongodb+srv://{}:{}@{}.mongodb.net/?retryWrites=true&w=majority".format(getenv('DB_USER'),
                                                                                      getenv('DB_PW'),
                                                                                      getenv('DB_SOCKET_PATH'))
    DB_COLLECTION_NAME = 'consumers'
    DB_TEST_COLLECTION_NAME = 'tst_consumers'
    VALID_DATA_FILE_MSG = 'DB_COMPLETED'  # msg appended to data file to indicate the data fetched from API is valid
    DFLT_PORT = 27017  # the default port number used by the pymongo.mongo_client.MongoClient() class

    def __init__(self, is_reset_collection=False, is_test=False):
        """
        Class initializer
        :param is_reset_collection: boolean for if the DB collection is reset at instantiation; for new API fetching
        :param is_test: boolean for if the current usage of the cloud database is for testing purposes
        """
        self.client = None
        self.err = None
        self.res = None

        try:
            self.client = MongoClient(host=self.DB_URI,
                                      port=self.DFLT_PORT,
                                      server_api=ServerApi('1'))
        except ConfigurationError as exp:
            self.err = exp
        else:
            self.db = self.client.GitRepoStatsDB
            self.collection = self.db[self.DB_TEST_COLLECTION_NAME] if is_test else self.db[self.DB_COLLECTION_NAME]

            if is_reset_collection:
                try:
                    self.collection.drop()
                except OperationFailure as exp:
                    self.err = exp

    def insert_collection_doc(self, doc):
        """
        Insert a repo_stats_dict as a new document to the database collection
        :param doc: the repo_stats_dict in the following format:
            repo_stats_dict =>
              {
                dateRange: [[start_date (str), end_date (str)]],
                totalRepoCount: int,
                repos: {
                  repo_owner_name: {
                    dateRange: [(start_date (str), end_date (str))],
                    totalCommitsInDateRange: int,
                    totalRepoCommits: int,
                    isTDD: Boolean,
                    isDevOps: Boolean,
                    languages: {
                      language_name: {
                        fileSize: int
                      }
                    }
                  }
                }
              }
        """
        if self.client:
            self.err = None
            self.res = None

            try:
                self.res = self.collection.insert_one(doc)
            except OperationFailure as exp:
                self.err = exp

    def retrieve_collection(self, path='files/'):
        """
        Retrieve all documents in a collection and merge them when valid to a .json file for statistical analysis, such:
        repo_stats_dict .json format =>
          {
            dateRange: [[start_date (str), end_date (str)]],
            totalRepoCount: int,
            repos: {
              repo_owner_name: {
                dateRange: [(start_date (str), end_date (str))],
                totalCommitsInDateRange: int,
                totalRepoCommits: int,
                isTDD: Boolean,
                isDevOps: Boolean,
                languages: {
                  language_name: {
                    fileSize: int
                  }
                }
              }
            }
          }
        :param path: file path
        """
        if self.client:
            collection_docs = self.collection.find()

            if collection_docs:
                repo_stats_data = dict()

                for doc in collection_docs:
                    if doc:
                        repo_stats_data = self.merge_formatted_stats_data(repo_stats_data, doc)

                if len(repo_stats_data) > 0:
                    with open(path + 'repo_stats_data_' + str([max(dr[0] for dr in repo_stats_data['dateRange']),
                                                               min(dr[1] for dr in repo_stats_data['dateRange'])])
                              + '_' + self.VALID_DATA_FILE_MSG + '.json', 'w') as json_file:
                        dump({'dateRange': repo_stats_data['dateRange'],
                              'totalRepoCount': repo_stats_data['totalRepoCount'],
                              'repos': repo_stats_data['repos']},
                             json_file)
            else:
                print('Error: Database collection is empty!')

    @staticmethod
    def is_date_range_valid(existing_dates_arr, new_dates_arr):
        """
        Boolean conditional function to validate the date range from one GitHub API scraping dataset does not
         fall within the existing dataset date range so it can be merged without statistical inaccuracies.
        :param existing_dates_arr: the date range already in the existing dataset used for statistical analysis
        :param new_dates_arr: the new dataset date range which is compared with the existing date range
        :return: True if the new date range does not fall within the existing date range, otherwise False
        """
        return new_dates_arr[1] and not any([new_dates_arr[1] <= date_range[0] <= new_dates_arr[0] or
                                             new_dates_arr[1] <= date_range[1] <= new_dates_arr[0]
                                             for date_range in existing_dates_arr])

    @staticmethod
    def merge_formatted_stats_data(existing_data, new_data):
        """
        Merge formatted data fetched from the GitHub API for statistical analysis
        :param existing_data: the date range from the repo_stats_data dict already in use
        :param new_data: the date range from the new repo stats data dict being merged with the existing data dict
        :return: the repo_stats_data dict whether merged or not merged with new data, or initialized with the new data
        """
        if not existing_data or len(existing_data) == 0:
            return new_data
        elif all([DatabaseManager.is_date_range_valid(existing_data['dateRange'], date_range)
                  for date_range in new_data['dateRange']]):
            existing_data['totalRepoCount'] += new_data['totalRepoCount']  # increment population num
            existing_data['dateRange'] += existing_data['dateRange']

            # iteratively add new or update existing repo data for statistical analysis if repo date range valid
            for repo_name in new_data['repos']:
                # add new repo to stored repo file dict, including all data, iff meets following conditions
                if repo_name not in existing_data['repos'].keys():
                    existing_data['repos'][repo_name] = new_data['repos'][repo_name]
                else:
                    if all([DatabaseManager.is_date_range_valid(existing_data['repos'][repo_name]['dateRange'],
                                                                date_range)
                            for date_range in new_data['repos'][repo_name]['dateRange']]):
                        # otherwise, update repo commit count in date range, and isTDD and isDevOps conditionals
                        existing_data['repos'][repo_name]['dateRange'] += new_data['repos'][repo_name]['dateRange']
                        existing_data['repos'][repo_name]['totalCommitsInDateRange'] += \
                            new_data['repos'][repo_name]['totalCommitsInDateRange']
                        existing_data['repos'][repo_name]['isTDD'] = \
                            existing_data['repos'][repo_name]['isTDD'] or \
                            new_data['repos'][repo_name]['isTDD']
                        existing_data['repos'][repo_name]['isDevOps'] = \
                            existing_data['repos'][repo_name]['isDevOps'] or \
                            new_data['repos'][repo_name]['isDevOps']

                        # if repo version is more current, replace total repo commit and language file size data
                        if max(new_data['repos'][repo_name]['dateRange'][0]) > \
                                max([max(date_range) for date_range
                                     in existing_data['repos'][repo_name]['dateRange']]):
                            existing_data['repos'][repo_name]['totalRepoCommits'] = \
                                new_data['repos'][repo_name]['totalRepoCommits']
                            for language in new_data['repos'][repo_name]['languages']:
                                existing_data['repos'][repo_name]['languages'][language] = \
                                    new_data['repos'][repo_name]['languages'][language]
            return existing_data

    def close_conn(self):
        """
        Shut down database client connection
        """
        self.client.close()


def deployment_retrieve_db_data():
    """
    Function for retrieving all the DB data and saving it to .json file for git hook pushing to production
    """
    deployment_db_app = DatabaseManager()
    deployment_db_app.retrieve_collection()
    deployment_db_app.close_conn()


if __name__ == '__main__':
    deployment_retrieve_db_data()  # intended for only the deployment VM to execute
