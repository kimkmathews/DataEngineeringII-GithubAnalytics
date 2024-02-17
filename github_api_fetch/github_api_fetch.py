from abc import ABC, abstractmethod
from http import HTTPStatus
from requests import auth
from os import getenv


class FetchGitHubAPI(ABC):
    """
    Superclass for fetching data from the GitHub GraphQL API
        - ideally implemented by Consumer(s) (via the GitHubStatsData class)

    Author: Adam Ross
    Copyright: Only for the use of Group 17 for the course 1TD075 Data Engineering II held at Uppsala University in 2023
    """

    DFLT_GITHUB_USERNAME = getenv("GH_USERNAME")  # default GitHub username that is set as an environment variable
    DFLT_GITHUB_TOKEN = getenv("GH_TOKEN")  # default GitHub token that is set as an environment variable
    DFLT_NUM_DATA_FETCHED = 50  # default num data fetched per API query - can return more, but this can time out
    GITHUB_GRAPHQL_API_POST_REQ_URL = 'https://api.github.com/graphql'  # URL for GitHub GraphQL API queries

    def __init__(self,
                 github_username=DFLT_GITHUB_USERNAME,
                 github_token=DFLT_GITHUB_TOKEN,
                 num_data_fetched=DFLT_NUM_DATA_FETCHED):
        """
        Class initializer
        :param github_username: GitHub account username associated with the GitHub token string being used
        :param github_token: GitHub token string for reading public repository data in GitHub API fetches
        """
        # constant variables for GitHub GraphQL API fetching
        self.github_authorisation = auth.HTTPBasicAuth(github_username, github_token)  # authorisation for API requests
        self.num_data_fetched = num_data_fetched

        # global variables for GitHub GraphQL API fetch response data
        self.response = dict()  # all query response data, including status code and JSON with data for stat analysis
        self.page_info = list()  # query response data specifically for pagination

    def is_request_response_status_200_ok(self):
        """
        Boolean conditional function to determine if a request response status is OK and data processing can continue
        :return: True if the request response status is OK (HTTP status code: 200), otherwise false for any other status
        """
        return not isinstance(self.response, dict) and self.response.status_code == HTTPStatus.OK

    @abstractmethod
    def query(self, *args, **kwargs):
        pass
