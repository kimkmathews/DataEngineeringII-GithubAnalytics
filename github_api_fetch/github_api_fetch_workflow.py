from github_api_fetch.github_api_fetch import FetchGitHubAPI
from requests import post


class WorkflowFetchGitHubAPI(FetchGitHubAPI):
    """
    Class for fetching workflow file data from the GitHub GraphQL API
        - ideally implemented by Consumer(s) (via the GitHubStatsData class)

    Author: Adam Ross
    Copyright: Only for the use of Group 17 for the course 1TD075 Data Engineering II held at Uppsala University in 2023
    """

    # the string used for each query to the GitHub GraphQL API for fetching repo Actions workflow file names
    ACTION_WORKFLOW_FILE_NAMES_FETCH_GRAPHQL_STR = """
      query ($owner_str: String!, $repo_str: String!) {
        repository(owner: $owner_str, name: $repo_str) {
          object(expression: "HEAD:.github/workflows") {
            ... on Tree {
              entries {
                name
              }
            }
          }
        }
      }
    """

    def __init__(self,
                 github_username=None,
                 github_token=None,
                 num_workflow_files_fetched=None):
        """
        Class initializer
        :param github_username: GitHub account username associated with the GitHub token string being used
        :param github_token: GitHub token string for reading public repository data in GitHub API fetches
        :param num_workflow_files_fetched: the number of workflow files being fetched per each GitHub GraphQL API query
        """
        super().__init__(github_username, github_token, num_workflow_files_fetched)

    def query(self, repo_owner_name):
        """
        Fetch GitHub Actions workflow entries data for retrieving name of files used for GitHub CI/CD Devops and testing
        :param repo_owner_name: name of repository data is fetched from the API for: owner-username/repository-name
        :return: array with name strings for each file used in the given repo Actions workflows directory
        """
        repo_owner, repo_name = repo_owner_name.split('/')
        variables = {'owner_str': repo_owner,
                     'repo_str': repo_name}
        self.response = post(self.GITHUB_GRAPHQL_API_POST_REQ_URL,
                             json={'query': self.ACTION_WORKFLOW_FILE_NAMES_FETCH_GRAPHQL_STR,
                                   'variables': variables},
                             auth=self.github_authorisation)
        if self.is_request_response_status_200_ok() and 'errors' not in self.response.json() \
                and self.response.json()['data']['repository']['object']:
            actions_workflow_entries = self.response.json()['data']['repository']['object']['entries']
            return [file['name'] for file in actions_workflow_entries]
        else:
            return []
