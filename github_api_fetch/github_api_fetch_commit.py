from github_api_fetch.github_api_fetch import FetchGitHubAPI
from requests import post


class CommitFetchGitHubAPI(FetchGitHubAPI):
    """
    Class for fetching commit data from the GitHub GraphQL API
        - ideally implemented by Consumer(s) (via the GitHubStatsData class)

    Author: Adam Ross
    Copyright: Only for the use of Group 17 for the course 1TD075 Data Engineering II held at Uppsala University in 2023
    """

    # the string used for each query to the GitHub GraphQL API for fetching repo commit data within a given date range
    COMMIT_FETCH_GRAPHQL_STR = """
      query ($owner_str: String!, $repo_str: String!, $num_repos_fetched: Int!, 
      $start_date: GitTimestamp!, $end_date: GitTimestamp!, $pagination: String) {
        repository(owner: $owner_str, name: $repo_str) {
          defaultBranchRef  {
            target {
              ... on Commit {
                history(first: $num_repos_fetched, since: $start_date, until: $end_date, after: $pagination) {
                  pageInfo {
                    hasNextPage
                    endCursor
                  }
                  edges {
                    node {
                      message
                    }
                  }
                }
              }
            }
          }
        }
      }
    """

    def __init__(self,
                 github_username=None,
                 github_token=None,
                 num_commits_fetched=None):
        """
        Class initializer
        :param github_username: GitHub account username associated with the GitHub token string being used
        :param github_token: GitHub token string for reading public repository data in GitHub API fetches
        :param num_commits_fetched: the number of commits being fetched per each GitHub GraphQL API query
        """
        super().__init__(github_username, github_token, num_commits_fetched)

    def query(self, start_date, end_date, repo_owner_name, pagination=None):
        """
        Recursively fetch repo commit data for a given date range in reverse for update frequency, TDD and DevOps stats.
        Also, now fetches GitHub Actions workflow entries data for retrieving name of files used for GitHub CI/CD Devops
        :param start_date: the start/current date of the repo data fetching from the GitHub API for the given date range
        :param end_date: the final date of the repo fetching for the given date range (is in reverse calendar order)
        :param repo_owner_name: name of repository data is fetched from the API for: owner-username/repository-name
        :param pagination: pagination string for the continued fetching to start from or default None if not pagination
        :return: array of tuples with file types and messages for each repo commit in date range: [([file(s)], message)]
        """
        repo_owner, repo_name = repo_owner_name.split('/')
        variables = {'owner_str': repo_owner,
                     'repo_str': repo_name,
                     'num_repos_fetched': self.num_data_fetched,
                     'start_date': end_date + 'T00:00:00',
                     'end_date': start_date + 'T23:59:59',
                     'pagination': pagination}
        self.response = post(self.GITHUB_GRAPHQL_API_POST_REQ_URL,
                             json={'query': self.COMMIT_FETCH_GRAPHQL_STR, 'variables': variables},
                             auth=self.github_authorisation)
        if self.is_request_response_status_200_ok() and 'errors' not in self.response.json():
            commit_data = self.response.json()['data']['repository']['defaultBranchRef']['target']['history']
            return [nodes['node']['message'] for nodes in commit_data['edges']] + \
                (self.query(start_date=start_date,
                            end_date=end_date,
                            repo_owner_name=repo_owner_name,
                            pagination=commit_data['pageInfo']['endCursor'])
                 if commit_data['pageInfo']['hasNextPage'] else [])
        else:
            return []
