from github_api_fetch.github_api_fetch import FetchGitHubAPI
from requests import post


class RepoFetchGitHubAPI(FetchGitHubAPI):
    """
    Class for fetching repository data from the GitHub GraphQL API
        - ideally implemented by Consumer(s) (via the GitHubStatsData class)

    Author: Adam Ross
    Copyright: Only for the use of Group 17 for the course 1TD075 Data Engineering II held at Uppsala University in 2023
    """

    MIN_REPO_SIZE = 30000  # min size repos returned by API search for research relevance without bias (1000 = 1KB)
    # GraphQL query for repos with commits at date, excluding private, archived, mirror, fork, empty and star-less repos
    GITHUB_GRAPHQL_QUERY_STR_DFLT = " is:public fork:false mirror:false archived:false size:>" \
                                    + str(MIN_REPO_SIZE) + " stars:>1 sort:committedDate-asc"
    # the string used for each query to the GitHub GraphQL API for fetching any repo data having pushed on a given date
    REPO_FETCH_GRAPHQL_STR = """
      query ($query_str: String!, $num_repos_fetched: Int!, $pagination: String) {
        search(query: $query_str, type: REPOSITORY, first: $num_repos_fetched, after: $pagination) {
          repositoryCount
          pageInfo {
            hasNextPage
            endCursor
          }
          edges {
            node {
              ... on Repository {
                nameWithOwner
                languages(first: 10, orderBy: { 
                  field: SIZE,
                  direction: DESC
                }) {
                  edges {
                    size
                    node {
                      name
                    }
                  }
                }
                repositoryTopics(first: 20) {
                  edges {
                    node {
                      topic {
                        name
                      }
                    }
                  }
                }
                defaultBranchRef {
                  target {
                    ... on Commit {
                      committedDate
                      history(first: 0) {
                        totalCount
                      }
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
                 repo_created_before_date,
                 github_username=None,
                 github_token=None,
                 num_repos_fetched=None):
        """
        Class initializer
        :param repo_created_before_date: the date each repo data is fetched from must be created before (the date range)
        :param github_username: GitHub account username associated with the GitHub token string being used
        :param github_token: GitHub token string for reading public repository data in GitHub API fetches
        :param num_repos_fetched: the number of repos being fetched per each GitHub GraphQL API query
        """
        super().__init__(github_username, github_token, num_repos_fetched)

        # constant variables for GitHub GraphQL API fetching
        self.query_str = " created:<" + repo_created_before_date + self.GITHUB_GRAPHQL_QUERY_STR_DFLT  # query string

    def query(self, date, pagination=None):
        """
        Post query to GitHub GraphQL API for given date with optional pagination, set response for statistical analysis
        :param date: the date the repository data is being fetched from the GitHub GraphQL API for
        :param pagination: pagination string for the continued fetching to start from or default null if not pagination
        """
        variables = {'query_str': "pushed:" + date + self.query_str,
                     'num_repos_fetched': self.num_data_fetched,
                     'pagination': pagination}
        self.response = post(self.GITHUB_GRAPHQL_API_POST_REQ_URL,
                             json={'query': self.REPO_FETCH_GRAPHQL_STR, 'variables': variables},
                             auth=self.github_authorisation)
        if self.is_request_response_status_200_ok() and 'errors' not in self.response.json():
            self.page_info = self.response.json()['data']['search']['pageInfo']  # set request response pagination data
