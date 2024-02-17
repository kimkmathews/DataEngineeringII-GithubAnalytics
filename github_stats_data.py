from logging import getLogger, FileHandler, StreamHandler, info, error, warning, INFO, Formatter
from github_api_fetch.github_api_fetch_workflow import WorkflowFetchGitHubAPI
from github_api_fetch.github_api_fetch_commit import CommitFetchGitHubAPI
from github_api_fetch.github_api_fetch_repo import RepoFetchGitHubAPI
from datetime import datetime, timedelta
from db_manager import DatabaseManager
from time import sleep, time
from json import dump
from math import ceil


class GitHubStatsData:
    """
    Class for storing repository data fetched from the GitHub API for statistical analysis
        - ideally implemented by Consumer(s), but with each instance instantiated by the Producer
            - consumer can then run:
                - github_stats_data_instance.init_db()
                - github_stats_data_instance.fetch_repo_data()
        - stores repository data in the following dict format:
    repo_stats_dict =>
      {
        dateRange: [[start_date (str), end_date (str)]],
        totalRepoCount: int,
        repos: {
          repo_owner_name: {
            dateRange: [(start_date (str), end_date (str)],
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

    Author: Adam Ross
    Copyright: Only for the use of Group 17 for the course 1TD075 Data Engineering II held at Uppsala University in 2023
    """

    TODAY = datetime.now()  # constant for the default start date, which is used for the first or the only consumer
    MAX_DAYS_FETCHED = 365  # constant for the default and the maximum number of days fetched from the start date
    NUM_API_QUERY_ATTEMPTS = 3  # constant for the default number of failed API query attempts before the app quits
    MAX_DAILY_REPOS_FETCHED = 1000  # constant for the default maximum number of repo data fetches per date fetched
    DFLT_CREATED_BEFORE_DATE = '2022-05-30'  # default date all repositories data is fetched from must be created before
    RATE_LIMIT_PAUSE_TIME = 630  # rate limit exceeded event pause time in seconds (=10 minutes and 30 seconds)
    EXCLUDED_TOP_LANGUAGES = ['Text']  # GitHub's language names that should be excluded as they clearly aren't intended
    TDD_STR_MATCHES = ['TDD', 'BDD', 'TEST-DRIVEN', 'BEHAVIOR-DRIVEN', 'UNIT TEST', 'TEST-', 'CODE COV']  # TDD key-word
    DEVOPS_STR_MATCHES = ['DEVOPS', 'CI', 'CI/CD', 'CD', 'CONTINUOUS INTEGRATION', 'CONTINUOUS DE', 'ACTIONS', 'IAC',
                          'IAAS', 'PAAS', 'KUBERNETES', 'ANSIBLE', 'TERRAFORM', 'AUTOMA', '.YML']  # DevOps key-words
    COMPLETED_MSG = 'COMPLETE'  # message appended to data file to indicate that the data fetching from API is completed

    def __init__(self,
                 github_username,
                 github_token,
                 num_repos_fetched=None,
                 start_date=TODAY,
                 num_days_fetch=MAX_DAYS_FETCHED,
                 created_before_date=DFLT_CREATED_BEFORE_DATE,
                 instance_id=0):
        """
        Class initializer
        :param github_username: GitHub account username associated with the GitHub token string being used
        :param github_token: GitHub token string for reading public repository data in GitHub API fetches
        :param start_date: the initial datetime for GitHub API fetching - the current datetime by default
        :param num_days_fetch: the number of days in the date range from the start date of day-by-day GitHub API fetches
        :param num_repos_fetched: the number of repos being fetched per each GitHub GraphQL API query
        :param created_before_date: the date each repo data is fetched from must be created before (before date range)
        :param instance_id: instance ID for statistical analysis of fetched repo data from any number of consumers
        """
        # constant variables for GitHub GraphQL API fetching of repository data
        self.instance_id = instance_id
        self.start_date = start_date
        self.start_date_str = self.get_date_str(0)
        created_before_date = created_before_date if created_before_date else self.get_date_str(num_days_fetch)
        self.graph_gl_api_repo = RepoFetchGitHubAPI(repo_created_before_date=created_before_date,
                                                    github_username=github_username,
                                                    github_token=github_token)
        self.graph_gl_api_repo.num_data_fetched = num_repos_fetched if num_repos_fetched else \
            self.graph_gl_api_repo.num_data_fetched
        self.num_days_fetch = min(num_days_fetch, self.MAX_DAYS_FETCHED)  # lesser of the given num of days and default

        # init event logging and database management
        self.init_logging()
        self.db_manager = None

        # global variable for storing API fetched repo data for statistical analysis
        self.repo_stats_data = {'dateRange': [[self.start_date_str, None]],
                                'totalRepoCount': 0,
                                'repos': dict()}
        self.daily_fetch_times = list()
        self.timer = 0
        self.is_rate_limit_exceeded_paused = False

    def init_logging(self):
        """
        Initialize logging file and format for printing to console logged statements with timestamps
        """
        getLogger('').setLevel(INFO)
        formatter = Formatter(fmt='%(asctime)s: %(levelname)s: %(message)s',
                              datefmt='%m/%d/%Y %I:%M:%S %p')
        file_handler = FileHandler(filename='ID-' + str(self.instance_id) + '_' + self.start_date_str + '.log',
                                   encoding='utf-8',
                                   mode='w')
        file_handler.setLevel(INFO)
        file_handler.setFormatter(formatter)
        getLogger('').addHandler(file_handler)
        stream_handler = StreamHandler()
        stream_handler.setLevel(INFO)
        stream_handler.setFormatter(formatter)
        getLogger('').addHandler(stream_handler)

    def init_db(self, is_reset_collection=False, is_test=False):
        """
        Initializes the database connection for storing repo stats data fetched from the GitHub API
        :param is_reset_collection: boolean for if the DB collection is reset at instantiation; for new API fetching
        :param is_test: boolean for if the current usage of the cloud database is for testing purposes
        """
        self.db_manager = DatabaseManager(is_reset_collection=is_reset_collection, is_test=is_test)
        if self.db_manager.err:
            error(self.db_manager.err)
            exit(1)

    def get_date_str(self, num_days_before_today):
        """
        Get a date string in the correct format for GitHub GraphQL API queries
        :param num_days_before_today: number of days from the start date corresponding to the date being API fetched
        :return: date string for number of days from start date in the correct format for GitHub GraphQL API queries
        """
        return (self.start_date - timedelta(days=num_days_before_today)).strftime('%Y-%m-%d')

    def fetch_repo_commit_and_actions_data(self, repo_owner_name, date_str, end_date, is_workflow=False):
        """
        Get repo commit frequency and msg, and Actions workflow file data fetched from GitHub GraphQL for stats analysis
        :param repo_owner_name: the name of the GitHub repo and the owner of the repository's username: username/repo
        :param date_str: the date string for the current date the data is being fetched for
        :param end_date: the final date in the date range that the commit data is being fetched: date_str -> end_date
        :param is_workflow: boolean conditional to determine if the fetching is for commit data or Actions workflow data
        :return: array of either Actions workflow file data if is_workflow, or commit msgs otherwise, each in date range
        """
        result = []
        for _ in range(self.NUM_API_QUERY_ATTEMPTS):
            if not self.is_rate_limit_exceeded_paused:
                graph_ql_api = WorkflowFetchGitHubAPI() if is_workflow else CommitFetchGitHubAPI()
                result = graph_ql_api.query(repo_owner_name=repo_owner_name) if is_workflow \
                    else graph_ql_api.query(start_date=self.start_date_str,
                                            end_date=end_date,
                                            repo_owner_name=repo_owner_name)
                # exit iterative query attempts if successful API query
                if graph_ql_api.is_request_response_status_200_ok():
                    if self.is_rate_limit_exceeded():
                        self.pause_api_fetch(date_paused=date_str)
                        continue
                    break
        self.data_fetch_api_error_catch(date_shutdown=date_str)  # catch error
        return result

    def search_fetched_data_for_tdd_key_words(self, repo_owner_name, fetched_data, is_topic=False):
        """
        If not isTDD, checks TDD key-words are in the fetched_data array and if any is found, sets isTDD to True
        :param repo_owner_name: the name of the GitHub repo and the owner of the repository's username: username/repo
        :param fetched_data: array of data fetched from GitHub API, whether of commit msgs, topic or workflow files
        :param is_topic: boolean conditional to determine if the fetched data is topic data and requires specifying keys
        """
        if not self.repo_stats_data['repos'][repo_owner_name]['isTDD']:
            if any([any([t_str in (data_element['node']['topic']['name'] if is_topic else data_element).upper()
                         for t_str in self.TDD_STR_MATCHES]) for data_element in fetched_data]):
                self.repo_stats_data['repos'][repo_owner_name]['isTDD'] = True

    def search_fetched_data_for_devop_key_words(self, repo_owner_name, fetched_data, is_topic=False, is_workflow=False):
        """
        If isTDD and not isDevOps, checks DevOps key-words are in fetched_data array and if any, sets isDevOps to True
        :param repo_owner_name: the name of the GitHub repo and the owner of the repository's username: username/repo
        :param fetched_data: array of data fetched from GitHub API, whether of commit msgs, topic or workflow files
        :param is_topic: boolean conditional to determine if the fetched data is topic data and requires specifying keys
        :param is_workflow: boolean conditional to determine if the fetched data is workflow files and compares all data
        """
        if self.repo_stats_data['repos'][repo_owner_name]['isTDD'] \
                and not self.repo_stats_data['repos'][repo_owner_name]['isDevOps']:
            if any([any([t_str in (data_element['node']['topic']['name'] if is_topic else data_element).upper()
                         for t_str in (self.DEVOPS_STR_MATCHES[:-1] if is_workflow else self.DEVOPS_STR_MATCHES)])
                    for data_element in fetched_data]):
                self.repo_stats_data['repos'][repo_owner_name]['isDevOps'] = True

    def format_fetched_repo_data_for_stat_analysis(self, fetch_date_str):
        """
        Format meaningful repository data for each GitHub GraphQL and REST API query for later statistical analysis:
          {
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
        :param fetch_date_str: date string matching API fetching date so to validate if a commit is made on this day
        """
        repos_data_arr = self.graph_gl_api_repo.response.json()['data']['search']['edges']
        for repo_data in repos_data_arr:
            repo_data = repo_data['node']
            repo_owner_name = repo_data['nameWithOwner']

            if repo_owner_name not in self.repo_stats_data.keys() and \
                    repo_data['defaultBranchRef']['target']['committedDate'][:10] == fetch_date_str:
                # add repo commit frequency and msg, and actions data fetched from GitHub GraphQL for stats analysis
                end_date = self.get_date_str(num_days_before_today=self.num_days_fetch)
                date_range_commit_data = self.fetch_repo_commit_and_actions_data(repo_owner_name=repo_owner_name,
                                                                                 date_str=fetch_date_str,
                                                                                 end_date=end_date)
                actions_workflow_file_data = self.fetch_repo_commit_and_actions_data(repo_owner_name=repo_owner_name,
                                                                                     date_str=fetch_date_str,
                                                                                     end_date=end_date,
                                                                                     is_workflow=True)

                # set commit and date range data for the repository to the repo_stats_data for later stats analysis
                self.repo_stats_data['repos'][repo_owner_name] = \
                    {'dateRange': [(self.start_date_str, end_date)],
                     'totalCommitsInDateRange': len(date_range_commit_data),
                     'totalRepoCommits': int(repo_data['defaultBranchRef']['target']['history']['totalCount'])}

                # initialize the isTDD and isDevOps boolean flags, and languages dict also for later stats analysis
                self.repo_stats_data['repos'][repo_owner_name]['isTDD'] = False
                self.repo_stats_data['repos'][repo_owner_name]['isDevOps'] = False
                self.repo_stats_data['repos'][repo_owner_name]['languages'] = dict()

                # skip evaluating whether the repository follows TDD or DevOps if there are not any repository languages
                if len(repo_data['languages']['edges']) == 0:
                    continue

                # add repo language data fetched from GitHub GraphQL and REST API for statistical analysis
                for language in repo_data['languages']['edges']:
                    self.repo_stats_data['repos'][repo_owner_name]['languages'][language['node']['name']] = \
                        {'fileSize': int(language['size'])}

                # set isTDD boolean flags to True if there is any indication in repo topics
                self.search_fetched_data_for_tdd_key_words(repo_owner_name=repo_owner_name,
                                                           fetched_data=repo_data['repositoryTopics']['edges'],
                                                           is_topic=True)
                # if TDD key-words are not in the repository topics then search Actions workflow file names
                self.search_fetched_data_for_tdd_key_words(repo_owner_name=repo_owner_name,
                                                           fetched_data=actions_workflow_file_data)
                # if TDD key-words are also not in the repo workflow files then search commit messages within date range
                self.search_fetched_data_for_tdd_key_words(repo_owner_name=repo_owner_name,
                                                           fetched_data=date_range_commit_data)

                # if isTDD, set DevOps boolean flags to True if there is any indication in repo topics
                self.search_fetched_data_for_devop_key_words(repo_owner_name=repo_owner_name,
                                                             fetched_data=repo_data['repositoryTopics']['edges'],
                                                             is_topic=True)
                # if isTDD, but not DevOps key-words in repo topics then search Actions workflow file names
                self.search_fetched_data_for_devop_key_words(repo_owner_name=repo_owner_name,
                                                             fetched_data=actions_workflow_file_data,
                                                             is_workflow=True)
                # if isTDD, but not DevOps key-words in workflow files then search commit messages within date range
                self.search_fetched_data_for_devop_key_words(repo_owner_name=repo_owner_name,
                                                             fetched_data=date_range_commit_data)

    def save_repo_stats_data(self, date=None, msg=None):
        """
        Save formatted instance data from GitHub API fetching for statistical analysis with other instance data
        :param date: date of the most recent fetch of repo data from the API (when rate limit exceeded), or start date
        :param msg: message to declare if data is saved in the event of either an error or rate limit being exceeded
        """
        self.repo_stats_data['dateRange'][0][1] = date if date else self.get_date_str(self.num_days_fetch - 1)
        with open('files/repo_stats_data_' + str(self.instance_id) +
                  ('_' + msg if msg else '') + ('_' + date if date else '') + '.json', 'w') as json_file:
            dump(self.repo_stats_data, json_file)
        if msg == self.COMPLETED_MSG:
            info('Time taken to compute stats for %s days of repository data (s) = %s',
                 str(self.num_days_fetch),
                 str(sum(self.daily_fetch_times)))
            if self.db_manager:
                self.db_manager.insert_collection_doc(self.repo_stats_data)  # insert repo stats data as new doc to DB
                if self.db_manager.err:
                    error(self.db_manager.err)
                else:
                    info('Updated database with formatted stats data ' + str(self.db_manager.res.inserted_id) +
                         ' fetched from GitHub API')

    def is_rate_limit_exceeded(self):
        """
        Boolean conditional function to determine if the rate limit for fetching data from the API has been exceeded
        :return: true if the rate limit is exceeded, false otherwise
        """
        res_data = self.graph_gl_api_repo.response.json()
        return 'errors' in res_data and 'RATE_LIMITED' in [err['type'] for err in res_data['errors']]

    def pause_api_fetch(self, date_paused):
        """
        Pause API fetching for RATE_LIMIT_PAUSE_TIME due to exceeding rate limit. Save the current API fetching data
        :param date_paused: the date the data fetching and formatting is at when the API fetching rate limit is exceeded
        """
        self.save_repo_stats_data(date=date_paused, msg='RATE_LIMIT')
        warning('Rate Limit Exceed! -- pausing for %s seconds!', str(self.RATE_LIMIT_PAUSE_TIME))
        self.is_rate_limit_exceeded_paused = True
        sleep(self.RATE_LIMIT_PAUSE_TIME)
        self.is_rate_limit_exceeded_paused = False

    def data_fetch_api_error_catch(self, date_shutdown):
        """
        Catches if there is an error returned from the GitHub API, saves the formatted data to-date and shuts down
        :param date_shutdown: the date the data fetching and formatting is at when the API returns an error response
        """
        if not self.graph_gl_api_repo.is_request_response_status_200_ok():
            self.save_repo_stats_data(date=date_shutdown, msg='ERROR')
            error('%s', str(self.graph_gl_api_repo.response.status_code))
            exit(0)

    def print_api_fetch_status(self, date_str):
        """
        Print current status of fetching from API date range progress and the time taken and estimation of completion
        :param date_str: the date the data fetching from API and formatting for statistical analysis has just completed
        """
        self.daily_fetch_times.append(time() - self.timer)
        info('... Fetched: %s -- %s/%s days in %s s', date_str, str(len(self.daily_fetch_times)),
             str(self.num_days_fetch), str(sum(self.daily_fetch_times)))
        info('Estimated time remaining: %s s', str(sum(self.daily_fetch_times) / len(self.daily_fetch_times) *
                                                   (self.num_days_fetch - len(self.daily_fetch_times))))

    def init_api_fetching_for_date(self, date_iter):
        """
        Initializes API fetching for a given date with timer start, formatting of date into a string and status report
        :param date_iter: integer representing the number of days from the start date
        :return: date string for the date date_iter number of days from the start date
        """
        self.timer = time()
        date_str = self.get_date_str(date_iter)  # convert the iter into a date string
        info('Fetching repository and commit data from GitHub API for date: %s...', date_str)
        return date_str

    def fetch_repo_data(self):
        """
        Iteratively fetch repository data from GitHub GraphQL API using pagination for each date in the given date range
        """
        # iterate each day in the calendar range of dates
        for day in range(self.num_days_fetch):
            date_str = self.init_api_fetching_for_date(date_iter=day)

            # iteratively query API until response is returned with OK status code or NUM_API_QUERY_ATTEMPTS fail codes
            for _ in range(self.NUM_API_QUERY_ATTEMPTS):
                self.graph_gl_api_repo.query(date=date_str)  # query GitHub GraphQL API

                # store statistics and exit iterative query attempts if successful API query
                if self.graph_gl_api_repo.is_request_response_status_200_ok():
                    if self.is_rate_limit_exceeded():
                        self.pause_api_fetch(date_paused=date_str)
                        continue
                    self.format_fetched_repo_data_for_stat_analysis(fetch_date_str=date_str)
                    self.repo_stats_data['totalRepoCount'] += \
                        self.graph_gl_api_repo.response.json()['data']['search']['repositoryCount']
                    break
            self.data_fetch_api_error_catch(date_shutdown=date_str)  # catch error if query attempts are not successful

            # for each date, iterate until data for 1000 repositories has been fetched using pagination
            for _ in range(int(ceil(self.MAX_DAILY_REPOS_FETCHED / self.graph_gl_api_repo.num_data_fetched))):
                # if there is no next page for continuing pagination, exit iterative pagination
                if not self.graph_gl_api_repo.page_info or not self.graph_gl_api_repo.page_info['hasNextPage']:
                    break

                # query GitHub GraphQL API using pagination and store statistics
                self.graph_gl_api_repo.query(date=date_str, pagination=self.graph_gl_api_repo.page_info['endCursor'])
                self.data_fetch_api_error_catch(date_shutdown=date_str)  # catch error if query attempt not successful
                if self.is_rate_limit_exceeded():
                    self.pause_api_fetch(date_paused=date_str)
                self.format_fetched_repo_data_for_stat_analysis(fetch_date_str=date_str)

            self.print_api_fetch_status(date_str=date_str)  # print current status of API fetching progress and time
        self.save_repo_stats_data(msg=self.COMPLETED_MSG)
        self.db_manager.close_conn()
