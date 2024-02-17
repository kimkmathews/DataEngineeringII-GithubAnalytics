from db_manager import DatabaseManager
from glob import iglob
from json import load


class GitHubStatsAnalysis:
    """
    Class for computing statistical analysis of processed repository data fetched from the GitHub API in the format:
    repo_stats_dict =>
      {
        dateRange: [[start_date (str), end_date (str)]]
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

    Author: Adam Ross
    Copyright: Only for the use of Group 17 for the course 1TD075 Data Engineering II held at Uppsala University in 2023
    """

    DFLT_NUM_TOP_STATS = 10  # default number of top statistics to return
    VALID_DATA_FILE_MSG = 'DB_COMPLETED'  # msg appended to data file to indicate the data fetched from API is valid

    def __init__(self, num_top_statistics=DFLT_NUM_TOP_STATS):
        """
        Class initializer
        :param num_top_statistics: the number of top statistics being computed for each problem being answered
        """
        # constant variables
        self.num_top_statistics = num_top_statistics

        # global variables
        self.formatted_repo_data = dict()
        self.most_updated_repo_stats = list()
        self.most_popular_repo_languages_stats = list()
        self.most_popular_tdd_repo_language_stats = list()
        self.most_popular_tdd_and_devops_repo_language_stats = list()

    def set_num_top_statistics(self, num_top_statistics):
        """
        Set the number of the top GitHub repository and language statistics being returned and printed to UI
        :param num_top_statistics: integer value representing the number of the top statistics to return
        """
        self.num_top_statistics = num_top_statistics

    def reset_repo_stats(self):
        """
        Reset the lists for each of the results from the statistical analysis computations
        """
        self.most_updated_repo_stats = list()
        self.most_popular_repo_languages_stats = list()
        self.most_popular_tdd_repo_language_stats = list()
        self.most_popular_tdd_and_devops_repo_language_stats = list()

    def load_formatted_stats_data(self, path='files/'):
        """
        Load formatted data fetched from the GitHub API for statistical analysis
        :param path: file path
        """
        for json_file_name in iglob(path + 'repo_stats_data**' + self.VALID_DATA_FILE_MSG + '**.json',
                                    recursive=True):
            with open(json_file_name, 'r') as json_file:
                new_stats_dict = load(json_file)
                self.formatted_repo_data = DatabaseManager.merge_formatted_stats_data(self.formatted_repo_data,
                                                                                      new_stats_dict)
                self.reset_repo_stats()  # reset all top stats global variables when formatted data dict is updated
                self.compute_all_repo_stats()  # recompute all top repo stats with updated stats data

    def compute_all_repo_stats(self):
        """
        Compute all the below statistical analyses in a single function call
        """
        self.compute_most_popular_repo_language_stats()
        self.compute_most_popular_repo_language_stats()
        self.compute_most_popular_tdd_repo_language_stats()
        self.compute_most_popular_tdd_and_devops_repo_language_stats()

    def get_date_range(self):
        """
        Get the minimum and maximum of date strings within the date ranges in the dateRange array
        :return: tuple containing the minimum and the maximum date strings in the date ranges array
        """
        return (min(date_range[1] for date_range in self.formatted_repo_data['dateRange']),
                max(date_range[0] for date_range in self.formatted_repo_data['dateRange']))

    def get_sample_dataset_len(self):
        """
        Get the length of the repos array from the formatted repo dictionary as the count of the sample dataset
        :return: the sample dataset count: the length of the repos array in the formatted repo dictionary
        """
        return len(self.formatted_repo_data['repos'])

    def get_repo_population_count(self):
        """
        Get the estimated total number of repositories that could have been fetched within the date range(s) from
         the totalRepoCount key in the formatted repo dictionary as the count of the estimated target population
        :return: estimated target population count: max num repos that could have been fetched within the date range(s)
        """
        return self.formatted_repo_data['totalRepoCount']

    def compute_most_updated_repo_stats(self):
        """
        Compute the most frequently updated repos by commit count within a given date range then total all-time commits
        :return: array num_top_statistics tuples from most updated repos: [(repo-name, (commit-count, total-commits))]
        """
        if len(self.most_updated_repo_stats) == 0 and len(self.formatted_repo_data) > 0:
            # preprocess repo commit data as a dict: {repo-name: (commit-count-in-date-range, all-time-commit-count)}
            self.most_updated_repo_stats = {repo_name: [repo_data['totalCommitsInDateRange'],
                                                        repo_data['totalRepoCommits']]
                                            for repo_name, repo_data in self.formatted_repo_data['repos'].items()}
            # sort dict of repo commit counts in order of greatest first tuple element val before the second tuple val
            self.most_updated_repo_stats = sorted(self.most_updated_repo_stats.items(),
                                                  key=lambda x: x[1],
                                                  reverse=True)
        return self.most_updated_repo_stats[:self.num_top_statistics]\
            if len(self.most_updated_repo_stats) >= self.num_top_statistics else self.most_updated_repo_stats

    def compute_most_popular_repo_language_stats(self, is_tdd=False, is_devops=False):
        """
        Compute the most popular repo languages by repo use count within a given date range then total file size use
        :param is_tdd: if computing the most popular languages for repos following the TDD practices
        :param is_devops: if computing the most popular languages for repos following the TDD and DevOps practices
        :return: array num_top_statistics tuples for most popular languages: [(language, (repo-count, total-file-size))]
        """
        if (not is_tdd and not is_devops and len(self.most_popular_repo_languages_stats) == 0 or is_tdd or is_devops)\
                and len(self.formatted_repo_data) > 0:
            # preprocess repo language data as a dict: {language-name: (total-repo-use-count, total-file-size)}
            tmp_most_popular_languages_stats = dict()
            for repo_data in self.formatted_repo_data['repos'].values():
                if is_tdd and not repo_data['isTDD'] or is_devops and not repo_data['isDevOps']:
                    continue
                for language_name, val in repo_data['languages'].items():
                    if language_name in tmp_most_popular_languages_stats.keys():
                        tmp_most_popular_languages_stats[language_name][0] += 1
                        tmp_most_popular_languages_stats[language_name][1] += val['fileSize']
                    else:
                        tmp_most_popular_languages_stats[language_name] = [1, val['fileSize']]
            # sort dict of language use counts in order of greatest first tuple element val before the second tuple val
            tmp_most_popular_languages_stats = sorted(tmp_most_popular_languages_stats.items(),
                                                      key=lambda x: x[1],
                                                      reverse=True)
            if is_tdd or is_devops:
                return tmp_most_popular_languages_stats
            self.most_popular_repo_languages_stats = tmp_most_popular_languages_stats
        return self.most_popular_repo_languages_stats[:self.num_top_statistics] \
            if len(self.most_popular_repo_languages_stats) >= self.num_top_statistics \
            else self.most_popular_repo_languages_stats

    def compute_most_popular_tdd_repo_language_stats(self):
        """
        Compute the most popular TDD repo languages by repo use count within a given date range then total file size use
        :return: array num_top_statistics tuples for most popular TDD languages:
                                                                             [(language, (repo-count, total-file-size))]
        """
        if len(self.most_popular_tdd_repo_language_stats) == 0 and len(self.formatted_repo_data) > 0:
            self.most_popular_tdd_repo_language_stats = self.compute_most_popular_repo_language_stats(is_tdd=True)
        return self.most_popular_tdd_repo_language_stats[:self.num_top_statistics] \
            if len(self.most_popular_tdd_repo_language_stats) >= self.num_top_statistics \
            else self.most_popular_tdd_repo_language_stats

    def compute_most_popular_tdd_and_devops_repo_language_stats(self):
        """
        Compute most popular TDD and DevOps repo languages by repo use count in a date range then total file size use
        :return: array num_top_statistics tuples for most popular TDD and DevOps languages:
                                                                             [(language, (repo-count, total-file-size))]
        """
        if len(self.most_popular_tdd_and_devops_repo_language_stats) == 0 and len(self.formatted_repo_data) > 0:
            self.most_popular_tdd_and_devops_repo_language_stats = \
                self.compute_most_popular_repo_language_stats(is_tdd=True, is_devops=True)
        return self.most_popular_tdd_and_devops_repo_language_stats[:self.num_top_statistics] \
            if len(self.most_popular_tdd_and_devops_repo_language_stats) >= self.num_top_statistics \
            else self.most_popular_tdd_and_devops_repo_language_stats
