from github_stats_analysis import GitHubStatsAnalysis
from github_stats_data import GitHubStatsData
from datetime import datetime, timedelta
from db_manager import DatabaseManager
from os import getenv
from time import time
from sys import argv

"""
Test suite for simulating the producer-consumer module of the GitHub analytics streaming infrastructure using workflows

Author: Adam Ross
Copyright: Only for the use of Group 17 for the course 1TD075 Data Engineering II held at Uppsala University in 2023
"""


def test_api_stats(github_username=None, github_token=None, num_days_fetch=12, consumer_num=0):
    """
    Test the GitHub API fetching and statistical analysis - not used with Apache Pulsar producer-consumer
    :param github_username: GitHub account username associated with the GitHub token string being used
    :param github_token: GitHub token string for reading public repository data in GitHub API fetches
    :param num_days_fetch: the number of days in the date range from the start date of day-by-day GitHub API fetches
    :param consumer_num: the number of the consumer: to multiply num_days_fetch with for sequential parallel fetching
    """
    # simulate consumer fetch GitHub repo stats data from API
    test_data = GitHubStatsData(github_username=github_username if github_username else getenv("GH_USERNAME"),
                                github_token=github_token if github_token else getenv("GH_TOKEN"),
                                num_days_fetch=num_days_fetch,
                                start_date=datetime.now() - timedelta(days=num_days_fetch * consumer_num))
    test_data.init_db(is_test=True, is_reset_collection=True)
    test_data.fetch_repo_data()

    # simulate deployment retrieve from the DB data fetched from API and save it to .json file for statistical analysis
    print('\nRetrieving data from DB and saving to .json file for statistical analysis...')
    deployment_sim = DatabaseManager(is_test=True)
    timer = time()
    deployment_sim.retrieve_collection()
    deployment_sim.close_conn()
    timer = time() - timer
    print('\nTime taken to retrieve data from DB (s) =', str(timer))

    # simulate production compute statistical analysis
    stats_analysis = GitHubStatsAnalysis()
    timer = time()
    stats_analysis.load_formatted_stats_data()
    stats_top_repos = stats_analysis.compute_most_updated_repo_stats()
    stats_top_languages = stats_analysis.compute_most_popular_repo_language_stats()
    stats_top_languages_tdd = stats_analysis.compute_most_popular_tdd_repo_language_stats()
    stats_top_languages_tdd_devops = stats_analysis.compute_most_popular_tdd_and_devops_repo_language_stats()
    timer = time() - timer
    print('\nTime taken to compute stats for ' + str(num_days_fetch) + ' days of repository data (s) =', str(timer))

    print('\n#############################################################')
    print('# GitHub Statistics for Date Range: ' +
          test_data.get_date_str(num_days_fetch) + ' - ' + test_data.get_date_str(0) + ' #')
    print('#############################################################')

    print('\nTop ' + str(stats_analysis.num_top_statistics) + ' Most Frequently Updated Repositories: ')
    print(stats_top_repos)

    print('\nTop ' + str(stats_analysis.num_top_statistics) + ' Most Popular Repository Languages: ')
    print(stats_top_languages)

    print('\nTop ' + str(stats_analysis.num_top_statistics) + ' Most Popular TDD Repository Languages: ')
    print(stats_top_languages_tdd)

    print('\nTop ' + str(stats_analysis.num_top_statistics) + ' Most Popular TDD and DevOps Repository Languages: ')
    print(stats_top_languages_tdd_devops)

    print('\nSample size: ' + str(len(test_data.repo_stats_data['repos'])))
    print('Estimated population size for given date range: ' + str(test_data.repo_stats_data['totalRepoCount']))


if __name__ == "__main__":
    if len(argv) >= 2:
        # command: python3 test_github_stats.py <num_days_fetch (int)> [consumer_num (int)]
        num_consumer = 0 if len(argv) == 2 else min(0, int(argv[2]))
        test_api_stats(num_days_fetch=int(argv[1]), consumer_num=num_consumer)
    else:
        test_api_stats()  # command: python3 test_github_stats.py
