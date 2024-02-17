from github_stats_analysis import GitHubStatsAnalysis
from flask import Flask, render_template

"""
Development server for the production web app interactive UI graphical representation of GitHub analytical results

Author: Kim Kuruvilla Mathews, and Adam Ross
"""

DEFAULT_NUM_DISPLAYED_STATS = 10
MAX_NUM_DISPLAYED_STATS = 100

app = Flask(__name__)

global stats_analysis, is_stats_computed


def get_github_stats():
    """
    Instantiate GitHub analytics dataset for computing statistical analyses
    It is instantiated with an initial maximum 100 top statistics being returned
    :return: Singleton GitHub statistical analysis instance with dataset loaded from .json file
    """
    global stats_analysis, is_stats_computed

    if not is_stats_computed:
        stats_analysis = GitHubStatsAnalysis(num_top_statistics=MAX_NUM_DISPLAYED_STATS)
        stats_analysis.load_formatted_stats_data()
        stats_analysis.compute_all_repo_stats()
        is_stats_computed = True
    return stats_analysis, stats_analysis.get_date_range(), \
        stats_analysis.get_sample_dataset_len(), stats_analysis.get_repo_population_count()


# Route for the index page
@app.route('/')
def index():
    return render_template('index.html')


# Define route to render the top most frequently updated repositories
@app.route('/updated_repos')
def updated_repos():
    global stats_analysis
    stats_analysis, date_range, sample_count, population_count = get_github_stats()
    stats_top_repos = stats_analysis.compute_most_updated_repo_stats()

    return render_template('updated_repos.html',
                           min_date=date_range[0],
                           max_date=date_range[1],
                           sample=sample_count,
                           population=population_count,
                           repos=stats_top_repos)


# Define route to render the top most popular repository languages
@app.route('/popular_languages')
def popular_languages():
    global stats_analysis
    stats_analysis, date_range, sample_count, population_count = get_github_stats()
    stats_top_languages = stats_analysis.compute_most_popular_repo_language_stats()

    return render_template('popular_languages.html',
                           header='',
                           min_date=date_range[0],
                           max_date=date_range[1],
                           sample=sample_count,
                           population=population_count,
                           languages=stats_top_languages)


# Define route to render the top most popular TDD repository languages
@app.route('/tdd_languages')
def tdd_languages():
    global stats_analysis
    stats_analysis, date_range, sample_count, population_count = get_github_stats()
    stats_top_languages_tdd = stats_analysis.compute_most_popular_tdd_repo_language_stats()

    return render_template('popular_languages.html',
                           header=' TDD',
                           min_date=date_range[0],
                           max_date=date_range[1],
                           sample=sample_count,
                           population=population_count,
                           languages=stats_top_languages_tdd)


# Define route to render the top most popular TDD and DevOps repository languages
@app.route('/tdd_devops_languages')
def tdd_devops_languages():
    global stats_analysis
    stats_analysis, date_range, sample_count, population_count = get_github_stats()
    stats_top_languages_tdd_devops = stats_analysis.compute_most_popular_tdd_and_devops_repo_language_stats()

    return render_template('popular_languages.html',
                           header=' TDD and DevOps',
                           min_date=date_range[0],
                           max_date=date_range[1],
                           sample=sample_count,
                           population=population_count,
                           languages=stats_top_languages_tdd_devops)


def main():
    global is_stats_computed
    is_stats_computed = False
    get_github_stats()
    app.run(debug=True)


if __name__ == '__main__':
    main()
