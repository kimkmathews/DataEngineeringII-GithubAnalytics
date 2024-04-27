# Data Engineering II - Project 2
GitHub Analytic System leverages Apache Pulsar and MongoDB to create a scalable streaming framework for real-time analysis of GitHub repository data. With parallelized data fetching and preprocessing, it offers insights into repository trends and programming language usage patterns. The system features a dynamic web UI for interactive visualization of top repository metrics.
## Instructions

### Authentication

The application requires authenticating with the GitHub API. To authenticate with the GitHub API, a [GitHub token](https://github.com/settings/personal-access-tokens/new) (default settings are sufficient) and associated GitHub account username are required to be set as OS environment variables with the following variable names, respectively:

> GH_TOKEN

> GH_USERNAME

Example for setting environment variables in a Bash terminal:

```bash
export GH_TOKEN=<generated-GitHub-token>
```
```bash
export GH_USERNAME=<associated-GitHub-account-username>
```

### Database

The application also requires connecting to an Atlas MongoDB cloud database with the following environment variables:

> DB_USER

> DB_PW

> DB_SOCKET_PATH

The following temporary credential can be set to each environment variable:

```bash
export DB_USER=team
```
```bash
export DB_PW=team
```
```bash
export DB_SOCKET_PATH=repostatsdata.gswndkw
```

### Requirements

Numerous libraries are required to run the application. These can all be installed by executing the following command:

```bash
pip install -r requirements.txt
```

### Test

The following command runs a test simulation of the distributed pipeline using a single consumer and CLI as production.

```bash
python3 test_github_stats.py <num_days_fetch (int)> [consumer_num (int)]
```

Here is a description of the command line arguments used to run the test simulation:

> The num_days_fetch argument is mandatory and sets the number of days fetched in each date range per consumer. 

> The consumer_num is the consumer ID, starting from 1 so that the date ranges fetched in parallel is sequential

### Run

#### Client

...

#### Producer

```bash
python3 producer-consumer/producer.py
```

#### Consumer

```bash
python3 producer-consumer/consumer.py
```

#### Deployment

To retrieve the documents inserted by each consumer following successful completion of fetching data from the GitHub API, execute the following command:

```bash
python3 db_manager.py
```

This will merge all document data and convert it to a single .json file for git pushing to production using Git Hooks:

```bash
git add **/repo_stats_data_DB_COMPLETED.json
```
```bash
git commit -m "Update GitHub stats data .json"
```
```bash
git push production main
```

#### Production

```bash
python3 ui_app.py
```
