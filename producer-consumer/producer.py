#!/usr/bin/env python3

from github_stats_data import GitHubStatsData
from datetime import datetime, timedelta
from pulsar_client import PulsarClient
from pulsar import schema
from pickle import dumps
from os import getenv
from sys import argv


class Producer(PulsarClient):
    """
    Untested producer class

    Author: Adam Ross
    Copyright: Only for the use of Group 17 for the course 1TD075 Data Engineering II held at Uppsala University in 2023
    """

    TODAY = datetime.now()  # constant for the default start date, which is used for the first or the only consumer
    MAX_NUM_DAYS = 365  #
    PRODUCER_NAME = 'GitHubRepoStatsDataFetchingParallelizedCoordinator'
    TIMEOUT = 604800  #

    def __init__(self):
        """
        Class constructor
        """
        super().__init__()
        self.producer = self.pulsar_client.create_producer(topic=self.topic_name,
                                                           producer_name=self.PRODUCER_NAME,
                                                           initial_sequence_id=0,
                                                           schema=schema.BytesSchema)  # create a new producer

    def publish(self, start_date=TODAY, num_days=MAX_NUM_DAYS, num_consumers=1):
        """
        Publish method for the producer
        :param start_date: the start date of all data fetching from the GitHub API
        :param num_days: the total number of days data is being fetched from the GitHub API
        :param num_consumers: the number of consumer fetching data from the GitHub API
        """
        consumers_split_num_days = num_days / num_consumers
        for consumer_id in range(num_consumers):
            consumer_start_date = start_date - timedelta(days=consumers_split_num_days * consumer_id +
                                                         min(consumer_id, num_days % num_consumers))
            consumer_split_num_days = consumers_split_num_days + (1 if num_days % num_consumers > consumer_id else 0)
            consumer_instance = GitHubStatsData(github_username=getenv('GH_USERNAME_' + str(consumer_id)),
                                                github_token=getenv('GH_TOKEN_' + str(consumer_id)),
                                                start_date=consumer_start_date,
                                                num_days_fetch=consumer_split_num_days,
                                                created_before_date=num_days,
                                                instance_id=consumer_id)
            self.producer.send_async(content=dumps(consumer_instance),
                                     callback=None,
                                     chunking_enabled=True,
                                     send_timeout_millis=self.TIMEOUT)
            self.producer.flush()

    def close(self):
        """
        Method for closing the resources used for the producer
        """
        self.producer.close()
        self.close_client()


def pulsar_client_producer():
    """
    Main function for the producer
    """
    producer = Producer()  # instantiate new Producer instance
    producer.publish(num_days=int(argv[1]), num_consumers=int(argv[2]))
    producer.close()


if __name__ == '__main__':
    if len(argv) == 3:
        pulsar_client_producer()
