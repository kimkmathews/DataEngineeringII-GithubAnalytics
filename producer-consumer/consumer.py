#!/usr/bin/env python3

from pulsar import ConsumerType, schema
from pulsar_client import PulsarClient
from pickle import loads


class Consumer(PulsarClient):
    """
    Untested consumer class

    Author: Adam Ross
    Copyright: Only for the use of Group 17 for the course 1TD075 Data Engineering II held at Uppsala University in 2023
    """

    SUBSCRIPTION_NAME = "GitHubRepoStatsDataFetchingParallelized"

    def __init__(self):
        """
        Class constructor
        """
        super().__init__()
        self.consumer = self.pulsar_client.subscribe(topic=self.topic_name,
                                                     subscription_name=self.SUBSCRIPTION_NAME,
                                                     schema=schema.BytesSchema,
                                                     consumer_type=ConsumerType.Shared)  # create a new consumer
        self.status = None

    def consume(self):
        """
        Consume method for consumer
        """
        msg = self.consumer.receive()
        try:
            github_stats_data_instance = loads(msg.data())
            github_stats_data_instance.fetch_repo_data()
        except Exception:
            self.consumer.negative_acknowledge(msg)

    def close(self):
        """
        Method for closing the resources used for the consumer
        """
        self.consumer.close()
        self.close_client()


def pulsar_client_consumer():
    """
    Main function for the consumer
    """
    consumer = Consumer()  # instantiate new Consumer instance
    consumer.consume()
    consumer.close()


if __name__ == '__main__':
    pulsar_client_consumer()
