#!/usr/bin/env python3

from pulsar import Client


class PulsarClient:
    """
    Superclass for the producer-consumer components

    Author: Adam Ross
    Copyright: Only for the use of Group 17 for the course 1TD075 Data Engineering II held at Uppsala University in 2023
    """

    SERVICE_URL = "pulsar://localhost:6650"
    TOPIC_NAME = "GitHubStatsFetchingInstance"

    def __init__(self, topic_name=TOPIC_NAME):
        """
        Class constructor
        :param topic_name: the name of the topic used by the producer and consumer
        """
        self.pulsar_client = Client(service_url=self.SERVICE_URL)  # instantiate new Pulsar Client instance
        self.topic_name = topic_name

    def close_client(self):
        """
        Method for closing resources used by both the producer and consumer
        """
        self.pulsar_client.close()
