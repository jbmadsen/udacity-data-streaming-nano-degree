"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        # TODO: Configure the broker properties below. 
        # Make sure to reference the project README and use the Host URL for Kafka and Schema Registry!
        # see documentation: https://kafka.apache.org/documentation/#consumerconfigs
        # and https://docs.confluent.io/platform/current/clients/consumer.html#ak-consumer-configuration
        self.broker_properties = {
                # See README under "Running and Testing" for setting below - for Kafka Service
                "bootstrap.servers": "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094",
                # See documentation for this
                "group.id": f"{self.topic_name_pattern}",
                # Implementing init parameters
                "fetch.wait.max.ms": (int)(self.consume_timeout * 1000),
                "auto.offset.reset": "earliest" if self.offset_earliest else "latest"
        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        # TODO: Configure the AvroConsumer and subscribe to the topics. 
        # Make sure to think about how the `on_assign` callback should be invoked.
        # See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.subscribe
        # and exercise 2.5 for code inspiration
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        if self.offset_earliest:
            for partition in partitions:
                partition.offset = confluent_kafka.OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        # See exercise 2.5 for inspiration for this code
        try:
            # Try get message
            message = self.consumer.poll(timeout=self.consume_timeout)
            if message is None:
                # No message this time
                #logger.info(f"No message this time")
                return 0
            if message.error() is not None:
                # Error getting message
                logger.error(f"Error getting message: {message.error()}")
                return 0
            # Got message, lets handle it
            self.message_handler(message)
            return 1
        except Exception as e:
            logger.error(f"Exception getting message: {str(e)}")
            return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        # TODO: Cleanup the kafka consumer
        self.consumer.close()
