"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#producer
        self.broker_properties = {
            # TODO: Host URL Lifted from Exercise 1.2 / Exercise 4.7
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            # TODO: Schema Registry From: https://docs.confluent.io/platform/current/schema-registry/connect.html
            "schema.registry.url": "http://localhost:8081",
            # TODO: What is the last one? Something for the Avro Producer?
            # !FIXME
            # Additional suggested configurations suggested in the learning material
            "client.id": "public.transport.producer",
            "message.send.max.retries": 1,
            "enable.idempotence": False,
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        # Inspired from Exercise 4.7, and documentation
        # See: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.avro.AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        
        # TODO: Write code that creates the topic for this producer
        # if it does not already exist on the Kafka Broker.

        # Lifted from exercise 2.4
        bootstrap_servers = self.broker_properties["bootstrap.servers"]
        client = AdminClient({"bootstrap.servers": bootstrap_servers})

        # Check if topic already exists
        # From: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.list_topics
        existing_topics = client.list_topics(timeout=30)
        if self.topic_name in existing_topics.topics:
            logger.info("Producer topic already exists - skipping")
            return

        # Create if not exists
        # Lifted from exercise 2.2, where config is present as well
        futures = client.create_topics(
            [NewTopic(
                topic=self.topic_name, 
                num_partitions=self.num_partitions, 
                replication_factor=self.num_replicas,
                config={
                    "cleanup.policy": "delete",
                    "compression.type": "lz4",
                    "delete.retention.ms": "2000",
                    "file.delete.delay.ms": "2000",
                },
                )]
        )
        for _, future in futures.items():
            try:
                future.result()
            except Exception as e:
                print(f"Error creating Producer topic. Exiting production loop. Error: {str(e)}")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""

        # TODO: Write cleanup code for the Producer here
        # Inspired from: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-avro.html
        if self.producer is not None:
            self.producer.flush()
            #self.producer.close() # NOTE: Unsure about this, even though the function is called close, the comment suggest only to prepare for closing - i.e. flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        # There are two of these functions in the provided template code. Accident?
        return int(round(time.time() * 1000))
        
