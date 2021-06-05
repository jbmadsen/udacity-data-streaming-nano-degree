import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "my-first-python-topic"


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    # Configure the producer with `bootstrap.servers`
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#producer
    p = Producer({"bootstrap.servers": BROKER_URL})

    curr_iteration = 0
    while True:
        # Produce a message to the topic
        # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.produce
        p.produce(topic_name, f"iteration {curr_iteration}".encode("utf-8"))

        curr_iteration += 1
        await asyncio.sleep(1)


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    # Configure the consumer with `bootstrap.servers` and `group.id`
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#consumer
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "my-first-consumer-group"})

    # Subscribe to the topic
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.subscribe
    c.subscribe([topic_name])

    while True:
        # Poll for a message
        # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.poll
        message = c.poll(1.0)

        # TODO: Handle the message. Remember that you should:
        if message is None:
            # 1. Check if the message is `None`
            print("no message received by consumer")
        elif message.error() is not None:
            # 2. Check if the message has an error: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Message.error
            print(f"error from consumer {message.error()}")
        else:
            # 3. If 1 and 2 were false, print the message key and value
            # Key: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Message.key
            # Value: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Message.value
            print(f"consumed message {message.key()}: {message.value()}")

        await asyncio.sleep(1)


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


def main():
    """Runs the exercise"""
    # Configure the AdminClient with `bootstrap.servers`
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.AdminClient
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    # Create a NewTopic object. Don't forget to set partitions and replication factor to 1!
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.NewTopic
    topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)

    # Using `client`, create the topic
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.AdminClient.create_topics
    client.create_topics([topic])

    try:
        asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")
    finally:
        # Using `client`, delete the topic
        # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.AdminClient.delete_topics
        client.delete_topics([topic])
        pass


if __name__ == "__main__":
    main()
