from quixstreams import Application
from loguru import logger
from src.hopsworks_api import push_value_to_feature_group, get_feature_store
from typing import List


def topic_to_feature_store(
    kafka_broker_address: str,
    kafka_input_topic: str,
    kafka_consumer_group: str,
    feature_group_name: str,
    feature_group_version: int,
    feature_group_primary_keys: List[str],
    feature_group_event_time: str,
    start_offline_materialization:bool,
    batch_size:int,
    # feature store creds: TBU
):
    """ 
    Reads incoming messages from a Kafka topic `kafka_input_topic` and writes them to the `feature_group_name` in the feature store.

    Args:
        kafka_broker_address (str): Kafka broker address
        kafka_input_topic (str): Kafka topic to read from
        kafka_consumer_group (str): Kafka consumer group
        feature_group_name (str): Name of the feature group
        feature_group_version (int): Version of the feature group
        feature_group_primary_keys (List[str]): List of primary key columns
        feature_group_event_time (str): Event time column
        start_offline_materialization (bool): Whether to store the data in offline storage as well when we save the `value` to the feature group
        batch_size (int): Number of messages to accumulate to memory before writing to the feature store

    Returns:
        None
    """
    # Configure an Application. 
    # The config params will be used for the Consumer instance too.
    app = Application(
        broker_address=kafka_broker_address, 
        auto_offset_reset='latest', # this arg is used to tell the consumer where to start reading messages from - from beginning of the topic or from the latest message
        consumer_group=kafka_consumer_group,
    )

    feature_store = get_feature_store()
    # breakpoint()
    batch = []
    # breakpoint()

    # Create a consumer and start a polling loop
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_input_topic])

        while True:
            msg = consumer.poll(0.1) # how long to wait for messages in seconds

            if msg is None:
                continue
            elif msg.error():
                # print('Kafka error:', msg.error())
                logger.error(f"Kafka error: {msg.error()}")
                continue

            value = msg.value() # this is in bytes

            # decode the bytes to dict
            import json
            value = json.loads(value.decode('utf-8'))

            # Append the message to the batch
            batch.append(value)

            # If the batch is not full yet, continue polling
            if len(batch) < batch_size:
                logger.debug(f'Batch has length of {len(batch)} < {batch_size}. Continuing...')
                continue

            logger.debug(f'Batch has length of {len(batch)} >= {batch_size}. Pushing data to feature store...')

            # breakpoint()
            # now we need to push the value to the feature store
            push_value_to_feature_group(
                feature_store,
                batch, 
                feature_group_name,
                feature_group_version, 
                feature_group_primary_keys,
                feature_group_event_time,
                start_offline_materialization,        

            )

            # clear the batch
            batch = []

            # Store the offset of the processed message on the Consumer 
            # for the auto-commit mechanism.
            # It will send it to Kafka in the background.
            # Storing offset only after the message is processed enables at-least-once delivery
            # guarantees.
            consumer.store_offsets(message=msg)


if __name__ == "__main__":
    from src.config import config
    # app = init_app(
    #     kafka_broker_address=config.kafka_broker_address,
    #     kafka_consumer_group=config.kafka_consumer_group)

    topic_to_feature_store(
        # app=app,
        kafka_broker_address=config.kafka_broker_address,
        kafka_input_topic=config.kafka_input_topic,
        kafka_consumer_group=config.kafka_consumer_group,
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_version,
        feature_group_primary_keys=config.feature_group_primary_keys,
        feature_group_event_time=config.feature_group_event_time,
        start_offline_materialization=config.start_offline_materialization,
        batch_size=config.batch_size
    )