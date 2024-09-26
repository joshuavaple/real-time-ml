# purpose of this program: reads trade data from Kraken, then writes it to a Kafka topic
from quixstreams import Application
# from src.kraken_websocket_api import KrakenWebsocketAPI
from loguru import logger
from typing import List
from src.trade_data_source import Trade, TradeSource


def produce_trades(
    kafka_broker_address: str,
    kafka_topic: str,
    # product_id:str 
    trade_data_source: TradeSource
):
    """ 
    Reads trades from the Kraken websocket API and saves them in the given `kafka_topic`

    Args:
        kafka_broker_address (str): the address of the Kafka broker
        kafka_topic (str): the name of the Kafka topic to write the trades to
        product_id (str): the product id of the trades to read from the Kraken API
    
    Returns:
        None
    """
    

    # Create an Application instance with Kafka config
    app = Application(broker_address=kafka_broker_address)

    # Define a topic "my_topic" with JSON serialization
    topic = app.topic(name=kafka_topic, value_serializer='json')

    # Create a KrakenWebsocketAPI instance
    # kraken_api = KrakenWebsocketAPI(product_id=product_id)

    # Create a Producer instance
    with app.get_producer() as producer:
        # while True:
        while not trade_data_source.is_done():
            # trades: List[Trade] = kraken_api.get_trades()
            trades: List[Trade] = trade_data_source.get_trades()
            for trade in trades:
                # serialize the trade to a message and push it to the Kafka topic
                message = topic.serialize(key=trade.product_id, value=trade.model_dump())
                producer.produce(topic=topic.name, value=message.value, key=message.key)

                logger.debug(f"Pushed trade to Kafka: {trade}")


if __name__ == "__main__":
    # broker address is specified in the redpanda yml file
    from src.config import config
    from src.trade_data_source.kraken_websocket_api import KrakenWebsocketAPI
    kraken_api = KrakenWebsocketAPI(product_id=config.product_id)

    produce_trades(
        kafka_broker_address=config.kafka_broker_address,
        kafka_topic=config.kafka_topic,
        # product_id=config.product_id
        trade_data_source=kraken_api,
    )
