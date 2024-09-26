from quixstreams import Application
from loguru import logger
from datetime import datetime, timedelta
from typing import Any, List, Optional, Tuple


def init_ohlcv_candle(trade: dict):
    """ 
    Returns the initial state of the OHLCV candle when the first 'trade' in that window is received.
    """
    return {
        'open': trade['price'],
        'high': trade['price'],
        'low': trade['price'],
        'close': trade['price'],
        'volume': trade['quantity'],
        'product_id': trade['product_id'],
        # 'timestamp': None,
    }

def update_ohlcv_candle(candle: dict, trade: dict):
    """ 
    Updates the OHLCV candle with the new 'trade' data.

    Args:
        candle (dict): The current state of the OHLCV candle.
        trade (dict): The new trade data.
    """
    candle['high'] = max(candle['high'], trade['price']) # the max between the previous high and the current trade price
    candle['low'] = min(candle['low'], trade['price'])
    candle['close'] = trade['price']
    candle['volume'] += trade['quantity']
    candle['product_id'] = trade['product_id']
    # candle['timestamp'] = trade['timestamp']
    return candle

def custom_ts_extractor(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type #TimestampType, # not sure where to import this dtype from, ignore for now
) -> int:
    """
    Specifying a custom timestamp extractor to use the timestamp from the message payload 
    instead of Kafka timestamp.

    Extracts the field where the timestamp is stored in the message payload.
    """
    return value["timestamp_ms"]

def transform_trade_to_ohlcv(
    kafka_broker_address: str,
    kafka_input_topic: str,
    kafka_output_topic: str,
    kafka_consumer_group: str,
    ohlcv_window_seconds: int,
):
    """ 
    Reads incoming trades from the given `kafka_input_topic`, transforms them into OHLC data (stateful transformation)
    and outputs them to the given `kafka_output_topic`.

    Args:
        kafka_broker_address (str): The address of the Kafka broker.
        kafka_input_topic (str): The topic to read trades from.
        kafka_output_topic (str): The topic to write OHLC data to.
        kafka_consumer_group (str): The consumer group to use when reading trades.
    
    Returns:
        None
    """

    app = Application(
        broker_address = kafka_broker_address,
        consumer_group = kafka_consumer_group,
    )

    input_topic = app.topic(name=kafka_input_topic, value_deserializer='json', timestamp_extractor=custom_ts_extractor)
    output_topic = app.topic(name=kafka_output_topic, value_serializer='json')

    # Create a QuixStreams streaming dataframe:
    sdf = app.dataframe(input_topic)

    # check if we are actually reading the trades
    # sdf.update(logger.debug)

    # we need to customize how the window is initialized and updated by providing custom functions
    # the .final() method is used to indicate that the window is complete and the final result should be emitted
    # everytime a new trade comes, the reducer will be used to update the window
    # only at the end of the window, the .final() method will be called to emit the final result
    # e.g., there are 60 trades in the window, only the final result will be emitted, there is only 1 candle
    # there is .current() method that can be used to emit all the intermediate results of the window, 
    # this will be one-to-one mapping between the trade and the candle, but this is not what we want
    
    sdf = (
        sdf.tumbling_window(duration_ms=timedelta(seconds=ohlcv_window_seconds))
        .reduce(reducer=update_ohlcv_candle, initializer=init_ohlcv_candle)
        .final()
        # .current() # for testing purposes
    )
    # sdf.apply(logger.debug)
    # breakpoint()
    
    # unpack the dictionary into separate columns
    sdf['open'] = sdf['value']['open']
    sdf['high'] = sdf['value']['high']
    sdf['low'] = sdf['value']['low']
    sdf['close'] = sdf['value']['close']
    sdf['volume'] = sdf['value']['volume']
    sdf['product_id'] = sdf['value']['product_id']
    sdf['timestamp_ms'] = sdf['end']

    # keep only the columns we want to output
    # sdf = sdf[['product_id','timestamp_ms', 'open', 'high', 'low', 'close', 'volume']]
    sdf = sdf[['product_id','timestamp_ms', 'open', 'high', 'low', 'close', 'volume']]
    
    # print output to console
    sdf.update(logger.debug)

    # breakpoint()

    # push this message to the output topic
    sdf.to_topic(output_topic)

    app.run(sdf)

if __name__ == "__main__":

    from src.config import config

    transform_trade_to_ohlcv(
        kafka_broker_address=config.kafka_broker_address, #'localhost:19092',
        kafka_input_topic=config.kafka_input_topic, #'trades',
        kafka_output_topic=config.kafka_output_topic, #'ohlcv',
        kafka_consumer_group=config.kafka_consumer_group, #'consumer_group_trade_to_ohlcv',
        ohlcv_window_seconds=config.ohlcv_window_seconds, #60,
    )