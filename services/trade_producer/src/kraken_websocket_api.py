from typing import List
from time import sleep
from websocket import create_connection
from loguru import logger
import json
from pydantic import BaseModel
from datetime import datetime, timezone


class Trade(BaseModel):
    product_id: str
    price: float
    quantity: float
    timestamp_ms: int

class KrakenWebsocketAPI:
    """
    Class for reading real-time data from Kraken websocket API
    """

    URL = 'wss://ws.kraken.com/v2'

    def __init__(self, product_id: str):
        """
        Initializes the KrakenWebsocketAPI class

        Args:
            product_id (str): the product id of the trades to read from the Kraken API
        """
        self.product_id = product_id

        # establish connection to the Kraken websocket API
        self._ws = create_connection(self.URL)
        logger.debug('Connection established')

        # subscribe to the trades for the given `product_id`
        self._subscribe(product_id)
    
    def _subscribe(self, product_id: str):
        """
        Establish connection to the Kraken websocket API and subscribe to the trades for the given `product_id`.
        """
        logger.info(f'Subscribing to trades for {product_id}')
        # let's subscribe to the trades for the given `product_id`
        msg = {
            'method': 'subscribe',
            'params': {
                'channel': 'trade',
                'symbol': [product_id],
                'snapshot': False,
            },
        }

        # send the subscription message
        self._ws.send(json.dumps(msg)) # needs to be in json string format
        logger.info('Subscription worked!')

        # For each product_id we dump
        # the first 2 messages we got from the websocket, because they contain
        # no trade data, just confirmation on their end that the subscription was successful
        # hence we can discard them using the following loop
        for product_id in product_id:
            _ = self._ws.recv()
            _ = self._ws.recv()
    
    def get_trades(self) -> List[Trade]:
        """ 
        Returns the latest batch of trades from the Kraken websocket API

        Args: 
            None

        Returns:
            List[Trade]: A list of Trade objects
        """
        # # fake event for testing
        # event = [{"product_id": "ETH/USD",
        # "price": 1000,
        # "qty": 1,
        # "timestamp_ms": 1612345678900
        # }]
        # sleep(1)
        # return event

        message = self._ws.recv()

        if 'heartbeat' in message:
            # when I get a heartbeat, I return an empty list
            logger.info('Heartbeat received')
            return []

        # parse the message string as a dictionary
        message = json.loads(message)

        # extract trades from the message['data'] field
        trades = []
        for trade in message['data']:
            # we want to extract the following fields from the trade:
            # product_id, price, qty, timestamp in ms

            # breakpoint() # this is a debugging tool to stop the program at this point and inspect the variables from the terminal


            trades.append(
                Trade(
                    product_id=trade['symbol'],
                    price=trade['price'],
                    quantity=trade['qty'],
                    timestamp_ms=self.to_ms(trade['timestamp']),
                )
            )

        return trades
        

    def is_done(self) -> bool:
        """
        Returns True if the websocket connection is closed
        """
        pass
    
    @staticmethod
    def to_ms(timestamp: str) -> int:
        """
        A function that transforms a timestamps expressed
        as a string like this '2024-06-17T09:36:39.467866Z'
        into a timestamp expressed in milliseconds.

        Args:
            timestamp (str): A timestamp expressed as a string.

        Returns:
            int: A timestamp expressed in milliseconds.
        """
        # parse a string like this '2024-06-17T09:36:39.467866Z'
        # into a datetime object assuming UTC timezone
        # and then transform this datetime object into Unix timestamp
        # expressed in milliseconds
        

        timestamp = datetime.fromisoformat(timestamp[:-1]).replace(tzinfo=timezone.utc)
        return int(timestamp.timestamp() * 1000)