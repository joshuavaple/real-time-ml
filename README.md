# real-time-ml
Real Time ML Course


## Session 1 todos
- [x] Redpanda up and running with docker
- [x] Push some fake data to Redpanda
- [x] Push real-time data from Kraken

## Session 2
- [x] Extract config params out of the source code, such as the crypto name and currency, with Pydantic Settings.
- [x] Dockerize it, config the network, and run the service in a container
- [ ] Homework: adjust the codes so that the trade_producer can produce several product_ids = ['BTC/USD', 'BTC/EUR']. Some ideas:
    * the config file
    * the Kraken websocket API class

- [x] Build trade-to-ohlc (open-high-low-close) service (read-and-write)
- [ ] Homework: 
    * Extract parameters
    * Dockerize the trade-to-ohlc service

- [x] Topic to feature store service (Kafka consumer, read-only data from Kafka)
- [ ] Start the backfill
    - [x] Implement a Kraken Historical data reader (trade producer)
    - [x] Adjust timestamps used to bucket trades into windows (trade to ohlc)
    - [x] Save historical OHLC features in batches to the offline feature store (topic to feature store)

