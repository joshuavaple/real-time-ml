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

- [x] Build trade-to-ohlc service
- [ ] Homework: 
    * Extract parameters
    * Dockerize the trade-to-ohlc service