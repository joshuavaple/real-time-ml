run-live-dev:
	cp live.dev.env .env
	poetry run python src/main.py

run-historical-dev:
	cp historical.dev.env .env
	poetry run python src/main.py

build:
	docker build -t trade-to-ohlc .

run-live: build
	docker run \
	 --network=redpanda_network \
	 --env-file live.prod.env \
	 trade-to-ohlc

run-historical: build
	docker run \
	 --network=redpanda_network \
	 --env-file historical.prod.env \
	 trade-to-ohlc
