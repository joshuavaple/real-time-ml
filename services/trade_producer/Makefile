run-live-dev:
	cp live.dev.env .env
	poetry run python src/main.py

run-historical-dev:
	cp historical.dev.env .env
	poetry run python src/main.py

build:
	docker build -t trade-producer .

run-live: build
	docker run \
	 --network=redpanda_network \
	 --env-file live.prod.env \
	 trade-producer

run-historical: build
	docker run \
	 --network=redpanda_network \
	 --env-file historical.prod.env \
	 trade-producer