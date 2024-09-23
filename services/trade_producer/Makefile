run-dev:
	cp dev.env .env
	poetry run python src/main.py

build:
	docker build -t trade-producer .

run:
	docker run \
	 --network=redpanda_network \
	 --env-file prod.env \
	 trade-producer
