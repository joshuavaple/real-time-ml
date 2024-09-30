start-redpanda:
	docker compose -f redpanda.yml up -d

stop-redpanda:
	docker compose -f redpanda.yml down

start-feature-pipeline:
	# docker compose -f feature_pipeline.yml up -d
	docker compose -f feature_pipeline.yml build --no-cache
	docker compose -f feature_pipeline.yml up -d

stop-feature-pipeline:
	docker compose -f feature_pipeline.yml down