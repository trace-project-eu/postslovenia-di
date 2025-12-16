SERVICE_NAME = post-slovenia-ingestor

build:
	docker compose build

images:
	docker images

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker logs -f $(SERVICE_NAME)

restart: down build up logs

shell:
	docker exec -it $(SERVICE_NAME) /bin/bash
