.PHONY: build seed run job stop logs clean test

# Build the Docker image (use make rebuild to force no-cache)
build:
	docker compose build

# Force rebuild without cache
rebuild:
	docker compose build --no-cache

# Seed the database with starter cards
seed: build
	docker compose run --rm seed

# Start the dashboard at http://localhost:8000
run: build
	docker compose up dashboard

# Run the daily job (backfill 7 days)
job: build
	docker compose run --rm daily-job

# Stop all containers
stop:
	docker compose down

# View dashboard logs
logs:
	docker compose logs -f dashboard

# Remove all containers and volumes (resets database)
clean:
	docker compose down -v

# Run tests locally
test:
	python -m pytest tests/ -v

# Full setup: build, seed, and start dashboard
setup: build seed run
