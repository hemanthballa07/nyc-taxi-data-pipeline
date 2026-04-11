---
name: docker-ops
description: Manage Docker services for the project. Use when starting/stopping services, debugging container issues, or modifying docker-compose.yml.
---

# Docker Operations Skill

## Services in docker-compose.yml
| Service    | Port | Purpose                    |
|------------|------|----------------------------|
| postgres   | 5432 | Data warehouse             |
| airflow-*  | 8080 | Pipeline orchestration     |
| metabase   | 3000 | Dashboard / BI             |

## Common Operations
```bash
# Start everything
docker compose up -d

# Start only Postgres (for development)
docker compose up -d postgres

# Check status
docker compose ps

# View logs
docker compose logs -f postgres
docker compose logs -f airflow-webserver

# Connect to Postgres
docker compose exec postgres psql -U nyctaxi -d nyctaxi

# Stop everything
docker compose down

# Stop and remove volumes (DESTRUCTIVE - resets all data)
docker compose down -v
```

## Troubleshooting
- If Postgres won't start: check if port 5432 is already in use (`lsof -i :5432`)
- If Airflow tasks fail: check logs in the Airflow UI at http://localhost:8080
- If Metabase can't connect to Postgres: verify they're on the same Docker network
- If containers are slow: check Docker resource allocation (at least 4GB RAM)

## Environment Variables
Stored in `.env` file (gitignored). Template in `.env.example`:
```
POSTGRES_USER=nyctaxi
POSTGRES_PASSWORD=nyctaxi
POSTGRES_DB=nyctaxi
AIRFLOW__CORE__FERNET_KEY=<generate with python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
```

## Adding a New Service
1. Add service definition to `docker-compose.yml`
2. Ensure it's on the same network as other services
3. Add health check if other services depend on it
4. Document the port in this skill and in CLAUDE.md
