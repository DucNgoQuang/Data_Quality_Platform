#!/bin/bash

docker compose exec superset superset fab create-admin --username admin --firstname superset --lastname admin --email admin@superset.com --password admin
docker compose exec superset superset db upgrade
docker compose exec superset superset init