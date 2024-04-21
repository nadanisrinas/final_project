#!/bin/bash
airflow db init
echo "AUTH_ROLE_PUBLIC = 'Admin'" >> webserver_config.py
airflow connections add 'postgres_tgs' \
    --conn-type 'postgres' \
    --conn-login $POSTGRES_USER \
    --conn-password $POSTGRES_PASSWORD \
    --conn-host $POSTGRES_CONTAINER_NAME \
    --conn-port $POSTGRES_PORT \
    --conn-schema $POSTGRES_DB
airflow webserver
