"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        print(f"{CONNECTOR_NAME} connector alaready exists")
        logging.debug("connector already created skipping recreation")
        return

    # Impelement Kafka Connect with JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    resp = requests.post(
       KAFKA_CONNECT_URL,
       headers={"Content-Type": "application/json"},
       data=json.dumps({
           "name": CONNECTOR_NAME,
           "config": {
               "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
               "key.converter": "org.apache.kafka.connect.json.JsonConverter",
               "key.converter.schemas.enable": "false",
               "value.converter": "org.apache.kafka.connect.json.JsonConverter",
               "value.converter.schemas.enable": "false",
               "batch.max.rows": "500",
               "connection.url": "jdbc:postgresql://localhost:5432/cta",
               "connection.user": "cta_admin",
               "connection.password": "chicago",
               "table.whitelist": "stations",
               "mode": "incrementing",
               "incrementing.column.name": "stop_id",
               "topic.prefix": "connect-",
               "poll.interval.ms": "20000"
           }
       }),
    )

    # Ensure a healthy response was given
    print("Creating connector")
    try:
        resp.raise_for_status()
        print("connector created successfully")
    except:
        print(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)


if __name__ == "__main__":
    configure_connector()
