"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # Configure the broker properties with the Host URL for Kafka and Schema Registry.
        #
        #
        self.broker_properties = {
            'broker': 'PLAINTEXT://localhost:9092',
            'schema_registry': 'http://localhost:8081'
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(
            {
                'bootstrap.servers': self.broker_properties.get('broker'),
                'schema.registry.url': self.broker_properties['schema_registry']
            }
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        admin_client = AdminClient(
                {
                    'bootstrap.servers': self.broker_properties['broker']
                }
            )
        
        # Creates a topic 
        topic = NewTopic(
            self.topic_name,
            self.num_partitions,
            self.num_replicas,
            config={
                    "cleanup.policy": "delete",
                    "compression.type": "lz4",
                    "delete.retention.ms": "2000",
                    "file.delete.delay.ms": "2000",
                }
                
        )
        logger.info(f"topic creation: {self.topic_name}")
        admin_client.create_topics([topic])

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
