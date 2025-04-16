import datetime
import threading
from decimal import Decimal
from time import sleep
import time
import logging
from pathlib import Path

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd

# Ensure logs directory exists
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

# Set up logging
logging.basicConfig(
    filename='logs/producer.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    """
    if err is not None:
        logger.error(f"Delivery failed for User record {msg.key()}: {err}")
        return
    logger.info(f'User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '5CLA4BMA2Y3HMDWO',
    'sasl.password': 'gD/zGnXMfR4VNKoB1CG9VyYLdL2qGhvoIfu1qc4FIpkN2+0LFZ9fsZjEn6yKvR/8',
    'debug': 'all'  # Enable debug logs for troubleshooting
}

# Schema Registry configuration
schema_registry_config = {
    'url': 'https://psrc-v1593j.us-central1.gcp.confluent.cloud',
    'basic.auth.user.info': 'EPEGU2XHDP4T2QSV:dRmy+J9XXaNSL6D8HIWdWq6H6Toao18dxJi1/WV/LBstNcT3azXSmF67wgdEl3CO'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient(schema_registry_config)

# Fetch or register schema
subject_name = 'retail_data_test-value'
schema_file_path = 'schemas/retail_data_avro_schema.json'

try:
    # Try to fetch the latest schema
    schema = schema_registry_client.get_latest_version(subject_name).schema
    schema_str = schema.schema_str
    logger.info(f"Schema fetched for {subject_name}")
except Exception as e:
    if "404" in str(e).lower() or "not found" in str(e).lower():
        try:
            # Load schema from file
            with open(schema_file_path, 'r') as f:
                schema_str = f.read()
            # Create a Schema object
            schema = Schema(schema_str, schema_type='AVRO')
            # Register the schema
            schema_id = schema_registry_client.register_schema(subject_name, schema)
            logger.info(f"Schema registered for {subject_name} with ID {schema_id}")
        except Exception as se:
            logger.error(f"Failed to register schema: {se}")
            raise
    else:
        logger.error(f"Failed to get schema: {e}")
        raise

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,
    'value.serializer': avro_serializer,
    'debug': 'all'  # Enable debug logs
})

# Load the CSV data into a pandas DataFrame
try:
    df = pd.read_csv('data/retail_data.csv')
    print(df)
    df = df.fillna('null')
    logger.info("Data loaded successfully from CSV")
except Exception as e:
    logger.error(f"Failed to load data: {e}")
    raise

# Iterate over DataFrame rows and produce to Kafka
try:
    for index, row in df.iterrows():
        value = row.to_dict()
        # Convert Decimal types to float for Avro compatibility
        for k, v in value.items():
            if isinstance(v, Decimal):
                value[k] = float(v)
        producer.produce(
            topic='retail_data_test',
            key=str(index),
            value=value,
            on_delivery=delivery_report
        )
        producer.poll(0)  # Trigger delivery callbacks
        time.sleep(0.5)  # Reduced delay for better performance
    producer.flush()  # Ensure all messages are sent
    logger.info("All data successfully published to Kafka")
except Exception as e:
    logger.error(f"Error producing data: {e}")
    raise

logger.info("Producer process completed")