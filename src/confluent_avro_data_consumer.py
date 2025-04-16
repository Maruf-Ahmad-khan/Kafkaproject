import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import logging
from pathlib import Path

# Ensure logs directory exists
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

# Set up logging
logging.basicConfig(
    filename='logs/consumer.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '5CLA4BMA2Y3HMDWO',
    'sasl.password': 'gD/zGnXMfR4VNKoB1CG9VyYLdL2qGhvoIfu1qc4FIpkN2+0LFZ9fsZjEn6yKvR/8',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-v1593j.us-central1.gcp.confluent.cloud',
    'basic.auth.user.info': 'EPEGU2XHDP4T2QSV:dRmy+J9XXaNSL6D8HIWdWq6H6Toao18dxJi1/WV/LBstNcT3azXSmF67wgdEl3CO'
})

# Fetch the latest Avro schema for the value
subject_name = 'retail_data_test-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
})

# Subscribe to the 'retail_data_test' topic
consumer.subscribe(['retail_data_test'])

# Continually read messages from Kafka
try:
    logger.info("Starting consumer, press Ctrl+C to exit...")
    print("Starting consumer, press Ctrl+C to exit...")
    while True:
        msg = consumer.poll(1.0)  # How many seconds to wait for message

        if msg is None:
            continue
        if msg.error():
            logger.error(f'Consumer error: {msg.error()}')
            print(f'Consumer error: {msg.error()}')
            continue

        # Remove decode() since data is already string
        print(f'Successfully consumed record with key {msg.key()} and value {msg.value()}')
        logger.info(f'Successfully consumed record with key {msg.key()} and value {msg.value()}')

except KeyboardInterrupt:
    logger.info("Consumer interrupted by user")
    print("Consumer interrupted by user")
finally:
    consumer.close()
    logger.info("Consumer closed")
    print("Consumer closed")