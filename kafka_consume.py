from kafka import KafkaConsumer
import configparser
import cdc_load_elk as c
import json

def cdc_consume():
    config = configparser.ConfigParser()
    config.read('config/config.ini')

    KAFKA_BOOTSTRAP_SERVERS = config.get('Kafka', 'host')
    KAFKA_TOPIC_MONGO_CDC = "mongo-cdc"
    KAFKA_CONSUMER_GROUP_ID = "mongo_cdc_elk"

    consumer = KafkaConsumer(
        KAFKA_TOPIC_MONGO_CDC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=KAFKA_CONSUMER_GROUP_ID
    )

    print(f"Listening for messages on topic: {KAFKA_TOPIC_MONGO_CDC}...")
    try:

        for message in consumer:
            print("Processing...")
            #print(f"Received message: {message.value}")
            c.cdc_elk_refresh(message.value)

    except Exception as e:
        print(f"Error connecting or loading data: {e}")
    finally:
        consumer.close()
