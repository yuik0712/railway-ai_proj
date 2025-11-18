from confluent_kafka import Consumer
from pymongo import MongoClient
import argparse
import os
import base64
import datetime

parser = argparse.ArgumentParser()
parser.add_argument('--topic', required=True)
args = parser.parse_args()

# Kafka Consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'video_group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([args.topic])

# MongoDB 연결
client = MongoClient('mongodb://localhost:27017/')
db = client['video_db']
collection = db['frames']

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Consumer error:", msg.error())
        continue
    frame_doc = {
        'topic': msg.topic(),
        'timestamp': datetime.datetime.utcnow(),
        'frame_base64': base64.b64encode(msg.value()).decode('utf-8')
    }
    collection.insert_one(frame_doc)
    print(f"Stored frame of size {len(msg.value())} bytes to MongoDB")
