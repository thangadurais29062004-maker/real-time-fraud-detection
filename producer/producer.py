import json
import random
import time
import uuid

import pandas as pd
from confluent_kafka import Producer

TOPIC = "transactions"
producer = Producer({"bootstrap.servers": "localhost:9092"})

df = pd.read_csv("demo_stream.csv")
df = df.drop(columns=["transaction_id"], errors="ignore")


def on_delivery(err, msg):
    if err is not None:
        print("Delivery failed:", err)


print("Producer started. Press Ctrl+C to stop.")
try:
    while True:
        row = json.loads(df.sample(1).iloc[0].to_json())
        row["transaction_id"] = str(uuid.uuid4())
        row["actual_is_fraud"] = row.pop("is_fraud")   # for evaluation only, never a model input
        row["timestamp"] = time.time()

        producer.produce(TOPIC, json.dumps(row).encode("utf-8"), callback=on_delivery)
        producer.poll(0)
        print("Sent:", row["transaction_id"][:8], "amount =", row["amount"])
        time.sleep(random.uniform(0.5, 1.5))
except KeyboardInterrupt:
    print("Stopping...")
finally:
    producer.flush()

