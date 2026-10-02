import json

import joblib
import pandas as pd
import psycopg2
import redis
from confluent_kafka import Consumer

# ---------- setup ----------
with open("config.json") as f:
    cfg = json.load(f)

model = joblib.load("saved_models/fraud_model.joblib")
FEATURES = ["amount", "transaction_hour", "merchant_category", "location",
            "device_type", "transaction_frequency", "previous_fraud_count"]

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

conn = psycopg2.connect(host="localhost", port=5432, dbname="fraud_db",
                        user="fraud", password="fraud123")
conn.autocommit = True
cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS predictions (
        id SERIAL PRIMARY KEY,
        transaction_id TEXT,
        amount DOUBLE PRECISION,
        transaction_hour INT,
        merchant_category TEXT,
        location TEXT,
        device_type TEXT,
        transaction_frequency DOUBLE PRECISION,
        previous_fraud_count DOUBLE PRECISION,
        risk_score DOUBLE PRECISION,
        decision TEXT,
        actual_is_fraud INT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
""")

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "fraud-scorer",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["transactions"])


def decide(score):
    if score >= cfg["block_at_or_above"]:
        return "BLOCK"
    if score >= cfg["allow_below"]:
        return "ALERT"
    return "ALLOW"


# ---------- main loop ----------
print("Consumer started. Press Ctrl+C to stop.")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Kafka error:", msg.error())
            continue

        tx = json.loads(msg.value().decode("utf-8"))
        X = pd.DataFrame([tx])[FEATURES]
        score = float(model.predict_proba(X)[:, 1][0])
        decision = decide(score)

        cur.execute(
            """INSERT INTO predictions
               (transaction_id, amount, transaction_hour, merchant_category, location,
                device_type, transaction_frequency, previous_fraud_count,
                risk_score, decision, actual_is_fraud)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (tx["transaction_id"], tx["amount"], tx["transaction_hour"],
             tx["merchant_category"], tx["location"], tx["device_type"],
             tx["transaction_frequency"], tx["previous_fraud_count"],
             score, decision, tx.get("actual_is_fraud")),
        )

        # Redis: fast counters + latest alerts for the dashboard
        r.incr("stats:total")
        r.incr(f"stats:{decision}")
        if decision != "ALLOW":
            r.lpush("recent_alerts", json.dumps({
                "id": tx["transaction_id"][:8], "amount": tx["amount"],
                "score": round(score, 3), "decision": decision}))
            r.ltrim("recent_alerts", 0, 49)

        print(f"{tx['transaction_id'][:8]}  amount={tx['amount']:<9} "
              f"score={score:.3f}  -> {decision}")
except KeyboardInterrupt:
    print("Stopping...")
finally:
    consumer.close()
    conn.close()