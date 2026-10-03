import json

import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

KAFKA = "kafka:29092"
IN_TOPIC = "transactions"
OUT_TOPIC = "enriched_transactions"
WINDOW_SECONDS = 300

spark = (SparkSession.builder.appName("fraud-stream")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA)
       .option("subscribe", IN_TOPIC)
       .option("startingOffsets", "latest")
       .load()
       .select(col("value").cast("string").alias("json")))


def enrich_batch(batch_df, batch_id):
    rows = [row["json"] for row in batch_df.collect()]
    if not rows:
        return

    r = redis.Redis(host="redis", port=6379, decode_responses=True)
    txs = sorted((json.loads(x) for x in rows), key=lambda t: t["timestamp"])
    out = []

    for tx in txs:
        key = f"user:{tx['user_id']}:window"
        ts = tx["timestamp"]

        r.zremrangebyscore(key, 0, ts - WINDOW_SECONDS)
        history = [json.loads(m) for m in r.zrange(key, 0, -1)]
        amounts = [h["amount"] for h in history]
        locations = {h["location"] for h in history}

        tx["tx_count_5m"] = len(history)
        tx["avg_amount_5m"] = round(sum(amounts) / len(amounts), 2) if amounts else 0.0
        tx["distinct_locations_5m"] = len(locations | {tx["location"]})
        tx["secs_since_last_tx"] = (
            round(ts - max(h["ts"] for h in history), 2) if history else -1.0
        )

        member = json.dumps({"id": tx["transaction_id"], "amount": tx["amount"],
                             "location": tx["location"], "ts": ts})
        r.zadd(key, {member: ts})
        r.expire(key, WINDOW_SECONDS * 2)
        out.append((json.dumps(tx),))

    (spark.createDataFrame(out, ["value"]).write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("topic", OUT_TOPIC)
        .save())
    print(f"batch {batch_id}: enriched {len(out)} transactions", flush=True)


query = (raw.writeStream.foreachBatch(enrich_batch)
         .option("checkpointLocation", "/tmp/spark-checkpoint")
         .trigger(processingTime="2 seconds")
         .start())
query.awaitTermination()
