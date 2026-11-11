import json
import time

import psycopg
from prometheus_client import Counter, Histogram, start_http_server

from src.common import BUCKETS, DB, consumer, messages

WRITTEN = Counter("writer_scores_total", "Inserted scores")
FRAUD = Counter("writer_fraud_total", "Inserted fraud flags")
LATENCY = Histogram("writer_insert_seconds", "PostgreSQL commit time")
SCORES = Histogram("writer_score", "Inserted scores", buckets=BUCKETS)
DUPLICATES = Counter("writer_duplicates_total", "Repeated transaction IDs")


def insert(connection, result):
    with connection.transaction():
        row = connection.execute("""
            INSERT INTO scores
                (transaction_id, score, fraud_flag, model_version, amount, us_state,
                 merch, cat_id, transaction_time, scored_at)
            VALUES (%(transaction_id)s, %(score)s, %(fraud_flag)s, %(model_version)s,
                    %(amount)s, %(us_state)s, %(merch)s, %(cat_id)s,
                    %(transaction_time)s, %(scored_at)s)
            ON CONFLICT (transaction_id) DO NOTHING RETURNING transaction_id
        """, result).fetchone()
    return row is not None


def main():
    start_http_server(8001)
    source = consumer("writer", "scoring")
    try:
        with psycopg.connect(DB, autocommit=True) as connection:
            for message in messages(source):
                if message is None:
                    continue
                result = json.loads(message.value())
                started = time.perf_counter()
                inserted = insert(connection, result)
                elapsed = time.perf_counter() - started
                source.commit(message=message, asynchronous=False)
                if inserted:
                    WRITTEN.inc()
                    FRAUD.inc(result["fraud_flag"])
                    LATENCY.observe(elapsed)
                    SCORES.observe(result["score"])
                else:
                    DUPLICATES.inc()
    finally:
        source.close()


if __name__ == "__main__":
    main()
