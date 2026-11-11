import json
import os
import time
from collections import deque
from datetime import datetime
from pathlib import Path

from catboost import CatBoostClassifier
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from src.common import BUCKETS, FEATURES, consumer, messages, now, producer, send

PROCESSED = Counter("scorer_transactions_total", "Scored transactions")
FRAUD = Counter("scorer_fraud_total", "Transactions flagged as fraud")
LATENCY = Histogram("scorer_processing_seconds", "Scoring and delivery time")
SCORES = Histogram("scorer_score", "Predicted scores", buckets=BUCKETS)
RATIO = Gauge("scorer_fraud_ratio", "Fraud share in the last 1000 scores")
LAG = Histogram("scorer_event_lag_seconds", "Event age at scoring", buckets=(.01, .05, .1, .5, 1, 2, 5, 10, 30, 60))
LAST = Gauge("scorer_last_transaction_timestamp_seconds", "Last processed event")
RELOADS = Counter("scorer_model_reloads_total", "Hot model reloads", ["version"])
RELOADED_AT = Gauge("scorer_model_loaded_timestamp_seconds", "Model activation time", ["version"])
ERRORS = Counter("scorer_model_reload_errors_total", "Failed model reloads")


class Model:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.version = None
        self.model = None

    def reload(self):
        manifest = json.loads((self.directory / "latest.json").read_text())
        if manifest["version"] == self.version:
            return False
        model = CatBoostClassifier()
        model.load_model(str(self.directory / manifest["file"]))
        model.predict_proba([[1.0, "CA", "test", "test"]], thread_count=1)
        previous = self.version
        self.model, self.version = model, manifest["version"]
        if previous is not None:
            RELOADS.labels(self.version).inc()
            RELOADED_AT.labels(self.version).set(time.time())
        print(f"model={self.version}", flush=True)
        return True

    def score(self, data):
        values = [float(data["amount"])] + [str(data[k]) for k in FEATURES[1:]]
        return float(self.model.predict_proba([values], thread_count=1)[0, 1])


def main():
    model = Model(os.getenv("MODELS_DIR", "/models"))
    model.reload()
    LAST.set(time.time())
    start_http_server(8000)
    source, sink = consumer("scorer", "transactions"), producer()
    recent = deque(maxlen=1000)
    checked_at = 0.0
    try:
        for message in messages(source):
            if time.monotonic() - checked_at >= 1:
                try:
                    model.reload()
                except Exception as error:
                    ERRORS.inc()
                    print(f"reload failed: {error}", flush=True)
                checked_at = time.monotonic()
            if message is None:
                continue
            started = time.perf_counter()
            event = json.loads(message.value())
            data = event["data"]
            lag = max(0, time.time() - datetime.fromisoformat(data["transaction_time"]).timestamp())
            score = model.score(data)
            flag = int(score > 0.5)
            result = {k: data[k] for k in FEATURES}
            result.update(
                transaction_id=event["transaction_id"], score=score, fraud_flag=flag,
                model_version=model.version, transaction_time=data["transaction_time"], scored_at=now(),
            )
            send(sink, "scoring", result)
            source.commit(message=message, asynchronous=False)
            PROCESSED.inc()
            FRAUD.inc(flag)
            SCORES.observe(score)
            LATENCY.observe(time.perf_counter() - started)
            LAG.observe(lag)
            recent.append(flag)
            RATIO.set(sum(recent) / len(recent))
            LAST.set(time.time())
    finally:
        source.close()


if __name__ == "__main__":
    main()
