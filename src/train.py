import argparse
import json
import os
import uuid
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier

from src.common import FEATURES, now

MODELS = Path(os.getenv("MODELS_DIR", "/models"))


def train(only_if_missing=False):
    MODELS.mkdir(parents=True, exist_ok=True)
    if only_if_missing and (MODELS / "latest.json").exists():
        return
    frame = pd.read_csv(os.getenv("DATA_CSV", "/app/data/demo.csv"))
    model = CatBoostClassifier(
        iterations=100, depth=5, learning_rate=0.1, loss_function="Logloss",
        random_seed=42, thread_count=2, verbose=False, allow_writing_files=False,
    )
    model.fit(frame[FEATURES], frame["target"], cat_features=FEATURES[1:])
    version = uuid.uuid4().hex[:12]
    model.save_model(str(MODELS / f"{version}.cbm"))
    manifest = {"version": version, "file": f"{version}.cbm", "trained_at": now()}
    temporary = MODELS / f"{version}.json.tmp"
    temporary.write_text(json.dumps(manifest))
    temporary.replace(MODELS / "latest.json")
    print(json.dumps(manifest), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--if-missing", action="store_true")
    train(parser.parse_args().if_missing)
