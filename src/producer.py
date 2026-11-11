import os
import random
import time
import uuid
from pathlib import Path

import pandas as pd

from src.common import now, producer, send


def main():
    frame = pd.read_csv(os.getenv("DATA_CSV", "/app/data/demo.csv"))
    rows = frame.to_dict("records")
    fraud = frame[frame.target == 1].to_dict("records")
    client = producer()
    control = Path("/control/mode")
    interval = float(os.getenv("SEND_INTERVAL", "0.05"))
    while True:
        mode = control.read_text().strip() if control.exists() else "normal"
        if mode != "paused":
            data = dict(random.choice(fraud if mode == "fraud" else rows))
            data.pop("target", None)
            data["transaction_time"] = now()
            send(client, "transactions", {"transaction_id": str(uuid.uuid4()), "data": data})
        time.sleep(interval)


if __name__ == "__main__":
    main()
