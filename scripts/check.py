import argparse
import base64
import json
import subprocess
import time
from http.client import HTTPException
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
AUTH = "Basic " + base64.b64encode(b"admin:admin").decode()


def request(port, path, data=None):
    headers = {"Authorization": AUTH, "Content-Type": "application/json"}
    body = json.dumps(data).encode() if data is not None else None
    with urlopen(Request(f"http://localhost:{port}{path}", body, headers), timeout=15) as response:
        return json.load(response)


def compose(*args, input=None):
    return subprocess.check_output(
        ["docker", "compose", *args], cwd=ROOT, input=input, text=True,
    ).strip()


def wait_for(fn, timeout=90):
    deadline = time.monotonic() + timeout
    error = None
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result:
                return result
        except (URLError, ConnectionError, HTTPException) as caught:
            error = caught
        time.sleep(2)
    raise AssertionError(f"Timed out: {fn}; last error: {error}")


def prom(expr):
    response = request(9090, "/api/v1/query?" + urlencode({"query": expr}))
    assert response["status"] == "success", response
    return response["data"]["result"]


def number(expr):
    values = prom(expr)
    return float(values[0]["value"][1]) if values else 0


def sql(query, format="table"):
    response = request(3000, "/api/ds/query", {
        "from": str(int((time.time() - 900) * 1000)), "to": str(int(time.time() * 1000)),
        "queries": [{"refId": "A", "datasource": {"uid": "postgres", "type": "grafana-postgresql-datasource"},
                     "rawSql": query, "format": format, "intervalMs": 1000, "maxDataPoints": 1000}],
    })["results"]["A"]
    assert "error" not in response, response
    frames = response["frames"]
    return frames[0]["data"]["values"] if frames else []


def quoted(values):
    return ",".join("'" + value.replace("'", "''") + "'" for value in values)


def mode(value):
    compose("exec", "-T", "producer", "python", "-c",
            f"from pathlib import Path; Path('/control/mode').write_text({value!r})")


def dashboards():
    states = sql("SELECT DISTINCT us_state FROM scores")[0]
    merchants = sql("SELECT DISTINCT merch FROM scores")[0]
    checked = 0
    for path in sorted((ROOT / "grafana/dashboards").glob("*.json")):
        dashboard = request(3000, f"/api/dashboards/uid/{path.stem}")["dashboard"]
        for panel in dashboard["panels"]:
            for target in panel["targets"]:
                if "expr" in target:
                    values = prom(target["expr"])
                    assert values, panel["title"]
                    assert all(v["value"][1] not in ("NaN", "+Inf", "-Inf") for v in values), panel["title"]
                else:
                    query = target["rawSql"].replace("${us_state:sqlstring}", quoted(states)).replace("${merch:sqlstring}", quoted(merchants))
                    values = sql(query, target["format"])
                    assert values and values[0], panel["title"]
                checked += 1
    business = request(3000, "/api/dashboards/uid/business")["dashboard"]
    state, merchant = sql("SELECT us_state, merch FROM scores GROUP BY 1, 2 ORDER BY count(*) DESC LIMIT 1")
    for panel in business["panels"][:2]:
        target = panel["targets"][0]
        query = target["rawSql"].replace("${us_state:sqlstring}", quoted(state)).replace("${merch:sqlstring}", quoted(merchant))
        values = sql(query, target["format"])
        assert values and values[0], panel["title"]
        if panel["id"] == 1:
            assert abs(sum(values[1]) * 0.05 - 1) < 1e-6, values
    return checked


def alert_firing(title):
    groups = request(3000, "/api/prometheus/grafana/api/v1/rules")["data"]["groups"]
    return any(rule["name"] == title and rule["state"] == "firing" for group in groups for rule in group["rules"])


def main(alerts):
    mode("normal")
    wait_for(lambda: request(3000, "/api/health")["database"] == "ok")
    wait_for(lambda: number('count(up{job=~"scorer|writer|node"} == 1)') == 3)
    wait_for(lambda: number("writer_scores_total") > 50)
    before = number("scorer_transactions_total")
    wait_for(lambda: number("scorer_transactions_total") > before + 30)
    assert number("writer_scores_total") > 0
    assert sql("SELECT count(*) FROM scores WHERE score < 0 OR score > 1 OR fraud_flag <> (score > 0.5)::int")[0][0] == 0
    for uid in ("prometheus", "postgres"):
        assert request(3000, f"/api/datasources/uid/{uid}/health")["status"] == "OK"
    assert len(request(3000, "/api/v1/provisioning/alert-rules")) == 2
    panels = dashboards()
    print(f"OK: 3 targets, 2 datasources, 4 dashboards, {panels} queries", flush=True)

    container = compose("ps", "-q", "scorer")
    inspect = ["docker", "inspect", "--format", "{{.State.StartedAt}}", container]
    started_at = subprocess.check_output(inspect, text=True)
    manifest = json.loads(compose("run", "--rm", "trainer", "python", "-m", "src.train").splitlines()[-1])
    version = manifest["version"]
    wait_for(lambda: number(f'scorer_model_reloads_total{{version="{version}"}}') == 1)
    wait_for(lambda: sql(f"SELECT count(*) FROM scores WHERE model_version = '{version}'")[0][0] > 0)
    annotation = request(3000, "/api/dashboards/uid/scoring")["dashboard"]["annotations"]["list"][0]
    assert any(v["metric"]["version"] == version for v in prom(annotation["expr"]))
    assert compose("ps", "-q", "scorer") == container
    assert subprocess.check_output(inspect, text=True) == started_at
    print(f"OK: hot reload {version}, annotation, same container", flush=True)

    row = sql("SELECT row_to_json(t)::text FROM (SELECT * FROM scores ORDER BY id LIMIT 1) t")[0][0]
    duplicates = number("writer_duplicates_total")
    compose("exec", "-T", "producer", "python", "-c",
            "import json,sys; from src.common import producer,send; send(producer(),'scoring',json.load(sys.stdin))", input=row)
    wait_for(lambda: number("writer_duplicates_total") > duplicates)
    assert sql("SELECT count(*) - count(DISTINCT transaction_id) FROM scores")[0][0] == 0
    print("OK: replay does not duplicate a database row", flush=True)

    if alerts:
        try:
            mode("fraud")
            wait_for(lambda: alert_firing("Доля фрода выше 30%"), 150)
            print("OK: high-fraud firing", flush=True)
            mode("paused")
            wait_for(lambda: alert_firing("Нет транзакций 20 секунд"), 100)
            print("OK: stream-stopped firing", flush=True)
        finally:
            mode("normal")
        wait_for(lambda: not alert_firing("Доля фрода выше 30%") and not alert_firing("Нет транзакций 20 секунд"), 150)
        print("OK: alerts recovered", flush=True)
    summary = {"checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "targets": 3,
               "dashboards": 4, "queries": panels, "hot_reload": version, "deduplication": True,
               "alerts_tested": alerts, "database_rows": sql("SELECT count(*) FROM scores")[0][0]}
    (ROOT / "results/check.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--alerts", action="store_true")
    main(parser.parse_args().alerts)
