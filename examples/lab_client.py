"""Tiny stdlib-only client shared by the example scripts."""

import json
import os
import time
import urllib.error
import urllib.request

BASE = os.environ.get("LAB_BASE", "http://localhost:8000")

TERMINAL = {"done", "error", "canceled"}


def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise SystemExit(f"HTTP {e.code} {method} {path}: {e.read().decode()}")


def new_round():
    rnd = req("POST", "/rounds")
    print(f"round {rnd['id']} created (schema {rnd['schema']})")
    return rnd["id"]


def begin(rid, tx):
    req("POST", f"/rounds/{rid}/tx/{tx}/begin")


def sql(rid, tx, statement):
    rec = req("POST", f"/rounds/{rid}/tx/{tx}/sql", {"sql": statement})
    return rec["seq"]


def commit(rid, tx):
    return req("POST", f"/rounds/{rid}/tx/{tx}/commit")["seq"]


def rollback(rid, tx):
    return req("POST", f"/rounds/{rid}/tx/{tx}/rollback")["seq"]


def step(rid, seq):
    return req("GET", f"/rounds/{rid}/steps/{seq}")


def wait_step(rid, seq, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        rec = step(rid, seq)
        if rec["status"] in TERMINAL:
            return rec
        time.sleep(0.1)
    raise SystemExit(f"step {seq} did not finish within {timeout}s")


def wait_round(rid, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        rnd = req("GET", f"/rounds/{rid}")
        if rnd["state"] == "finished":
            return rnd
        time.sleep(0.2)
    raise SystemExit(f"round {rid} did not finish within {timeout}s")


def show(rec):
    line = f"  step {rec['seq']:>2} tx {rec['tx']} [{rec['kind']}] {rec['status']}"
    print(line)
    if rec.get("sql"):
        print(f"      sql: {rec['sql']}")
    res = rec.get("result")
    if res and "rows" in res:
        print(f"      columns: {res['columns']}")
        for row in res["rows"]:
            print(f"      row: {row}")
    elif res:
        print(f"      result: {res}")
    if rec.get("error"):
        print(f"      ERROR ({rec.get('error_class')}, pgcode={rec.get('pgcode')}):"
              f" {rec['error'].splitlines()[0]}")


def show_round(rid):
    rnd = req("GET", f"/rounds/{rid}")
    print(f"round {rid}: state={rnd['state']} reason={rnd['reason']} "
          f"txA={rnd['txs']['A']['state']} txB={rnd['txs']['B']['state']}")
    for rec in rnd["records"]:
        show(rec)
    return rnd
