#!/usr/bin/env python3
"""Fork-from-active-round self-test.

Covers the key scenarios:

1. fork copies the committed backfill of both tables (same snapshot)
2. the new round keeps both key constraints, an empty step record and fresh
   transaction paths; the source round can still commit / roll back
3. the parallel-round cap is shared between baseline rounds and forks
4. the source schema is cleaned up before the fork -> the reason is reported,
   no residual data, the slot is returned
5. the copy fails after the target schema was created -> the reason is
   reported, the partial schema is dropped, the slot is returned
6. a half-applied source transaction (uncommitted) is never visible in a fork

Run against a live API + PostgreSQL:
    python3 tests/fork_scenarios.py
"""

import os
import sys
import time

import httpx
import psycopg2

BASE = os.environ.get("LAB_BASE", "http://localhost:8000")
DSN = os.environ.get("DATABASE_URL",
                     "postgresql://postgres:postgres@127.0.0.1:5432/postgres")
TERMINAL = {"done", "error", "canceled"}

failures = []


def check(cond, label):
    print(("PASS  " if cond else "FAIL  ") + label)
    if not cond:
        failures.append(label)


def req(method, path, body=None, expect=None):
    r = httpx.request(method, BASE + path, json=body, timeout=30)
    if expect is not None:
        check(r.status_code == expect,
              f"{method} {path} -> {r.status_code} (expected {expect}): "
              f"{r.text[:300]}")
    try:
        return r.json()
    except ValueError:
        return {"_text": r.text}


def wait_step(rid, seq, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        rec = req("GET", f"/rounds/{rid}/steps/{seq}")
        if rec["status"] in TERMINAL:
            return rec
        time.sleep(0.1)
    raise AssertionError(f"step {seq} in round {rid} did not finish")


def sql(rid, tx, statement):
    rec = req("POST", f"/rounds/{rid}/tx/{tx}/sql", {"sql": statement}, 202)
    return wait_step(rid, rec["seq"])


def begin(rid, tx):
    req("POST", f"/rounds/{rid}/tx/{tx}/begin", expect=200)


def finish(rid):
    req("POST", f"/rounds/{rid}/finish")
    deadline = time.time() + 20
    while time.time() < deadline:
        rnd = req("GET", f"/rounds/{rid}")
        if rnd["state"] == "finished":
            return rnd
        time.sleep(0.2)
    raise AssertionError(f"round {rid} did not finish")


def lab_schemas():
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT schema_name FROM information_schema.schemata "
                        "WHERE schema_name LIKE 'r\\_%'")
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


print("=== 1/2: fork a committed backfill; constraints; source still usable ===")
# start from a clean slate: finish any rounds left over by an earlier run
for old in req("GET", "/rounds"):
    if old["state"] != "finished":
        finish(old["id"])
src = req("POST", "/rounds", expect=201)
src_id = src["id"]
begin(src_id, "A")
check(sql(src_id, "A", "INSERT INTO batches VALUES (3, true)")["status"] == "done",
      "backfill: batch inserted")
check(sql(src_id, "A",
          "INSERT INTO readings VALUES (10, 3, 12.1), (11, 3, 12.9)")["status"]
      == "done", "backfill: readings inserted")
check(sql(src_id, "A", "UPDATE batches SET ready = true WHERE id = 1")["status"]
      == "done", "backfill: extra update")
req("POST", f"/rounds/{src_id}/tx/A/commit", expect=202)
wait_step(src_id, 4)
check(req("GET", f"/rounds/{src_id}")["txs"]["A"]["state"] == "committed",
      "source tx A committed")

fork = req("POST", f"/rounds/{src_id}/fork", expect=201)
fork_id = fork["id"]
check(fork["source_round_id"] == src_id, "fork records its source round")
check(fork["copied"] == {"batches": 3, "readings": 5},
      f"fork copied both tables completely: {fork['copied']}")
check(fork["state"] == "active", "fork is active")
check(req("GET", f"/rounds/{fork_id}")["records"] == [],
      "fork step record starts empty")
check(fork["txs"]["A"]["state"] == "not_started"
      and fork["txs"]["B"]["state"] == "not_started",
      "fork transaction paths start fresh")
check(fork["schema"] != src["schema"], "fork has its own schema")

begin(fork_id, "A")
r = sql(fork_id, "A", "SELECT * FROM readings ORDER BY id")
check(r["status"] == "done"
      and [row[0] for row in r["result"]["rows"]] == [1, 2, 3, 10, 11],
      "fork sees the committed backfill in readings")
r = sql(fork_id, "A", "INSERT INTO batches VALUES (3, false)")
check(r["status"] == "error" and r["pgcode"] == "23505",
      "fork keeps the batches PRIMARY KEY")
# A's transaction is now aborted; use the other path for the FK check
begin(fork_id, "B")
r = sql(fork_id, "B", "INSERT INTO readings VALUES (12, 99, 1.0)")
check(r["status"] == "error" and r["pgcode"] == "23503",
      "fork keeps the readings -> batches foreign key")
req("POST", f"/rounds/{fork_id}/tx/A/rollback", expect=202)
req("POST", f"/rounds/{fork_id}/tx/B/rollback", expect=202)

# source remains fully usable after the fork
begin(src_id, "B")
check(sql(src_id, "B", "UPDATE batches SET ready = false WHERE id = 2")["status"]
      == "done", "source round still accepts new work after the fork")
req("POST", f"/rounds/{src_id}/tx/B/rollback", expect=202)
check(req("GET", f"/rounds/{src_id}")["state"] == "active",
      "source round is still active after the fork")

print("\n=== 3: parallel-round cap shared with baseline rounds ===")
third = req("POST", "/rounds", expect=409)
check("parallel" in third.get("detail", "").lower(),
      f"third round refused: {third.get('detail')}")
fork_again = req("POST", f"/rounds/{fork_id}/fork", expect=409)
check("parallel" in fork_again.get("detail", "").lower(),
      f"fork also counts against the cap: {fork_again.get('detail')}")

missing = req("POST", "/rounds/does-not-exist/fork", expect=404)
check("not found" in missing.get("detail", "").lower(),
      "forking an unknown round is 404")

# a source that is stopping or already cleaned up is refused
req("POST", f"/rounds/{src_id}/stop", expect=200)
stopping = req("POST", f"/rounds/{src_id}/fork", expect=409)
check("only an active round" in stopping.get("detail", "").lower(),
      f"stopping/finished source refused with reason: {stopping.get('detail')}")

finish(fork_id)
finish(src_id)
check(not lab_schemas(), f"both schemas cleaned up: {lab_schemas()}")

print("\n=== 4/5/6: in-process failure-injection scenarios ===")
from app.core import ForkFailed, RoundManager, SourceRoundGone  # noqa: E402

mgr = RoundManager(DSN, max_rounds=2)
created = []


def cleanup():
    for rnd in mgr.list_rounds():
        try:
            mgr.drop_schema(rnd.schema)
        except Exception:
            pass


# 4: source schema cleaned up before its snapshot could be read
base = mgr.create_round()
created.append(base.id)
mgr.drop_schema(base.schema)  # simulate the source's cleanup winning the race
try:
    mgr.fork_round(base.id)
except SourceRoundGone as exc:
    check(True, "cleaned-up source raises SourceRoundGone")
    check("cleaned up" in str(exc) and "no new round" in str(exc),
          f"reason names the cleanup: {exc}")
else:
    check(False, "cleaned-up source raises SourceRoundGone")
check(mgr._pending == 0, "slot returned after the source disappeared")
check(mgr.get_round(base.id).state == "active"  # record untouched
      and base.schema not in lab_schemas(),
      "no residual schema after the source disappeared")
# slot really free -> a new round can be created even though two rounds are
# registered (one active, one pending of the failed fork is gone)
try:
    r = mgr.create_round()
    created.append(r.id)
    check(True, "slot is usable after the source disappeared")
except Exception as exc:
    check(False, f"slot usable after the source disappeared: {exc}")

cleanup()
mgr.rounds.clear()

# 5: copy fails *after* the target schema was created
failsrc = mgr.create_round()
orig_copy = mgr._copy_snapshot


def broken_copy(source_schema, target_schema):
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{target_schema}"')
    finally:
        conn.close()
    raise psycopg2.OperationalError("injected copy failure")


mgr._copy_snapshot = broken_copy
before = lab_schemas()
try:
    mgr.fork_round(failsrc.id)
except ForkFailed as exc:
    check(True, "copy failure raises ForkFailed")
    check("injected copy failure" in str(exc)
          and "removed" in str(exc) and "released" in str(exc),
          f"reason explains failure + cleanup + slot return: {exc}")
else:
    check(False, "copy failure raises ForkFailed")
finally:
    mgr._copy_snapshot = orig_copy
check(mgr._pending == 0, "slot returned after the copy failure")
check(lab_schemas() == {failsrc.schema},
      f"partial data dropped, only the source schema remains: {lab_schemas()}")

# 6: a half-applied (uncommitted) source transaction is never visible
failsrc_tx = mgr.get_round(failsrc.id)
failsrc_tx.begin_tx("A")
failsrc_tx.submit_step("A", "sql", "INSERT INTO batches VALUES (7, true)")
time.sleep(0.3)  # let the INSERT run, but do NOT commit it
with failsrc_tx.lock:
    pending = [r for r in failsrc_tx.records if r["status"] == "done"]
check(len(pending) == 1, "source has uncommitted half-applied data")
mid = mgr.fork_round(failsrc.id)
created.append(mid.id)
check(mid.copied == {"batches": 2, "readings": 3},
      f"fork sees only the committed snapshot, not the half-applied batch: "
      f"{mid.copied}")
failsrc_tx.submit_step("A", "rollback")
time.sleep(0.3)
check(mgr.get_round(failsrc.id).state == "active",
      "source round may still roll back after the fork")

# a fork itself can be forked (the data chain keeps working).  The slot cap
# still applies, so wind the first source down first.
failsrc_tx.stop("completed")
deadline = time.time() + 20
while mgr.get_round(failsrc.id).state != "finished":
    check_deadline = time.time() < deadline
    if not check_deadline:
        raise AssertionError("source round did not finish")
    time.sleep(0.1)
chain = mgr.fork_round(mid.id)
created.append(chain.id)
check(chain.source_round_id == mid.id and chain.copied == mid.copied,
      f"a forked round can be forked again: {chain.copied}")
check(mgr._pending == 0, "no slot leaked across the whole chain")

# 7: the source schema still exists but one of its tables is gone -> the copy
#    fails, partial data is dropped and the slot is returned
chain.stop("completed")
mid.stop("completed")
for rnd in (chain, mid):
    deadline = time.time() + 20
    while mgr.get_round(rnd.id).state != "finished":
        if time.time() > deadline:
            raise AssertionError(f"round {rnd.id} did not finish")
        time.sleep(0.1)
broken = mgr.create_round()
created.append(broken.id)
broken.begin_tx("A")
broken.submit_step("A", "sql", "DROP TABLE readings")
time.sleep(0.3)
broken.submit_step("A", "commit")
time.sleep(0.3)
before = lab_schemas()
try:
    mgr.fork_round(broken.id)
except ForkFailed as exc:
    check(True, "missing source table raises ForkFailed")
    check("42P01" in str(exc) and "removed" in str(exc)
          and "released" in str(exc),
          f"reason names the DB error + cleanup: {exc}")
else:
    check(False, "missing source table raises ForkFailed")
check(lab_schemas() == before,
      f"no residual schema when a source table is missing: {lab_schemas()}")
try:
    spare = mgr.create_round()
    created.append(spare.id)
    check(True, "slot usable after the failed copy of a broken source")
except Exception as exc:
    check(False, f"slot usable after the failed copy: {exc}")
broken.stop("completed")
deadline = time.time() + 20
while mgr.get_round(broken.id).state != "finished":
    if time.time() > deadline:
        raise AssertionError("broken source round did not finish")
    time.sleep(0.1)

cleanup()
mgr.rounds.clear()

print()
if failures:
    print(f"{len(failures)} FAILURE(S):")
    for f in failures:
        print(" -", f)
    sys.exit(1)
print("all fork scenarios passed")
