#!/usr/bin/env python3
"""Half-batch read: the loader marks the batch ready before its details
are committed, so the reader sees a ready batch with zero readings.

Round 1 reproduces the anomaly (bad load boundary: `ready=true` committed
before the readings).  Round 2 shows the fixed boundary: batch row,
readings and `ready=true` in ONE commit - the reader then sees either
nothing or the complete batch, never half of it.

Run:  python3 examples/half_batch.py
"""

from lab_client import (begin, commit, new_round, req, show_round, sql,
                        wait_round, wait_step)

print("=== round 1: bad boundary -> half-batch read ===")
rid = new_round()
begin(rid, "A")  # loader
begin(rid, "B")  # reader

# Loader commits the batch master row marked ready BEFORE the details.
wait_step(rid, sql(rid, "A", "INSERT INTO batches VALUES (3, true)"))
wait_step(rid, commit(rid, "A"))

# Reader runs at READ COMMITTED: it sees batch 3 as ready ...
wait_step(rid, sql(rid, "B", "SELECT * FROM batches WHERE ready = true ORDER BY id"))
# ... but the readings are not there yet -> HALF BATCH.
wait_step(rid, sql(rid, "B", "SELECT * FROM readings WHERE batch_id = 3 ORDER BY id"))
wait_step(rid, commit(rid, "B"))

req("POST", f"/rounds/{rid}/finish")
wait_round(rid)
show_round(rid)

print()
print("=== round 2: fixed boundary -> all-or-nothing ===")
rid = new_round()
begin(rid, "A")
begin(rid, "B")

# Loader: batch row (not ready) + readings + ready flag in ONE transaction.
wait_step(rid, sql(rid, "A", "INSERT INTO batches VALUES (3, false)"))
wait_step(rid, sql(rid, "A",
                   "INSERT INTO readings VALUES (10, 3, 12.1), (11, 3, 12.9)"))
wait_step(rid, sql(rid, "A", "UPDATE batches SET ready = true WHERE id = 3"))
wait_step(rid, commit(rid, "A"))

# Reader now sees the batch only together with its complete details.
wait_step(rid, sql(rid, "B", "SELECT * FROM batches WHERE ready = true ORDER BY id"))
wait_step(rid, sql(rid, "B", "SELECT * FROM readings WHERE batch_id = 3 ORDER BY id"))
wait_step(rid, commit(rid, "B"))

req("POST", f"/rounds/{rid}/finish")
wait_round(rid)
show_round(rid)
