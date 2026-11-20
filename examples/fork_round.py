#!/usr/bin/env python3
"""Fork a backfill experiment into a new round.

The engineer appends a correction batch in round 1 and commits it.  A fork
of round 1 opens round 2 with `batches` and `readings` copied from that
same committed snapshot - so the corrected load SQL can be verified against
the recorded data.  The source round stays usable (here it rolls back a
later attempt and finishes normally), and the fork starts with an empty
step record on fresh A/B transaction paths.

Run:  python3 examples/fork_round.py
"""

from lab_client import (begin, commit, new_round, req, rollback, show_round,
                        sql, wait_round, wait_step)

print("=== round 1: engineer appends a correction batch and commits it ===")
rid = new_round()
begin(rid, "A")
wait_step(rid, sql(rid, "A", "INSERT INTO batches VALUES (3, false)"))
wait_step(rid, sql(rid, "A",
                   "INSERT INTO readings VALUES (10, 3, 12.1), (11, 3, 12.9)"))
wait_step(rid, sql(rid, "A", "UPDATE batches SET ready = true WHERE id = 3"))
wait_step(rid, commit(rid, "A"))
print(f"round {rid}: committed batch 3 with 2 readings")

print("\n=== fork: new round from the same committed snapshot ===")
fork = req("POST", f"/rounds/{rid}/fork")
fid = fork["id"]
print(f"round {fid} created from {fork['source_round_id']}: "
      f"copied {fork['copied']}")

begin(fid, "A")
wait_step(fid, sql(fid, "A", "SELECT * FROM batches ORDER BY id"))
wait_step(fid, sql(fid, "A", "SELECT * FROM readings ORDER BY id"))
wait_step(fid, commit(fid, "A"))

print("\n=== source round is still usable after the fork ===")
begin(rid, "B")
wait_step(rid, sql(rid, "B", "DELETE FROM readings WHERE id = 10"))
wait_step(rid, rollback(rid, "B"))
req("POST", f"/rounds/{rid}/finish")
wait_round(rid)
show_round(rid)

req("POST", f"/rounds/{fid}/finish")
wait_round(fid)
show_round(fid)
