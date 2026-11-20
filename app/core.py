"""Core engine for the half-batch transaction lab.

A *round* is an isolated experiment: it gets its own PostgreSQL schema
(``r_<round_id>``) preloaded with the baseline ``batches`` / ``readings``
tables, and two independent transaction paths (``A`` and ``B``).  Each
transaction path is served by exactly one dedicated worker thread holding
exactly one dedicated connection for the whole life of the transaction.
If a statement fails or deadlocks, the error is reported on that very
connection/transaction - the lab never swaps in a fresh connection to
impersonate the original transaction.

Records are append-only.  Once a step record reaches a terminal state
(``done`` / ``error`` / ``canceled``) it is never rewritten, so late
results arriving after a stop cannot falsify the log.
"""

from __future__ import annotations

import queue
import threading
import time
import uuid
from datetime import datetime, timezone

import psycopg2
import psycopg2.extensions

TERMINAL_STEP_STATES = frozenset({"done", "error", "canceled"})
# tx states in which the path no longer holds resources that must be
# awaited before the round may report its final state
IDLE_TX_STATES = frozenset({"not_started", "committed", "rolled_back", "dead", "closed"})
MAX_ROWS = 500  # per-SELECT row cap kept in the record

BASELINE_BATCHES = [(1, True), (2, False)]
BASELINE_READINGS = [(1, 1, 10.5), (2, 1, 11.0), (3, 1, 9.8)]

_SHUTDOWN = object()  # queue sentinel telling a worker to wind down


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def jsonable(value):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


class BusyError(Exception):
    """Raised when the maximum number of parallel rounds is reached."""


class RoundStateError(Exception):
    """Raised when an operation is not allowed in the current round/tx state."""


class TxWorker(threading.Thread):
    """One worker thread == one connection == one transaction path."""

    def __init__(self, rnd: "Round", tx_name: str, dsn: str):
        super().__init__(daemon=True, name=f"tx-{rnd.id}-{tx_name}")
        self.rnd = rnd
        self.tx_name = tx_name
        self.dsn = dsn
        self.q: "queue.Queue" = queue.Queue()
        self.conn = None
        self.backend_pid = None
        self.running = False  # True while a statement executes on the conn
        self.stop_requested = threading.Event()
        self.ready_event = threading.Event()
        self.connect_error = None

    # ---- called from API threads ----
    def submit(self, item: dict) -> None:
        self.q.put(item)

    def request_stop(self) -> None:
        """Ask the worker to finish promptly: cancel any in-flight statement,
        discard queued steps, roll back and release the connection."""
        self.stop_requested.set()
        conn = self.conn
        if conn is not None and self.running and not conn.closed:
            try:
                conn.cancel()  # safe to call from another thread
            except Exception:
                pass
        self.q.put(_SHUTDOWN)

    # ---- worker thread body ----
    def run(self) -> None:
        try:
            self.conn = psycopg2.connect(self.dsn)
            self.conn.autocommit = False
            self.backend_pid = self.conn.get_backend_pid()
            with self.conn.cursor() as cur:
                cur.execute(f'SET search_path TO "{self.rnd.schema}"')
            self.conn.commit()
            self.rnd.tx_ready(self.tx_name)
        except Exception as exc:  # connection could not be established
            self.connect_error = exc
            self.rnd.tx_connect_failed(self.tx_name, exc)
            return
        try:
            while True:
                item = self.q.get()
                if item is _SHUTDOWN:
                    break
                if self.stop_requested.is_set():
                    # stopped before this step started: never execute it
                    self.rnd.finalize_step(
                        item["rec"], "canceled",
                        error="round stopped before this step started",
                    )
                    continue
                self._run_item(item)
                if self.conn is None or self.conn.closed:
                    # Connection is gone.  We must NOT reconnect to impersonate
                    # the original transaction; drain what is left as canceled.
                    self.rnd.tx_connection_lost(self.tx_name)
                    self._drain("connection lost; step not executed")
                    break
        finally:
            self._cleanup()

    def _drain(self, message: str) -> None:
        while True:
            try:
                pending = self.q.get_nowait()
            except queue.Empty:
                return
            if pending is _SHUTDOWN:
                return
            self.rnd.finalize_step(pending["rec"], "canceled", error=message)

    def _run_item(self, item: dict) -> None:
        rec, kind, sql = item["rec"], item["kind"], item.get("sql")
        self.rnd.mark_running(rec)
        self.running = True
        try:
            if kind == "sql":
                self.rnd.finalize_step(rec, "done", result=self._exec_sql(sql))
            elif kind == "commit":
                self.conn.commit()
                self.rnd.finalize_step(rec, "done", result={"outcome": "committed"})
                self.rnd.tx_finished(self.tx_name, "committed")
            elif kind == "rollback":
                self.conn.rollback()
                self.rnd.finalize_step(rec, "done", result={"outcome": "rolled_back"})
                self.rnd.tx_finished(self.tx_name, "rolled_back")
        except psycopg2.Error as exc:
            # Raw database error, kept verbatim for the record.  The
            # transaction (if any) is now aborted on THIS connection; it is
            # up to the user to roll it back - we never silently reconnect.
            self.rnd.finalize_step(
                rec, "error",
                error=str(exc).strip(),
                error_class=type(exc).__name__,
                pgcode=getattr(exc, "pgcode", None),
            )
        except Exception as exc:  # e.g. InterfaceError on a broken connection
            self.rnd.finalize_step(
                rec, "error", error=str(exc), error_class=type(exc).__name__,
            )
        finally:
            self.running = False

    def _exec_sql(self, sql: str) -> dict:
        with self.conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is not None:
                cols = [d.name for d in cur.description]
                rows = cur.fetchmany(MAX_ROWS + 1)
                truncated = len(rows) > MAX_ROWS
                return {
                    "columns": cols,
                    "rows": [[jsonable(v) for v in row] for row in rows[:MAX_ROWS]],
                    "rowcount": cur.rowcount,
                    "truncated": truncated,
                }
            return {"rowcount": cur.rowcount, "status": cur.statusmessage}

    def _cleanup(self) -> None:
        conn, self.conn = self.conn, None
        if conn is not None and not conn.closed:
            try:
                if conn.get_transaction_status() != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
                    conn.rollback()  # uncommitted work is rolled back
            except Exception:
                pass
            try:
                conn.close()  # connection (and its locks) released
            except Exception:
                pass
        self.rnd.tx_closed(self.tx_name)


class Round:
    def __init__(self, manager: "RoundManager", round_id: str, schema: str):
        self.manager = manager
        self.id = round_id
        self.schema = schema
        self.lock = threading.RLock()
        self.state = "active"  # active -> stopping -> finished
        self.reason = None     # completed | stopped | timeout | shutdown
        self.detail = None
        self.created_at = utcnow_iso()
        self.finished_at = None
        self.stopping_since = None  # monotonic, for the terminate fallback
        self.cleanup_started = False  # guards against duplicate cleanup
        self.records: list[dict] = []
        self.seq = 0
        self.txs = {
            name: {"state": "not_started", "worker": None, "closing": False, "detail": None}
            for name in ("A", "B")
        }

    # ---------------- API-facing ----------------
    def begin_tx(self, tx: str) -> None:
        with self.lock:
            self._require_active()
            slot = self.txs[tx]
            if slot["state"] != "not_started":
                raise RoundStateError(f"tx {tx} already started (state={slot['state']})")
            worker = TxWorker(self, tx, self.manager.dsn)
            slot["worker"] = worker
            slot["state"] = "connecting"
            worker.start()
        if not worker.ready_event.wait(timeout=5.0):
            raise RoundStateError("timed out waiting for the database connection")
        if worker.connect_error is not None:
            raise RoundStateError(f"connection failed: {worker.connect_error}")

    def submit_step(self, tx: str, kind: str, sql: str | None = None) -> dict:
        with self.lock:
            self._require_active()
            slot = self.txs[tx]
            if slot["state"] != "open":
                raise RoundStateError(f"tx {tx} is not open (state={slot['state']})")
            if slot["closing"]:
                raise RoundStateError(
                    f"tx {tx} already received commit/rollback; no further steps")
            self.seq += 1
            rec = {
                "seq": self.seq, "tx": tx, "kind": kind, "sql": sql,
                "status": "queued", "result": None, "error": None,
                "error_class": None, "pgcode": None, "elapsed_ms": None,
                "queued_at": utcnow_iso(), "started_at": None, "ended_at": None,
            }
            self.records.append(rec)
            if kind in ("commit", "rollback"):
                slot["closing"] = True
            slot["worker"].submit({"rec": rec, "kind": kind, "sql": sql})
            return rec

    def stop(self, reason: str) -> None:
        """Accept a stop promptly: flip state, cancel in-flight statements,
        then let the workers wind down.  The final state is reported only
        after both paths ended and connections were released."""
        with self.lock:
            if self.state == "finished":
                return
            if self.state == "active":
                self.state = "stopping"
                self.reason = reason
                self.stopping_since = time.monotonic()
            workers = [s["worker"] for s in self.txs.values() if s["worker"] is not None]
        for w in workers:
            w.request_stop()
        self._maybe_finish()

    # ---------------- worker callbacks ----------------
    def tx_ready(self, tx: str) -> None:
        with self.lock:
            slot = self.txs[tx]
            slot["state"] = "open"
            slot["worker"].ready_event.set()

    def tx_connect_failed(self, tx: str, exc: Exception) -> None:
        with self.lock:
            slot = self.txs[tx]
            slot["state"] = "dead"
            slot["detail"] = f"connect failed: {exc}"
            slot["worker"].ready_event.set()
        self._maybe_finish()

    def tx_finished(self, tx: str, outcome: str) -> None:
        with self.lock:
            self.txs[tx]["state"] = outcome  # committed | rolled_back

    def tx_connection_lost(self, tx: str) -> None:
        with self.lock:
            slot = self.txs[tx]
            if slot["state"] not in ("committed", "rolled_back"):
                slot["state"] = "dead"
                slot["detail"] = "connection lost; transaction cannot continue"

    def tx_closed(self, tx: str) -> None:
        with self.lock:
            slot = self.txs[tx]
            if slot["state"] not in ("committed", "rolled_back", "dead"):
                slot["state"] = "closed"  # was open; rolled back and released
        self._maybe_finish()

    def mark_running(self, rec: dict) -> None:
        with self.lock:
            if rec["status"] in TERMINAL_STEP_STATES:
                return
            rec["status"] = "running"
            rec["started_at"] = utcnow_iso()
            rec["_mono"] = time.monotonic()

    def finalize_step(self, rec: dict, status: str, *, result=None, error=None,
                      error_class=None, pgcode=None) -> None:
        with self.lock:
            if rec["status"] in TERMINAL_STEP_STATES:
                return  # a late result must not rewrite the record
            rec["status"] = status
            rec["ended_at"] = utcnow_iso()
            mono = rec.pop("_mono", None)
            if mono is not None:
                rec["elapsed_ms"] = round((time.monotonic() - mono) * 1000, 1)
            if result is not None:
                rec["result"] = result
            if error is not None:
                rec["error"] = error
            if error_class is not None:
                rec["error_class"] = error_class
            if pgcode is not None:
                rec["pgcode"] = pgcode

    # ---------------- internals ----------------
    def _require_active(self) -> None:
        if self.state != "active":
            raise RoundStateError(
                f"round is {self.state}; no new steps are accepted")

    def _maybe_finish(self) -> None:
        with self.lock:
            if self.state != "stopping" or self.cleanup_started:
                return
            for slot in self.txs.values():
                if slot["state"] not in IDLE_TX_STATES:
                    return  # a path is still running / holding a connection
            # Both SQL paths ended, uncommitted work was rolled back and the
            # connections were released.  Run the cleanup exactly once.
            self.cleanup_started = True
        try:
            self.manager.drop_schema(self.schema)
            error = None
        except Exception as exc:
            error = str(exc)
        with self.lock:
            if error:
                # keep the round in "stopping" so the monitor retries the
                # cleanup instead of leaking the schema into later rounds
                self.cleanup_started = False
                self.detail = f"schema cleanup failed, retrying: {error}"
                return
            self.state = "finished"
            self.finished_at = utcnow_iso()

    # ---------------- views ----------------
    def summary(self) -> dict:
        with self.lock:
            return {
                "id": self.id,
                "state": self.state,
                "reason": self.reason,
                "schema": self.schema,
                "created_at": self.created_at,
                "finished_at": self.finished_at,
                "detail": self.detail,
                "txs": {
                    name: {"state": s["state"], "detail": s["detail"]}
                    for name, s in self.txs.items()
                },
            }

    def snapshot(self) -> dict:
        with self.lock:
            data = self.summary()
            data["records"] = [
                {k: v for k, v in rec.items() if not k.startswith("_")}
                for rec in self.records
            ]
            return data

    def find_step(self, seq: int) -> dict | None:
        with self.lock:
            for rec in self.records:
                if rec["seq"] == seq:
                    return {k: v for k, v in rec.items() if not k.startswith("_")}
            return None


class RoundManager:
    def __init__(self, dsn: str, step_timeout: float = 10.0, max_rounds: int = 2):
        self.dsn = dsn
        self.step_timeout = step_timeout
        self.max_rounds = max_rounds
        self.lock = threading.Lock()
        self.rounds: dict[str, Round] = {}
        self._halt = threading.Event()
        self._monitor = threading.Thread(
            target=self._monitor_loop, daemon=True, name="round-monitor")

    def start(self) -> None:
        self._monitor.start()

    # ---------------- round lifecycle ----------------
    def create_round(self) -> Round:
        with self.lock:
            active = [r for r in self.rounds.values() if r.state != "finished"]
            if len(active) >= self.max_rounds:
                raise BusyError(
                    f"already {len(active)} round(s) in flight; "
                    f"at most {self.max_rounds} parallel rounds are allowed")
            round_id = uuid.uuid4().hex[:8]
            schema = f"r_{round_id}"
        self._create_schema(schema)
        rnd = Round(self, round_id, schema)
        with self.lock:
            self.rounds[round_id] = rnd
        return rnd

    def get_round(self, round_id: str) -> Round | None:
        with self.lock:
            return self.rounds.get(round_id)

    def list_rounds(self) -> list[Round]:
        with self.lock:
            return sorted(self.rounds.values(), key=lambda r: r.created_at)

    def shutdown(self) -> None:
        for rnd in self.list_rounds():
            rnd.stop("shutdown")
        deadline = time.monotonic() + self.step_timeout + 10
        while time.monotonic() < deadline:
            if all(r.state == "finished" for r in self.list_rounds()):
                break
            time.sleep(0.1)
        self._halt.set()
        self._monitor.join(timeout=5)

    # ---------------- schema / baseline ----------------
    def _admin_connect(self):
        conn = psycopg2.connect(self.dsn)
        conn.autocommit = True
        return conn

    def _create_schema(self, schema: str) -> None:
        conn = self._admin_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA "{schema}"')
                cur.execute(
                    f'CREATE TABLE "{schema}".batches('
                    "id INT PRIMARY KEY, ready BOOLEAN NOT NULL)")
                cur.execute(
                    f'CREATE TABLE "{schema}".readings('
                    "id INT PRIMARY KEY, "
                    f'batch_id INT NOT NULL REFERENCES "{schema}".batches(id), '
                    "value NUMERIC NOT NULL)")
                cur.executemany(
                    f'INSERT INTO "{schema}".batches(id, ready) VALUES (%s, %s)',
                    BASELINE_BATCHES)
                cur.executemany(
                    f'INSERT INTO "{schema}".readings(id, batch_id, value)'
                    " VALUES (%s, %s, %s)",
                    BASELINE_READINGS)
        finally:
            conn.close()

    def drop_schema(self, schema: str) -> None:
        conn = self._admin_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            conn.close()

    # ---------------- monitor: 10s step timeout + terminate fallback ----------------
    def _monitor_loop(self) -> None:
        while not self._halt.wait(0.25):
            now = time.monotonic()
            for rnd in self.list_rounds():
                if rnd.state == "active":
                    self._check_step_timeout(rnd, now)
                elif rnd.state == "stopping":
                    self._check_stuck_round(rnd, now)
                    rnd._maybe_finish()  # retries a failed cleanup

    def _check_step_timeout(self, rnd: Round, now: float) -> None:
        with rnd.lock:
            if rnd.state != "active":
                return
            for rec in rnd.records:
                mono = rec.get("_mono")
                if rec["status"] == "running" and mono is not None \
                        and now - mono > self.step_timeout:
                    break
            else:
                return
        rnd.stop("timeout")

    def _check_stuck_round(self, rnd: Round, now: float) -> None:
        # Safety net: if a connection ignores cancel(), force-terminate its
        # backend so a failed round never leaks locks into the next round.
        with rnd.lock:
            if rnd.stopping_since is None or now - rnd.stopping_since < 15:
                return
            pids = [w.backend_pid for s in rnd.txs.values()
                    if (w := s["worker"]) is not None and w.is_alive()
                    and w.backend_pid]
        if not pids:
            return
        try:
            conn = self._admin_connect()
            try:
                with conn.cursor() as cur:
                    for pid in pids:
                        cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
            finally:
                conn.close()
        except Exception:
            pass
