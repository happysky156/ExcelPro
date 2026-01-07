# Copyright 2025
# Licensed under the Apache License, Version 2.0

from __future__ import annotations

import json
import sqlite3
import threading
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue, Empty
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

from .config import AppConfig
from .utils import ensure_dir, now_iso, new_job_id, safe_filename, write_json
from .ops import run_operation


@dataclass
class JobRecord:
    job_id: str
    created_at: str
    updated_at: str
    operation: str
    status: str          # queued/running/succeeded/failed/cancelled
    progress: int        # 0-100
    message: str
    params_json: str
    inputs_json: str
    output_path: Optional[str]
    error: Optional[str]


class JobStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        ensure_dir(db_path.parent)
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                  job_id TEXT PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  operation TEXT NOT NULL,
                  status TEXT NOT NULL,
                  progress INTEGER NOT NULL DEFAULT 0,
                  message TEXT NOT NULL DEFAULT '',
                  params_json TEXT NOT NULL,
                  inputs_json TEXT NOT NULL,
                  output_path TEXT,
                  error TEXT
                );
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);")
            conn.commit()

    def create_job(
        self,
        operation: str,
        params: Dict[str, Any],
        inputs: List[Dict[str, Any]],
    ) -> str:
        job_id = new_job_id()
        ts = now_iso()
        rec = {
            "job_id": job_id,
            "created_at": ts,
            "updated_at": ts,
            "operation": operation,
            "status": "queued",
            "progress": 0,
            "message": "Queued",
            "params_json": json.dumps(params, ensure_ascii=False),
            "inputs_json": json.dumps(inputs, ensure_ascii=False),
            "output_path": None,
            "error": None,
        }
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO jobs
                (job_id, created_at, updated_at, operation, status, progress, message, params_json, inputs_json, output_path, error)
                VALUES
                (:job_id, :created_at, :updated_at, :operation, :status, :progress, :message, :params_json, :inputs_json, :output_path, :error)
                """,
                rec,
            )
            conn.commit()
        return job_id

    def update_job(
        self,
        job_id: str,
        *,
        status: Optional[str] = None,
        progress: Optional[int] = None,
        message: Optional[str] = None,
        output_path: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        fields = []
        values: Dict[str, Any] = {"job_id": job_id, "updated_at": now_iso()}
        if status is not None:
            fields.append("status = :status")
            values["status"] = status
        if progress is not None:
            fields.append("progress = :progress")
            values["progress"] = int(progress)
        if message is not None:
            fields.append("message = :message")
            values["message"] = message
        if output_path is not None:
            fields.append("output_path = :output_path")
            values["output_path"] = output_path
        if error is not None:
            fields.append("error = :error")
            values["error"] = error

        fields.append("updated_at = :updated_at")

        with self._lock, self._connect() as conn:
            conn.execute(
                f"UPDATE jobs SET {', '.join(fields)} WHERE job_id = :job_id",
                values,
            )
            conn.commit()

    def update_inputs(self, job_id: str, inputs: List[Dict[str, Any]]) -> None:
        """把上传文件的 path 写回 inputs_json（供 worker 读取）。"""
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE jobs SET inputs_json = ?, updated_at = ? WHERE job_id = ?",
                (json.dumps(inputs, ensure_ascii=False), now_iso(), job_id),
            )
            conn.commit()

    def requeue_stale_running(self, stale_seconds: int = 15 * 60) -> int:
        """
        有些情况下 app 重启/崩溃会留下 running 状态，但实际没有 worker 在跑。
        把“超过阈值仍是 running”的任务回滚到 queued，等待重新派发。
        """
        cutoff = (datetime.utcnow() - timedelta(seconds=stale_seconds)).isoformat()
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT job_id FROM jobs WHERE status = 'running' AND updated_at < ?",
                (cutoff,),
            ).fetchall()
            ids = [r["job_id"] for r in rows]
            if not ids:
                return 0
            conn.execute(
                f"UPDATE jobs SET status='queued', progress=0, message='Re-queued (stale running)', updated_at=? "
                f"WHERE job_id IN ({','.join(['?'] * len(ids))})",
                [now_iso(), *ids],
            )
            conn.commit()
            return len(ids)

    def get_job(self, job_id: str) -> Optional[JobRecord]:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if not row:
                return None
            return JobRecord(**dict(row))

    def list_jobs(
        self,
        limit: int = 50,
        offset: int = 0,
        status: Optional[str] = None,
        operation: Optional[str] = None,
    ) -> List[JobRecord]:
        q = "SELECT * FROM jobs"
        clauses = []
        params: List[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if operation:
            clauses.append("operation = ?")
            params.append(operation)
        if clauses:
            q += " WHERE " + " AND ".join(clauses)
        q += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._lock, self._connect() as conn:
            rows = conn.execute(q, params).fetchall()
            return [JobRecord(**dict(r)) for r in rows]


class JobRunner:
    def __init__(self, cfg: AppConfig, store: JobStore):
        self.cfg = cfg
        self.store = store
        self.queue: "Queue[str]" = Queue()
        self._started = False
        self._start_lock = threading.Lock()
        self._workers: List[threading.Thread] = []

        # 防重复派发（UI手动派发 + 自动派发可能重复调用 submit）
        self._inflight_lock = threading.Lock()
        self._inflight: set[str] = set()

        ensure_dir(cfg.data_dir)
        ensure_dir(cfg.artifacts_dir)

    def start(self) -> None:
        with self._start_lock:
            if self._started:
                return
            self._started = True
            for i in range(self.cfg.workers):
                t = threading.Thread(target=self._worker_loop, name=f"excelops-worker-{i+1}", daemon=True)
                t.start()
                self._workers.append(t)

    def submit(self, job_id: str, *, force: bool = False) -> bool:
        """
        派发任务到 worker queue。
        force=True：强制再次入队（用于“执行/重试”按钮）
        """
        self.start()
        with self._inflight_lock:
            if (not force) and (job_id in self._inflight):
                return False
            self._inflight.add(job_id)
        self.queue.put(job_id)
        return True

    def dispatch_pending(self, *, limit: int = 200, stale_seconds: int = 15 * 60) -> Dict[str, int]:
        """
        自动派发：把 DB 里的 queued（以及卡住的 running）都派发到 queue。
        """
        self.start()
        requeued = self.store.requeue_stale_running(stale_seconds=stale_seconds)

        dispatched = 0
        for rec in self.store.list_jobs(status="queued", limit=limit, offset=0):
            if self.submit(rec.job_id):
                dispatched += 1

        return {"requeued": requeued, "dispatched": dispatched}

    def stats(self) -> Dict[str, Any]:
        return {
            "queued": self.queue.qsize(),
            "workers": len(self._workers),
            "inflight": len(self._inflight),
        }

    def _job_dirs(self, job_id: str) -> Tuple[Path, Path]:
        job_dir = ensure_dir(self.cfg.data_dir / "jobs" / job_id)
        uploads_dir = ensure_dir(job_dir / "uploads")
        artifacts_dir = ensure_dir(self.cfg.artifacts_dir / job_id)
        return uploads_dir, artifacts_dir

    def save_uploads(self, job_id: str, uploaded_files: List[Any]) -> List[Dict[str, Any]]:
        uploads_dir, _ = self._job_dirs(job_id)
        inputs_meta: List[Dict[str, Any]] = []

        for uf in uploaded_files:
            fname = safe_filename(getattr(uf, "name", "upload.bin"))
            fpath = uploads_dir / fname
            fpath.write_bytes(uf.getvalue())
            inputs_meta.append({"name": fname, "path": str(fpath)})

        write_json(uploads_dir / "inputs.json", {"files": inputs_meta})
        return inputs_meta

    def _done(self, job_id: str) -> None:
        with self._inflight_lock:
            self._inflight.discard(job_id)

    def _worker_loop(self) -> None:
        while True:
            try:
                job_id = self.queue.get(timeout=0.5)
            except Empty:
                continue

            try:
                rec = self.store.get_job(job_id)
                if rec is None:
                    continue

                # 防重复：只有 queued 才执行
                if rec.status != "queued":
                    continue

                _, artifacts_dir = self._job_dirs(job_id)

                self.store.update_job(job_id, status="running", progress=1, message="Running")

                params = json.loads(rec.params_json)
                inputs = json.loads(rec.inputs_json)
                input_paths = [Path(it["path"]) for it in inputs if isinstance(it, dict) and "path" in it]

                def progress_cb(p: int, msg: str = "") -> None:
                    self.store.update_job(job_id, progress=p, message=msg or "Running")

                out_path = run_operation(
                    operation=rec.operation,
                    params=params,
                    input_paths=input_paths,
                    output_dir=artifacts_dir,
                    progress_cb=progress_cb,
                )

                self.store.update_job(
                    job_id,
                    status="succeeded",
                    progress=100,
                    message="Succeeded",
                    output_path=str(out_path) if out_path else None,
                    error=None,
                )

            except Exception as e:
                tb = traceback.format_exc()
                self.store.update_job(
                    job_id,
                    status="failed",
                    progress=100,
                    message="Failed",
                    error=f"{str(e)}\n\n{tb}",
                )
            finally:
                self._done(job_id)
                self.queue.task_done()


@st.cache_resource
def get_job_store(cfg: AppConfig) -> JobStore:
    return JobStore(cfg.db_path)


@st.cache_resource
def get_job_runner(cfg: AppConfig, _store: JobStore) -> JobRunner:
    return JobRunner(cfg, _store)

