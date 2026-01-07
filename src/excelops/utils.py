# Copyright 2025
# Licensed under the Apache License, Version 2.0

from __future__ import annotations

import re
import json
import uuid
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict


_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_job_id() -> str:
    return uuid.uuid4().hex


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def safe_filename(name: str) -> str:
    name = name.strip().replace("\\", "/").split("/")[-1]
    name = _SAFE_NAME_RE.sub("_", name)
    return name[:180] if len(name) > 180 else name


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def read_file_bytes(path: Path) -> bytes:
    return path.read_bytes()
