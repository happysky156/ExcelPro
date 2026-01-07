# Copyright 2025
# Licensed under the Apache License, Version 2.0

from __future__ import annotations

import os
import sys
import traceback
import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Tuple

import streamlit as st


# -----------------------------
# Path bootstrap for "src/" layout
# -----------------------------
ROOT = Path(__file__).resolve().parent
SRC_DIR = ROOT / "src"
if SRC_DIR.exists() and str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


# -----------------------------
# Enterprise error UI skin (only used when crash)
# -----------------------------
def _inject_error_css(app_name: str) -> None:
    st.markdown(
        f"""
<style>
:root {{
  --bg: #0b1220;
  --panel: #0f172a;
  --card: rgba(15,23,42,0.65);
  --line: #1f2a44;
  --text: #e5e7eb;
  --muted: #9ca3af;
  --accent: #4f46e5;
  --danger: #ef4444;
}}
html, body, [data-testid="stAppViewContainer"] {{
  background: radial-gradient(1200px 600px at 20% 0%, rgba(79,70,229,0.22), transparent 60%),
              radial-gradient(900px 500px at 80% 20%, rgba(34,197,94,0.12), transparent 55%),
              var(--bg);
  color: var(--text);
}}
header, footer {{ visibility: hidden; }}
.block-container {{ padding-top: 1.2rem; max-width: 1200px; }}

.topbar {{
  display:flex; align-items:center; justify-content:space-between;
  padding: 14px 18px;
  border: 1px solid var(--line);
  border-radius: 14px;
  background: linear-gradient(135deg, rgba(79,70,229,0.18), rgba(15,23,42,1));
  margin-bottom: 14px;
}}
.topbar .brand .name {{
  font-size: 18px; font-weight: 800; color: #fff;
}}
.topbar .brand .tag {{
  font-size: 12px; color: var(--muted);
}}

.card {{
  border: 1px solid var(--line);
  border-radius: 14px;
  background: var(--card);
  padding: 16px;
}}
.small {{ color: var(--muted); font-size: 12px; }}
.hr {{ height:1px; background: var(--line); margin: 12px 0; }}

.error-title {{
  font-size: 26px; font-weight: 900; margin: 2px 0 4px 0;
}}
.error-msg {{
  color: var(--text);
  font-size: 14px;
  padding: 10px 12px;
  border: 1px solid rgba(239,68,68,0.35);
  background: rgba(239,68,68,0.08);
  border-radius: 12px;
}}
</style>
""",
        unsafe_allow_html=True,
    )


def _render_error_screen(app_name: str, exc: BaseException) -> None:
    _inject_error_css(app_name)

    st.markdown(
        f"""
<div class="topbar">
  <div class="brand">
    <div class="name">📊 {app_name} · System Notice</div>
    <div class="tag">Enterprise UI · Queue · Batch History</div>
  </div>
  <div class="small">License: Apache-2.0</div>
</div>
""",
        unsafe_allow_html=True,
    )

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="error-title">Unexpected error</div>', unsafe_allow_html=True)
    st.caption("Tip: 如果你是用户，可以先重试；如果你是开发者，请展开 technical details 排查。")

    msg = str(exc) if str(exc) else exc.__class__.__name__
    st.markdown(f'<div class="error-msg">{msg}</div>', unsafe_allow_html=True)

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("🔁 Retry", type="primary"):
            try:
                st.cache_resource.clear()
            except Exception:
                pass
            st.rerun()
    with col2:
        if st.button("🧹 Clear cache & Retry"):
            try:
                st.cache_resource.clear()
            except Exception:
                pass
            st.rerun()

    details = traceback.format_exc()
    with st.expander("Show technical details (for developer)"):
        st.code(details)

        st.download_button(
            "⬇️ Download technical details",
            data=details.encode("utf-8"),
            file_name=f"{app_name}_technical_details.txt",
            mime="text/plain",
        )

    st.markdown("</div>", unsafe_allow_html=True)


# -----------------------------
# Runtime config fallback (in case excelops.config is strict)
# -----------------------------
@dataclass
class _FallbackConfig:
    app_name: str = "ExcelPro"
    data_dir: Path = ROOT / "data"
    artifacts_dir: Path = ROOT / "artifacts"
    db_path: Path = (ROOT / "data" / "excelpro_jobs.sqlite")
    workers: int = 2


def _load_config() -> Any:
    """
    Try to use excelops.config.AppConfig first.
    If it cannot be constructed, fall back to a minimal config object.
    """
    try:
        import excelops.config as cfg_mod  # type: ignore

        if hasattr(cfg_mod, "load_config") and callable(cfg_mod.load_config):
            cfg = cfg_mod.load_config()
        elif hasattr(cfg_mod, "get_config") and callable(cfg_mod.get_config):
            cfg = cfg_mod.get_config()
        else:
            AppConfig = getattr(cfg_mod, "AppConfig", None)
            if AppConfig is None:
                raise RuntimeError("excelops.config.AppConfig not found")

            if hasattr(AppConfig, "from_env") and callable(getattr(AppConfig, "from_env")):
                cfg = AppConfig.from_env()
            else:
                cfg = AppConfig()

        for attr in ("data_dir", "artifacts_dir"):
            p = getattr(cfg, attr, None)
            if p:
                Path(p).mkdir(parents=True, exist_ok=True)

        dbp = getattr(cfg, "db_path", None)
        if dbp:
            Path(dbp).parent.mkdir(parents=True, exist_ok=True)

        if not getattr(cfg, "app_name", None):
            cfg.app_name = "ExcelPro"

        return cfg

    except Exception:
        data_dir = os.getenv("EXCELOPS_DATA_DIR") or str(ROOT / "data")
        art_dir = os.getenv("EXCELOPS_ARTIFACTS_DIR") or str(ROOT / "artifacts")
        workers = os.getenv("EXCELOPS_WORKERS") or "2"

        cfg = _FallbackConfig(
            app_name="ExcelPro",
            data_dir=Path(data_dir),
            artifacts_dir=Path(art_dir),
            db_path=Path(data_dir) / "excelpro_jobs.sqlite",
            workers=int(workers),
        )
        cfg.data_dir.mkdir(parents=True, exist_ok=True)
        cfg.artifacts_dir.mkdir(parents=True, exist_ok=True)
        cfg.db_path.parent.mkdir(parents=True, exist_ok=True)
        return cfg


def _init_by_signature(cls: Any, **candidates: Any) -> Any:
    sig = inspect.signature(cls.__init__)
    params = list(sig.parameters.values())[1:]
    allowed = {p.name for p in params}
    kwargs = {k: v for k, v in candidates.items() if k in allowed}

    try:
        return cls(**kwargs)
    except TypeError:
        if len(params) == 1:
            only = params[0].name
            if only in candidates:
                return cls(candidates[only])
        return cls()


@st.cache_resource
def _bootstrap_runtime() -> Tuple[Any, Any, Any]:
    cfg = _load_config()
    from excelops.jobs import JobStore, JobRunner  # type: ignore

    store = _init_by_signature(
        JobStore,
        cfg=cfg,
        config=cfg,
        db_path=getattr(cfg, "db_path", None),
        data_dir=getattr(cfg, "data_dir", None),
        root_dir=getattr(cfg, "data_dir", None),
    )

    runner = _init_by_signature(
        JobRunner,
        cfg=cfg,
        config=cfg,
        store=store,
        job_store=store,
        artifacts_dir=getattr(cfg, "artifacts_dir", None),
        workers=getattr(cfg, "workers", None),
    )

    if hasattr(runner, "start") and callable(getattr(runner, "start")):
        try:
            runner.start()
        except Exception:
            pass

    return cfg, store, runner


def main() -> None:
    st.set_page_config(page_title="ExcelPro", page_icon="📊", layout="wide")

    try:
        cfg, store, runner = _bootstrap_runtime()

        # ✅ 优先使用固定入口 run_app(cfg)（更稳定）
        try:
            from excelops.ui import run_app  # type: ignore

            run_app(cfg)
            return
        except Exception:
            # 回退到旧式 render_app(cfg, store, runner)
            from excelops.ui import render_app  # type: ignore

            render_app(cfg, store, runner)

    except Exception as exc:
        _render_error_screen("ExcelPro", exc)


if __name__ == "__main__":
    main()
