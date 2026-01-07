# Copyright 2025
# Licensed under the Apache License, Version 2.0

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import streamlit as st

from .config import AppConfig
from .jobs import JobStore, JobRunner, get_job_store, get_job_runner
from .ops import OP_REGISTRY
from .utils import read_file_bytes

USER_GUIDE_MD = """
# 📘 ExcelPro 使用指南

## 1) 这是什么？
ExcelPro 是一个基于 Web 的 Excel 批处理与数据整合工具，适合“重度Excel用户”做批量处理、合并、拆分、转换等任务。

---

## 2) 功能清单（工具中心）
在左侧导航进入「🛠 工具中心」，下拉选择功能后上传文件、设置参数并提交执行：

1. **数据表拼接（按列结构一致合并）**  
   - 作用：把多个 Excel 文件里“结构一致”的表追加合并成一张表  
   - 适用：同模板月报/周报合并、多个供应商同结构数据汇总

2. **多表关联（按 Key 字段 Join）**  
   - 作用：按指定 Key 字段把两张表（或多张表）做 left/inner/outer 关联  
   - 适用：主数据表 + 明细表、SKU表 + 价格表

3. **多文件 Sheet 合并（可保留样式）**  
   - 作用：把多个 Excel 的所有 Sheet 合并到一个新工作簿中  
   - 说明：开启“保留样式”会更慢（实验性）

4. **单文件 Sheet 拆分（ZIP）**  
   - 作用：把一个 Excel 的每个 Sheet 拆成单独的 Excel，并打包成 zip

5. **Excel → CSV（按 Sheet 输出 ZIP）**  
   - 作用：每个 Sheet 输出一个 CSV 文件，打包成 zip

6. **CSV → Excel（批量 ZIP）**  
   - 作用：把多个 CSV 批量转换为 Excel，打包 zip

7. **Excel → PDF（按 Sheet 输出 ZIP，企业版）**  
   - 作用：把每个 Sheet 导出成 PDF，打包 zip  
   - 注意：超大表建议限制最大行数

8. **PDF → Excel（抽表格，批量 ZIP）**  
   - 作用：从 PDF 中抽取表格导出 Excel（效果取决于 PDF 是否为可解析表格）

---

## 3) 标准使用流程
**Step 1 · 上传文件**：拖拽或选择文件（支持多选）  
**Step 2 · 参数设置**：根据功能填写参数/输出文件名  
**Step 3 · 提交/执行**：  
- “🚀 提交并执行”：立刻派发给 worker 执行  
- “🧾 仅入库（queued）”：先入队，之后在「队列与历史」再派发  
**Step 4 · 下载结果**：执行成功后出现下载按钮

---

## 4) 队列与历史（📦）
这里可以查看最近任务、下载结果、失败重试、手动派发。

---

## 5) 重要注意事项（请务必阅读）
### 5.1 文件安全与隐私
- 上传的文件会用于任务处理并生成结果文件
- 不要上传敏感/保密数据（试用阶段建议先用脱敏数据）

### 5.2 文件大小与性能
- 文件越大、Sheet 越多，处理越慢
- “保留样式”会明显变慢（且可能出现兼容性问题）

### 5.3 兼容性提示（⚠️ 目前已知问题）
- ✅ Windows 电脑：可正常使用  
- ✅ Android 手机：可正常使用  
- ⚠️ iPhone / iPad（iOS / iPadOS）：**目前无法正常打开/使用**（会出现页面脚本/正则相关报错）  
  - 临时解决：请在电脑或安卓设备上使用  
- Mac：尚未全面测试（如遇问题请反馈）

### 5.4 左侧「⚙️ 设置」说明
- **⚙️ 设置目前是预留入口**，还没有正式实现业务配置功能  
- 目前页面仅展示一些系统配置参数（后续会增加默认输出、历史保留等设置）

---

## 6) 问题反馈
如果你遇到报错、功能异常、或希望增加新功能，请发送邮件到：  
**harley.xie@foxmail.com**

建议邮件包含：
- 你选择的功能名称
- 你上传的文件类型（xlsx/csv/pdf）与大概大小
- 错误提示截图/报错文本
- 你期望的输出结果描述
"""


def _op_label(op_key: str) -> str:
    v = OP_REGISTRY.get(op_key, op_key)
    if isinstance(v, dict):
        return str(v.get("label", op_key))
    return str(v)


def _rerun() -> None:
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()


def _inject_enterprise_css(app_name: str) -> None:
    st.markdown(
        f"""
<style>
:root {{
  --bg: #0b1220;
  --panel: #0f172a;
  --card: #0b1324;
  --line: #1f2a44;
  --text: #e5e7eb;
  --muted: #9ca3af;
  --accent: #4f46e5;
  --accent2: #22c55e;
  --warn: #f59e0b;
  --danger: #ef4444;
}}

html, body, .stApp {{
  background: var(--bg) !important;
  color: var(--text) !important;
}}

[data-testid="stAppViewContainer"] {{
  background: var(--bg) !important;
}}

[data-testid="stHeader"], [data-testid="stToolbar"] {{
  background: transparent !important;
}}

.block-container {{
  padding-top: 2.2rem;
  padding-bottom: 2rem;
}}

.expro-topbar {{
  display:flex;
  align-items:center;
  justify-content:space-between;
  padding: 14px 18px;
  border: 1px solid var(--line);
  border-radius: 14px;
  background: linear-gradient(180deg, rgba(79,70,229,0.22), rgba(15,23,42,0.7));
  box-shadow: 0 10px 30px rgba(0,0,0,0.35);
}}

.expro-badge {{
  display:inline-flex;
  gap:8px;
  align-items:center;
  padding: 6px 10px;
  border-radius: 999px;
  border: 1px solid rgba(255,255,255,0.08);
  background: rgba(11,18,32,0.65);
  color: var(--muted);
  font-size: 12px;
}}

.expro-card {{
  border: 1px solid var(--line);
  border-radius: 16px;
  padding: 18px;
  background: rgba(11,19,36,0.92);
  box-shadow: 0 12px 34px rgba(0,0,0,0.30);
}}

.expro-title {{
  font-size: 30px;
  font-weight: 800;
  margin: 0;
}}

.expro-sub {{
  color: var(--muted);
  margin-top: 4px;
}}

.expro-kpi {{
  display:flex;
  gap:10px;
  flex-wrap:wrap;
  margin-top: 10px;
}}

.expro-pill {{
  padding: 6px 10px;
  border-radius: 999px;
  border: 1px solid rgba(255,255,255,0.08);
  background: rgba(15,23,42,0.85);
  color: var(--text);
  font-size: 12px;
}}

.expro-danger {{
  border-color: rgba(239,68,68,0.35) !important;
  background: rgba(239,68,68,0.08) !important;
}}

.expro-warn {{
  border-color: rgba(245,158,11,0.35) !important;
  background: rgba(245,158,11,0.08) !important;
}}

.expro-ok {{
  border-color: rgba(34,197,94,0.35) !important;
  background: rgba(34,197,94,0.08) !important;
}}

</style>
""",
        unsafe_allow_html=True,
    )


def render_app(cfg: AppConfig, store: JobStore, runner: JobRunner) -> None:
    _inject_enterprise_css(cfg.app_name)

    stats = runner.stats()
    st.markdown(
        f"""
<div class="expro-topbar">
  <div>
    <div class="expro-badge">🧩 {cfg.app_name} · System Console</div>
    <h1 class="expro-title">工具中心</h1>
    <div class="expro-sub">上传文件 → 设置参数 → 提交/执行 → 在「队列与历史」下载结果</div>
  </div>
  <div class="expro-kpi">
    <div class="expro-pill">Queue: {stats["queued"]}</div>
    <div class="expro-pill">Workers: {stats["workers"]}</div>
    <div class="expro-pill">In-flight: {stats["inflight"]}</div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

    # Sidebar
    with st.sidebar:
        st.markdown(f"### {cfg.app_name}")
        st.caption("Excel heavy users · batch & queue oriented")

        auto_dispatch = st.toggle("Auto-dispatch queued jobs", value=True, help="自动把 DB 里的 queued 任务派发到 worker")
        if st.button("↻ Dispatch now"):
            res = runner.dispatch_pending()
            st.success(f"Dispatched: {res['dispatched']} · Requeued: {res['requeued']}")
            _rerun()

        st.divider()

        page = st.radio("导航", ["🛠 工具中心", "📦 队列与历史", "⚙️ 设置", "📘 使用指南"], index=0)

    # 自动派发（每次刷新都可执行；JobRunner 已做防重复）
    if auto_dispatch:
        runner.dispatch_pending()

    if page == "🛠 工具中心":
        render_tools_page(cfg, store, runner)
    elif page == "📦 队列与历史":
        render_jobs_page(cfg, store, runner)
    else:
            if page == "🛠 工具中心":
                render_tools_page(cfg, store, runner)
            elif page == "📦 队列与历史":
                render_jobs_page(cfg, store, runner)
            elif page == "⚙️ 设置":
                render_settings_page(cfg)
            elif page == "📘 使用指南":
                render_user_guide_page(cfg)
            else:
                render_settings_page(cfg)



def render_tools_page(cfg: AppConfig, store: JobStore, runner: JobRunner) -> None:
    st.markdown('<div class="expro-card">', unsafe_allow_html=True)

    # Step 0: choose operation
    op_labels = [(k, _op_label(k)) for k in OP_REGISTRY.keys()]

    label_to_key = {label: k for k, label in op_labels}

    op_label = st.selectbox(
        "选择功能",
        [label for _, label in op_labels],
        index=0,
    )
    op = label_to_key[op_label]

    st.divider()

    # Step 1: upload
    st.subheader("Step 1 · 上传文件")
    exts = _accept_types(op)
    uploaded = st.file_uploader(
        "拖拽或选择文件（支持多选）",
        type=exts,
        accept_multiple_files=True,
        help=f"支持：{', '.join(exts)}",
    )

    # Step 2: params
    st.subheader("Step 2 · 参数设置")
    params = render_params_panel(op)

    # Step 3: submit/execute
    st.subheader("Step 3 · 提交 / 执行")
    cols = st.columns([1, 1, 2])
    with cols[0]:
        submit_btn = st.button("🚀 提交并执行", use_container_width=True)
    with cols[1]:
        only_queue_btn = st.button("🧾 仅入库（queued）", use_container_width=True)
    with cols[2]:
        st.caption("说明：提交会生成 job 记录；执行=派发到 worker queue。你也可以在「队列与历史」里重试/再执行。")

    # Step 4: quick view
    st.subheader("Step 4 · 快速查看")
    last_job_id = st.session_state.get("last_job_id")

    if submit_btn or only_queue_btn:
        if not uploaded:
            st.warning("请先上传文件。")
        else:
            job_id = store.create_job(operation=op, params=params, inputs=[])
            inputs_meta = runner.save_uploads(job_id, uploaded)
            store.update_inputs(job_id, inputs_meta)

            st.session_state["last_job_id"] = job_id
            st.success(f"已创建任务：{job_id}")

            if submit_btn:
                runner.submit(job_id, force=True)
                st.info("已派发到 worker（如页面未更新，点击下方“刷新状态”）。")

    if last_job_id:
        rec = store.get_job(last_job_id)
        if rec:
            st.write(f"状态：**{rec.status}** · 进度：**{rec.progress}%** · {rec.message}")

            action_cols = st.columns([1, 1, 1, 2])
            with action_cols[0]:
                if st.button("▶ 执行/重试", use_container_width=True):
                    # 如果是 failed/succeeded，也允许强制再派发（一般用于 queued 卡住场景）
                    runner.submit(last_job_id, force=True)
                    store.update_job(last_job_id, status="queued", progress=0, message="Manual dispatch")
                    _rerun()
            with action_cols[1]:
                if st.button("🔁 新建任务重试", use_container_width=True):
                    # 复制参数 + 复用同一批输入文件（保留历史）
                    params2 = json.loads(rec.params_json)
                    inputs2 = json.loads(rec.inputs_json)
                    new_id = store.create_job(rec.operation, params2, inputs2)
                    runner.submit(new_id, force=True)
                    st.session_state["last_job_id"] = new_id
                    st.success(f"已新建并派发：{new_id}")
                    _rerun()
            with action_cols[2]:
                if st.button("↻ 刷新状态", use_container_width=True):
                    _rerun()

            if rec.status == "succeeded" and rec.output_path:
                out_path = Path(rec.output_path)
                if out_path.exists():
                    st.download_button(
                        "⬇️ 下载输出",
                        data=read_file_bytes(out_path),
                        file_name=out_path.name,
                        mime=_guess_mime(out_path),
                    )

            if rec.status == "failed" and rec.error:
                st.markdown('<div class="expro-card expro-danger">', unsafe_allow_html=True)
                st.subheader("Technical details (developer)")
                st.code(rec.error)
                st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)


def render_settings_page(cfg: AppConfig) -> None:
    st.markdown('<div class="expro-card">', unsafe_allow_html=True)
    st.subheader("⚙️ 设置")
    st.write("（预留：后续可放默认输出目录、最大文件大小、历史保留周期等）")
    st.json(
        {
            "app_name": getattr(cfg, "app_name", "ExcelPro"),
            "data_dir": str(getattr(cfg, "data_dir", "")),
            "artifacts_dir": str(getattr(cfg, "artifacts_dir", "")),
            "db_path": str(getattr(cfg, "db_path", "")),
            "workers": getattr(cfg, "workers", None),
        }
    )
    st.markdown("</div>", unsafe_allow_html=True)

def render_user_guide_page(cfg: AppConfig) -> None:
    st.markdown('<div class="expro-card">', unsafe_allow_html=True)
    st.markdown(USER_GUIDE_MD, unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

def render_jobs_page(cfg: AppConfig, store: JobStore, runner: JobRunner) -> None:
    st.markdown('<div class="expro-card">', unsafe_allow_html=True)
    st.subheader("📦 队列与历史")

    jobs = store.list_jobs(limit=50, offset=0)

    if not jobs:
        st.info("暂无任务记录。")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    for rec in jobs:
        with st.expander(f"{rec.job_id} · {rec.operation} · {rec.status} · {rec.progress}%"):
            st.write(f"创建：{rec.created_at} · 更新：{rec.updated_at}")
            st.write(f"消息：{rec.message}")

            btn_cols = st.columns([1, 1, 1, 3])
            with btn_cols[0]:
                if rec.status == "queued" and st.button("▶ 派发", key=f"dispatch_{rec.job_id}"):
                    runner.submit(rec.job_id, force=True)
                    store.update_job(rec.job_id, status="queued", progress=0, message="Manual dispatch")
                    _rerun()

            with btn_cols[1]:
                if rec.status in ("failed", "succeeded") and st.button("🔁 重试(新任务)", key=f"retry_{rec.job_id}"):
                    params = json.loads(rec.params_json)
                    inputs = json.loads(rec.inputs_json)
                    new_id = store.create_job(rec.operation, params, inputs)
                    runner.submit(new_id, force=True)
                    st.success(f"已重新提交：{new_id}")
                    _rerun()

            with btn_cols[2]:
                if st.button("↻ 刷新", key=f"refresh_{rec.job_id}"):
                    _rerun()

            if rec.status == "succeeded" and rec.output_path:
                out_path = Path(rec.output_path)
                if out_path.exists():
                    st.success(f"输出文件：{out_path.name}")
                    st.download_button(
                        "⬇️ 下载输出",
                        data=read_file_bytes(out_path),
                        file_name=out_path.name,
                        mime=_guess_mime(out_path),
                        key=f"dl_{rec.job_id}",
                    )

            if rec.status == "failed" and rec.error:
                st.markdown('<div class="expro-card expro-danger">', unsafe_allow_html=True)
                st.subheader("Technical details (developer)")
                st.code(rec.error)
                st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)


def render_params_panel(op: str) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    params["output_name"] = st.text_input("输出文件名（可改）", value=_default_output_name(op))

    if op == "join_tables":
        params["key"] = st.text_input("Join Key 字段名（必须）", value="")
        params["how"] = st.selectbox("Join 方式", ["left", "inner", "outer"], index=0)

    if op == "merge_sheets":
        params["preserve_styles"] = st.toggle("保留样式（更慢，实验性）", value=False)

    if op == "excel_to_pdf":
        params["page_mode"] = st.selectbox("PDF 页面方向", ["landscape", "portrait"], index=0)
        params["max_rows"] = st.number_input("每张表最大导出行数（防止超大表）", min_value=50, max_value=5000, value=200, step=50)

    return params


def _accept_types(op: str) -> List[str]:
    if op in ("csv_to_excel",):
        return ["csv"]
    if op in ("pdf_to_excel",):
        return ["pdf"]
    return ["xlsx"]


def _default_output_name(op: str) -> str:
    mapping = {
        "concat_tables": "concat_result.xlsx",
        "join_tables": "join_result.xlsx",
        "merge_sheets": "merged_sheets.xlsx",
        "split_sheets": "split_sheets.zip",
        "excel_to_csv": "excel_to_csv.zip",
        "csv_to_excel": "csv_to_excel.zip",
        "excel_to_pdf": "excel_to_pdf.zip",
        "pdf_to_excel": "pdf_to_excel.zip",
    }
    return mapping.get(op, "output.bin")


def _guess_mime(p: Path) -> str:
    ext = p.suffix.lower()
    if ext == ".xlsx":
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if ext == ".zip":
        return "application/zip"
    if ext == ".pdf":
        return "application/pdf"
    return "application/octet-stream"


# ✅ 关键：固定 UI 入口（解决你之前的 entrypoint 报错）
def run_app(cfg: AppConfig) -> None:
    store = get_job_store(cfg)
    runner = get_job_runner(cfg, store)
    runner.start()
    render_app(cfg, store, runner)


# 兼容别名
main = run_app
app = run_app
render_ui = run_app
launch = run_app
start = run_app
build_ui = run_app
