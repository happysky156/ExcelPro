# src/excelops/config.py

APP_NAME = "ExcelPro"

APP_TAGLINE = {
    "zh": "企业级 Excel 批处理与数据整合平台",
    "en": "Enterprise Excel automation & batch processing",
}

APP_FOOTER = {
    "zh": "ExcelPro · 让 Excel 批处理更简单",
    "en": "ExcelPro · Make Excel batch-friendly",
}

# Copyright 2025
# Licensed under the Apache License, Version 2.0

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class AppConfig:
    app_name: str
    data_dir: Path
    artifacts_dir: Path
    db_path: Path
    workers: int

    @staticmethod
    def from_env() -> "AppConfig":
        root = Path(os.getenv("SHEETOPS_ROOT", ".")).resolve()

        data_dir = Path(os.getenv("SHEETOPS_DATA_DIR", root / "data")).resolve()
        artifacts_dir = Path(os.getenv("SHEETOPS_ARTIFACTS_DIR", root / "artifacts")).resolve()
        db_path = Path(os.getenv("SHEETOPS_DB_PATH", data_dir / "sheetops.db")).resolve()

        workers = int(os.getenv("SHEETOPS_WORKERS", "1"))  # 默认单 worker 更稳定
        return AppConfig(
            app_name=os.getenv("SHEETOPS_APP_NAME", "Excel Pro"),
            data_dir=data_dir,
            artifacts_dir=artifacts_dir,
            db_path=db_path,
            workers=max(1, workers),
        )
