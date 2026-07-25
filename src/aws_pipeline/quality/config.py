from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


DEFAULT_DASHBOARD_REFRESH_MINUTES = 5
DEFAULT_PUBLISH_BUDGET_SECONDS = 90
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "quality.yml"
CONFIG_PATH_ENV_VAR = "QUALITY_CONFIG_PATH"


@dataclass(frozen=True)
class SlaConfig:
    dashboard_refresh_minutes: int = DEFAULT_DASHBOARD_REFRESH_MINUTES
    publish_budget_seconds: int = DEFAULT_PUBLISH_BUDGET_SECONDS


@dataclass(frozen=True)
class QualityConfig:
    sla: SlaConfig = SlaConfig()


@lru_cache(maxsize=1)
def get_quality_config() -> QualityConfig:
    config_path = Path(os.environ.get(CONFIG_PATH_ENV_VAR, DEFAULT_CONFIG_PATH))
    if not config_path.exists():
        return QualityConfig()

    with config_path.open("r", encoding="utf-8") as config_file:
        raw_config = yaml.safe_load(config_file) or {}

    if not isinstance(raw_config, dict):
        raise ValueError("Invalid quality config: root must be a mapping")

    return QualityConfig(
        sla=_load_sla_config(raw_config.get("sla", {})),
    )


def get_sla_config() -> SlaConfig:
    return get_quality_config().sla


def _load_sla_config(raw_sla_config: Any) -> SlaConfig:
    if not isinstance(raw_sla_config, dict):
        raise ValueError("Invalid quality config: 'sla' must be a mapping")

    return SlaConfig(
        dashboard_refresh_minutes=_positive_int(
            raw_sla_config,
            "dashboard_refresh_minutes",
            DEFAULT_DASHBOARD_REFRESH_MINUTES,
        ),
        publish_budget_seconds=_positive_int(
            raw_sla_config,
            "publish_budget_seconds",
            DEFAULT_PUBLISH_BUDGET_SECONDS,
        ),
    )


def _positive_int(config: dict[str, Any], key: str, default: int) -> int:
    value = config.get(key, default)
    try:
        parsed_value = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid quality config: '{key}' must be an integer") from exc

    if parsed_value <= 0:
        raise ValueError(f"Invalid quality config: '{key}' must be > 0")

    return parsed_value
