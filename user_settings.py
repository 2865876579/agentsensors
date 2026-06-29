from __future__ import annotations

import copy
import json
import os
from datetime import datetime, timedelta
from typing import Any

from config import TIMEZONE

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - old Python fallback
    ZoneInfo = None


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SETTINGS_PATH = os.getenv(
    "XIAOAN_USER_SETTINGS_PATH",
    os.path.join(BASE_DIR, "user_settings.json"),
)


DEFAULT_SETTINGS: dict[str, Any] = {
    "version": 1,
    "personality": {
        "name": "小安",
        "tone": "gentle",
        "style": "short",
        "initiative": "low",
        "description": "温柔、克制、低打扰，像真实的枕边生活助手，不要有太重的 AI 味。",
    },
    "quiet_periods": [
        {
            "name": "night_sleep",
            "label": "夜间睡眠",
            "enabled": True,
            "start": "22:30",
            "end": "07:30",
        },
        {
            "name": "nap",
            "label": "午休",
            "enabled": False,
            "start": "13:00",
            "end": "14:30",
        },
    ],
    "quiet_rules": {
        "block_ai_voice": True,
        "block_ai_light": True,
        "block_ai_screen": True,
        "block_ai_pc_agent": True,
        "allow_gentle_pillow_adjust": True,
        "alert_method": "phone_only",
    },
    "memory": [
        "用户喜欢低打扰、自然一点的表达，不喜欢太明显的 AI 味。",
        "睡眠时不要主动语音播报、闪灯或亮屏。",
        "灯带默认应是高级、柔和的暖色，不要上电就七彩变化。",
    ],
    "updated_at": "",
}


def _now() -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo(TIMEZONE))
    return datetime.utcnow() + timedelta(hours=8)


def _deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in (patch or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _time_text(value: Any, fallback: str) -> str:
    text = str(value or "").strip()
    try:
        hour_text, minute_text = text.split(":", 1)
        hour = max(0, min(23, int(hour_text)))
        minute = max(0, min(59, int(minute_text)))
        return f"{hour:02d}:{minute:02d}"
    except Exception:
        return fallback


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "y"}
    if value is None:
        return default
    return bool(value)


def _normalize_periods(periods: Any) -> list[dict[str, Any]]:
    defaults = {item["name"]: item for item in DEFAULT_SETTINGS["quiet_periods"]}
    incoming = {}
    if isinstance(periods, list):
        for item in periods:
            if isinstance(item, dict) and item.get("name"):
                incoming[str(item["name"])] = item

    normalized: list[dict[str, Any]] = []
    for name, default in defaults.items():
        item = _deep_merge(default, incoming.get(name, {}))
        item["name"] = name
        item["label"] = str(item.get("label") or default["label"])
        item["enabled"] = _to_bool(item.get("enabled"), bool(default["enabled"]))
        item["start"] = _time_text(item.get("start"), default["start"])
        item["end"] = _time_text(item.get("end"), default["end"])
        normalized.append(item)
    return normalized


def normalize_settings(data: dict[str, Any] | None) -> dict[str, Any]:
    settings = _deep_merge(DEFAULT_SETTINGS, data or {})

    personality = settings.get("personality") or {}
    settings["personality"] = {
        "name": str(personality.get("name") or "小安")[:20],
        "tone": str(personality.get("tone") or "gentle")[:32],
        "style": str(personality.get("style") or "short")[:32],
        "initiative": str(personality.get("initiative") or "low")[:32],
        "description": str(personality.get("description") or "")[:300],
    }

    settings["quiet_periods"] = _normalize_periods(settings.get("quiet_periods"))

    rules = _deep_merge(DEFAULT_SETTINGS["quiet_rules"], settings.get("quiet_rules") or {})
    settings["quiet_rules"] = {
        "block_ai_voice": _to_bool(rules.get("block_ai_voice"), True),
        "block_ai_light": _to_bool(rules.get("block_ai_light"), True),
        "block_ai_screen": _to_bool(rules.get("block_ai_screen"), True),
        "block_ai_pc_agent": _to_bool(rules.get("block_ai_pc_agent"), True),
        "allow_gentle_pillow_adjust": _to_bool(rules.get("allow_gentle_pillow_adjust"), True),
        "alert_method": str(rules.get("alert_method") or "phone_only")[:40],
    }

    memory = settings.get("memory") or []
    if isinstance(memory, str):
        memory = [line.strip() for line in memory.splitlines()]
    settings["memory"] = [
        str(item).strip()[:140]
        for item in memory
        if str(item).strip()
    ][:20]

    settings["version"] = 1
    settings["updated_at"] = str(settings.get("updated_at") or "")
    return settings


def _write_settings(settings: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(SETTINGS_PATH), exist_ok=True)
    tmp_path = SETTINGS_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp_path, SETTINGS_PATH)


def load_user_settings() -> dict[str, Any]:
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        settings = normalize_settings(DEFAULT_SETTINGS)
        settings["updated_at"] = _now().isoformat(timespec="seconds")
        _write_settings(settings)
        return settings
    except Exception as exc:
        print(f"[Settings] load failed: {exc}")
        return normalize_settings(DEFAULT_SETTINGS)
    return normalize_settings(raw)


def save_user_settings(update: dict[str, Any]) -> dict[str, Any]:
    current = load_user_settings()
    settings = normalize_settings(_deep_merge(current, update or {}))
    settings["updated_at"] = _now().isoformat(timespec="seconds")
    _write_settings(settings)
    return settings


def _minutes(text: str) -> int:
    hour, minute = _time_text(text, "00:00").split(":", 1)
    return int(hour) * 60 + int(minute)


def _period_active(now_min: int, start: str, end: str) -> bool:
    start_min = _minutes(start)
    end_min = _minutes(end)
    if start_min == end_min:
        return True
    if start_min < end_min:
        return start_min <= now_min < end_min
    return now_min >= start_min or now_min < end_min


def get_quiet_status(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = settings or load_user_settings()
    now = _now()
    now_min = now.hour * 60 + now.minute
    active_period = None
    for period in settings.get("quiet_periods", []):
        if period.get("enabled") and _period_active(now_min, period["start"], period["end"]):
            active_period = period
            break
    return {
        "active": active_period is not None,
        "period": active_period,
        "now": now.isoformat(timespec="seconds"),
        "timezone": TIMEZONE,
        "rules": settings.get("quiet_rules", {}),
    }


def is_ai_voice_blocked(settings: dict[str, Any] | None = None) -> bool:
    settings = settings or load_user_settings()
    status = get_quiet_status(settings)
    return bool(status["active"] and settings["quiet_rules"].get("block_ai_voice"))


def is_ai_screen_blocked(settings: dict[str, Any] | None = None) -> bool:
    settings = settings or load_user_settings()
    status = get_quiet_status(settings)
    return bool(status["active"] and settings["quiet_rules"].get("block_ai_screen"))


def build_ai_context_prompt(settings: dict[str, Any] | None = None) -> str:
    settings = settings or load_user_settings()
    personality = settings["personality"]
    quiet_status = get_quiet_status(settings)
    period = quiet_status.get("period") or {}
    memory_lines = settings.get("memory") or []
    memory_text = "\n".join(f"- {item}" for item in memory_lines) or "- 暂无长期偏好。"

    quiet_text = "未处于睡眠勿扰时间段"
    if quiet_status["active"]:
        quiet_text = (
            f"当前处于{period.get('label', '睡眠勿扰')}时间段 "
            f"({period.get('start')} - {period.get('end')})"
        )

    return f"""

【永久用户设置】
- 助手名称：{personality['name']}
- 性格基调：{personality['tone']}
- 回复风格：{personality['style']}
- 主动性：{personality['initiative']}
- 性格描述：{personality['description']}

【用户长期偏好】
{memory_text}

【当前睡眠策略】
- {quiet_text}
- 这些设置是用户在 App 中保存的永久机制，不是临时聊天上下文。
- 睡眠勿扰时间段内，你必须低打扰：不要主动语音播报、不要主动亮屏、不要主动开灯/闪灯、不要主动让 PC Agent 做有打扰感的任务。
- 睡眠勿扰时间段内，只允许安静解释、手机端文字回复、静默记录，以及必要且轻微的枕头调节。
- 即使你认为某个动作有帮助，也必须服从云端策略拦截；不能用文字承诺已经执行被拦截的动作。
""".strip()


def guard_ai_action(action_type: str, **kwargs: Any) -> dict[str, Any]:
    settings = load_user_settings()
    quiet_status = get_quiet_status(settings)
    if not quiet_status["active"]:
        return {"allowed": True, "reason": "", "overrides": {}, "quiet_status": quiet_status}

    rules = settings["quiet_rules"]
    action = str(kwargs.get("action") or "").lower()

    if action_type == "led":
        brightness = kwargs.get("brightness_pct")
        is_off = action in {"off", "close", "shutdown"} or brightness == 0
        if rules.get("block_ai_light") and not is_off:
            return {
                "allowed": False,
                "reason": "现在是睡眠勿扰时间，我不会主动开灯、闪灯或改变灯效，以免打扰睡眠。需要开灯请在 App 手动控制。",
                "overrides": {},
                "quiet_status": quiet_status,
            }

    if action_type == "pc_agent" and rules.get("block_ai_pc_agent"):
        return {
            "allowed": False,
            "reason": "现在是睡眠勿扰时间，我不会主动让 PC Agent 执行可能打扰用户的任务。可以先把需求记录下来，等勿扰结束后再处理。",
            "overrides": {},
            "quiet_status": quiet_status,
        }

    if action_type == "pillow":
        if action in {"halt", "stop", "emergency_stop"}:
            return {"allowed": True, "reason": "", "overrides": {}, "quiet_status": quiet_status}
        if kwargs.get("target_kpa") is not None:
            return {
                "allowed": False,
                "reason": "现在是睡眠勿扰时间，我不会主动做目标气压的大幅调整。需要调整枕头请在 App 手动控制。",
                "overrides": {},
                "quiet_status": quiet_status,
            }
        if not rules.get("allow_gentle_pillow_adjust"):
            return {
                "allowed": False,
                "reason": "现在是睡眠勿扰时间，当前设置不允许 AI 主动调节枕头。",
                "overrides": {},
                "quiet_status": quiet_status,
            }
        duration = kwargs.get("duration_sec")
        try:
            duration_int = int(duration)
        except Exception:
            duration_int = 1
        return {
            "allowed": True,
            "reason": "已按睡眠勿扰策略降级为轻微枕头调节。",
            "overrides": {"duration_sec": max(0, min(2, duration_int))},
            "quiet_status": quiet_status,
        }

    return {"allowed": True, "reason": "", "overrides": {}, "quiet_status": quiet_status}
