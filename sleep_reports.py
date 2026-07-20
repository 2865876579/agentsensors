"""Persistent sleep-session tracking and cloud-only report generation."""

from __future__ import annotations

import html
import math
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


FSR_PERSON_THRESHOLD_N = 0.10
SAMPLE_INTERVAL_SEC = 10.0
OFF_PILLOW_GRACE_SEC = 90.0
MAX_UNKNOWN_GAP_SEC = 30 * 60.0
SNORE_MERGE_SEC = 30.0


def _number(value, default=None):
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _person_on_pillow(payload: dict) -> bool:
    for sensor in payload.get("fsr") or []:
        if not isinstance(sensor, dict) or sensor.get("valid") is False:
            continue
        if (_number(sensor.get("n"), 0.0) or 0.0) >= FSR_PERSON_THRESHOLD_N:
            return True
    return False


def _average(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _format_duration(seconds: float) -> str:
    minutes = max(0, round(seconds / 60))
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}小时{minutes}分钟"
    return f"{minutes}分钟"


class SleepHistory:
    def __init__(self, database_path: Path, timezone: str = "Asia/Shanghai"):
        self.database_path = Path(database_path)
        self.timezone = timezone
        self._lock = threading.RLock()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._lock, self._connect() as db:
            db.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS sleep_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT NOT NULL,
                    started_at REAL NOT NULL,
                    last_on_pillow_at REAL NOT NULL,
                    ended_at REAL,
                    status TEXT NOT NULL DEFAULT 'active'
                );
                CREATE INDEX IF NOT EXISTS idx_sleep_sessions_device_time
                    ON sleep_sessions(device_id, started_at DESC);
                CREATE TABLE IF NOT EXISTS sleep_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    sampled_at REAL NOT NULL,
                    heart_bpm REAL,
                    breath_bpm REAL,
                    motion_level REAL,
                    pressure_kpa REAL,
                    temperature_c REAL,
                    humidity_pct REAL,
                    FOREIGN KEY(session_id) REFERENCES sleep_sessions(id)
                );
                CREATE INDEX IF NOT EXISTS idx_sleep_samples_session_time
                    ON sleep_samples(session_id, sampled_at);
                CREATE TABLE IF NOT EXISTS sleep_snore_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    detected_at REAL NOT NULL,
                    score REAL,
                    FOREIGN KEY(session_id) REFERENCES sleep_sessions(id)
                );
                CREATE INDEX IF NOT EXISTS idx_sleep_snore_session_time
                    ON sleep_snore_events(session_id, detected_at);
                """
            )

    @staticmethod
    def person_on_pillow(payload: dict) -> bool:
        return _person_on_pillow(payload)

    def _active_session(self, db: sqlite3.Connection, device_id: str):
        return db.execute(
            "SELECT * FROM sleep_sessions WHERE device_id=? AND status='active' "
            "ORDER BY started_at DESC LIMIT 1",
            (device_id,),
        ).fetchone()

    def record_sensor(self, device_id: str, payload: dict, now: float | None = None) -> int | None:
        if not device_id or not isinstance(payload, dict):
            return None
        now = float(now or time.time())
        on_pillow = _person_on_pillow(payload)

        with self._lock, self._connect() as db:
            session = self._active_session(db, device_id)
            if not on_pillow:
                if session and now - float(session["last_on_pillow_at"]) >= OFF_PILLOW_GRACE_SEC:
                    db.execute(
                        "UPDATE sleep_sessions SET status='complete', ended_at=? WHERE id=?",
                        (float(session["last_on_pillow_at"]), int(session["id"])),
                    )
                return int(session["id"]) if session else None

            if session and now - float(session["last_on_pillow_at"]) > MAX_UNKNOWN_GAP_SEC:
                db.execute(
                    "UPDATE sleep_sessions SET status='complete', ended_at=? WHERE id=?",
                    (float(session["last_on_pillow_at"]), int(session["id"])),
                )
                session = None
            if session is None:
                cursor = db.execute(
                    "INSERT INTO sleep_sessions(device_id, started_at, last_on_pillow_at) VALUES(?,?,?)",
                    (device_id, now, now),
                )
                session_id = int(cursor.lastrowid)
            else:
                session_id = int(session["id"])
                db.execute(
                    "UPDATE sleep_sessions SET last_on_pillow_at=? WHERE id=?",
                    (now, session_id),
                )

            latest = db.execute(
                "SELECT sampled_at FROM sleep_samples WHERE session_id=? ORDER BY sampled_at DESC LIMIT 1",
                (session_id,),
            ).fetchone()
            if latest and now - float(latest["sampled_at"]) < SAMPLE_INTERVAL_SEC:
                return session_id

            radar_valid = payload.get("radar_valid") is True
            heart = _number(payload.get("heart_rate_bpm", payload.get("radar_heart_bpm"))) if radar_valid else None
            breath = _number(payload.get("breath_rate_bpm", payload.get("radar_breath_bpm"))) if radar_valid else None
            if heart is not None and not 30 <= heart <= 220:
                heart = None
            if breath is not None and not 4 <= breath <= 60:
                breath = None
            motion = _number(payload.get("body_motion", payload.get("motion_level"))) \
                if payload.get("body_motion_valid") is True else None
            pressure = _number(payload.get("pressure_kpa")) if payload.get("pressure_valid") is True else None
            temperature = _number(payload.get("temperature_c")) if payload.get("env_valid") is True else None
            humidity = _number(payload.get("humidity_pct")) if payload.get("env_valid") is True else None
            db.execute(
                "INSERT INTO sleep_samples(session_id,sampled_at,heart_bpm,breath_bpm,motion_level,"
                "pressure_kpa,temperature_c,humidity_pct) VALUES(?,?,?,?,?,?,?,?)",
                (session_id, now, heart, breath, motion, pressure, temperature, humidity),
            )
            return session_id

    def record_snore(self, device_id: str, event: dict, now: float | None = None) -> bool:
        if not device_id or not event.get("snore", True):
            return False
        now = float(now or time.time())
        with self._lock, self._connect() as db:
            session = self._active_session(db, device_id)
            if not session or now - float(session["last_on_pillow_at"]) > OFF_PILLOW_GRACE_SEC:
                return False
            latest = db.execute(
                "SELECT detected_at FROM sleep_snore_events WHERE session_id=? "
                "ORDER BY detected_at DESC LIMIT 1",
                (int(session["id"]),),
            ).fetchone()
            if latest and now - float(latest["detected_at"]) < SNORE_MERGE_SEC:
                return False
            db.execute(
                "INSERT INTO sleep_snore_events(session_id,detected_at,score) VALUES(?,?,?)",
                (int(session["id"]), now, _number(event.get("score"))),
            )
            return True

    def latest_session_id(self, device_id: str = "") -> int | None:
        where = "WHERE device_id=?" if device_id else ""
        params = (device_id,) if device_id else ()
        with self._lock, self._connect() as db:
            rows = db.execute(
                f"SELECT id,started_at,COALESCE(ended_at,last_on_pillow_at) AS finished_at "
                f"FROM sleep_sessions {where} ORDER BY started_at DESC LIMIT 10",
                params,
            ).fetchall()
        if not rows:
            return None
        for row in rows:
            if float(row["finished_at"]) - float(row["started_at"]) >= 5 * 60:
                return int(row["id"])
        return int(rows[0]["id"])

    def _local_datetime(self, timestamp: float) -> datetime:
        if ZoneInfo is not None:
            try:
                return datetime.fromtimestamp(timestamp, ZoneInfo(self.timezone))
            except Exception:
                pass
        return datetime.fromtimestamp(timestamp)

    @staticmethod
    def _motion_episodes(values: list[float | None], threshold: float = 20.0) -> int:
        episodes = 0
        active = False
        for value in values:
            above = value is not None and value >= threshold
            if above and not active:
                episodes += 1
            active = above
        return episodes

    @staticmethod
    def _chart(values: list[float | None], color: str, unit: str) -> str:
        valid = [value for value in values if value is not None]
        if len(valid) < 2:
            return '<div class="chart-empty">有效数据不足</div>'
        if len(values) > 120:
            step = math.ceil(len(values) / 120)
            values = values[::step]
        low, high = min(valid), max(valid)
        span = max(1.0, high - low)
        points = []
        for index, value in enumerate(values):
            if value is None:
                continue
            x = 8 + index * 584 / max(1, len(values) - 1)
            y = 108 - (value - low) * 88 / span
            points.append(f"{x:.1f},{y:.1f}")
        return (
            '<svg class="chart" viewBox="0 0 600 120" role="img">'
            f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" '
            'stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>'
            f'<text x="8" y="116">{low:.1f}{html.escape(unit)}</text>'
            f'<text x="520" y="16">{high:.1f}{html.escape(unit)}</text></svg>'
        )

    def build_report(self, session_id: int, output_dir: Path) -> dict:
        with self._lock, self._connect() as db:
            session = db.execute("SELECT * FROM sleep_sessions WHERE id=?", (session_id,)).fetchone()
            if session is None:
                raise LookupError("没有找到睡眠记录")
            samples = db.execute(
                "SELECT * FROM sleep_samples WHERE session_id=? ORDER BY sampled_at",
                (session_id,),
            ).fetchall()
            snore_events = db.execute(
                "SELECT detected_at,score FROM sleep_snore_events WHERE session_id=? ORDER BY detected_at",
                (session_id,),
            ).fetchall()
        if not samples:
            raise LookupError("这段记录没有可用的传感器样本")

        started_at = float(session["started_at"])
        ended_at = float(session["ended_at"] or session["last_on_pillow_at"])
        duration_sec = max(0.0, ended_at - started_at)
        hearts = [row["heart_bpm"] for row in samples if row["heart_bpm"] is not None]
        breaths = [row["breath_bpm"] for row in samples if row["breath_bpm"] is not None]
        motions = [row["motion_level"] for row in samples]
        motion_valid = [value for value in motions if value is not None]
        pressures = [row["pressure_kpa"] for row in samples if row["pressure_kpa"] is not None]
        temperatures = [row["temperature_c"] for row in samples if row["temperature_c"] is not None]
        humidities = [row["humidity_pct"] for row in samples if row["humidity_pct"] is not None]
        heart_avg = _average(hearts)
        breath_avg = _average(breaths)
        motion_avg = _average(motion_valid)
        motion_episodes = self._motion_episodes(motions)
        hours = max(duration_sec / 3600.0, 1 / 60)
        snore_count = len(snore_events)
        snore_per_hour = snore_count / hours

        advice: list[str] = []
        if duration_sec < 6 * 3600:
            advice.append("本次在床时长偏短，建议尽量为睡眠预留7到9小时。")
        elif duration_sec > 10 * 3600:
            advice.append("本次在床时间较长，如醒后仍疲惫，可结合连续多日趋势观察。")
        else:
            advice.append("本次在床时长处于较合理范围，请结合主观精神状态综合判断。")
        if snore_per_hour > 5:
            advice.append("鼾声事件较频繁，建议关注睡姿与鼻腔通畅；持续明显时可咨询专业医生。")
        if motion_episodes / hours > 10:
            advice.append("体动相对频繁，可检查枕头高度、床垫支撑和室内温湿度是否舒适。")
        if heart_avg is None or breath_avg is None:
            advice.append("生命体征有效样本不足，本次不据此评价生理状态。")
        elif not 50 <= heart_avg <= 100 or not 10 <= breath_avg <= 24:
            advice.append("平均心率或呼吸率偏离常见静息范围，设备数据仅供参考，如持续异常请复测或咨询专业人员。")
        if humidities and (_average(humidities) or 0) < 40:
            advice.append("夜间平均湿度偏低，可适当增加空气湿度。")

        enough = duration_sec >= 20 * 60
        if enough:
            duration_score = 35 if 7 <= hours <= 9 else 28 if 6 <= hours < 10 else 18
            snore_score = 20 if snore_per_hour <= 2 else 14 if snore_per_hour <= 5 else 7
            motion_rate = motion_episodes / hours
            motion_score = 20 if motion_rate <= 5 else 14 if motion_rate <= 10 else 7
            vital_score = 25 if heart_avg is not None and breath_avg is not None else 10
            score = duration_score + snore_score + motion_score + vital_score
            quality = "较好" if score >= 85 else "一般" if score >= 65 else "需关注"
        else:
            score = None
            quality = "记录时间不足"

        start_local = self._local_datetime(started_at)
        end_local = self._local_datetime(ended_at)
        def metric(value, suffix="", digits=1):
            return "无有效数据" if value is None else f"{value:.{digits}f}{suffix}"

        summary = (
            f"最近一次有效在床记录为{_format_duration(duration_sec)}，"
            f"鼾声{snore_count}次，体动{motion_episodes}次，"
            f"平均心率{metric(heart_avg, '次/分')}，平均呼吸{metric(breath_avg, '次/分')}。"
        )
        score_text = "--" if score is None else str(score)
        cards = [
            ("睡眠质量参考", f"{score_text}分 · {quality}"),
            ("有效在床时长", _format_duration(duration_sec)),
            ("鼾声次数", f"{snore_count}次"),
            ("体动次数", f"{motion_episodes}次"),
            ("平均心率", metric(heart_avg, "次/分")),
            ("平均呼吸", metric(breath_avg, "次/分")),
        ]
        cards_html = "".join(
            f'<div class="metric"><span>{html.escape(label)}</span><strong>{html.escape(value)}</strong></div>'
            for label, value in cards
        )
        advice_html = "".join(f"<li>{html.escape(item)}</li>" for item in advice)
        snore_items = []
        snore_times = []
        for event in snore_events:
            detected = self._local_datetime(float(event["detected_at"]))
            snore_times.append(detected.isoformat(timespec="seconds"))
            score_value = _number(event["score"])
            confidence = "" if score_value is None else f" · 置信度{score_value * 100:.1f}%"
            snore_items.append(
                f'<li><time>{detected:%Y-%m-%d %H:%M:%S}</time>{html.escape(confidence)}</li>'
            )
        snore_timeline = (
            '<ol class="events">' + "".join(snore_items) + "</ol>"
            if snore_items else '<div class="chart-empty">本次记录中没有鼾声事件</div>'
        )
        report_html = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>小安睡眠质量报告</title><style>
body{{margin:0;background:#f4f7f6;color:#17211f;font:15px/1.65 system-ui,-apple-system,"Microsoft YaHei",sans-serif}}
.page{{max-width:860px;margin:auto;background:#fff;min-height:100vh;padding:32px;box-sizing:border-box}}
h1{{font-size:28px;margin:0 0 4px}}h2{{font-size:18px;margin:28px 0 10px;border-bottom:1px solid #dbe4e1;padding-bottom:6px}}
.period{{color:#60706c}}.grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin:22px 0}}
.metric{{border:1px solid #dbe4e1;border-radius:6px;padding:12px;background:#f8fbfa}}.metric span{{display:block;color:#667672;font-size:13px}}
.metric strong{{display:block;margin-top:4px;font-size:18px}}.chart{{width:100%;height:130px;background:#f8fbfa;border:1px solid #e3ebe8}}
.chart text{{font-size:11px;fill:#71807c}}.chart-empty{{padding:24px;color:#788783;background:#f8fbfa}}
li{{margin:7px 0}}.events{{columns:2;padding-left:24px}}.events time{{font-variant-numeric:tabular-nums}}
.note{{margin-top:28px;padding:12px;background:#f2f5f4;color:#5c6a67;font-size:13px;border-radius:6px}}
@media(max-width:640px){{.page{{padding:20px}}.grid{{grid-template-columns:repeat(2,1fr)}}}}
</style></head><body><main class="page">
<h1>小安睡眠质量报告</h1>
<div class="period">{start_local:%Y-%m-%d %H:%M} 至 {end_local:%Y-%m-%d %H:%M}</div>
<div class="grid">{cards_html}</div>
<h2>生命体征趋势</h2>
<p>心率</p>{self._chart([row['heart_bpm'] for row in samples], '#d95757', '次/分')}
<p>呼吸率</p>{self._chart([row['breath_bpm'] for row in samples], '#287d73', '次/分')}
<h2>体动趋势</h2>{self._chart(motions, '#d08a28', '')}
<h2>鼾声时间</h2>{snore_timeline}
<h2>睡眠参考建议</h2><ul>{advice_html}</ul>
<div class="note">本报告仅在有效FSR压力达到{FSR_PERSON_THRESHOLD_N:.1f}N后记录。“在床时长”不等同于医学睡眠时长；心率、呼吸、鼾声和体动均为设备测量结果，仅供个人健康管理参考，不用于诊断。</div>
</main></body></html>"""

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        original_name = f"小安睡眠报告_{start_local:%Y%m%d_%H%M}.html"
        path = output_dir / f"sleep_{session_id}.html"
        path.write_text(report_html, encoding="utf-8")
        return {
            "session_id": session_id,
            "path": path,
            "original_name": original_name,
            "summary": summary,
            "metrics": {
                "started_at": started_at,
                "ended_at": ended_at,
                "duration_sec": round(duration_sec),
                "score": score,
                "quality": quality,
                "heart_avg": round(heart_avg, 1) if heart_avg is not None else None,
                "breath_avg": round(breath_avg, 1) if breath_avg is not None else None,
                "motion_avg": round(motion_avg, 1) if motion_avg is not None else None,
                "motion_episodes": motion_episodes,
                "snore_count": snore_count,
                "snore_times": snore_times,
                "pressure_avg": round(_average(pressures), 2) if pressures else None,
                "temperature_avg": round(_average(temperatures), 1) if temperatures else None,
                "humidity_avg": round(_average(humidities), 1) if humidities else None,
                "sample_count": len(samples),
            },
        }
