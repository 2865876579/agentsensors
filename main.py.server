"""
智能枕头云端服务 - WebSocket 主入口

整体架构：
  ESP32（录音/播放） <--WebSocket--> 本服务（STT+LLM+TTS） <--WebSocket--> PC Agent（控制电脑）

本服务负责：
  1. 接收 ESP32 上传的音频，调用讯飞 STT 转成文字
  2. 文字送 DeepSeek LLM，返回回复文本 + 可选的电脑控制命令
  3. 回复文本走 Edge TTS 合成语音，回传 ESP32 播放
  4. 如果 LLM 返回了电脑控制命令，转发给已连接的 PC Agent 执行

WebSocket 端点：
  /ws/esp32     - ESP32 设备连接入口
  /ws/pc_agent  - PC Agent 连接入口
  /health       - HTTP 健康检查
"""
import sys
import json
import math
import asyncio
import base64
import re
import uuid
import opuslib
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

# Windows 控制台默认 GBK，强制 UTF-8 避免 emoji 打印崩溃
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
import time
import urllib.parse
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from config import SERVER_HOST, SERVER_PORT, TIMEZONE
from stt_xunfei import recognize, recognize_queue
from llm_deepseek import (
    chat_stream,
    classify_alarm_request,
    classify_didi_ride_request,
    classify_music_request,
    classify_pre_sleep_light_reply,
    generate_automation_reply,
    set_pc_command_callback,
    set_pillow_callback,
    set_led_callback,
    set_ir_device_callback,
    set_read_sensors_callback,
    set_didi_ride_link_callback,
)
from tts_volc import synthesize
from netease_music import find_playable_song, iter_song_opus_frames
from web_search import search_web  # 搜索工具，供后续 function calling 工具接入时使用
from user_settings import (
    get_quiet_status,
    get_upcoming_quiet_period,
    is_ai_screen_blocked,
    is_ai_voice_blocked,
    load_user_settings,
    save_user_settings,
)
from avatar_image2 import (
    current_preview_path,
    current_rgb666_path,
    generate_lcd_avatar,
    get_current_avatar_manifest,
)
from didi_mcp import create_basic_ride_link


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_sensor_poll_task()
    ensure_alarm_scheduler_task()
    yield


app = FastAPI(title="Smart Pillow Cloud Server", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
APP_VERSION = "xiaozhi_realtime_v5"

WAKE_TRIGGER_TEXT = "__wake__"
WAKE_REPLY_TEXT = "我在，你说。"
SLEEP_GREETING_TRIGGER_TEXT = "用户刚刚躺下了，请温柔地主动问候一句"
EXIT_REPLY_TEXT = "好的，再见小安。"
EXIT_PHRASES = (
    "再见小安",
    "小安再见",
    "拜拜小安",
    "退出对话",
    "结束对话",
)

PCM_FRAME_BYTES = 1280
OPUS_SAMPLE_RATE = 16000
OPUS_CHANNELS = 1
OPUS_FRAME_SAMPLES = 960

# 存储已连接的 PC Agent，key 是连接 id，value 是 WebSocket 对象
# 当 LLM 返回电脑控制命令时，会从这里取一个 Agent 转发命令
pc_agents: dict[str, WebSocket] = {}

# 存储已连接的 ESP32 客户端，PC Agent 回传结果时用于播报到设备。
esp32_clients: dict[str, WebSocket] = {}
esp32_send_locks: dict[str, asyncio.Lock] = {}
esp32_sessions: dict[str, dict] = {}
pc_command_contexts: dict[str, dict] = {}
_pc_command_futures: dict[str, asyncio.Future] = {}  # LLM → PC Agent 往返
_sensor_futures: dict[str, asyncio.Future] = {}     # LLM → ESP32 传感器读取
last_active_esp32_id: str | None = None

# Mobile H5 clients and latest ESP32 sensor cache.
app_clients: dict[str, WebSocket] = {}
app_chat_histories: dict[str, list[dict]] = {}
latest_sensor_data: dict | None = None
latest_snore_event: dict | None = None
sensor_poll_task: asyncio.Task | None = None
alarm_scheduler_task: asyncio.Task | None = None
alarm_runtime: dict = {
    "active": False,
    "stage": "idle",
    "alarm_id": "",
    "message": "未触发",
    "started_at": 0.0,
}
SENSOR_POLL_INTERVAL_SEC = 2.0
device_tts_busy_until: dict[str, float] = {}
TTS_PLAYBACK_COOLDOWN_SEC = 4.0

PRE_SLEEP_WINDOW_MINUTES = 10
SLEEP_GREETING_WINDOW_MINUTES = 180
PRE_SLEEP_FSR_PRESSURE_THRESHOLD_N = 2.0
ALARM_FSR_ON_THRESHOLD_N = 1.0
ALARM_FSR_OFF_THRESHOLD_N = 0.6
PRE_SLEEP_LIGHT_THRESHOLD_LUX = 140.0
PRE_SLEEP_REPLY_TIMEOUT_SEC = 30.0
AIR_BAD_PPM_THRESHOLD = 2.0
AIR_BAD_RESET_PPM = 1.5
DRY_HUMIDITY_THRESHOLD = 54.0
DRY_HUMIDITY_RESET_PCT = 55.0
SNORE_TARGET_DELTA_KPA = 0.8
SNORE_DEFAULT_TARGET_KPA = 4.0
SNORE_POLICY_COOLDOWN_SEC = 300

automation_states: dict[str, dict] = {}


async def _pc_command_cb(action: str, params: dict, client_id: str, turn_id: int) -> str:
    """LLM 调 pc_command 工具时的回调：发命令到 PC Agent 并等结果。"""
    if not pc_agents:
        return "电脑助手未连接，请先在电脑上运行 pc_agent.py"

    command_id = f"{client_id}:{turn_id}:{int(time.time() * 1000)}"
    future: asyncio.Future = asyncio.get_event_loop().create_future()
    _pc_command_futures[command_id] = future

    sent = await send_pc_command({"action": action, "params": params}, client_id, turn_id)
    if not sent:
        _pc_command_futures.pop(command_id, None)
        return "发送命令失败，电脑可能断开了"

    try:
        result = await asyncio.wait_for(future, timeout=20.0)
        return result
    except asyncio.TimeoutError:
        return "电脑操作超时，请确认 PC Agent 还在运行"
    finally:
        _pc_command_futures.pop(command_id, None)


async def _pillow_cb(action: str, duration_sec: int, client_id: str, turn_id: int,
                     target_kpa=None) -> str:
    """LLM 调 pillow_control 工具时：发 WebSocket 命令到 ESP32。"""
    target = pick_esp32_client(client_id)
    if not target:
        return "ESP32 未连接"
    payload = {"type": "pillow_cmd", "action": action, "duration_sec": duration_sec}
    if target_kpa is not None:
        target_value = float(target_kpa)
        if not math.isfinite(target_value):
            return "目标压力无效"
        payload["target_kpa"] = max(0.0, min(10.0, target_value))
    ok = await send_json_to_esp32(target, payload)
    return "已发送枕头指令" if ok else "发送失败"


def _clamp_int(value, default: int, low: int, high: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return max(low, min(high, number))


async def _led_cb(action: str, mode: str, color: str, brightness_pct, speed_pct,
                  duration_sec, client_id: str, turn_id: int) -> str:
    """LLM 调 led_control 工具时：把语义灯效参数转发到 ESP32。"""
    target = pick_esp32_client(client_id)
    if not target:
        return "ESP32 未连接"

    payload = {
        "type": "led_cmd",
        "action": action or "set",
    }
    if mode:
        payload["mode"] = mode
    if color:
        payload["color"] = color
    if brightness_pct is not None:
        payload["brightness_pct"] = _clamp_int(brightness_pct, 45, 0, 100)
    if speed_pct is not None:
        payload["speed_pct"] = _clamp_int(speed_pct, 35, 0, 100)
    if duration_sec is not None:
        payload["duration_sec"] = _clamp_int(duration_sec, 0, 0, 600)

    ok = await send_json_to_esp32(target, payload)
    return "已发送灯带指令" if ok else "发送失败"


def _normalize_ir_device(device: str) -> str:
    name = str(device or "").strip().lower()
    aliases = {
        "fan": "fan",
        "fengshan": "fan",
        "风扇": "fan",
        "humidifier": "humidifier",
        "humid": "humidifier",
        "jiashiqi": "humidifier",
        "加湿器": "humidifier",
        "air_conditioner": "air_conditioner",
        "air-conditioner": "air_conditioner",
        "airconditioner": "air_conditioner",
        "ac": "air_conditioner",
        "a/c": "air_conditioner",
        "aircon": "air_conditioner",
        "air_con": "air_conditioner",
        "kongtiao": "air_conditioner",
        "空调": "air_conditioner",
    }
    return aliases.get(name, name)


async def _ir_device_cb(device: str, action: str, client_id: str, turn_id: int) -> str:
    """LLM 调 ir_device_control 工具时：转发红外设备开关命令到 ESP32。"""
    target = pick_esp32_client(client_id)
    if not target:
        return "ESP32 未连接"

    device = _normalize_ir_device(device)
    action = (action or "").strip().lower()
    if device not in {"fan", "humidifier", "air_conditioner"}:
        return "只支持控制风扇、加湿器和空调"
    if action not in {"on", "off", "toggle"}:
        return "红外动作只支持 on/off/toggle"

    ok = await send_json_to_esp32(target, {
        "type": "ir_cmd",
        "device": device,
        "action": action,
    })
    return "已发送红外设备指令" if ok else "发送失败"


async def _read_sensors_cb(client_id: str, turn_id: int) -> str:
    """LLM 调 read_sensors 工具时：发 WebSocket 命令到 ESP32 并等待传感器数据。"""
    target = pick_esp32_client(client_id)
    if not target:
        return "ESP32 未连接"
    request_id = f"sensor:{target}:{turn_id}:{int(time.time() * 1000)}"
    future: asyncio.Future = asyncio.get_event_loop().create_future()
    _sensor_futures[request_id] = future

    ok = await send_json_to_esp32(target, {"type": "read_sensors", "request_id": request_id})
    if not ok:
        _sensor_futures.pop(request_id, None)
        return "发送传感器读取命令失败"

    try:
        result = await asyncio.wait_for(future, timeout=10.0)
        return result
    except asyncio.TimeoutError:
        return "传感器数据读取超时，请稍后重试"
    finally:
        _sensor_futures.pop(request_id, None)

# 注册回调

async def _didi_ride_link_cb(
    from_place: str,
    to_place: str,
    city: str,
    product_category: str,
    client_id: str,
    turn_id: int,
) -> str:
    """LLM tool callback: generate a DiDi basic ride link and broadcast it to H5."""
    result = await create_basic_ride_link(
        from_place=from_place,
        to_place=to_place,
        city=city,
        product_category=product_category,
    )
    payload = result.get("payload") if isinstance(result, dict) else None
    if payload:
        payload["client_id"] = client_id
        payload["turn_id"] = turn_id
        await broadcast_to_apps(payload)
        await send_screen_status(
            client_id,
            "状态：已生成滴滴打车链接，请在手机端打开确认。",
            event="didi_ride_link",
        )
    if isinstance(result, dict):
        return result.get("message") or "滴滴打车链接已生成，请在手机上完成确认和支付。"
    return "滴滴打车链接已生成，请在手机上完成确认和支付。"

set_pc_command_callback(_pc_command_cb)
set_pillow_callback(_pillow_cb)
set_led_callback(_led_cb)
set_ir_device_callback(_ir_device_cb)
set_read_sensors_callback(_read_sensors_cb)
set_didi_ride_link_callback(_didi_ride_link_cb)


def is_exit_phrase(text: str) -> bool:
    normalized = re.sub(r"[\s，。！？、,.!?~～]", "", text or "")
    return any(phrase in normalized for phrase in EXIT_PHRASES)


def is_sleep_greeting_trigger(text: str) -> bool:
    return str(text or "").strip() == SLEEP_GREETING_TRIGGER_TEXT


async def drop_esp32_client(client_id: str, reason: str = "") -> None:
    """Remove a stale ESP32 websocket so future commands target a healthy client."""
    global last_active_esp32_id
    websocket = esp32_clients.pop(client_id, None)
    esp32_send_locks.pop(client_id, None)
    session = esp32_sessions.pop(client_id, None) or {}
    device_tts_busy_until.pop(client_id, None)

    task = session.get("active_task")
    current = asyncio.current_task()
    if task and task is not current and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            print(f"[ESP32] stale task cleanup error: {exc}")

    if websocket is not None:
        try:
            await websocket.close()
        except Exception:
            pass

    if last_active_esp32_id == client_id:
        last_active_esp32_id = pick_esp32_client()
    if reason:
        print(f"[ESP32] removed stale client {client_id}: {reason}")


async def send_json_to_esp32(client_id: str, payload: dict) -> bool:
    """串行发送一条 JSON 消息到指定 ESP32，避免多任务并发写同一 WebSocket。"""
    websocket = esp32_clients.get(client_id)
    lock = esp32_send_locks.get(client_id)
    if websocket is None or lock is None:
        return False

    try:
        async with lock:
            await websocket.send_text(json.dumps(payload, ensure_ascii=False))
        return True
    except Exception as exc:
        await drop_esp32_client(client_id, f"json send failed: {exc}")
        return False


async def send_bytes_to_esp32(client_id: str, frame: bytes) -> bool:
    websocket = esp32_clients.get(client_id)
    lock = esp32_send_locks.get(client_id)
    if websocket is None or lock is None:
        return False
    try:
        async with lock:
            await websocket.send_bytes(frame)
        return True
    except Exception as exc:
        await drop_esp32_client(client_id, f"bytes send failed: {exc}")
        return False


def session_id_of(client_id: str) -> str:
    session = esp32_sessions.get(client_id) or {}
    return session.get("session_id", "")


async def send_tts_state_to_esp32(
    client_id: str,
    state: str,
    *,
    source: str = "assistant",
    turn_id: int | None = None,
    end_dialog: bool = False,
    text: str | None = None,
) -> bool:
    payload = {"type": "tts", "state": state}
    sid = session_id_of(client_id)
    if sid:
        payload["session_id"] = sid
    if state == "stop" and end_dialog:
        payload["dialog_end"] = True
    if source != "assistant":
        payload["source"] = source
    if turn_id is not None:
        payload["turn_id"] = turn_id
    if text is not None:
        payload["text"] = text
    ok = await send_json_to_esp32(client_id, payload)
    if ok:
        now = time.time()
        if state == "start":
            device_tts_busy_until[client_id] = now + 60.0
        elif state == "stop":
            device_tts_busy_until[client_id] = now + TTS_PLAYBACK_COOLDOWN_SEC
    return ok


async def send_tts_audio_frames_to_esp32(
    client_id: str,
    text: str,
    *,
    source: str = "assistant",
    turn_id: int | None = None,
    encoder=None,
) -> int:
    websocket = esp32_clients.get(client_id)
    lock = esp32_send_locks.get(client_id)
    if websocket is None or lock is None:
        return 0

    frame_count = 0
    async for frame in synthesize(text, encoder=encoder):
        if not frame:
            continue
        if not await send_bytes_to_esp32(client_id, frame):
            return frame_count
        frame_count += 1
    return frame_count


async def send_tts_stream_to_esp32(
    client_id: str,
    text: str,
    *,
    source: str = "assistant",
    turn_id: int | None = None,
    end_dialog: bool = False,
    wait_playback: bool = False,
) -> bool:
    """
    小安协议 TTS：
      - {"type":"tts","state":"start"}
      - binary Opus frames
      - {"type":"tts","state":"stop"}
    """
    if client_id not in esp32_clients:
        return False
    # ★ AI 回复不应该被 turn_id 拦截——后台任务可能跨多个录音周期

    print(f"[TTS-send] start, text_len={len(text)}")
    if not await send_tts_state_to_esp32(
        client_id, "start", source=source, turn_id=turn_id
    ):
        return False
    try:
        seq = await send_tts_audio_frames_to_esp32(
            client_id, text, source=source, turn_id=turn_id
        )
    except Exception as exc:
        print(f"[TTS-send] failed: {exc}")
        await send_tts_state_to_esp32(
            client_id, "stop", source=source, turn_id=turn_id, end_dialog=end_dialog
        )
        return False

    await send_tts_state_to_esp32(
        client_id, "stop", source=source, turn_id=turn_id, end_dialog=end_dialog
    )
    if seq <= 0:
        return False

    print(f"[TTS-send] 全部完成, {seq} 帧")
    if wait_playback:
        await asyncio.sleep(min(4.0, seq * 0.06 + 0.25))
    return True


async def send_music_frames_to_esp32(
    client_id: str,
    query: str,
    *,
    title: str = "",
    artist: str = "",
    kind: str = "",
    source: str = "music",
    turn_id: int | None = None,
    wait_before_audio: asyncio.Task | None = None,
) -> bool:
    """Search NetEase Cloud Music and play it through the existing Opus pipeline."""
    if client_id not in esp32_clients:
        return False

    query = (query or "").strip()
    if not query:
        return False

    async def wait_preroll() -> None:
        if wait_before_audio is None:
            return
        try:
            await wait_before_audio
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[Music] preroll TTS failed: {exc}")

    if kind == "artist" and artist:
        search_label = f"{artist}的可播放歌曲"
    elif title and artist:
        search_label = f"{artist}的《{title}》"
    else:
        search_label = f"《{query}》"

    await send_screen_status(client_id, f"状态：正在搜索{search_label}。", event="music_search")
    try:
        song = await find_playable_song(query, title=title, artist=artist, kind=kind)
    except Exception as exc:
        print(f"[Music] search failed: {exc}")
        await send_screen_status(client_id, "状态：音乐搜索失败。", event="music_error")
        await wait_preroll()
        await send_tts_stream_to_esp32(
            client_id,
            "这首歌暂时没有找到可信的可播放版本。",
            source=source,
            turn_id=turn_id,
        )
        return False

    if not song:
        await send_screen_status(client_id, f"状态：没有找到{search_label}的可信可播放版本。", event="music_not_found")
        if kind == "artist" and artist:
            fail_text = f"网易云这边暂时拿不到{artist}的可信可播放歌曲，我先不乱放。"
        elif title and artist:
            fail_text = f"网易云这边暂时拿不到{artist}的《{title}》，我先不乱放。"
        else:
            fail_text = "这首歌我暂时拿不到可信的可播放版本，先不乱放。"
        await wait_preroll()
        await send_tts_stream_to_esp32(
            client_id,
            fail_text,
            source=source,
            turn_id=turn_id,
        )
        return False

    print(f"[Music] play {song.label} id={song.id} br={song.br}")
    await send_screen_status(client_id, f"状态：正在播放《{song.name}》。", event="music_play")
    await wait_preroll()
    if not await send_tts_state_to_esp32(client_id, "start", source=source, turn_id=turn_id):
        return False

    frame_count = 0
    import opuslib as _opuslib
    encoder = _opuslib.Encoder(OPUS_SAMPLE_RATE, OPUS_CHANNELS, _opuslib.APPLICATION_AUDIO)
    try:
        encoder.bitrate = 24000
        encoder.complexity = 1
    except Exception:
        pass

    try:
        await send_tts_state_to_esp32(
            client_id,
            "sentence_start",
            source=source,
            turn_id=turn_id,
            text=f"正在播放：{song.label}",
        )
        websocket = esp32_clients.get(client_id)
        lock = esp32_send_locks.get(client_id)
        if websocket is None or lock is None:
            return False
        async for frame in iter_song_opus_frames(song, encoder=encoder):
            if not frame:
                continue
            if not await send_bytes_to_esp32(client_id, frame):
                return frame_count > 0
            frame_count += 1
            await asyncio.sleep(0.055)
    except asyncio.CancelledError:
        print(f"[Music] cancelled: {song.label}")
        raise
    except Exception as exc:
        print(f"[Music] play failed: {exc}")
        await send_screen_status(client_id, "状态：音乐播放失败。", event="music_error")
        return False
    finally:
        await send_tts_state_to_esp32(client_id, "stop", source=source, turn_id=turn_id)

    print(f"[Music] done, frames={frame_count}, song={song.label}")
    return frame_count > 0


async def answer_music_request_if_needed(client_id: str, text: str, turn_id: int) -> bool:
    """Handle play/stop music commands using an LLM semantic classifier."""
    music_intent = await classify_music_request(text)
    music_action = music_intent.get("action")
    music_query = (music_intent.get("query") or "").strip()
    music_title = (music_intent.get("title") or "").strip()
    music_artist = (music_intent.get("artist") or "").strip()
    music_kind = (music_intent.get("kind") or "").strip()
    if music_action not in {"play", "stop"}:
        return False

    if music_action == "stop":
        await send_tts_state_to_esp32(client_id, "stop", source="music", turn_id=turn_id)
        await send_screen_status(client_id, "状态：已停止播放音乐。", event="music_stop")
        await send_tts_stream_to_esp32(client_id, "音乐已停止。", source="music", turn_id=turn_id)
        return True

    ok = await send_music_frames_to_esp32(
        client_id,
        music_query,
        title=music_title,
        artist=music_artist,
        kind=music_kind,
        source="music",
        turn_id=turn_id,
    )
    if not ok:
        print(f"[Music] no playable result for query={music_query!r}")
    return True

async def send_pc_command(pc_command: dict, client_id: str, turn_id: int) -> bool:
    """把 LLM 产生的电脑控制命令转发给任意一个已连接的 PC Agent。"""
    if not pc_agents:
        return False

    command_id = f"{client_id}:{turn_id}:{int(time.time() * 1000)}"
    pc_command_contexts[command_id] = {
        "client_id": client_id,
        "turn_id": turn_id,
        "action": pc_command.get("action"),
        "created_at": time.monotonic(),
    }

    agent_ws = next(iter(pc_agents.values()))
    await agent_ws.send_text(json.dumps({
        "type": "pc_command",
        "client_id": client_id,
        "turn_id": turn_id,
        "command_id": command_id,
        "command": pc_command
    }, ensure_ascii=False))
    return True


def next_turn_id(client_id: str) -> int:
    session = esp32_sessions.setdefault(client_id, {"turn_id": 0})
    session["turn_id"] = int(session.get("turn_id", 0)) + 1
    return session["turn_id"]


def is_current_turn(client_id: str, turn_id: int | None) -> bool:
    if turn_id is None:
        return True
    session = esp32_sessions.get(client_id) or {}
    return int(session.get("turn_id", -1)) == int(turn_id)


def pick_esp32_client(client_id: str | None = None) -> str | None:
    """优先选择指定客户端，其次选择最近活跃客户端，最后选择任意在线 ESP32。"""
    if client_id and client_id in esp32_clients:
        return client_id
    if latest_sensor_data:
        sensor_client = latest_sensor_data.get("client_id")
        received_at = float(latest_sensor_data.get("received_at") or 0)
        if sensor_client in esp32_clients and time.time() - received_at < 10.0:
            return sensor_client
    if last_active_esp32_id and last_active_esp32_id in esp32_clients:
        return last_active_esp32_id
    active = [
        (float((esp32_sessions.get(cid) or {}).get("last_seen_at") or 0), cid)
        for cid in esp32_clients.keys()
    ]
    if active:
        return max(active)[1]
    return next(iter(esp32_clients.keys()), None)


async def broadcast_to_apps(payload: dict) -> None:
    """Broadcast JSON to all connected mobile H5 clients."""
    dead: list[str] = []
    text = json.dumps(payload, ensure_ascii=False)
    for app_id, ws in list(app_clients.items()):
        try:
            await ws.send_text(text)
        except Exception:
            dead.append(app_id)
    for app_id in dead:
        app_clients.pop(app_id, None)


def build_settings_update_for_ai_persona(persona: str) -> dict | None:
    """Map ESP32/TJC AI persona keys to the H5 permanent personality settings."""
    key = str(persona or "").strip().upper()
    if key == "GENTLE":
        return {
            "personality": {
                "tone": "gentle",
                "style": "balanced",
                "initiative": "normal",
                "description": "温柔陪伴、自然克制，像真实的枕边生活助手。",
            }
        }
    if key == "PRO":
        return {
            "personality": {
                "tone": "pro",
                "style": "short",
                "initiative": "normal",
                "description": "专业健康、简洁可靠，重点关注睡眠和传感器状态。",
            }
        }
    if key == "SHORT":
        return {
            "personality": {
                "tone": "short",
                "style": "short",
                "initiative": "low",
                "description": "简短直接、少打扰，只给必要提醒。",
            }
        }
    if key == "SLEEP":
        return {
            "personality": {
                "tone": "sleep",
                "style": "short",
                "initiative": "low",
                "description": "睡眠守护、低打扰、少出声，优先帮助用户放松入睡。",
            }
        }
    return None


async def handle_esp32_ai_persona_update(persona: str, client_id: str = "") -> dict | None:
    update = build_settings_update_for_ai_persona(persona)
    if not update:
        print(f"[ESP32] unknown ai_persona={persona!r} client={client_id}")
        return None

    settings = save_user_settings(update)
    payload = {
        "type": "settings_state",
        "ok": True,
        "source": "esp32_tjc",
        "ai_persona": str(persona or "").strip().upper(),
        "client_id": client_id,
        "settings": settings,
        "quiet_status": get_quiet_status(settings),
        "alarm_state": dict(alarm_runtime),
    }
    await push_snore_policy(client_id, settings=settings)
    await broadcast_to_apps(payload)
    print(
        f"[ESP32] ai_persona={payload['ai_persona']} saved "
        f"tone={settings.get('personality', {}).get('tone')} "
        f"style={settings.get('personality', {}).get('style')}"
    )
    return payload


async def handle_esp32_pillow_calibration_save(saved_kpa, client_id: str = "") -> dict | None:
    try:
        value = float(saved_kpa)
    except (TypeError, ValueError):
        print(f"[ESP32] invalid pillow calibration saved_kpa={saved_kpa!r} client={client_id}")
        return None
    if not math.isfinite(value):
        print(f"[ESP32] invalid pillow calibration saved_kpa={saved_kpa!r} client={client_id}")
        return None
    value = max(0.0, min(10.0, value))
    settings = save_user_settings({
        "pillow_calibration": {
            "saved_kpa": round(value, 1),
            "saved_at": datetime.now().isoformat(timespec="seconds"),
        }
    })
    payload = {
        "type": "settings_state",
        "ok": True,
        "source": "esp32_tjc",
        "event": "pillow_calibration_save",
        "client_id": client_id,
        "settings": settings,
        "quiet_status": get_quiet_status(settings),
        "alarm_state": dict(alarm_runtime),
    }
    await broadcast_to_apps(payload)
    print(f"[ESP32] pillow calibration saved {value:.1f} kPa client={client_id}")
    return payload


def _build_snore_policy(
    settings: dict | None = None,
    sensor_payload: dict | None = None,
) -> dict:
    settings = settings or load_user_settings()
    quiet_status = get_quiet_status(settings)
    period = quiet_status.get("period") or {}
    period_name = str(period.get("name") or "")
    in_sleep_period = bool(
        quiet_status.get("active") and period_name in {"night_sleep", "nap"}
    )
    on_pillow = _is_user_on_pillow(sensor_payload or {})
    calibration = settings.get("pillow_calibration") or {}
    enabled = bool(calibration.get("snore_adjust_enabled", True))
    saved_kpa = _safe_float(calibration.get("saved_kpa"), SNORE_DEFAULT_TARGET_KPA - SNORE_TARGET_DELTA_KPA)
    target_kpa = max(0.5, min(5.0, saved_kpa + SNORE_TARGET_DELTA_KPA))
    if not math.isfinite(target_kpa):
        target_kpa = SNORE_DEFAULT_TARGET_KPA
    return {
        "type": "snore_policy",
        "enabled": enabled,
        "sleep_active": bool(enabled and in_sleep_period and on_pillow),
        "target_kpa": round(target_kpa, 2),
        "cooldown_sec": SNORE_POLICY_COOLDOWN_SEC,
        "period_name": period_name,
        "on_pillow": on_pillow,
    }


async def push_snore_policy(
    client_id: str | None = None,
    *,
    sensor_payload: dict | None = None,
    settings: dict | None = None,
) -> bool:
    target = pick_esp32_client(client_id)
    if not target:
        return False
    payload = _build_snore_policy(settings=settings, sensor_payload=sensor_payload)
    return await send_json_to_esp32(target, payload)


async def request_sensor_data(client_id: str | None = None) -> bool:
    """Ask the active ESP32 to return one sensor_data frame."""
    target = pick_esp32_client(client_id)
    if not target:
        return False
    return await send_json_to_esp32(target, {
        "type": "read_sensors",
        "request_id": f"app-{int(time.time() * 1000)}",
    })


async def sensor_poll_loop() -> None:
    """Keep sensor data fresh for both H5 and cloud automations."""
    while True:
        try:
            if esp32_clients:
                await request_sensor_data()
        except Exception as e:
            print(f"[APP] sensor poll error: {e}")
        await asyncio.sleep(SENSOR_POLL_INTERVAL_SEC)


def ensure_sensor_poll_task() -> None:
    global sensor_poll_task
    if sensor_poll_task is None or sensor_poll_task.done():
        sensor_poll_task = asyncio.create_task(sensor_poll_loop())


def _get_automation_state(client_id: str) -> dict:
    state = automation_states.setdefault(
        client_id,
        {
            "pending_light_prompt": None,
            "last_pre_sleep_prompt_key": "",
            "last_sleep_greeting_day": "",
            "sleep_greeting_in_progress_day": "",
            "sleep_greeting_in_progress_until": 0.0,
            "last_on_pillow": None,
            "sleep_period_key": "",
            "sleep_period_started_on_pillow": False,
            "sleep_greeting_late_allowed_key": "",
            "fan_alarm_active": False,
            "humidifier_alarm_active": False,
            "worker_task": None,
        },
    )
    pending = state.get("pending_light_prompt")
    if pending and float(pending.get("expires_at") or 0) <= time.time():
        state["pending_light_prompt"] = None
    return state


def _safe_float(value, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if math.isfinite(result):
        return result
    return default


def _is_user_on_pillow(sensor_payload: dict) -> bool:
    for item in sensor_payload.get("fsr") or []:
        if isinstance(item, dict) and item.get("valid") is not False:
            if _safe_float(item.get("n")) >= PRE_SLEEP_FSR_PRESSURE_THRESHOLD_N:
                return True
    return False


def _current_sleep_quiet_key(quiet_status: dict) -> str:
    """Return a stable key for the active night-sleep period, otherwise empty."""
    if not quiet_status or not quiet_status.get("active"):
        return ""
    period = quiet_status.get("period") or {}
    if str(period.get("name") or "") != "night_sleep":
        return ""
    day_key = str(quiet_status.get("now") or "")[:10]
    return f"night_sleep:{day_key}:{period.get('start')}:{period.get('end')}"


def _update_sleep_arrival_state(client_id: str, sensor_payload: dict, settings: dict) -> None:
    """Track whether the user was already on the pillow when night sleep started.

    Night sleep is silent by default. If the user was not on the pillow at the
    sleep start window and later lies down, one gentle proactive reminder is allowed.
    """
    state = _get_automation_state(client_id)
    on_pillow = _is_user_on_pillow(sensor_payload)
    quiet_status = get_quiet_status(settings)
    sleep_key = _current_sleep_quiet_key(quiet_status)
    prev_on_pillow = state.get("last_on_pillow")

    if sleep_key:
        if state.get("sleep_period_key") != sleep_key:
            state["sleep_period_key"] = sleep_key
            state["sleep_period_started_on_pillow"] = bool(on_pillow)
            if on_pillow and prev_on_pillow is False:
                state["sleep_greeting_late_allowed_key"] = sleep_key
            elif not on_pillow:
                state["sleep_greeting_late_allowed_key"] = sleep_key
            else:
                state["sleep_greeting_late_allowed_key"] = ""
        elif prev_on_pillow is False and on_pillow and not state.get("sleep_period_started_on_pillow"):
            state["sleep_greeting_late_allowed_key"] = sleep_key
    else:
        state["sleep_period_key"] = ""
        state["sleep_period_started_on_pillow"] = False
        state["sleep_greeting_late_allowed_key"] = ""

    state["last_on_pillow"] = bool(on_pillow)


def _alarm_now() -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo(TIMEZONE))
    return datetime.now()


def _alarm_minutes(text: str) -> int:
    hour, minute = str(text or "00:00").split(":", 1)
    return int(hour) * 60 + int(minute)


def _alarm_repeat_matches(alarm: dict, now: datetime) -> bool:
    repeat = str(alarm.get("repeat") or "daily").lower()
    weekday = now.weekday()
    if repeat == "daily":
        return True
    if repeat == "workday":
        return weekday < 5
    if repeat == "weekend":
        return weekday >= 5
    return repeat == "once"


def _alarm_trigger_key(alarm: dict, now: datetime) -> str:
    return f"{now.date().isoformat()}:{alarm.get('id')}:{alarm.get('time')}"


def _alarm_sensor_payload(client_id: str | None = None) -> dict | None:
    if not latest_sensor_data:
        return None
    if client_id and latest_sensor_data.get("client_id") != client_id:
        return None
    if time.time() - float(latest_sensor_data.get("received_at") or 0) > 8.0:
        return None
    data = latest_sensor_data.get("data")
    return data if isinstance(data, dict) else None


def _alarm_user_on_pillow(sensor_payload: dict | None) -> bool | None:
    if not sensor_payload:
        return None
    fsr = sensor_payload.get("fsr") or []
    has_valid = False
    max_force = 0.0
    for item in fsr:
        if isinstance(item, dict) and item.get("valid") is not False:
            has_valid = True
            max_force = max(max_force, _safe_float(item.get("n")))
    if not has_valid:
        return None
    if max_force >= ALARM_FSR_ON_THRESHOLD_N:
        return True
    if max_force < ALARM_FSR_OFF_THRESHOLD_N:
        return False
    return True


def _alarm_max_force_n(sensor_payload: dict | None) -> float | None:
    if not sensor_payload:
        return None
    fsr = sensor_payload.get("fsr") or []
    values = [
        _safe_float(item.get("n"))
        for item in fsr
        if isinstance(item, dict) and item.get("valid") is not False
    ]
    return max(values) if values else None


def _alarm_pressure_kpa(sensor_payload: dict | None) -> float | None:
    if not sensor_payload:
        return None
    if sensor_payload.get("pressure_valid") is False:
        return None
    try:
        value = float(sensor_payload.get("pressure_kpa"))
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _alarm_target_reached(sensor_payload: dict | None, action: str, target_kpa: float) -> bool:
    if not sensor_payload:
        return False

    expected_action = "tilt_to" if action == "tilt" else "recover_to"
    last_pump = sensor_payload.get("last_pump")
    if isinstance(last_pump, dict) and last_pump.get("action") == expected_action:
        try:
            last_target = float(last_pump.get("target_kpa"))
            last_result = float(last_pump.get("result_kpa"))
        except (TypeError, ValueError):
            last_target = math.nan
            last_result = math.nan
        if math.isfinite(last_target) and abs(last_target - target_kpa) <= 0.08:
            if action == "tilt" and math.isfinite(last_result) and last_result >= target_kpa - 0.10:
                return True
            if action == "recover" and math.isfinite(last_result) and last_result <= target_kpa + 0.10:
                return True

    pressure = _alarm_pressure_kpa(sensor_payload)
    if pressure is None:
        return False
    if action == "tilt":
        return pressure >= target_kpa - 0.10
    return pressure <= target_kpa + 0.10


def _pick_alarm_client(client_id: str | None = None) -> str | None:
    """Alarm control should follow the ESP32 that is currently reporting sensors."""
    if latest_sensor_data:
        sensor_client = latest_sensor_data.get("client_id")
        received_at = float(latest_sensor_data.get("received_at") or 0)
        if sensor_client in esp32_clients and time.time() - received_at < 8.0:
            return sensor_client
    return pick_esp32_client(client_id)


async def _alarm_send_json(client_id: str | None, payload: dict, label: str) -> tuple[str | None, bool]:
    target = _pick_alarm_client(client_id)
    if not target:
        print(f"[Alarm] {label}: no ESP32 client, payload={payload}")
        return client_id, False
    if client_id and target != client_id:
        print(f"[Alarm] retarget ESP32 {client_id} -> {target}")

    ok = await send_json_to_esp32(target, payload)
    if not ok:
        retry = _pick_alarm_client(None)
        if retry and retry != target:
            print(f"[Alarm] {label}: retry on ESP32 {retry}")
            ok = await send_json_to_esp32(retry, payload)
            target = retry

    print(f"[Alarm] {label}: client={target} ok={ok} payload={payload}")
    return target, ok


async def _alarm_send_screen_status(client_id: str | None, text: str, event: str) -> str | None:
    target = _pick_alarm_client(client_id)
    if target:
        await send_screen_status(target, text, event=event)
    return target or client_id


async def broadcast_alarm_state(stage: str, message: str, alarm: dict | None = None) -> None:
    active = stage not in {"idle", "done", "skipped"}
    alarm_runtime.update({
        "active": active,
        "stage": stage,
        "alarm_id": str((alarm or {}).get("id") or ""),
        "message": message,
        "started_at": (alarm_runtime.get("started_at") or time.time()) if active else 0.0,
    })
    await broadcast_to_apps({
        "type": "alarm_state",
        "state": dict(alarm_runtime),
        "alarm": alarm or {},
    })


async def _alarm_wait_for_leave(
    client_id: str,
    *,
    timeout_sec: int | None,
    leave_confirm_sec: int,
    stage: str,
) -> tuple[bool, str]:
    start = time.time()
    leave_since = None
    while timeout_sec is None or time.time() - start < timeout_sec:
        client_id = _pick_alarm_client(client_id) or client_id
        await request_sensor_data(client_id)
        await asyncio.sleep(1.0)
        on_pillow = _alarm_user_on_pillow(_alarm_sensor_payload(client_id))
        max_force = _alarm_max_force_n(_alarm_sensor_payload(client_id))
        now = time.time()
        force_text = "unknown" if max_force is None else f"{max_force:.2f}N"
        elapsed = int(now - start)
        print(f"[Alarm] stage={stage} elapsed={elapsed}s client={client_id} on_pillow={on_pillow} fsr_max={force_text}")
        if on_pillow is False:
            leave_since = leave_since or now
            if now - leave_since >= leave_confirm_sec:
                print(f"[Alarm] stage={stage} leave confirmed after {leave_confirm_sec}s")
                return True, client_id
        else:
            leave_since = None
    return False, client_id


async def _alarm_wait_for_pillow_target(
    client_id: str,
    *,
    action: str,
    target_kpa: float,
    leave_confirm_sec: int,
    timeout_sec: int = 45,
) -> tuple[bool, str, bool]:
    start = time.time()
    leave_since = None
    while time.time() - start < timeout_sec:
        client_id = _pick_alarm_client(client_id) or client_id
        await request_sensor_data(client_id)
        await asyncio.sleep(1.0)
        sensor_payload = _alarm_sensor_payload(client_id)
        on_pillow = _alarm_user_on_pillow(sensor_payload)
        max_force = _alarm_max_force_n(sensor_payload)
        pressure = _alarm_pressure_kpa(sensor_payload)
        reached = _alarm_target_reached(sensor_payload, action, target_kpa)
        now = time.time()
        force_text = "unknown" if max_force is None else f"{max_force:.2f}N"
        pressure_text = "unknown" if pressure is None else f"{pressure:.2f}kPa"
        print(
            f"[Alarm] stage=pillow_wakeup action={action} target={target_kpa:.2f}kPa "
            f"client={client_id} on_pillow={on_pillow} fsr_max={force_text} "
            f"pressure={pressure_text} reached={reached}"
        )

        if on_pillow is False:
            leave_since = leave_since or now
            if now - leave_since >= leave_confirm_sec:
                print(f"[Alarm] pillow_wakeup leave confirmed after {leave_confirm_sec}s")
                return True, client_id, reached
        else:
            leave_since = None

        if reached:
            return False, client_id, True

    print(f"[Alarm] pillow_wakeup target timeout action={action} target={target_kpa:.2f}kPa")
    return False, client_id, False


async def _cancel_alarm_music(task: asyncio.Task | None, client_id: str, turn_id: int) -> None:
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            print(f"[Alarm] music task error: {exc}")
    await send_tts_state_to_esp32(client_id, "stop", source="alarm_music", turn_id=turn_id)


async def finish_alarm(client_id: str, alarm: dict, stage: str, message: str, turn_id: int) -> None:
    client_id, _ = await _alarm_send_json(
        client_id,
        {"type": "pillow_cmd", "action": "halt", "duration_sec": 0},
        f"finish/{stage}/halt",
    )
    await _alarm_send_screen_status(client_id, f"状态：{message}", event=f"alarm_{stage}")
    print(f"[Alarm] finish stage={stage} message={message} client={client_id} turn={turn_id}")
    alarm_runtime.update({"active": False, "stage": stage, "message": message, "started_at": 0.0})
    await broadcast_alarm_state(stage, message, alarm)


async def run_alarm(alarm: dict, client_id: str, turn_id: int) -> None:
    alarm_runtime.update({
        "active": True,
        "stage": "music_wakeup",
        "alarm_id": str(alarm.get("id") or ""),
        "message": "闹钟已触发",
        "started_at": time.time(),
    })
    leave_confirm = int(alarm.get("leave_confirm_seconds") or 5)
    music_seconds = int(alarm.get("music_stage_seconds") or 30)
    high_kpa = max(0.0, min(10.0, float(alarm.get("pillow_high_kpa") or 5.0)))
    low_kpa = max(0.0, min(10.0, float(alarm.get("pillow_low_kpa") or 0.7)))
    song_query = str(alarm.get("song_query") or "小半").strip() or "小半"
    music_task = None

    try:
        client_id = _pick_alarm_client(client_id) or client_id
        print(
            f"[Alarm] start id={alarm.get('id')} client={client_id} turn={turn_id} "
            f"song={song_query} music_stage={music_seconds}s leave_confirm={leave_confirm}s "
            f"pillow={high_kpa:.2f}kPa->{low_kpa:.2f}kPa"
        )
        await request_sensor_data(client_id)
        await asyncio.sleep(1.0)
        on_pillow = _alarm_user_on_pillow(_alarm_sensor_payload(client_id))
        max_force = _alarm_max_force_n(_alarm_sensor_payload(client_id))
        print(f"[Alarm] initial on_pillow={on_pillow} fsr_max={max_force} client={client_id}")
        if on_pillow is False:
            await finish_alarm(client_id, alarm, "skipped", "闹钟已到，检测到无人，已跳过", turn_id)
            return

        await broadcast_alarm_state("music_wakeup", "闹钟响起，正在播放起床音乐", alarm)
        client_id = await _alarm_send_screen_status(
            client_id,
            "状态：闹钟响起，正在播放起床音乐。",
            event="alarm_music",
        ) or client_id
        client_id, _ = await _alarm_send_json(
            client_id,
            {
                "type": "led_cmd",
                "action": "set",
                "enabled": True,
                "mode": "solid",
                "color": "warm",
                "brightness_pct": 35,
            },
            "music_wakeup/led",
        )
        music_task = asyncio.create_task(
            send_music_frames_to_esp32(
                client_id,
                song_query,
                source="alarm_music",
                turn_id=turn_id,
            )
        )
        left, client_id = await _alarm_wait_for_leave(
            client_id,
            timeout_sec=music_seconds,
            leave_confirm_sec=leave_confirm,
            stage="music_wakeup",
        )
        if left:
            await _cancel_alarm_music(music_task, client_id, turn_id)
            await finish_alarm(client_id, alarm, "done", "已离枕5秒，闹钟暂停", turn_id)
            return

        await broadcast_alarm_state("pillow_wakeup", "仍未起床，枕头正在强制唤醒", alarm)
        client_id = await _alarm_send_screen_status(
            client_id,
            "状态：仍未起床，枕头正在强制唤醒。",
            event="alarm_pillow",
        ) or client_id
        print("[Alarm] enter pillow_wakeup, music continues unless the stream ends")

        while True:
            for action, target_kpa in (("tilt", high_kpa), ("recover", low_kpa)):
                client_id = _pick_alarm_client(client_id) or client_id
                if music_task is None or music_task.done():
                    if music_task is not None:
                        try:
                            print(f"[Alarm] music task ended result={music_task.result()}")
                        except asyncio.CancelledError:
                            print("[Alarm] music task was cancelled")
                        except Exception as exc:
                            print(f"[Alarm] music task ended error={exc}")
                    print(f"[Alarm] restart music in pillow_wakeup song={song_query} client={client_id}")
                    music_task = asyncio.create_task(
                        send_music_frames_to_esp32(
                            client_id,
                            song_query,
                            source="alarm_music",
                            turn_id=turn_id,
                        )
                    )

                action_text = "升高到" if action == "tilt" else "降低到"
                await broadcast_alarm_state(
                    f"pillow_{action}",
                    f"强唤醒：枕头{action_text}{target_kpa:.1f}kPa",
                    alarm,
                )
                client_id = await _alarm_send_screen_status(
                    client_id,
                    f"状态：闹钟强唤醒，枕头{action_text}{target_kpa:.1f}kPa。",
                    event=f"alarm_pillow_{action}",
                ) or client_id
                client_id, ok = await _alarm_send_json(
                    client_id,
                    {
                        "type": "pillow_cmd",
                        "action": action,
                        "duration_sec": 0,
                        "target_kpa": target_kpa,
                    },
                    f"pillow_wakeup/{action}",
                )
                if not ok:
                    await asyncio.sleep(1.0)
                    continue

                left, client_id, reached = await _alarm_wait_for_pillow_target(
                    client_id,
                    action=action,
                    target_kpa=target_kpa,
                    leave_confirm_sec=leave_confirm,
                )
                if left:
                    await _cancel_alarm_music(music_task, client_id, turn_id)
                    await finish_alarm(client_id, alarm, "done", "已离枕5秒，闹钟暂停", turn_id)
                    return
                if not reached:
                    print(f"[Alarm] continue cycle after target miss action={action} target={target_kpa:.2f}kPa")
    except asyncio.CancelledError:
        await _cancel_alarm_music(music_task, client_id, turn_id)
        await _alarm_send_json(
            client_id,
            {"type": "pillow_cmd", "action": "halt", "duration_sec": 0},
            "cancel/halt",
        )
        raise
    except Exception as exc:
        print(f"[Alarm] run failed: {exc}")
        await _cancel_alarm_music(music_task, client_id, turn_id)
        await finish_alarm(client_id, alarm, "done", "闹钟执行异常，已停止", turn_id)


async def maybe_trigger_alarm() -> None:
    if alarm_runtime.get("active"):
        return
    settings = load_user_settings()
    alarms = settings.get("alarms") or []
    now = _alarm_now()
    now_min = now.hour * 60 + now.minute
    target = pick_esp32_client()
    if not target:
        return

    for alarm in alarms:
        if not alarm.get("enabled"):
            continue
        if not _alarm_repeat_matches(alarm, now):
            continue
        try:
            alarm_min = _alarm_minutes(str(alarm.get("time") or "00:00"))
        except Exception:
            continue
        if alarm_min != now_min:
            continue
        trigger_key = _alarm_trigger_key(alarm, now)
        if alarm.get("last_triggered_key") == trigger_key:
            continue

        updated_alarms = []
        for item in alarms:
            updated = dict(item)
            if updated.get("id") == alarm.get("id"):
                updated["last_triggered_key"] = trigger_key
                if updated.get("repeat") == "once":
                    updated["enabled"] = False
            updated_alarms.append(updated)
        settings = save_user_settings({"alarms": updated_alarms})
        await broadcast_to_apps({
            "type": "settings_state",
            "settings": settings,
            "quiet_status": get_quiet_status(settings),
            "alarm_state": dict(alarm_runtime),
        })

        await cancel_active_task(target)
        turn_id = next_turn_id(target)
        task = asyncio.create_task(run_alarm(dict(alarm), target, turn_id))
        esp32_sessions.setdefault(target, {})["active_task"] = task
        return


async def alarm_scheduler_loop() -> None:
    while True:
        try:
            await maybe_trigger_alarm()
        except Exception as exc:
            print(f"[Alarm] scheduler error: {exc}")
        await asyncio.sleep(2.0)


def ensure_alarm_scheduler_task() -> None:
    global alarm_scheduler_task
    if alarm_scheduler_task is None or alarm_scheduler_task.done():
        alarm_scheduler_task = asyncio.create_task(alarm_scheduler_loop())


def _alarm_time_from_intent(intent: dict) -> tuple[datetime | None, str]:
    now = _alarm_now()
    relative_minutes = int(intent.get("relative_minutes") or 0)
    if relative_minutes > 0:
        target = now + timedelta(minutes=relative_minutes)
        return target, f"{relative_minutes}分钟后"

    time_text = str(intent.get("time") or "").strip()
    try:
        hour_text, minute_text = time_text.split(":", 1)
        hour = max(0, min(23, int(hour_text)))
        minute = max(0, min(59, int(minute_text)))
    except Exception:
        return None, ""

    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target, target.strftime("%H:%M")


def _format_alarm_reply(target: datetime, song_query: str, relative_text: str) -> str:
    song = song_query.strip() or "默认音乐"
    if relative_text.endswith("分钟后"):
        return f"好，{relative_text}用《{song}》叫你。"
    return f"好，已设好{target.strftime('%H:%M')}的闹钟，用《{song}》唤醒。"


async def handle_alarm_request_if_needed(
    client_id: str,
    text: str,
    turn_id: int,
    *,
    allow_voice: bool = True,
    source: str = "alarm_setting",
) -> str | None:
    intent = await classify_alarm_request(text)
    action = intent.get("action")
    if action == "none":
        return None

    settings = load_user_settings()
    alarms = list(settings.get("alarms") or [])
    current = dict(alarms[0]) if alarms else {
        "id": "wake_alarm",
        "enabled": False,
        "time": "07:30",
        "repeat": "daily",
        "song_query": "小半",
        "music_stage_seconds": 30,
        "leave_confirm_seconds": 5,
        "pillow_up_seconds": 3,
        "pillow_down_seconds": 3,
        "pillow_high_kpa": 5.0,
        "pillow_low_kpa": 0.7,
        "last_triggered_key": "",
    }

    if action == "cancel":
        current["enabled"] = False
        current["last_triggered_key"] = ""
        updated = save_user_settings({"alarms": [current] + alarms[1:]})
        await broadcast_to_apps({
            "type": "settings_state",
            "settings": updated,
            "quiet_status": get_quiet_status(updated),
            "alarm_state": dict(alarm_runtime),
        })
        reply = "好，闹钟先关掉。"
        await send_screen_status(client_id, "状态：闹钟已关闭。", event="alarm_setting")
        if allow_voice:
            await send_tts_stream_to_esp32(client_id, reply, source=source, turn_id=turn_id)
        return reply

    target, relative_text = _alarm_time_from_intent(intent)
    if target is None:
        reply = "我没听清具体时间，你再说一遍几点或多久后。"
        if allow_voice:
            await send_tts_stream_to_esp32(client_id, reply, source=source, turn_id=turn_id)
        return reply

    song_query = str(intent.get("song_query") or "").strip() or str(current.get("song_query") or "小半")
    current.update({
        "enabled": True,
        "time": target.strftime("%H:%M"),
        "repeat": str(intent.get("repeat") or "once"),
        "song_query": song_query,
        "music_stage_seconds": int(current.get("music_stage_seconds") or 30),
        "leave_confirm_seconds": int(current.get("leave_confirm_seconds") or 5),
        "pillow_up_seconds": int(current.get("pillow_up_seconds") or 3),
        "pillow_down_seconds": int(current.get("pillow_down_seconds") or 3),
        "pillow_high_kpa": float(current.get("pillow_high_kpa") or 5.0),
        "pillow_low_kpa": float(current.get("pillow_low_kpa") or 0.7),
        "last_triggered_key": "",
    })
    updated = save_user_settings({"alarms": [current] + alarms[1:]})
    await broadcast_to_apps({
        "type": "settings_state",
        "settings": updated,
        "quiet_status": get_quiet_status(updated),
        "alarm_state": dict(alarm_runtime),
    })

    reply = _format_alarm_reply(target, song_query, relative_text)
    await send_screen_status(
        client_id,
        f"状态：已设置{target.strftime('%H:%M')}闹钟，用《{song_query}》唤醒。",
        event="alarm_setting",
    )
    print(f"[AlarmSetting] set time={current['time']} repeat={current['repeat']} song={song_query!r}")
    if allow_voice:
        await send_tts_stream_to_esp32(client_id, reply, source=source, turn_id=turn_id)
    return reply


def _update_cached_switch_state(client_id: str, key: str, value) -> None:
    global latest_sensor_data
    if latest_sensor_data and latest_sensor_data.get("client_id") == client_id:
        sensor_data = latest_sensor_data.setdefault("data", {})
        sensor_data[key] = value


def _dialog_busy(client_id: str) -> bool:
    if time.time() < float(device_tts_busy_until.get(client_id) or 0):
        return True
    session = esp32_sessions.get(client_id) or {}
    active_task = session.get("active_task")
    if active_task and not active_task.done():
        return True
    return bool(session.get("incoming_audio"))


async def send_screen_status(client_id: str, text: str, *, event: str = "") -> bool:
    text = str(text or "").strip()
    if not text:
        return False
    payload = {"type": "screen_status", "msg": text}
    if event:
        payload["event"] = event
    ok = await send_json_to_esp32(client_id, payload) if client_id else False
    await broadcast_to_apps({
        "type": "automation_status",
        "client_id": client_id,
        "event": event,
        "text": text,
    })
    return ok


async def _emit_automation_notice(
    client_id: str,
    text: str,
    *,
    event: str,
    allow_voice: bool,
) -> bool:
    await send_screen_status(client_id, text, event=event)
    if not allow_voice:
        return False
    if _dialog_busy(client_id):
        print(f"[AUTO] skip voice while dialog busy: {event}")
        return False
    return await send_tts_stream_to_esp32(client_id, text, source="automation")


async def request_one_shot_listen(client_id: str, reason: str) -> bool:
    return await send_json_to_esp32(client_id, {
        "type": "listen_once",
        "reason": reason,
    })


async def _apply_pre_sleep_light_action(client_id: str, action: str) -> bool:
    if action == "off":
        ok = await send_json_to_esp32(client_id, {"type": "led_cmd", "action": "off"})
        if ok:
            _update_cached_switch_state(client_id, "led_enabled", False)
            _update_cached_switch_state(client_id, "led_brightness", 0)
            _update_cached_switch_state(client_id, "led_brightness_pct", 0)
        return ok

    if action == "dim":
        ok = await send_json_to_esp32(
            client_id,
            {
                "type": "led_cmd",
                "action": "set",
                "mode": "solid",
                "color": "warm",
                "brightness_pct": 18,
                "speed_pct": 20,
            },
        )
        if ok:
            _update_cached_switch_state(client_id, "led_enabled", True)
            _update_cached_switch_state(client_id, "led_brightness_pct", 18)
            _update_cached_switch_state(client_id, "led_mode", "solid")
            _update_cached_switch_state(client_id, "led_color", "warm")
        return ok

    return False


async def _consume_pre_sleep_light_reply(
    client_id: str,
    user_text: str,
    *,
    allow_voice: bool,
) -> str | None:
    state = _get_automation_state(client_id)
    pending = state.get("pending_light_prompt")
    if not pending:
        return None

    state["pending_light_prompt"] = None
    action = await classify_pre_sleep_light_reply(user_text)

    if action == "off":
        ok = await _apply_pre_sleep_light_action(client_id, "off")
        reply = "已帮你关掉灯带。" if ok else "我收到关灯请求了，但灯带暂时没有响应。"
    elif action == "dim":
        ok = await _apply_pre_sleep_light_action(client_id, "dim")
        reply = "已把灯光调暗一点。" if ok else "我收到调暗请求了，但灯带暂时没有响应。"
    else:
        reply = "好，那我先不动灯光。"

    await _emit_automation_notice(
        client_id,
        reply,
        event="pre_sleep_light_reply",
        allow_voice=allow_voice,
    )
    return reply


async def _maybe_prompt_pre_sleep_light(client_id: str, sensor_payload: dict, settings: dict) -> None:
    upcoming = get_upcoming_quiet_period(settings, PRE_SLEEP_WINDOW_MINUTES)
    quiet_status = get_quiet_status(settings)
    in_sleep_period = bool(quiet_status.get("active"))
    if not upcoming.get("active") and not in_sleep_period:
        return

    state = _get_automation_state(client_id)
    if upcoming.get("active"):
        window_key = str(upcoming.get("window_key") or "")
        reply = "快到睡觉时间了，房间光线有点亮，要我帮你关灯或调暗吗？"
    else:
        period = quiet_status.get("period") or {}
        now_key = str(quiet_status.get("now") or "")[:10]
        period_name = str(period.get("name") or "sleep")
        window_key = f"sleep-light:{period_name}:{now_key}"
        reply = "你已经躺下了，房间光线偏亮，要我帮你关灯或调暗吗？"

    if window_key and state.get("last_pre_sleep_prompt_key") == window_key:
        return
    if state.get("pending_light_prompt"):
        return
    if _dialog_busy(client_id):
        return

    light_lux = _safe_float(sensor_payload.get("light_lux"))
    if not _is_user_on_pillow(sensor_payload):
        return
    if light_lux < PRE_SLEEP_LIGHT_THRESHOLD_LUX:
        return

    state["last_pre_sleep_prompt_key"] = window_key or f"pre-sleep:{int(time.time())}"
    state["pending_light_prompt"] = {
        "expires_at": time.time() + PRE_SLEEP_REPLY_TIMEOUT_SEC,
        "window_key": window_key,
    }
    allow_voice = not is_ai_voice_blocked(settings)
    voice_sent = await _emit_automation_notice(
        client_id,
        reply,
        event="pre_sleep_light_prompt",
        allow_voice=allow_voice,
    )
    if voice_sent:
        await request_one_shot_listen(client_id, "pre_sleep_light_reply")


async def _maybe_auto_control_environment(client_id: str, sensor_payload: dict, settings: dict) -> None:
    state = _get_automation_state(client_id)
    allow_voice = not is_ai_voice_blocked(settings)

    if sensor_payload.get("mq135_valid"):
        ppm = _safe_float(sensor_payload.get("mq135_ppm"))
        if ppm >= AIR_BAD_PPM_THRESHOLD and not state.get("fan_alarm_active"):
            state["fan_alarm_active"] = True
            await send_json_to_esp32(client_id, {
                "type": "ir_cmd",
                "device": "fan",
                "action": "on",
            })
            _update_cached_switch_state(client_id, "fan_on", True)
            reply = await generate_automation_reply(
                f"空气质量已经偏差，我准备打开风扇处理一下。当前读数大约是 {ppm:.2f}。"
                "请用一句自然的话告诉用户你已经在处理空气问题。"
            )
            await _emit_automation_notice(
                client_id,
                reply,
                event="auto_fan_on",
                allow_voice=allow_voice,
            )
        elif ppm < AIR_BAD_RESET_PPM and state.get("fan_alarm_active"):
            state["fan_alarm_active"] = False
            await send_json_to_esp32(client_id, {
                "type": "ir_cmd",
                "device": "fan",
                "action": "off",
            })
            _update_cached_switch_state(client_id, "fan_on", False)
            reply = await generate_automation_reply(
                f"空气已经恢复正常，我准备把风扇关掉。当前读数大约是 {ppm:.2f}。"
                "请用一句自然的话告诉用户风扇已经关掉。"
            )
            await _emit_automation_notice(
                client_id,
                reply,
                event="auto_fan_off",
                allow_voice=allow_voice,
            )
        elif ppm < AIR_BAD_RESET_PPM:
            state["fan_alarm_active"] = False

    if sensor_payload.get("env_valid"):
        humidity = _safe_float(sensor_payload.get("humidity_pct"))
        if humidity <= DRY_HUMIDITY_THRESHOLD and not state.get("humidifier_alarm_active"):
            state["humidifier_alarm_active"] = True
            await send_json_to_esp32(client_id, {
                "type": "ir_cmd",
                "device": "humidifier",
                "action": "on",
            })
            _update_cached_switch_state(client_id, "humidifier_on", True)
            reply = await generate_automation_reply(
                f"环境开始偏干了，我准备打开加湿器。当前湿度大约是 {humidity:.1f}% 。"
                "请用一句自然的话告诉用户你已经在处理干燥问题。"
            )
            await _emit_automation_notice(
                client_id,
                reply,
                event="auto_humidifier_on",
                allow_voice=allow_voice,
            )
        elif humidity >= DRY_HUMIDITY_RESET_PCT and state.get("humidifier_alarm_active"):
            state["humidifier_alarm_active"] = False
            await send_json_to_esp32(client_id, {
                "type": "ir_cmd",
                "device": "humidifier",
                "action": "off",
            })
            _update_cached_switch_state(client_id, "humidifier_on", False)
            reply = await generate_automation_reply(
                f"湿度已经回到合适范围，我准备把加湿器关掉。当前湿度大约是 {humidity:.1f}% 。"
                "请用一句自然的话告诉用户加湿器已经关掉。"
            )
            await _emit_automation_notice(
                client_id,
                reply,
                event="auto_humidifier_off",
                allow_voice=allow_voice,
            )
        elif humidity >= DRY_HUMIDITY_RESET_PCT:
            state["humidifier_alarm_active"] = False


async def evaluate_sensor_automations(client_id: str, sensor_payload: dict) -> None:
    if not client_id or not isinstance(sensor_payload, dict):
        return
    settings = load_user_settings()
    _update_sleep_arrival_state(client_id, sensor_payload, settings)
    await _maybe_prompt_pre_sleep_light(client_id, sensor_payload, settings)
    if settings.get("quiet_rules", {}).get("allow_sleep_environment_control", True):
        await _maybe_auto_control_environment(client_id, sensor_payload, settings)


def schedule_sensor_automations(client_id: str, sensor_payload: dict) -> None:
    state = _get_automation_state(client_id)
    task = state.get("worker_task")
    if task and not task.done():
        return
    state["worker_task"] = asyncio.create_task(
        evaluate_sensor_automations(client_id, dict(sensor_payload or {}))
    )


async def handle_sleep_greeting_trigger(
    client_id: str,
    history: list[dict],
    settings: dict,
) -> None:
    state = _get_automation_state(client_id)
    quiet_status = get_quiet_status(settings)
    day_key = str(quiet_status.get("now") or "")[:10]
    sleep_key = _current_sleep_quiet_key(quiet_status)
    voice_blocked = is_ai_voice_blocked(settings)

    if voice_blocked and sleep_key and state.get("sleep_greeting_late_allowed_key") != sleep_key:
        await send_json_to_esp32(client_id, {"type": "status"})
        print(f"[SleepGreeting] night sleep is silent; skip proactive voice key={sleep_key}")
        return

    if day_key and state.get("last_sleep_greeting_day") == day_key:
        await send_json_to_esp32(client_id, {"type": "status"})
        print(f"[就寝] 今日已主动问候过，跳过: {day_key}")
        return

    now = time.time()
    if (
        day_key
        and state.get("sleep_greeting_in_progress_day") == day_key
        and float(state.get("sleep_greeting_in_progress_until") or 0) > now
    ):
        print(f"[就寝] 主动问候正在生成/播放，跳过重复触发: {day_key}")
        return

    state["sleep_greeting_in_progress_day"] = day_key
    state["sleep_greeting_in_progress_until"] = now + 45.0
    try:
        await send_screen_status(client_id, "状态：今日首次躺下，正在进行关怀问候。", event="sleep_greeting")
        print(f"[就寝] 开始生成主动问候: {day_key}")
        reply = await generate_automation_reply(
            "压力传感器检测到用户刚刚躺下，这是今天第一次进入有人模式。"
            "请用一句非常自然、低打扰的躺下关怀问候用户，像真实枕边助手。"
            "不要提传感器、系统、检测、模式，也不要催促用户回答。",
            fallback="躺好啦？我在这儿陪着你。",
        )
        reply = reply.strip() or "躺好啦？今天辛苦了，我在这儿陪着你。"

        print(f"[就寝] 主动问候回复: {reply}")
        ok = await send_tts_stream_to_esp32(client_id, reply, source="sleep_greeting")
        print(f"[就寝] 主动问候 TTS ok={ok}")
        if ok:
            state["last_sleep_greeting_day"] = day_key
            state["sleep_greeting_late_allowed_key"] = ""
            history.append({"role": "user", "content": SLEEP_GREETING_TRIGGER_TEXT})
            history.append({"role": "assistant", "content": reply})
            await request_one_shot_listen(client_id, "sleep_greeting_reply")
    except asyncio.CancelledError:
        print(f"[就寝] 主动问候被取消，未计入今日已问候: {day_key}")
        raise
    finally:
        if state.get("sleep_greeting_in_progress_day") == day_key:
            state["sleep_greeting_in_progress_day"] = ""
            state["sleep_greeting_in_progress_until"] = 0.0


def extract_search_query_from_url(url: str) -> str | None:
    """Return query text if URL is a search-engine result page."""
    parsed = urllib.parse.urlparse(url)
    host = parsed.netloc.lower()
    params = urllib.parse.parse_qs(parsed.query)

    search_param_names: tuple[str, ...] | None = None
    if "baidu.com" in host:
        search_param_names = ("wd", "word", "q")
    elif "bing.com" in host:
        search_param_names = ("q",)
    elif "google." in host:
        search_param_names = ("q",)
    elif "duckduckgo.com" in host:
        search_param_names = ("q",)
    elif "sogou.com" in host:
        search_param_names = ("query", "keyword", "q")
    elif "so.com" in host or "haosou.com" in host:
        search_param_names = ("q",)

    if not search_param_names:
        return None

    for name in search_param_names:
        values = params.get(name)
        if values and values[0].strip():
            return urllib.parse.unquote_plus(values[0]).strip()
    return None



def _should_flush_tts(text: str, is_first: bool = False) -> int:
    """★ xiaozhi 指针法：返回从 text 开头可 flush 的字符数，0=继续缓冲。
       不 strip()，调用方用指针跟踪已发位置，空白自然占位不丢偏移。"""
    if not text:
        return 0
    last = text[-1]
    if last in "。！？!?；;：:":
        return len(text)
    if is_first:
        visible = text.lstrip()
        if visible and visible[-1] in "，、…～":
            return len(text)
    if len(text) >= 50:
        for i in range(len(text) - 2, max(0, len(text) - 35), -1):
            if text[i] in "。！？!?；;：:，、…～":
                return i + 1
        if len(text) >= 80:
            return len(text)
    return 0


async def handle_ai_stream_result(client_id: str, user_text: str, history: list[dict], turn_id: int) -> None:
    text_buffer = ""          # 全部原始输出，不清空
    processed = 0             # 已发送到的位置（指针）
    full_reply = ""
    started_tts = False
    total_frames = 0
    is_first_sentence = True

    # ★ 一个 LLM 回复内共享编码器，避免冷启动首帧 -4/-2
    import opuslib as _opuslib
    encoder = _opuslib.Encoder(16000, 1, _opuslib.APPLICATION_VOIP)

    try:
        async for delta in chat_stream(user_text, history,
                                        client_id=client_id, turn_id=turn_id):
            full_reply += delta
            text_buffer += delta

            while True:
                new_part = text_buffer[processed:]
                n = _should_flush_tts(new_part, is_first=is_first_sentence)
                if not n:
                    break

                sentence = text_buffer[processed:processed + n].strip()
                processed += n
                if not sentence:
                    continue

                if not started_tts:
                    started_tts = await send_tts_state_to_esp32(client_id, "start", turn_id=turn_id)
                if started_tts:
                    await send_tts_state_to_esp32(
                        client_id, "sentence_start", turn_id=turn_id, text=sentence
                    )
                    total_frames += await send_tts_audio_frames_to_esp32(
                        client_id, sentence, turn_id=turn_id, encoder=encoder
                    )
                is_first_sentence = False

        remaining = text_buffer[processed:].strip()
        if remaining:
            if not started_tts:
                started_tts = await send_tts_state_to_esp32(client_id, "start", turn_id=turn_id)
            if started_tts:
                await send_tts_state_to_esp32(
                    client_id, "sentence_start", turn_id=turn_id, text=remaining
                )
                total_frames += await send_tts_audio_frames_to_esp32(
                    client_id, remaining, turn_id=turn_id, encoder=encoder
                )
    finally:
        if started_tts:
            await send_tts_state_to_esp32(client_id, "stop", turn_id=turn_id)

    print(f"[LLM-stream] frames={total_frames}, reply={full_reply.strip()!r}")


async def send_app_message(websocket: WebSocket, payload: dict) -> bool:
    try:
        await websocket.send_text(json.dumps(payload, ensure_ascii=False))
        return True
    except Exception as exc:
        print(f"[APP] send failed: {exc}")
        return False


def quick_music_preroll_text(text: str) -> str:
    """Fast UX hint only; the real music intent and query still come from the LLM."""
    raw = (text or "").strip()
    compact = re.sub(r"\s+", "", raw)
    if not compact:
        return ""

    stop_words = ("停止播放", "暂停播放", "停止音乐", "暂停音乐", "关闭音乐", "关掉音乐", "停歌", "别放了")
    if any(word in compact for word in stop_words):
        return "我先停掉音乐。"

    play_words = ("播放", "放一首", "放首", "播一首", "播首", "来一首", "来首", "听一首", "听首")
    if not any(word in compact for word in play_words):
        return ""

    query = raw
    query = re.sub(r"^(?:小安)?(?:帮我|给我|可以)?(?:播放|放一首|放首|播一首|播首|来一首|来首|听一首|听首)", "", query).strip()
    query = re.sub(r"(?:这首歌|这首|歌曲|音乐)$", "", query).strip()
    query = query.strip("《》“”\"' ，。！？,.!?")
    if 1 <= len(query) <= 18:
        return f"我先帮你找《{query}》。"
    return "我先帮你找一下。"


async def handle_didi_ride_request_by_ai(
    websocket: WebSocket,
    text: str,
    request_id: str,
    *,
    client_id: str = "",
    turn_id: int = 0,
    source: str = "app_chat",
    quiet_status: dict | None = None,
) -> bool:
    """AI 语义识别打车需求；识别为打车后调用滴滴 MCP 基础版。"""
    intent = await classify_didi_ride_request(text)
    if intent.get("action") != "ride":
        return False

    await send_app_message(websocket, {
        "type": "app_chat_start",
        "request_id": request_id,
        "esp32_connected": bool(esp32_clients),
        "device_tts": False,
        "quiet_status": quiet_status,
        "source": source,
    })

    result = await create_basic_ride_link(
        from_place=intent.get("from_place", ""),
        to_place=intent.get("to_place", ""),
        city=intent.get("city", ""),
        product_category=intent.get("product_category", ""),
    )
    reply = result.get("message") if isinstance(result, dict) else ""
    if not reply:
        reply = "滴滴打车链接已生成，请在手机上完成确认和支付。"

    payload = result.get("payload") if isinstance(result, dict) else None
    if payload:
        payload["client_id"] = client_id
        payload["turn_id"] = turn_id
        payload["request_id"] = request_id
        payload["source"] = source
        await broadcast_to_apps(payload)
        if client_id:
            await send_screen_status(
                client_id,
                "状态：已生成滴滴打车链接，请在手机端打开确认。",
                event="didi_ride_link",
            )

    await send_app_message(websocket, {
        "type": "app_chat_delta",
        "request_id": request_id,
        "delta": reply,
        "text": reply,
        "source": source,
    })
    await send_app_message(websocket, {
        "type": "app_chat_done",
        "request_id": request_id,
        "text": reply,
        "turn_id": turn_id,
        "device_tts": False,
        "quiet_status": quiet_status,
        "source": source,
    })
    return True



async def handle_pc_agent_task_once(
    websocket: WebSocket,
    text: str,
    request_id: str | None = None,
    session_id: str | None = None,
) -> None:
    """MCP/PC Agent panel chat: run cloud tools without requiring ESP32 online."""
    user_text = (text or "").strip()
    request_id = request_id or f"mcp-task-{int(time.time() * 1000)}"
    if not user_text:
        return

    await send_app_message(websocket, {
        "type": "app_chat_start",
        "request_id": request_id,
        "esp32_connected": bool(esp32_clients),
        "device_tts": False,
        "source": "pc_agent_task",
    })

    history_key = f"mcp:{(session_id or str(id(websocket))).strip() or str(id(websocket))}"
    history = app_chat_histories.setdefault(history_key, [])
    full_reply = ""
    target = pick_esp32_client() or ""
    turn_id = next_turn_id(target) if target else int(time.time() * 1000) % 1000000000

    try:
        if await handle_didi_ride_request_by_ai(
            websocket,
            user_text,
            request_id,
            client_id=target,
            turn_id=turn_id,
            source="pc_agent_task",
        ):
            return

        async for delta in chat_stream(user_text, history, client_id=target, turn_id=turn_id):
            if not delta:
                continue
            full_reply += delta
            await send_app_message(websocket, {
                "type": "app_chat_delta",
                "request_id": request_id,
                "delta": delta,
                "text": full_reply,
                "source": "pc_agent_task",
            })

        await send_app_message(websocket, {
            "type": "app_chat_done",
            "request_id": request_id,
            "text": full_reply.strip() or "任务已完成。",
            "turn_id": turn_id,
            "device_tts": False,
            "source": "pc_agent_task",
        })
    except Exception as exc:
        print(f"[PC-Agent-task] error: {exc}")
        await send_app_message(websocket, {
            "type": "app_chat_error",
            "request_id": request_id,
            "error": str(exc)[:160],
            "source": "pc_agent_task",
        })


async def handle_app_chat_once(
    websocket: WebSocket,
    text: str,
    request_id: str | None = None,
    session_id: str | None = None,
) -> None:
    """One-shot mobile text chat: reply by app text + ESP32 TTS/subtitle, without starting listen mode."""
    user_text = (text or "").strip()
    request_id = request_id or f"app-chat-{int(time.time() * 1000)}"
    if not user_text:
        return

    settings = load_user_settings()
    quiet_status = get_quiet_status(settings)

    # 滴滴打车是云端 MCP 能力，不依赖 ESP32 在线；先做 AI 语义分类。
    if await handle_didi_ride_request_by_ai(
        websocket,
        user_text,
        request_id,
        client_id="",
        turn_id=int(time.time() * 1000) % 1000000000,
        source="app_chat",
        quiet_status=quiet_status,
    ):
        return

    target = pick_esp32_client()
    if not target:
        await send_app_message(websocket, {
            "type": "app_chat_error",
            "request_id": request_id,
            "error": "ESP32 not connected",
        })
        return

    allow_device_tts = not is_ai_voice_blocked(settings)
    allow_device_status = not is_ai_screen_blocked(settings)

    pending_reply = await _consume_pre_sleep_light_reply(
        target,
        user_text,
        allow_voice=allow_device_tts,
    )
    if pending_reply is not None:
        await send_app_message(websocket, {
            "type": "app_chat_start",
            "request_id": request_id,
            "esp32_connected": True,
            "device_tts": allow_device_tts,
            "quiet_status": quiet_status,
        })
        await send_app_message(websocket, {
            "type": "app_chat_delta",
            "request_id": request_id,
            "delta": pending_reply,
            "text": pending_reply,
        })
        await send_app_message(websocket, {
            "type": "app_chat_done",
            "request_id": request_id,
            "text": pending_reply,
            "turn_id": 0,
            "device_tts": allow_device_tts,
            "quiet_status": quiet_status,
        })
        return

    turn_id = next_turn_id(target)
    await cancel_active_task(target)
    if allow_device_status:
        await send_json_to_esp32(target, {
            "type": "status",
            "text": user_text,
            "source": "app_chat",
            "turn_id": turn_id,
        })

    await send_app_message(websocket, {
        "type": "app_chat_start",
        "request_id": request_id,
        "esp32_connected": True,
        "device_tts": allow_device_tts,
        "quiet_status": quiet_status,
    })

    alarm_reply = await handle_alarm_request_if_needed(
        target,
        user_text,
        turn_id,
        allow_voice=allow_device_tts,
        source="app_chat",
    )
    if alarm_reply is not None:
        await send_app_message(websocket, {
            "type": "app_chat_delta",
            "request_id": request_id,
            "delta": alarm_reply,
            "text": alarm_reply,
        })
        await send_app_message(websocket, {
            "type": "app_chat_done",
            "request_id": request_id,
            "text": alarm_reply,
            "turn_id": turn_id,
            "device_tts": allow_device_tts,
            "quiet_status": quiet_status,
        })
        return

    quick_music_reply = quick_music_preroll_text(user_text)
    quick_music_preroll_task = None
    if quick_music_reply:
        await send_app_message(websocket, {
            "type": "app_chat_delta",
            "request_id": request_id,
            "delta": quick_music_reply,
            "text": quick_music_reply,
        })
        if allow_device_tts:
            quick_music_preroll_task = asyncio.create_task(
                send_tts_stream_to_esp32(
                    target,
                    quick_music_reply,
                    source="app_chat",
                    turn_id=turn_id,
                    wait_playback=True,
                )
            )

    music_intent = await classify_music_request(user_text)
    music_action = music_intent.get("action")
    music_query = (music_intent.get("query") or "").strip()
    music_title = (music_intent.get("title") or "").strip()
    music_artist = (music_intent.get("artist") or "").strip()
    music_kind = (music_intent.get("kind") or "").strip()
    if music_action in {"play", "stop"}:
        if allow_device_status:
            await send_json_to_esp32(target, {
                "type": "status",
                "text": user_text,
                "source": "app_chat",
                "turn_id": turn_id,
            })

        if music_action == "stop":
            reply = "音乐已停止。"
            await send_tts_state_to_esp32(target, "stop", source="music", turn_id=turn_id)
            await send_screen_status(target, "状态：已停止播放音乐。", event="music_stop")
            if allow_device_tts:
                await send_tts_stream_to_esp32(target, reply, source="app_chat", turn_id=turn_id)
        else:
            if music_kind == "artist" and music_artist:
                reply = f"我先找一首{music_artist}能播的歌。"
            elif music_title and music_artist:
                reply = f"我先找{music_artist}的《{music_title}》。"
            else:
                reply = f"我先帮你找《{music_query}》。"

            async def _music_task() -> None:
                preroll_task = quick_music_preroll_task
                if preroll_task is None and allow_device_tts:
                    preroll_task = asyncio.create_task(
                        send_tts_stream_to_esp32(
                            target,
                            reply,
                            source="app_chat",
                            turn_id=turn_id,
                            wait_playback=True,
                        )
                    )
                await send_music_frames_to_esp32(
                    target,
                    music_query,
                    title=music_title,
                    artist=music_artist,
                    kind=music_kind,
                    source="app_chat_music",
                    turn_id=turn_id,
                    wait_before_audio=preroll_task,
                )

            esp32_sessions[target]["active_task"] = asyncio.create_task(_music_task())

        await send_app_message(websocket, {
            "type": "app_chat_delta",
            "request_id": request_id,
            "delta": reply,
            "text": reply,
        })
        await send_app_message(websocket, {
            "type": "app_chat_done",
            "request_id": request_id,
            "text": reply,
            "turn_id": turn_id,
            "device_tts": allow_device_tts,
            "quiet_status": quiet_status,
        })
        return

    history_key = (session_id or str(id(websocket))).strip() or str(id(websocket))
    history = app_chat_histories.setdefault(history_key, [])
    text_buffer = ""
    processed = 0
    full_reply = ""
    started_tts = False
    is_first_sentence = True
    total_frames = 0

    import opuslib as _opuslib
    encoder = _opuslib.Encoder(16000, 1, _opuslib.APPLICATION_VOIP)

    try:
        if is_exit_phrase(user_text):
            full_reply = EXIT_REPLY_TEXT
            await send_app_message(websocket, {
                "type": "app_chat_delta",
                "request_id": request_id,
                "delta": full_reply,
                "text": full_reply,
            })
            if allow_device_tts:
                started_tts = await send_tts_state_to_esp32(target, "start", source="app_chat", turn_id=turn_id)
            if started_tts:
                await send_tts_state_to_esp32(
                    target, "sentence_start", source="app_chat", turn_id=turn_id, text=full_reply
                )
                total_frames += await send_tts_audio_frames_to_esp32(
                    target, full_reply, source="app_chat", turn_id=turn_id, encoder=encoder
                )
        else:
            async for delta in chat_stream(user_text, history, client_id=target, turn_id=turn_id):
                if not delta:
                    continue
                full_reply += delta
                text_buffer += delta
                await send_app_message(websocket, {
                    "type": "app_chat_delta",
                    "request_id": request_id,
                    "delta": delta,
                    "text": full_reply,
                })

                while True:
                    new_part = text_buffer[processed:]
                    n = _should_flush_tts(new_part, is_first=is_first_sentence)
                    if not n:
                        break
                    sentence = text_buffer[processed:processed + n].strip()
                    processed += n
                    if not sentence:
                        continue
                    if allow_device_tts and not started_tts:
                        started_tts = await send_tts_state_to_esp32(
                            target, "start", source="app_chat", turn_id=turn_id
                        )
                    if started_tts:
                        await send_tts_state_to_esp32(
                            target, "sentence_start", source="app_chat", turn_id=turn_id, text=sentence
                        )
                        total_frames += await send_tts_audio_frames_to_esp32(
                            target, sentence, source="app_chat", turn_id=turn_id, encoder=encoder
                        )
                    is_first_sentence = False

            remaining = text_buffer[processed:].strip()
            if remaining:
                if allow_device_tts and not started_tts:
                    started_tts = await send_tts_state_to_esp32(
                        target, "start", source="app_chat", turn_id=turn_id
                    )
                if started_tts:
                    await send_tts_state_to_esp32(
                        target, "sentence_start", source="app_chat", turn_id=turn_id, text=remaining
                    )
                    total_frames += await send_tts_audio_frames_to_esp32(
                        target, remaining, source="app_chat", turn_id=turn_id, encoder=encoder
                    )

        await send_app_message(websocket, {
            "type": "app_chat_done",
            "request_id": request_id,
            "text": full_reply.strip(),
            "turn_id": turn_id,
            "device_tts": allow_device_tts,
            "quiet_status": quiet_status,
        })
        print(f"[APP-chat] frames={total_frames}, reply={full_reply.strip()!r}")
    except Exception as exc:
        print(f"[APP-chat] error: {exc}")
        await send_app_message(websocket, {
            "type": "app_chat_error",
            "request_id": request_id,
            "error": str(exc)[:160],
        })
    finally:
        if started_tts:
            await send_tts_state_to_esp32(target, "stop", source="app_chat", turn_id=turn_id)


async def answer_user_text(client_id: str, text: str, history: list[dict], turn_id: int) -> None:
    if is_exit_phrase(text):
        await send_tts_stream_to_esp32(
            client_id, EXIT_REPLY_TEXT, turn_id=turn_id, end_dialog=True
        )
        return

    settings = load_user_settings()
    if is_sleep_greeting_trigger(text):
        await handle_sleep_greeting_trigger(client_id, history, settings)
        return

    alarm_reply = await handle_alarm_request_if_needed(
        client_id,
        text,
        turn_id,
        allow_voice=not is_ai_voice_blocked(settings),
        source="voice_alarm",
    )
    if alarm_reply is not None:
        history.append({"role": "user", "content": text})
        history.append({"role": "assistant", "content": alarm_reply})
        return

    if await answer_music_request_if_needed(client_id, text, turn_id):
        return

    pending_reply = await _consume_pre_sleep_light_reply(
        client_id,
        text,
        allow_voice=not is_ai_voice_blocked(settings),
    )
    if pending_reply is not None:
        return

    # ★ xiaozhi 流式：chat_stream 自带 Function Calling，所有对话统一走流式
    await handle_ai_stream_result(client_id, text, history, turn_id)


async def cancel_active_task(client_id: str) -> None:
    session = esp32_sessions.get(client_id) or {}
    task = session.get("active_task")
    if not task or task.done():
        return

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        print(f"[Task] 取消旧任务时发现异常: {exc}")


async def process_text_turn(client_id: str, text: str, history: list[dict], turn_id: int) -> None:
    try:
        if text == WAKE_TRIGGER_TEXT:
            await send_tts_stream_to_esp32(client_id, WAKE_REPLY_TEXT, turn_id=turn_id)
            return

        await answer_user_text(client_id, text, history, turn_id)
    except asyncio.CancelledError:
        print(f"[Task] 旧文本任务已取消: turn_id={turn_id}")
        raise
    except Exception as e:
        print(f"[ERROR] 处理文本消息出错: {e}")
        import traceback
        traceback.print_exc()
        if is_current_turn(client_id, turn_id):
            try:
                await send_json_to_esp32(client_id, {
                    "type": "status",
                    "msg": f"处理出错: {str(e)[:100]}",
                    "turn_id": turn_id
                })
            except Exception:
                pass


def split_pcm_frames(audio_bytes: bytes) -> list[bytes]:
    return [
        audio_bytes[i:i + PCM_FRAME_BYTES]
        for i in range(0, len(audio_bytes), PCM_FRAME_BYTES)
        if audio_bytes[i:i + PCM_FRAME_BYTES]
    ]


async def process_audio_frames_turn(client_id: str, frames: list[bytes], history: list[dict], turn_id: int, source: str) -> None:
    try:
        total_bytes = sum(len(frame) for frame in frames)
        print(f"[Audio] {source} turn_id={turn_id}, frames={len(frames)}, bytes={total_bytes}")
        if not frames:
            await send_json_to_esp32(client_id, {
                "type": "status",
                "msg": "没收到音频，请再说一次",
                "turn_id": turn_id
            })
            return

        text = await recognize(frames)
        if not is_current_turn(client_id, turn_id):
            return
        print(f"[STT] result_len={len(text.strip())}, text={text!r}")
        if not text.strip():
            await send_json_to_esp32(client_id, {"type": "status", "turn_id": turn_id})
            return

        print(f"[STT] {text}")
        await send_json_to_esp32(client_id, {
            "type": "stt_result",
            "text": text,
            "turn_id": turn_id
        })

        await answer_user_text(client_id, text, history, turn_id)
    except asyncio.CancelledError:
        print(f"[Task] 旧语音任务已取消: turn_id={turn_id}")
        raise
    except Exception as e:
        print(f"[ERROR] 处理语音消息出错: {e}")
        import traceback
        traceback.print_exc()
        if is_current_turn(client_id, turn_id):
            try:
                await send_json_to_esp32(client_id, {
                    "type": "status",
                    "msg": f"处理出错: {str(e)[:100]}",
                    "turn_id": turn_id
                })
            except Exception:
                pass


async def process_audio_turn(client_id: str, audio_b64: str, history: list[dict], turn_id: int) -> None:
    print(f"[Audio] received legacy turn_id={turn_id}, b64_len={len(audio_b64)}")
    audio_bytes = base64.b64decode(audio_b64)
    frames = split_pcm_frames(audio_bytes)
    await process_audio_frames_turn(client_id, frames, history, turn_id, "legacy_pcm")


async def process_realtime_audio(client_id: str, pcm_queue: asyncio.Queue, history: list[dict], turn_id: int) -> None:
    try:
        print(f"[Audio] realtime STT start turn_id={turn_id}")
        text = await recognize_queue(pcm_queue)
        if not is_current_turn(client_id, turn_id):
            return

        print(f"[STT] realtime result_len={len(text.strip())}, text={text!r}")
        if not text.strip():
            # 空录音 → 发空 status 让 ESP32 继续录，但不设 turn_done？
            # 不行，ESP32 需要 turn_done 才能重录。改 ESP32 端太复杂。
            # 这里改用：发 status 让它重录，但 listen start 不 cancel AI 任务
            await send_json_to_esp32(client_id, {"type": "status", "turn_id": turn_id})
            return

        await send_json_to_esp32(client_id, {
            "type": "stt_result",
            "text": text,
            "turn_id": turn_id
        })
        await answer_user_text(client_id, text, history, turn_id)
    except asyncio.CancelledError:
        print(f"[Task] 实时语音任务已取消: turn_id={turn_id}")
        raise
    except Exception as e:
        print(f"[ERROR] 处理实时语音出错: {e}")
        import traceback
        traceback.print_exc()
        if is_current_turn(client_id, turn_id):
            try:
                await send_json_to_esp32(client_id, {
                    "type": "status",
                    "msg": f"处理出错: {str(e)[:100]}",
                    "turn_id": turn_id
                })
            except Exception:
                pass


@app.websocket("/ws/esp32")
async def esp32_endpoint(websocket: WebSocket):
    """
    ESP32 设备入口。

    新协议：hello / listen start / binary Opus / listen stop / tts start|stop。
    同时保留 text、audio、audio_start、audio_end 等旧测试入口。
    """
    global last_active_esp32_id, latest_sensor_data, latest_snore_event

    await websocket.accept()
    client_id = str(id(websocket))
    esp32_clients[client_id] = websocket
    esp32_send_locks[client_id] = asyncio.Lock()
    esp32_sessions[client_id] = {"turn_id": 0, "connected_at": time.time(), "last_seen_at": time.time()}
    last_active_esp32_id = client_id
    ensure_sensor_poll_task()
    history: list[dict] = []
    print(f"[ESP32] 已连接 ({client_id})")

    try:
        while True:
            # 等待 ESP32 发来的消息：text JSON 控制帧 + binary Opus 音频帧
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                raise WebSocketDisconnect
            last_active_esp32_id = client_id
            esp32_sessions.setdefault(client_id, {})["last_seen_at"] = time.time()

            try:
                if "bytes" in message and message["bytes"] is not None:
                    incoming = esp32_sessions[client_id].get("incoming_audio")
                    if not incoming:
                        print(f"[ESP32] drop binary audio without listen start, bytes={len(message['bytes'])}")
                        continue
                    pcm = incoming["decoder"].decode(
                        message["bytes"], OPUS_FRAME_SAMPLES
                    )
                    if "pcm_queue" in incoming:
                        incoming["pcm_buffer"].extend(pcm)
                        while len(incoming["pcm_buffer"]) >= PCM_FRAME_BYTES:
                            frame = bytes(incoming["pcm_buffer"][:PCM_FRAME_BYTES])
                            del incoming["pcm_buffer"][:PCM_FRAME_BYTES]
                            incoming["pcm_queue"].put_nowait(frame)
                    else:
                        incoming["pcm"].extend(pcm)
                    incoming["chunks"] += 1
                    incoming["opus_bytes"] += len(message["bytes"])
                    continue

                if "text" not in message or message["text"] is None:
                    continue

                data = json.loads(message["text"])
                msg_type = data.get("type")

                if msg_type == "hello":
                    session_id = data.get("session_id") or uuid.uuid4().hex[:12]
                    esp32_sessions[client_id]["session_id"] = session_id
                    esp32_sessions[client_id]["audio_params"] = data.get("audio_params", {})
                    for old_id, old_session in list(esp32_sessions.items()):
                        if old_id != client_id and old_session.get("session_id") == session_id:
                            await drop_esp32_client(old_id, f"superseded by session {session_id}")
                    await send_json_to_esp32(client_id, {
                        "type": "hello",
                        "session_id": session_id,
                        "server_version": APP_VERSION,
                        "audio_params": {
                            "format": "opus",
                            "sample_rate": OPUS_SAMPLE_RATE,
                            "channels": OPUS_CHANNELS,
                            "frame_duration": 60,
                        },
                    })
                    await push_snore_policy(client_id)
                    print(f"[ESP32] hello session_id={session_id}, version={data.get('version')}")
                    continue

                if msg_type == "listen":
                    state = data.get("state")
                    if state == "start":
                        # ★ AI 回复进行中 → 丢弃这次录音，不创建 STT 任务
                        active = esp32_sessions[client_id].get("active_task")
                        if active and not active.done():
                            esp32_sessions[client_id]["incoming_audio"] = None
                            continue

                        turn_id = next_turn_id(client_id)
                        pcm_queue: asyncio.Queue = asyncio.Queue()
                        esp32_sessions[client_id]["incoming_audio"] = {
                            "turn_id": turn_id,
                            "pcm_queue": pcm_queue,
                            "pcm_buffer": bytearray(),
                            "decoder": opuslib.Decoder(OPUS_SAMPLE_RATE, OPUS_CHANNELS),
                            "chunks": 0,
                            "opus_bytes": 0,
                        }
                        task = asyncio.create_task(
                            process_realtime_audio(client_id, pcm_queue, history, turn_id)
                        )
                        esp32_sessions[client_id]["active_task"] = task
                        print(f"[ESP32] listen start turn_id={turn_id}, mode={data.get('mode')}")
                    elif state == "stop":
                        incoming = esp32_sessions[client_id].pop("incoming_audio", None)
                        if not incoming:
                            print("[ESP32] listen stop without listen start")
                            continue
                        if incoming.get("pcm_buffer"):
                            await incoming["pcm_queue"].put(bytes(incoming["pcm_buffer"]))
                        await incoming["pcm_queue"].put(None)
                        print(
                            f"[ESP32] listen stop turn_id={incoming['turn_id']}, "
                            f"chunks={incoming['chunks']}, opus_bytes={incoming['opus_bytes']}"
                        )
                    elif state == "detect":
                        print(f"[ESP32] wake detected: {data.get('text', '')}")
                        turn_id = next_turn_id(client_id)
                        await cancel_active_task(client_id)
                        task = asyncio.create_task(
                            send_tts_stream_to_esp32(
                                client_id,
                                WAKE_REPLY_TEXT,
                                turn_id=turn_id,
                            )
                        )
                        esp32_sessions[client_id]["active_task"] = task
                    continue

                if msg_type == "abort":
                    esp32_sessions[client_id].pop("incoming_audio", None)
                    await cancel_active_task(client_id)
                    await send_tts_state_to_esp32(client_id, "stop")
                    print(f"[ESP32] abort reason={data.get('reason')}")
                    continue

                # ========== 文字模式（调试用，跳过 STT）==========
                if msg_type == "text":
                    text = str(data["text"]).strip()
                    if not text:
                        continue
                    turn_id = next_turn_id(client_id)
                    await cancel_active_task(client_id)
                    esp32_sessions[client_id].pop("incoming_audio", None)
                    print(f"[Text] {text}")

                    task = asyncio.create_task(process_text_turn(client_id, text, history, turn_id))
                    esp32_sessions[client_id]["active_task"] = task

                # ========== 兼容旧 PCM/base64 语音消息 ==========
                elif msg_type == "audio":
                    audio_b64 = data["audio"]
                    turn_id = next_turn_id(client_id)
                    await cancel_active_task(client_id)
                    esp32_sessions[client_id].pop("incoming_audio", None)
                    print(f"[ESP32] legacy audio turn_id={turn_id}, b64_len={len(audio_b64)}")

                    task = asyncio.create_task(process_audio_turn(client_id, audio_b64, history, turn_id))
                    esp32_sessions[client_id]["active_task"] = task

                # ========== 新 Opus 上传：开始 ==========
                elif msg_type == "audio_start":
                    turn_id = next_turn_id(client_id)
                    await cancel_active_task(client_id)
                    esp32_sessions[client_id]["incoming_audio"] = {
                        "turn_id": turn_id,
                        "pcm": bytearray(),
                        "decoder": opuslib.Decoder(OPUS_SAMPLE_RATE, OPUS_CHANNELS),
                        "chunks": 0,
                        "opus_bytes": 0,
                    }
                    print(f"[ESP32] opus audio_start turn_id={turn_id}")

                # ========== 新 Opus 上传：结束 ==========
                elif msg_type == "audio_end":
                    incoming = esp32_sessions[client_id].pop("incoming_audio", None)
                    if not incoming:
                        print("[ESP32] audio_end without audio_start")
                        continue
                    turn_id = int(incoming["turn_id"])
                    pcm_bytes = bytes(incoming["pcm"])
                    frames = split_pcm_frames(pcm_bytes)
                    print(
                        f"[ESP32] opus audio_end turn_id={turn_id}, "
                        f"chunks={incoming['chunks']}, opus_bytes={incoming['opus_bytes']}, pcm_bytes={len(pcm_bytes)}"
                    )
                    task = asyncio.create_task(process_audio_frames_turn(client_id, frames, history, turn_id, "opus"))
                    esp32_sessions[client_id]["active_task"] = task

                # ========== 传感器数据回传 ==========
                elif msg_type == "sensor_data":
                    request_id = data.get("request_id", "")
                    sensor_payload = data.get("data", {})
                    if isinstance(sensor_payload, dict) and latest_snore_event:
                        sensor_payload.setdefault("last_snore_event", latest_snore_event)
                    latest_sensor_data = {
                        "received_at": time.time(),
                        "client_id": client_id,
                        "data": sensor_payload,
                    }
                    await broadcast_to_apps({
                        "type": "sensor_data",
                        "esp32_connected": True,
                        "latest": latest_sensor_data,
                    })
                    if request_id and request_id in _sensor_futures:
                        future = _sensor_futures.pop(request_id, None)
                        if future and not future.done():
                            future.set_result(json.dumps(sensor_payload, ensure_ascii=False))
                            print(f"[ESP32] sensor_data resolved request_id={request_id}")
                    schedule_sensor_automations(client_id, sensor_payload)
                    await push_snore_policy(client_id, sensor_payload=sensor_payload)

                elif msg_type == "snore_event":
                    event_payload = {
                        "received_at": time.time(),
                        "client_id": client_id,
                        "snore": bool(data.get("snore", True)),
                        "score": _safe_float(data.get("score")),
                        "non_snore": _safe_float(data.get("non_snore")),
                        "rms": int(data.get("rms") or 0),
                        "peak": int(data.get("peak") or 0),
                        "active_chunks": int(data.get("active_chunks") or 0),
                        "action": data.get("action") or "inflate",
                        "adjusted": bool(data.get("adjusted", True)),
                        "target_kpa": _safe_float(data.get("target_kpa")),
                        "source": data.get("source") or "local_snore_ai",
                    }
                    latest_snore_event = event_payload
                    if latest_sensor_data is None:
                        latest_sensor_data = {
                            "received_at": time.time(),
                            "client_id": client_id,
                            "data": {},
                        }
                    latest_sensor_data["received_at"] = time.time()
                    latest_sensor_data["client_id"] = client_id
                    latest_sensor_data.setdefault("data", {})["last_snore_event"] = event_payload
                    await broadcast_to_apps({
                        "type": "snore_event",
                        "esp32_connected": True,
                        "data": event_payload,
                        "latest": latest_sensor_data,
                    })

                elif msg_type == "ai_persona":
                    persona = data.get("persona") or data.get("mode") or ""
                    payload = await handle_esp32_ai_persona_update(persona, client_id)
                    if payload:
                        await send_json_to_esp32(client_id, {
                            "type": "status",
                            "msg": f"AI ??????{payload['ai_persona']}",
                        })
                    else:
                        await send_json_to_esp32(client_id, {
                            "type": "status",
                            "msg": "AI ???????????",
                        })

                elif msg_type == "pillow_calibration_save":
                    saved_kpa = data.get("saved_kpa")
                    payload = await handle_esp32_pillow_calibration_save(saved_kpa, client_id)
                    if payload:
                        cal = payload.get("settings", {}).get("pillow_calibration", {})
                        await send_json_to_esp32(client_id, {
                            "type": "status",
                            "msg": f"?????????{float(cal.get('saved_kpa', 0)):.1f} kPa",
                        })
                    else:
                        await send_json_to_esp32(client_id, {
                            "type": "status",
                            "msg": "????????",
                        })

                # ========== 心跳 ==========
                elif msg_type == "pump_result":
                    if latest_sensor_data and latest_sensor_data.get("data") is not None:
                        latest_sensor_data["data"]["last_pump"] = {
                            "action": data.get("action"),
                            "target_kpa": data.get("target_kpa"),
                            "result_kpa": data.get("result_kpa"),
                        }
                    await broadcast_to_apps({
                        "type": "pump_result",
                        "esp32_connected": True,
                        "data": data,
                    })

                elif msg_type == "led_state":
                    if latest_sensor_data and latest_sensor_data.get("data") is not None:
                        latest_sensor_data["data"]["led_enabled"] = bool(data.get("enabled"))
                        latest_sensor_data["data"]["led_brightness"] = int(data.get("brightness") or 0)
                        latest_sensor_data["data"]["led_brightness_pct"] = int(data.get("brightness_pct") or 0)
                        latest_sensor_data["data"]["led_mode"] = data.get("mode") or "solid"
                        latest_sensor_data["data"]["led_color"] = data.get("color") or "warm"
                        latest_sensor_data["data"]["led_speed_pct"] = int(data.get("speed_pct") or 0)
                    await broadcast_to_apps({
                        "type": "led_state",
                        "esp32_connected": True,
                        "data": data,
                    })

                elif msg_type == "ir_state":
                    if latest_sensor_data is None:
                        latest_sensor_data = {
                            "received_at": time.time(),
                            "client_id": client_id,
                            "data": {},
                        }
                    latest_sensor_data["received_at"] = time.time()
                    latest_sensor_data["client_id"] = client_id
                    sensor_data = latest_sensor_data.setdefault("data", {})
                    sensor_data["fan_on"] = bool(data.get("fan_on"))
                    sensor_data["humidifier_on"] = bool(data.get("humidifier_on"))
                    sensor_data["air_conditioner_on"] = bool(
                        data.get("air_conditioner_on", data.get("ac_on"))
                    )
                    sensor_data["ac_on"] = sensor_data["air_conditioner_on"]
                    await broadcast_to_apps({
                        "type": "ir_state",
                        "esp32_connected": True,
                        "data": data,
                    })

                elif msg_type == "ping":
                    await send_json_to_esp32(client_id, {"type": "pong"})

            except Exception as e:
                print(f"[ERROR] 处理消息出错: {e}")
                import traceback
                traceback.print_exc()
                try:
                    await send_json_to_esp32(client_id, {
                        "type": "status",
                        "msg": f"处理出错: {str(e)[:100]}"
                    })
                except Exception:
                    pass

    except WebSocketDisconnect:
        print(f"[ESP32] 已断开 ({client_id})")
    finally:
        try:
            await cancel_active_task(client_id)
        except Exception:
            pass
        esp32_clients.pop(client_id, None)
        esp32_send_locks.pop(client_id, None)
        esp32_sessions.pop(client_id, None)
        for command_id, context in list(pc_command_contexts.items()):
            if context.get("client_id") == client_id:
                pc_command_contexts.pop(command_id, None)
        # 清理未完成的传感器请求
        for key in list(_sensor_futures.keys()):
            if client_id in key:
                fut = _sensor_futures.pop(key, None)
                if fut and not fut.done():
                    fut.set_result("ESP32 已断开连接")
        if last_active_esp32_id == client_id:
            last_active_esp32_id = pick_esp32_client()


@app.get("/api/latest_sensors")
async def api_latest_sensors():
    return {
        "ok": True,
        "esp32_connected": bool(esp32_clients),
        "app_clients": len(app_clients),
        "latest": latest_sensor_data,
        "latest_snore_event": latest_snore_event,
    }


@app.get("/api/user_settings")
async def api_user_settings():
    settings = load_user_settings()
    return {
        "ok": True,
        "settings": settings,
        "quiet_status": get_quiet_status(settings),
        "alarm_state": dict(alarm_runtime),
    }


@app.post("/api/user_settings")
async def api_update_user_settings(payload: dict):
    settings = save_user_settings(payload.get("settings") if "settings" in payload else payload)
    quiet_status = get_quiet_status(settings)
    await push_snore_policy(settings=settings)
    await broadcast_to_apps({
        "type": "settings_state",
        "settings": settings,
        "quiet_status": quiet_status,
        "alarm_state": dict(alarm_runtime),
    })
    return {
        "ok": True,
        "settings": settings,
        "quiet_status": quiet_status,
        "alarm_state": dict(alarm_runtime),
    }


@app.get("/api/avatar/current/manifest")
async def api_avatar_current_manifest():
    """当前 LCD AI 形象资源清单，ESP32/App 都可以读取。"""
    return get_current_avatar_manifest()


@app.get("/api/avatar/current/preview.png")
async def api_avatar_current_preview():
    path = current_preview_path()
    if not path.exists():
        raise HTTPException(status_code=404, detail="当前还没有生成过 LCD 形象预览")
    return FileResponse(path, media_type="image/png", filename="preview.png")


@app.get("/api/avatar/current/avatar_base_rgb666.bin")
async def api_avatar_current_rgb666():
    path = current_rgb666_path()
    if not path.exists():
        raise HTTPException(status_code=404, detail="当前还没有生成过 LCD RGB666 资源")
    return FileResponse(
        path,
        media_type="application/octet-stream",
        filename="avatar_base_rgb666.bin",
    )


@app.post("/api/avatar/generate")
async def api_avatar_generate(payload: dict):
    """调用 image2 生成 AI 形象，并转换为 ESP32 LCD 可直接显示的 RGB666。"""
    prompt = str(payload.get("prompt") or "").strip()
    try:
        manifest = await generate_lcd_avatar(prompt)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:240])

    await broadcast_to_apps({
        "type": "avatar_generated",
        "ok": True,
        "manifest": manifest,
    })
    return manifest


@app.post("/api/avatar/sync")
async def api_avatar_sync(payload: dict):
    """通知 ESP32 有新的 LCD 形象资源可下载。ESP32 热切换逻辑随后接入。"""
    manifest = get_current_avatar_manifest()
    if not manifest.get("ok"):
        raise HTTPException(status_code=404, detail=manifest.get("error") or "当前没有可同步的形象")

    target = pick_esp32_client(payload.get("client_id"))
    ok = await send_json_to_esp32(target, {
        "type": "avatar_update",
        "manifest": manifest,
        "manifest_url": (manifest.get("urls") or {}).get("manifest"),
        "rgb666_url": (manifest.get("urls") or {}).get("rgb666"),
        "width": manifest.get("width"),
        "height": manifest.get("height"),
        "crc32": manifest.get("crc32"),
        "bin_size": manifest.get("bin_size"),
    }) if target else False

    return {
        "ok": ok,
        "esp32_connected": bool(target),
        "manifest": manifest,
        "note": "云端资源已准备好；ESP32 端需要接入 avatar_update 下载与切换逻辑。",
    }


@app.websocket("/ws/app")
async def app_endpoint(websocket: WebSocket):
    """Mobile H5 entry: receive live ESP32 sensor telemetry."""
    await websocket.accept()
    app_id = str(id(websocket))
    app_clients[app_id] = websocket
    ensure_sensor_poll_task()
    ensure_alarm_scheduler_task()
    print(f"[APP] connected ({app_id})")

    await websocket.send_text(json.dumps({
        "type": "app_hello",
        "esp32_connected": bool(esp32_clients),
        "latest": latest_sensor_data,
        "latest_snore_event": latest_snore_event,
        "settings": load_user_settings(),
        "quiet_status": get_quiet_status(),
        "alarm_state": dict(alarm_runtime),
    }, ensure_ascii=False))
    await request_sensor_data()

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
            except Exception:
                continue

            msg_type = data.get("type")
            if msg_type == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}, ensure_ascii=False))
            elif msg_type == "settings_get":
                settings = load_user_settings()
                await websocket.send_text(json.dumps({
                    "type": "settings_state",
                    "settings": settings,
                    "quiet_status": get_quiet_status(settings),
                    "alarm_state": dict(alarm_runtime),
                }, ensure_ascii=False))
            elif msg_type == "settings_update":
                settings = save_user_settings(data.get("settings") or {})
                quiet_status = get_quiet_status(settings)
                await push_snore_policy(settings=settings)
                payload = {
                    "type": "settings_state",
                    "ok": True,
                    "settings": settings,
                    "quiet_status": quiet_status,
                    "alarm_state": dict(alarm_runtime),
                }
                await websocket.send_text(json.dumps(payload, ensure_ascii=False))
                await broadcast_to_apps(payload)
            elif msg_type == "read_sensors":
                ok = await request_sensor_data(data.get("client_id"))
                await websocket.send_text(json.dumps({
                    "type": "read_sensors_ack",
                    "ok": ok,
                    "esp32_connected": bool(esp32_clients),
                }, ensure_ascii=False))
            elif msg_type == "pc_agent_task":
                request_id = str(data.get("request_id") or "")
                try:
                    await handle_pc_agent_task_once(
                        websocket,
                        str(data.get("text") or ""),
                        request_id,
                        str(data.get("session_id") or app_id),
                    )
                except Exception as exc:
                    print(f"[PC-Agent-task] fatal error: {exc}")
                    import traceback
                    traceback.print_exc()
                    await send_app_message(websocket, {
                        "type": "app_chat_error",
                        "request_id": request_id,
                        "error": str(exc)[:160],
                        "source": "pc_agent_task",
                    })
            elif msg_type == "app_chat":
                request_id = str(data.get("request_id") or "")
                try:
                    await handle_app_chat_once(
                        websocket,
                        str(data.get("text") or ""),
                        request_id,
                        str(data.get("session_id") or app_id),
                    )
                except Exception as exc:
                    print(f"[APP-chat] fatal error: {exc}")
                    import traceback
                    traceback.print_exc()
                    await send_app_message(websocket, {
                        "type": "app_chat_error",
                        "request_id": request_id,
                        "error": str(exc)[:160],
                    })
            elif msg_type == "pillow_cmd":
                target = pick_esp32_client(data.get("client_id"))
                payload = {
                    "type": "pillow_cmd",
                    "action": data.get("action"),
                    "duration_sec": int(data.get("duration_sec") or 3),
                }
                if data.get("target_kpa") is not None:
                    target_value = float(data.get("target_kpa"))
                    if math.isfinite(target_value):
                        payload["target_kpa"] = max(0.0, min(10.0, target_value))
                ok = await send_json_to_esp32(target, payload) if target else False
                await websocket.send_text(json.dumps({
                    "type": "command_ack",
                    "target": "pillow",
                    "ok": ok,
                }, ensure_ascii=False))
            elif msg_type == "led_cmd":
                target = pick_esp32_client(data.get("client_id"))
                payload = {
                    "type": "led_cmd",
                    "action": data.get("action") or "set",
                }
                for key in (
                    "enabled", "on", "brightness", "brightness_pct",
                    "mode", "color", "speed_pct", "duration_sec",
                    "r", "g", "b",
                ):
                    if key in data:
                        payload[key] = data.get(key)
                ok = await send_json_to_esp32(target, payload) if target else False
                ack = {
                    "type": "command_ack",
                    "target": "led",
                    "ok": ok,
                    "enabled": payload.get("enabled", payload.get("on")),
                    "brightness": payload.get("brightness"),
                    "brightness_pct": payload.get("brightness_pct"),
                    "mode": payload.get("mode"),
                    "color": payload.get("color"),
                    "speed_pct": payload.get("speed_pct"),
                    "duration_sec": payload.get("duration_sec"),
                }
                await websocket.send_text(json.dumps(ack, ensure_ascii=False))
            elif msg_type == "ir_cmd":
                target = pick_esp32_client(data.get("client_id"))
                device = _normalize_ir_device(data.get("device"))
                action = str(data.get("action") or "").strip().lower()
                if device not in {"fan", "humidifier", "air_conditioner"}:
                    ok = False
                elif action not in {"on", "off", "toggle"}:
                    ok = False
                else:
                    ok = await send_json_to_esp32(target, {
                        "type": "ir_cmd",
                        "device": device,
                        "action": action,
                    }) if target else False
                await websocket.send_text(json.dumps({
                    "type": "command_ack",
                    "target": "ir",
                    "device": device,
                    "action": action,
                    "ok": ok,
                }, ensure_ascii=False))
    except WebSocketDisconnect:
        print(f"[APP] disconnected ({app_id})")
    finally:
        app_clients.pop(app_id, None)


@app.websocket("/ws/pc_agent")
async def pc_agent_endpoint(websocket: WebSocket):
    """
    PC Agent 的 WebSocket 连接入口

    PC 端运行一个 Agent 程序，连接到这里等待命令。
    当 LLM 判断用户想控制电脑时，命令会通过这个连接下发。

    PC Agent 发来的消息类型：
    - {"type": "result", "result": "执行结果文字"}  执行完毕后返回结果
    - {"type": "ping"}  心跳

    服务端下发的消息类型：
    - {"type": "pc_command", "command": {"action": "...", "params": {...}}}
    - {"type": "pong"}
    """
    await websocket.accept()
    agent_id = str(id(websocket))
    pc_agents[agent_id] = websocket
    print(f"[PC Agent] 已连接 ({agent_id})")

    try:
        while True:
            message = await websocket.receive_text()
            data = json.loads(message)

            # PC Agent 执行完命令后返回结果
            if data.get("type") == "result":
                result_text = data.get("result", "")
                command_id = data.get("command_id")
                result_turn_id = data.get("turn_id")
                print(f"[PC Agent] 执行结果: {result_text}")

                # ★ LLM 发起的 PC 命令：resolve future，让 LLM 拿到结果后组织语言播报
                if command_id:
                    future = _pc_command_futures.pop(command_id, None)
                    if future and not future.done():
                        future.set_result(result_text)
                        continue  # LLM 接管播报，不重复走老逻辑

                context = pc_command_contexts.pop(command_id, None) if command_id else None
                target_id = pick_esp32_client(data.get("client_id") or (context or {}).get("client_id"))
                action = (context or {}).get("action")
                current_turn_id = (esp32_sessions.get(target_id or "", {}) or {}).get("turn_id")
                age = time.monotonic() - float((context or {}).get("created_at", time.monotonic()))
                is_current = (
                    target_id
                    and result_turn_id is not None
                    and current_turn_id is not None
                    and int(result_turn_id) == int(current_turn_id)
                    and age <= 8
                )

                if target_id and result_text:
                    if action in ("open_url", "open_file"):
                        print(f"[ESP32] PC Agent {action} 结果静音，不打断对话 -> {target_id}")
                    elif action == "summarize_file" and is_current:
                        sent = await send_tts_stream_to_esp32(
                            target_id,
                            result_text,
                            source="pc_result",
                            turn_id=int(result_turn_id)
                        )
                        if sent:
                            print(f"[ESP32] 已播报 PC Agent 结果 -> {target_id}")
                    else:
                        await send_json_to_esp32(target_id, {
                            "type": "status",
                            "msg": result_text,
                            "source": "pc_result",
                            "turn_id": result_turn_id
                        })
                        print(f"[ESP32] PC Agent 结果仅状态提示，不语音插队 -> {target_id}")
                elif not target_id:
                    print("[ESP32] 没有在线客户端，无法播报 PC Agent 结果")

            elif data.get("type") == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}, ensure_ascii=False))

    except WebSocketDisconnect:
        pc_agents.pop(agent_id, None)
        print(f"[PC Agent] 已断开 ({agent_id})")


@app.get("/health")
async def health():
    """健康检查接口，用于确认服务是否在线"""
    return {
        "status": "ok",
        "esp32_connected": bool(esp32_clients),
        "esp32_clients": len(esp32_clients),
        "pc_agents": len(pc_agents),
        "app_clients": len(app_clients),
        "has_sensor_data": latest_sensor_data is not None,
        "version": APP_VERSION,
    }


WEB_DIR = Path(__file__).resolve().parent / "web"
WEB_MEDIA_TYPES = {
    "xiaoan-h5-v5.html": "text/html; charset=utf-8",
    "lucide.min.js": "application/javascript; charset=utf-8",
    "xiaoan-bedroom.jpg": "image/jpeg",
    "xiaoan-bedroom-anime-soft.png": "image/png",
    "xiaoan-device.png": "image/png",
}


def web_file_response(filename: str) -> FileResponse:
    if filename not in WEB_MEDIA_TYPES:
        raise HTTPException(status_code=404, detail="web asset not found")
    path = WEB_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="web asset not uploaded")
    return FileResponse(path, media_type=WEB_MEDIA_TYPES[filename])


@app.get("/")
@app.get("/xiaoan-h5-v5.html")
async def h5_index():
    return web_file_response("xiaoan-h5-v5.html")


@app.get("/lucide.min.js")
async def h5_lucide():
    return web_file_response("lucide.min.js")


@app.get("/xiaoan-bedroom.jpg")
async def h5_bedroom():
    return web_file_response("xiaoan-bedroom.jpg")


@app.get("/xiaoan-bedroom-anime-soft.png")
async def h5_bedroom_anime():
    return web_file_response("xiaoan-bedroom-anime-soft.png")


@app.get("/xiaoan-device.png")
async def h5_device():
    return web_file_response("xiaoan-device.png")


@app.get("/h5/{filename}")
async def h5_asset(filename: str):
    return web_file_response(filename)


if __name__ == "__main__":
    import uvicorn
    # 启动 WebSocket 服务，默认监听 0.0.0.0:8000
    print(f"[ESPAgent] version={APP_VERSION}")
    uvicorn.run(
        app,
        host=SERVER_HOST,
        port=int(SERVER_PORT),
        ws_ping_interval=30,
        ws_ping_timeout=120
    )
