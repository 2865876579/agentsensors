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

# Windows 控制台默认 GBK，强制 UTF-8 避免 emoji 打印崩溃
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
import time
import urllib.parse
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from config import SERVER_HOST, SERVER_PORT
from stt_xunfei import recognize, recognize_queue
from llm_deepseek import (
    chat_stream,
    set_pc_command_callback,
    set_pillow_callback,
    set_led_callback,
    set_read_sensors_callback,
)
from tts_volc import synthesize
from web_search import search_web  # 搜索工具，供后续 function calling 工具接入时使用
from user_settings import (
    get_quiet_status,
    is_ai_screen_blocked,
    is_ai_voice_blocked,
    load_user_settings,
    save_user_settings,
)

app = FastAPI(title="Smart Pillow Cloud Server")
APP_VERSION = "xiaozhi_realtime_v5"

WAKE_TRIGGER_TEXT = "__wake__"
WAKE_REPLY_TEXT = "我在，你说。"
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
sensor_poll_task: asyncio.Task | None = None
SENSOR_POLL_INTERVAL_SEC = 2.0


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
            return "目标气压无效"
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
set_pc_command_callback(_pc_command_cb)
set_pillow_callback(_pillow_cb)
set_led_callback(_led_cb)
set_read_sensors_callback(_read_sensors_cb)


def is_exit_phrase(text: str) -> bool:
    normalized = re.sub(r"[\s，。！？、,.!?~～]", "", text or "")
    return any(phrase in normalized for phrase in EXIT_PHRASES)


async def send_json_to_esp32(client_id: str, payload: dict) -> bool:
    """串行发送一条 JSON 消息到指定 ESP32，避免多任务并发写同一 WebSocket。"""
    websocket = esp32_clients.get(client_id)
    lock = esp32_send_locks.get(client_id)
    if websocket is None or lock is None:
        return False

    async with lock:
        await websocket.send_text(json.dumps(payload, ensure_ascii=False))
    return True


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
    return await send_json_to_esp32(client_id, payload)


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
        async with lock:
            await websocket.send_bytes(frame)
        frame_count += 1
    return frame_count


async def send_tts_stream_to_esp32(
    client_id: str,
    text: str,
    *,
    source: str = "assistant",
    turn_id: int | None = None,
    end_dialog: bool = False,
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
    if last_active_esp32_id and last_active_esp32_id in esp32_clients:
        return last_active_esp32_id
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
    """Poll ESP32 only while at least one mobile H5 client is connected."""
    while True:
        try:
            if app_clients:
                await request_sensor_data()
        except Exception as e:
            print(f"[APP] sensor poll error: {e}")
        await asyncio.sleep(SENSOR_POLL_INTERVAL_SEC)


def ensure_sensor_poll_task() -> None:
    global sensor_poll_task
    if sensor_poll_task is None or sensor_poll_task.done():
        sensor_poll_task = asyncio.create_task(sensor_poll_loop())


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


async def send_app_message(websocket: WebSocket, payload: dict) -> None:
    await websocket.send_text(json.dumps(payload, ensure_ascii=False))


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

    target = pick_esp32_client()
    if not target:
        await send_app_message(websocket, {
            "type": "app_chat_error",
            "request_id": request_id,
            "error": "ESP32 not connected",
        })
        return

    settings = load_user_settings()
    quiet_status = get_quiet_status(settings)
    allow_device_tts = not is_ai_voice_blocked(settings)
    allow_device_status = not is_ai_screen_blocked(settings)

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
    global last_active_esp32_id, latest_sensor_data

    await websocket.accept()
    client_id = str(id(websocket))
    esp32_clients[client_id] = websocket
    esp32_send_locks[client_id] = asyncio.Lock()
    esp32_sessions[client_id] = {"turn_id": 0}
    last_active_esp32_id = client_id
    history: list[dict] = []
    print(f"[ESP32] 已连接 ({client_id})")

    try:
        while True:
            # 等待 ESP32 发来的消息：text JSON 控制帧 + binary Opus 音频帧
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                raise WebSocketDisconnect
            last_active_esp32_id = client_id

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

                # ========== 心跳 ==========
                elif msg_type == "pump_result":
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
    }


@app.get("/api/user_settings")
async def api_user_settings():
    settings = load_user_settings()
    return {
        "ok": True,
        "settings": settings,
        "quiet_status": get_quiet_status(settings),
    }


@app.post("/api/user_settings")
async def api_update_user_settings(payload: dict):
    settings = save_user_settings(payload.get("settings") if "settings" in payload else payload)
    quiet_status = get_quiet_status(settings)
    await broadcast_to_apps({
        "type": "settings_state",
        "settings": settings,
        "quiet_status": quiet_status,
    })
    return {
        "ok": True,
        "settings": settings,
        "quiet_status": quiet_status,
    }


@app.websocket("/ws/app")
async def app_endpoint(websocket: WebSocket):
    """Mobile H5 entry: receive live ESP32 sensor telemetry."""
    await websocket.accept()
    app_id = str(id(websocket))
    app_clients[app_id] = websocket
    ensure_sensor_poll_task()
    print(f"[APP] connected ({app_id})")

    await websocket.send_text(json.dumps({
        "type": "app_hello",
        "esp32_connected": bool(esp32_clients),
        "latest": latest_sensor_data,
        "settings": load_user_settings(),
        "quiet_status": get_quiet_status(),
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
                }, ensure_ascii=False))
            elif msg_type == "settings_update":
                settings = save_user_settings(data.get("settings") or {})
                quiet_status = get_quiet_status(settings)
                payload = {
                    "type": "settings_state",
                    "ok": True,
                    "settings": settings,
                    "quiet_status": quiet_status,
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
            elif msg_type == "app_chat":
                await handle_app_chat_once(
                    websocket,
                    str(data.get("text") or ""),
                    str(data.get("request_id") or ""),
                    str(data.get("session_id") or app_id),
                )
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
