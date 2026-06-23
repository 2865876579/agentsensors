"""
讯飞流式语音识别模块

功能：接收 PCM 音频帧，通过讯飞 WebSocket API 实时转成文字。

讯飞 API 文档：https://www.xfyun.cn/doc/asr/voicedictation/API.html

音频要求：
  - 格式：PCM 原始数据（不是 WAV，不带文件头）
  - 采样率：16kHz
  - 位深：16bit
  - 声道：单声道

调用流程：
  1. 用 APP_ID + API_KEY + API_SECRET 生成鉴权 URL
  2. 建立 WebSocket 连接到讯飞服务器
  3. 分帧发送音频数据（每帧约 40ms）
  4. 实时接收识别结果，最终拼接成完整文字
"""
import websocket
import hashlib
import hmac
import base64
import json
import time
from datetime import datetime
from urllib.parse import urlencode, urlparse
from config import XF_APP_ID, XF_API_KEY, XF_API_SECRET, XF_STT_SEND_INTERVAL_SEC, XF_VAD_EOS_MS


def _create_url():
    """
    生成讯飞 WebSocket 鉴权 URL

    讯飞用 HMAC-SHA256 签名做鉴权，把签名信息编码在 URL query 参数里。
    签名内容包括：host、date、请求行，用 API_SECRET 做 HMAC 密钥。
    """
    url = "wss://iat-api.xfyun.cn/v2/iat"
    now = datetime.utcnow()
    date = now.strftime("%a, %d %b %Y %H:%M:%S GMT")

    parsed = urlparse(url)
    # 按讯飞要求拼接签名原文
    signature_origin = (
        f"host: {parsed.netloc}\n"
        f"date: {date}\n"
        f"GET {parsed.path} HTTP/1.1"
    )

    # HMAC-SHA256 签名
    signature_sha = hmac.new(
        XF_API_SECRET.encode(), signature_origin.encode(), digestmod=hashlib.sha256
    ).digest()
    signature = base64.b64encode(signature_sha).decode()

    # 拼接 authorization 字符串
    authorization_origin = (
        f'api_key="{XF_API_KEY}", '
        f'algorithm="hmac-sha256", '
        f'headers="host date request-line", '
        f'signature="{signature}"'
    )
    authorization = base64.b64encode(authorization_origin.encode()).decode()

    # 最终鉴权参数放在 URL query 里
    params = {"authorization": authorization, "date": date, "host": parsed.netloc}
    return url + "?" + urlencode(params)


async def recognize(audio_frames: list[bytes]) -> str:
    """
    语音识别主函数

    参数：
      audio_frames: PCM 音频帧列表，每帧 1280 字节（40ms，16kHz/16bit/mono）

    返回：
      识别出的文字字符串，识别失败返回空字符串

    实现方式：
      因为讯飞 SDK 用的是同步 websocket-client 库，
      这里在子线程里跑 WebSocket，主协程等待识别完成事件。
    """
    import asyncio

    loop = asyncio.get_running_loop()
    result_text = []  # 收集识别结果片段
    done_event = asyncio.Event()  # 标记识别结束
    error_text = []

    def finish():
        loop.call_soon_threadsafe(done_event.set)

    def on_message(ws, message):
        """收到讯飞返回的识别结果"""
        data = json.loads(message)
        if data.get("code") != 0:
            # 识别出错，结束
            error_text.append(f"code={data.get('code')} message={data.get('message')}")
            finish()
            return
        # 解析识别结果：data.result.ws[].cw[].w 是文字片段
        results = data.get("data", {}).get("result", {}).get("ws", [])
        for w in results:
            for cw in w.get("cw", []):
                result_text.append(cw.get("w", ""))
        # status == 2 表示识别结束
        if data.get("data", {}).get("status") == 2:
            finish()

    def on_open(ws):
        """连接建立后，开始逐帧发送音频"""
        print(f"[STT] xfyun open, frames={len(audio_frames)}, bytes={sum(len(f) for f in audio_frames)}")
        # 讯飞协议：第一帧带 common+business，中间帧只带 data，最后一帧标记结束
        STATUS_FIRST = 0
        STATUS_CONTINUE = 1
        STATUS_LAST = 2

        common = {"app_id": XF_APP_ID}
        business = {
            "language": "zh_cn",      # 中文
            "domain": "iat",          # 日常用语
            "accent": "mandarin",     # 普通话
            "vad_eos": XF_VAD_EOS_MS,  # 静音检测：服务端已截断音频，这里只做兜底
        }

        for i, frame in enumerate(audio_frames):
            if i == 0:
                status = STATUS_FIRST
            elif i == len(audio_frames) - 1:
                status = STATUS_LAST
            else:
                status = STATUS_CONTINUE

            data = {
                "status": status,
                "format": "audio/L16;rate=16000",  # PCM 16kHz
                "encoding": "raw",
                "audio": base64.b64encode(frame).decode(),
            }

            payload = {"data": data}
            if status == STATUS_FIRST:
                # 第一帧要带上应用信息和业务参数
                payload["common"] = common
                payload["business"] = business

            ws.send(json.dumps(payload))
            # 已经是完整录音，快速送给讯飞，避免录完再等一遍音频时长。
            if XF_STT_SEND_INTERVAL_SEC > 0:
                time.sleep(XF_STT_SEND_INTERVAL_SEC)

    def on_error(ws, error):
        """连接出错时结束等待"""
        error_text.append(str(error))
        print(f"[STT] xfyun error: {error}")
        finish()

    def on_close(ws, close_status_code, close_msg):
        """连接关闭时兜底结束等待，避免任务长期挂起"""
        if close_status_code or close_msg:
            print(f"[STT] xfyun close: code={close_status_code}, msg={close_msg}")
        finish()

    # 建立到讯飞的 WebSocket 连接
    url = _create_url()
    ws = websocket.WebSocketApp(
        url, on_message=on_message, on_open=on_open, on_error=on_error, on_close=on_close
    )

    # 在子线程运行同步 WebSocket（避免阻塞 asyncio 事件循环）
    import threading
    t = threading.Thread(target=ws.run_forever)
    t.start()

    # 等待识别完成，最多等 15 秒
    await asyncio.wait_for(done_event.wait(), timeout=15)
    ws.close()
    t.join(timeout=2)

    if error_text:
        print(f"[STT] xfyun failed: {'; '.join(error_text)[:200]}")
    return "".join(result_text)


async def recognize_queue(audio_queue) -> str:
    """
    从 asyncio.Queue 持续读取 PCM 帧并送讯飞识别。

    调用方把 bytes 帧放进队列，结束时放 None。这样 ESP32 录音期间
    服务端就可以同步把音频推给讯飞，不必等整段录音结束后再重放。
    """
    import asyncio
    import queue
    import threading

    loop = asyncio.get_running_loop()
    sync_queue: queue.Queue = queue.Queue()
    done_event = asyncio.Event()
    stop_event = threading.Event()
    result_text = []
    error_text = []

    def finish():
        loop.call_soon_threadsafe(done_event.set)

    async def pump_queue():
        while True:
            frame = await audio_queue.get()
            sync_queue.put(frame)
            if frame is None:
                break

    def send_frame(ws, frame: bytes, status: int):
        data = {
            "status": status,
            "format": "audio/L16;rate=16000",
            "encoding": "raw",
            "audio": base64.b64encode(frame or b"").decode(),
        }
        payload = {"data": data}
        if status == 0:
            payload["common"] = {"app_id": XF_APP_ID}
            payload["business"] = {
                "language": "zh_cn",
                "domain": "iat",
                "accent": "mandarin",
                "vad_eos": XF_VAD_EOS_MS,
            }
        ws.send(json.dumps(payload))

    def on_message(ws, message):
        data = json.loads(message)
        if data.get("code") != 0:
            error_text.append(f"code={data.get('code')} message={data.get('message')}")
            finish()
            return

        results = data.get("data", {}).get("result", {}).get("ws", [])
        for w in results:
            for cw in w.get("cw", []):
                result_text.append(cw.get("w", ""))

        if data.get("data", {}).get("status") == 2:
            finish()

    def on_open(ws):
        print("[STT] xfyun realtime open")
        try:
            first_frame = sync_queue.get(timeout=10)
            if first_frame is None:
                finish()
                return

            send_frame(ws, first_frame, 0)
            frame_count = 1
            while not stop_event.is_set():
                frame = sync_queue.get()
                if frame is None:
                    send_frame(ws, b"", 2)
                    print(f"[STT] xfyun realtime sent frames={frame_count}")
                    return
                frame_count += 1
                send_frame(ws, frame, 1)
        except Exception as exc:
            error_text.append(str(exc))
            finish()

    def on_error(ws, error):
        error_text.append(str(error))
        print(f"[STT] xfyun realtime error: {error}")
        finish()

    def on_close(ws, close_status_code, close_msg):
        if close_status_code or close_msg:
            print(f"[STT] xfyun realtime close: code={close_status_code}, msg={close_msg}")
        finish()

    url = _create_url()
    ws = websocket.WebSocketApp(
        url,
        on_message=on_message,
        on_open=on_open,
        on_error=on_error,
        on_close=on_close,
    )

    pump_task = asyncio.create_task(pump_queue())
    thread = threading.Thread(target=ws.run_forever, daemon=True)
    thread.start()

    try:
        await asyncio.wait_for(done_event.wait(), timeout=20)
    finally:
        stop_event.set()
        sync_queue.put(None)
        try:
            ws.close()
        except Exception:
            pass
        thread.join(timeout=2)
        if not pump_task.done():
            pump_task.cancel()

    if error_text:
        print(f"[STT] xfyun realtime failed: {'; '.join(error_text)[:200]}")
    return "".join(result_text)
