"""
本地测试客户端 - 模拟 ESP32 发送文字消息测试完整链路

用法：
  1. 先启动服务端：python main.py
  2. 再运行本脚本：python test_client.py
  3. 输入文字即可测试对话，AI 回复会保存为 reply.mp3

功能：
  - 跳过 STT（直接发文字），测试 LLM 对话 + TTS 合成
  - 不需要麦克风和 ESP32 硬件
  - 用于验证云端服务是否正常工作

环境变量：
  WS_URL: WebSocket 地址，默认 ws://localhost:8000/ws/esp32
  测试远程服务器时设为 ws://你的服务器IP:8000/ws/esp32
"""
import asyncio
import json
import base64
import websockets
import os


async def recv_tts_stream(ws, expected_turn_id: int) -> None:
    """接收服务端的流式 TTS 消息，直到 tts_audio_end。"""
    audio_parts: list[bytes] = []
    reply_text = ""
    stream_source = "assistant"
    stream_turn_id: int | None = None

    while True:
        response = await asyncio.wait_for(ws.recv(), timeout=180)
        data = json.loads(response)
        msg_type = data.get("type")

        if msg_type == "tts_audio_start":
            reply_text = data.get("text", "")
            stream_source = data.get("source", "assistant")
            stream_turn_id = data.get("turn_id")
            if stream_turn_id is not None:
                stream_turn_id = int(stream_turn_id)
            if stream_source == "assistant" and stream_turn_id == expected_turn_id:
                print(f"AI: {reply_text}")
            else:
                print(f"[异步] {reply_text}")

        elif msg_type == "tts_audio_chunk":
            audio_b64 = data.get("audio", "")
            if audio_b64:
                audio_parts.append(base64.b64decode(audio_b64))

        elif msg_type == "tts_audio_end":
            is_current_answer = stream_source == "assistant" and stream_turn_id == expected_turn_id
            if not reply_text and is_current_answer:
                reply_text = data.get("text", "")
                print(f"AI: {reply_text}")
            if audio_parts:
                audio_bytes = b"".join(audio_parts)
                filename = "reply.mp3"
                with open(filename, "wb") as f:
                    f.write(audio_bytes)
                if is_current_answer:
                    print(f"[音频已保存到 {filename}，{len(audio_bytes)} bytes]\n")
            else:
                if is_current_answer:
                    print("[TTS 未生成音频，但文字回复正常]\n")
            audio_parts = []
            reply_text = ""
            if is_current_answer:
                return

        # 兼容旧协议，方便回归测试。
        elif msg_type == "tts_audio":
            print(f"AI: {data.get('text')}")
            audio_b64 = data.get("audio", "")
            if audio_b64:
                audio_bytes = base64.b64decode(audio_b64)
                filename = "reply.mp3"
                with open(filename, "wb") as f:
                    f.write(audio_bytes)
                print(f"[音频已保存到 {filename}，{len(audio_bytes)} bytes]\n")
            else:
                print("[TTS 未生成音频，但文字回复正常]\n")
            return

        elif msg_type == "stt_result":
            print(f"[识别] {data.get('text')}")

        elif msg_type == "status":
            source = data.get("source", "")
            prefix = "[异步状态]" if source == "pc_result" else "[状态]"
            print(f"{prefix} {data.get('msg')}\n")


async def test_text_mode():
    """文字模式测试：跳过 STT，直接发文字给 LLM，收到 TTS 音频保存为 mp3"""
    # 连接地址，默认本地，可通过环境变量改为远程服务器
    uri = os.getenv("WS_URL", "ws://localhost:8000/ws/esp32")

    while True:
        try:
            print(f"连接到 {uri} ...")
            async with websockets.connect(
                uri,
                max_size=10 * 1024 * 1024,
                ping_interval=30,
                ping_timeout=120
            ) as ws:
                print("已连接！输入文字测试对话，输入 quit 退出\n")
                turn_id = 0

                while True:
                    # input() 会阻塞事件循环，导致 WebSocket 不能及时响应 ping。
                    # 放到线程里执行，连接空闲时仍能正常处理 keepalive。
                    text = await asyncio.to_thread(input, "你: ")
                    text = text.strip()
                    if text.lower() == "quit":
                        return
                    if not text:
                        continue
                    turn_id += 1

                    # 发送文字消息给服务端（type=text 会跳过 STT，直接送 LLM）
                    await ws.send(json.dumps({
                        "type": "text",
                        "text": text
                    }, ensure_ascii=False))

                    try:
                        await recv_tts_stream(ws, turn_id)
                    except asyncio.TimeoutError:
                        print("[超时] 服务端 180 秒没完成 TTS 回复\n")

        except (ConnectionRefusedError, OSError, websockets.exceptions.ConnectionClosed) as e:
            print(f"[断线] {e}，5秒后重连...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(test_text_mode())
