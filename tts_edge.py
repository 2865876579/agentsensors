"""
Edge TTS 文字转语音模块 —— Opus 编码版

输出格式：Opus 压缩帧（60ms/帧，16kHz 单声道）
每个 Opus 帧约 80-120 字节，通过 WebSocket binary frame 直接发送。

对齐小智（xiaozhi-esp32）架构：binary frame + Opus 压缩。
"""
import re
from collections.abc import AsyncIterator

import edge_tts
import miniaudio
import opuslib
from config import TTS_VOICE, TTS_RATE, TTS_VOLUME

# 匹配 emoji 和特殊符号，TTS 不会读这些
_EMOJI_RE = re.compile(
    "[\U0001F300-\U0001FFFF"   # 表情符号、杂项符号
    "\U0000FE00-\U0000FE0F"    # 变体选择器
    "\U0000200D"                # 零宽连接符
    "\U0001F1E0-\U0001F1FF"    # 区域指示符（国旗）
    "☀-➿"            # 杂项符号
    "⭐⭕✂✅✨❄❇❌❓-❗❤➕-➗⤴⤵"  # 常见 emoji
    "▪▫▶◀◻-◾☕☺♈-♓♻♿⚓⚠⚡⚪⚫⚽⚾⛄⛅⛔⛪⛲⛳⛵⛺⛽"  # 更多符号
    "]",
    re.UNICODE,
)

# Opus 参数
SAMPLE_RATE = 16000
CHANNELS = 1
FRAME_DURATION_MS = 60
FRAME_SAMPLES = SAMPLE_RATE * FRAME_DURATION_MS // 1000  # 960 samples


async def synthesize(text: str, encoder=None) -> AsyncIterator[bytes]:
    """
    文字转语音流（Opus 编码）。encoder 为 None 时内部创建，传入则复用。

    产出：Opus 编码音频帧（每帧 60ms，约 80-120 字节）
    """
    text = _EMOJI_RE.sub("", text).strip()
    if not text:
        return

    _enc = encoder
    if _enc is None:
        _enc = opuslib.Encoder(SAMPLE_RATE, CHANNELS, opuslib.APPLICATION_VOIP)

    import time as _time
    _t0 = _time.monotonic()
    try:
        print(f"[TTS] Step1: Edge TTS 开始, voice={TTS_VOICE}, rate={TTS_RATE}, volume={TTS_VOLUME}")
        communicate = edge_tts.Communicate(text, TTS_VOICE, rate=TTS_RATE, volume=TTS_VOLUME)
        mp3_chunks: list[bytes] = []
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                mp3_chunks.append(chunk["data"])
        _t1 = _time.monotonic()
        print(f"[TTS] Step1: Edge TTS 完成, chunks={len(mp3_chunks)},耗时={_t1-_t0:.1f}s")

        if not mp3_chunks:
            print("[TTS] Step1: Edge TTS 返回空音频!")
            return

        print(f"[TTS] Step2: MP3→PCM 解码开始, mp3_size={sum(len(c) for c in mp3_chunks)}")
        mp3_data = b"".join(mp3_chunks)
        decoded = miniaudio.decode(
            mp3_data,
            output_format=miniaudio.SampleFormat.SIGNED16,
            nchannels=CHANNELS,
            sample_rate=SAMPLE_RATE,
        )
        pcm = bytes(decoded.samples)
        _t2 = _time.monotonic()
        print(f"[TTS] Step2: MP3→PCM 完成, pcm_samples={len(pcm)//2},耗时={_t2-_t1:.1f}s")

        # ★ PCM → Opus 帧。统一处理：不足 60ms 的尾帧补零
        total_samples = len(pcm) // 2
        frame_count = 0
        pos = 0
        while pos < total_samples:
            end = pos + FRAME_SAMPLES
            if end > total_samples:
                end = total_samples
            raw_frame = pcm[pos * 2 : end * 2]
            if len(raw_frame) < FRAME_SAMPLES * 2:
                raw_frame = raw_frame.ljust(FRAME_SAMPLES * 2, b"\x00")

            opus_frame = _enc.encode(raw_frame, FRAME_SAMPLES)
            frame_count += 1
            yield opus_frame
            pos = end

        _t3 = _time.monotonic()
        print(f"[TTS] Step3: Opus 编码完成, frames={frame_count},耗时={_t3-_t2:.1f}s,总耗时={_t3-_t0:.1f}s")

    except Exception as e:
        import traceback
        print(f"[TTS] 失败: {e}")
        traceback.print_exc()
        return
