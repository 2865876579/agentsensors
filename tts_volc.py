"""
火山引擎 TTS — 豆包语音合成模型 2.0
X-Api-Key 鉴权，seed-tts-2.0-expressive
"""
import re
import base64
import json
from collections.abc import AsyncIterator

import aiohttp
import miniaudio
import opuslib

from config import VOLC_API_KEY, VOLC_VOICE_TYPE, VOLC_TTS_SPEED, VOLC_TTS_VOLUME

URL = "https://openspeech.bytedance.com/api/v3/tts/unidirectional"
MODEL = "seed-tts-2.0-expressive"
SAMPLE_RATE = 16000
CHANNELS = 1
FRAME_SAMPLES = 960

_EMOJI_RE = re.compile(
    "[\U0001F300-\U0001FFFF\U0000FE00-\U0000FE0F\U0000200D\U0001F1E0-\U0001F1FF"
    "☀-➿⭐⭕✂✅✨❄❇❌❓-❗❤➕-➗⤴⤵▪▫▶◀◻-◾☕☺♈-♓♻♿⚓⚠⚡⚪⚫⚽⚾⛄⛅⛔⛪⛲⛳⛵⛺⛽]",
    re.UNICODE,
)


async def synthesize(text: str, encoder=None) -> AsyncIterator[bytes]:
    text = _EMOJI_RE.sub("", text).strip()
    if not text:
        return

    _enc = encoder
    if _enc is None:
        _enc = opuslib.Encoder(SAMPLE_RATE, CHANNELS, opuslib.APPLICATION_VOIP)

    headers = {
        "Content-Type": "application/json",
        "X-Api-Key": VOLC_API_KEY,
        "X-Api-Resource-Id": "seed-tts-2.0",
    }
    body = {
        "user": {"uid": "esp32"},
        "req_params": {
            "text": text,
            "model": MODEL,
            "speaker": VOLC_VOICE_TYPE,
            "audio_params": {
                "format": "mp3",
                "sample_rate": SAMPLE_RATE,
                "speech_rate": VOLC_TTS_SPEED,
                "loudness_rate": VOLC_TTS_VOLUME,
            },
            "additions": json.dumps({"silence_duration": 300}),
        },
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                URL, headers=headers, json=body,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    err = await resp.text()
                    print(f"[VolcTTS] HTTP {resp.status}: {err[:300]}")
                    return
                text = await resp.text()
                mp3_chunks = []
                for line in text.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    audio_b64 = obj.get("data", "")
                    if audio_b64:
                        mp3_chunks.append(base64.b64decode(audio_b64))
                if not mp3_chunks:
                    print("[VolcTTS] 返回空音频")
                    return
                mp3_data = b"".join(mp3_chunks)

        decoded = miniaudio.decode(
            mp3_data,
            output_format=miniaudio.SampleFormat.SIGNED16,
            nchannels=CHANNELS,
            sample_rate=SAMPLE_RATE,
        )
        pcm = bytes(decoded.samples)

        total_samples = len(pcm) // 2
        pos = 0
        frame_count = 0
        while pos < total_samples:
            end = pos + FRAME_SAMPLES
            if end > total_samples:
                end = total_samples
            raw_frame = pcm[pos * 2 : end * 2]
            if len(raw_frame) < FRAME_SAMPLES * 2:
                raw_frame = raw_frame.ljust(FRAME_SAMPLES * 2, b"\x00")
            yield _enc.encode(raw_frame, FRAME_SAMPLES)
            frame_count += 1
            pos = end

        print(f"[VolcTTS] OK, {frame_count} frames, text_len={len(text)}")

    except aiohttp.ClientError as e:
        print(f"[VolcTTS] 网络错误: {e}")
    except Exception as e:
        import traceback
        print(f"[VolcTTS] 失败: {e}")
        traceback.print_exc()
