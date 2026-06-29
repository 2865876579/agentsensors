"""
火山引擎 TTS — 豆包语音合成模型 2.0
X-Api-Key 鉴权，seed-tts-2.0-expressive
"""
import re
import base64
import json
import uuid
from collections.abc import AsyncIterator

import aiohttp
import miniaudio
import opuslib

from config import (
    VOLC_API_KEY,
    VOLC_RESOURCE_ID,
    VOLC_VOICE_TYPE,
    VOLC_TTS_SPEED,
    VOLC_TTS_VOLUME,
)

URL = "https://openspeech.bytedance.com/api/v3/tts/unidirectional"
MODEL = "seed-tts-2.0-expressive"
SAMPLE_RATE = 16000
CHANNELS = 1
FRAME_SAMPLES = 960
FADE_SAMPLES = 160
BOUNDARY_SILENCE_SAMPLES = 160

_EMOJI_RE = re.compile(
    "[\U0001F300-\U0001FFFF\U0000FE00-\U0000FE0F\U0000200D\U0001F1E0-\U0001F1FF"
    "☀-➿⭐⭕✂✅✨❄❇❌❓-❗❤➕-➗⤴⤵▪▫▶◀◻-◾☕☺♈-♓♻♿⚓⚠⚡⚪⚫⚽⚾⛄⛅⛔⛪⛲⛳⛵⛺⛽]",
    re.UNICODE,
)


def _fade_pcm16_mono(pcm: bytes) -> bytes:
    sample_count = len(pcm) // 2
    fade = min(FADE_SAMPLES, sample_count // 2)
    if fade <= 1:
        return pcm

    data = bytearray(pcm)
    for i in range(fade):
        in_gain = i / fade
        out_gain = (fade - i - 1) / fade

        start = i * 2
        start_val = int.from_bytes(data[start:start + 2], "little", signed=True)
        start_val = int(start_val * in_gain)
        data[start:start + 2] = int(start_val).to_bytes(2, "little", signed=True)

        end = (sample_count - 1 - i) * 2
        end_val = int.from_bytes(data[end:end + 2], "little", signed=True)
        end_val = int(end_val * out_gain)
        data[end:end + 2] = int(end_val).to_bytes(2, "little", signed=True)

    return bytes(data)


async def synthesize(text: str, encoder=None) -> AsyncIterator[bytes]:
    text = _EMOJI_RE.sub("", text).strip()
    if not text:
        return

    _enc = encoder
    if _enc is None:
        _enc = opuslib.Encoder(SAMPLE_RATE, CHANNELS, opuslib.APPLICATION_VOIP)

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "X-Api-Key": VOLC_API_KEY,
        "X-Api-Resource-Id": VOLC_RESOURCE_ID,
        "X-Api-Request-Id": f"esp32-tts-{uuid.uuid4()}",
    }
    body = {
        "user": {"uid": "esp32"},
        "req_params": {
            "text": text,
            "speaker": VOLC_VOICE_TYPE,
            "audio_params": {
                "format": "mp3",
                "sample_rate": SAMPLE_RATE,
                "speech_rate": VOLC_TTS_SPEED,
                "loudness_rate": VOLC_TTS_VOLUME,
            },
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
                response_text = await resp.text()
                mp3_chunks = []
                for line in response_text.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    code = obj.get("code")
                    if code not in (None, 0, 20000000):
                        print(f"[VolcTTS] code={code} message={obj.get('message', '')}")
                        return

                    audio_b64 = obj.get("data") or obj.get("audio") or ""
                    if isinstance(audio_b64, dict):
                        audio_b64 = audio_b64.get("data") or audio_b64.get("audio") or ""
                    if audio_b64:
                        mp3_chunks.append(base64.b64decode(audio_b64))
                if not mp3_chunks:
                    print(f"[VolcTTS] empty audio, response={response_text[:300]}")
                    return
                mp3_data = b"".join(mp3_chunks)

        decoded = miniaudio.decode(
            mp3_data,
            output_format=miniaudio.SampleFormat.SIGNED16,
            nchannels=CHANNELS,
            sample_rate=SAMPLE_RATE,
        )
        pcm = _fade_pcm16_mono(bytes(decoded.samples))
        pcm += b"\x00\x00" * BOUNDARY_SILENCE_SAMPLES

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
