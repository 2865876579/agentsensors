"""
火山引擎 TTS — 豆包语音合成模型 2.0
X-Api-Key 鉴权，seed-tts-2.0-expressive
"""
import re
import base64
import binascii
import json
import uuid
import asyncio
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

_CJK_REPEAT_RE = re.compile(r"([\u4e00-\u9fff])\1{2,}")


def _normalize_tts_text(text: str) -> str:
    """Prevent a malformed streaming reply from speaking one CJK character repeatedly."""
    text = _EMOJI_RE.sub("", str(text or ""))
    return _CJK_REPEAT_RE.sub(r"\1\1", text).strip()


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


def _mask_value(value: str) -> str:
    if not value:
        return "(empty)"
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def _extract_error_details(raw_text: str) -> tuple[str | None, str | None]:
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError:
        return None, None

    header = payload.get("header") if isinstance(payload, dict) else None
    if isinstance(header, dict):
        code = header.get("code")
        message = header.get("message")
        return str(code) if code is not None else None, message

    code = payload.get("code") if isinstance(payload, dict) else None
    message = payload.get("message") if isinstance(payload, dict) else None
    return str(code) if code is not None else None, message


def _log_http_error(status: int, raw_text: str) -> None:
    code, message = _extract_error_details(raw_text)
    detail = f"HTTP {status}"
    if code:
        detail += f" code={code}"
    if message:
        detail += f" message={message}"
    print(f"[VolcTTS] {detail}")

    if code == "45000010" or "Invalid X-Api-Key" in raw_text:
        print(
            "[VolcTTS] 当前走的是新版 API Key 鉴权，但服务端明确返回 Invalid X-Api-Key。"
        )
        print(
            "[VolcTTS] 这个接口需要豆包语音/方舟语音模型页面生成的专属 API Key，"
            "不是 Access Key/Secret，也不是其他产品的普通 API Key。"
        )
        print(
            f"[VolcTTS] resource_id={VOLC_RESOURCE_ID} voice={VOLC_VOICE_TYPE} "
            f"api_key={_mask_value(VOLC_API_KEY)}"
        )
    else:
        print(f"[VolcTTS] raw={raw_text[:300]}")


async def _synthesize_buffered_mp3(text: str, encoder=None) -> AsyncIterator[bytes]:
    text = _EMOJI_RE.sub("", text).strip()
    if not text:
        return
    if not VOLC_API_KEY:
        print("[VolcTTS] 未配置 VOLC_API_KEY")
        return

    _enc = encoder
    if _enc is None:
        _enc = opuslib.Encoder(SAMPLE_RATE, CHANNELS, opuslib.APPLICATION_VOIP)

    headers = {
        "Connection": "keep-alive",
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
            payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
            async with session.post(
                URL,
                headers=headers,
                data=payload,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    err = await resp.text()
                    _log_http_error(resp.status, err)
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


FRAME_BYTES = FRAME_SAMPLES * 2


def _request_body(text: str, audio_format: str) -> dict:
    return {
        "user": {"uid": "esp32"},
        "req_params": {
            "text": text,
            "speaker": VOLC_VOICE_TYPE,
            "audio_params": {
                "format": audio_format,
                "sample_rate": SAMPLE_RATE,
                "speech_rate": VOLC_TTS_SPEED,
                "loudness_rate": VOLC_TTS_VOLUME,
            },
        },
    }


def _audio_bytes_from_response(obj: dict) -> bytes:
    audio = obj.get("data") or obj.get("audio") or ""
    if isinstance(audio, dict):
        audio = audio.get("data") or audio.get("audio") or ""
    if not isinstance(audio, str) or not audio:
        return b""
    try:
        return base64.b64decode(audio)
    except (ValueError, binascii.Error):
        return b""


async def _iter_json_lines(resp) -> AsyncIterator[dict]:
    buffer = bytearray()
    async for chunk in resp.content.iter_chunked(4096):
        buffer.extend(chunk)
        while True:
            newline = buffer.find(b"\n")
            if newline < 0:
                break
            raw = bytes(buffer[:newline]).strip()
            del buffer[:newline + 1]
            if not raw:
                continue
            try:
                obj = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(obj, dict):
                yield obj

    raw = bytes(buffer).strip()
    if raw:
        try:
            obj = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return
        if isinstance(obj, dict):
            yield obj


async def _stream_pcm(
    session: aiohttp.ClientSession,
    headers: dict,
    text: str,
    encoder,
    state: dict[str, bool],
) -> AsyncIterator[bytes]:
    payload = json.dumps(_request_body(text, "pcm"), ensure_ascii=False).encode("utf-8")
    pending = bytearray()
    previous_audio = None
    try:
        async with session.post(
            URL,
            headers=headers,
            data=payload,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as resp:
            if resp.status != 200:
                state["failed"] = True
                return
            async for obj in _iter_json_lines(resp):
                code = obj.get("code")
                if code not in (None, 0, 20000000):
                    print(f"[VolcTTS] PCM code={code} message={obj.get('message', '')}")
                    state["failed"] = True
                    return
                audio = _audio_bytes_from_response(obj)
                if not audio:
                    continue
                # The provider may resend an identical audio chunk during a
                # transient stream retry. Appending it would make a syllable
                # repeat audibly; only suppress exact adjacent duplicates.
                if previous_audio is not None and len(audio) >= 256 and audio == previous_audio:
                    print(f"[VolcTTS] duplicate PCM chunk suppressed bytes={len(audio)}")
                    continue
                previous_audio = audio
                if not state["saw_audio"] and (
                    audio.startswith(b"ID3") or
                    (len(audio) >= 2 and audio[0] == 0xFF and (audio[1] & 0xE0) == 0xE0)
                ):
                    state["failed"] = True
                    print("[VolcTTS] PCM request returned MP3; using buffered fallback")
                    return
                if not state["saw_audio"] and audio.startswith(b"RIFF"):
                    data_marker = audio.find(b"data")
                    if data_marker >= 0 and len(audio) >= data_marker + 8:
                        audio = audio[data_marker + 8:]
                state["saw_audio"] = True
                pending.extend(audio)
                while len(pending) >= FRAME_BYTES:
                    raw_frame = bytes(pending[:FRAME_BYTES])
                    del pending[:FRAME_BYTES]
                    frame = encoder.encode(raw_frame, FRAME_SAMPLES)
                    state["yielded_frame"] = True
                    yield frame

        if state["saw_audio"]:
            pending.extend(b"\x00\x00" * BOUNDARY_SILENCE_SAMPLES)
            while pending:
                raw_frame = bytes(pending[:FRAME_BYTES]).ljust(FRAME_BYTES, b"\x00")
                del pending[:FRAME_BYTES]
                frame = encoder.encode(raw_frame, FRAME_SAMPLES)
                state["yielded_frame"] = True
                yield frame
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        state["failed"] = True
        print(f"[VolcTTS] PCM stream error: {exc}")


async def synthesize(text: str, encoder=None) -> AsyncIterator[bytes]:
    text = _normalize_tts_text(text)
    if not text:
        return
    if not VOLC_API_KEY:
        print("[VolcTTS] 未配置 VOLC_API_KEY")
        return

    _enc = encoder or opuslib.Encoder(SAMPLE_RATE, CHANNELS, opuslib.APPLICATION_VOIP)
    headers = {
        "Connection": "keep-alive",
        "Content-Type": "application/json; charset=utf-8",
        "X-Api-Key": VOLC_API_KEY,
        "X-Api-Resource-Id": VOLC_RESOURCE_ID,
        "X-Api-Request-Id": f"esp32-tts-{uuid.uuid4()}",
    }
    state = {"saw_audio": False, "failed": False, "yielded_frame": False}
    try:
        async with aiohttp.ClientSession() as session:
            async for frame in _stream_pcm(session, headers, text, _enc, state):
                yield frame
            if state["yielded_frame"]:
                if state["failed"]:
                    print("[VolcTTS] PCM stream stopped after audio; no duplicate fallback")
                return
            async for frame in _synthesize_buffered_mp3(text, _enc):
                yield frame
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        print(f"[VolcTTS] network error: {exc}")
    except Exception as exc:
        import traceback
        print(f"[VolcTTS] failed: {exc}")
        traceback.print_exc()
