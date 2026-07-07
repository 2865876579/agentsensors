"""
AI avatar generation and LCD resource conversion.

Flow:
  App/cloud prompt -> image2 PNG -> cover crop to 320x480 -> RGB666 bin.

The ESP32 does not call image2 and does not decode images; it only downloads the ready RGB666 file.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import re
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from PIL import Image, ImageOps

from config import (
    AVATAR_PUBLIC_BASE_URL,
    IMAGE2_API_KEY,
    IMAGE2_BASE_URL,
    IMAGE2_MODEL,
    IMAGE2_TEXT_MODEL,
    IMAGE2_QUALITY,
    IMAGE2_REASONING_EFFORT,
    IMAGE2_PROXY,
)


AVATAR_WIDTH = 320
AVATAR_HEIGHT = 480
AVATAR_ROOT = Path(__file__).resolve().parent / "avatars"
AVATAR_CURRENT_DIR = AVATAR_ROOT / "current"
AVATAR_ARCHIVE_DIR = AVATAR_ROOT / "archive"


DEFAULT_AVATAR_PROMPT = (
    "Create a gentle, healing, futuristic AI companion character for a smart sleep pillow product. "
    "Portrait composition, half-body avatar, centered character, clean background, soft blue-purple glow, "
    "premium product visual, suitable for a 320x480 vertical LCD."
)


RESPONSES_INSTRUCTIONS = (
    "You are a tool runner. Pass the user prompt to image_generation VERBATIM. "
    "DO NOT rewrite, expand, polish, or revise it in any way. Use the exact text the user gave."
)


IMAGE_B64_KEYS = {
    "result",
    "image",
    "image_b64",
    "b64_json",
    "base64",
    "partial_image_b64",
}


def _now_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _safe_slug(text: str, max_len: int = 36) -> str:
    slug = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "-", text).strip("-")
    return slug[:max_len] or "avatar"


def _responses_endpoint() -> str:
    base = (IMAGE2_BASE_URL or "").rstrip("/") or "https://www.fhl.mom"
    if base.endswith("/v1"):
        return f"{base}/responses"
    return f"{base}/v1/responses"


def _images_endpoint() -> str:
    # Legacy Images API fallback. Your Image Studio profile mainly uses Responses API.
    base = (IMAGE2_BASE_URL or "").rstrip("/") or "https://www.fhl.mom"
    if base.endswith("/v1"):
        return f"{base}/images/generations"
    return f"{base}/v1/images/generations"


def _strip_data_url(value: str) -> str:
    value = value.strip()
    if value.startswith("data:image") and "," in value:
        return value.split(",", 1)[1].strip()
    return value


def _decode_image_b64(value: str) -> bytes | None:
    value = _strip_data_url(value)
    if len(value) < 128:
        return None
    try:
        raw = base64.b64decode(value, validate=False)
    except Exception:
        return None
    # PNG / JPEG / WEBP magic bytes. The relay currently returns PNG.
    if raw.startswith(b"\x89PNG\r\n\x1a\n") or raw.startswith(b"\xff\xd8\xff") or raw.startswith(b"RIFF"):
        return raw
    return None


def _collect_image_b64(obj: Any, out: list[str], parent_key: str = "") -> None:
    """Recursively collect image base64 fields from Responses/SSE events."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_l = str(key)
            if isinstance(value, str) and key_l in IMAGE_B64_KEYS:
                if _decode_image_b64(value) is not None:
                    out.append(value)
            elif isinstance(value, (dict, list)):
                _collect_image_b64(value, out, key_l)
    elif isinstance(obj, list):
        for item in obj:
            _collect_image_b64(item, out, parent_key)


def _response_error_message(obj: dict[str, Any]) -> str | None:
    typ = str(obj.get("type") or "")
    err = obj.get("error")
    if err:
        if isinstance(err, dict):
            return err.get("message") or json.dumps(err, ensure_ascii=False)[:500]
        return str(err)
    resp = obj.get("response")
    if isinstance(resp, dict) and resp.get("error"):
        err = resp.get("error")
        if isinstance(err, dict):
            return err.get("message") or json.dumps(err, ensure_ascii=False)[:500]
        return str(err)
    if typ in {"response.failed", "response.incomplete"}:
        return json.dumps(obj, ensure_ascii=False)[:500]
    return None



def _httpx_client(timeout: httpx.Timeout) -> httpx.AsyncClient:
    # Keep the proxy scoped to image2 only. Other cloud APIs / ESP32 websockets are unaffected.
    proxy = (IMAGE2_PROXY or "").strip() or None
    return httpx.AsyncClient(timeout=timeout, proxy=proxy, trust_env=False)

def _responses_payload(prompt: str, image_model: str) -> dict[str, Any]:
    # Match RoseKhlifa/Image-Studio shared/kernel/requestModel.js as closely as possible.
    tool: dict[str, Any] = {
        "type": "image_generation",
        "model": image_model,
        "action": "generate",
        "size": "auto",
        "quality": IMAGE2_QUALITY or "auto",
        "output_format": "png",
        "partial_images": 1,
        "background": "auto",
        "moderation": "low",
    }

    payload: dict[str, Any] = {
        "model": IMAGE2_TEXT_MODEL or "gpt-5.5",
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt.strip()},
                ],
            }
        ],
        "tools": [tool],
        "tool_choice": {"type": "image_generation"},
        "reasoning": {"effort": IMAGE2_REASONING_EFFORT or "xhigh"},
        "store": False,
        "stream": True,
        "instructions": RESPONSES_INSTRUCTIONS,
    }
    return payload


async def _call_image2_responses(prompt: str, image_model: str) -> bytes:
    endpoint = _responses_endpoint()
    headers = {
        "Authorization": f"Bearer {IMAGE2_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
    }
    payload = _responses_payload(prompt, image_model)

    last_b64: str | None = None
    last_error: str | None = None

    timeout = httpx.Timeout(300.0, connect=30.0, read=300.0)
    async with _httpx_client(timeout) as client:
        async with client.stream("POST", endpoint, headers=headers, json=payload) as resp:
            if resp.status_code >= 400:
                body = (await resp.aread()).decode("utf-8", errors="replace")
                raise RuntimeError(f"Responses API HTTP {resp.status_code}: {body[:800]}")

            async for line in resp.aiter_lines():
                line = (line or "").strip()
                if not line:
                    continue
                if line.startswith(":"):
                    continue
                if not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if not data or data == "[DONE]":
                    continue

                try:
                    obj = json.loads(data)
                except json.JSONDecodeError:
                    continue

                err_msg = _response_error_message(obj)
                if err_msg:
                    last_error = err_msg

                found: list[str] = []
                _collect_image_b64(obj, found)
                if found:
                    # SSE may first send partial_image_b64 and later send output_item.done/result.
                    # Always keep the latest valid image payload.
                    last_b64 = found[-1]

    if last_b64:
        raw = _decode_image_b64(last_b64)
        if raw:
            return raw

    if last_error:
        raise RuntimeError(f"Responses API returned no image. Error: {last_error}")
    raise RuntimeError("Responses API returned no image b64; check whether the relay allows image_generation.")


async def _call_image2_images_legacy(prompt: str, size: str = "1024x1536") -> bytes:
    """Legacy Images API fallback. Your screenshot is not this mode."""
    endpoint = _images_endpoint()
    headers = {
        "Authorization": f"Bearer {IMAGE2_API_KEY}",
        "Content-Type": "application/json",
    }
    payload: dict[str, Any] = {
        "model": IMAGE2_MODEL or "gpt-image-2",
        "prompt": prompt,
        "size": size,
        "n": 1,
        "response_format": "b64_json",
    }

    async with _httpx_client(httpx.Timeout(180.0, connect=20.0)) as client:
        resp = await client.post(endpoint, headers=headers, json=payload)
        if resp.status_code >= 400 and "response_format" in resp.text:
            payload.pop("response_format", None)
            resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        body = resp.json()

        item = (body.get("data") or [{}])[0]
        b64 = item.get("b64_json") or item.get("base64")
        if b64:
            raw = _decode_image_b64(b64)
            if raw:
                return raw
            return base64.b64decode(_strip_data_url(b64), validate=False)

        url = item.get("url")
        if url:
            img_resp = await client.get(url)
            img_resp.raise_for_status()
            return img_resp.content

    raise RuntimeError("Images API returned neither b64_json nor url")


async def _call_image2(prompt: str) -> bytes:
    if not IMAGE2_API_KEY:
        raise RuntimeError("IMAGE2_API_KEY is not configured")

    configured_model = IMAGE2_MODEL or "gpt-image-2-codex"
    candidates: list[str] = []
    for model in [configured_model, "gpt-image-2-codex", "gpt-image-2"]:
        if model and model not in candidates:
            candidates.append(model)

    errors: list[str] = []
    retry_delays = [20, 45, 90, 120]
    for model_index, model in enumerate(candidates):
        # Image Studio logs show upstream_error followed by a successful retry after a noticeable delay.
        # qjc.one/Cloudflare 502 is usually an upstream-node issue; immediate retries often hit the same bad window.
        max_attempts = 5 if model_index == 0 else 2
        for attempt in range(1, max_attempts + 1):
            try:
                return await _call_image2_responses(prompt, model)
            except Exception as exc:
                err = f"Responses/{model}/attempt{attempt}: {exc}"
                errors.append(err)
                msg = str(exc).lower()
                fatal = any(key in msg for key in [
                    "unauthorized",
                    "invalid api key",
                    "invalid_api_key",
                    "insufficient",
                    "quota",
                    "rate_limit",
                    "rate limit",
                ])
                retryable = any(key in msg for key in [
                    "502",
                    "503",
                    "504",
                    "bad gateway",
                    "upstream",
                    "timeout",
                    "temporarily",
                    "cf-error",
                    "cloudflare",
                    "qjc.one",
                ])
                if fatal:
                    break
                if not retryable:
                    break
                if attempt >= max_attempts:
                    break
                delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                await asyncio.sleep(delay)

    # Keep the old endpoint as a last fallback, but your current relay profile probably will not use it.
    try:
        return await _call_image2_images_legacy(prompt)
    except Exception as exc:
        errors.append(f"Images legacy: {exc}")

    # Do not send a full Cloudflare HTML page back to the H5 UI.
    compact_errors = []
    for e in errors[-8:]:
        e = re.sub(r"<[^>]+>", " ", e)
        e = re.sub(r"\s+", " ", e).strip()
        compact_errors.append(e[:260])
    raise RuntimeError("image2 generation failed after retries; " + " | ".join(compact_errors))


def _cover_resize_to_lcd(src_path: Path, out_png: Path) -> Image.Image:
    with Image.open(src_path) as im:
        im = ImageOps.exif_transpose(im).convert("RGB")
        target_ratio = AVATAR_WIDTH / AVATAR_HEIGHT
        src_ratio = im.width / im.height
        if src_ratio > target_ratio:
            new_h = AVATAR_HEIGHT
            new_w = round(new_h * src_ratio)
        else:
            new_w = AVATAR_WIDTH
            new_h = round(new_w / src_ratio)
        im = im.resize((new_w, new_h), Image.Resampling.LANCZOS)
        left = max(0, (new_w - AVATAR_WIDTH) // 2)
        top = max(0, (new_h - AVATAR_HEIGHT) // 2)
        im = im.crop((left, top, left + AVATAR_WIDTH, top + AVATAR_HEIGHT))
        out_png.parent.mkdir(parents=True, exist_ok=True)
        im.save(out_png, "PNG", optimize=True)
        return im.copy()


def _write_rgb666_bin(image: Image.Image, out_bin: Path) -> int:
    image = image.convert("RGB")
    pixels = image.tobytes()
    out = bytearray(len(pixels))
    # ILI9488 RGB666: each channel uses upper 6 bits, lower 2 bits cleared.
    for i, value in enumerate(pixels):
        out[i] = value & 0xFC
    out_bin.parent.mkdir(parents=True, exist_ok=True)
    out_bin.write_bytes(out)
    return binascii.crc32(out) & 0xFFFFFFFF


def _public_url(path: str) -> str:
    base = (AVATAR_PUBLIC_BASE_URL or "").rstrip("/")
    return f"{base}{path}"


def _write_manifest(target_dir: Path, avatar_id: str, prompt: str, crc32: int, bin_size: int) -> dict:
    manifest = {
        "ok": True,
        "id": avatar_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "prompt": prompt,
        "width": AVATAR_WIDTH,
        "height": AVATAR_HEIGHT,
        "format": "rgb666",
        "bytes_per_pixel": 3,
        "bin_size": bin_size,
        "crc32": f"{crc32:08x}",
        "files": {
            "preview": "/api/avatar/current/preview.png",
            "rgb666": "/api/avatar/current/avatar_base_rgb666.bin",
            "manifest": "/api/avatar/current/manifest",
        },
        "urls": {
            "preview": _public_url("/api/avatar/current/preview.png"),
            "rgb666": _public_url("/api/avatar/current/avatar_base_rgb666.bin"),
            "manifest": _public_url("/api/avatar/current/manifest"),
        },
    }
    (target_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return manifest


def get_current_avatar_manifest() -> dict:
    manifest_path = AVATAR_CURRENT_DIR / "manifest.json"
    if not manifest_path.exists():
        return {"ok": False, "error": "No LCD avatar has been generated yet"}
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"ok": False, "error": f"Failed to read manifest: {exc}"}


async def generate_lcd_avatar(prompt: str | None = None) -> dict:
    prompt = (prompt or "").strip() or DEFAULT_AVATAR_PROMPT
    full_prompt = (
        prompt
        + "\n\nHard requirements: vertical portrait, single AI companion character, no text, no watermark, simple background. "
        + "The head and upper body must be clear, suitable for cropping into a 320x480 LCD image."
    )

    avatar_id = f"{_now_id()}_{_safe_slug(prompt)}"
    work_dir = AVATAR_ARCHIVE_DIR / avatar_id
    work_dir.mkdir(parents=True, exist_ok=True)

    raw_path = work_dir / "source.png"
    preview_path = work_dir / "preview.png"
    bin_path = work_dir / "avatar_base_rgb666.bin"

    raw_bytes = await _call_image2(full_prompt)
    raw_path.write_bytes(raw_bytes)
    lcd_image = _cover_resize_to_lcd(raw_path, preview_path)
    crc32 = _write_rgb666_bin(lcd_image, bin_path)
    manifest = _write_manifest(work_dir, avatar_id, prompt, crc32, bin_path.stat().st_size)

    tmp_current = AVATAR_ROOT / f".current_tmp_{int(time.time())}"
    if tmp_current.exists():
        shutil.rmtree(tmp_current)
    shutil.copytree(work_dir, tmp_current)
    if AVATAR_CURRENT_DIR.exists():
        shutil.rmtree(AVATAR_CURRENT_DIR)
    tmp_current.rename(AVATAR_CURRENT_DIR)
    return manifest


def current_preview_path() -> Path:
    return AVATAR_CURRENT_DIR / "preview.png"


def current_rgb666_path() -> Path:
    return AVATAR_CURRENT_DIR / "avatar_base_rgb666.bin"
