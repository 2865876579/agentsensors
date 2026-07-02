"""
NetEase Cloud Music playback helper.

This module searches for a playable song URL and converts the downloaded audio
to 16 kHz mono Opus frames, matching the ESP32 playback pipeline.
"""
from __future__ import annotations

import asyncio
import json
import math
import re
import time
import urllib.request
from dataclasses import dataclass
from typing import AsyncIterator

import aiohttp
import miniaudio
import opuslib

from config import NETEASE_BR, NETEASE_COOKIE, NETEASE_MAX_PLAY_SECONDS

SAMPLE_RATE = 16000
CHANNELS = 1
FRAME_SAMPLES = 960
BOUNDARY_SILENCE_SAMPLES = 320
DEFAULT_MAX_AUDIO_BYTES = 80 * 1024 * 1024

SEARCH_URL = "https://music.163.com/api/search/get/web"
PLAYER_URL = "https://music.163.com/api/song/enhance/player/url"
OUTER_URL = "https://music.163.com/song/media/outer/url"
ARTIST_URL = "https://music.163.com/api/artist"

MUSIC_STOP_PHRASES = (
    "停止播放",
    "停止音乐",
    "暂停播放",
    "暂停音乐",
    "关闭音乐",
    "关掉音乐",
    "别放了",
    "不放了",
    "停歌",
)

_LEADING_PLAY_RE = re.compile(
    r"^(?:小安)?(?:帮我|给我|可以)?(?:播放|放一下|放一首|放首|放个|放|听一下|听|来一首|来个|我想听)"
)


@dataclass(frozen=True)
class NeteaseSong:
    id: int
    name: str
    artists: str
    album: str
    duration_ms: int
    url: str
    br: int

    @property
    def label(self) -> str:
        artist = self.artists.strip()
        return f"{self.name} - {artist}" if artist else self.name


def extract_music_query(text: str) -> str | None:
    """Extract song keywords from natural Chinese playback requests."""
    raw = (text or "").strip()
    if not raw:
        return None

    compact = re.sub(r"[\s，。！？、,.!?~～]+", "", raw)
    if any(phrase in compact for phrase in MUSIC_STOP_PHRASES):
        return "__stop_music__"

    if not any(word in raw for word in ("播放", "放", "听", "来一首", "来个")):
        return None

    query = _LEADING_PLAY_RE.sub("", raw, count=1).strip()
    query = re.sub(r"^(?:一下|一首|一个|点|些|音乐|歌曲|歌)\s*", "", query)
    query = re.sub(r"(?:这首歌|这首|歌曲|音乐|给我听|听听|吧|一下)$", "", query).strip()
    query = query.strip("《》“”\"' ，。！？、,.!?~～")

    if query in {"歌", "音乐", "歌曲", "一首歌", "一首音乐"}:
        return None
    return query if len(query) >= 2 else None


def _normalize(text: str) -> str:
    return re.sub(r"[\s《》“”\"'，。！？、,.!?~～\-_/]+", "", text or "").lower()


def _headers() -> dict[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0 Safari/537.36"
        ),
        "Referer": "https://music.163.com/",
        "Accept": "*/*",
    }
    if NETEASE_COOKIE:
        headers["Cookie"] = NETEASE_COOKIE
    return headers


class _UrlAudioStream(miniaudio.StreamableSource):
    def __init__(self, url: str, headers: dict[str, str]):
        request = urllib.request.Request(url, headers=headers)
        self._response = urllib.request.urlopen(request, timeout=20)
        self._bytes_read = 0

    def read(self, num_bytes: int) -> bytes:
        data = self._response.read(num_bytes)
        self._bytes_read += len(data)
        if self._bytes_read > DEFAULT_MAX_AUDIO_BYTES:
            raise RuntimeError("歌曲文件过大，已取消播放")
        return data

    def close(self) -> None:
        try:
            self._response.close()
        except Exception:
            pass


async def _fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    params: dict,
    timeout_sec: float = 12.0,
) -> dict:
    async with session.get(
        url,
        params=params,
        timeout=aiohttp.ClientTimeout(total=timeout_sec),
    ) as resp:
        text = await resp.text()
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}: {text[:120]}")
        return json.loads(text)


async def _search(session: aiohttp.ClientSession, query: str, search_type: int, limit: int = 12) -> dict:
    data = await _fetch_json(
        session,
        SEARCH_URL,
        params={
            "csrf_token": "",
            "s": query,
            "type": search_type,
            "offset": 0,
            "total": "true",
            "limit": limit,
        },
    )
    return data.get("result") or {}


async def _search_songs(session: aiohttp.ClientSession, query: str, limit: int = 12) -> list[dict]:
    result = await _search(session, query, 1, limit)
    songs = result.get("songs") or []
    return songs if isinstance(songs, list) else []


async def _search_artists(session: aiohttp.ClientSession, query: str, limit: int = 5) -> list[dict]:
    result = await _search(session, query, 100, limit)
    artists = result.get("artists") or []
    return artists if isinstance(artists, list) else []


async def _get_artist_hot_songs(session: aiohttp.ClientSession, artist_id: int) -> list[dict]:
    data = await _fetch_json(
        session,
        f"{ARTIST_URL}/{artist_id}",
        params={"csrf_token": ""},
    )
    songs = data.get("hotSongs") or []
    return songs if isinstance(songs, list) else []


async def _get_player_url(session: aiohttp.ClientSession, song_id: int) -> tuple[str, int] | None:
    data = await _fetch_json(
        session,
        PLAYER_URL,
        params={"ids": json.dumps([song_id]), "br": int(NETEASE_BR)},
    )
    rows = data.get("data") or []
    if rows:
        row = rows[0] or {}
        url = str(row.get("url") or "").strip()
        code = int(row.get("code") or 0)
        if url and code == 200:
            return url, int(row.get("br") or NETEASE_BR)

    async with session.get(
        OUTER_URL,
        params={"id": f"{song_id}.mp3"},
        allow_redirects=True,
        timeout=aiohttp.ClientTimeout(total=12),
    ) as resp:
        content_type = (resp.headers.get("Content-Type") or "").lower()
        final_url = str(resp.url)
        if resp.status == 200 and "audio" in content_type and final_url:
            return final_url, int(NETEASE_BR)
    return None


def _score_song(query: str, song: dict, *, title: str = "", artist: str = "") -> int:
    query_norm = _normalize(query)
    title_norm = _normalize(title)
    artist_required = _normalize(artist)
    name = str(song.get("name") or "")
    artist_names = [str(a.get("name") or "") for a in song.get("artists") or [] if isinstance(a, dict)]
    name_norm = _normalize(name)
    artist_norms = [_normalize(a) for a in artist_names]
    artist_joined = _normalize("".join(artist_names))

    if title_norm:
        clean_name = re.sub(r"(?:原唱|翻唱|cover|伴奏|dj|版|女版|男版)", "", name_norm, flags=re.IGNORECASE)
        if title_norm != name_norm and title_norm not in clean_name:
            return -1000

    if artist_required:
        # 明确要某个歌手时，只接受单一歌手精确匹配或近似精确匹配。
        # 否则会把“周杰伦.、INKK”这种搬运/拼接源当成原唱，实际播放经常不对。
        exact_artist = any(
            artist_required == a or artist_required in a or a in artist_required
            for a in artist_norms
        )
        if not exact_artist:
            return -1000
        if len([a for a in artist_norms if a]) > 1:
            return -900

    score = 0
    if title_norm and title_norm == name_norm:
        score += 120
    elif query_norm == name_norm:
        score += 80
    elif name_norm and name_norm in query_norm:
        score += 60
    elif query_norm and query_norm in name_norm:
        score += 40

    if artist_required:
        score += 80
    elif artist_joined and artist_joined in query_norm:
        score += 25

    if "vip" in str(song).lower():
        score -= 2
    return score


def _song_artists(song: dict) -> str:
    artists = song.get("artists") or []
    if not artists:
        artists = song.get("ar") or []
    names = [str(a.get("name") or "").strip() for a in artists if isinstance(a, dict)]
    return "、".join([name for name in names if name])


def _artist_names(song: dict) -> list[str]:
    artists = song.get("artists") or song.get("ar") or []
    return [str(a.get("name") or "").strip() for a in artists if isinstance(a, dict)]


def _artist_matches(name: str, required: str) -> bool:
    name_norm = _normalize(name)
    required_norm = _normalize(required)
    return bool(name_norm and required_norm and (
        name_norm == required_norm or required_norm in name_norm or name_norm in required_norm
    ))


def _song_from_dict(song: dict, url: str, br: int) -> NeteaseSong:
    album = song.get("album") or song.get("al") or {}
    duration = song.get("duration") or song.get("dt") or 0
    return NeteaseSong(
        id=int(song.get("id")),
        name=str(song.get("name") or ""),
        artists=_song_artists(song),
        album=str(album.get("name") or "") if isinstance(album, dict) else "",
        duration_ms=int(duration or 0),
        url=url,
        br=br,
    )


async def _find_playable_artist_hot_song(session: aiohttp.ClientSession, artist: str) -> NeteaseSong | None:
    artists = await _search_artists(session, artist)
    required_norm = _normalize(artist)
    matched_artist = None
    for item in artists:
        name = str(item.get("name") or "")
        if _artist_matches(name, artist):
            matched_artist = item
            break
    if not matched_artist and artists:
        first_name = str(artists[0].get("name") or "")
        if _normalize(first_name) == required_norm:
            matched_artist = artists[0]
    if not matched_artist:
        return None

    artist_id = int(matched_artist.get("id") or 0)
    if not artist_id:
        return None
    hot_songs = await _get_artist_hot_songs(session, artist_id)
    for song in hot_songs:
        names = _artist_names(song)
        if not any(_artist_matches(name, artist) for name in names):
            continue
        # 歌手页热门歌可能包含合作曲，这里允许合作，但必须有指定歌手。
        playable = await _get_player_url(session, int(song.get("id") or 0))
        if not playable:
            continue
        url, br = playable
        return _song_from_dict(song, url, br)
    return None


async def find_playable_song(
    query: str,
    *,
    title: str = "",
    artist: str = "",
    kind: str = "",
) -> NeteaseSong | None:
    query = (query or "").strip()
    if not query:
        return None

    async with aiohttp.ClientSession(headers=_headers()) as session:
        if artist and not title and kind == "artist":
            return await _find_playable_artist_hot_song(session, artist)

        songs = await _search_songs(session, query)
        if not songs:
            return None

        ranked = sorted(
            songs,
            key=lambda item: _score_song(query, item, title=title, artist=artist),
            reverse=True,
        )
        for song in ranked:
            score = _score_song(query, song, title=title, artist=artist)
            if score < 0:
                continue
            song_id = song.get("id")
            if not song_id:
                continue
            playable = await _get_player_url(session, int(song_id))
            if not playable:
                continue
            url, br = playable
            return _song_from_dict(song, url, br)
    return None


def _apply_gain(pcm: bytes, gain: float) -> bytes:
    if not math.isfinite(gain) or abs(gain - 1.0) < 0.001:
        return pcm
    gain = max(0.05, min(2.0, gain))
    data = bytearray(pcm)
    for offset in range(0, len(data) - 1, 2):
        sample = int.from_bytes(data[offset:offset + 2], "little", signed=True)
        sample = max(-32768, min(32767, int(sample * gain)))
        data[offset:offset + 2] = int(sample).to_bytes(2, "little", signed=True)
    return bytes(data)


async def download_song_audio(song: NeteaseSong) -> bytes:
    headers = _headers()
    headers["Accept"] = "audio/*,*/*"
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(
            song.url,
            timeout=aiohttp.ClientTimeout(total=45),
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"歌曲下载失败 HTTP {resp.status}: {text[:120]}")
            content_length = int(resp.headers.get("Content-Length") or 0)
            if content_length > DEFAULT_MAX_AUDIO_BYTES:
                raise RuntimeError("歌曲文件过大，已取消播放")
            audio = await resp.read()
            if len(audio) > DEFAULT_MAX_AUDIO_BYTES:
                raise RuntimeError("歌曲文件过大，已取消播放")
            return audio


def decode_music_to_pcm(audio: bytes, *, max_play_seconds: int | None = None) -> bytes:
    decoded = miniaudio.decode(
        audio,
        output_format=miniaudio.SampleFormat.SIGNED16,
        nchannels=CHANNELS,
        sample_rate=SAMPLE_RATE,
    )
    pcm = bytes(decoded.samples)
    max_seconds = int(max_play_seconds or NETEASE_MAX_PLAY_SECONDS or 0)
    if max_seconds > 0:
        max_bytes = max_seconds * SAMPLE_RATE * CHANNELS * 2
        pcm = pcm[:max_bytes]
    pcm = _apply_gain(pcm, 0.88)
    pcm += b"\x00\x00" * BOUNDARY_SILENCE_SAMPLES
    return pcm


def _next_stream_chunk(stream) -> bytes | None:
    try:
        samples = next(stream)
    except StopIteration:
        return None
    return samples.tobytes()


async def iter_song_opus_frames(song: NeteaseSong, encoder=None) -> AsyncIterator[bytes]:
    started = time.monotonic()
    headers = _headers()
    headers["Accept"] = "audio/*,*/*"
    source = await asyncio.to_thread(_UrlAudioStream, song.url, headers)
    stream = miniaudio.stream_any(
        source,
        source_format=miniaudio.FileFormat.MP3,
        output_format=miniaudio.SampleFormat.SIGNED16,
        nchannels=CHANNELS,
        sample_rate=SAMPLE_RATE,
        frames_to_read=FRAME_SAMPLES,
    )

    enc = encoder or opuslib.Encoder(SAMPLE_RATE, CHANNELS, opuslib.APPLICATION_AUDIO)
    max_seconds = int(NETEASE_MAX_PLAY_SECONDS or 0)
    max_samples = max_seconds * SAMPLE_RATE if max_seconds > 0 else 0
    sent_samples = 0
    frame_count = 0

    try:
        while True:
            raw_frame = await asyncio.to_thread(_next_stream_chunk, stream)
            if raw_frame is None:
                break
            if not raw_frame:
                continue
            if len(raw_frame) < FRAME_SAMPLES * 2:
                raw_frame = raw_frame.ljust(FRAME_SAMPLES * 2, b"\x00")
            elif len(raw_frame) > FRAME_SAMPLES * 2:
                raw_frame = raw_frame[:FRAME_SAMPLES * 2]
            raw_frame = _apply_gain(raw_frame, 0.88)
            yield enc.encode(raw_frame, FRAME_SAMPLES)
            frame_count += 1
            sent_samples += FRAME_SAMPLES
            if max_samples and sent_samples >= max_samples:
                break

        silence = b"\x00\x00" * FRAME_SAMPLES
        for _ in range(max(1, BOUNDARY_SILENCE_SAMPLES // FRAME_SAMPLES)):
            yield enc.encode(silence, FRAME_SAMPLES)
    finally:
        await asyncio.to_thread(source.close)
        print(
            f"[Music] streamed frames={frame_count}, seconds={sent_samples / SAMPLE_RATE:.1f}, "
            f"cost={time.monotonic() - started:.2f}s, song={song.label}"
        )
