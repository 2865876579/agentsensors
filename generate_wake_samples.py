"""
生成唤醒词训练样本 — 火山引擎 TTS × 多参数组合 × 数据增强
输出 200+ 条 16kHz mono WAV 到 training_data/
"""
import os, io, random, struct, wave
import base64, json, hashlib, hmac, time, uuid
from concurrent.futures import ThreadPoolExecutor

import requests
import miniaudio

# ── 火山引擎 TTS 配置 ──
API_KEY = "0125ffe2-58de-4004-a3aa-8b39ae0fc3da"
TTS_URL = "https://openspeech.bytedance.com/api/v3/tts/unidirectional"
WAKE_WORD = "你好小眠"
OUTPUT_DIR = "training_data"
SAMPLE_RATE = 16000

# ── 参数组合 ──
SPEAKERS = [
    "zh_female_vv_uranus_bigtts",
    "zh_female_shuangkuaisisi_moon_bigtts",
    "zh_male_bvlazysheep",
    "BV120_streaming",
    "zh_male_ahu_conversation_wvae_bigtts",
]
SPEEDS = [0.8, 1.0, 1.2, 1.5]
VOLUMES = [0.8, 1.0]

HEADERS = {
    "Content-Type": "application/json",
    "X-Api-Key": API_KEY,
    "X-Api-Resource-Id": "seed-tts-2.0",
}


def generate_tts(text: str, speaker: str, speed: float, volume: float) -> bytes:
    """调火山引擎 TTS，返回 PCM 16kHz bytes"""
    speech_rate = int((speed - 1.0) * 100)
    body = {
        "user": {"uid": str(uuid.uuid4())[:8]},
        "req_params": {
            "text": text,
            "model": "seed-tts-2.0-expressive",
            "speaker": speaker,
            "audio_params": {
                "format": "mp3",
                "sample_rate": SAMPLE_RATE,
                "speech_rate": speech_rate,
                "loudness_rate": int((volume - 1.0) * 100),
            },
            "additions": json.dumps({"silence_duration": 100}),
        },
    }
    resp = requests.post(TTS_URL, headers=HEADERS, json=body, timeout=30)
    if resp.status_code != 200:
        print(f"  TTS HTTP {resp.status_code}: {resp.text[:100]}")
        return b""

    mp3_data = b""
    for line in resp.text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        b64 = obj.get("data", "")
        if b64:
            mp3_data += base64.b64decode(b64)

    if not mp3_data:
        print(f"  TTS 返回空音频")
        return b""

    decoded = miniaudio.decode(
        mp3_data,
        output_format=miniaudio.SampleFormat.SIGNED16,
        nchannels=1,
        sample_rate=SAMPLE_RATE,
    )
    return bytes(decoded.samples)


def apply_enhancements(pcm_bytes: bytes, idx: int) -> bytes:
    """随机数据增强：噪声、音量、微变速、裁剪"""
    samples = list(struct.unpack(f"<{len(pcm_bytes)//2}h", pcm_bytes))
    rng = random.Random(idx * 42 + 7)

    # 1. 随机音量 ±25%
    gain = rng.uniform(0.75, 1.25)
    samples = [max(-32768, min(32767, int(s * gain))) for s in samples]

    # 2. 加噪声
    noise_level = rng.uniform(0, 0.02)  # 最多 2% 白噪声
    if noise_level > 0:
        noise_peak = int(noise_level * 32767)
        samples = [max(-32768, min(32767, s + rng.randint(-noise_peak, noise_peak))) for s in samples]

    # 3. 微变速（线性重采样模拟 ±8%）
    speed_factor = rng.uniform(0.92, 1.08)
    if abs(speed_factor - 1.0) > 0.01:
        new_len = int(len(samples) / speed_factor)
        new_samples = []
        for i in range(new_len):
            src = i * speed_factor
            src_i = int(src)
            frac = src - src_i
            if src_i + 1 < len(samples):
                new_samples.append(int(samples[src_i] * (1 - frac) + samples[src_i + 1] * frac))
            else:
                new_samples.append(samples[src_i])
        samples = new_samples

    # 4. 随机前后加/删静音 (0~200ms)
    pre_silence = rng.randint(0, int(SAMPLE_RATE * 0.2))
    post_silence = rng.randint(0, int(SAMPLE_RATE * 0.15))
    samples = [0] * pre_silence + samples + [0] * post_silence

    # 5. 裁剪保证 1~2.5 秒
    max_len = int(SAMPLE_RATE * 2.5)
    if len(samples) > max_len:
        samples = samples[:max_len]

    return struct.pack(f"<{len(samples)}h", *samples)


def save_wav(path: str, pcm_bytes: bytes):
    with wave.open(path, "w") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(SAMPLE_RATE)
        w.writeframes(pcm_bytes)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    jobs = []

    # Step 1: 生成参数组合 × TTS 调用
    idx = 0
    for speaker in SPEAKERS:
        for speed in SPEEDS:
            for vol in VOLUMES:
                idx += 1
                jobs.append((speaker, speed, vol, idx))

    print(f"共 {len(jobs)} 种参数组合，开始调用 TTS...")
    base_samples = []
    for speaker, speed, vol, n in jobs:
        label = f"[{n}/{len(jobs)}] {speaker} spd={speed} vol={vol}"
        print(f"  {label}", end=" ", flush=True)
        pcm = generate_tts(WAKE_WORD, speaker, speed, vol)
        if pcm and len(pcm) > SAMPLE_RATE * 0.5 * 2:  # >= 0.5s
            base_samples.append(pcm)
            print(f"OK ({len(pcm)//2} samples)")
        else:
            print(f"SKIP (too short or empty)")

    print(f"\n基础样本: {len(base_samples)} 条，开始增强...")

    # Step 2: 数据增强，扩到 200+ 条
    total = 0
    enhance_rounds = max(1, (200 + len(base_samples) - 1) // len(base_samples))
    for rnd in range(enhance_rounds):
        for i, pcm in enumerate(base_samples):
            enhanced = apply_enhancements(pcm, rnd * 1000 + i)
            path = os.path.join(OUTPUT_DIR, f"wake_{total+1:04d}.wav")
            save_wav(path, enhanced)
            total += 1
            if total >= 250:
                break
        if total >= 250:
            break

    print(f"\n生成完成: {total} 条 → {OUTPUT_DIR}/")
    print(f"训练平台: https://dl.espressif.com/public/wake_word_tool/")


if __name__ == "__main__":
    main()
