"""
配置文件 - 从 .env 文件读取所有 API 密钥和服务参数

使用方法：
  1. 复制 .env.example 为 .env
  2. 填入你的 API Key
  3. 其他模块 import config 即可使用

配置项说明：
  - XF_*: 讯飞开放平台的语音识别凭证
  - DEEPSEEK_*: DeepSeek LLM 的 API 配置
  - TTS_VOICE/TTS_RATE/TTS_VOLUME: Edge TTS 的音色、语速和音量
  - SERVER_*: WebSocket 服务监听地址和端口
"""
import os
from dotenv import load_dotenv

# 从项目根目录的 .env 文件加载环境变量
load_dotenv()

# ==================== 讯飞语音识别配置 ====================
# 注册地址：https://www.xfyun.cn/
# 需要开通「语音听写（流式版）」服务
XF_APP_ID = os.getenv("XF_APP_ID", "")
XF_API_KEY = os.getenv("XF_API_KEY", "")
XF_API_SECRET = os.getenv("XF_API_SECRET", "")
# 服务端收到的是已完成并截断的录音，快速发送给讯飞以降低回答延迟
XF_STT_SEND_INTERVAL_SEC = float(os.getenv("XF_STT_SEND_INTERVAL_SEC", "0.005"))
XF_VAD_EOS_MS = int(os.getenv("XF_VAD_EOS_MS", "800"))

# ==================== DeepSeek LLM 配置 ====================
# 注册地址：https://platform.deepseek.com/
# API 兼容 OpenAI 格式，用 openai 库直接调用
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

# ==================== Edge TTS 配置 ====================
# 免费，不需要 API Key
# 常用中文音色：zh-CN-XiaoxiaoNeural（女）、zh-CN-YunxiNeural（男）
# 风格：assistant, chat, gentle, narration-relaxed, empathetic, affectionate
TTS_VOICE = os.getenv("TTS_VOICE", "zh-CN-XiaoxiaoNeural")
# 语速：默认正常语速，减少语音回复前后的拖沓感
TTS_RATE = os.getenv("TTS_RATE", "+30%")
# 音量：略微提高输出，避免设备端听感偏弱
TTS_VOLUME = os.getenv("TTS_VOLUME", "+15%")
# 语调风格：gentle(温柔) / narration-relaxed(放松叙述) / empathetic(共情)
TTS_STYLE = os.getenv("TTS_STYLE", "gentle")
# 风格强度 0.0~1.0
TTS_STYLE_DEGREE = float(os.getenv("TTS_STYLE_DEGREE", "1.2"))

# ==================== 火山引擎 TTS 配置 ====================
# 豆包语音合成模型 2.0，字符版
# 接入地址：https://console.volcengine.com/speech/service/tts
# 密钥管理：控制台右上角头像 → 访问控制 → 密钥管理
VOLC_APP_ID = os.getenv("VOLC_APP_ID", "")
VOLC_API_KEY = os.getenv("VOLC_API_KEY", "")
VOLC_RESOURCE_ID = os.getenv("VOLC_RESOURCE_ID", "seed-tts-2.0")
VOLC_VOICE_TYPE = os.getenv("VOLC_VOICE_TYPE", "zh_male_xionger_uranus_bigtts")
# 语速 -500~+500（0=正常，正数=加快，负数=减慢），默认 +20 偏快
VOLC_TTS_SPEED = int(os.getenv("VOLC_TTS_SPEED", "15"))
# 音量 -500~+500（0=正常，正数=加大），默认 +10 稍大
VOLC_TTS_VOLUME = int(os.getenv("VOLC_TTS_VOLUME", "-10"))
# 可选: VOLC_TTS_ENDPOINT 自定义 API 地址

# ==================== 时区配置 ====================
# 服务端时区，用于向 LLM 注入当前时间、时区转换等时间功能
# 常用值：Asia/Shanghai（北京）、America/New_York（纽约）、Europe/London（伦敦）
TIMEZONE = os.getenv("TIMEZONE", "Asia/Shanghai")

# ==================== 位置配置 ====================
# 设备所在地，注入到 LLM 对话中，让模型知道用户在哪
# 天气、本地新闻等无需用户每次说城市名
LOCATION = os.getenv("LOCATION", "重庆")

# ==================== 邮箱配置（QQ邮箱 IMAP）====================
# 用于服务端后台拉取邮件，不需要 PC Agent
# 授权码获取：QQ邮箱网页版 → 设置 → 账户 → POP3/IMAP → 开启 → 生成
EMAIL_HOST = os.getenv("EMAIL_HOST", "imap.qq.com")
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASS = os.getenv("EMAIL_PASS", "")

# ==================== 服务配置 ====================
SERVER_HOST = os.getenv("SERVER_HOST", "0.0.0.0")  # 监听地址，0.0.0.0 表示所有网卡
SERVER_PORT = int(os.getenv("SERVER_PORT", "8000"))  # 监听端口1

# ==================== 网易云音乐配置 ====================
# 登录 Cookie 放在 .env，不要写进代码仓库。
NETEASE_COOKIE = os.getenv("NETEASE_COOKIE", "")
NETEASE_BR = int(os.getenv("NETEASE_BR", "320000"))
NETEASE_MAX_PLAY_SECONDS = int(os.getenv("NETEASE_MAX_PLAY_SECONDS", "360"))
