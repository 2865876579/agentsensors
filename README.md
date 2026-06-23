# 智能枕头云端服务

ESP32 语音助手的云端后端，负责语音识别、AI 对话、语音合成，以及转发电脑控制命令给 PC Agent。

## 架构

```
ESP32（录音/播放）<--WebSocket--> 云端服务（STT+LLM+TTS）<--WebSocket--> PC Agent（控制电脑）
```

## 技术栈

| 模块 | 方案 |
|---|---|
| 框架 | Python + FastAPI + WebSocket |
| STT | 讯飞流式语音识别 |
| LLM | DeepSeek（兼容 OpenAI 格式） |
| TTS | Edge TTS（免费） |

## 文件说明

```
server/
├── main.py           # WebSocket 主服务入口
├── config.py         # 配置（从 .env 读取）
├── stt_xunfei.py     # 讯飞语音识别模块
├── llm_deepseek.py   # DeepSeek 对话 + 结构化命令输出
├── tts_edge.py       # Edge TTS 文字转语音
├── test_client.py    # 本地文字测试客户端（不需要 ESP32）
├── .env.example      # 配置模板
└── requirements.txt  # Python 依赖
```

## 部署步骤

### 1. 安装依赖

```bash
cd server
pip install -r requirements.txt
```

### 2. 配置 API Key

```bash
cp .env.example .env
```

编辑 `.env`，填入：

| 配置项 | 来源 |
|---|---|
| XF_APP_ID / XF_API_KEY / XF_API_SECRET | [讯飞开放平台](https://www.xfyun.cn/) → 创建应用 → 语音听写（流式版） |
| DEEPSEEK_API_KEY | [DeepSeek 平台](https://platform.deepseek.com/) → API Keys |

Edge TTS 不需要 key。

### 3. 启动服务

```bash
python main.py
```

服务默认监听 `0.0.0.0:8000`。

### 4. 验证服务在线

```bash
curl http://你的服务器IP:8000/health
```

返回 `{"status": "ok", ...}` 即成功。

## 本地测试（不需要 ESP32）

```bash
# 终端 1：启动服务
python main.py

# 终端 2：运行测试客户端
python test_client.py
```

输入文字即可测试 LLM 对话 + TTS 合成，音频保存为 `reply.mp3`。

## WebSocket 协议

### ESP32 端 → 服务端

```json
{"type": "text", "text": "用户说的话"}
```

```json
{"type": "audio", "audio": "base64编码的PCM音频（16kHz/16bit/单声道）"}
```

```json
{"type": "ping"}
```

### 服务端 → ESP32 端

```json
{"type": "tts_audio", "format": "mp3", "audio": "base64音频", "text": "回复文字"}
```

```json
{"type": "stt_result", "text": "识别出的文字"}
```

```json
{"type": "status", "msg": "状态提示"}
```

### 服务端 → PC Agent

```json
{"type": "pc_command", "command": {"action": "search", "params": {"query": "搜索词"}}}
```

### PC Agent → 服务端

```json
{"type": "result", "result": "执行结果"}
```

## 防火墙

宝塔面板需要放行 8000 端口（TCP），阿里云安全组也需要放行。

## 后续

- [ ] 接入 ESP32 实际录音上传
- [ ] 编写 PC Agent 客户端
- [ ] 支持流式 TTS（边合成边播放，降低延迟）
