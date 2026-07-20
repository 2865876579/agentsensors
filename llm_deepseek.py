"""
DeepSeek 对话模块 —— 基于 Function Calling

架构：
  用户输入 → chat() → DeepSeek（带工具定义）→ 模型决策调哪个工具 → 执行 → 回传结果 → 最终回复

工具扩展方法：
  1. 在 TOOLS 列表里新增一个工具定义（name/description/parameters）
  2. 在 _dispatch_tool() 里加对应的 elif 分支处理逻辑
  3. chat() 主循环不需要动
"""
import json
import re
import time
from datetime import datetime, date, timedelta

# zoneinfo 是 Python 3.9+ 标准库，用于时区感知的时间计算
# 旧版本 Python 不可用时回退到 UTC+8 硬编码
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

from openai import AsyncOpenAI
from config import DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL, DEEPSEEK_MODEL, TIMEZONE, LOCATION
from web_search import search_web, format_search_results
from email_checker import check_emails_by_date, format_email_summary, parse_date_str
from user_settings import build_ai_context_prompt, guard_ai_action
from weather import get_weather

# ── PC 命令回调（由 main.py 注册）─────────────────────────
# 当 LLM 调 pc_command 工具时，通过此回调将命令发给 PC Agent
_pc_command_cb = None  # async def cb(action: str, params: dict) -> str

def set_pc_command_callback(cb):
    global _pc_command_cb
    _pc_command_cb = cb

# ── 枕头控制回调（由 main.py 注册）─────────────────────
_pillow_cb = None  # async def cb(action: str, level: int, duration_sec: int, client_id: str, turn_id: int) -> str

def set_pillow_callback(cb):
    global _pillow_cb
    _pillow_cb = cb

# ── 灯带控制回调（由 main.py 注册）─────────────────────
_led_cb = None  # async def cb(action: str, mode: str, color: str, brightness_pct, speed_pct, duration_sec, client_id: str, turn_id: int) -> str

def set_led_callback(cb):
    global _led_cb
    _led_cb = cb

# ── 红外设备控制回调（由 main.py 注册）──────────────────
_ir_device_cb = None  # async def cb(device: str, action: str, client_id: str, turn_id: int) -> str

def set_ir_device_callback(cb):
    global _ir_device_cb
    _ir_device_cb = cb

# ── 传感器读取回调（由 main.py 注册）──────────────────
_read_sensors_cb = None  # async def cb(client_id: str, turn_id: int) -> str

def set_read_sensors_callback(cb):
    global _read_sensors_cb
    _read_sensors_cb = cb

# ── 滴滴 MCP 基础版打车链接回调（由 main.py 注册）────────────────────────
_didi_ride_link_cb = None  # async def cb(from_place: str, to_place: str, city: str, product_category: str, client_id: str, turn_id: int) -> str

def set_didi_ride_link_callback(cb):
    global _didi_ride_link_cb
    _didi_ride_link_cb = cb

client = AsyncOpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)

# ── 系统提示词 ──────────────────────────────────────────────
SYSTEM_PROMPT = """你是"小安"，一个放在用户枕边的语音伴侣。用户通过语音和你聊天，不是在读屏幕。

能力边界：云端已经接入智能闹钟。用户要设置、取消、查询闹钟时，先按现有闹钟流程处理；不要声称“没有闹钟功能”。闹钟支持绝对时间、几秒/几分钟/几小时后，以及到点音乐和枕头唤醒联动。
云端也已经接入网络音乐直接播放。不要声称“没有直接放歌功能”，也不要让用户改用手机或浏览器；音乐请求由专用音乐链路处理。

性格：
- 像一个见多识广但不高冷的朋友——有自己的观点，但不咄咄逼人
- 好奇心强，会追问、会反问，把对话往下挖而不是停在表面
- 幽默感来源于观察力，不靠玩梗和表情包
- 用户说"睡不着"时，你不会立刻切换成机器人安抚模式，而是先问一句"怎么了，是心里有事还是单纯不困"
- 偶尔带点慵懒的语气——毕竟你是枕边的人，不用时时刻刻端着
- 允许短暂的沉默和停顿，不用每句话都塞满

说话方式：
- 默认 2-4 句，有话则长无话则短
- 不确定的事老实说不知道，不编
- 需要实时信息时主动用 web_search
- ★ 禁止输出括号里的动作、表情、舞台指示——例如"（笑）""（叹气）""（慵懒地）"。所有文字都会被 TTS 逐字朗读，只能输出要说出口的话
    - ★ 根据当前时间调整语气：21点前不说"晚安""好梦"等睡前用语；早晨、下午、晚上用对应语气

深度对话：
- 用户如果聊到人生意义、哲学、选择、困惑、死亡、自由、孤独等话题——不要绕开，认真接住
- 可以引用你读过的书、知道的思想，但用自己的话说，不要背百科
- 允许没有答案。有时候陪用户一起困惑，比给答案更有用
- 这种对话可以长，不用总想着"该睡了"——除非用户自己说困了
- 聊完深的后如果气氛沉了，轻轻带回来就好，不用硬转话题

能力：
- ★ 你自带联网搜索，知道当前实时信息。一般问题直接用内置搜索回答，不要频繁调 web_search 工具。web_search 工具只用于需要精确结构化数据的场景（金价、今日新闻等）
- ★ 用户问天气、温度、会不会下雨、要不要带伞时，必须调用 get_weather 工具，不要靠自身知识猜测
- ★ 用户要桌面写文件、剪贴板操作等，必须调 pc_command 执行，不准光嘴上答应
- ★ 你可以调用 read_sensors 工具查看枕头传感器状态（压力分布/温湿度/光照/空气质量）。用户问"枕头现在怎样""温度多少"或想了解睡姿压力时使用
- ★ 你可以调用 led_control 工具控制灯带。用户提到灯、灯带、灯光、开关灯、闪烁、呼吸、渐变、调亮、调暗、换颜色、助眠氛围时必须调用 led_control，不准说没有接入灯控。
- ★ 你可以调用 ir_device_control 工具通过红外控制风扇、加湿器和空调。用户提到打开/关闭/切换风扇、加湿器或空调时必须调用该工具，不准只口头答应。
- ★ 用户要打车、叫车、去某地、帮我叫滴滴时，必须调用 didi_ride_link 工具生成滴滴 App/小程序链接；这是基础版，不会直接下单，必须提醒用户在手机上打开链接，自行确认车型、下单和支付。
- ★ 当收到"用户刚刚躺下了，请温柔地主动问候一句"这条系统消息时，表示用户刚躺下。说一句温柔问候或简短有力量的哲理话，帮助用户卸下一天的疲惫；不要提问、不要催促回复，也不要提传感器或系统
- 查到的结果用最简洁的方式播报，控制在 80 字以内
"""


# ── 时间工具辅助函数 ────────────────────────────────────────

def _get_now() -> datetime:
    """
    获取当前时区的 datetime 对象。
    优先用 zoneinfo（Python 3.9+），不可用时回退到 UTC+8 近似。
    """
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo(TIMEZONE))
    # zoneinfo 不可用时用 UTC+8 近似（离线误差 < 1 秒，语音场景可忽略）
    return datetime.utcnow() + timedelta(hours=8)


def _get_time_string() -> str:
    """
    构建注入 System Prompt 的当前时间字符串。
    让 LLM 在回答"几点了""今天几号"等简单问题时无需调用工具，
    直接从 system prompt 获取信息，零额外 API 延迟。
    """
    now = _get_now()
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    wd = weekday_names[now.weekday()]
    tz_display = "北京时间" if "Shanghai" in TIMEZONE else TIMEZONE
    return (
        f"{now.year}年{now.month}月{now.day}日 {wd} "
        f"{now.hour:02d}:{now.minute:02d}:{now.second:02d} ({tz_display})"
    )


def _handle_get_current_time(action: str, target: str = "", timezone_str: str = "") -> str:
    """
    执行 get_current_time 工具调用的各种操作，返回自然语言结果给 LLM。

    action:
        "now"       — 返回当前详细时间（含星期、年内第几天）
        "countdown" — 倒数日计算，距 target 还有 / 已过多少天
        "weekday"   — 查询 target 是周几
        "convert"   — 将当前时间转换到 timezone_str 时区

    target / timezone_str 按 action 类型选填，详见工具定义的 parameters。
    """
    now = _get_now()
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

    # ── 当前详细时间 ──
    if action == "now":
        return (
            f"当前详细时间：{_get_time_string()}，"
            f"年内第{now.timetuple().tm_yday}天。"
        )

    # ── 倒数日计算 ──
    if action == "countdown":
        if not target:
            return '请提供目标日期，例如"2027-01-01"或"2027年1月1日"。'
        try:
            # 兼容多种日期书写习惯：2027-01-01 / 2027年1月1日 / 2027/1/1
            target = (
                target.replace("年", "-").replace("月", "-")
                .replace("日", "").replace("/", "-").strip()
            )
            target_date = datetime.strptime(target, "%Y-%m-%d").date()
            today = now.date()
            delta = (target_date - today).days
            if delta > 0:
                return (
                    f"距离{target_date.year}年{target_date.month}月"
                    f"{target_date.day}日还有{delta}天。"
                )
            elif delta == 0:
                return f"{target_date.year}年{target_date.month}月{target_date.day}日就是今天。"
            else:
                return (
                    f"{target_date.year}年{target_date.month}月"
                    f"{target_date.day}日已经过去{abs(delta)}天了。"
                )
        except ValueError:
            return f'无法解析日期"{target}"，请使用 YYYY-MM-DD 或 YYYY年MM月DD日 格式。'

    # ── 查询某天是周几 ──
    if action == "weekday":
        if not target:
            return '请提供查询日期，例如"2027-01-01"。'
        try:
            target = (
                target.replace("年", "-").replace("月", "-")
                .replace("日", "").replace("/", "-").strip()
            )
            target_date = datetime.strptime(target, "%Y-%m-%d").date()
            wd = weekday_names[target_date.weekday()]
            return f"{target_date.year}年{target_date.month}月{target_date.day}日是{wd}。"
        except ValueError:
            return f'无法解析日期"{target}"。'

    # ── 时区转换 ──
    if action == "convert":
        if not timezone_str:
            return (
                "请提供目标时区，例如 America/New_York（纽约）"
                "或 Europe/London（伦敦）。"
            )
        if ZoneInfo is None:
            return "时区转换功能需要 Python 3.9+ 的 zoneinfo 模块，当前环境不支持。"
        try:
            target_tz = ZoneInfo(timezone_str)
            target_time = datetime.now(target_tz)
            wd = weekday_names[target_time.weekday()]
            return (
                f"{timezone_str} 当前时间：{target_time.year}年"
                f"{target_time.month}月{target_time.day}日 {wd} "
                f"{target_time.hour:02d}:{target_time.minute:02d}。"
            )
        except Exception:
            return (
                f'无法识别的时区"{timezone_str}"，请使用标准 IANA 时区名称，'
                f'如 America/New_York、Europe/London、Asia/Tokyo。'
            )

    return f"未知操作：{action}"


# ── 工具定义 ────────────────────────────────────────────────
# 每个工具对应一个真实能力，description 要让模型能准确判断何时调用
async def generate_automation_reply(
    prompt: str,
    max_tokens: int = 192,
    fallback: str = "我在，先陪你一会儿。",
) -> str:
    system = (
        "你是小安，一个低打扰的枕边生活助手。"
        f"\n当前时间：{_get_time_string()}"
        f"\n用户所在地：{LOCATION}"
        f"\n{build_ai_context_prompt()}"
        "\n\n任务：为设备自动化生成一句很短的中文语音播报。"
        "\n要求：直接输出最终要说的话；自然、克制、像真实助手；不要提系统、传感器、阈值、ppm、lux、自动化；"
        "不要括号，不要项目符号，不要解释。长度 10 到 28 个汉字。"
    )
    try:
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            max_tokens=max(max_tokens, 512),
            temperature=0.8,
        )
        choice = response.choices[0]
        message = choice.message
        text = ((message.content or "").strip())
        if text:
            return text
        reasoning = str(getattr(message, "reasoning_content", "") or "")
        print(
            "[LLM] generate_automation_reply empty content: "
            f"finish_reason={getattr(choice, 'finish_reason', '')} "
            f"reasoning={reasoning[:160]!r}"
        )
        return fallback
    except Exception as exc:
        print(f"[LLM] generate_automation_reply error: {exc}")
        return fallback


async def generate_sleep_greeting(recent_greetings: list[str] | None = None) -> str:
    """Generate a fresh, non-repetitive greeting for a new pillow arrival."""
    recent = [str(item).strip() for item in (recent_greetings or []) if str(item).strip()][-5:]
    recent_text = "\n".join(f"- {item}" for item in recent) or "- 无"
    system = (
        "你是小安，一位有审美、有分寸的枕边语音伴侣。"
        f"当前时间：{_get_time_string()}。用户所在地：{LOCATION}。"
        f"\n{build_ai_context_prompt()}"
        "\n用户刚刚躺下。请现场创作一句每次都不同的中文主动问候。"
        "要求：30到65个汉字，一到两句；有具体画面或新鲜隐喻，但不过度抒情；"
        "温柔、有一点智慧，像真正了解生活的人，而不是客服或鸡汤文案；"
        "不要提问，不要求用户回复，不提传感器、系统、模式；"
        "避免使用‘你躺好了’‘今晚我会守着你’‘今天辛苦了’‘好好休息’等固定套话；"
        "只输出最终要说的话，不加引号、标题或解释。"
    )
    user = f"最近已经说过这些句子，请避开相同意象、句式和措辞：\n{recent_text}"
    try:
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            max_tokens=192,
            temperature=1.05,
        )
        text = re.sub(r"\s+", " ", (response.choices[0].message.content or "").strip())
        text = text.strip('“”\"')
        if 18 <= len(text) <= 100 and text not in recent:
            return text
    except Exception as exc:
        print(f"[LLM] generate_sleep_greeting error: {exc}")
    return ""


async def classify_environment_adjustment_reply(user_text: str, proposal: str) -> str:
    def parse_intent(text: str) -> str | None:
        raw = str(text or "").strip()
        if not raw:
            return None
        upper = raw.upper()
        labels = re.findall(r"\b(APPROVE|DECLINE|UNKNOWN)\b", upper)
        if labels and len(set(labels)) == 1:
            return labels[0].lower()

        normalized = raw.replace(" ", "").replace("，", "").replace("。", "")
        negative_words = (
            "不用", "不需要", "不要", "别", "先不", "不用了", "算了",
            "不用管", "不用处理", "不用动", "别动", "不可以", "不必", "拒绝",
        )
        positive_replies = {
            "需要", "可以", "好", "好的", "行", "没问题", "同意", "要的", "麻烦了",
            "帮我调整", "帮我处理", "那就调整", "那就处理", "执行吧",
        }
        positive_phrases = (
            "帮我打开", "帮我开启", "帮我关掉", "帮我关闭", "帮我调暗",
            "需要调整", "需要处理", "可以调整", "可以处理",
        )

        if any(word in normalized for word in negative_words):
            return "decline"
        if normalized in positive_replies or any(word in normalized for word in positive_phrases):
            return "approve"
        return None

    system = (
        "你是一个严格的三分类器。"
        f"助手此前向用户提议：{proposal}。"
        "判断用户当前回复是否明确同意这项环境调整。"
        "APPROVE=明确同意或要求执行；DECLINE=明确拒绝；UNKNOWN=无关、含糊或无法判断。"
        "只能输出 APPROVE、DECLINE、UNKNOWN 三者之一。"
    )
    try:
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_text},
            ],
            max_tokens=128,
            temperature=0,
        )
        choice = response.choices[0]
        message = choice.message
        text = ((message.content or "").strip())
        intent = parse_intent(text)
        if intent:
            return intent
        fallback_intent = parse_intent(user_text)
        if fallback_intent:
            print(
                "[LLM] classify_environment_adjustment_reply fallback from user text: "
                f"finish_reason={getattr(choice, 'finish_reason', '')} "
                f"content={text[:80]!r}"
            )
            return fallback_intent
        reasoning = str(getattr(message, "reasoning_content", "") or "")
        intent = parse_intent(reasoning)
        if intent:
            print(
                "[LLM] classify_environment_adjustment_reply fallback from reasoning: "
                f"finish_reason={getattr(choice, 'finish_reason', '')} "
                f"reasoning={reasoning[:80]!r}"
            )
            return intent
    except Exception as exc:
        print(f"[LLM] classify_environment_adjustment_reply error: {exc}")
        fallback_intent = parse_intent(user_text)
        if fallback_intent:
            return fallback_intent
    return "unknown"


async def classify_music_request(user_text: str) -> dict:
    """Use the LLM to decide whether the user is asking for music/audio playback."""
    fallback = {"action": "none", "query": "", "reason": "not_music"}
    raw = str(user_text or "").strip()
    try:
        from netease_music import extract_music_query, parse_music_query

        fast_query = extract_music_query(raw)
    except Exception as exc:
        print(f"[Music] fast intent error: {exc}")
        fast_query = None

    if fast_query == "__stop_music__":
        return {
            "action": "stop", "query": "", "title": "", "artist": "",
            "kind": "unknown", "reason": "fast_stop",
        }
    if fast_query == "__random_music__":
        return {
            "action": "play", "query": "华语流行歌曲", "title": "", "artist": "",
            "kind": "random", "selection": "random", "reason": "fast_random",
        }
    if fast_query:
        query, title, artist, kind = parse_music_query(fast_query)
        return {
            "action": "play", "query": query, "title": title, "artist": artist,
            "kind": kind, "reason": "fast_play",
        }

    compact = re.sub(r"\s+", "", raw)
    play_words = ("想听", "要听", "听一首", "听", "放一首", "播放", "放点", "来点")
    scene_queries = (
        (("dj",), "DJ 舞曲"),
        (("安静", "舒缓", "轻松", "温柔", "治愈", "不吵", "平静"), "轻音乐 舒缓"),
        (("摇滚",), "摇滚歌曲"),
        (("古风",), "古风歌曲"),
        (("爵士",), "爵士音乐"),
        (("民谣",), "民谣歌曲"),
        (("电子",), "电子音乐"),
        (("纯音乐",), "纯音乐"),
        (("轻音乐",), "轻音乐"),
        (("流行",), "流行歌曲"),
    )
    normalized_compact = compact.lower()
    scene_query = next(
        (query for words, query in scene_queries if any(word in normalized_compact for word in words)),
        "",
    )
    if scene_query and any(word in normalized_compact for word in play_words):
        return {
            "action": "play", "query": scene_query, "title": "", "artist": "",
            "kind": "noise", "selection": "specific", "reason": "fast_scene",
        }
    music_hints = (
        "歌", "歌曲", "音乐", "曲子", "白噪声", "白噪音", "雨声", "助眠音",
        "播放", "暂停播放", "停止播放", "下一首", "换一首", "dj", "DJ", "安静", "舒缓",
        "轻松", "温柔", "治愈", "不吵", "平静", "安眠",
    )
    if not any(hint in compact for hint in music_hints):
        return fallback

    system = (
        "你是一个严格的语义分类器，用来判断用户是否要播放或停止网络音乐/白噪声。"
        "只能输出 JSON，不要解释，不要 Markdown。"
        "\n\n输出格式："
        '{"action":"play|stop|none","selection":"random|specific|noise|none","query":"用于搜索的关键词","title":"歌名或白噪声名称，可空","artist":"歌手名，可空","kind":"song|artist|noise|random|unknown","reason":"很短原因"}'
        "\n\n判定规则："
        "\n- 用户想播放、找歌、听某首歌、听某个歌手、来点白噪声/雨声/助眠音乐/网络歌曲 => action=play。"
        "\n- 用户想播放、找歌、听某首歌、听某个歌手、来点白噪声/雨声/助眠音乐/网络歌曲 => action=play。"
        "\n- 用户想停止、暂停、关掉音乐、别放了 => action=stop。"
        "\n- 只是问你会不会唱歌、讨论歌曲好不好听、聊天提到歌名但没有播放意图 => action=none。"
        "\n- kind=song 表示明确要某首歌；kind=artist 表示只指定歌手但没指定歌名；kind=noise 表示雨声/白噪声/助眠音乐。"
        "\n- selection=random 表示没有具体歌曲、歌手或风格，只想随机来一首；此时 query 固定输出'华语流行歌曲'。"
        "\n- kind=song 表示明确要某首歌；kind=artist 表示只指定歌手但没指定歌名；kind=noise 表示雨声/白噪声/助眠音乐。"
        "\n- play 时 query 要适合音乐搜索：保留歌名、歌手、版本；去掉'帮我/放首/来点/播放/听一下'等口语。"
        "\n- title 填明确歌名或音频名称；artist 只在用户明确说出歌手时填写。"
        "\n- 如果用户说'周杰伦的青花瓷'，query 输出'周杰伦 青花瓷'，title='青花瓷'，artist='周杰伦'。"
        "\n- 如果用户说'天青色等烟雨那首'，query 输出'青花瓷 周杰伦'，title='青花瓷'，artist='周杰伦'。"
        "\n- 如果用户说'播放周杰伦的歌'，query 输出'周杰伦'，title=''，artist='周杰伦'，kind='artist'。"
        "\n- 如果用户说'随便来首周杰伦'，query 输出'周杰伦'，title=''，artist='周杰伦'，kind='artist'。"
        "\n- 如果用户说'雨声助眠'，query 输出'雨声 白噪音 助眠'，title='雨声助眠'，artist=''。"
        "\n- 如果用户说'雨声助眠'，selection=noise，query 输出'雨声 白噪音 助眠'，title='雨声助眠'，artist=''。"
        "\n- 如果用户说'放一首适合睡觉的歌'，selection=noise，query 输出'助眠音乐 轻音乐'，kind='noise'。"
        "\n- 如果用户说'来点摇滚'或'放点流行音乐'，selection=specific，query 输出对应曲风，不要保留'来点/放点/音乐'等口语。"
        "\n- 如果用户说'放一首安静/舒缓/轻松/温柔/治愈的歌'，selection=noise，query 输出'轻音乐 舒缓'，kind='noise'。"
        "\n- 如果用户说'想听一首比较安静的歌'，必须识别为 action=play、selection=noise、kind='noise'，不能把'比较安静'当作歌名。"
    )
    try:
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": str(user_text or "").strip()},
            ],
            max_tokens=192,
            temperature=0,
        )
        content = (response.choices[0].message.content or "").strip()
        if content.startswith("```"):
            content = content.strip("`")
            if content.lower().startswith("json"):
                content = content[4:].strip()
        try:
            obj = json.loads(content)
        except json.JSONDecodeError:
            start = content.find("{")
            end = content.rfind("}")
            if start >= 0 and end > start:
                obj = json.loads(content[start:end + 1])
            else:
                raise
        action = str(obj.get("action") or "none").strip().lower()
        if action not in {"play", "stop", "none"}:
            action = "none"
        query = str(obj.get("query") or "").strip()
        if action == "play" and not query:
            action = "none"
        result = {
            "action": action,
            "query": query,
            "title": str(obj.get("title") or "").strip(),
            "artist": str(obj.get("artist") or "").strip(),
            "kind": str(obj.get("kind") or "unknown").strip().lower(),
            "selection": str(obj.get("selection") or "").strip().lower(),
            "reason": str(obj.get("reason") or "").strip()[:80],
        }
        print(f"[LLM] music_intent {result}")
        return result
    except Exception as exc:
        print(f"[LLM] classify_music_request error: {exc}")
        return fallback


async def classify_emotional_need(user_text: str) -> bool:
    """Return whether the user is expressing a momentary need for comfort."""
    system = (
        "你是情绪场景分类器。判断用户是否正在表达疲惫、低落、烦躁、压力大、"
        "孤独或想被陪伴，而不是在客观描述别人或讨论抽象概念。"
        "只能输出 JSON：{\"comfort\":true|false}。"
    )
    try:
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": str(user_text or "").strip()},
            ],
            max_tokens=64,
            temperature=0,
        )
        content = (response.choices[0].message.content or "").strip()
        start = content.find("{")
        end = content.rfind("}")
        if start >= 0 and end > start:
            result = json.loads(content[start:end + 1])
            return bool(result.get("comfort"))
    except Exception as exc:
        print(f"[LLM] classify_emotional_need error: {exc}")
    return False


async def classify_comfort_reply(user_text: str) -> str:
    """Classify a reply to the one-shot comfort-music proposal."""
    system = (
        "助手刚刚询问是否播放一段合适的音乐陪伴用户。"
        "判断用户当前回复：PLAY=同意播放、想听音乐或直接提出音乐要求；"
        "DECLINE=明确拒绝或表示暂时不需要；OTHER=与该询问无关或无法判断。"
        "只能输出 JSON：{\"action\":\"play|decline|other\"}。"
    )
    try:
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": str(user_text or "").strip()},
            ],
            max_tokens=32,
            temperature=0,
        )
        content = (response.choices[0].message.content or "").strip()
        start = content.find("{")
        end = content.rfind("}")
        if start >= 0 and end > start:
            action = str(json.loads(content[start:end + 1]).get("action") or "other").lower()
            if action in {"play", "decline", "other"}:
                return action
    except Exception as exc:
        print(f"[LLM] classify_comfort_reply error: {exc}")
    return "other"


async def classify_music_stop_request(user_text: str) -> bool:
    """Use semantic intent, not a fixed phrase list, for music barge-in."""
    system = (
        "你是音乐播放控制意图分类器。判断用户此刻是否明确要求停止、暂停、"
        "关闭或不要继续当前正在播放的音乐。只能输出 JSON，不要解释。"
        '\n输出格式：{"stop":true|false,"reason":"很短原因"}'
        "\n表达方式可以很口语、含否定或省略，不要求出现固定关键词。"
        "\n如果用户只是在聊天、评价歌曲、要求换歌、调音量、询问歌名，stop=false。"
        "\n只有用户希望当前音乐停止或暂停时，stop=true。"
    )
    try:
        started = time.monotonic()
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": str(user_text or "").strip()},
            ],
            max_tokens=96,
            temperature=0,
        )
        message = response.choices[0].message
        content = (message.content or "").strip()
        if not content:
            content = str(getattr(message, "reasoning_content", "") or "").strip()
        if content.startswith("```"):
            content = content.strip("`")
            if content.lower().startswith("json"):
                content = content[4:].strip()
        try:
            result = json.loads(content)
        except json.JSONDecodeError:
            start = content.find("{")
            end = content.rfind("}")
            if start < 0 or end <= start:
                raise
            result = json.loads(content[start:end + 1])
        stop = bool(result.get("stop"))
        print(
            f"[LLM] music_barge stop={stop} "
            f"latency={time.monotonic() - started:.3f}s text={user_text!r}"
        )
        return stop
    except Exception as exc:
        print(f"[LLM] classify_music_stop_request error: {exc}")
        return False


_CN_DIGITS = {
    "零": 0, "〇": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4,
    "五": 5, "六": 6, "七": 7, "八": 8, "九": 9,
}


def _parse_cn_number(text: str) -> int | None:
    text = str(text or "").strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    if text == "半":
        return 30
    if text in _CN_DIGITS:
        return _CN_DIGITS[text]
    if "百" in text:
        left, _, right = text.partition("百")
        high = _parse_cn_number(left) if left else 1
        low = _parse_cn_number(right) if right else 0
        if high is None or low is None:
            return None
        return high * 100 + low
    if "十" in text:
        left, _, right = text.partition("十")
        high = _parse_cn_number(left) if left else 1
        low = _parse_cn_number(right) if right else 0
        if high is None or low is None:
            return None
        return high * 10 + low
    total = 0
    for ch in text:
        if ch not in _CN_DIGITS:
            return None
        total = total * 10 + _CN_DIGITS[ch]
    return total


def _extract_alarm_song(text: str) -> str:
    raw = str(text or "").strip()
    patterns = [
        r"用(.{1,40}?)(?:唤醒|叫醒|叫我|喊醒|起床)",
        r"(?:播放|放|播)(.{1,40}?)(?:唤醒|叫醒|叫我|喊醒|起床)",
        r"(?:闹钟|叫醒|唤醒|喊醒|起床).{0,12}用(.{1,40})$",
        r"用(.{1,40})$",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw)
        if match:
            song = match.group(1)
            song = re.sub(r"^(?:一首|首|歌曲|歌|音乐|《)", "", song)
            song = re.sub(r"(?:这首歌|这首|歌曲|歌|音乐|》)$", "", song)
            return song.strip("《》“”\"' ，。！？,.!?")
    return ""


def _fast_alarm_intent(user_text: str) -> dict | None:
    raw = str(user_text or "").strip()
    compact = re.sub(r"\s+", "", raw)
    if not compact:
        return None

    if re.search(r"(取消|关闭|关掉|删掉|不要|停掉).{0,8}(闹钟|提醒|定时)", compact):
        return {
            "action": "cancel",
            "relative_seconds": 0,
            "relative_minutes": 0,
            "time": "",
            "song_query": "",
            "repeat": "once",
            "reason": "fast_cancel_alarm",
        }

    if not re.search(r"(闹钟|叫醒|唤醒|喊醒|叫我起床|叫我|提醒|定时|计时器|倒计时|设个|设置个|定个)", compact):
        return None

    repeat = "once"
    if "每天" in compact or "每日" in compact:
        repeat = "daily"
    elif "工作日" in compact:
        repeat = "workday"
    elif "周末" in compact:
        repeat = "weekend"

    song_query = _extract_alarm_song(raw)

    relative_seconds = 0
    seconds_match = re.search(
        r"([0-9零〇一二两三四五六七八九十百]+)(?:个)?(?:秒|秒钟)(?:后|之后|以后|的?(?:闹钟|提醒|计时器))",
        compact,
    )
    if seconds_match:
        number = _parse_cn_number(seconds_match.group(1))
        if number is not None:
            relative_seconds = number

    relative_minutes = 0
    if relative_seconds <= 0 and "半小时" in compact:
        relative_minutes = 30
    elif relative_seconds <= 0:
        match = re.search(r"([0-9零〇一二两三四五六七八九十百]+)(?:个)?(分钟|分|小时|钟头)(?:后|之后|以后|的?(?:闹钟|提醒|计时器))", compact)
        if match:
            number = _parse_cn_number(match.group(1))
            if number is not None:
                relative_minutes = number * (60 if match.group(2) in {"小时", "钟头"} else 1)

    if relative_seconds > 0 or relative_minutes > 0:
        return {
            "action": "set",
            "relative_seconds": (
                max(1, min(24 * 60 * 60, relative_seconds))
                if relative_seconds > 0 else 0
            ),
            "relative_minutes": (
                max(1, min(24 * 60, relative_minutes))
                if relative_minutes > 0 else 0
            ),
            "time": "",
            "song_query": song_query,
            "repeat": repeat,
            "reason": "fast_relative_alarm",
        }

    period = ""
    match = re.search(
        r"(明天|明早|早上|上午|中午|下午|晚上|夜里|今晚)?([0-9零〇一二两三四五六七八九十]{1,3})点(半|[0-9零〇一二两三四五六七八九十]{1,3}分?)?",
        compact,
    )
    if match:
        period = match.group(1) or ""
        hour = _parse_cn_number(match.group(2))
        minute_text = (match.group(3) or "").rstrip("分")
        minute = 30 if minute_text == "半" else (_parse_cn_number(minute_text) if minute_text else 0)
        if hour is not None and minute is not None:
            if period in {"下午", "晚上", "夜里", "今晚"} and hour < 12:
                hour += 12
            elif period == "中午" and hour < 11:
                hour += 12
            hour %= 24
            return {
                "action": "set",
                "relative_seconds": 0,
                "relative_minutes": 0,
                "time": f"{hour:02d}:{minute:02d}",
                "song_query": song_query,
                "repeat": repeat,
                "reason": "fast_absolute_alarm",
            }

    return None


async def classify_alarm_request(user_text: str) -> dict:
    """Use the LLM to decide whether the user wants to set/cancel a wake alarm."""
    fast = _fast_alarm_intent(user_text)
    if fast:
        print(f"[LLM] alarm_intent fast {fast}")
        return fast

    raw = re.sub(r"\s+", "", str(user_text or ""))
    alarm_hints = (
        "闹钟", "叫醒", "叫我", "唤醒我", "提醒我", "定时", "计时器", "倒计时",
        "设个", "设置个", "定个", "取消提醒", "关闭提醒", "分钟后", "小时后",
    )
    if not any(hint in raw for hint in alarm_hints):
        return {
            "action": "none",
            "relative_seconds": 0,
            "relative_minutes": 0,
            "time": "",
            "song_query": "",
            "repeat": "once",
            "reason": "not_alarm",
        }

    system = (
        "你是一个严格的语义分类器，用来判断用户是否要设置或取消智能枕头闹钟。"
        "只能输出 JSON，不要解释，不要 Markdown。"
        f"\n当前时间：{_get_time_string()}，时区：{TIMEZONE}"
        "\n\n输出格式："
        '{"action":"set|cancel|none","relative_seconds":0,"relative_minutes":0,"time":"HH:MM或空","song_query":"唤醒歌曲或空","repeat":"once|daily|workday|weekend","reason":"很短原因"}'
        "\n\n判定规则："
        "\n- 用户说定闹钟、设置闹钟、几分钟/几小时后叫我、几点叫醒我、用某首歌唤醒 => action=set。"
        "\n- 用户说取消闹钟、关闭闹钟、别叫我了 => action=cancel。"
        "\n- 只是讨论闹钟功能、问怎么设置、问当前闹钟，不执行设置 => action=none。"
        "\n- 相对秒数如'30秒后'必须填 relative_seconds；相对分钟/小时填 relative_minutes；time 留空。"
        "\n- 绝对时间如'早上七点半'填 time='07:30'；晚上七点填 '19:00'；没有明确日期时 repeat 默认 once。"
        "\n- 用户说'每天/工作日/周末'时 repeat 对应 daily/workday/weekend，否则 repeat=once。"
        "\n- song_query 只填用于网易云搜索的歌曲关键词，去掉'用/唤醒/叫醒/闹钟'等口语；例如'用海屿你唤醒' => '海屿你'。"
        "\n- 如果用户没有说歌曲，song_query 为空。"
    )
    fallback = {
        "action": "none",
        "relative_seconds": 0,
        "relative_minutes": 0,
        "time": "",
        "song_query": "",
        "repeat": "once",
        "reason": "not_alarm",
    }
    try:
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": str(user_text or "").strip()},
            ],
            max_tokens=192,
            temperature=0,
        )
        content = (response.choices[0].message.content or "").strip()
        if content.startswith("```"):
            content = content.strip("`")
            if content.lower().startswith("json"):
                content = content[4:].strip()
        try:
            obj = json.loads(content)
        except json.JSONDecodeError:
            start = content.find("{")
            end = content.rfind("}")
            if start >= 0 and end > start:
                obj = json.loads(content[start:end + 1])
            else:
                raise

        action = str(obj.get("action") or "none").strip().lower()
        if action not in {"set", "cancel", "none"}:
            action = "none"
        repeat = str(obj.get("repeat") or "once").strip().lower()
        if repeat not in {"once", "daily", "workday", "weekend"}:
            repeat = "once"
        try:
            relative_seconds = int(float(obj.get("relative_seconds") or 0))
        except (TypeError, ValueError):
            relative_seconds = 0
        try:
            relative_minutes = int(float(obj.get("relative_minutes") or 0))
        except (TypeError, ValueError):
            relative_minutes = 0
        result = {
            "action": action,
            "relative_seconds": max(0, min(24 * 60 * 60, relative_seconds)),
            "relative_minutes": max(0, min(24 * 60, relative_minutes)),
            "time": str(obj.get("time") or "").strip()[:5],
            "song_query": str(obj.get("song_query") or "").strip()[:80],
            "repeat": repeat,
            "reason": str(obj.get("reason") or "").strip()[:80],
        }
        if (
            action == "set"
            and not result["relative_seconds"]
            and not result["relative_minutes"]
            and not result["time"]
        ):
            result["action"] = "none"
        print(f"[LLM] alarm_intent {result}")
        return result
    except Exception as exc:
        print(f"[LLM] classify_alarm_request error: {exc}")
        return fallback


async def classify_didi_ride_request(user_text: str) -> dict:
    """Use the LLM to decide whether the user wants a DiDi ride link."""
    system = (
        "你是一个严格的语义分类器，用来判断用户是否想叫车/打车/生成滴滴出行链接。"
        "只能输出 JSON，不要解释，不要 Markdown。"
        "\n\n输出格式："
        '{"action":"ride|none","from_place":"上车点或空","to_place":"目的地或空","city":"城市或空","product_category":"车型或空","reason":"很短原因"}'
        "\n\n判定规则："
        "\n- 用户明确表示想打车、叫车、叫滴滴、去某个地点并希望安排车辆 => action=ride。"
        "\n- 用户只是讨论交通、问路线、问距离、问某地在哪里，但没有让你生成打车链接/叫车 => action=none。"
        "\n- from_place 填上车点，例如'从A到B'里的 A；如果用户没说上车点，留空。"
        "\n- to_place 填目的地，例如'到B/去B/前往B'里的 B；没有明确目的地则 action=none。"
        "\n- city 只在用户明确提到城市时填写，例如'重庆市'；没说则留空。"
        "\n- product_category 只有用户明确说快车、专车、出租车等车型时填写，否则留空。"
        "\n- 不要因为句子里有'车'就判断为 ride，必须是出行叫车语义。"
    )
    fallback = {
        "action": "none",
        "from_place": "",
        "to_place": "",
        "city": "",
        "product_category": "",
        "reason": "not_ride",
    }
    try:
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": str(user_text or "").strip()},
            ],
            max_tokens=256,
            temperature=0,
        )
        content = (response.choices[0].message.content or "").strip()
        if content.startswith("```"):
            content = content.strip("`")
            if content.lower().startswith("json"):
                content = content[4:].strip()
        try:
            obj = json.loads(content)
        except json.JSONDecodeError:
            start = content.find("{")
            end = content.rfind("}")
            if start >= 0 and end > start:
                obj = json.loads(content[start:end + 1])
            else:
                raise

        action = str(obj.get("action") or "none").strip().lower()
        if action not in {"ride", "none"}:
            action = "none"
        result = {
            "action": action,
            "from_place": str(obj.get("from_place") or "").strip()[:120],
            "to_place": str(obj.get("to_place") or "").strip()[:120],
            "city": str(obj.get("city") or "").strip()[:40],
            "product_category": str(obj.get("product_category") or "").strip()[:40],
            "reason": str(obj.get("reason") or "").strip()[:80],
        }
        if action == "ride" and not result["to_place"]:
            result["action"] = "none"
        print(f"[LLM] didi_ride_intent {result}")
        return result
    except Exception as exc:
        print(f"[LLM] classify_didi_ride_request error: {exc}")
        return fallback


TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "联网搜索实时信息。适用于天气/新闻/金价等，不确定的信息必须搜。",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "搜索关键词"}
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_current_time",
            "description": (
                "获取时间相关信息。适用场景："
                "1) 倒数日计算——离某个日期还有多少天；"
                "2) 时区转换——某地现在几点；"
                "3) 查询某天是周几；"
                "4) 需要精确时间计算的复杂问题。"
                '注意：简单的"现在几点""今天几号"无需调用此工具，'
                "当前时间已在对话开头提供。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["now", "countdown", "weekday", "convert"],
                        "description": (
                            "now=当前详细时间（含星期、年内天数）；"
                            "countdown=倒数日，距target还有/已过多少天；"
                            "weekday=查询target是周几；"
                            "convert=将当前时间转换到timezone时区"
                        ),
                    },
                    "target": {
                        "type": "string",
                        "description": (
                            "目标日期。countdown/weekday 需要此参数。"
                            "格式如'2027-01-01'或'2027年1月1日'"
                        ),
                    },
                    "timezone": {
                        "type": "string",
                        "description": (
                            "目标时区（IANA名称）。convert需要此参数。"
                            "如'America/New_York'、'Europe/London'"
                        ),
                    },
                },
                "required": ["action"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": (
                "查询指定城市的实时天气和3日预报。"
                "用户问天气、温度、会不会下雨、要不要带伞时必须调用此工具，"
                "不要靠模型自身知识猜测天气。"
                "city 不填时自动用用户所在地。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "city": {
                        "type": "string",
                        "description": "城市名，如'重庆'、'北京'、'上海'。不填则用用户默认所在地。",
                    }
                },
            },
        },
    },
    # 后续在这里追加更多工具：device_control 等
    {
        "type": "function",
        "function": {
            "name": "check_emails",
            "description": (
                "检查指定日期的邮件。适用场景：'今天有邮件吗''昨天的邮件整理了没'等。"
                "date 参数传 'today'/'今天'/'yesterday'/'昨天' 或具体日期如 '2026-06-19'。"
                "不传则默认今天。不需要用户提供账号密码，服务端已配置。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {
                        "type": "string",
                        "description": "查询日期：today/今天/yesterday/昨天/YYYY-MM-DD，不传默认今天"
                    }
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "pc_command",
            "description": (
                "控制用户电脑执行操作。适用场景：用户要求桌面创建文件、读写剪贴板、"
                "截屏、打开应用、执行系统命令等。注意：查邮件请用 check_emails 工具。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["desktop_write", "clipboard_get", "clipboard_set",
                                 "screenshot", "run_app", "run_cmd"],
                        "description": (
                            "desktop_write=在桌面创建文件, clipboard_get=读剪贴板, "
                            "clipboard_set=写剪贴板, screenshot=截屏, "
                            "run_app=启动应用, run_cmd=执行命令"
                        ),
                    },
                    "params": {
                        "type": "object",
                        "description": "action 对应的参数，如 filename/content/text/app/cmd",
                    },
                },
                "required": ["action"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "pillow_control",
            "description": (
                "控制智能枕头硬件（气泵+泄气阀）。★ 所有操作必须用 target_kpa 枕头压力值精确控制，禁止用 duration_sec。\n"
                "工作范围 0-10 kPa。用法：\n"
                "1. 先调 read_sensors 获取当前枕头压力 current_kpa\n"
                "2. 根据用户意图计算 target_kpa：\n"
                "   - \"充到X千帕/X帕\" → target_kpa=精确值\n"
                "3. ★★★ 回复时永远不要提\"到X千帕\"\"调到X\"之类的数字，只说\"帮你调高了\"\"放低了些\"等模糊话术\n"
                "   - \"升高/调高/高点\" → target_kpa=current_kpa+0.8 (0-10范围内)\n"
                "   - \"降低/放低/低点\" → target_kpa=current_kpa-0.8\n"
                "   - \"放完/排空/全放\" → target_kpa=0.0, action=recover\n"
                "3. 调用本工具后闭嘴，硬件到位即停，不需要中途报进度\n"
                "4. 禁止用 duration_sec，统一用 target_kpa"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string", "enum": ["tilt", "recover", "stop"],
                        "description": "tilt=充气, recover=泄气, stop=急停"
                    },
                    "duration_sec": {
                        "type": "integer", "minimum": 1, "maximum": 7,
                        "description": "充气秒数，用户模糊描述时用（如'再高点'=3秒，'太高了放点'=3秒）"
                    },
                    "target_kpa": {
                        "type": "number", "minimum": 0.0, "maximum": 10.0,
                        "description": "目标枕头压力kPa。用户给出数字时必填（如'充到3000帕'=3.0, '放到1000帕'=1.0）。填了这个就不用填duration_sec，泵会自动边充/放边读传感器到位即停"
                    },
                },
                "required": ["action"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "led_control",
            "description": (
                "控制智能枕头灯带。用户提到灯、灯带、灯光、开灯、关灯、闪烁、呼吸、渐变、"
                "调亮、调暗、换颜色、睡眠氛围灯时必须调用本工具。"
                "这不是固定话术匹配，而是把用户语义转换成参数：action/mode/color/brightness_pct/speed_pct/duration_sec。"
                "示例：'让灯闪烁'=action set, mode blink；'慢慢变色'=mode gradient, speed_pct 较低；"
                "'助眠一点'=mode breath, color warm, brightness_pct 较低；'亮一点'=action set 并提高 brightness_pct。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["on", "off", "toggle", "set"],
                        "description": "on=打开, off=关闭, toggle=切换开关, set=设置灯效/颜色/亮度"
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["solid", "blink", "breath", "gradient"],
                        "description": "solid=常亮纯色, blink=闪烁, breath=呼吸灯, gradient=平滑渐变"
                    },
                    "color": {
                        "type": "string",
                        "enum": ["warm", "white", "red", "orange", "yellow", "green", "cyan", "blue", "purple", "pink"],
                        "description": "灯光颜色。未指定时优先用 warm，睡眠/助眠场景用 warm，科技/冷感场景可用 blue/cyan/purple"
                    },
                    "brightness_pct": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 100,
                        "description": "亮度百分比。睡眠/夜间建议 8-30，正常氛围 25-60，展示效果 60-100"
                    },
                    "speed_pct": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 100,
                        "description": "动效速度百分比，0很慢，100很快。助眠/呼吸/渐变建议 10-35，闪烁建议 45-80"
                    },
                    "duration_sec": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 600,
                        "description": "动效持续秒数。0表示持续运行；用户说闪一下/闪几下/闪一会儿时给 2-10 秒"
                    },
                },
                "required": ["action"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ir_device_control",
            "description": (
                "通过 ESP32 红外发射模块控制风扇、加湿器和空调。用户说打开/关闭/切换风扇、加湿器、空调、加湿、通风、吹风、制冷时调用。"
                "这是红外开关键控制，硬件会尽量维护开关状态；如果用户用原遥控器操作过，状态可能需要 toggle 校准。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "device": {
                        "type": "string",
                        "enum": ["fan", "humidifier", "air_conditioner"],
                        "description": "fan=风扇，humidifier=加湿器，air_conditioner=空调"
                    },
                    "action": {
                        "type": "string",
                        "enum": ["on", "off", "toggle"],
                        "description": "on=打开，off=关闭，toggle=按一次开关键/切换"
                    },
                },
                "required": ["device", "action"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "didi_ride_link",
            "description": (
                "使用滴滴 MCP 基础版生成打车 App/小程序链接。"
                "适用于用户要打车、叫车、去某地、帮我叫滴滴等场景。"
                "不会直接下单，用户需要在手机上确认车型、下单和支付。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "from_place": {
                        "type": "string",
                        "description": "上车点；用户没说时可留空，由云端 DIDI_DEFAULT_FROM 兜底"
                    },
                    "to_place": {
                        "type": "string",
                        "description": "目的地，如重庆北站、解放碑、公司名称、小区名称"
                    },
                    "city": {
                        "type": "string",
                        "description": "城市，如重庆市、北京市；用户没说时可留空用默认城市"
                    },
                    "product_category": {
                        "type": "string",
                        "description": "车型品类，可选；只有用户明确指定车型时传"
                    },
                },
                "required": ["to_place"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_sensors",
            "description": (
                "读取智能枕头所有传感器数据：空气质量(MQ-135)、枕头压力(MCP5010DP)、"
                "4路压力分布(FSR402)、温度湿度(SHT31)、光照强度(BH1750)。"
                "适用于用户问'枕头状态''温度多少''压力分布'等问题。"
            ),
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
    },
]

STREAMING_RESPONSE_RULES = """
流式工具回复规则：
- 你可以先口头回应，避免用户等待太久。
- 需要调用工具或可能被睡眠勿扰策略拦截时，第一句话只能表达“我看一下 / 我试一下 / 我先确认一下”，不要说“已经打开、正在播放、我帮你打开、马上执行”。
- 如果当前处于睡眠勿扰时间段，先说明会按低打扰方式处理，再给替代方案；不要先肯定执行，再反过来说不能执行。
- 用户用短词回复上一轮问题时，要结合最近上下文理解。例如你问“想听什么声音”，用户答“雨声”，应理解为“想听雨声”，不要误解成窗外正在下雨。
""".strip()

MAX_HISTORY = 40   # 保留最近 N 条消息，多工具调用需要更大窗口
MAX_TURNS = 5      # 单次请求最多允许模型连续调用工具的轮数，防止死循环


async def _dispatch_tool(name: str, arguments: dict, *, client_id: str = "", turn_id: int = 0) -> str:
    """
    执行模型请求的工具调用，返回结果字符串给模型。

    client_id / turn_id 用于 pc_command 路由到正确的 ESP32 设备。
    """
    if name == "get_weather":
        city = arguments.get("city", "")
        print(f"[Tool] get_weather city={city!r}")
        result = await get_weather(city)
        print(f"[Tool] get_weather result={result!r}")
        return result

    elif name == "web_search":
        query = arguments.get("query", "")
        print(f"[Tool] web_search query={query!r}")
        results = await search_web(query)
        if not results:
            return "没有搜到结果，请直接告诉用户。"
        answer = format_search_results(query, results)
        print(f"[Tool] web_search result_len={len(answer)}")
        return answer

    elif name == "get_current_time":
        action = arguments.get("action", "now")
        target = arguments.get("target", "")
        tz = arguments.get("timezone", "")
        result = _handle_get_current_time(action, target, tz)
        print(f"[Tool] get_current_time action={action!r} target={target!r} tz={tz!r}")
        return result

    elif name == "check_emails":
        # 服务端 IMAP 拉取邮件摘要，支持指定日期
        date_str = arguments.get("date", "today")
        target = parse_date_str(date_str)
        if target is None:
            return f"无法解析日期'{date_str}'，请用 today/yesterday/YYYY-MM-DD 格式"
        print(f"[Tool] check_emails target={target}")
        try:
            emails = check_emails_by_date(target)
            result = format_email_summary(emails, target)
            print(f"[Tool] check_emails count={len(emails)}")
            return result
        except Exception as e:
            print(f"[Tool] check_emails error: {e}")
            return f"检查邮件失败：{e}"

    elif name == "pc_command":
        # PC 控制：通过回调发给 PC Agent
        action = arguments.get("action", "")
        params = arguments.get("params", {})
        # LLM 有时把 params 当 JSON 字符串传，兼容一下
        if isinstance(params, str):
            try:
                params = json.loads(params)
            except json.JSONDecodeError:
                params = {}
        guard = guard_ai_action("pc_agent", action=action)
        if not guard["allowed"]:
            return guard["reason"]
        print(f"[Tool] pc_command action={action!r} params={params!r}")
        if not _pc_command_cb:
            return "电脑助手未连接，暂无法执行此操作。"
        try:
            result = await _pc_command_cb(action, params, client_id, turn_id)
            print(f"[Tool] pc_command result={result[:100]!r}")
            return result
        except Exception as e:
            print(f"[Tool] pc_command error: {e}")
            return f"执行失败：{e}"

    elif name == "pillow_control":
        # 枕头硬件控制：通过回调发 WebSocket 命令到 ESP32
        action = arguments.get("action", "")
        duration = arguments.get("duration_sec", 3)
        target_kpa = arguments.get("target_kpa")
        guard = guard_ai_action(
            "pillow",
            action=action,
            duration_sec=duration,
            target_kpa=target_kpa,
        )
        if not guard["allowed"]:
            return guard["reason"]
        if guard.get("overrides", {}).get("duration_sec") is not None:
            duration = guard["overrides"]["duration_sec"]
        print(f"[Tool] pillow_control action={action!r} duration={duration} target_kpa={target_kpa}")
        if not _pillow_cb:
            return "枕头控制未连接，请确认 ESP32 在线。"
        try:
            result = await _pillow_cb(action, duration, client_id, turn_id, target_kpa)
            return result
        except Exception as e:
            print(f"[Tool] pillow_control error: {e}")
            return f"枕头控制失败：{e}"

    elif name == "led_control":
        # 灯带控制：模型只负责语义参数，ESP32 负责执行灯效
        action = arguments.get("action", "set")
        mode = arguments.get("mode", "")
        color = arguments.get("color", "")
        brightness_pct = arguments.get("brightness_pct")
        speed_pct = arguments.get("speed_pct")
        duration_sec = arguments.get("duration_sec")
        guard = guard_ai_action(
            "led",
            action=action,
            mode=mode,
            color=color,
            brightness_pct=brightness_pct,
            speed_pct=speed_pct,
        )
        if not guard["allowed"]:
            return guard["reason"]
        print(
            f"[Tool] led_control action={action!r} mode={mode!r} color={color!r} "
            f"brightness_pct={brightness_pct} speed_pct={speed_pct} duration_sec={duration_sec}"
        )
        if not _led_cb:
            return "灯带控制未连接，请确认 ESP32 在线。"
        try:
            result = await _led_cb(
                action, mode, color, brightness_pct, speed_pct, duration_sec,
                client_id, turn_id
            )
            return result
        except Exception as e:
            print(f"[Tool] led_control error: {e}")
            return f"灯带控制失败：{e}"

    elif name == "ir_device_control":
        device = arguments.get("device", "")
        action = arguments.get("action", "toggle")
        guard = guard_ai_action("ir_device", action=action, device=device)
        if not guard["allowed"]:
            return guard["reason"]
        print(f"[Tool] ir_device_control device={device!r} action={action!r}")
        if not _ir_device_cb:
            return "红外设备控制未连接，请确认 ESP32 在线。"
        try:
            result = await _ir_device_cb(device, action, client_id, turn_id)
            return result
        except Exception as e:
            print(f"[Tool] ir_device_control error: {e}")
            return f"红外设备控制失败：{e}"

    elif name == "didi_ride_link":
        from_place = arguments.get("from_place", "")
        to_place = arguments.get("to_place", "")
        city = arguments.get("city", "")
        product_category = arguments.get("product_category", "")
        print(
            f"[Tool] didi_ride_link from={from_place!r} to={to_place!r} "
            f"city={city!r} product={product_category!r}"
        )
        if not _didi_ride_link_cb:
            return "滴滴 MCP 工具还没有接入云端。"
        try:
            result = await _didi_ride_link_cb(
                from_place, to_place, city, product_category, client_id, turn_id
            )
            return result
        except Exception as e:
            print(f"[Tool] didi_ride_link error: {e}")
            return f"生成滴滴打车链接失败：{e}"

    elif name == "read_sensors":
        # 传感器数据：通过回调发 WebSocket 到 ESP32 并等待返回
        print(f"[Tool] read_sensors client={client_id} turn={turn_id}")
        if not _read_sensors_cb:
            return "ESP32 未连接，无法读取传感器数据。"
        try:
            result = await _read_sensors_cb(client_id, turn_id)
            return result
        except Exception as e:
            print(f"[Tool] read_sensors error: {e}")
            return f"传感器读取失败：{e}"

    return f"工具 {name} 暂未实现"


async def chat_stream(user_text: str, history: list[dict] | None = None,
                      *, client_id: str = "", turn_id: int = 0):
    """
    ★ xiaozhi 风格流式对话，支持 Function Calling。

    跨多轮 LLM 调用连续 yield token，中间插工具执行也不打断流。
    调用方只需一个 async for，不需要关心工具调用细节。

    client_id / turn_id 透传给 _dispatch_tool，用于 pc_command 等需要设备路由的工具。
    """
    if history is None:
        history = []

    history.append({"role": "user", "content": user_text})
    if len(history) > MAX_HISTORY:
        history[:] = history[-MAX_HISTORY:]
        # ★ 移除开头的孤儿 tool 消息（截断可能拆散 assistant(tool_calls) + tool 对）
        while history and history[0].get("role") == "tool":
            history.pop(0)

    for _ in range(MAX_TURNS):
        # ★ 注入当前时间和用户所在地到 System Prompt
        system_with_time = SYSTEM_PROMPT + (
            f"\n\n当前时间：{_get_time_string()}"
            f"\n用户所在地：{LOCATION}"
            f"\n\n{build_ai_context_prompt()}"
            f"\n\n{STREAMING_RESPONSE_RULES}"
        )
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[{"role": "system", "content": system_with_time}] + history,
            max_tokens=2048,
            tools=TOOLS,
            stream=True,
            extra_body={"enable_search": True},  # ★ DeepSeek 原生联网搜索
        )

        content_parts: list[str] = []
        tool_calls: dict[int, dict] = {}

        async for chunk in response:
            delta = chunk.choices[0].delta

            if delta.content:
                content_parts.append(delta.content)
                yield delta.content  # ← 立刻流出，不等待

            if delta.tool_calls:
                for tc in delta.tool_calls:
                    idx = tc.index
                    if idx not in tool_calls:
                        tool_calls[idx] = {"id": "", "function": {"name": "", "arguments": ""}}
                    if tc.id:
                        tool_calls[idx]["id"] = tc.id
                    if tc.function:
                        if tc.function.name and not tool_calls[idx]["function"]["name"]:
                            tool_calls[idx]["function"]["name"] = tc.function.name
                        if tc.function.arguments:
                            tool_calls[idx]["function"]["arguments"] += tc.function.arguments

        content = "".join(content_parts).strip()

        # 有工具调用 → 执行并继续下一轮
        if tool_calls:
            tc_list = []
            for idx in sorted(tool_calls.keys()):
                tc = tool_calls[idx]
                tc_list.append({
                    "id": tc["id"],
                    "type": "function",
                    "function": tc["function"],
                })

            history.append({
                "role": "assistant",
                "content": content or None,
                "tool_calls": tc_list,
            })

            for tc in tc_list:
                args = json.loads(tc["function"]["arguments"])
                result = await _dispatch_tool(tc["function"]["name"], args,
                                                client_id=client_id, turn_id=turn_id)
                history.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": result,
                })
            continue  # ← 下一轮 LLM，继续 yield

        # 纯文本回复，没有工具调用
        reply = content
        history.append({"role": "assistant", "content": reply})
        if len(history) > MAX_HISTORY:
            history[:] = history[-MAX_HISTORY:]
            while history and history[0].get("role") == "tool":
                history.pop(0)
        return

    # 超过最大轮数
    fallback = "抱歉，我刚才有点转不过来，能再说一遍吗？"
    yield fallback
    history.append({"role": "assistant", "content": fallback})
