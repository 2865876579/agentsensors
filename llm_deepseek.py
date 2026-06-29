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

client = AsyncOpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)

# ── 系统提示词 ──────────────────────────────────────────────
SYSTEM_PROMPT = """你是"小安"，一个放在用户枕边的语音伴侣。用户通过语音和你聊天，不是在读屏幕。

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
- ★ 你自带联网搜索，知道当前实时信息。一般问题直接用内置搜索回答，不要频繁调 web_search 工具。web_search 工具只用于需要精确结构化数据的场景（金价、天气、今日新闻等）
- ★ 用户要桌面写文件、剪贴板操作等，必须调 pc_command 执行，不准光嘴上答应
- ★ 你可以调用 read_sensors 工具查看枕头传感器状态（压力分布/温湿度/光照/空气质量）。用户问"枕头现在怎样""温度多少"或想了解睡姿压力时使用
- ★ 你可以调用 led_control 工具控制灯带。用户提到灯、灯带、灯光、开关灯、闪烁、呼吸、渐变、调亮、调暗、换颜色、助眠氛围时必须调用 led_control，不准说没有接入灯控。
- ★ 你可以调用 ir_device_control 工具通过红外控制风扇和加湿器。用户提到打开/关闭/切换风扇或加湿器时必须调用该工具，不准只口头答应。
- ★ 当收到"用户刚刚躺下了，请温柔地主动问候一句"这条系统消息时，表示感应器检测到用户就寝。此时简短温柔地问候一句（1-2句），不要等用户回复，不要提"感应器""系统"之类的话，自然得像你感觉到身边有人躺下了一样
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
                "控制智能枕头硬件（气泵+泄气阀）。★ 所有操作必须用 target_kpa 气压值精确控制，禁止用 duration_sec。\n"
                "工作范围 0-10 kPa。用法：\n"
                "1. 先调 read_sensors 获取当前气压 current_kpa\n"
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
                        "description": "目标气压kPa。用户给出数字时必填（如'充到3000帕'=3.0, '放到1000帕'=1.0）。填了这个就不用填duration_sec，泵会自动边充/放边读传感器到位即停"
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
                "通过 ESP32 红外发射模块控制风扇和加湿器。用户说打开/关闭/切换风扇、加湿器、加湿、通风、吹风时调用。"
                "这是红外开关键控制，硬件会尽量维护开关状态；如果用户用原遥控器操作过，状态可能需要 toggle 校准。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "device": {
                        "type": "string",
                        "enum": ["fan", "humidifier"],
                        "description": "fan=风扇，humidifier=加湿器"
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
            "name": "read_sensors",
            "description": (
                "读取智能枕头所有传感器数据：空气质量(MQ-135)、气压(MCP5010DP)、"
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
    if name == "web_search":
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
