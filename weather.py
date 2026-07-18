"""
和风天气 API 封装

免费档：每天 1000 次，足够个人使用。
注册地址：https://dev.qweather.com
"""
import asyncio
import aiohttp
from config import QWEATHER_API_KEY, LOCATION

# 和风天气 API 地址
_GEO_URL = "https://geoapi.qweather.com/v2/city/lookup"
_NOW_URL = "https://devapi.qweather.com/v7/weather/now"
_FORECAST_URL = "https://devapi.qweather.com/v7/weather/3d"

# 天气状态码 → 简洁中文描述（节选常用）
_WEATHER_TEXT_OVERRIDE: dict[str, str] = {}  # 可自定义覆盖

_TIMEOUT = aiohttp.ClientTimeout(total=8)


async def _geo_lookup(city: str) -> str | None:
    """把城市名转成和风天气的 location ID，失败返回 None。"""
    if not QWEATHER_API_KEY:
        return None
    params = {"location": city, "key": QWEATHER_API_KEY, "number": 1}
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            async with session.get(_GEO_URL, params=params) as resp:
                data = await resp.json()
        if data.get("code") == "200" and data.get("location"):
            loc = data["location"][0]
            return loc["id"]
    except Exception as exc:
        print(f"[Weather] geo_lookup error: {exc}")
    return None


async def get_weather(city: str = "") -> str:
    """
    查询指定城市当前天气 + 3 日预报，返回供 LLM 播报的简洁字符串。

    city 为空时使用 config.LOCATION（默认用户所在地）。
    失败时返回说明原因的字符串，不抛异常。
    """
    if not QWEATHER_API_KEY:
        return "天气功能未配置 API key，请在 .env 里填写 QWEATHER_API_KEY。"

    target_city = (city.strip() or LOCATION or "重庆").strip()

    # 1. 城市 ID 查询
    location_id = await _geo_lookup(target_city)
    if not location_id:
        return f"找不到城市「{target_city}」，请确认城市名拼写。"

    params = {"location": location_id, "key": QWEATHER_API_KEY}

    # 2. 并发请求当前天气 + 3日预报
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
        now_task = session.get(_NOW_URL, params=params)
        forecast_task = session.get(_FORECAST_URL, params=params)
        try:
            async with now_task as now_resp, forecast_task as fc_resp:
                now_data = await now_resp.json()
                fc_data = await fc_resp.json()
        except Exception as exc:
            print(f"[Weather] fetch error: {exc}")
            return f"天气数据获取失败：{exc}"

    # 3. 解析当前天气
    if now_data.get("code") != "200":
        return f"天气 API 返回错误码 {now_data.get('code')}，请检查 API key 是否有效。"

    now = now_data["now"]
    temp = now.get("temp", "?")
    feels_like = now.get("feelsLike", "?")
    text = now.get("text", "?")
    humidity = now.get("humidity", "?")
    wind_dir = now.get("windDir", "")
    wind_scale = now.get("windScale", "")

    lines = [
        f"{target_city}当前天气：{text}，{temp}°C（体感 {feels_like}°C）",
        f"湿度 {humidity}%，{wind_dir}{wind_scale}级风",
    ]

    # 4. 解析 3 日预报
    if fc_data.get("code") == "200" and fc_data.get("daily"):
        day_labels = ["今天", "明天", "后天"]
        forecast_parts = []
        for i, day in enumerate(fc_data["daily"][:3]):
            label = day_labels[i] if i < len(day_labels) else f"第{i+1}天"
            day_text = day.get("textDay", "?")
            t_min = day.get("tempMin", "?")
            t_max = day.get("tempMax", "?")
            forecast_parts.append(f"{label}{day_text} {t_min}~{t_max}°C")
        lines.append("预报：" + "，".join(forecast_parts))

    return "；".join(lines) + "。"
