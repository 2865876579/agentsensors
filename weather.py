"""Weather lookup backed by Photon geocoding and Open-Meteo."""
import aiohttp
from config import LOCATION

_PHOTON_URL = "https://photon.komoot.io/api/"
_OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
_TIMEOUT = aiohttp.ClientTimeout(total=8)

_OPEN_METEO_CODES = {
    0: "晴",
    1: "大部晴朗",
    2: "多云",
    3: "阴",
    45: "有雾",
    48: "雾凇",
    51: "小毛毛雨",
    53: "毛毛雨",
    55: "较强毛毛雨",
    61: "小雨",
    63: "中雨",
    65: "大雨",
    71: "小雪",
    73: "中雪",
    75: "大雪",
    80: "阵雨",
    81: "较强阵雨",
    82: "强阵雨",
    95: "雷雨",
    96: "雷雨伴冰雹",
    99: "强雷雨伴冰雹",
}


async def get_weather(city: str = "") -> str:
    """Return current weather and a three-day forecast for a city."""
    city = (city.strip() or LOCATION or "重庆").strip()
    headers = {"User-Agent": "XiaoAnSmartPillow/1.0"}
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT, headers=headers) as session:
            async with session.get(_PHOTON_URL, params={"q": city, "limit": 5}) as resp:
                resp.raise_for_status()
                features = (await resp.json()).get("features") or []
            if not features:
                return f"找不到城市「{city}」，请确认城市名拼写。"

            place = next(
                (item for item in features
                 if item.get("properties", {}).get("countrycode") == "CN"),
                features[0],
            )
            longitude, latitude = place["geometry"]["coordinates"]
            params = {
                "latitude": latitude,
                "longitude": longitude,
                "current": (
                    "temperature_2m,apparent_temperature,relative_humidity_2m,"
                    "weather_code,wind_speed_10m,wind_direction_10m"
                ),
                "daily": "weather_code,temperature_2m_max,temperature_2m_min",
                "timezone": "auto",
                "forecast_days": 3,
            }
            async with session.get(_OPEN_METEO_URL, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
    except Exception as exc:
        print(f"[Weather] Open-Meteo error: {exc}")
        return "天气数据暂时获取失败，请稍后再试。"

    current = data.get("current") or {}
    daily = data.get("daily") or {}
    code = int(current.get("weather_code", -1))
    text = _OPEN_METEO_CODES.get(code, f"天气代码{code}")
    lines = [
        f"{city}当前天气：{text}，{current.get('temperature_2m', '?')}°C"
        f"（体感 {current.get('apparent_temperature', '?')}°C）",
        f"湿度 {current.get('relative_humidity_2m', '?')}%，"
        f"风速 {current.get('wind_speed_10m', '?')}公里每小时",
    ]
    codes = daily.get("weather_code") or []
    mins = daily.get("temperature_2m_min") or []
    maxes = daily.get("temperature_2m_max") or []
    labels = ["今天", "明天", "后天"]
    forecast = []
    for index in range(min(3, len(codes), len(mins), len(maxes))):
        day_text = _OPEN_METEO_CODES.get(int(codes[index]), "天气未知")
        forecast.append(
            f"{labels[index]}{day_text} {mins[index]}~{maxes[index]}°C"
        )
    if forecast:
        lines.append("预报：" + "，".join(forecast))
    return "；".join(lines) + "。"
