"""DiDi MCP basic ride-link integration.

基础版：只生成滴滴 App/小程序打车链接，不直接创建订单。
用户需要在手机滴滴里自行确认车型、下单和支付。
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

DIDI_MCP_KEY = os.getenv("DIDI_MCP_KEY", "").strip()
DIDI_MCP_BASE_URL = os.getenv("DIDI_MCP_BASE_URL", "https://mcp.didichuxing.com").rstrip("/")
DIDI_MCP_SANDBOX = os.getenv("DIDI_MCP_SANDBOX", "0").strip().lower() in {"1", "true", "yes", "on"}
DIDI_DEFAULT_CITY = os.getenv("DIDI_DEFAULT_CITY", os.getenv("LOCATION", "")).strip()
DIDI_DEFAULT_FROM = os.getenv("DIDI_DEFAULT_FROM", "").strip()
DIDI_REQUEST_TIMEOUT = float(os.getenv("DIDI_REQUEST_TIMEOUT", "30"))


@dataclass
class Poi:
    name: str
    address: str
    lng: str
    lat: str
    city: str = ""


def _server_url() -> str:
    path = "mcp-servers-sandbox" if DIDI_MCP_SANDBOX else "mcp-servers"
    return f"{DIDI_MCP_BASE_URL}/{path}?key={DIDI_MCP_KEY}"


def _normalize_city(city: str | None) -> str:
    city = (city or DIDI_DEFAULT_CITY or "").strip()
    if not city:
        return ""
    if city.endswith(("市", "州", "地区", "盟", "县")):
        return city
    # 常见直辖市/城市名补全，避免 MCP 地图搜索因城市格式失败。
    return city + "市"


def _extract_json_text(result: dict[str, Any]) -> Any:
    content = (result.get("result") or {}).get("content") or []
    if content and isinstance(content, list):
        text = content[0].get("text") if isinstance(content[0], dict) else ""
        if isinstance(text, str):
            try:
                return json.loads(text)
            except Exception:
                return text
    return None


def _content_text(result: dict[str, Any]) -> str:
    content = (result.get("result") or {}).get("content") or []
    if content and isinstance(content, list) and isinstance(content[0], dict):
        return str(content[0].get("text") or "")
    if "error" in result:
        err = result.get("error") or {}
        return f"{err.get('message') or err}"
    return ""


def _structured(result: dict[str, Any]) -> dict[str, Any]:
    return ((result.get("result") or {}).get("structuredContent") or {}) if isinstance(result, dict) else {}


def _first_url(text: str) -> str:
    m = re.search(r"https?://[^\s\]\)\u3002\uff0c,]+", text or "")
    return m.group(0) if m else ""


async def mcp_call(tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    if not DIDI_MCP_KEY:
        return {"error": {"code": "NO_KEY", "message": "DIDI_MCP_KEY 未配置"}}
    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "id": 1,
        "params": {
            "name": tool_name,
            "arguments": arguments,
        },
    }
    async with httpx.AsyncClient(timeout=DIDI_REQUEST_TIMEOUT) as client:
        resp = await client.post(
            _server_url(),
            json=payload,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        # MCP 业务错误可能仍是 200 + error/result.isError，这里统一返回 JSON。
        try:
            return resp.json()
        except Exception:
            return {"error": {"code": resp.status_code, "message": resp.text[:300]}}


async def search_poi(keywords: str, city: str, *, location: str = "") -> Poi | None:
    args: dict[str, Any] = {"keywords": keywords, "city": city}
    if location:
        args["location"] = location
    data = await mcp_call("maps_textsearch", args)
    parsed = _extract_json_text(data)
    if not isinstance(parsed, list) or not parsed:
        return None
    item = parsed[0]
    loc = item.get("location") or {}
    lng = loc.get("lng")
    lat = loc.get("lat")
    if lng is None or lat is None:
        return None
    return Poi(
        name=str(item.get("display_name") or keywords),
        address=str(item.get("address_all") or item.get("address") or ""),
        lng=str(lng),
        lat=str(lat),
        city=str(item.get("city") or city),
    )


def _format_estimate(data: dict[str, Any]) -> str:
    if data.get("error"):
        return ""
    sc = _structured(data)
    items = sc.get("items") or []
    if not isinstance(items, list) or not items:
        return ""
    rows = []
    for it in items[:4]:
        name = it.get("productName") or it.get("name") or it.get("productCategory") or "车型"
        price = it.get("priceText") or it.get("estimatePrice") or it.get("price") or ""
        price = str(price)
        if price and not price.endswith("元") and price.replace(".", "", 1).isdigit():
            price = f"约{price}元"
        rows.append(f"{name}{price and ' ' + price}")
    return "，".join(rows)


async def create_basic_ride_link(
    *,
    from_place: str = "",
    to_place: str = "",
    city: str = "",
    product_category: str = "",
) -> dict[str, Any]:
    """Generate a DiDi ride app link and a short user-facing message."""
    city = _normalize_city(city)
    from_place = (from_place or DIDI_DEFAULT_FROM or "").strip()
    to_place = (to_place or "").strip()
    product_category = (product_category or "").strip()

    if not DIDI_MCP_KEY:
        return {"ok": False, "message": "滴滴 MCP Key 还没配置，无法生成打车链接。"}
    if not city:
        return {"ok": False, "message": "请先告诉我你所在城市，比如北京市、重庆市。"}
    if not to_place:
        return {"ok": False, "message": "请告诉我要去哪里。"}
    if not from_place:
        return {"ok": False, "message": "请告诉我从哪里上车，或者在 .env 里配置 DIDI_DEFAULT_FROM。"}

    from_poi = await search_poi(from_place, city)
    if not from_poi:
        return {"ok": False, "message": f"没有搜到上车点：{from_place}。请换一个更具体的位置。"}
    ref_loc = f"{from_poi.lng},{from_poi.lat}"
    to_poi = await search_poi(to_place, city, location=ref_loc)
    if not to_poi:
        return {"ok": False, "message": f"没有搜到目的地：{to_place}。请换一个更具体的位置。"}

    estimate_text = ""
    try:
        estimate = await mcp_call("taxi_estimate", {
            "from_lng": from_poi.lng,
            "from_lat": from_poi.lat,
            "from_name": from_poi.name,
            "to_lng": to_poi.lng,
            "to_lat": to_poi.lat,
            "to_name": to_poi.name,
        })
        estimate_text = _format_estimate(estimate)
    except Exception:
        estimate_text = ""

    link_args: dict[str, Any] = {
        "from_lng": from_poi.lng,
        "from_lat": from_poi.lat,
        "to_lng": to_poi.lng,
        "to_lat": to_poi.lat,
    }
    if product_category:
        link_args["product_category"] = product_category
    link_data = await mcp_call("taxi_generate_ride_app_link", link_args)
    if link_data.get("error"):
        return {"ok": False, "message": f"生成滴滴链接失败：{_content_text(link_data)}"}
    if (link_data.get("result") or {}).get("isError"):
        return {"ok": False, "message": f"生成滴滴链接失败：{_content_text(link_data)}"}

    sc = _structured(link_data)
    app_link = str(sc.get("appLink") or sc.get("browserLink") or "")
    mini_link = str(sc.get("miniprogramLink") or "")
    text = _content_text(link_data)
    if not app_link:
        app_link = _first_url(text)
    if not app_link and not mini_link:
        return {"ok": False, "message": "滴滴返回了结果，但没有拿到可打开的链接。"}

    route = f"{from_poi.name} → {to_poi.name}"
    estimate_part = f"预估车型/价格：{estimate_text}。" if estimate_text else ""
    message = (
        f"已生成滴滴打车链接：{route}。"
        f"{estimate_part}"
        "我已经把链接发到手机端页面了，请在手机上打开滴滴链接，选择车型并完成确认和支付。"
        "我不会替你直接下单或支付。"
    )
    payload = {
        "type": "didi_ride_link",
        "route": route,
        "from": from_poi.__dict__,
        "to": to_poi.__dict__,
        "city": city,
        "estimate": estimate_text,
        "appLink": app_link,
        "miniprogramLink": mini_link,
        "text": text,
    }
    return {"ok": True, "message": message, "payload": payload}
