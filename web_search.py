"""
Web search helper used by the cloud server.

It intentionally uses only Python standard library modules so the local
prototype does not need another API key or package install.
"""
from __future__ import annotations

import asyncio
import html
import json
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
)

WEATHER_TRANSLATIONS = {
    "Sunny": "晴",
    "Clear": "晴",
    "Partly cloudy": "局部多云",
    "Cloudy": "多云",
    "Overcast": "阴",
    "Mist": "薄雾",
    "Fog": "雾",
    "Haze": "霾",
    "Smoky haze": "烟霾",
    "Light rain": "小雨",
    "Moderate rain": "中雨",
    "Heavy rain": "大雨",
    "Patchy rain nearby": "附近有零星降雨",
    "Thundery outbreaks in nearby": "附近有雷雨",
}

GOLD_KEYWORDS = ("金价", "黄金", "金店", "足金", "支付宝金", "积存金", "现货金", "Au99")

NEWS_KEYWORDS = (
    "新闻", "大事", "时事", "热点", "热搜", "要闻", "头条", "国内外",
    "今天发生", "今日发生", "最近发生", "发生了什么",
)

NEWS_DOMESTIC_WORDS = ("国内", "中国", "全国", "内地")
NEWS_WORLD_WORDS = ("国际", "国外", "海外", "全球", "世界")

JUNK_SEARCH_KEYWORDS = (
    "历史上的今天", "黄历", "老黄历", "万年历", "农历", "日历", "吉日",
    "星座", "彩票", "开奖", "梦见", "周公解梦",
)

QUERY_STOPWORDS = (
    "今天", "今日", "现在", "最新", "最近", "帮我", "请问", "查询", "搜索",
    "查一下", "搜一下", "一下", "什么", "多少", "怎么样", "如何", "国内",
    "国际", "国外", "海外", "有", "吗", "呢", "的",
)


@dataclass
class SearchResult:
    title: str
    url: str
    snippet: str


def _clean_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(keyword.lower() in lowered for keyword in keywords)


def _is_gold_query(query: str) -> bool:
    return _contains_any(query, GOLD_KEYWORDS)


def _is_news_query(query: str) -> bool:
    if _contains_any(query, NEWS_KEYWORDS):
        return True
    # 含硬新闻词（局势/冲突/战争等）大概率是新闻查询
    hard_words = ("局势", "冲突", "战争", "选举", "制裁", "外交", "政变", "抗议", "暴乱", "恐怖", "袭击")
    if any(word in query for word in hard_words):
        return True
    has_scope_word = any(word in query for word in NEWS_DOMESTIC_WORDS + NEWS_WORLD_WORDS)
    if has_scope_word:
        return True  # "国际新闻""国内大事" 都是新闻
    has_time_word = any(word in query for word in ("今天", "今日", "现在", "最新", "最近"))
    has_news_word = any(word in query for word in ("发生", "事件", "消息", "要闻", "头条", "热点"))
    return has_time_word and has_news_word


def _result_has_junk(item: SearchResult, query: str) -> bool:
    haystack = f"{item.title} {item.snippet}".lower()
    query_lower = query.lower()
    for keyword in JUNK_SEARCH_KEYWORDS:
        lowered = keyword.lower()
        if lowered in haystack and lowered not in query_lower:
            return True
    return False


def _query_terms(query: str) -> list[str]:
    cleaned = query
    for word in QUERY_STOPWORDS:
        cleaned = cleaned.replace(word, " ")
    cleaned = re.sub(r"[，。！？、,.!?;；:：]", " ", cleaned)

    terms: list[str] = []
    for token in re.findall(r"[A-Za-z0-9]+", cleaned):
        if len(token) >= 2:
            terms.append(token.lower())

    for segment in re.findall(r"[\u4e00-\u9fff]+", cleaned):
        if len(segment) == 1:
            terms.append(segment)
        else:
            for index in range(len(segment) - 1):
                term = segment[index:index + 2]
                if term.strip():
                    terms.append(term)

    deduped: list[str] = []
    seen: set[str] = set()
    for term in terms:
        if term not in seen:
            seen.add(term)
            deduped.append(term)
    return deduped


def _result_is_relevant(item: SearchResult, query: str) -> bool:
    terms = _query_terms(query)
    if not terms:
        return True
    haystack = f"{item.title} {item.snippet} {item.url}".lower()
    return any(term in haystack for term in terms)


def _normalize_title(title: str) -> str:
    title = re.sub(r"^(国内|国际|滚动)[：:]\s*", "", title)
    title = re.sub(r"\s+", "", title)
    return title.lower()


def _dedupe_results(results: list[SearchResult]) -> list[SearchResult]:
    deduped: list[SearchResult] = []
    seen: set[str] = set()
    for item in results:
        key = _normalize_title(item.title) or item.url
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _limit_text(text: str, max_chars: int) -> str:
    text = _clean_text(text)
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "..."


def _read_url(url: str, timeout: int = 8) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def _read_sina_quote(symbol: str) -> str:
    url = f"https://hq.sinajs.cn/list={symbol}"
    request = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Referer": "https://finance.sina.com.cn/",
    })
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.read().decode("gbk", errors="replace")


def _parse_sina_assignment(text: str) -> list[str]:
    match = re.search(r'="(.*)";', text)
    if not match:
        return []
    value = match.group(1).strip()
    if not value:
        return []
    return [part.strip() for part in value.split(",")]


def _fmt_number(value: str, suffix: str = "") -> str:
    try:
        return f"{float(value):.2f}{suffix}"
    except Exception:
        return value + suffix if value else "未知"


def _gold_search(query: str) -> list[SearchResult]:
    if not _is_gold_query(query):
        return []

    results: list[SearchResult] = []

    try:
        parts = _parse_sina_assignment(_read_sina_quote("SGE_AU9999"))
        if len(parts) >= 18:
            results.append(SearchResult(
                title="上海黄金交易所 Au99.99",
                url="https://finance.sina.com.cn/futures/quotes/SGE_AU9999.shtml",
                snippet=(
                    f"最新{_fmt_number(parts[3], '元/克')}，"
                    f"涨跌幅{parts[17]}，时间{parts[16]}。"
                ),
            ))
    except Exception as exc:
        print(f"[GoldSearch] SGE_AU9999 failed: {exc}")

    try:
        parts = _parse_sina_assignment(_read_sina_quote("SGE_AUTD"))
        if len(parts) >= 18:
            results.append(SearchResult(
                title="上海黄金交易所 Au(T+D)",
                url="https://finance.sina.com.cn/futures/quotes/SGE_AUTD.shtml",
                snippet=(
                    f"最新{_fmt_number(parts[3], '元/克')}，"
                    f"涨跌幅{parts[17]}，时间{parts[16]}。"
                ),
            ))
    except Exception as exc:
        print(f"[GoldSearch] SGE_AUTD failed: {exc}")

    try:
        parts = _parse_sina_assignment(_read_sina_quote("hf_XAU"))
        if len(parts) >= 15:
            results.append(SearchResult(
                title=parts[14] or "伦敦金（现货黄金）",
                url="https://finance.sina.com.cn/futures/quotes/hf_XAU.shtml",
                snippet=(
                    f"最新{_fmt_number(parts[0], '美元/盎司')}，"
                    f"日内高点{_fmt_number(parts[4])}，低点{_fmt_number(parts[5])}，"
                    f"时间{parts[13]} {parts[6]}。"
                ),
            ))
    except Exception as exc:
        print(f"[GoldSearch] hf_XAU failed: {exc}")

    if "支付宝" in query and results:
        results.insert(0, SearchResult(
            title="支付宝金价说明",
            url="https://www.alipay.com/",
            snippet=(
                "支付宝内的黄金价格通常和具体黄金基金、积存金产品、买卖差价或服务费有关，"
                "未必等同于上海金实时盘面价。下面行情可作为基准参考，最终以支付宝 App 内页面为准。"
            ),
        ))

    return results


def _extract_weather_location(query: str) -> str | None:
    if "天气" not in query and "气温" not in query:
        return None

    location = query
    for word in ("天气预报", "天气", "气温", "今天", "今日", "明天", "后天", "昨天", "现在", "最新", "查询", "查一下", "怎么样", "如何", "的"):
        location = location.replace(word, " ")
    location = re.sub(r"[，。！？、,.!?]", " ", location)
    location = re.sub(r"\s+", " ", location).strip()
    if not location:
        return None
    return location[:30]


def _weather_search(query: str) -> list[SearchResult]:
    location = _extract_weather_location(query)
    if not location:
        return []

    url_location = urllib.parse.quote(location)
    url = f"https://wttr.in/{url_location}?format=j1&lang=zh"
    data = json.loads(_read_url(url, timeout=8))
    current = data.get("current_condition", [{}])[0]
    weather = current.get("lang_zh") or current.get("weatherDesc") or [{}]
    weather_text = weather[0].get("value", "") if weather else ""
    weather_text = WEATHER_TRANSLATIONS.get(weather_text, weather_text)

    parts = [
        f"当前{current.get('temp_C', '?')}℃",
        f"体感{current.get('FeelsLikeC', '?')}℃",
        f"湿度{current.get('humidity', '?')}%",
        f"能见度{current.get('visibility', '?')}公里",
    ]
    if weather_text:
        parts.insert(0, weather_text)

    forecast_today = data.get("weather", [{}])[0]
    if forecast_today:
        parts.append(
            f"今日{forecast_today.get('mintempC', '?')}到{forecast_today.get('maxtempC', '?')}℃"
        )
    # 明天预报——LLM 查明天天气时第一次就能拿到答案
    if len(data.get("weather", [])) > 1:
        forecast_tomorrow = data["weather"][1]
        parts.append(
            f"明天{forecast_tomorrow.get('mintempC', '?')}到{forecast_tomorrow.get('maxtempC', '?')}℃"
        )

    return [SearchResult(
        title=f"{location}当前天气",
        url=f"https://wttr.in/{url_location}",
        snippet="，".join(parts),
    )]


def _parse_jsonp(text: str) -> dict:
    match = re.search(r"^[^(]+\((.*)\)\s*$", text.strip(), flags=re.S)
    if not match:
        return {}
    return json.loads(match.group(1))


def _cctv_news_search(scope: str, url: str, max_results: int) -> list[SearchResult]:
    data = _parse_jsonp(_read_url(url, timeout=6))
    items = data.get("data", {}).get("list", [])
    results: list[SearchResult] = []

    for item in items:
        title = _clean_text(str(item.get("title", "")))
        link = _clean_text(str(item.get("url", "")))
        brief = _clean_text(str(item.get("brief", "")))
        focus_date = _clean_text(str(item.get("focus_date", "")))
        if not title or not link:
            continue
        snippet_parts = ["央视新闻"]
        if focus_date:
            snippet_parts.append(focus_date)
        if brief:
            snippet_parts.append(_limit_text(brief, 80))
        results.append(SearchResult(
            title=f"{scope}：{title}",
            url=link,
            snippet="，".join(snippet_parts),
        ))
        if len(results) >= max_results:
            break
    return results


def _rss_news_search(scope: str, url: str, max_results: int) -> list[SearchResult]:
    root = ET.fromstring(_read_url(url, timeout=6))
    results: list[SearchResult] = []

    for item in root.findall(".//item"):
        title = _clean_text(item.findtext("title") or "")
        link = _clean_text(item.findtext("link") or "")
        description = _clean_text(item.findtext("description") or "")
        pub_date = _clean_text(item.findtext("pubDate") or "")
        if not title or not link:
            continue
        snippet_parts = ["中新网"]
        if pub_date:
            snippet_parts.append(pub_date)
        if description:
            snippet_parts.append(_limit_text(description, 80))
        results.append(SearchResult(
            title=f"{scope}：{title}",
            url=link,
            snippet="，".join(snippet_parts),
        ))
        if len(results) >= max_results:
            break
    return results


def _news_search(query: str, max_results: int) -> list[SearchResult]:
    if not _is_news_query(query):
        return []

    query_has_domestic = any(word in query for word in NEWS_DOMESTIC_WORDS)
    query_has_world = any(word in query for word in NEWS_WORLD_WORDS)
    wants_both = "国内外" in query or (query_has_domestic and query_has_world)
    wants_domestic = wants_both or query_has_domestic or not query_has_world
    wants_world = wants_both or query_has_world or not query_has_domestic

    source_jobs: list[tuple[str, str, str]] = []
    if wants_domestic:
        source_jobs.extend([
            ("cctv", "国内", "https://news.cctv.com/2019/07/gaiban/cmsdatainterface/page/news_1.jsonp?cb=news"),
            ("rss", "国内", "https://www.chinanews.com.cn/rss/china.xml"),
        ])
    if wants_world:
        source_jobs.extend([
            ("cctv", "国际", "https://news.cctv.com/2019/07/gaiban/cmsdatainterface/page/world_1.jsonp?cb=world"),
            ("rss", "国际", "https://www.chinanews.com.cn/rss/world.xml"),
        ])
    if wants_both or (wants_domestic and wants_world):
        source_jobs.append(("rss", "滚动", "https://www.chinanews.com.cn/rss/scroll-news.xml"))

    results: list[SearchResult] = []
    errors: list[str] = []
    for kind, scope, url in source_jobs:
        try:
            if kind == "cctv":
                results.extend(_cctv_news_search(scope, url, max_results))
            else:
                results.extend(_rss_news_search(scope, url, max_results))
        except Exception as exc:
            errors.append(f"{scope}:{exc}")

    filtered = [
        item for item in _dedupe_results(results)
        if not _result_has_junk(item, query)
    ]

    if wants_domestic and wants_world:
        domestic = [item for item in filtered if item.title.startswith("国内：")]
        world = [item for item in filtered if item.title.startswith("国际：")]
        other = [item for item in filtered if item not in domestic and item not in world]
        mixed: list[SearchResult] = []
        for index in range(max(len(domestic), len(world))):
            if index < len(domestic):
                mixed.append(domestic[index])
            if index < len(world):
                mixed.append(world[index])
            if len(mixed) >= max_results:
                break
        mixed.extend(other)
        return mixed[:max_results]

    if not filtered and errors:
        print("[NewsSearch] failed: " + " | ".join(errors))
    return filtered[:max_results]


def _search_bing_rss(query: str, max_results: int) -> list[SearchResult]:
    params = urllib.parse.urlencode({"q": query, "format": "rss", "setlang": "zh-CN"})
    xml_text = _read_url(f"https://www.bing.com/search?{params}", timeout=6)
    root = ET.fromstring(xml_text)

    results: list[SearchResult] = []
    for item in root.findall(".//item"):
        title = _clean_text(item.findtext("title") or "")
        link = _clean_text(item.findtext("link") or "")
        snippet = _clean_text(item.findtext("description") or "")
        if title and link:
            results.append(SearchResult(title=title, url=link, snippet=snippet))
        if len(results) >= max_results:
            break
    return results


def _decode_duckduckgo_url(url: str) -> str:
    parsed = urllib.parse.urlparse(html.unescape(url))
    query = urllib.parse.parse_qs(parsed.query)
    uddg = query.get("uddg")
    if uddg:
        return urllib.parse.unquote(uddg[0])
    if url.startswith("//"):
        return "https:" + url
    return url


def _search_duckduckgo_lite(query: str, max_results: int) -> list[SearchResult]:
    params = urllib.parse.urlencode({"q": query})
    html_text = _read_url(f"https://lite.duckduckgo.com/lite/?{params}", timeout=6)

    link_matches = re.findall(
        r'<a[^>]+class="result-link"[^>]+href="([^"]+)"[^>]*>(.*?)</a>',
        html_text,
        flags=re.I | re.S,
    )
    snippets = re.findall(
        r'<td[^>]+class="result-snippet"[^>]*>(.*?)</td>',
        html_text,
        flags=re.I | re.S,
    )

    results: list[SearchResult] = []
    for index, (url, title) in enumerate(link_matches[:max_results]):
        snippet = snippets[index] if index < len(snippets) else ""
        results.append(SearchResult(
            title=_clean_text(title),
            url=_decode_duckduckgo_url(url),
            snippet=_clean_text(snippet),
        ))
    return [item for item in results if item.title and item.url]


def _search_sync(query: str, max_results: int) -> list[SearchResult]:
    errors: list[str] = []
    try:
        priority_results = _gold_search(query)
    except Exception as exc:
        priority_results = []
        errors.append(f"_gold_search: {exc}")

    try:
        priority_results += _weather_search(query)
    except Exception as exc:
        errors.append(f"_weather_search: {exc}")

    try:
        priority_results += _news_search(query, max_results)
    except Exception as exc:
        errors.append(f"_news_search: {exc}")

    if priority_results:
        return _dedupe_results(priority_results)[:max_results]

    for searcher in (_search_bing_rss, _search_duckduckgo_lite):
        try:
            results = [
                item for item in searcher(query, max_results)
                if not _result_has_junk(item, query) and _result_is_relevant(item, query)
            ]
            if results:
                return _dedupe_results(results)[:max_results]
        except Exception as exc:
            errors.append(f"{searcher.__name__}: {exc}")
    print("[WebSearch] failed: " + " | ".join(errors))
    return []


async def search_web(query: str, max_results: int = 5) -> list[SearchResult]:
    """Run a web search without blocking the asyncio event loop."""
    return await asyncio.to_thread(_search_sync, query, max_results)


def format_search_results(query: str, results: list[SearchResult]) -> str:
    lines = [f"搜索词：{query}"]
    for index, item in enumerate(results, start=1):
        lines.append(f"{index}. {item.title}")
        if item.snippet:
            lines.append(f"摘要：{item.snippet}")
        lines.append(f"链接：{item.url}")
    return "\n".join(lines)


def direct_answer_from_results(query: str, results: list[SearchResult]) -> str | None:
    """不再硬编码解析，始终返回 None，让 LLM 自己读 format_search_results 输出做总结。"""
    return None
