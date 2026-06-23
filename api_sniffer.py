"""
API 抓包代理 —— 截获并记录所有经过的 OpenAI-兼容 API 请求/响应

用途：
  把 DEEPSEEK_BASE_URL 指向本代理（如 http://127.0.0.1:9000），
  本代理会原封不动转发到真实上游，同时记录一切细节。

用法：
  1. 先在本机启动：python api_sniffer.py
  2. 修改 .env：DEEPSEEK_BASE_URL=http://127.0.0.1:9000
  3. 重启服务端，正常使用
  4. 所有请求/响应会被打印 + 写入 logs/sniff_*.jsonl

检查重点（判断商家是否"掺水"）：
  - 请求里的 model vs 响应里的 model —— 是否被替换
  - 响应头里是否有上游代理痕迹（如 x-proxy-by, via, server）
  - 响应耗时 —— 中转站是否在"背答案"（秒回）还是真的调了模型
  - response body 里的 model 字段 —— 返回的是不是你花钱买的模型
"""

import json
import sys
import time
import os
import re
import urllib.request
import urllib.error
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone, timedelta

# ── 配置 ──────────────────────────────────────────────────────
LISTEN_HOST = os.getenv("SNIFF_HOST", "127.0.0.1")
LISTEN_PORT = int(os.getenv("SNIFF_PORT", "9000"))

# 上游真实 API 地址（你要查的中转站地址）
UPSTREAM_BASE = os.getenv(
    "SNIFF_UPSTREAM",
    os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
)

# 日志目录
LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"sniff_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl")

# 被屏蔽的敏感 header 值（打印时替换）
MASK_HEADERS = {"authorization", "api-key", "x-api-key", "cookie", "set-cookie"}

# 是否打印完整 response body（大型响应可能刷屏）
PRINT_FULL_BODY = os.getenv("SNIFF_FULL_BODY", "1") == "1"

# ── 统计 ──────────────────────────────────────────────────────
stats = {
    "total_requests": 0,
    "total_errors": 0,
    "models_seen": {},      # 请求中的 model → 次数
    "resp_models_seen": {}, # 响应中的 model → 次数
    "model_mismatches": [], # model 不一致的记录
    "started_at": time.time(),
}

# ── 工具函数 ──────────────────────────────────────────────────
TZ_CN = timezone(timedelta(hours=8))

def ts() -> str:
    return datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S")

def mask_headers(headers: dict) -> dict:
    return {
        k: ("***" if k.lower() in MASK_HEADERS else v)
        for k, v in headers.items()
    }

def safe_json(text: str) -> dict | None:
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None

def red(text: str) -> str:
    """终端红色高亮"""
    return f"\033[91m{text}\033[0m"

def green(text: str) -> str:
    return f"\033[92m{text}\033[0m"

def yellow(text: str) -> str:
    return f"\033[93m{text}\033[0m"

def cyan(text: str) -> str:
    return f"\033[96m{text}\033[0m"

# ── 核心：请求处理器 ──────────────────────────────────────────
class SniffHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        self._forward("GET")

    def do_POST(self):
        self._forward("POST")

    def do_PUT(self):
        self._forward("PUT")

    def do_DELETE(self):
        self._forward("DELETE")

    def do_PATCH(self):
        self._forward("PATCH")

    def _forward(self, method: str):
        stats["total_requests"] += 1
        req_id = stats["total_requests"]
        upstream_url = UPSTREAM_BASE.rstrip("/") + self.path
        t_start = time.time()

        # ── 读取请求体 ────────────────────────────────────
        content_length = int(self.headers.get("Content-Length", 0))
        req_body_bytes = self.rfile.read(content_length) if content_length > 0 else b""
        req_body_str = req_body_bytes.decode("utf-8", errors="replace")
        req_json = safe_json(req_body_str)

        # ── 提取请求中的 model ────────────────────────────
        req_model = req_json.get("model", "unknown") if req_json else "unknown"
        stats["models_seen"][req_model] = stats["models_seen"].get(req_model, 0) + 1

        # ── 打印请求 ──────────────────────────────────────
        print(f"\n{'═'*70}")
        print(f" {cyan(f'[REQ #{req_id}]')} {method} {self.path}")
        print(f" {cyan('Target:')}    {upstream_url}")
        print(f" {cyan('Model:')}     {yellow(req_model)}")
        print(f" {cyan('Time:')}      {ts()}")
        print(f" {cyan('Headers:')}")
        for k, v in mask_headers(dict(self.headers)).items():
            print(f"    {k}: {v}")
        if req_json and PRINT_FULL_BODY:
            body_preview = json.dumps(req_json, ensure_ascii=False, indent=2)
            if len(body_preview) > 3000:
                body_preview = body_preview[:3000] + f"\n... [截断, 共 {len(body_preview)} 字符]"
            print(f" {cyan('Body:')}")
            for line in body_preview.split("\n"):
                print(f"    {line}")

        # ── 转发到上游 ────────────────────────────────────
        try:
            upstream_req = urllib.request.Request(
                upstream_url,
                data=req_body_bytes,
                headers={k: v for k, v in self.headers.items()
                         if k.lower() not in ("host", "content-length")},
                method=method,
            )
            upstream_req.add_header("Host", urllib.parse.urlparse(upstream_url).netloc)

            resp = urllib.request.urlopen(upstream_req, timeout=120)
            resp_body_bytes = resp.read()
            resp_body_str = resp_body_bytes.decode("utf-8", errors="replace")
            resp_headers = dict(resp.headers)
            elapsed = time.time() - t_start

        except urllib.error.HTTPError as e:
            resp_body_bytes = e.read()
            resp_body_str = resp_body_bytes.decode("utf-8", errors="replace")
            resp_headers = dict(e.headers)
            elapsed = time.time() - t_start
            stats["total_errors"] += 1

        except Exception as e:
            elapsed = time.time() - t_start
            stats["total_errors"] += 1
            error_msg = json.dumps({"error": str(e)}, ensure_ascii=False)
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(error_msg.encode("utf-8"))
            print(f" {red(f'[ERROR] {e}')}")
            return

        # ── 分析响应 ──────────────────────────────────────
        resp_json = safe_json(resp_body_str)
        resp_model = None
        if resp_json:
            # OpenAI 格式: {"model": "...", "choices": [...]}
            resp_model = resp_json.get("model")
            if not resp_model:
                # 可能是 chunk 或其他格式
                pass

        if resp_model:
            stats["resp_models_seen"][resp_model] = stats["resp_models_seen"].get(resp_model, 0) + 1

        # 🔴 关键检查：请求 model vs 响应 model
        model_match = (resp_model is None) or (resp_model == req_model)
        mismatch_warning = ""
        if not model_match:
            mismatch_warning = red(f" ⚠ MODEL MISMATCH! req={req_model} → resp={resp_model}")
            stats["model_mismatches"].append({
                "req_id": req_id,
                "time": ts(),
                "requested": req_model,
                "returned": resp_model,
                "url": upstream_url,
            })

        # ── 检查上游代理痕迹 ──────────────────────────────
        proxy_headers = {}
        for h in resp_headers:
            if any(kw in h.lower() for kw in
                   ("x-proxy", "via", "x-forwarded", "x-real", "x-upstream",
                    "x-cache", "cf-cache", "x-served-by", "server", "x-runtime",
                    "x-request-id", "x-trace", "x-amzn", "x-envoy")):
                proxy_headers[h] = resp_headers[h]

        # ── 打印响应 ──────────────────────────────────────
        status_code = (resp.getcode() if 'resp' in dir() and hasattr(resp, 'getcode')
                       else getattr(resp, 'status', getattr(resp, 'code', 0)))
        print(f" {cyan('Status:')}    {green(str(status_code)) if status_code and int(status_code) < 400 else red(str(status_code))}")
        print(f" {cyan('Resp Model:')}{yellow(resp_model or 'N/A')}{mismatch_warning}")
        print(f" {cyan('Elapsed:')}   {elapsed:.2f}s")

        if proxy_headers:
            print(f" {cyan('Proxy Hints:')}")
            for k, v in proxy_headers.items():
                print(f"    {k}: {v}")

        if resp_json and PRINT_FULL_BODY:
            body_preview = json.dumps(resp_json, ensure_ascii=False, indent=2)
            if len(body_preview) > 3000:
                body_preview = body_preview[:3000] + f"\n... [截断, 共 {len(body_preview)} 字符]"
            print(f" {cyan('Resp Body:')}")
            for line in body_preview.split("\n"):
                print(f"    {line}")

        # ── 检查可疑信号 ──────────────────────────────────
        warnings = []
        # 1. 响应太快（< 0.5s）可能是缓存/预生成
        if elapsed < 0.5 and resp_json and resp_json.get("choices"):
            warnings.append(f"⚡ 响应极快 ({elapsed:.2f}s)，可能是缓存/预生成回复")
        # 2. 响应 model 与请求 model 不同
        if not model_match:
            warnings.append(f"🎭 Model 被替换: {req_model} → {resp_model}")
        # 3. 没有返回 model 字段
        if resp_json and not resp_model and "choices" in resp_json:
            warnings.append("❓ 响应未声明 model 字段（异常）")
        # 4. 响应体异常小
        if resp_json and resp_json.get("choices") and len(resp_body_str) < 100:
            warnings.append(f"📏 响应体异常短小 ({len(resp_body_str)} bytes)")

        for w in warnings:
            print(f" {red(w)}")

        # ── 写入日志文件 ──────────────────────────────────
        log_entry = {
            "req_id": req_id,
            "time": ts(),
            "method": method,
            "path": self.path,
            "upstream": upstream_url,
            "req_model": req_model,
            "req_headers": mask_headers(dict(self.headers)),
            "req_body": req_json,
            "resp_status": status_code,
            "resp_model": resp_model,
            "resp_headers": mask_headers(resp_headers),
            "resp_body": resp_json if PRINT_FULL_BODY else f"[{len(resp_body_str)} bytes]",
            "elapsed": round(elapsed, 3),
            "warnings": warnings,
            "proxy_headers": proxy_headers,
        }
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

        # ── 转发响应给客户端 ──────────────────────────────
        self.send_response(status_code or 200)
        for k, v in resp_headers.items():
            if k.lower() not in ("transfer-encoding", "content-encoding", "content-length"):
                self.send_header(k, v)
        self.send_header("Content-Length", str(len(resp_body_bytes)))
        self.end_headers()
        self.wfile.write(resp_body_bytes)

    def log_message(self, format, *args):
        """抑制默认的 http.server 日志（我们有自定义日志）"""
        pass


# ── 启动 ──────────────────────────────────────────────────────
def main():
    print(f"""
{cyan('╔══════════════════════════════════════════════════════╗')}
{cyan('║')}       {yellow('🔍 API Sniffer - 中转站抓包分析工具')}         {cyan('║')}
{cyan('╠══════════════════════════════════════════════════════╣')}
{cyan('║')}  {green('监听地址:')} {LISTEN_HOST}:{LISTEN_PORT}                              {cyan('║')}
{cyan('║')}  {green('上游地址:')} {UPSTREAM_BASE}  {cyan('║')}
{cyan('║')}  {green('日志文件:')} {LOG_FILE}{cyan('║')}
{cyan('╚══════════════════════════════════════════════════════╝')}
""")

    print(f" {yellow('使用方法:')}")
    print(f"   1. 修改 .env: DEEPSEEK_BASE_URL=http://127.0.0.1:{LISTEN_PORT}")
    print(f"   2. 重启你的服务端 (main.py)")
    print(f"   3. 正常使用，所有 API 请求会被截获并记录")
    print(f"   4. 按 Ctrl+C 停止抓包，查看统计摘要")
    print(f"")

    server = HTTPServer((LISTEN_HOST, LISTEN_PORT), SniffHandler)
    print(f" {green('[✓]')} 代理已启动，等待请求...\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n\n{'═'*70}")
        print(f" {yellow('📊 统计摘要')}")
        print(f"{'═'*70}")
        runtime = time.time() - stats["started_at"]
        print(f"  运行时间:     {runtime:.0f}s")
        print(f"  总请求数:     {stats['total_requests']}")
        print(f"  错误数:       {stats['total_errors']}")
        print(f"  请求 models:  {json.dumps(stats['models_seen'], ensure_ascii=False)}")
        print(f"  响应 models:  {json.dumps(stats['resp_models_seen'], ensure_ascii=False)}")
        print(f"  Model 不一致: {len(stats['model_mismatches'])} 次")
        for m in stats["model_mismatches"]:
            print(f"    {red(f'{m[\"requested\"]} → {m[\"returned\"]}')} at {m['time']}")
        print(f"\n  详细日志已保存到: {LOG_FILE}")
        print(f"{'═'*70}\n")
        server.shutdown()


if __name__ == "__main__":
    main()
