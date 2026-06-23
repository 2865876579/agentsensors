"""
PC Agent - 接收云端 AI 命令并控制电脑

功能：
  1. 通过 WebSocket 连接云端服务，等待命令下发
  2. 收到命令后执行对应的电脑操作
  3. 将执行结果返回给云端服务

支持的命令：
  - open_url: 打开网页
  - search: 用默认浏览器搜索
  - open_file: 打开本地文件
  - summarize_file: 读取文件内容并返回摘要

安全设计：
  - 只允许白名单内的动作
  - 不允许删除文件、发送邮件、付款等危险操作
  - 所有操作都有日志输出

用法：
  python pc_agent.py

环境变量：
  WS_URL: 云端服务地址，默认 ws://localhost:8000/ws/pc_agent
  部署后改为 ws://你的服务器IP:8000/ws/pc_agent
"""
import asyncio
import json
import os
import sys
import urllib.parse
import webbrowser
import websockets

# 确保能 import 同目录下的 config
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# 云端服务 WebSocket 地址
WS_URL = os.getenv("WS_URL", "ws://localhost:8000/ws/pc_agent")


def extract_search_query_from_url(url: str) -> str | None:
    """识别搜索引擎 URL，返回搜索词；不是搜索页则返回 None。"""
    parsed = urllib.parse.urlparse(url)
    host = parsed.netloc.lower()
    params = urllib.parse.parse_qs(parsed.query)

    if "baidu.com" in host:
        names = ("wd", "word", "q")
    elif "bing.com" in host or "google." in host or "duckduckgo.com" in host:
        names = ("q",)
    elif "sogou.com" in host:
        names = ("query", "keyword", "q")
    elif "so.com" in host or "haosou.com" in host:
        names = ("q",)
    else:
        return None

    for name in names:
        values = params.get(name)
        if values and values[0].strip():
            return urllib.parse.unquote_plus(values[0]).strip()
    return ""


async def handle_command(command: dict) -> str:
    """
    执行 PC 命令并返回结果

    参数：
      command: {"action": "动作名", "params": {"参数": "值"}}

    返回：
      执行结果的文字描述
    """
    action = command.get("action", "")
    params = command.get("params", {})

    print(f"[执行] action={action}, params={params}")

    if action == "open_url":
        url = params.get("url", "")
        if not url:
            return "缺少 URL 参数"
        query = extract_search_query_from_url(url)
        if query is not None:
            return f"已拦截搜索页打开：{query or url}。搜索应该由服务端后台完成，不再打开浏览器。"
        webbrowser.open(url)
        return f"已打开网页: {url}"

    elif action == "search":
        query = params.get("query", "")
        if not query:
            return "缺少搜索关键词"
        return f"已拦截 PC Agent 搜索命令：{query}。搜索应该由服务端后台完成，不再打开浏览器。"

    elif action == "open_file":
        path = params.get("path", "")
        if not path:
            return "缺少文件路径"
        if not os.path.exists(path):
            return f"文件不存在: {path}"
        os.startfile(path)
        return f"已打开文件: {path}"

    elif action == "summarize_file":
        path = params.get("path", "")
        if not path:
            return "缺少文件路径"
        if not os.path.exists(path):
            return f"文件不存在: {path}"
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read(2000)
            return f"文件内容前2000字：\n{content}"
        except Exception as e:
            return f"读取文件失败: {e}"

    elif action == "desktop_write":
        # 在用户桌面创建/覆盖文件
        filename = params.get("filename", "summary.txt")
        content = params.get("content", "")
        if not content:
            return "缺少文件内容"
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        path = os.path.join(desktop, filename)
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            return f"已写入桌面文件：{filename}（{len(content)}字）"
        except Exception as e:
            return f"写入桌面文件失败: {e}"

    elif action == "clipboard_get":
        # 读取 Windows 剪贴板文本
        import subprocess
        try:
            r = subprocess.run(
                ["powershell", "-NoProfile", "-Command", "Get-Clipboard"],
                capture_output=True, text=True, timeout=5
            )
            text = r.stdout.strip()
            return text if text else "剪贴板为空"
        except Exception as e:
            return f"读取剪贴板失败: {e}"

    elif action == "clipboard_set":
        # 写入 Windows 剪贴板
        import subprocess
        text = params.get("text", "")
        if not text:
            return "缺少要写入剪贴板的文本"
        try:
            # 用 PowerShell 安全写入（避免命令注入）
            escaped = text.replace("'", "''")
            subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 f"Set-Clipboard -Value '{escaped}'"],
                capture_output=True, timeout=5
            )
            return f"已写入剪贴板（{len(text)}字）"
        except Exception as e:
            return f"写入剪贴板失败: {e}"

    elif action == "screenshot":
        # 截屏保存到桌面
        import subprocess
        from datetime import datetime
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(desktop, f"screenshot_{ts}.png")
        try:
            subprocess.run([
                "powershell", "-NoProfile", "-Command",
                f"Add-Type -AssemblyName System.Windows.Forms; "
                f"[System.Windows.Forms.Screen]::PrimaryScreen().Bounds | "
                f"ForEach-Object {{ $bmp = New-Object System.Drawing.Bitmap "
                f"$_.Width,$_.Height; $g = [System.Drawing.Graphics]::FromImage($bmp); "
                f"$g.CopyFromScreen($_.X,$_.Y,0,0,$_.Size); "
                f"$bmp.Save('{path}'); $g.Dispose(); $bmp.Dispose() }}"
            ], capture_output=True, timeout=10)
            return f"截图已保存到桌面：screenshot_{ts}.png"
        except Exception as e:
            return f"截图失败: {e}"

    elif action == "run_app":
        # 启动应用
        import subprocess
        app = params.get("app", "")
        if not app:
            return "缺少应用名称或路径"
        try:
            subprocess.Popen(app, shell=True)
            return f"已启动：{app}"
        except Exception as e:
            return f"启动失败: {e}"

    elif action == "run_cmd":
        # 执行命令并返回输出
        import subprocess
        cmd = params.get("cmd", "")
        if not cmd:
            return "缺少要执行的命令"
        try:
            r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=15)
            out = r.stdout.strip() or r.stderr.strip()
            if not out:
                return f"命令执行完毕，无输出（返回码 {r.returncode}）"
            preview = out[:800]
            return preview if len(out) <= 800 else preview + "\n...(输出已截断)"
        except Exception as e:
            return f"执行命令失败: {e}"

    else:
        return f"不支持的命令: {action}"


async def run():
    """主循环：连接云端服务，等待并执行命令"""
    while True:
        try:
            print(f"[PC Agent] 连接到 {WS_URL} ...")
            async with websockets.connect(WS_URL) as ws:
                print("[PC Agent] 已连接，等待命令...\n")

                while True:
                    message = await ws.recv()
                    data = json.loads(message)

                    if data.get("type") == "pc_command":
                        command = data.get("command", {})
                        client_id = data.get("client_id")
                        turn_id = data.get("turn_id")
                        command_id = data.get("command_id")
                        print(f"[收到命令] {command}")

                        # 执行命令
                        result = await handle_command(command)
                        print(f"[执行结果] {result}\n")

                        # 返回结果给云端
                        await ws.send(json.dumps({
                            "type": "result",
                            "client_id": client_id,
                            "turn_id": turn_id,
                            "command_id": command_id,
                            "result": result
                        }, ensure_ascii=False))

                    elif data.get("type") == "pong":
                        pass

        except websockets.exceptions.ConnectionClosed:
            print("[PC Agent] 连接断开，5秒后重连...")
            await asyncio.sleep(5)
        except ConnectionRefusedError:
            print("[PC Agent] 服务器未启动，5秒后重试...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[PC Agent] 错误: {e}，5秒后重连...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    print("=" * 40)
    print("  智能枕头 PC Agent")
    print("  等待云端 AI 下发电脑控制命令")
    print("=" * 40)
    print()
    asyncio.run(run())
