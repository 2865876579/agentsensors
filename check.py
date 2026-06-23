"""快速诊断脚本 - 逐步检查依赖和模块是否正常"""

print("=" * 40)
print("开始检查...")
print("=" * 40)

# 检查依赖
try:
    import fastapi
    print("[OK] fastapi")
except ImportError as e:
    print(f"[FAIL] fastapi: {e}")

try:
    import uvicorn
    print("[OK] uvicorn")
except ImportError as e:
    print(f"[FAIL] uvicorn: {e}")

try:
    import websockets
    print("[OK] websockets")
except ImportError as e:
    print(f"[FAIL] websockets: {e}")

try:
    import edge_tts
    print("[OK] edge_tts")
except ImportError as e:
    print(f"[FAIL] edge_tts: {e}")

try:
    import openai
    print("[OK] openai")
except ImportError as e:
    print(f"[FAIL] openai: {e}")

try:
    import dotenv
    print("[OK] python-dotenv")
except ImportError as e:
    print(f"[FAIL] python-dotenv: {e}")

print()
print("检查模块导入...")

try:
    from config import DEEPSEEK_API_KEY, SERVER_PORT
    print(f"[OK] config - DEEPSEEK_API_KEY={'已设置' if DEEPSEEK_API_KEY else '未设置'}, PORT={SERVER_PORT}")
except Exception as e:
    print(f"[FAIL] config: {e}")

try:
    from tts_edge import synthesize
    print("[OK] tts_edge")
except Exception as e:
    print(f"[FAIL] tts_edge: {e}")

try:
    from llm_deepseek import chat
    print("[OK] llm_deepseek")
except Exception as e:
    print(f"[FAIL] llm_deepseek: {e}")

try:
    from stt_xunfei import recognize
    print("[OK] stt_xunfei")
except Exception as e:
    print(f"[FAIL] stt_xunfei: {e}")

try:
    from web_search import search_web
    print("[OK] web_search")
except Exception as e:
    print(f"[FAIL] web_search: {e}")

print()
print("=" * 40)
print("如果上面全是 [OK]，运行: python main.py")
print("=" * 40)
