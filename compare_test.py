"""
中转站 vs 官方 API 对比测试 —— 一次请求，两边同时发，直接对比

用法：
  python compare_test.py

会根据 .env 里的配置（DEEPSEEK_BASE_URL / DEEPSEEK_API_KEY / DEEPSEEK_MODEL）
同时向"你的中转站"和"官方 DeepSeek API"发送相同请求，逐项对比差异。

对比维度：
  - 返回的 model 名称是否一致
  - 回复内容长度、质量
  - 响应耗时
  - token 消耗
  - 回复内容是否相似（可能掺水/降级模型）
"""

import json
import os
import sys
import time
import asyncio
from dotenv import load_dotenv

# 加载当前目录的 .env
load_dotenv()

PROXY_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
PROXY_KEY = os.getenv("DEEPSEEK_API_KEY", "")
PROXY_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

# 官方 DeepSeek API（用作对照）
REAL_URL = "https://api.deepseek.com"

# 真实的 API Key（如果中转站用的是中转站的 key，这里填你真正的 deepseek key）
REAL_KEY = os.getenv("REAL_DEEPSEEK_KEY", PROXY_KEY)
REAL_MODEL = os.getenv("REAL_DEEPSEEK_MODEL", "deepseek-chat")

# ── 测试用 prompt ─────────────────────────────────────────────
# 用几个不同的 prompt 来测试，避免缓存干扰
TEST_PROMPTS = [
    # 1. 长文本理解 + 总结（测模型能力）
    """请用一句话总结以下段落的核心观点，并在最后用括号标注你是什么模型：

人工智能的发展经历了多次起伏。从1956年达特茅斯会议正式提出AI概念，到20世纪80年代的专家系统热潮，再到2012年深度学习在ImageNet上的突破，以及2022年ChatGPT引领的大语言模型浪潮。每一次技术突破都带来了新的应用场景和产业变革。当前，AI正在从"感知智能"向"认知智能"演进，多模态、具身智能、AI Agent成为新的研究方向。""",

    # 2. 简单问答（测是否偷换小模型）
    """你好，请告诉我：1+1等于几？另外请问你是什么模型？""",

    # 3. 逻辑推理（小模型容易翻车）
    """小明比小红大3岁，小红比小刚大2岁。5年后，小明比小刚大几岁？请一步步推理，并在最后说明你是什么模型。""",
]


async def test_endpoint(name: str, base_url: str, api_key: str, model: str, prompt: str) -> dict:
    """向指定端点发送请求，返回完整结果"""
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=api_key, base_url=base_url)
    t0 = time.time()

    try:
        resp = await client.chat.completions.create(
            model=model,
            messages=[
                {"role": "user", "content": prompt}
            ],
            max_tokens=512,
            temperature=0.7,
        )
        elapsed = time.time() - t0

        choice = resp.choices[0]
        return {
            "success": True,
            "model_requested": model,
            "model_returned": resp.model,
            "content": choice.message.content,
            "finish_reason": choice.finish_reason,
            "prompt_tokens": resp.usage.prompt_tokens if resp.usage else None,
            "completion_tokens": resp.usage.completion_tokens if resp.usage else None,
            "total_tokens": resp.usage.total_tokens if resp.usage else None,
            "elapsed": round(elapsed, 3),
            "error": None,
        }
    except Exception as e:
        elapsed = time.time() - t0
        return {
            "success": False,
            "model_requested": model,
            "model_returned": None,
            "content": None,
            "finish_reason": None,
            "prompt_tokens": None,
            "completion_tokens": None,
            "total_tokens": None,
            "elapsed": round(elapsed, 3),
            "error": str(e),
        }


def red(s): return f"\033[91m{s}\033[0m"
def green(s): return f"\033[92m{s}\033[0m"
def yellow(s): return f"\033[93m{s}\033[0m"
def cyan(s): return f"\033[96m{s}\033[0m"
def bold(s): return f"\033[1m{s}\033[0m"


async def main():
    print(f"""
{cyan('╔══════════════════════════════════════════════════════════╗')}
{cyan('║')}     {bold('🔬 中转站 vs 官方 API 对比测试')}                    {cyan('║')}
{cyan('╠══════════════════════════════════════════════════════════╣')}
{cyan('║')}  中转站 URL:  {yellow(PROXY_URL[:55])}
{cyan('║')}  官方 API:    {green(REAL_URL)}
{cyan('║')}  请求 Model:  {PROXY_MODEL}
{cyan('╚══════════════════════════════════════════════════════════╝')}
""")

    if PROXY_URL.rstrip("/") == REAL_URL.rstrip("/"):
        print(f" {yellow('⚠ 你的 DEEPSEEK_BASE_URL 就是官方 API，没有使用中转站。')}")
        print(f" {yellow('  如果你想测试中转站，请先修改 .env 里的 DEEPSEEK_BASE_URL。')}")
        print(f" {yellow('  不过我还是会跑一遍测试作为基准参考。')}")
        print()

    for i, prompt in enumerate(TEST_PROMPTS):
        print(f"\n{'═'*70}")
        print(f" {bold(f'[测试 {i+1}/{len(TEST_PROMPTS)}]')} {prompt[:60]}...")
        print(f"{'═'*70}")

        # 并发请求两边
        proxy_task = test_endpoint("中转站", PROXY_URL, PROXY_KEY, PROXY_MODEL, prompt)
        real_task = test_endpoint("官方API", REAL_URL, REAL_KEY, REAL_MODEL, prompt)

        proxy_result, real_result = await asyncio.gather(proxy_task, real_task)

        # ── 打印结果 ──────────────────────────────────────────
        for label, result, color in [
            ("中转站", proxy_result, yellow),
            ("官方API", real_result, green),
        ]:
            print(f"\n {color(f'── {label} ──')}")
            if not result["success"]:
                print(f"  {red(f'❌ 请求失败: {result[\"error\"]}')}")
                continue

            print(f"  请求 Model:  {result['model_requested']}")
            print(f"  返回 Model:  {color(result['model_returned'])}")
            print(f"  耗时:        {result['elapsed']}s")
            print(f"  Token 用量:  prompt={result['prompt_tokens']}, "
                  f"completion={result['completion_tokens']}, "
                  f"total={result['total_tokens']}")
            print(f"  结束原因:    {result['finish_reason']}")
            content = result['content'] or ""
            if len(content) > 200:
                content = content[:200] + "..."
            print(f"  回复内容:    {content}")

        # ── 对比分析 ──────────────────────────────────────────
        if proxy_result["success"] and real_result["success"]:
            print(f"\n {bold('📊 对比分析:')}")

            issues = []

            # 1. Model 名称检查
            pm = proxy_result["model_returned"]
            rm = real_result["model_returned"]
            if pm and rm and pm != rm:
                issues.append(red(f"  ⚠ Model 不一致: 中转站返回 '{pm}' vs 官方 '{rm}'"))
            elif pm and pm != PROXY_MODEL:
                issues.append(red(f"  ⚠ 中转站返回 model='{pm}'，但你请求的是 '{PROXY_MODEL}'"))

            # 2. 耗时对比
            pt = proxy_result["elapsed"]
            rt = real_result["elapsed"]
            if pt < rt * 0.3:
                issues.append(red(f"  ⚡ 中转站太快 ({pt}s vs {rt}s)，疑似缓存/预生成回复"))

            # 3. Token 对比
            if proxy_result["completion_tokens"] and real_result["completion_tokens"]:
                ratio = proxy_result["completion_tokens"] / max(real_result["completion_tokens"], 1)
                if ratio < 0.5:
                    issues.append(yellow(f"  📏 中转站回复明显更短 (completion tokens: {proxy_result['completion_tokens']} vs {real_result['completion_tokens']})"))
                elif ratio > 3:
                    issues.append(yellow(f"  📏 中转站回复明显更长 (completion tokens: {proxy_result['completion_tokens']} vs {real_result['completion_tokens']})"))

            # 4. 内容相似度（简单版本）
            p_content = proxy_result["content"] or ""
            r_content = real_result["content"] or ""
            if p_content and r_content:
                # 检查是否几乎一样（可能都是正确的，但也说明中转站在转发）
                p_words = set(p_content)
                r_words = set(r_content)
                if p_words and r_words:
                    overlap = len(p_words & r_words) / max(len(p_words | r_words), 1)
                    if overlap < 0.1:
                        issues.append(yellow(f"  🔀 回复内容差异极大 (相似度 {overlap*100:.0f}%)，可能是不同模型"))

            if not issues:
                print(f" {green('  ✓ 未发现明显异常')}")
            else:
                for issue in issues:
                    print(issue)

    # ── 总结 ──────────────────────────────────────────────────
    print(f"\n\n{'═'*70}")
    print(f" {bold('🏁 测试完毕')}")
    print(f"{'═'*70}")
    print(f"  如果发现以下信号，说明中转站可能掺水：")
    print(f"  1. 返回的 model 名称与你请求的不一致")
    print(f"  2. 回复速度异常快（< 0.5s），可能是缓存或本地小模型")
    print(f"  3. 回复质量明显差于官方 API（逻辑错误、答非所问）")
    print(f"  4. Token 消耗与官方差异悬殊")
    print(f"")
    print(f"  更彻底的检查方法：使用 api_sniffer.py 代理抓包")
    print(f"  可以抓到每一笔请求的完整 HTTP 头和 body")
    print()


if __name__ == "__main__":
    asyncio.run(main())
