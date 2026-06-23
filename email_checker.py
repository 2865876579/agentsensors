"""
邮箱检查器 —— 服务端 IMAP 拉取邮件摘要

架构：
  LLM 调 check_emails 工具 → 本模块 IMAP 拉取 → 返回摘要 → LLM 组织语言播报

使用 QQ 邮箱 IMAP，不需要 PC Agent / 浏览器 / Outlook。
"""
from __future__ import annotations

import email
import imaplib
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from email.header import decode_header, make_header

from config import EMAIL_HOST, EMAIL_USER, EMAIL_PASS


@dataclass
class EmailSummary:
    sender: str
    subject: str
    body_preview: str  # 前 200 字
    date_str: str
    has_attachment: bool


def _decode_mime(text: str | bytes | None) -> str:
    """解码 MIME 编码的邮件头（=?UTF-8?B?...?= 等）。"""
    if not text:
        return ""
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    try:
        from email.header import make_header
        return str(make_header(decode_header(text)))
    except Exception:
        return text


def _extract_body(msg: email.message.Message) -> str:
    """提取邮件正文（纯文本优先，降级到 HTML 摘取）。"""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    body = payload.decode("utf-8", errors="replace")
                    # 去掉过长引用
                    body = re.sub(r"^>.*$", "", body, flags=re.MULTILINE)
                    body = re.sub(r"\n{3,}", "\n\n", body)
                    return body.strip()
        # 无纯文本 → 摘 HTML
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    html = payload.decode("utf-8", errors="replace")
                    # 简单去标签
                    text = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL | re.I)
                    text = re.sub(r"<[^>]+>", " ", text)
                    text = re.sub(r"\s+", " ", text)
                    return text.strip()[:500]
        return ""
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            return payload.decode("utf-8", errors="replace").strip()
        return ""


def _preview(text: str, max_chars: int = 200) -> str:
    """截取前 N 字的可读预览。"""
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "..."


def check_emails_by_date(target_date: date | None = None, max_count: int = 10) -> list[EmailSummary]:
    """
    拉取指定日期的邮件摘要。

    target_date: None=今天, date(2026,6,19)=指定日
    max_count: 最多返回 N 封
    """
    if target_date is None:
        target_date = date.today()

    if not EMAIL_USER or not EMAIL_PASS:
        raise RuntimeError("邮箱未配置，请在 .env 中设置 EMAIL_USER 和 EMAIL_PASS")

    results: list[EmailSummary] = []

    # IMAP 连接（SSL）
    imap = imaplib.IMAP4_SSL(EMAIL_HOST, timeout=10)
    try:
        imap.login(EMAIL_USER, EMAIL_PASS)

        # ★ 163 要求客户端自报身份，否则 select 被拒
        tag = imap._new_tag()
        imap.send(tag + b' ID ("name" "SmartPillow" "version" "1.0" "os" "linux")\r\n')
        imap._get_tagged_response(tag, "ID")

        # 选择收件箱
        status, _ = imap.select("INBOX", readonly=True)
        if status != "OK":
            imap.select("INBOX", readonly=True)

        # 搜索全部，客户端过滤日期
        status, msg_ids = imap.search(None, "ALL")
        if status != "OK" or not msg_ids[0]:
            return results

        ids = msg_ids[0].split()
        ids = ids[-max_count * 3:]

        for msg_id in reversed(ids):
            status, data = imap.fetch(msg_id, "(RFC822)")
            if status != "OK":
                continue

            raw = data[0][1]
            msg = email.message_from_bytes(raw)

            sender = _decode_mime(msg.get("From", ""))
            subject = _decode_mime(msg.get("Subject", ""))

            # ★ 日期过滤
            date_str = msg.get("Date", "")
            try:
                from email.utils import parsedate_to_datetime
                mail_dt = parsedate_to_datetime(date_str)
                if mail_dt.date() != target_date:
                    continue
            except Exception:
                continue

            # 附件
            has_attachment = False
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get("Content-Disposition") and "attachment" in part.get("Content-Disposition"):
                        has_attachment = True
                        break

            # 正文
            body = _extract_body(msg)

            results.append(EmailSummary(
                sender=sender,
                subject=subject,
                body_preview=_preview(body),
                date_str=date_str,
                has_attachment=has_attachment,
            ))

            if len(results) >= max_count:
                break

    finally:
        try:
            imap.close()
            imap.logout()
        except Exception:
            pass

    return results


def parse_date_str(text: str) -> date | None:
    """
    把 LLM 传来的日期字符串转为 date 对象。
    支持: "today"/"今天", "yesterday"/"昨天", "2026-06-19"
    """
    text = text.strip().lower()
    if text in ("today", "今天", ""):
        return date.today()
    if text in ("yesterday", "昨天"):
        return date.today() - timedelta(days=1)
    # YYYY-MM-DD
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        pass
    # YYYY/MM/DD
    try:
        return datetime.strptime(text, "%Y/%m/%d").date()
    except ValueError:
        pass
    return None


def format_email_summary(emails: list[EmailSummary], target_date: date | None = None) -> str:
    """把邮件摘要列表格式化为给 LLM 的自然语言文本。"""
    if not emails:
        label = f"{target_date}没有新邮件。" if target_date else "没有新邮件。"
        return label

    if target_date is None:
        label = "今天"
    elif target_date == date.today():
        label = "今天"
    elif target_date == date.today() - timedelta(days=1):
        label = "昨天"
    else:
        label = str(target_date)
    lines = [f"{label}共 {len(emails)} 封邮件："]
    for i, em in enumerate(emails, 1):
        sender_short = em.sender.split("<")[0].strip().rstrip()
        att = " [有附件]" if em.has_attachment else ""
        lines.append(
            f"{i}. 发件人：{sender_short}，主题：{em.subject}{att}，"
            f"内容摘要：{em.body_preview}"
        )
    return "\n".join(lines)
