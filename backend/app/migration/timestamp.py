"""event_timestamp 文本 → Glue TIMESTAMP 的转换引擎。

Hive 侧 event_timestamp 是 STRING，历史文本格式混杂：
  - 带时区偏移：2026-11-23 08:15:30.123456+08:00
  - 带 Z 偏移：  2026-11-23T00:15:30Z
  - 亚秒精度：   毫秒(3位)/微秒(6位)/纳秒(9位)
  - 空值：       NULL / 空串
Glue 侧 event_timestamp 是 TIMESTAMP。Glue/Spark 的 TIMESTAMP 语义为
TIMESTAMP WITH LOCAL TIME ZONE：会话时区归一化存储，亚秒保留到微秒，
纳秒位按微秒截断（Spark timestamp 微秒粒度），这里显式记录截断而不是静默丢弃。

设计原则：
  - 空值保留为 SQL NULL，绝不转成字符串 "null" 或 epoch；
  - 无法转换的行不做猜测、不吞掉，返回错误原因交由上层隔离；
  - 转换是纯函数，同一条文本永远得到同一个结论，保证回切可复核。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

NULL_TOKENS = frozenset({"", "null", "none", "\\n", "nan"})

# Glue/Spark 会话时区；TIMESTAMP 值按此时区归一化。
GLUE_SESSION_TZ = "UTC"
# Spark timestamp 最大支持微秒（6 位小数），纳秒位截断并显式留痕。
MAX_FRACTION_DIGITS = 6


@dataclass
class ParsedTimestamp:
    ok: bool
    iso_utc: str | None = None          # 归一化到 UTC 的 ISO 文本（新类型解析结果）
    raw: str | None = None              # 旧文本表现（原样保留）
    error: str | None = None
    is_null: bool = False
    truncated_nanos: bool = False       # 是否发生纳秒→微秒截断
    offset: str | None = None           # 从旧文本中识别出的时区偏移
    fraction_digits: int = 0


def _build_iso(dt: datetime, fraction_digits: int) -> str:
    """输出 TIMESTAMP 的规范化 ISO 文本（UTC，Z 结尾，保留实际精度）。

    毫秒输入保留 3 位、微秒保留 6 位，不做补零；纳秒已在解析阶段截断到微秒。
    """
    if dt.microsecond == 0 or fraction_digits == 0:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    digits = min(fraction_digits, MAX_FRACTION_DIGITS)
    frac = f"{dt.microsecond:06d}"[:digits]
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{frac}Z"


def parse_event_timestamp(raw: str | None) -> ParsedTimestamp:
    """把 Hive STRING 文本按 Glue TIMESTAMP 语义解析。

    返回 ParsedTimestamp；调用方据此区分可转换行、空值行、不可转换行。
    """
    original = raw
    if raw is None:
        return ParsedTimestamp(ok=True, iso_utc=None, raw=None, is_null=True)

    text = raw.strip()
    if text.casefold() in NULL_TOKENS:
        # 空值：保留 NULL 语义
        return ParsedTimestamp(ok=True, iso_utc=None, raw=original, is_null=True)

    offset = None
    truncated = False
    fraction_digits = 0
    candidate = text

    # 归一化分隔符：ISO 的 T 与 Hive 常用空格都接受
    if "T" in candidate:
        normalized = candidate
    else:
        normalized = candidate.replace(" ", "T", 1)

    # 识别时区偏移（Z 或 ±HH:MM / ±HHMM）
    if normalized.endswith("Z"):
        offset = "Z"
        dt_part = normalized[:-1]
        tz = timezone.utc
    else:
        tz = None
        dt_part = normalized
        for idx in (len(normalized) - 6, len(normalized) - 5):
            if idx > 0 and normalized[idx] in "+-":
                off_text = normalized[idx:]
                sign = 1 if off_text[0] == "+" else -1
                digits = off_text[1:].replace(":", "")
                if digits.isdigit() and len(digits) == 4:
                    hours, minutes = int(digits[:2]), int(digits[2:])
                    from datetime import timedelta
                    tz = timezone(sign * timedelta(hours=hours, minutes=minutes))
                    offset = off_text
                    dt_part = normalized[:idx]
                    break

    # 拆分亚秒精度
    if "." in dt_part:
        head, frac = dt_part.split(".", 1)
        fraction_digits = len(frac)
        if fraction_digits > MAX_FRACTION_DIGITS:
            # 纳秒(9位)超出 Spark 微秒粒度：截断并留痕，不四舍五入
            frac = frac[:MAX_FRACTION_DIGITS]
            truncated = True
        dt_part = f"{head}.{frac}"
    else:
        head = dt_part

    try:
        if tz is None:
            # 不带偏移的文本按会话时区(UTC)解释，同时标记偏移缺失
            dt = datetime.strptime(dt_part, "%Y-%m-%dT%H:%M:%S.%f" if "." in dt_part
                                   else "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            offset = offset or "(none->UTC)"
        else:
            dt = datetime.strptime(dt_part, "%Y-%m-%dT%H:%M:%S.%f" if "." in dt_part
                                   else "%Y-%m-%dT%H:%M:%S").replace(tzinfo=tz)
    except ValueError as exc:
        return ParsedTimestamp(ok=False, raw=original, error=f"unparseable timestamp: {exc}")

    dt_utc = dt.astimezone(timezone.utc)
    iso = _build_iso(dt_utc, fraction_digits)
    return ParsedTimestamp(
        ok=True, iso_utc=iso, raw=original, is_null=False,
        truncated_nanos=truncated, offset=offset, fraction_digits=fraction_digits,
    )


def render_legacy_text(raw: str | None) -> str | None:
    """旧报表读取契约：原样返回 Hive 文本，空值仍为 None（SQL NULL）。"""
    if raw is None:
        return None
    return raw
