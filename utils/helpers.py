import re
import os
from typing import Optional
from datetime import datetime


def format_bytes(size_bytes: int) -> str:
    if size_bytes == 0:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def truncate_string(s: str, max_len: int = 80, suffix: str = "...") -> str:
    if len(s) <= max_len:
        return s
    return s[: max_len - len(suffix)] + suffix


def sanitize_sql(sql: str) -> str:
    sql = sql.strip().rstrip(";").strip()
    if ";" in sql:
        first_stmt = sql.split(";")[0].strip()
        if first_stmt:
            return first_stmt
    return sql


def extract_database_name_from_input(user_input: str) -> Optional[str]:
    patterns = [
        r"use\s+`?(\w+)`?",
        r"switch\s+to\s+`?(\w+)`?",
        r"connect\s+to\s+`?(\w+)`?",
        r"go\s+to\s+(?:database\s+)?`?(\w+)`?",
        r"open\s+(?:database\s+)?`?(\w+)`?",
        r"work\s+(?:on|with)\s+`?(\w+)`?",
    ]
    for pattern in patterns:
        match = re.search(pattern, user_input, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def extract_table_name_from_input(user_input: str) -> Optional[str]:
    patterns = [
        r"(?:describe|desc|show\s+columns?\s+(?:of|from|in))\s+`?(\w+)`?",
        r"(?:structure\s+of|schema\s+of)\s+`?(\w+)`?",
        r"table\s+`?(\w+)`?",
    ]
    for pattern in patterns:
        match = re.search(pattern, user_input, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def is_safe_query(sql: str) -> bool:
    first_word = sql.strip().split()[0].upper() if sql.strip() else ""
    safe_keywords = {"SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN"}
    return first_word in safe_keywords


def format_duration(milliseconds: int) -> str:
    if milliseconds < 1000:
        return f"{milliseconds}ms"
    elif milliseconds < 60000:
        return f"{milliseconds / 1000:.2f}s"
    else:
        minutes = milliseconds // 60000
        seconds = (milliseconds % 60000) / 1000
        return f"{minutes}m {seconds:.1f}s"


def get_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def build_thread_display_name(thread_id: str, db_name: str, host: str) -> str:
    return f"{db_name}@{host} [{thread_id[:8]}...]"


def parse_mysql_version(version_string: str) -> str:
    match = re.search(r"(\d+\.\d+\.\d+)", version_string)
    return match.group(1) if match else version_string


def clear_terminal():
    os.system("clear" if os.name != "nt" else "cls")