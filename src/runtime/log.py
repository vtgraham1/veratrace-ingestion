"""Shared structured-logging helpers.

`logfmt()` and `http_error_body()` exist in one place so every module that emits
event-style log lines or captures 4xx response bodies for debugging uses the
same escaping rules.
"""
from __future__ import annotations

import urllib.error


def logfmt(event: str, **fields) -> str:
    """Emit a single logfmt-style line: event=foo key=val key2="val with spaces".

    Newlines/tabs/CRs in values are escaped (e.g. \\n) so a multiline traceback
    in `error=...` doesn't split the line and break downstream log parsers.
    """
    parts = [f"event={event}"]
    for k, v in fields.items():
        if v is None:
            continue
        s = str(v)
        if any(c in s for c in (" ", "=", '"', "\n", "\r", "\t")):
            s = s.replace("\\", "\\\\")
            s = s.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
            s = s.replace('"', '\\"')
            s = '"' + s + '"'
        parts.append(f"{k}={s}")
    return " ".join(parts)


def http_error_body(e: urllib.error.HTTPError, limit: int = 300) -> str:
    """Capture HTTPError body for logs without crashing if .read() fails or fp is consumed.

    The whole point of grabbing the body is to make 4xx debuggable; raising during
    the capture defeats that purpose.
    """
    if not e.fp:
        return ""
    try:
        return e.read()[:limit].decode("utf-8", "replace")
    except Exception:
        return ""
