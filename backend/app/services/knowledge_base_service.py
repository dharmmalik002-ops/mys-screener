"""AI Knowledge Base — stores user trading principles, blog extracts, YouTube notes."""

import json
import logging
import re
import uuid
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

logger = logging.getLogger(__name__)

_KB_PATH = Path(__file__).parent.parent.parent / "data" / "ai_knowledge_base.json"


# ─────────────────────────── HTML stripper ───────────────────────────────────

class _TextExtractor(HTMLParser):
    _SKIP = {"script", "style", "nav", "header", "footer", "noscript", "svg", "aside"}

    def __init__(self) -> None:
        super().__init__()
        self._depth = 0
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag in self._SKIP:
            self._depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in self._SKIP and self._depth > 0:
            self._depth -= 1

    def handle_data(self, data: str) -> None:
        if self._depth == 0:
            s = data.strip()
            if len(s) > 1:
                self._parts.append(s)

    def get_text(self) -> str:
        return "\n".join(self._parts)


# ─────────────────────────── Storage helpers ─────────────────────────────────

def _load() -> dict:
    if _KB_PATH.exists():
        try:
            return json.loads(_KB_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"entries": []}


def _save(data: dict) -> None:
    _KB_PATH.parent.mkdir(parents=True, exist_ok=True)
    _KB_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


# ─────────────────────────── CRUD ────────────────────────────────────────────

def list_entries() -> list[dict]:
    entries = _load().get("entries", [])
    result = []
    for e in entries:
        content = e.get("content", "")
        result.append(
            {
                "id": e["id"],
                "type": e.get("type", "text"),
                "title": e.get("title", ""),
                "content_preview": (content[:280] + "…") if len(content) > 280 else content,
                "source_url": e.get("source_url"),
                "added_at": e.get("added_at", ""),
                "content_length": len(content),
            }
        )
    return result


def get_full_entries() -> list[dict]:
    """Return full entries for AI injection (not paginated)."""
    return _load().get("entries", [])


def add_entry(
    entry_type: str,
    title: str,
    content: str,
    source_url: str | None = None,
) -> dict:
    data = _load()
    entry = {
        "id": str(uuid.uuid4()),
        "type": entry_type,
        "title": title,
        "content": content,
        "source_url": source_url,
        "added_at": datetime.now(timezone.utc).isoformat(),
    }
    data.setdefault("entries", []).append(entry)
    _save(data)
    return {
        "id": entry["id"],
        "type": entry["type"],
        "title": entry["title"],
        "content_preview": content[:280],
        "source_url": source_url,
        "added_at": entry["added_at"],
        "content_length": len(content),
    }


def delete_entry(entry_id: str) -> bool:
    data = _load()
    before = len(data.get("entries", []))
    data["entries"] = [e for e in data.get("entries", []) if e["id"] != entry_id]
    if len(data["entries"]) < before:
        _save(data)
        return True
    return False


# ─────────────────────────── URL ingestion ───────────────────────────────────

def _validate_fetch_url(url: str) -> None:
    """Reject URLs that target local or internal network resources (SSRF prevention)."""
    import urllib.parse
    import ipaddress

    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Only http/https URLs are allowed (got '{parsed.scheme}://')")

    hostname = parsed.hostname or ""
    if not hostname:
        raise ValueError("URL must have a valid hostname")

    # Block localhost variants
    if hostname in ("localhost", "127.0.0.1", "::1") or hostname.endswith(".localhost"):
        raise ValueError("Requests to localhost are not allowed")

    # Block internal/private IP ranges (including cloud metadata endpoints)
    try:
        addr = ipaddress.ip_address(hostname)
        if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved:
            raise ValueError("Requests to private or reserved IP addresses are not allowed")
    except ValueError as exc:
        # Re-raise our own validation errors; ignore non-IP hostnames
        if "not allowed" in str(exc) or "localhost" in str(exc):
            raise


def fetch_url_content(url: str) -> dict:
    """Fetch and extract text from a URL (web page or YouTube)."""
    import urllib.request
    import urllib.error

    _validate_fetch_url(url)

    yt_match = re.search(r"(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_\-]{11})", url)
    if yt_match:
        return _ingest_youtube(url, yt_match.group(1))

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=20) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            html = resp.read(400_000).decode(charset, errors="replace")
    except urllib.error.URLError as exc:
        raise ValueError(f"Could not fetch URL: {exc.reason}") from exc
    except Exception as exc:
        raise ValueError(f"Could not fetch URL: {exc}") from exc

    title_m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    title = re.sub(r"\s+", " ", title_m.group(1)).strip() if title_m else url

    extractor = _TextExtractor()
    try:
        extractor.feed(html)
        raw = extractor.get_text()
    except Exception:
        raw = re.sub(r"<[^>]+>", " ", html)

    text = re.sub(r"[ \t]{4,}", "   ", re.sub(r"\n{5,}", "\n\n\n", raw)).strip()
    text = text[:10_000]
    return {"title": title, "content": text, "source_type": "web"}


def _ingest_youtube(url: str, video_id: str) -> dict:
    # 1. Try youtube-transcript-api (optional dependency)
    try:
        from youtube_transcript_api import YouTubeTranscriptApi  # type: ignore

        transcripts = YouTubeTranscriptApi.list_transcripts(video_id)
        transcript = transcripts.find_generated_transcript(["en", "en-US", "en-GB"])
        text = " ".join(t["text"] for t in transcript.fetch())
        title = f"YouTube: {video_id}"
        try:
            import urllib.request as _ureq

            req = _ureq.Request(
                f"https://www.youtube.com/watch?v={video_id}",
                headers={"User-Agent": "Mozilla/5.0"},
            )
            with _ureq.urlopen(req, timeout=10) as r:
                h = r.read(20_000).decode("utf-8", errors="replace")
            tm = re.search(r'"title"\s*:\s*"([^"]{3,200})"', h)
            if tm:
                title = tm.group(1)
        except Exception:
            pass
        return {"title": title, "content": text[:10_000], "source_type": "youtube"}
    except ImportError:
        pass
    except Exception as exc:
        logger.debug("youtube-transcript-api failed: %s", exc)

    # 2. Fallback: page scrape for title + description
    try:
        import urllib.request as _ureq

        req = _ureq.Request(
            f"https://www.youtube.com/watch?v={video_id}",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with _ureq.urlopen(req, timeout=15) as r:
            html = r.read(120_000).decode("utf-8", errors="replace")
    except Exception as exc:
        raise ValueError(f"Could not fetch YouTube page: {exc}") from exc

    title_m = re.search(r'"title"\s*:\s*"([^"]{3,300})"', html)
    title = (
        re.sub(r"\\.", " ", title_m.group(1)).strip() if title_m else f"YouTube: {video_id}"
    )
    desc_m = re.search(r'"shortDescription"\s*:\s*"((?:[^"\\]|\\.){0,3000})"', html)
    desc = ""
    if desc_m:
        desc = desc_m.group(1).replace("\\n", "\n").replace('\\"', '"')

    content = (
        f"YouTube Video: {title}\n\n"
        f"URL: {url}\n\n"
        f"Description:\n{desc}\n\n"
        "(Full transcript unavailable – add key insights manually if needed.)"
    )
    return {"title": title, "content": content[:10_000], "source_type": "youtube"}
