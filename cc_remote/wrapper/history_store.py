"""Persistent, rebuildable materialized history pages.

The engine transcript/rollout remains the source of truth.  This store keeps
only derived conversation pages so opening a session does not repeatedly scan
and translate the same JSONL bytes.  Every row is bound to a strong-enough
source fingerprint and can therefore be discarded without affecting engine
state or recovery.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from cc_remote.attachments import (
    ALLOWED_IMAGE_TYPES,
    MAX_IMAGE_DIMENSION,
    MAX_IMAGE_PIXELS,
    MAX_SINGLE_ATTACHMENT_BYTES,
    decode_attachment,
    image_dimensions,
)


_SCHEMA_VERSION = 9
_FINGERPRINT_SAMPLE_BYTES = 64 * 1024
_DEFAULT_MAX_ENTRIES = 128
_DEFAULT_MAX_BYTES = 64 * 1024 * 1024
_SUMMARY_PROMPT_MAX_CHARS = 128 * 1024
_SUMMARY_TEXT_MAX_CHARS = 256 * 1024
_SUMMARY_LIVE_TEXT_MAX_CHARS = 64 * 1024
_SUMMARY_LIVE_MESSAGE_MAX_CHARS = 16 * 1024
_SUMMARY_LIVE_FIELD_MAX_CHARS = 4 * 1024
_SUMMARY_LIVE_BLOCK_MAX = 24
_SUMMARY_BLOCK_MAX = 32
_VOLATILE_EVENT_FIELDS = frozenset({"ts", "seq", "to", "route_id"})


def _sqlite_file_id(value: int) -> int | str:
    """Keep wide Windows file IDs exact without SQLite INTEGER overflow.

    Non-numeric text avoids INTEGER affinity coercing a decimal string to an
    imprecise REAL. Existing signed-64-bit IDs retain their storage format.
    """
    return value if -(1 << 63) <= value < (1 << 63) else f"wide:{value:x}"


@dataclass(frozen=True)
class HistorySourceFingerprint:
    """Identity of the exact transcript snapshot used to build a page."""

    path: str
    device: int
    inode: int
    size: int
    mtime_ns: int
    head_sha256: str
    tail_sha256: str
    sample_sha256: str

    @classmethod
    def capture(cls, path: str | os.PathLike[str]) -> "HistorySourceFingerprint":
        resolved = os.path.realpath(os.fspath(path))
        with open(resolved, "rb") as source:
            stat = os.fstat(source.fileno())
            head = source.read(_FINGERPRINT_SAMPLE_BYTES)
            tail = head
            digest = hashlib.sha256()
            digest.update(head)
            if stat.st_size > _FINGERPRINT_SAMPLE_BYTES:
                source.seek(max(0, stat.st_size - _FINGERPRINT_SAMPLE_BYTES))
                tail = source.read(_FINGERPRINT_SAMPLE_BYTES)
                digest.update(tail)
        return cls(
            path=resolved,
            device=int(stat.st_dev),
            inode=int(stat.st_ino),
            size=int(stat.st_size),
            mtime_ns=int(stat.st_mtime_ns),
            head_sha256=hashlib.sha256(head).hexdigest(),
            tail_sha256=hashlib.sha256(tail).hexdigest(),
            sample_sha256=digest.hexdigest(),
        )

    @property
    def token(self) -> str:
        payload = "\0".join((
            self.path,
            str(self.device),
            str(self.inode),
            str(self.size),
            str(self.mtime_ns),
            self.head_sha256,
            self.tail_sha256,
            self.sample_sha256,
        ))
        return hashlib.sha256(payload.encode("utf-8", "surrogatepass")).hexdigest()


@dataclass(frozen=True)
class MaterializedHistoryPage:
    """Immutable narrative projection stored independently of live control."""

    events: tuple[dict[str, Any], ...]
    has_more: bool
    oldest_id: str | None
    newest_id: str | None
    turns: tuple[dict[str, Any], ...] = ()

    def as_payload(self) -> dict[str, Any]:
        return {
            "events": list(self.events),
            "has_more": self.has_more,
            "oldest_id": self.oldest_id,
            "newest_id": self.newest_id,
            "turns": list(self.turns),
        }

    def semantic_token(self) -> str:
        """Hash narrative identity while ignoring transport-time metadata.

        A few legacy translators construct synthetic start/end envelopes after
        reading the transcript, so their default ``ts`` can vary across equal
        parses.  That timestamp is useful for the first materialization but is
        not evidence that the underlying conversation changed.
        """
        events = [
            {key: value for key, value in event.items()
             if key not in _VOLATILE_EVENT_FIELDS}
            for event in self.events
        ]
        payload = {
            "events": events,
            "has_more": self.has_more,
            "oldest_id": self.oldest_id,
            "newest_id": self.newest_id,
            "turns": list(self.turns),
        }
        encoded = json.dumps(
            payload, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def semantically_equals(self, other: "MaterializedHistoryPage") -> bool:
        return self.semantic_token() == other.semantic_token()

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "MaterializedHistoryPage":
        events = payload.get("events")
        if not isinstance(events, list) or not all(
                isinstance(event, dict) for event in events):
            raise ValueError("invalid materialized history events")
        turns = payload.get("turns")
        if turns is None:
            turns = materialize_history_turns(events)
        if not isinstance(turns, (list, tuple)) or not all(
                isinstance(turn, dict) for turn in turns):
            raise ValueError("invalid materialized history turns")
        return cls(
            events=tuple(events),
            has_more=bool(payload.get("has_more")),
            oldest_id=(payload.get("oldest_id")
                       if isinstance(payload.get("oldest_id"), str) else None),
            newest_id=(payload.get("newest_id")
                       if isinstance(payload.get("newest_id"), str) else None),
            turns=tuple(turns),
        )


def _event_ms(value: Any) -> int | None:
    if not isinstance(value, (int, float)):
        return None
    return round(float(value) * 1000)


def group_history_events(
    events: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> list[list[dict[str, Any]]]:
    """Split translated wire events into stable completed/in-flight turns."""
    groups: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for event in events:
        event_type = event.get("type")
        if event_type in {"model", "effort", "perm", "fast",
                          "collaboration_mode", "session_control"}:
            continue
        if event_type == "user_msg" and current:
            groups.append(current)
            current = []
        current.append(event)
        if event_type == "turn_end":
            groups.append(current)
            current = []
    if current:
        groups.append(current)
    return groups


def _turn_id(group: list[dict[str, Any]]) -> str | None:
    for event in group:
        if event.get("type") == "user_msg" and isinstance(event.get("msg_id"), str):
            return event["msg_id"]
    for event in reversed(group):
        if event.get("type") == "turn_end" and isinstance(event.get("turn_id"), str):
            return event["turn_id"]
    for field in ("message_id", "item_id", "tool_use_id"):
        for event in group:
            if isinstance(event.get(field), str):
                return event[field]
    return None


def history_image_id(turn_id: str, index: int) -> str:
    digest = hashlib.sha256(
        f"{turn_id}\0{index}".encode("utf-8", "surrogatepass")
    ).hexdigest()[:24]
    return f"img-{digest}"


def _history_image_refs(turn_id: str, raw_images: Any) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    if not isinstance(raw_images, list):
        return refs
    for index, image in enumerate(raw_images):
        if not isinstance(image, dict):
            continue
        media_type = image.get("media_type")
        data = image.get("data")
        if not isinstance(media_type, str) or not isinstance(data, str):
            continue
        if media_type not in ALLOWED_IMAGE_TYPES:
            continue
        try:
            decoded = decode_attachment(data)
        except ValueError:
            continue
        if len(decoded) > MAX_SINGLE_ATTACHMENT_BYTES:
            continue
        dimensions = image_dimensions(decoded, media_type)
        if dimensions is None:
            continue
        width, height = dimensions
        if (
            width <= 0 or height <= 0
            or width > MAX_IMAGE_DIMENSION or height > MAX_IMAGE_DIMENSION
            or width * height > MAX_IMAGE_PIXELS
        ):
            continue
        refs.append({
            "image_id": history_image_id(turn_id, index),
            "media_type": media_type,
            "width": width,
            "height": height,
            "byte_size": len(decoded),
        })
    return refs


def history_image_from_events(
    events: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    turn_id: str,
    image_id: str,
) -> dict[str, Any] | None:
    """Resolve an opaque history image id inside one indexed turn group."""
    for event in events:
        if event.get("type") != "user_msg" or event.get("msg_id") != turn_id:
            continue
        images = event.get("images")
        if not isinstance(images, list):
            return None
        for index, image in enumerate(images):
            if history_image_id(turn_id, index) == image_id and isinstance(image, dict):
                return image
    return None


def materialize_history_turns(
    events: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    include_live_detail: bool = False,
) -> tuple[dict[str, Any], ...]:
    """Build the lightweight UI projection persisted beside full detail.

    Tool/process/commentary bodies deliberately stay in ``events`` for an
    explicit detail request.  The initial projection contains only the prompt,
    final answer, terminal state and a count advertising expandable detail.
    Native Codex App activity has no live app-server stream, so its current
    moving head may additionally carry bounded commentary and lifecycle
    summaries. Large inputs, outputs, diffs and reasoning remain deferred.
    """
    turns: list[dict[str, Any]] = []
    for group in group_history_events(events):
        turn_id = _turn_id(group)
        if turn_id is None:
            continue
        prompt = ""
        has_user = False
        prompt_truncated = False
        image_refs: list[dict[str, Any]] = []
        deferred_image_count = 0
        files = None
        started_ms = None
        done_ms = None
        duration_ms = None
        fork_point = None
        checkpoint_id = None
        done = False
        interrupted = False
        error = None
        channels: dict[str, str] = {}
        texts: dict[str, list[str]] = {}
        text_order: list[str] = []
        text_done: set[str] = set()
        detail_items: set[str] = set()
        live_blocks: list[dict[str, Any]] = []
        live_texts: dict[str, dict[str, Any]] = {}
        live_tools: dict[str, dict[str, Any]] = {}
        live_processes: dict[str, dict[str, Any]] = {}

        def short(value: Any) -> str | None:
            if not isinstance(value, str) or not value:
                return None
            return value[:_SUMMARY_LIVE_FIELD_MAX_CHARS]

        def add_live_text(message_id: str, channel: str) -> dict[str, Any] | None:
            if not include_live_detail or channel not in {
                    "commentary", "thinking"}:
                return None
            block = live_texts.get(message_id)
            if block is None:
                block = {
                    "kind": "text",
                    "message_id": message_id,
                    "text": "",
                    "done": False,
                    "channel": channel,
                }
                live_texts[message_id] = block
                live_blocks.append(block)
            elif channel != "unknown":
                block["channel"] = channel
            return block

        for event in group:
            event_type = event.get("type")
            if started_ms is None:
                started_ms = _event_ms(event.get("ts"))
            if event_type == "user_msg":
                has_user = True
                if isinstance(event.get("prompt"), str):
                    prompt = event["prompt"]
                    if len(prompt) > _SUMMARY_PROMPT_MAX_CHARS:
                        suffix = "\n\n…（完整问题请展开本轮过程）"
                        keep = _SUMMARY_PROMPT_MAX_CHARS - len(suffix)
                        prompt = prompt[:keep] + suffix
                        prompt_truncated = True
                raw_images = event.get("images")
                image_refs = _history_image_refs(turn_id, raw_images)
                if isinstance(raw_images, list):
                    deferred_image_count = max(
                        0, len(raw_images) - len(image_refs))
                files = event.get("files")
            elif event_type == "assistant_msg_start":
                message_id = event.get("message_id")
                if isinstance(message_id, str):
                    channels[message_id] = str(event.get("channel") or "unknown")
                    if message_id not in texts:
                        texts[message_id] = []
                        text_order.append(message_id)
                    add_live_text(message_id, channels[message_id])
            elif event_type == "delta":
                message_id = event.get("message_id")
                if isinstance(message_id, str) and isinstance(event.get("text"), str):
                    channels[message_id] = str(
                        event.get("channel") or channels.get(message_id) or "unknown")
                    if message_id not in texts:
                        texts[message_id] = []
                        text_order.append(message_id)
                    texts[message_id].append(event["text"])
                    block = add_live_text(message_id, channels[message_id])
                    if block is not None:
                        block["text"] += event["text"]
            elif event_type == "assistant_msg_end":
                message_id = event.get("message_id")
                if isinstance(message_id, str):
                    text_done.add(message_id)
                    block = live_texts.get(message_id)
                    if block is not None:
                        block["done"] = True
            elif event_type == "turn_binding":
                if isinstance(event.get("turn_id"), str):
                    fork_point = event["turn_id"]
            elif event_type == "turn_end":
                done = True
                done_ms = _event_ms(event.get("ts"))
                if isinstance(event.get("turn_id"), str):
                    fork_point = event["turn_id"]
                if isinstance(event.get("checkpoint_id"), str):
                    checkpoint_id = event["checkpoint_id"]
                result = event.get("result")
                if isinstance(result, dict):
                    if isinstance(result.get("duration_ms"), int):
                        duration_ms = result["duration_ms"]
                    subtype = str(result.get("subtype") or "")
                    interrupted = subtype in {
                        "interrupted", "error_during_execution", "aborted",
                    }
                    if bool(result.get("is_error")) and not interrupted:
                        error = (
                            "该轮未正常结束"
                            if not subtype or subtype == "error"
                            else subtype
                        )
            elif event_type == "error":
                if isinstance(event.get("message"), str):
                    error = (
                        "该轮未正常结束"
                        if event["message"].strip().lower() == "error"
                        else event["message"]
                    )
            if include_live_detail and event_type == "tool_use":
                tool_id = event.get("tool_use_id")
                message_id = event.get("message_id")
                tool = event.get("tool")
                if all(isinstance(value, str) for value in (
                        tool_id, message_id, tool)):
                    block = live_tools.get(tool_id)
                    if block is None:
                        block = {
                            "kind": "tool",
                            "message_id": message_id,
                            "tool_use_id": tool_id,
                            "tool": tool,
                            "input": {},
                            "category": event.get("category") or "tool",
                            "title": short(event.get("title")),
                            "parent_id": event.get("parent_id"),
                            "server": short(event.get("server")),
                            "done": False,
                        }
                        live_tools[tool_id] = block
                        live_blocks.append(block)
            elif include_live_detail and event_type == "tool_result":
                tool_id = event.get("tool_use_id")
                block = live_tools.get(tool_id) if isinstance(tool_id, str) else None
                if block is not None:
                    result = {
                        "content": "",
                        "is_error": bool(event.get("is_error")),
                    }
                    for key in ("status", "exit_code", "duration_ms"):
                        if event.get(key) is not None:
                            result[key] = event[key]
                    summary = short(event.get("summary"))
                    if summary is not None:
                        result["summary"] = summary
                    if event.get("truncated") is not None:
                        result["truncated"] = bool(event["truncated"])
                    block["result"] = result
                    block["done"] = True
            elif (include_live_detail and event_type == "process"
                  and event.get("kind") != "reasoning"):
                item_id = event.get("item_id")
                if isinstance(item_id, str):
                    block = live_processes.get(item_id)
                    if block is None:
                        block = {
                            "kind": "process",
                            "item_id": item_id,
                            "processKind": event.get("kind") or "task",
                            "phase": event.get("phase") or "snapshot",
                            "status": event.get("status") or "unknown",
                            "turn_id": event.get("turn_id"),
                            "parent_id": event.get("parent_id"),
                            "title": short(event.get("title")) or "处理",
                            "done": False,
                        }
                        live_processes[item_id] = block
                        live_blocks.append(block)
                    for target, source in (
                        ("summary", "summary"),
                        ("progress", "progress"),
                        ("server", "server"),
                        ("tool", "tool"),
                    ):
                        value = short(event.get(source))
                        if value is not None:
                            block[target] = value
                    for key in ("exit_code", "duration_ms", "truncated"):
                        if event.get(key) is not None:
                            block[key] = event[key]
                    block["phase"] = event.get("phase") or block["phase"]
                    block["status"] = event.get("status") or block["status"]
                    block["done"] = (
                        block["phase"] == "end"
                        or block["status"] in {
                            "succeeded", "failed", "declined", "cancelled",
                            "interrupted",
                        }
                    )
            elif include_live_detail and event_type == "turn_plan":
                item_id = event.get("item_id")
                if isinstance(item_id, str):
                    raw_plan = event.get("plan")
                    plan = []
                    if isinstance(raw_plan, list):
                        for entry in raw_plan[:32]:
                            if not isinstance(entry, dict):
                                continue
                            step = short(entry.get("step"))
                            status = entry.get("status")
                            if step and status in {
                                    "pending", "inProgress", "completed"}:
                                plan.append({"step": step, "status": status})
                    succeeded = bool(
                        plan and all(entry["status"] == "completed"
                                     for entry in plan)
                    )
                    replacement = {
                        "kind": "process",
                        "item_id": item_id,
                        "processKind": "plan",
                        "phase": "snapshot",
                        "status": "succeeded" if succeeded else "running",
                        "turn_id": event.get("turn_id"),
                        "parent_id": None,
                        "title": "计划",
                        "done": succeeded,
                        "plan": plan,
                    }
                    explanation = short(event.get("explanation"))
                    if explanation is not None:
                        replacement["explanation"] = explanation
                    block = live_processes.get(item_id)
                    if block is None:
                        block = replacement
                        live_processes[item_id] = block
                        live_blocks.append(block)
                    else:
                        block.clear()
                        block.update(replacement)
            if event_type in {"tool_use", "process", "turn_plan", "turn_diff"}:
                detail_id = event.get("tool_use_id") or event.get("item_id")
                if isinstance(detail_id, str):
                    detail_items.add(detail_id)

        final_ids = [
            message_id for message_id in text_order
            if channels.get(message_id) == "final"
        ]
        if not final_ids:
            final_ids = [
                message_id for message_id in text_order
                if channels.get(message_id) in {None, "unknown"}
            ]
        final_id_set = set(final_ids)
        for message_id in text_order:
            if message_id not in final_id_set and any(texts.get(message_id, ())):
                detail_items.add(message_id)
        # Codex reconstructs summary-only process/text envelopes while parsing
        # a rollout. For an assistant-only continuation those synthetic rows
        # can carry the parse time because there is no UserMsg to provide the
        # authoritative start. Never persist the impossible `ts > doneTs`
        # ordering: derive the start from the terminal and its duration instead.
        if (not has_user and done_ms is not None
                and (started_ms is None or started_ms > done_ms)):
            started_ms = max(0, done_ms - (duration_ms or 0))
        blocks = []
        if include_live_detail:
            final_id_set = set(final_ids)
            final_block_count = sum(
                bool("".join(texts.get(message_id, ())))
                for message_id in final_ids
            )
            live_block_limit = max(
                0,
                min(_SUMMARY_LIVE_BLOCK_MAX,
                    _SUMMARY_BLOCK_MAX - final_block_count),
            )
            candidates: list[dict[str, Any]] = []
            for block in live_blocks:
                if (block.get("kind") == "text"
                        and block.get("message_id") in final_id_set):
                    continue
                candidate = dict(block)
                if candidate.get("kind") == "text":
                    text = str(candidate.get("text") or "")
                    if not text:
                        continue
                    candidate["done"] = (
                        candidate.get("message_id") in text_done or done)
                candidates.append(candidate)
            if done:
                terminal_status = (
                    "interrupted" if interrupted
                    else "failed" if error
                    else "succeeded"
                )
                for block in candidates:
                    if block.get("done"):
                        continue
                    block["done"] = True
                    if block.get("kind") == "process":
                        block["phase"] = "end"
                        block["status"] = terminal_status
                    elif block.get("kind") == "tool":
                        block["result"] = {
                            "content": "",
                            "is_error": terminal_status != "succeeded",
                            "status": terminal_status,
                        }
            if live_block_limit == 0:
                candidates = []
            elif len(candidates) > live_block_limit:
                tail_size = max(0, live_block_limit - 1)
                tail = candidates[-tail_size:] if tail_size else []
                first_text = next(
                    (block for block in candidates
                     if block.get("kind") == "text"),
                    None,
                )
                candidates = (
                    [first_text, *tail]
                    if first_text is not None and first_text not in tail
                    else candidates[-live_block_limit:]
                )
            remaining_live_chars = _SUMMARY_LIVE_TEXT_MAX_CHARS
            for block in candidates:
                if block.get("kind") != "text":
                    continue
                text = str(block.get("text") or "")
                keep = min(
                    len(text),
                    _SUMMARY_LIVE_MESSAGE_MAX_CHARS,
                    remaining_live_chars,
                )
                block["text"] = text[:keep]
                remaining_live_chars -= keep
            blocks.extend(candidates)
        remaining_summary_chars = _SUMMARY_TEXT_MAX_CHARS
        summary_truncated = False
        for message_id in final_ids:
            text = "".join(texts.get(message_id, ()))
            if text:
                if remaining_summary_chars <= 0:
                    summary_truncated = True
                    continue
                if len(text) > remaining_summary_chars:
                    suffix = "\n\n…（完整内容请展开本轮过程）"
                    if remaining_summary_chars <= len(suffix):
                        text = suffix[:remaining_summary_chars]
                    else:
                        keep = remaining_summary_chars - len(suffix)
                        text = text[:keep] + suffix
                    summary_truncated = True
                remaining_summary_chars -= len(text)
                blocks.append({
                    "kind": "text",
                    "message_id": message_id,
                    "text": text,
                    "done": done,
                    "channel": "final",
                })
        if len(blocks) > _SUMMARY_BLOCK_MAX:
            # Many final messages can exceed the budget even with no live
            # blocks. Keep the opening plus latest conclusions; full history
            # stays available through the existing deferred detail endpoint.
            blocks = [blocks[0], *blocks[-(_SUMMARY_BLOCK_MAX - 1):]]
            summary_truncated = True
        turn: dict[str, Any] = {
            "id": turn_id,
            "prompt": prompt,
            "blocks": blocks,
            "done": done,
            "detailEventCount": (
                len(detail_items)
                + int(prompt_truncated)
                + int(summary_truncated)
                + deferred_image_count
            ),
            "detailLoaded": False,
        }
        optional = {
            "forkPointId": fork_point,
            "checkpointId": checkpoint_id,
            "imageRefs": image_refs or None,
            "files": files,
            "ts": started_ms,
            "doneTs": done_ms,
            "durationMs": duration_ms,
            "error": error,
        }
        turn.update({key: value for key, value in optional.items() if value is not None})
        if interrupted:
            turn["interrupted"] = True
        turns.append(turn)
    return tuple(turns)


class HistoryIndexStore:
    """Small SQLite LRU for source-bound materialized conversation pages.

    Connections are intentionally short lived.  History parsing runs in worker
    threads, and opening a connection per store operation avoids sharing one
    sqlite handle across the wrapper event loop and those workers.
    """

    def __init__(
        self,
        state_dir: Path,
        *,
        max_entries: int = _DEFAULT_MAX_ENTRIES,
        max_bytes: int = _DEFAULT_MAX_BYTES,
    ) -> None:
        self.path = Path(state_dir) / "history-index.sqlite3"
        self.max_entries = max(1, int(max_entries))
        self.max_bytes = max(1024, int(max_bytes))
        self._ensure_schema()

    @contextmanager
    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        # History pages are materialized concurrently in worker threads. A
        # longer busy window is safe because callers use asyncio.to_thread and
        # prevents a large rollout write from turning a simultaneous refresh
        # into a user-visible warning on Windows.
        connection = sqlite3.connect(self.path, timeout=15)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA busy_timeout=15000")
            with connection:
                yield connection
        finally:
            # sqlite3's transaction context commits/rolls back, but does not
            # close the handle. Deterministic closure matters on Windows.
            connection.close()

    def _ensure_schema(self) -> None:
        with self._connect() as connection:
            # Journal mode is database-wide. Negotiating it for every
            # short-lived reader requires an exclusive lock and was itself a
            # source of SQLITE_BUSY under concurrent catalog refreshes.
            connection.execute("PRAGMA journal_mode=WAL")
            current = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if current not in (0, _SCHEMA_VERSION):
                # This database is derived exclusively from engine transcripts.
                # Rebuilding is safer than carrying migrations for stale cached
                # projections across wire-shape changes.
                connection.execute("DROP TABLE IF EXISTS history_pages")
                connection.execute("DROP TABLE IF EXISTS history_turn_details")
                connection.execute("DROP TABLE IF EXISTS history_image_assets")
                current = 0
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS history_pages (
                    session_id TEXT NOT NULL,
                    engine TEXT NOT NULL,
                    source_token TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    source_device INTEGER NOT NULL,
                    source_inode INTEGER NOT NULL,
                    source_size INTEGER NOT NULL,
                    source_head_sha256 TEXT NOT NULL,
                    source_tail_sha256 TEXT NOT NULL,
                    before_cursor TEXT NOT NULL,
                    page_limit INTEGER NOT NULL,
                    payload_json BLOB NOT NULL,
                    payload_bytes INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    accessed_at REAL NOT NULL,
                    PRIMARY KEY (
                        session_id, engine, source_token,
                        before_cursor, page_limit
                    )
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS history_pages_lru "
                "ON history_pages(accessed_at)"
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS history_turn_details (
                    session_id TEXT NOT NULL,
                    engine TEXT NOT NULL,
                    source_token TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    turn_id TEXT NOT NULL,
                    payload_json BLOB NOT NULL,
                    payload_bytes INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    accessed_at REAL NOT NULL,
                    PRIMARY KEY (session_id, engine, source_token, turn_id)
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS history_turn_details_lookup "
                "ON history_turn_details(session_id, engine, source_path, "
                "turn_id, created_at DESC)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS history_turn_details_lru "
                "ON history_turn_details(accessed_at)"
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS history_image_assets (
                    session_id TEXT NOT NULL,
                    engine TEXT NOT NULL,
                    source_token TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    turn_id TEXT NOT NULL,
                    image_id TEXT NOT NULL,
                    variant TEXT NOT NULL,
                    media_type TEXT NOT NULL,
                    width INTEGER NOT NULL,
                    height INTEGER NOT NULL,
                    data BLOB NOT NULL,
                    payload_bytes INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    accessed_at REAL NOT NULL,
                    PRIMARY KEY (
                        session_id, engine, source_token,
                        turn_id, image_id, variant
                    )
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS history_image_assets_lookup "
                "ON history_image_assets(session_id, engine, source_path, "
                "turn_id, image_id, variant, created_at DESC)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS history_image_assets_lru "
                "ON history_image_assets(accessed_at)"
            )
            if current == 0:
                connection.execute(f"PRAGMA user_version={_SCHEMA_VERSION}")
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    @staticmethod
    def _cursor(before: str | None) -> str:
        return before or ""

    def get_page(
        self,
        session_id: str,
        engine: str,
        source: HistorySourceFingerprint,
        *,
        before: str | None,
        limit: int,
    ) -> MaterializedHistoryPage | None:
        now = time.time()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT payload_json FROM history_pages
                WHERE session_id=? AND engine=? AND source_token=?
                  AND before_cursor=? AND page_limit=?
                """,
                (session_id, engine, source.token, self._cursor(before), int(limit)),
            ).fetchone()
            if row is None:
                return None
            connection.execute(
                """
                UPDATE history_pages SET accessed_at=?
                WHERE session_id=? AND engine=? AND source_token=?
                  AND before_cursor=? AND page_limit=?
                """,
                (now, session_id, engine, source.token,
                 self._cursor(before), int(limit)),
            )
        try:
            payload = json.loads(bytes(row["payload_json"]).decode("utf-8"))
            if not isinstance(payload, dict):
                return None
            return MaterializedHistoryPage.from_payload(payload)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError):
            self.invalidate_session(session_id)
            return None

    def get_append_page(
        self,
        session_id: str,
        engine: str,
        source: HistorySourceFingerprint,
        *,
        before: str | None,
        limit: int,
    ) -> MaterializedHistoryPage | None:
        """Return a cached page whose source is a verified file prefix.

        The caller may paint this page while rebuilding the exact appended
        snapshot. Device/inode/size checks plus both sampled ends of the old
        source reject truncation, replacement and in-place rewrites.
        """
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT payload_json, source_size, source_head_sha256,
                       source_tail_sha256
                FROM history_pages
                WHERE session_id=? AND engine=? AND source_path=?
                  AND source_device=? AND source_inode=?
                  AND source_size<? AND before_cursor=? AND page_limit=?
                ORDER BY source_size DESC, created_at DESC
                LIMIT 1
                """,
                (session_id, engine, source.path,
                 _sqlite_file_id(source.device), _sqlite_file_id(source.inode),
                 source.size, self._cursor(before), int(limit)),
            ).fetchone()
        if row is None:
            return None
        old_size = int(row["source_size"])
        try:
            with open(source.path, "rb") as current:
                stat = os.fstat(current.fileno())
                if (int(stat.st_dev) != source.device
                        or int(stat.st_ino) != source.inode
                        or int(stat.st_size) < source.size):
                    return None
                sample_size = min(old_size, _FINGERPRINT_SAMPLE_BYTES)
                head = current.read(sample_size)
                current.seek(max(0, old_size - sample_size))
                tail = current.read(sample_size)
        except OSError:
            return None
        if (hashlib.sha256(head).hexdigest() != row["source_head_sha256"]
                or hashlib.sha256(tail).hexdigest()
                != row["source_tail_sha256"]):
            return None
        try:
            payload = json.loads(bytes(row["payload_json"]).decode("utf-8"))
            if not isinstance(payload, dict):
                return None
            return MaterializedHistoryPage.from_payload(payload)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError):
            self.invalidate_session(session_id)
            return None

    def put_page(
        self,
        session_id: str,
        engine: str,
        source: HistorySourceFingerprint,
        *,
        before: str | None,
        limit: int,
        page: MaterializedHistoryPage,
    ) -> bool:
        payload = json.dumps(
            page.as_payload(), ensure_ascii=False, separators=(",", ":"),
        ).encode("utf-8")
        now = time.time()
        with self._connect() as connection:
            # Detail is indexed per turn as well as inside the full page.  This
            # keeps expansion O(one turn) and remains available after an append
            # changes the exact page fingerprint.  Destructive rewrites call
            # invalidate_session(), so an old turn can never cross rollback.
            for group in group_history_events(page.events):
                turn_id = _turn_id(group)
                if turn_id is None:
                    continue
                detail_payload = json.dumps(
                    group, ensure_ascii=False, separators=(",", ":"),
                ).encode("utf-8")
                if len(detail_payload) > self.max_bytes:
                    continue
                connection.execute(
                    """
                    INSERT INTO history_turn_details (
                        session_id, engine, source_token, source_path,
                        turn_id, payload_json, payload_bytes,
                        created_at, accessed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (session_id, engine, source_token, turn_id)
                    DO UPDATE SET
                        source_path=excluded.source_path,
                        payload_json=excluded.payload_json,
                        payload_bytes=excluded.payload_bytes,
                        created_at=excluded.created_at,
                        accessed_at=excluded.accessed_at
                    """,
                    (session_id, engine, source.token, source.path, turn_id,
                     detail_payload, len(detail_payload), now, now),
                )
            self._prune_details(connection)
            if len(payload) > self.max_bytes:
                return False
            connection.execute(
                """
                INSERT INTO history_pages (
                    session_id, engine, source_token, source_path,
                    source_device, source_inode, source_size,
                    source_head_sha256, source_tail_sha256,
                    before_cursor, page_limit, payload_json, payload_bytes,
                    created_at, accessed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (
                    session_id, engine, source_token, before_cursor, page_limit
                ) DO UPDATE SET
                    source_path=excluded.source_path,
                    source_device=excluded.source_device,
                    source_inode=excluded.source_inode,
                    source_size=excluded.source_size,
                    source_head_sha256=excluded.source_head_sha256,
                    source_tail_sha256=excluded.source_tail_sha256,
                    payload_json=excluded.payload_json,
                    payload_bytes=excluded.payload_bytes,
                    created_at=excluded.created_at,
                    accessed_at=excluded.accessed_at
                """,
                (session_id, engine, source.token, source.path,
                 _sqlite_file_id(source.device), _sqlite_file_id(source.inode), source.size,
                 source.head_sha256, source.tail_sha256,
                 self._cursor(before), int(limit), payload, len(payload), now, now),
            )
            # A changed source makes every older snapshot for this session
            # unreachable.  Delete it eagerly rather than waiting for global LRU.
            connection.execute(
                """
                DELETE FROM history_pages
                WHERE session_id=? AND engine=? AND source_token<>?
                """,
                (session_id, engine, source.token),
            )
            self._prune(connection)
        return True

    def get_turn_detail(
        self,
        session_id: str,
        engine: str,
        source: HistorySourceFingerprint,
        turn_id: str,
    ) -> tuple[dict[str, Any], ...] | None:
        """Return one materialized turn without reading a complete page.

        Prefer the exact source snapshot.  If the transcript only appended
        after the page was painted, the newest row for the same source path is
        still a valid immutable completed turn.  Rollback explicitly removes
        every row for the session before its new revision is exposed.
        """
        now = time.time()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT source_token, payload_json FROM history_turn_details
                WHERE session_id=? AND engine=? AND source_path=? AND turn_id=?
                ORDER BY (source_token = ?) DESC, created_at DESC
                LIMIT 1
                """,
                (session_id, engine, source.path, turn_id, source.token),
            ).fetchone()
            if row is not None:
                try:
                    payload = json.loads(
                        bytes(row["payload_json"]).decode("utf-8"))
                    if (isinstance(payload, list)
                            and all(isinstance(event, dict) for event in payload)
                            and _turn_id(payload) == turn_id):
                        connection.execute(
                            """
                            UPDATE history_turn_details SET accessed_at=?
                            WHERE session_id=? AND engine=?
                              AND source_token=? AND turn_id=?
                            """,
                            (now, session_id, engine,
                             row["source_token"], turn_id),
                        )
                        return tuple(payload)
                except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
                    pass

                # A malformed derived row must not mask the canonical page
                # fallback below.
                connection.execute(
                    """
                    DELETE FROM history_turn_details
                    WHERE session_id=? AND engine=?
                      AND source_token=? AND turn_id=?
                    """,
                    (session_id, engine, row["source_token"], turn_id),
                )

            # Detail has a deliberately tighter LRU than complete pages.  A
            # browser may still be displaying a retained summary after its
            # standalone detail row was evicted, so recover the group from the
            # canonical page instead of returning a permanently retryable miss.
            pages = connection.execute(
                """
                SELECT rowid, payload_json FROM history_pages
                WHERE session_id=? AND engine=? AND source_path=?
                ORDER BY (source_token = ?) DESC,
                         accessed_at DESC, created_at DESC
                """,
                (session_id, engine, source.path, source.token),
            )
            for page_row in pages:
                try:
                    page_payload = json.loads(
                        bytes(page_row["payload_json"]).decode("utf-8"))
                    page = MaterializedHistoryPage.from_payload(page_payload)
                except (UnicodeDecodeError, json.JSONDecodeError,
                        ValueError, TypeError):
                    connection.execute(
                        "DELETE FROM history_pages WHERE rowid=?",
                        (page_row["rowid"],),
                    )
                    continue
                for group in group_history_events(page.events):
                    if _turn_id(group) != turn_id:
                        continue
                    connection.execute(
                        "UPDATE history_pages SET accessed_at=? WHERE rowid=?",
                        (now, page_row["rowid"]),
                    )
                    return tuple(group)
        return None

    def get_image_asset(
        self,
        session_id: str,
        engine: str,
        source: HistorySourceFingerprint,
        turn_id: str,
        image_id: str,
        variant: str,
    ) -> tuple[str, int, int, bytes] | None:
        now = time.time()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT source_token, media_type, width, height, data
                FROM history_image_assets
                WHERE session_id=? AND engine=? AND source_path=?
                  AND turn_id=? AND image_id=? AND variant=?
                ORDER BY (source_token = ?) DESC, created_at DESC
                LIMIT 1
                """,
                (session_id, engine, source.path, turn_id, image_id,
                 variant, source.token),
            ).fetchone()
            if row is None:
                return None
            connection.execute(
                """
                UPDATE history_image_assets SET accessed_at=?
                WHERE session_id=? AND engine=? AND source_token=?
                  AND turn_id=? AND image_id=? AND variant=?
                """,
                (now, session_id, engine, row["source_token"], turn_id,
                 image_id, variant),
            )
        return (
            str(row["media_type"]), int(row["width"]), int(row["height"]),
            bytes(row["data"]),
        )

    def put_image_asset(
        self,
        session_id: str,
        engine: str,
        source: HistorySourceFingerprint,
        turn_id: str,
        image_id: str,
        variant: str,
        media_type: str,
        width: int,
        height: int,
        data: bytes,
    ) -> None:
        now = time.time()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO history_image_assets (
                    session_id, engine, source_token, source_path,
                    turn_id, image_id, variant, media_type, width, height,
                    data, payload_bytes, created_at, accessed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (
                    session_id, engine, source_token,
                    turn_id, image_id, variant
                ) DO UPDATE SET
                    media_type=excluded.media_type,
                    width=excluded.width,
                    height=excluded.height,
                    data=excluded.data,
                    payload_bytes=excluded.payload_bytes,
                    created_at=excluded.created_at,
                    accessed_at=excluded.accessed_at
                """,
                (session_id, engine, source.token, source.path, turn_id,
                 image_id, variant, media_type, width, height, data,
                 len(data), now, now),
            )
            self._prune_image_assets(connection)

    def _prune(self, connection: sqlite3.Connection) -> None:
        row = connection.execute(
            "SELECT COUNT(*) AS entries, COALESCE(SUM(payload_bytes), 0) AS bytes "
            "FROM history_pages"
        ).fetchone()
        entries = int(row["entries"])
        total_bytes = int(row["bytes"])
        while entries > self.max_entries or total_bytes > self.max_bytes:
            victim = connection.execute(
                """
                SELECT session_id, engine, source_token, before_cursor,
                       page_limit, payload_bytes
                FROM history_pages ORDER BY accessed_at ASC LIMIT 1
                """
            ).fetchone()
            if victim is None:
                break
            connection.execute(
                """
                DELETE FROM history_pages
                WHERE session_id=? AND engine=? AND source_token=?
                  AND before_cursor=? AND page_limit=?
                """,
                tuple(victim[key] for key in (
                    "session_id", "engine", "source_token",
                    "before_cursor", "page_limit")),
            )
            entries -= 1
            total_bytes -= int(victim["payload_bytes"])

    def _prune_details(self, connection: sqlite3.Connection) -> None:
        row = connection.execute(
            "SELECT COUNT(*) AS entries, COALESCE(SUM(payload_bytes), 0) AS bytes "
            "FROM history_turn_details"
        ).fetchone()
        entries = int(row["entries"])
        total_bytes = int(row["bytes"])
        max_entries = self.max_entries * 4
        while entries > max_entries or total_bytes > self.max_bytes:
            victim = connection.execute(
                """
                SELECT session_id, engine, source_token, turn_id, payload_bytes
                FROM history_turn_details ORDER BY accessed_at ASC LIMIT 1
                """
            ).fetchone()
            if victim is None:
                break
            connection.execute(
                """
                DELETE FROM history_turn_details
                WHERE session_id=? AND engine=? AND source_token=? AND turn_id=?
                """,
                tuple(victim[key] for key in (
                    "session_id", "engine", "source_token", "turn_id")),
            )
            entries -= 1
            total_bytes -= int(victim["payload_bytes"])

    def _prune_image_assets(self, connection: sqlite3.Connection) -> None:
        row = connection.execute(
            "SELECT COUNT(*) AS entries, COALESCE(SUM(payload_bytes), 0) AS bytes "
            "FROM history_image_assets"
        ).fetchone()
        entries = int(row["entries"])
        total_bytes = int(row["bytes"])
        max_entries = self.max_entries * 8
        max_bytes = max(1024 * 1024, self.max_bytes // 2)
        while entries > max_entries or total_bytes > max_bytes:
            victim = connection.execute(
                """
                SELECT session_id, engine, source_token, turn_id,
                       image_id, variant, payload_bytes
                FROM history_image_assets ORDER BY accessed_at ASC LIMIT 1
                """
            ).fetchone()
            if victim is None:
                break
            connection.execute(
                """
                DELETE FROM history_image_assets
                WHERE session_id=? AND engine=? AND source_token=?
                  AND turn_id=? AND image_id=? AND variant=?
                """,
                tuple(victim[key] for key in (
                    "session_id", "engine", "source_token", "turn_id",
                    "image_id", "variant")),
            )
            entries -= 1
            total_bytes -= int(victim["payload_bytes"])

    def invalidate_session(self, session_id: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM history_pages WHERE session_id=?", (session_id,))
            connection.execute(
                "DELETE FROM history_turn_details WHERE session_id=?",
                (session_id,),
            )
            connection.execute(
                "DELETE FROM history_image_assets WHERE session_id=?",
                (session_id,),
            )

    def close(self) -> None:
        """Compatibility hook for WrapperMachine shutdown.

        The store uses operation-scoped connections, so there is no live handle
        to close.  Keeping an explicit hook makes future connection pooling safe.
        """
