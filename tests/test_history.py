"""Zero-token unit tests for on-demand bulk history (GetHistory/History) and the
cursor-aware hello (fresh snapshots, delta replay on reconnect). No relay/wrapper/
cc/model — these exercise the wrapper handlers directly with a stub transport.

Run: ./.venv/bin/python -m pytest tests/test_history.py -q
"""
from __future__ import annotations

import asyncio
import base64
import io
import json
import threading
from types import SimpleNamespace

from claude_agent_sdk.types import (
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)

from cc_remote.protocol import (
    serialize, deserialize,
    GetHistory, GetHistoryImage, GetTurnDetail, History, HistoryImage,
    TurnDetail, HistoryInvalidated,
    UserMsg, AssistantMsgStart, AssistantMsgEnd, Delta, ProcessEvent,
    TurnEnd, TurnResult, Error, is_downstream,
)
from cc_remote.wrapper import machine as mm
from cc_remote.wrapper.history_store import (
    HistoryIndexStore,
    HistorySourceFingerprint,
    MaterializedHistoryPage,
    history_image_id,
    materialize_history_turns,
)
from cc_remote.wrapper.stream import (
    StreamTranslator,
    last_assistant_model,
    transcript_internal_user_events,
    translate_history,
)
from tests.test_multisession import _mk_machine, _mk_ctx


def test_protocol_v19_get_history_and_materialized_summary_roundtrip():
    gh = GetHistory(
        session_id="s1", client_id="c1", limit=50, detail="summary")
    assert deserialize(serialize(gh)) == gh
    h = History(session_id="s1", revision="test-revision",
                generation="test-generation", build_seq=4, live_seq=11,
                events=[{"type": "user_msg", "msg_id": "u1"}],
                turns=[{
                    "id": "u1", "prompt": "hello", "blocks": [],
                    "imageRefs": [{
                        "image_id": "u1.img.0", "media_type": "image/png",
                        "width": 1, "height": 1, "byte_size": 24,
                    }],
                    "done": True, "detailEventCount": 3,
                    "detailLoaded": False,
                }],
                detail="summary",
                has_more=True, oldest_id="u1", newest_id="u9",
                in_progress=True)
    got = deserialize(serialize(h))
    assert got.type == "history" and got.session_id == "s1" and got.has_more is True
    assert got.events[0]["type"] == "user_msg"
    assert got.turns[0].id == "u1" and got.turns[0].detailEventCount == 3
    assert got.turns[0].imageRefs[0]["image_id"] == "u1.img.0"
    assert got.detail == "summary"
    assert got.in_progress is True
    assert got.build_seq == 4 and got.live_seq == 11
    assert got.generation == "test-generation"
    assert got.authoritative is True and got.error is None

    marker = HistoryInvalidated(
        session_id="s1", revision="test-revision-2", reason="rollback"
    )
    assert deserialize(serialize(marker)) == marker
    assert is_downstream(marker) is True

    request = GetTurnDetail(
        session_id="s1", turn_id="u1", client_id="c1",
        revision="test-revision",
    )
    assert deserialize(serialize(request)) == request

    image_request = GetHistoryImage(
        session_id="s1", turn_id="u1", image_id="u1.img.0",
        variant="thumbnail", request_id="request-1", client_id="c1",
        revision="test-revision",
    )
    assert deserialize(serialize(image_request)) == image_request
    image = HistoryImage(
        session_id="s1", turn_id="u1", image_id="u1.img.0",
        variant="thumbnail", request_id="request-1",
        revision="test-revision", media_type="image/png",
        width=1, height=1, data="aW1n", to="c1",
    )
    assert deserialize(serialize(image)) == image


def test_materialized_summary_keeps_image_metadata_without_full_payload():
    png = b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\x0dIHDR" + (
        b"\x00\x00\x00\x02\x00\x00\x00\x03")
    encoded = base64.b64encode(png).decode("ascii")
    events = [
        {"type": "user_msg", "msg_id": "turn-image", "prompt": "look",
         "images": [{"media_type": "image/png", "data": encoded}]},
        {"type": "turn_end", "turn_id": "turn-image",
         "result": {"subtype": "success", "duration_ms": 1,
                    "is_error": False}},
    ]
    turns = materialize_history_turns(events)
    assert len(turns) == 1
    assert "images" not in turns[0]
    assert turns[0]["imageRefs"] == [{
        "image_id": history_image_id("turn-image", 0),
        "media_type": "image/png",
        "width": 2,
        "height": 3,
        "byte_size": len(png),
    }]
    assert encoded not in json.dumps(turns)


def test_materialized_summary_never_exposes_bare_error_sentinel():
    turns = materialize_history_turns((
        {"type": "user_msg", "msg_id": "message-1", "prompt": "go"},
        {"type": "turn_end", "turn_id": "turn-1", "result": {
            "subtype": "error", "is_error": True, "duration_ms": 0,
        }},
    ))
    assert turns[0]["error"] == "该轮未正常结束"
    detail = TurnDetail(
        session_id="s1", turn_id="u1", revision="test-revision",
        events=[{"type": "user_msg", "msg_id": "u1", "prompt": "hello"}],
    )
    assert deserialize(serialize(detail)) == detail


def test_history_revision_is_boot_scoped_and_monotonic():
    first, _ = _mk_machine()
    restarted, _ = _mk_machine()

    initial = first._history_revision("s1")
    assert restarted._history_revision("s1") != initial
    assert first._bump_history_revision("s1") != initial
    assert first._history_revision("s1").endswith("-1")


def test_history_read_does_not_block_serial_commands_or_duplicate_retries():
    """A slow transcript read must not hold query/interrupt command intake."""
    async def go():
        machine, _ = _mk_machine()
        history_started = asyncio.Event()
        release_history = asyncio.Event()
        query_seen = asyncio.Event()
        history_calls = 0

        async def process(command):
            nonlocal history_calls
            if command.type == "get_history":
                history_calls += 1
                history_started.set()
                await release_history.wait()
                return
            if command.type == "query":
                query_seen.set()

        machine._process_command = process
        history = SimpleNamespace(
            type="get_history", client_id="client-1", cmd_id="history-1")
        machine._start_history_command(history)
        await asyncio.wait_for(history_started.wait(), timeout=1)

        # A reconnect retry shares the in-flight read instead of scanning the
        # same rollout/transcript twice.
        machine._start_history_command(history)
        await machine._process_command_safely(SimpleNamespace(type="query"))
        assert query_seen.is_set()
        assert history_calls == 1

        release_history.set()
        await asyncio.gather(*machine._history_command_tasks.values())

    asyncio.run(go())


def test_distinct_history_commands_share_one_page_build_and_route_per_client():
    async def go():
        machine, transport = _mk_machine()
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def build(sid, *, before, limit, cwd, detail):
            nonlocal calls
            calls += 1
            assert (sid, before, limit, cwd) == ("session-1", None, 4, None)
            assert detail == "full"
            started.set()
            await release.wait()
            return History(
                session_id=sid,
                revision="revision-1",
                events=[{"type": "user_msg", "msg_id": "turn-1"}],
                has_more=False,
            )

        machine._build_requested_history = build
        first = SimpleNamespace(
            session_id="session-1", client_id="client-1",
            cmd_id="command-1", before=None, limit=4, cwd=None,
        )
        second = SimpleNamespace(
            session_id="session-1", client_id="client-2",
            cmd_id="command-2", before=None, limit=4, cwd=None,
        )
        tasks = [
            asyncio.create_task(machine._handle_get_history(first)),
            asyncio.create_task(machine._handle_get_history(second)),
        ]
        await asyncio.wait_for(started.wait(), timeout=1)
        await asyncio.sleep(0)
        assert calls == 1
        release.set()
        await asyncio.gather(*tasks)

        histories = [message for message in transport.sent
                     if isinstance(message, History)]
        assert {message.to for message in histories} == {"client-1", "client-2"}
        assert all(message.events == [
            {"type": "user_msg", "msg_id": "turn-1"}
        ] for message in histories)
        assert calls == 1

    asyncio.run(go())


def test_history_content_does_not_wait_for_external_ownership_scan():
    async def go():
        machine, _ = _mk_machine()
        machine._watch_session = lambda sid: machine._watch.setdefault(
            sid, {"engine": "codex"})
        prime_called = False

        async def blocked_prime(_sid):
            nonlocal prime_called
            prime_called = True
            await asyncio.Event().wait()

        async def build(sid, **kwargs):
            return History(
                session_id=sid,
                revision="revision-1",
                detail=kwargs["detail"],
            )

        machine._prime_codex_ownership = blocked_prime
        machine._build_history = build
        history = await asyncio.wait_for(machine._build_requested_history(
            "session-1", before=None, limit=4, cwd=None, detail="summary",
        ), timeout=0.1)
        assert history.session_id == "session-1"
        assert prime_called is False

    asyncio.run(go())


def test_get_turn_detail_is_routed_and_revision_bound(monkeypatch, tmp_path):
    rollout = tmp_path / "rollout.jsonl"
    rollout.write_text("{}\n")
    monkeypatch.setattr(mm, "codex_rollout_path", lambda _sid: str(rollout))
    events = (
        {"type": "user_msg", "sid": "session-1", "msg_id": "message-1",
         "prompt": "inspect"},
        {"type": "tool_use", "sid": "session-1", "tool_use_id": "tool-1",
         "tool": "Read", "input": {"file_path": "/tmp/example"}},
        {"type": "tool_result", "sid": "session-1", "tool_use_id": "tool-1",
         "content": "ok", "is_error": False},
        {"type": "turn_end", "sid": "session-1", "turn_id": "turn-1",
         "result": {"subtype": "success", "duration_ms": 1,
                    "is_error": False}},
    )

    async def go():
        machine, transport = _mk_machine()
        machine._history_index = HistoryIndexStore(tmp_path / "state")
        ctx = _mk_ctx("session-1", "session-1")
        ctx.engine = "codex"
        machine.sessions[ctx.key] = ctx
        source = HistorySourceFingerprint.capture(rollout)
        page = MaterializedHistoryPage(
            events=events, has_more=False,
            oldest_id="message-1", newest_id="message-1",
            turns=materialize_history_turns(events),
        )
        machine._history_index.put_page(
            "session-1", "codex", source, before=None, limit=4, page=page)
        revision = machine._history_revision("session-1")

        response = await machine._handle_get_turn_detail(SimpleNamespace(
            session_id="session-1", turn_id="message-1",
            client_id="client-1", revision=revision,
        ))
        assert isinstance(response, TurnDetail)
        assert response.to == "client-1" and response.sid == "session-1"
        assert response.authoritative is True
        assert response.events == list(events)

        stale = await machine._handle_get_turn_detail(SimpleNamespace(
            session_id="session-1", turn_id="message-1",
            client_id="client-2", revision="old-revision",
        ))
        assert stale.to == "client-2"
        assert stale.authoritative is False and stale.events == []

        assert transport.sent[-2:] == [response, stale]

    asyncio.run(go())


def test_get_history_image_is_revision_bound_lazy_and_cached(
    monkeypatch, tmp_path,
):
    from PIL import Image

    buffer = io.BytesIO()
    Image.new("RGB", (640, 320), (26, 84, 140)).save(buffer, "PNG")
    raw = buffer.getvalue()
    encoded = base64.b64encode(raw).decode("ascii")
    rollout = tmp_path / "rollout.jsonl"
    rollout.write_text("{}\n")
    monkeypatch.setattr(mm, "codex_rollout_path", lambda _sid: str(rollout))
    events = (
        {"type": "user_msg", "sid": "session-1", "msg_id": "message-1",
         "prompt": "inspect", "images": [{
             "media_type": "image/png", "data": encoded,
         }]},
        {"type": "turn_end", "sid": "session-1", "turn_id": "turn-1",
         "result": {"subtype": "success", "duration_ms": 1,
                    "is_error": False}},
    )

    async def go():
        machine, transport = _mk_machine()
        machine._history_index = HistoryIndexStore(tmp_path / "state")
        ctx = _mk_ctx("session-1", "session-1")
        ctx.engine = "codex"
        machine.sessions[ctx.key] = ctx
        source = HistorySourceFingerprint.capture(rollout)
        page = MaterializedHistoryPage(
            events=events, has_more=False,
            oldest_id="message-1", newest_id="message-1",
            turns=materialize_history_turns(events),
        )
        machine._history_index.put_page(
            "session-1", "codex", source, before=None, limit=4, page=page)
        revision = machine._history_revision("session-1")
        image_id = history_image_id("message-1", 0)

        thumbnail = await machine._handle_get_history_image(SimpleNamespace(
            session_id="session-1", turn_id="message-1", image_id=image_id,
            variant="thumbnail", request_id="request-thumb",
            client_id="client-1", revision=revision,
        ))
        assert isinstance(thumbnail, HistoryImage)
        assert thumbnail.to == "client-1" and thumbnail.error is None
        assert thumbnail.media_type == "image/webp"
        assert thumbnail.width == 360 and thumbnail.height == 180
        assert len(base64.b64decode(thumbnail.data)) < len(raw)

        full = await machine._handle_get_history_image(SimpleNamespace(
            session_id="session-1", turn_id="message-1", image_id=image_id,
            variant="full", request_id="request-full",
            client_id="client-1", revision=revision,
        ))
        assert full.error is None and full.media_type == "image/png"
        assert full.width == 640 and full.height == 320
        assert base64.b64decode(full.data) == raw

        # A second request is served by the source-bound bounded image cache;
        # decoding/thumbnailing must not run again.
        monkeypatch.setattr(
            mm, "_render_history_image",
            lambda *_args: (_ for _ in ()).throw(AssertionError("re-rendered")),
        )
        cached = await machine._handle_get_history_image(SimpleNamespace(
            session_id="session-1", turn_id="message-1", image_id=image_id,
            variant="thumbnail", request_id="request-cached",
            client_id="client-1", revision=revision,
        ))
        assert cached.error is None and cached.data == thumbnail.data

        stale = await machine._handle_get_history_image(SimpleNamespace(
            session_id="session-1", turn_id="message-1", image_id=image_id,
            variant="full", request_id="request-stale",
            client_id="client-2", revision="old-revision",
        ))
        assert stale.to == "client-2" and stale.data is None
        assert stale.error == "会话历史已更新，请重新加载图片"
        assert transport.sent[-4:] == [thumbnail, full, cached, stale]

    asyncio.run(go())


def test_history_missing_or_stale_cwd_does_not_cache_empty_page(monkeypatch, tmp_path):
    source = tmp_path / "existing.jsonl"
    source.write_text('{"type":"user"}\n', encoding="utf-8")
    monkeypatch.setattr(mm, "transcript_path", lambda sid: str(source))
    monkeypatch.setattr(mm, "transcript_timestamps", lambda sid: {})
    monkeypatch.setattr(mm, "transcript_internal_user_events", lambda sid: {})
    monkeypatch.setattr(mm, "translate_subagent_history", lambda *a: [])
    calls = []
    def read(sid, directory=None):
        calls.append(directory)
        return [object()] if directory is None else []
    monkeypatch.setattr(mm, "get_session_messages", read)
    monkeypatch.setattr(mm, "last_assistant_model", lambda msgs: None)
    monkeypatch.setattr(mm, "translate_history", lambda msgs, *a, **kw:
                        [UserMsg(prompt="retained", msg_id="u1")] if msgs else [])

    async def go():
        for index, hint in enumerate((None, "stale-project")):
            machine, _ = _mk_machine()
            machine._history_index = HistoryIndexStore(tmp_path / f"state-{index}")
            calls.clear()
            first = await machine._build_history("s1", cwd_hint=hint, limit=4, detail="summary")
            assert len(first.turns) == 1
            assert calls == ([None] if hint is None else [hint, None])
            count = len(calls)
            cached = await machine._build_history("s1", cwd_hint="correct-project", limit=4, detail="summary")
            assert len(cached.turns) == 1
            assert len(calls) == count
    asyncio.run(go())


def test_history_build_materializes_source_bound_shadow_page(
    monkeypatch, tmp_path,
):
    rollout = tmp_path / "rollout.jsonl"
    rollout.write_text("".join(json.dumps(row) + "\n" for row in [
        {"timestamp": "2026-01-01T00:00:00Z", "type": "session_meta",
         "payload": {"id": "session-1"}},
        {"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
         "payload": {"type": "task_started", "turn_id": "turn-1"}},
        {"timestamp": "2026-01-01T00:00:02Z", "type": "event_msg",
         "payload": {"type": "user_message", "message": "hello"}},
        {"timestamp": "2026-01-01T00:00:03Z", "type": "event_msg",
         "payload": {"type": "task_complete", "turn_id": "turn-1",
                     "last_agent_message": "world"}},
    ]))
    monkeypatch.setattr(mm, "codex_rollout_path", lambda _sid: str(rollout))
    translate = mm.codex_translate_history
    translate_calls = 0

    def counted_translate(*args, **kwargs):
        nonlocal translate_calls
        translate_calls += 1
        return translate(*args, **kwargs)

    monkeypatch.setattr(mm, "codex_translate_history", counted_translate)

    async def go():
        machine, _ = _mk_machine()
        ctx = _mk_ctx("session-1", "session-1")
        ctx.engine = "codex"
        machine.sessions[ctx.key] = ctx

        history = await machine._build_history("session-1", limit=4)
        source = HistorySourceFingerprint.capture(rollout)
        indexed = machine._history_index.get_page(
            "session-1", "codex", source, before=None, limit=4)
        assert indexed is not None
        assert list(indexed.events) == history.events
        assert indexed.oldest_id == history.oldest_id
        assert indexed.newest_id == history.newest_id

        # A second identical build must preserve exact shadow parity.
        repeated = await machine._build_history("session-1", limit=4)
        repeated_page = machine._history_index.get_page(
            "session-1", "codex", source, before=None, limit=4)
        assert repeated_page is not None
        assert repeated_page.semantically_equals(indexed)
        assert [row["type"] for row in repeated.events] == [
            row["type"] for row in history.events]
        assert translate_calls == 1

        # A destructive revision barrier invalidates the materialized page even
        # if a coarse filesystem timestamp happens not to change.
        machine._bump_history_revision("session-1")
        await machine._build_history("session-1", limit=4)
        assert translate_calls == 2

        # Ordinary append invalidation is source-fingerprint based.
        with rollout.open("a") as stream:
            stream.write(json.dumps({
                "timestamp": "2026-01-01T00:00:04Z",
                "type": "event_msg",
                "payload": {"type": "task_started", "turn_id": "turn-2"},
            }) + "\n")
        await machine._build_history("session-1", limit=4)
        assert translate_calls == 3

        summary = await machine._build_history(
            "session-1", limit=4, detail="summary")
        assert summary.detail == "summary"
        assert summary.turns
        assert all(row["type"] in {"model", "effort"}
                   for row in summary.events)
        assert len(summary.model_dump_json()) < len(history.model_dump_json())

    asyncio.run(go())


def test_running_codex_summary_keeps_bounded_live_projection(
    monkeypatch, tmp_path,
):
    rollout = tmp_path / "rollout-live-summary.jsonl"
    rollout.write_text("".join(json.dumps(row) + "\n" for row in [
        {"timestamp": "2026-01-01T00:00:00Z", "type": "session_meta",
         "payload": {"id": "session-live"}},
        {"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
         "payload": {"type": "task_started", "turn_id": "turn-live"}},
        {"timestamp": "2026-01-01T00:00:02Z", "type": "event_msg",
         "payload": {"type": "user_message", "message": "inspect"}},
        {"timestamp": "2026-01-01T00:00:03Z", "type": "event_msg",
         "payload": {"type": "agent_message", "phase": "commentary",
                     "message": "working now"}},
        {"timestamp": "2026-01-01T00:00:04Z", "type": "event_msg",
         "payload": {"type": "context_compacted"}},
    ]))
    monkeypatch.setattr(
        mm, "codex_rollout_path", lambda _sid: str(rollout))

    async def go():
        machine, _ = _mk_machine()
        ctx = _mk_ctx("session-live", "session-live")
        ctx.engine = "codex"
        ctx.state = "running"
        machine.sessions[ctx.key] = ctx

        history = await machine._build_history(
            "session-live", limit=4, detail="summary")

        assert history.detail == "summary"
        assert len(history.turns) == 1
        blocks = history.turns[0].blocks
        assert any(
            block.get("kind") == "text"
            and block.get("channel") == "commentary"
            and block.get("text") == "working now"
            for block in blocks
        )
        assert any(
            block.get("kind") == "process"
            and block.get("processKind") == "compaction"
            for block in blocks
        )
        assert all(row["type"] in {"model", "effort"}
                   for row in history.events)

    asyncio.run(go())


def test_history_index_reuses_only_verified_append_prefix(tmp_path):
    transcript = tmp_path / "claude.jsonl"
    transcript.write_bytes(b'{"type":"user","value":"old"}\n')
    store = HistoryIndexStore(tmp_path / "state-append")
    old_source = HistorySourceFingerprint.capture(transcript)
    page = MaterializedHistoryPage(
        events=({"type": "user_msg", "msg_id": "old", "prompt": "old"},),
        has_more=False,
        oldest_id="old",
        newest_id="old",
        turns=(),
    )
    store.put_page(
        "session-append", "claude", old_source,
        before=None, limit=4, page=page,
    )

    with transcript.open("ab") as stream:
        stream.write(b'{"type":"assistant","value":"new"}\n')
    appended = HistorySourceFingerprint.capture(transcript)
    reused = store.get_append_page(
        "session-append", "claude", appended, before=None, limit=4)
    assert reused is not None and reused.newest_id == "old"

    # Same path/inode and a larger size are insufficient: an in-place rewrite
    # is a destructive source change, not a safe stale-while-revalidate prefix.
    transcript.write_bytes(
        b'{"type":"user","value":"rewritten"}\n'
        b'{"type":"assistant","value":"more"}\n')
    rewritten = HistorySourceFingerprint.capture(transcript)
    assert store.get_append_page(
        "session-append", "claude", rewritten, before=None, limit=4,
    ) is None


def test_claude_history_append_paints_cached_page_before_revalidation(
        monkeypatch, tmp_path):
    transcript = tmp_path / "claude.jsonl"
    transcript.write_bytes(b'{"type":"user","value":"old"}\n')
    monkeypatch.setattr(mm, "transcript_path", lambda _sid: str(transcript))
    monkeypatch.setattr(
        mm,
        "get_session_messages",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("append-stale first paint performed a full scan")),
    )

    async def go():
        machine, _ = _mk_machine()
        machine._history_index = HistoryIndexStore(tmp_path / "state-fast")
        ctx = _mk_ctx("claude-fast", "claude-fast")
        ctx.engine = "claude"
        machine.sessions[ctx.key] = ctx
        old_source = HistorySourceFingerprint.capture(transcript)
        events = (
            {"type": "user_msg", "sid": "claude-fast",
             "msg_id": "old", "prompt": "old"},
            {"type": "turn_end", "sid": "claude-fast",
             "result": {"subtype": "success", "duration_ms": 1,
                        "is_error": False}},
        )
        page = MaterializedHistoryPage(
            events=events,
            has_more=False,
            oldest_id="old",
            newest_id="old",
            turns=materialize_history_turns(events),
        )
        machine._history_index.put_page(
            "claude-fast", "claude", old_source,
            before=None, limit=4, page=page,
        )
        with transcript.open("ab") as stream:
            stream.write(b'{"type":"assistant","value":"new"}\n')
        refreshes = []
        monkeypatch.setattr(
            machine,
            "_schedule_history_refresh",
            lambda sid, **kwargs: refreshes.append((sid, kwargs)),
        )

        history = await machine._build_requested_history(
            "claude-fast", before=None, limit=4, cwd=ctx.cwd,
            detail="summary",
        )

        assert [turn.prompt for turn in history.turns] == ["old"]
        assert refreshes and refreshes[0][0] == "claude-fast"

    asyncio.run(go())


def test_claude_history_refresh_coalesces_appends_during_full_scan(monkeypatch):
    async def go():
        machine, transport = _mk_machine()
        entered = asyncio.Event()
        release = asyncio.Event()
        builds = 0

        async def build(sid, **_kwargs):
            nonlocal builds
            builds += 1
            if builds == 1:
                entered.set()
                await release.wait()
            return History(
                session_id=sid,
                revision="refresh-rev",
                events=[],
                turns=[],
                detail="summary",
                has_more=False,
            )

        monkeypatch.setattr(machine, "_build_history", build)
        args = {
            "before": None, "limit": 4, "cwd": "/repo", "detail": "summary",
        }
        machine._schedule_history_refresh("claude-refresh", **args)
        await entered.wait()
        machine._schedule_history_refresh("claude-refresh", **args)
        machine._schedule_history_refresh("claude-refresh", **args)
        release.set()
        await asyncio.gather(*list(machine._history_refresh_tasks.values()))

        assert builds == 2
        assert len([row for row in transport.sent
                    if isinstance(row, History)]) == 2

    asyncio.run(go())


def test_external_codex_turn_is_history_activity_not_resident_state(monkeypatch):
    """A mirrored native turn marks History active without owning Stop."""
    monkeypatch.setattr(mm, "codex_rollout_path", lambda _sid: None)

    async def go():
        machine, _ = _mk_machine()
        ctx = _mk_ctx("external-codex", "external-codex")
        ctx.engine = "codex"
        ctx.state = "idle"
        machine.sessions[ctx.key] = ctx
        machine._watch["external-codex"] = {
            "engine": "codex",
            "external": True,
            "active_external_turns": {"native-turn": 1.0},
            "takeover_pending": None,
        }

        history = await machine._build_history("external-codex", limit=20)

        assert history.external is True
        assert history.in_progress is True
        assert ctx.state == "idle"

    asyncio.run(go())


def test_active_external_codex_history_marks_growing_snapshot(monkeypatch, tmp_path):
    rollout = tmp_path / "active-external.jsonl"
    rollout.write_text(json.dumps({
        "timestamp": "2026-01-01T00:00:00Z",
        "type": "session_meta",
        "payload": {"id": "external-codex"},
    }) + "\n")
    monkeypatch.setattr(
        mm, "codex_rollout_path", lambda _sid: str(rollout))
    translate_kwargs = []

    def translate(*_args, **kwargs):
        translate_kwargs.append(kwargs)
        return [], None

    monkeypatch.setattr(mm, "codex_translate_history", translate)

    async def go():
        machine, _ = _mk_machine()
        ctx = _mk_ctx("external-codex", "external-codex")
        ctx.engine = "codex"
        ctx.state = "idle"
        machine.sessions[ctx.key] = ctx
        machine._watch["external-codex"] = {
            "engine": "codex",
            "external": True,
            "active_external_turns": {"native-turn": 1.0},
            "takeover_pending": None,
        }

        await machine._build_history("external-codex", limit=20)

        assert translate_kwargs
        assert translate_kwargs[0]["snapshot_in_progress"] is True

    asyncio.run(go())


def test_inflight_history_keeps_pre_rollback_revision(monkeypatch):
    entered = threading.Event()
    release = threading.Event()

    def delayed_messages(_sid, directory=None):
        entered.set()
        assert release.wait(2)
        return []

    monkeypatch.setattr(mm, "transcript_path", lambda _sid: None)
    monkeypatch.setattr(mm, "get_session_messages", delayed_messages)
    monkeypatch.setattr(mm, "transcript_timestamps", lambda _sid: {})
    monkeypatch.setattr(mm, "translate_history", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(mm, "translate_subagent_history", lambda *_args: [])

    async def go():
        machine, _ = _mk_machine()
        before = machine._history_revision("s1")
        task = asyncio.create_task(machine._build_history("s1", limit=20))
        assert await asyncio.to_thread(entered.wait, 2)
        after = machine._bump_history_revision("s1")
        release.set()
        history = await task
        assert history.revision == before
        assert history.revision != after

    asyncio.run(go())


def test_newest_history_builds_are_monotonic_and_capture_live_seq(monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    calls = 0
    calls_lock = threading.Lock()

    def delayed_first(_sid, directory=None):
        nonlocal calls
        with calls_lock:
            calls += 1
            current = calls
        if current == 1:
            entered.set()
            assert release.wait(2)
        return []

    monkeypatch.setattr(mm, "transcript_path", lambda _sid: None)
    monkeypatch.setattr(mm, "get_session_messages", delayed_first)
    monkeypatch.setattr(mm, "transcript_timestamps", lambda _sid: {})
    monkeypatch.setattr(mm, "translate_history", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(mm, "translate_subagent_history", lambda *_args: [])

    async def go():
        machine, _ = _mk_machine()
        ctx = _mk_ctx("s1", "s1")
        ctx.seq = 7
        machine.sessions["s1"] = ctx

        older_task = asyncio.create_task(machine._build_history("s1", limit=20))
        assert await asyncio.to_thread(entered.wait, 2)
        ctx.seq = 8
        newer = await machine._build_history("s1", limit=20)
        page = await machine._build_history("s1", before="older", limit=20)
        release.set()
        older = await older_task

        assert older.build_seq < newer.build_seq
        assert older.live_seq == 7
        assert newer.live_seq == 8
        assert page.build_seq == newer.build_seq

    asyncio.run(go())


def test_hello_sends_snapshots_and_control_state_without_replay_flood():
    """Hello sends one snapshot plus authoritative control state per resident
    session, but no buffered narrative replay."""
    async def go():
        m, tr = _mk_machine()
        for key in ("s1", "s2"):
            ctx = _mk_ctx(key, key)
            for i in range(5):
                ev = UserMsg(msg_id=f"{key}-{i}", prompt="x")
                ev.seq = ctx.next_seq()
                ev.sid = key
                ctx.buffer.append(ev)
            m.sessions[key] = ctx
        await m._handle_client_hello(SimpleNamespace(client_id="c1"))
        types = [msg.type for msg in tr.sent]
        assert types == ["snapshot", "perm", "snapshot", "perm"]
        assert "replay_start" not in types and "user_msg" not in types
        assert all(msg.to == "c1" for msg in tr.sent)     # routed to the requesting client
    asyncio.run(go())


def test_hello_with_cursor_replays_only_missing_tail():
    async def go():
        m, tr = _mk_machine()
        ctx = _mk_ctx("s1", "s1")
        for i in range(1, 4):
            ev = UserMsg(msg_id=f"m{i}", prompt="x")
            ev.seq = i
            ctx.seq = i
            ev.sid = "s1"
            ctx.buffer.append(ev)
        m.sessions["s1"] = ctx

        await m._handle_client_hello(SimpleNamespace(
            client_id="c1", cursors={"s1": 2},
            generations={"s1": m.instance_id}, last_seq=None))

        assert [msg.type for msg in tr.sent] == [
            "replay_start", "user_msg", "replay_end", "session_control", "perm"]
        assert tr.sent[1].msg_id == "m3"
        assert all(msg.to == "c1" for msg in tr.sent)

    asyncio.run(go())


def test_fresh_hello_replays_only_current_inflight_turn_after_snapshot():
    async def go():
        m, tr = _mk_machine()
        ctx = _mk_ctx("s1", "s1")
        ctx.state = "running"
        for i, prompt in enumerate(("old", "current"), 1):
            ev = UserMsg(msg_id=f"m{i}", prompt=prompt)
            ev.seq = ctx.next_seq()
            ctx.buffer.append(ev)
            delta = Delta(message_id=f"a{i}", text=prompt)
            delta.seq = ctx.next_seq()
            ctx.buffer.append(delta)
        m.sessions["s1"] = ctx

        await m._handle_client_hello(SimpleNamespace(
            client_id="c1", cursors=None, generations=None, last_seq=None))

        assert [msg.type for msg in tr.sent] == [
            "snapshot", "replay_start", "user_msg", "delta", "replay_end",
            "perm"]
        assert tr.sent[2].prompt == "current"
        assert all(msg.to == "c1" for msg in tr.sent)

    asyncio.run(go())


def test_get_history_returns_one_bulk_frame(monkeypatch):
    canned = [
        UserMsg(msg_id="u1", prompt="hi"),
        AssistantMsgStart(message_id="a1"),
        Delta(message_id="a1", text="hello"),
        TurnEnd(result=TurnResult(subtype="success", duration_ms=0, is_error=False)),
    ]
    monkeypatch.setattr(mm, "get_session_messages", lambda sid, directory=None: ["m"])
    monkeypatch.setattr(mm, "translate_history", lambda msgs, mx, timestamps=None: [e.model_copy() for e in canned])
    # A proxy transcript may expose its upstream model. The resident SDK control
    # state remains authoritative for both the selected Claude alias and effort.
    monkeypatch.setattr(mm, "last_assistant_model", lambda msgs: "glm-5.2")

    async def go():
        m, tr = _mk_machine()
        ctx = _mk_ctx("sX", "sX")
        ctx.sdk = SimpleNamespace(model="claude-opus-4-8", effort="max")
        ctx.state = "running"
        m.sessions["sX"] = ctx
        await m._handle_get_history(SimpleNamespace(
            session_id="sX", client_id="c1", cwd="/tmp/x", type="get_history"))
        assert len(tr.sent) == 1                          # ONE bulk frame, not N tiny frames
        hist = tr.sent[0]
        assert hist.type == "history" and hist.session_id == "sX" and hist.to == "c1"
        assert hist.has_more is False
        assert hist.in_progress is True
        assert hist.oldest_id == "u1" and hist.newest_id == "u1"
        # Current model + effort precede the translated transcript narrative.
        assert [(event["type"], event.get("model") or event.get("effort"))
                for event in hist.events[:2]] == [
                    ("model", "claude-opus-4-8"), ("effort", "max")]
        assert [e["type"] for e in hist.events[2:]] == [
            "user_msg", "assistant_msg_start", "delta", "turn_end"]
        # every event is stamped with the session id so the client routes them right
        assert all(e["sid"] == "sX" for e in hist.events)
    asyncio.run(go())


def test_oversized_single_turn_is_compacted_below_transport_cap(monkeypatch):
    canned = [
        UserMsg(msg_id="u1", prompt="hi"),
        AssistantMsgStart(message_id="a1"),
        Delta(message_id="a1", text="x" * 200_000),
        TurnEnd(result=TurnResult(
            subtype="success", duration_ms=1, is_error=False)),
    ]
    monkeypatch.setattr(mm, "get_session_messages", lambda sid, directory=None: ["m"])
    monkeypatch.setattr(
        mm, "translate_history",
        lambda msgs, mx, timestamps=None: [event.model_copy() for event in canned],
    )
    monkeypatch.setattr(mm, "last_assistant_model", lambda msgs: None)

    async def go():
        machine, _ = _mk_machine()
        machine.cfg.ws_max_size_bytes = 64 * 1024
        history = await machine._build_history("s1", limit=60)
        assert len(history.model_dump_json().encode()) < machine.cfg.ws_max_size_bytes
        assert any(row["type"] == "error" for row in history.events)

    asyncio.run(go())


def test_many_turn_history_shrinks_with_logarithmic_serializations(monkeypatch):
    canned = []
    for index in range(256):
        uid = f"u{index}"
        canned.extend([
            UserMsg(msg_id=uid, prompt="q"),
            Delta(message_id=f"a{index}", text="x" * 4096),
            TurnEnd(result=TurnResult(
                subtype="success", duration_ms=1, is_error=False)),
        ])
    monkeypatch.setattr(mm, "get_session_messages", lambda sid, directory=None: ["m"])
    monkeypatch.setattr(
        mm, "translate_history",
        lambda msgs, mx, timestamps=None: [event.model_copy() for event in canned],
    )
    monkeypatch.setattr(mm, "last_assistant_model", lambda msgs: None)

    calls = 0
    original = History.model_dump_json

    def counted(self, *args, **kwargs):
        nonlocal calls
        calls += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(History, "model_dump_json", counted)

    async def go():
        machine, _ = _mk_machine()
        machine.cfg.ws_max_size_bytes = 64 * 1024
        history = await machine._build_history("s1")
        assert history.has_more is True
        assert history.newest_id == "u255"
        assert len(history.model_dump_json().encode()) < machine.cfg.ws_max_size_bytes

    asyncio.run(go())
    # 256 linear removals were the prior failure mode. Binary search plus final
    # assertions stays comfortably below this fixed ceiling.
    assert calls < 20


def test_oversized_transcript_is_rejected_before_full_parse(monkeypatch, tmp_path):
    source = tmp_path / "huge.jsonl"
    source.write_text("{}\n")
    parsed = False

    def should_not_parse(*args, **kwargs):
        nonlocal parsed
        parsed = True
        raise AssertionError("history parser should not run")

    monkeypatch.setattr(mm, "transcript_path", lambda sid: str(source))
    monkeypatch.setattr(mm.os.path, "getsize", lambda path: 100_000_000)
    monkeypatch.setattr(mm, "get_session_messages", should_not_parse)

    async def go():
        machine, _ = _mk_machine()
        machine.cfg.history_source_max_bytes = 64 * 1024 * 1024
        history = await machine._build_history("s1", limit=60)
        assert history.events[0]["type"] == "error"
        assert "HISTORY_SOURCE_MAX_BYTES" in history.events[0]["message"]

    asyncio.run(go())
    assert parsed is False


def test_oversized_codex_rollout_reads_recent_turn_window(monkeypatch, tmp_path):
    source = tmp_path / "huge-rollout.jsonl"
    with source.open("wb") as rollout:
        # A sparse prefix makes the source larger than the Claude transcript
        # safety cap without allocating or parsing a giant test fixture.
        rollout.seek(70 * 1024 * 1024)
        rollout.write(b"\n")
        for index in range(1, 4):
            for row in (
                {"timestamp": f"2026-01-01T00:0{index}:01Z",
                 "type": "event_msg",
                 "payload": {"type": "task_started",
                             "turn_id": f"turn-{index}"}},
                {"timestamp": f"2026-01-01T00:0{index}:02Z",
                 "type": "event_msg",
                 "payload": {"type": "user_message",
                             "message": f"question {index}"}},
                {"timestamp": f"2026-01-01T00:0{index}:03Z",
                 "type": "event_msg",
                 "payload": {"type": "agent_message",
                             "message": f"answer {index}"}},
                {"timestamp": f"2026-01-01T00:0{index}:04Z",
                 "type": "event_msg",
                 "payload": {"type": "task_complete",
                             "turn_id": f"turn-{index}"}},
            ):
                rollout.write((json.dumps(row) + "\n").encode())

    monkeypatch.setattr(mm, "codex_rollout_path", lambda _sid: str(source))

    async def go():
        machine, _ = _mk_machine()
        machine.cfg.history_source_max_bytes = 64 * 1024 * 1024
        ctx = _mk_ctx("codex-large", "codex-large")
        ctx.engine = "codex"
        machine.sessions[ctx.key] = ctx

        newest = await machine._build_history("codex-large", limit=2)
        assert [row["prompt"] for row in newest.events
                if row["type"] == "user_msg"] == ["question 2", "question 3"]
        assert newest.oldest_id == "turn-2"
        assert newest.newest_id == "turn-3"
        assert newest.has_more is True
        assert not any("HISTORY_SOURCE_MAX_BYTES" in row.get("message", "")
                       for row in newest.events)

    asyncio.run(go())


def test_compacted_codex_tail_recovers_omitted_current_prompt(
        monkeypatch, tmp_path):
    source = tmp_path / "compacted-rollout.jsonl"
    rows = [
        {"timestamp": "2026-01-01T00:00:00Z", "type": "session_meta",
         "payload": {"id": "codex-compacted"}},
        {"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
         "payload": {"type": "task_started", "turn_id": "turn-long"}},
        {"timestamp": "2026-01-01T00:00:02Z", "type": "event_msg",
         "payload": {"type": "user_message",
                     "message": "the prompt before compact"}},
        {"timestamp": "2026-01-01T00:00:03Z", "type": "event_msg",
         "payload": {"type": "agent_message", "phase": "commentary",
                     "message": "work before compact"}},
        # One compact record is larger than the configured source window. The
        # newest-page translator therefore starts after it, just like a real
        # multi-hundred-MiB turn, while the prompt remains recoverable from the
        # already-discovered task boundary.
        {"timestamp": "2026-01-01T00:00:04Z", "type": "compacted",
         "payload": {"replacement_history": ["x" * (1100 * 1024)]}},
        {"timestamp": "2026-01-01T00:00:05Z", "type": "event_msg",
         "payload": {"type": "context_compacted"}},
        {"timestamp": "2026-01-01T00:00:06Z", "type": "event_msg",
         "payload": {"type": "agent_message", "phase": "final_answer",
                     "message": "answer after compact"}},
        {"timestamp": "2026-01-01T00:00:07Z", "type": "event_msg",
         "payload": {"type": "task_complete", "turn_id": "turn-long"}},
    ]
    source.write_text("".join(json.dumps(row) + "\n" for row in rows))
    monkeypatch.setattr(mm, "codex_rollout_path", lambda _sid: str(source))

    async def go():
        machine, _ = _mk_machine()
        machine.cfg.codex_history_window_max_bytes = 1024 * 1024
        ctx = _mk_ctx("codex-compacted", "codex-compacted")
        ctx.engine = "codex"
        machine.sessions[ctx.key] = ctx

        newest = await machine._build_history("codex-compacted", limit=60)
        assert [row["prompt"] for row in newest.events
                if row["type"] == "user_msg"] == [
                    "the prompt before compact"
                ]
        assert any(row.get("type") == "delta"
                   and "answer after compact" in row.get("text", "")
                   for row in newest.events)
        assert newest.oldest_id == "turn-long"
        assert newest.has_more is True

    asyncio.run(go())


def test_compact_continuation_split_from_terminal_is_not_an_error_turn(
        tmp_path):
    """A compact-continuation turn (turn_context + context_compacted + visible
    assistant content) that never reaches its terminal record before the next
    user_message — e.g. a history page that split the continuation from its
    task_complete — must close as a normal truncated turn, not a synthetic
    error turn."""
    from cc_remote.wrapper.codex_stream import codex_translate_history
    source = tmp_path / "compact-dangling.jsonl"
    rows = [
        {"timestamp": "2026-01-01T00:00:00Z", "type": "session_meta",
         "payload": {"id": "s"}},
        {"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
         "payload": {"type": "task_started", "turn_id": "t1"}},
        {"timestamp": "2026-01-01T00:00:02Z", "type": "event_msg",
         "payload": {"type": "user_message", "message": "first"}},
        {"timestamp": "2026-01-01T00:00:03Z", "type": "event_msg",
         "payload": {"type": "agent_message", "phase": "final_answer",
                     "message": "first answer"}},
        {"timestamp": "2026-01-01T00:00:04Z", "type": "event_msg",
         "payload": {"type": "task_complete", "turn_id": "t1"}},
        # compact continuation with visible content but NO terminal record
        # before the next user turn (mimics a page split).
        {"timestamp": "2026-01-01T00:00:05Z", "type": "turn_context",
         "payload": {"turn_id": "t2"}},
        {"timestamp": "2026-01-01T00:00:06Z", "type": "event_msg",
         "payload": {"type": "context_compacted"}},
        {"timestamp": "2026-01-01T00:00:07Z", "type": "event_msg",
         "payload": {"type": "agent_message", "phase": "final_answer",
                     "message": "continuation answer"}},
        # next user turn closes the dangling continuation.
        {"timestamp": "2026-01-01T00:00:08Z", "type": "event_msg",
         "payload": {"type": "user_message", "message": "second"}},
        {"timestamp": "2026-01-01T00:00:09Z", "type": "event_msg",
         "payload": {"type": "agent_message", "phase": "final_answer",
                     "message": "second answer"}},
        {"timestamp": "2026-01-01T00:00:10Z", "type": "event_msg",
         "payload": {"type": "task_complete", "turn_id": "t3"}},
    ]
    source.write_text("".join(json.dumps(row) + "\n" for row in rows))
    events, _ = codex_translate_history(str(source), tool_result_max=4096)
    turn_ends = [e for e in events if type(e).__name__ == "TurnEnd"]
    assert [e.turn_id for e in turn_ends] == ["t1", None, "t3"]
    assert [e.result.subtype for e in turn_ends] == [
        "success", "success", "success"]
    assert [e.result.is_error for e in turn_ends] == [False, False, False]


def test_context_compacted_before_user_belongs_to_that_user_turn(tmp_path):
    """A task can compact before Codex records its clean user_message.

    The compact marker is process metadata for the upcoming user turn, not an
    empty assistant-only turn that should be closed as a synthetic error.
    """
    from cc_remote.wrapper.codex_stream import codex_translate_history

    source = tmp_path / "compact-before-user.jsonl"
    rows = [
        {"timestamp": "2026-01-01T00:00:00Z", "type": "session_meta",
         "payload": {"id": "s"}},
        {"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
         "payload": {"type": "task_started", "turn_id": "turn-1"}},
        {"timestamp": "2026-01-01T00:00:02Z", "type": "compacted",
         "payload": {"replacement_history": []}},
        {"timestamp": "2026-01-01T00:00:03Z", "type": "event_msg",
         "payload": {"type": "context_compacted"}},
        {"timestamp": "2026-01-01T00:00:04Z", "type": "turn_context",
         "payload": {"turn_id": "turn-1"}},
        {"timestamp": "2026-01-01T00:00:05Z", "type": "event_msg",
         "payload": {"type": "user_message", "message": "continue"}},
        {"timestamp": "2026-01-01T00:00:06Z", "type": "event_msg",
         "payload": {"type": "agent_message", "phase": "final_answer",
                     "message": "done"}},
        {"timestamp": "2026-01-01T00:00:06Z", "type": "response_item",
         "payload": {"type": "message", "id": "message-1",
                     "role": "assistant", "phase": "final_answer",
                     "content": [{"type": "output_text", "text": "done"}]}},
        {"timestamp": "2026-01-01T00:00:07Z", "type": "event_msg",
         "payload": {"type": "task_complete", "turn_id": "turn-1",
                     "last_agent_message": "done"}},
    ]
    source.write_text("".join(json.dumps(row) + "\n" for row in rows))

    events, _ = codex_translate_history(str(source), tool_result_max=4096)
    user_index = next(
        index for index, event in enumerate(events)
        if isinstance(event, UserMsg)
    )
    compact_index = next(
        index for index, event in enumerate(events)
        if isinstance(event, ProcessEvent) and event.kind == "compaction"
    )
    assert compact_index > user_index
    assert events[compact_index].turn_id == "turn-1"
    assert events[compact_index].ts == 1767225603.0
    assert len([event for event in events if isinstance(event, TurnEnd)]) == 1
    assert not any(isinstance(event, Error) for event in events)

    turns = materialize_history_turns([
        event.model_dump(mode="json") for event in events
    ], include_live_detail=True)
    assert [(turn["prompt"], turn["done"], turn.get("error"))
            for turn in turns] == [("continue", True, None)]
    assert any(
        block.get("processKind") == "compaction"
        for block in turns[0]["blocks"]
    )


def test_pending_compaction_does_not_cross_a_new_task_owner(tmp_path):
    from cc_remote.wrapper.codex_stream import codex_translate_history

    source = tmp_path / "compact-owner.jsonl"
    rows = [
        {"timestamp": "2026-01-01T00:00:00Z", "type": "event_msg",
         "payload": {"type": "task_started", "turn_id": "old-turn"}},
        {"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
         "payload": {"type": "context_compacted"}},
        # No visible output or terminal ever materialized old-turn. Its marker
        # must not leak into the next authoritative task.
        {"timestamp": "2026-01-01T00:00:02Z", "type": "event_msg",
         "payload": {"type": "task_started", "turn_id": "new-turn"}},
        {"timestamp": "2026-01-01T00:00:03Z", "type": "event_msg",
         "payload": {"type": "user_message", "message": "new prompt"}},
        {"timestamp": "2026-01-01T00:00:04Z", "type": "event_msg",
         "payload": {"type": "agent_message", "phase": "final_answer",
                     "message": "new answer"}},
        {"timestamp": "2026-01-01T00:00:04Z", "type": "response_item",
         "payload": {"type": "message", "id": "new-message",
                     "role": "assistant", "phase": "final_answer",
                     "content": [{
                         "type": "output_text", "text": "new answer",
                     }]}},
        {"timestamp": "2026-01-01T00:00:05Z", "type": "event_msg",
         "payload": {"type": "task_complete", "turn_id": "new-turn",
                     "last_agent_message": "new answer"}},
    ]
    source.write_text("".join(json.dumps(row) + "\n" for row in rows))

    events, _ = codex_translate_history(str(source), tool_result_max=4096)
    assert not any(
        isinstance(event, ProcessEvent) and event.kind == "compaction"
        for event in events
    )
    assert [event.turn_id for event in events if isinstance(event, TurnEnd)] == [
        "new-turn"
    ]


def test_pending_compaction_keeps_authoritative_abort_terminal(tmp_path):
    from cc_remote.wrapper.codex_stream import codex_translate_history

    source = tmp_path / "compact-aborted.jsonl"
    rows = [
        {"timestamp": "2026-01-01T00:00:00Z", "type": "event_msg",
         "payload": {"type": "task_started", "turn_id": "turn-aborted"}},
        {"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
         "payload": {"type": "context_compacted"}},
        {"timestamp": "2026-01-01T00:00:02Z", "type": "event_msg",
         "payload": {"type": "turn_aborted", "turn_id": "turn-aborted"}},
    ]
    source.write_text("".join(json.dumps(row) + "\n" for row in rows))

    events, _ = codex_translate_history(str(source), tool_result_max=4096)
    compactions = [
        event for event in events
        if isinstance(event, ProcessEvent) and event.kind == "compaction"
    ]
    terminals = [event for event in events if isinstance(event, TurnEnd)]
    assert len(compactions) == 1
    assert compactions[0].turn_id == "turn-aborted"
    assert len(terminals) == 1
    assert terminals[0].turn_id == "turn-aborted"
    assert terminals[0].result.subtype == "error_during_execution"
    assert terminals[0].result.is_error is True


def test_pending_compaction_keeps_authoritative_failure_terminal(tmp_path):
    from cc_remote.wrapper.codex_stream import codex_translate_history

    source = tmp_path / "compact-failed.jsonl"
    rows = [
        {"timestamp": "2026-01-01T00:00:00Z", "type": "event_msg",
         "payload": {"type": "task_started", "turn_id": "turn-failed"}},
        {"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
         "payload": {"type": "context_compacted"}},
        {"timestamp": "2026-01-01T00:00:02Z", "type": "event_msg",
         "payload": {"type": "task_failed", "turn_id": "turn-failed"}},
    ]
    source.write_text("".join(json.dumps(row) + "\n" for row in rows))

    events, _ = codex_translate_history(str(source), tool_result_max=4096)
    compactions = [
        event for event in events
        if isinstance(event, ProcessEvent) and event.kind == "compaction"
    ]
    terminals = [event for event in events if isinstance(event, TurnEnd)]
    assert len(compactions) == 1
    assert compactions[0].turn_id == "turn-failed"
    assert len(terminals) == 1
    assert terminals[0].turn_id == "turn-failed"
    assert terminals[0].result.subtype == "error"
    assert terminals[0].result.is_error is True


def test_compaction_owner_comparison_uses_protocol_safe_turn_id(tmp_path):
    """Provider ids are normalized before ownership comparisons.

    An unsafe raw id must not make a valid marker look as though it belongs to
    another turn and disappear from the materialized history.
    """
    from cc_remote.wrapper.codex_stream import codex_translate_history

    source = tmp_path / "compact-normalized-owner.jsonl"
    rows = [
        {"timestamp": "2026-01-01T00:00:00Z", "type": "event_msg",
         "payload": {"type": "task_started", "turn_id": "unsafe turn id"}},
        {"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
         "payload": {"type": "context_compacted"}},
        {"timestamp": "2026-01-01T00:00:02Z", "type": "turn_context",
         "payload": {"turn_id": "unsafe turn id"}},
        {"timestamp": "2026-01-01T00:00:03Z", "type": "event_msg",
         "payload": {"type": "user_message", "message": "continue"}},
        {"timestamp": "2026-01-01T00:00:04Z", "type": "event_msg",
         "payload": {"type": "task_complete", "turn_id": "unsafe turn id",
                     "last_agent_message": "done"}},
    ]
    source.write_text("".join(json.dumps(row) + "\n" for row in rows))

    events, _ = codex_translate_history(str(source), tool_result_max=4096)
    compaction = next(
        event for event in events
        if isinstance(event, ProcessEvent) and event.kind == "compaction"
    )
    terminal = next(event for event in events if isinstance(event, TurnEnd))
    assert compaction.turn_id is not None
    # A hashed display identity must never be exposed as a resumable fork point.
    assert terminal.turn_id is None


def test_compact_only_eof_does_not_materialize_an_empty_turn(tmp_path):
    from cc_remote.wrapper.codex_stream import codex_translate_history

    source = tmp_path / "compact-eof.jsonl"
    source.write_text("".join(json.dumps(row) + "\n" for row in [
        {"timestamp": "2026-01-01T00:00:00Z", "type": "event_msg",
         "payload": {"type": "task_started", "turn_id": "turn-open"}},
        {"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
         "payload": {"type": "context_compacted"}},
    ]))

    events, _ = codex_translate_history(str(source), tool_result_max=4096)
    assert not any(isinstance(event, ProcessEvent) for event in events)
    assert not any(isinstance(event, TurnEnd) for event in events)


def test_history_agent_message_borrows_paired_live_item_id(tmp_path):
    from cc_remote.wrapper.codex_stream import codex_translate_history

    source = tmp_path / "paired-agent-message.jsonl"
    clean = "working"
    rows = [
        {"timestamp": "2026-01-01T00:00:00Z", "type": "event_msg",
         "payload": {"type": "task_started", "turn_id": "turn-1"}},
        {"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
         "payload": {"type": "user_message", "message": "inspect"}},
        {"timestamp": "2026-01-01T00:00:02Z", "type": "event_msg",
         "payload": {"type": "agent_message", "phase": "commentary",
                     "message": clean}},
        {"timestamp": "2026-01-01T00:00:03Z", "type": "response_item",
         "payload": {"type": "message", "id": "msg-stable",
                     "role": "assistant", "phase": "commentary",
                     "content": [{
                         "type": "output_text",
                         "text": clean + "\n\n<internal metadata>",
                     }]}},
        {"timestamp": "2026-01-01T00:00:04Z", "type": "event_msg",
         "payload": {"type": "task_complete", "turn_id": "turn-1",
                     "last_agent_message": clean}},
    ]
    source.write_text("".join(json.dumps(row) + "\n" for row in rows))

    events, _ = codex_translate_history(str(source), tool_result_max=4096)
    starts = [event for event in events if isinstance(event, AssistantMsgStart)]
    deltas = [event for event in events if isinstance(event, Delta)]
    ends = [event for event in events if isinstance(event, AssistantMsgEnd)]
    assert [event.message_id for event in starts] == ["msg-stable"]
    assert [(event.message_id, event.text) for event in deltas] == [
        ("msg-stable", clean)
    ]
    assert [event.message_id for event in ends] == ["msg-stable"]


def test_unpaired_history_agent_message_is_not_lost_at_snapshot_eof(tmp_path):
    from cc_remote.wrapper.codex_stream import codex_translate_history

    source = tmp_path / "unpaired-agent-message.jsonl"
    prefix = [
        {"timestamp": "2026-01-01T00:00:00Z", "type": "event_msg",
         "payload": {"type": "task_started", "turn_id": "turn-1"}},
        {"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
         "payload": {"type": "user_message", "message": "inspect"}},
        {"timestamp": "2026-01-01T00:00:02Z", "type": "event_msg",
         "payload": {"type": "agent_message", "phase": "commentary",
                     "message": "visible before pair"}},
    ]
    suffix = {
        "timestamp": "2026-01-01T00:00:03Z", "type": "response_item",
        "payload": {"type": "message", "id": "msg-after-snapshot",
                    "role": "assistant", "phase": "commentary",
                    "content": [{
                        "type": "output_text", "text": "visible before pair",
                    }]},
    }
    encoded_prefix = "".join(json.dumps(row) + "\n" for row in prefix)
    source.write_text(encoded_prefix + json.dumps(suffix) + "\n", newline="")

    events, _ = codex_translate_history(
        str(source), tool_result_max=4096,
        end_offset=len(encoded_prefix.encode()),
    )
    deltas = [event for event in events if isinstance(event, Delta)]
    assert [event.text for event in deltas] == ["visible before pair"]
    assert deltas[0].message_id != "msg-after-snapshot"


def test_active_snapshot_defers_unpaired_agent_message_until_canonical_item(
        tmp_path):
    """A growing rollout must not publish a temporary id at the mirror seam."""
    from cc_remote.wrapper.codex_stream import codex_translate_history

    source = tmp_path / "growing-agent-message.jsonl"
    prefix = [
        {"timestamp": "2026-01-01T00:00:00Z", "type": "event_msg",
         "payload": {"type": "task_started", "turn_id": "turn-1"}},
        {"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
         "payload": {"type": "user_message", "message": "inspect"}},
        {"timestamp": "2026-01-01T00:00:02Z", "type": "event_msg",
         "payload": {"type": "agent_message", "phase": "commentary",
                     "message": "one logical update"}},
    ]
    paired = {
        "timestamp": "2026-01-01T00:00:03Z", "type": "response_item",
        "payload": {"type": "message", "id": "msg-canonical",
                    "role": "assistant", "phase": "commentary",
                    "content": [{
                        "type": "output_text", "text": "one logical update",
                    }]},
    }
    encoded_prefix = "".join(json.dumps(row) + "\n" for row in prefix)
    source.write_text(encoded_prefix + json.dumps(paired) + "\n")

    partial, _ = codex_translate_history(
        str(source), tool_result_max=4096,
        end_offset=len(encoded_prefix.encode()),
        snapshot_in_progress=True,
    )
    assert not any(isinstance(event, Delta) for event in partial)

    complete, _ = codex_translate_history(
        str(source), tool_result_max=4096,
        end_offset=source.stat().st_size,
        snapshot_in_progress=True,
    )
    deltas = [event for event in complete if isinstance(event, Delta)]
    assert [(event.message_id, event.text) for event in deltas] == [
        ("msg-canonical", "one logical update"),
    ]


def test_authoritative_page_continuation_can_close_without_terminal(tmp_path):
    """Only the history selector can authorize a page-prefix continuation."""
    from cc_remote.wrapper.codex_stream import codex_translate_history
    source = tmp_path / "page-continuation.jsonl"
    rows = [
        {"timestamp": "2026-01-01T00:00:00Z", "type": "event_msg",
         "payload": {"type": "agent_message", "phase": "final_answer",
                     "message": "tail from an oversized turn"}},
        {"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
         "payload": {"type": "user_message", "message": "next"}},
        {"timestamp": "2026-01-01T00:00:02Z", "type": "event_msg",
         "payload": {"type": "agent_message", "phase": "final_answer",
                     "message": "next answer"}},
        {"timestamp": "2026-01-01T00:00:03Z", "type": "event_msg",
         "payload": {"type": "task_complete", "turn_id": "next-turn"}},
    ]
    source.write_text("".join(json.dumps(row) + "\n" for row in rows))

    events, _ = codex_translate_history(
        str(source), tool_result_max=4096,
        source_continuation="authoritative_page",
    )
    turn_ends = [e for e in events if type(e).__name__ == "TurnEnd"]
    assert [e.turn_id for e in turn_ends] == [None, "next-turn"]
    assert [e.result.subtype for e in turn_ends] == ["success", "success"]


def test_assistant_only_dangling_without_source_evidence_is_error(tmp_path):
    from cc_remote.wrapper.codex_stream import codex_translate_history
    source = tmp_path / "unproven-assistant-only.jsonl"
    rows = [
        {"timestamp": "2026-01-01T00:00:00Z", "type": "turn_context",
         "payload": {"turn_id": "background"}},
        {"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
         "payload": {"type": "agent_message", "phase": "final_answer",
                     "message": "partial background output"}},
        {"timestamp": "2026-01-01T00:00:02Z", "type": "event_msg",
         "payload": {"type": "user_message", "message": "next"}},
        {"timestamp": "2026-01-01T00:00:03Z", "type": "event_msg",
         "payload": {"type": "agent_message", "phase": "final_answer",
                     "message": "next answer"}},
        {"timestamp": "2026-01-01T00:00:04Z", "type": "event_msg",
         "payload": {"type": "task_complete", "turn_id": "next-turn"}},
    ]
    source.write_text("".join(json.dumps(row) + "\n" for row in rows))

    events, _ = codex_translate_history(str(source), tool_result_max=4096)
    turn_ends = [e for e in events if type(e).__name__ == "TurnEnd"]
    assert [e.turn_id for e in turn_ends] == [None, "next-turn"]
    assert [e.result.subtype for e in turn_ends] == ["error", "success"]
    assert [e.result.is_error for e in turn_ends] == [True, False]


def test_get_history_survives_transcript_read_failure(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("no transcript")
    monkeypatch.setattr(mm, "get_session_messages", boom)

    async def go():
        m, tr = _mk_machine()
        await m._handle_get_history(SimpleNamespace(
            session_id="sX", client_id="c1", cwd=None, type="get_history"))
        # The wrapper still replies and stops the loading attempt, but a read
        # failure is not authoritative evidence that the transcript is empty.
        assert len(tr.sent) == 1 and tr.sent[0].type == "history"
        assert tr.sent[0].events == []
        assert tr.sent[0].authoritative is False
        assert tr.sent[0].error == "历史暂时不可用，请稍后重试"
    asyncio.run(go())


def test_get_history_paginates_by_turn(monkeypatch):
    """5 turns u1..u5; verify newest-page + load-older paging by turn boundary."""
    def turn(uid):
        return [UserMsg(msg_id=uid, prompt="q"),
                TurnEnd(result=TurnResult(subtype="success", duration_ms=0, is_error=False))]
    canned = [ev for uid in ("u1", "u2", "u3", "u4", "u5") for ev in turn(uid)]
    monkeypatch.setattr(mm, "get_session_messages", lambda sid, directory=None: ["m"])
    monkeypatch.setattr(mm, "translate_history", lambda msgs, mx, timestamps=None: [e.model_copy() for e in canned])
    monkeypatch.setattr(mm, "last_assistant_model", lambda msgs: None)

    async def go():
        m, tr = _mk_machine()

        async def fetch(before, limit):
            await m._handle_get_history(SimpleNamespace(
                session_id="sX", client_id="c1", cwd="/tmp", before=before, limit=limit))
            h = tr.sent[-1]
            return h, [e["msg_id"] for e in h.events if e["type"] == "user_msg"]

        # newest 2 turns
        h, uids = await fetch(None, 2)
        assert uids == ["u4", "u5"] and h.has_more is True
        assert h.oldest_id == "u4" and h.newest_id == "u5" and h.before is None
        # older page before u4
        h, uids = await fetch("u4", 2)
        assert uids == ["u2", "u3"] and h.has_more is True and h.before == "u4"
        # oldest page — u1 only, no more
        h, uids = await fetch("u2", 2)
        assert uids == ["u1"] and h.has_more is False and h.oldest_id == "u1"
    asyncio.run(go())


def test_translate_history_stamps_real_timestamps():
    """UserMsg gets the ask-time; TurnEnd gets the turn's last message time
    (answer-done) — NOT 'now'. This fixes the 'every message shows now' clock bug."""
    from cc_remote.wrapper.stream import translate_history
    msgs = [
        SimpleNamespace(uuid="u1", type="user",
                        message={"role": "user", "content": "hi"}),
        SimpleNamespace(uuid="a1", type="assistant",
                        message={"role": "assistant", "content": [{"type": "text", "text": "hello"}]}),
    ]
    events = translate_history(msgs, 10000, timestamps={"u1": 1000.0, "a1": 1005.0})
    um = next(e for e in events if e.type == "user_msg")
    te = next(e for e in events if e.type == "turn_end")
    assert um.ts == 1000.0        # question time
    assert te.ts == 1005.0        # answer-done = last (assistant) message time
    assert te.result.duration_ms == 5000
    # missing timestamps must not crash (falls back to the _Base default)
    missing = translate_history(msgs, 10000)
    assert any(e.type == "user_msg" for e in missing)
    assert next(e for e in missing if e.type == "turn_end").result.duration_ms == 0

    backwards = translate_history(
        msgs, 10000, timestamps={"u1": 1005.0, "a1": 1000.0})
    assert next(
        e for e in backwards if e.type == "turn_end"
    ).result.duration_ms == 0


def test_translate_history_duration_spans_tool_results():
    """Tool-result user rows belong to the existing human turn and must not
    restart its elapsed clock."""
    from cc_remote.wrapper.stream import translate_history
    msgs = [
        SimpleNamespace(
            uuid="u1", type="user",
            message={"role": "user", "content": "inspect"}),
        SimpleNamespace(
            uuid="a1", type="assistant",
            message={"role": "assistant", "content": [{
                "type": "tool_use", "id": "tool-1", "name": "Read",
                "input": {"file_path": "/tmp/a"},
            }]}),
        SimpleNamespace(
            uuid="r1", type="user",
            message={"role": "user", "content": [{
                "type": "tool_result", "tool_use_id": "tool-1",
                "content": "contents",
            }]}),
        SimpleNamespace(
            uuid="a2", type="assistant",
            message={"role": "assistant", "content": [{
                "type": "text", "text": "done",
            }]}),
    ]
    events = translate_history(
        msgs, 10000, timestamps={
            "u1": 1000.0, "a1": 1002.0, "r1": 1004.0, "a2": 1007.0,
        })

    ends = [e for e in events if e.type == "turn_end"]
    assert len(ends) == 1
    assert ends[0].result.duration_ms == 7000


def test_task_notification_history_is_structured_only_with_raw_origin_evidence(
        monkeypatch, tmp_path):
    notification = """<task-notification>
<task-id>agent-task-1</task-id>
<tool-use-id>call-agent-1</tool-use-id>
<status>completed</status>
<summary>Agent \"code survey\" finished</summary>
<result>{}</result>
<usage><subagent_tokens>123</subagent_tokens><tool_uses>7</tool_uses><duration_ms>4500</duration_ms></usage>
</task-notification>""".format("very large private result " * 2000)
    path = tmp_path / "session.jsonl"
    rows = [
        {
            "type": "queue-operation", "operation": "enqueue",
            "content": notification,
        },
        {
            "type": "user", "uuid": "notification-row",
            "origin": {"kind": "task-notification"},
            "message": {"role": "user", "content": notification},
        },
    ]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    monkeypatch.setattr(
        "cc_remote.wrapper.stream.transcript_path", lambda _sid: str(path))

    metadata = transcript_internal_user_events("session")
    messages = [
        SimpleNamespace(
            uuid="human-turn", type="user",
            message={"role": "user", "content": "research"}),
        SimpleNamespace(
            uuid="assistant-row", type="assistant",
            message={"role": "assistant", "content": [{
                "type": "tool_use", "id": "call-agent-1", "name": "Agent",
                "input": {"description": "code survey"},
            }]}),
        SimpleNamespace(
            uuid="notification-row", type="user",
            message={"role": "user", "content": notification}),
    ]
    events = translate_history(
        messages, 10_000, internal_user_events=metadata)

    assert not any(
        isinstance(event, UserMsg) and "task-notification" in event.prompt
        for event in events)
    process = next(
        event for event in events
        if event.type == "process" and event.item_id == "agent:call-agent-1")
    assert process.kind == "agent" and process.phase == "end"
    assert process.status == "succeeded"
    assert process.parent_id == "call-agent-1"
    assert process.turn_id == "human-turn"
    assert process.title == 'Agent "code survey" finished'
    assert process.progress == "7 次工具调用 · 4.5s"
    assert process.output is None

    # Content shape alone is not authority. A human pasting the same XML stays
    # a visible user message when the raw transcript origin does not mark it as
    # an internal task notification.
    visible = translate_history([SimpleNamespace(
        uuid="human-paste", type="user",
        message={"role": "user", "content": notification},
    )], 10_000, internal_user_events=metadata)
    assert next(event for event in visible if isinstance(event, UserMsg)).prompt \
        == notification


def test_history_hides_cancelled_command_placeholders_without_hiding_real_text():
    user_id = "11111111-1111-4111-8111-111111111111"
    answer_id = "22222222-2222-4222-8222-222222222222"
    synthetic_ids = (
        "33333333-3333-4333-8333-333333333333",
        "44444444-4444-4444-8444-444444444444",
    )
    messages = [
        SimpleNamespace(
            uuid=user_id,
            type="user",
            message={"role": "user", "content": "hello"},
        ),
        SimpleNamespace(
            uuid=answer_id,
            type="assistant",
            message={
                "role": "assistant",
                "model": "claude-sonnet-5",
                "content": [{"type": "text", "text": "real answer"}],
            },
        ),
        *[
            SimpleNamespace(
                uuid=uid,
                type="assistant",
                message={
                    "role": "assistant",
                    "model": "<synthetic>",
                    "content": [{
                        "type": "text",
                        "text": "No response requested.",
                    }],
                },
            )
            for uid in synthetic_ids
        ],
    ]
    timestamps = {
        user_id: 1000.0,
        answer_id: 1005.0,
        synthetic_ids[0]: 1010.0,
        synthetic_ids[1]: 1015.0,
    }

    events = translate_history(messages, 10_000, timestamps=timestamps)
    deltas = [event.text for event in events if isinstance(event, Delta)]
    assert deltas == ["real answer"]
    terminal = next(event for event in events if isinstance(event, TurnEnd))
    assert terminal.turn_id == answer_id
    assert terminal.ts == 1005.0
    assert last_assistant_model(messages) == "claude-sonnet-5"

    real_same_text = SimpleNamespace(
        uuid="55555555-5555-4555-8555-555555555555",
        type="assistant",
        message={
            "role": "assistant",
            "model": "claude-sonnet-5",
            "content": [{"type": "text", "text": "No response requested."}],
        },
    )
    visible = translate_history([messages[0], real_same_text], 10_000)
    assert any(
        isinstance(event, Delta) and event.text == "No response requested."
        for event in visible
    )


def test_live_claude_turn_end_uses_last_assistant_transcript_uuid():
    """Tools can split one turn across several assistant transcript records.

    The branch point is the final transcript UUID, never the API message id.
    """
    first_uuid = "11111111-1111-4111-8111-111111111111"
    final_uuid = "22222222-2222-4222-8222-222222222222"
    translator = StreamTranslator(10_000)

    translator.feed(AssistantMessage(
        content=[ToolUseBlock(id="tool-1", name="Read", input={"path": "x"})],
        model="claude-test",
        message_id="api-message-id",
        uuid=first_uuid,
    ))
    translator.feed(UserMessage(
        content=[ToolResultBlock(tool_use_id="tool-1", content="ok")],
        uuid="33333333-3333-4333-8333-333333333333",
    ))
    translator.feed(AssistantMessage(
        content=[TextBlock(text="done")],
        model="claude-test",
        message_id="same-or-new-api-id",
        uuid=final_uuid,
    ))
    events = translator.feed(ResultMessage(
        subtype="success",
        duration_ms=10,
        duration_api_ms=9,
        is_error=False,
        num_turns=2,
        session_id="session-1",
    ))

    terminal = next(event for event in events if isinstance(event, TurnEnd))
    assert terminal.turn_id == final_uuid
    assert terminal.turn_id not in {"api-message-id", "same-or-new-api-id"}


def test_live_claude_turn_end_does_not_publish_non_uuid_fallback():
    translator = StreamTranslator(10_000)
    translator.feed(AssistantMessage(
        content=[TextBlock(text="partial")],
        model="claude-test",
        message_id="api-message-id",
        uuid=None,
    ))
    events = translator.feed(ResultMessage(
        subtype="error_during_execution",
        duration_ms=1,
        duration_api_ms=1,
        is_error=True,
        num_turns=1,
        session_id="session-1",
    ))

    assert next(event for event in events if isinstance(event, TurnEnd)).turn_id is None


def test_claude_history_turn_end_uses_final_assistant_uuid_after_tools():
    first_uuid = "44444444-4444-4444-8444-444444444444"
    final_uuid = "55555555-5555-4555-8555-555555555555"
    messages = [
        SimpleNamespace(
            uuid="66666666-6666-4666-8666-666666666666",
            type="user",
            message={"role": "user", "content": "run it"},
        ),
        SimpleNamespace(
            uuid=first_uuid,
            type="assistant",
            message={"role": "assistant", "content": [{
                "type": "tool_use", "id": "tool-1", "name": "Read",
                "input": {"path": "x"},
            }]},
        ),
        SimpleNamespace(
            uuid="77777777-7777-4777-8777-777777777777",
            type="user",
            message={"role": "user", "content": [{
                "type": "tool_result", "tool_use_id": "tool-1", "content": "ok",
            }]},
        ),
        SimpleNamespace(
            uuid=final_uuid,
            type="assistant",
            message={"role": "assistant", "content": [{
                "type": "text", "text": "done",
            }]},
        ),
    ]

    terminals = [
        event for event in translate_history(messages, 10_000)
        if isinstance(event, TurnEnd)
    ]
    assert len(terminals) == 1
    assert terminals[0].turn_id == final_uuid


def test_claude_history_never_uses_repaired_legacy_id_as_fork_point():
    messages = [
        SimpleNamespace(
            uuid="88888888-8888-4888-8888-888888888888",
            type="user",
            message={"role": "user", "content": "hello"},
        ),
        SimpleNamespace(
            uuid="",
            type="assistant",
            message={"role": "assistant", "content": [{
                "type": "text", "text": "answer",
            }]},
        ),
    ]

    terminal = next(
        event for event in translate_history(messages, 10_000)
        if isinstance(event, TurnEnd)
    )
    assert terminal.turn_id is None


def test_legacy_history_repairs_missing_message_and_tool_ids_stably():
    messages = [
        SimpleNamespace(
            type="user", uuid="",
            message={"role": "user", "content": "hello"},
        ),
        SimpleNamespace(
            type="assistant", uuid="",
            message={"role": "assistant", "content": [
                {"type": "tool_use", "name": "Read", "input": {"path": "x"}},
                {"type": "text", "text": "done"},
            ]},
        ),
        SimpleNamespace(
            type="user", uuid="user-tool-result",
            message={"role": "user", "content": [
                {"type": "tool_result", "content": "ok"},
            ]},
        ),
    ]

    first = translate_history(messages, 10_000)
    second = translate_history(messages, 10_000)
    first_ids = [
        getattr(event, name)
        for event in first
        for name in ("msg_id", "message_id", "tool_use_id")
        if getattr(event, name, None)
    ]
    second_ids = [
        getattr(event, name)
        for event in second
        for name in ("msg_id", "message_id", "tool_use_id")
        if getattr(event, name, None)
    ]
    assert first_ids and first_ids == second_ids
    assert all(identifier and len(identifier) <= 128 for identifier in first_ids)
