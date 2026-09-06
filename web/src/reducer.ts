// Turn/block state model for the chat UI.
//
// Multi-session: AppState holds a `runtimes` map keyed by session id (or a
// wrapper-assigned temp key for a brand-new session until its real id is
// captured). Each SessionRuntime has its own turns/state/model/perm/queue/etc.
// `focusedSid` selects the viewed one. Switching sessions is a pure view change
// (session_focus) — background turns keep streaming into their own runtime.
//
// Inbound frames carry `sid`; narrative events route to runtimes[msg.sid]
// (unknown sid → drop; null sid → focused). Control frames (session_list,
// session_focus, wrapper_reconnected, diff_report, ...) are global.
import type { ConnState, EventOwnership } from "./ws";
import type {
  ServerEvent, SessionInfo, State, ContextReport, StatusReport, ThreadGoal,
  QueryImg, QueryFile, DirEntry, AssistantChannel, ToolCategory, ProcessKind,
  ProcessStatus, PlanEntry, CollaborationModeName, Notice, RateLimitUpdate,
  StatusRateLimit, StatusRateWindow, SessionControl, ConversationImageRef,
} from "./protocol";
import {
  compareSessionControl, sessionControlLocksInput, sessionControlTargetsSid,
} from "./protocol";
import type { Catalog } from "./data";
import type { DiffLine, GitDiffSection } from "./diff";
import { parseGitDiff } from "./diff";
import { matchModelId } from "./data";
import { canEnqueueQuery, collectWaitingQueries, reduceTargetedRuntime } from "./runtime-drain";
import {
  historyContainsTurn, mergeAuthoritativeTurnDetail, mergeInitialHistory,
} from "./history-merge";
import { boundRuntimeTurns, pruneRuntimeMap } from "./runtime-bounds";
import { bumpSessionActivity, setSessionPinned } from "./session-order";
import { presentCommandProblem, presentTurnProblem } from "./problem-presentation";
import {
  matchQueryAcceptanceHistory,
  queryAcceptanceDescriptor,
  type QueryAcceptanceHistoryHead,
} from "./outbox";

export interface TextBlock {
  kind: "text";
  message_id: string;
  text: string;
  done: boolean;
  channel?: AssistantChannel;
}
export interface ToolBlock {
  kind: "tool";
  message_id: string;
  tool_use_id: string;
  tool: string;
  input: Record<string, unknown>;
  category?: ToolCategory;
  title?: string | null;
  parent_id?: string | null;
  server?: string | null;
  progress?: string;
  output?: string;
  diff?: string;
  result?: {
    content: string;
    is_error: boolean;
    truncated?: boolean | null;
    status?: ProcessStatus | null;
    summary?: string | null;
    diff?: string | null;
    exit_code?: number | null;
    duration_ms?: number | null;
  };
  done: boolean;
}
export interface ProcessBlock {
  kind: "process";
  item_id: string;
  processKind: ProcessKind;
  phase: "start" | "update" | "end" | "snapshot";
  status: ProcessStatus;
  turn_id?: string | null;
  parent_id?: string | null;
  title: string;
  summary?: string | null;
  detail?: string | null;
  input?: Record<string, unknown> | null;
  output?: string | null;
  diff?: string | null;
  progress?: string | null;
  server?: string | null;
  tool?: string | null;
  command?: string | null;
  cwd?: string | null;
  exit_code?: number | null;
  duration_ms?: number | null;
  truncated?: boolean | null;
  explanation?: string | null;
  plan?: PlanEntry[];
  done: boolean;
}
export type Block = TextBlock | ToolBlock | ProcessBlock;

/** A single running goal can emit an effectively unbounded number of distinct
 * app-server/SDK items.  Payload fields have their own byte limits, but without
 * an item-count limit every new tool/process/message still permanently grew the
 * active turn in the browser. */
export const MAX_TURN_BLOCKS = 256;
export const MAX_TURN_BLOCK_CHARS = 16 * 1024 * 1024;
export const OMITTED_PROCESS_ITEM_ID = "__cc_remote_earlier_process_omitted__";
// Notices are ephemeral UI control state, not transcript history.  Eight keeps
// simultaneous startup/config/security warnings available without allowing a
// noisy app-server to grow every resident session indefinitely.
export const MAX_SESSION_NOTICES = 8;

export interface Turn {
  id: string;
  // A just-sent browser turn keeps its optimistic id so live deltas do not
  // remount. Canonical history images are addressed by the transcript's native
  // user id, which can differ until the next cold history rebuild.
  historyTurnId?: string;
  // Engine-specific authoritative branch point: a Codex app-server turn id or
  // a Claude transcript assistant UUID. The wire keeps the legacy `turn_id`
  // name so already-deployed protocol-v5 peers remain compatible.
  forkPointId?: string;
  // Claude's authoritative top-level user transcript UUID. File rewind and
  // conversation rewind target this id, never the optimistic browser turn id.
  checkpointId?: string;
  /** @deprecated Read only while migrating CACHE_VER=5 entries. */
  codexTurnId?: string;
  prompt: string; // empty when we joined mid-turn (no user bubble rendered)
  blocks: Block[];
  done: boolean;
  interrupted?: boolean;
  error?: string;
  progress?: string;
  images?: QueryImg[];
  imageRefs?: ConversationImageRef[];
  files?: QueryFile[];
  ts?: number;
  doneTs?: number;
  durationMs?: number;
  // Summary history pages omit heavy tool/reasoning bodies. The count keeps
  // that omission explicit and becomes the affordance for on-demand detail.
  detailEventCount?: number;
  detailLoaded?: boolean;
  detailLoading?: boolean;
}

export interface PendingQuery {
  prompt: string;
  images?: QueryImg[];
  files?: QueryFile[];
}

export interface PreviewAssetState {
  mediaType?: string;
  data?: string;
  error?: string;
}

export interface Artifact {
  file: string;
  kind: "diff" | "md" | "file" | "gitdiff" | "html" | "image" | "pdf";
  sid?: string | null;
  requestId?: string;
  diff?: DiffLine[];
  content?: string;
  data?: string;
  mediaType?: string;
  convertedFrom?: string;
  sections?: GitDiffSection[];
  loading?: boolean;
  size?: number;
  truncated?: boolean;
  mtimeNs?: string;
  revision?: string;
  saveRequestId?: string;
  saving?: boolean;
  saveStatus?: "saved" | "conflict" | "error";
  saveError?: string;
  pendingContent?: string;
  line?: number;
  error?: string;
  assets?: Record<string, PreviewAssetState>;
}

export interface SessionRuntime {
  turns: Turn[];
  state: State;
  // Display-only activity observed from a native/external client. It must not
  // grant Stop/Interrupt semantics to a turn this wrapper does not own.
  mirroredRunning: boolean;
  model: string;
  effort: string;
  perm: string;
  collaborationMode: CollaborationModeName;
  fast: boolean | null;   // null until the wrapper reports the real service tier
  replaying: boolean;
  // True only after this connection has received this sid's Snapshot or
  // ReplayEnd. Prevents stale local "idle" state from draining work early.
  syncReady: boolean;
  truncated: boolean;
  // A replayable marker announced a destructive transcript rewrite. Until a
  // fresh non-pagination History arrives, never hydrate/merge an older tail.
  historyInvalidated: boolean;
  // Revision attached to the last authoritative non-pagination History and,
  // while a rollback barrier is pending, the exact revision allowed to clear it.
  historyRevision: string | null;
  pendingHistoryRevision: string | null;
  // Ordering watermark for newest-page History builds within one wrapper
  // generation. Pagination never advances it.
  historyGeneration: string | null;
  historyBuildSeq: number;
  historyLiveSeq: number;
  // A browser-triggered older-page response has been installed for the current
  // history revision/generation. Subsequent newest-page refreshes are only a
  // moving head window and must not discard those already-loaded older pages.
  // This is deliberately distinct from IndexedDB hydration: an authoritative
  // first page must still be able to replace stale cached rows.
  hasLoadedOlderHistory: boolean;
  // Greatest downstream sequence that confirmed a turn on this connection.
  // A History captured before it may merge rows but cannot delete the live tail.
  lastLiveSeq: number;
  // Greatest direct lifecycle state sequence. A positive running bit captured
  // by older History may recover from unrelated live settings races, but must
  // never overwrite a newer explicit idle state.
  lastLifecycleSeq: number;
  // IDs painted from IndexedDB before authoritative History arrives. They are
  // not a genuine live tail, even when an old cache row happens to be marked
  // unfinished (for example a tab closed halfway through streaming).
  hydratedCacheTurnIds: string[];
  // Native newest turn id from the last authoritative first History page. A
  // query freezes this together with revision/build/live watermarks so a later
  // materialized page can prove acceptance even when its UserMsg id is native.
  historyNewestId: string | null;
  // true while we've switched to a session but its history hasn't arrived yet
  // (no cache hit + waiting on the wrapper's cold spawn/replay) — drives a spinner.
  loading?: boolean;
  // pagination: older turns exist beyond what's loaded, and the oldest loaded
  // turn id — the cursor the "load more" button pages back from.
  hasMore?: boolean;
  oldestId?: string | null;
  // v15 authoritative control. Once populated, legacy external/takeover frames
  // are ignored; revision ordering owns every subsequent write-state decision.
  control: SessionControl | null;
  // Wrapper generation that owns control.revision. Kept even when an epoch
  // switch clears control so a delayed old-generation direct event is rejected.
  controlGeneration: string | null;
  // Sticky within this browser runtime: once revisioned control has been
  // accepted, unrevisioned compatibility frames can never regain authority,
  // including during the short gap between a generation switch and its seed.
  hasRevisionedControl: boolean;
  // Legacy compatibility/derived lock consumed by queue draining. When v15
  // control exists this is derived from it, never independently authoritative.
  external?: boolean;
  takeoverPending: boolean;
  takeoverMessage: string | null;
  ccSessionId?: string;
  pendingQuestion: { ask_id: string; header?: string | null; question: string; options: { label: string; ds?: string }[]; allow_text?: boolean; secret?: boolean } | null;
  contextReport: ContextReport | null;
  contextRequestId: string | null;
  contextError: string | null;
  goal: ThreadGoal | null;
  statusReport: StatusReport | null;
  statusRequestId: string | null;
  statusError: string | null;
  notices: Notice[];
  queue: PendingQuery[];
  pendingSend: PendingQuery | null;
  // Browser query accepted into the reliable outbox but not yet confirmed by
  // its exact user echo / native turn binding / correlated terminal Error.
  acceptancePending: string | null;
  acceptanceHistoryBaseline: QueryAcceptanceHistoryHead | null;
}

export interface AppState {
  // connection / global UI
  connState: ConnState;
  wrapperOnline: boolean;
  banner?: string;
  artifact: Artifact | null;
  dirPicker: { path: string; parent: string | null; dirs: DirEntry[] } | null;
  cwdByScope: Record<string, string>;
  sendMode: "interrupt" | "queue";
  // new-chat welcome page (global; only one new-chat flow at a time). model/effort
  // are the pre-selected values (null = use the wrapper's engine default).
  newChat: {
    cwd: string;
    cwdSource: "default" | "inherited" | "explicit";
    model: string | null;
    effort: string | null;
  } | null;
  // sessions + multi-session runtimes
  sessions: SessionInfo[];
  pendingCatalogSids: string[];
  focusedSid: string | null;
  runtimes: Record<string, SessionRuntime>;
  // /btw ephemeral side-fork: the fork's routing key (its runtime lives in
  // `runtimes[btwSid]`) + engine, or null when no side panel is open.
  btwSid: string | null;
  btwEngine?: string;
  // Model catalogs the engine reported (currently Codex only). Claude still sends
  // an empty catalog plus its cwd-aware defaults; data.ts keeps the static list.
  catalog: Catalog;
  // engine -> the model a NEW no-override session starts on.
  // Never the focused session's model — that one is per-session.
  catalogDefault: Record<string, string>;
  // engine -> effective reasoning strength for a no-override NEW session.
  catalogDefaultEffort: Record<string, string>;
  // engine -> cwd those defaults were resolved for. Claude defaults are only
  // rendered when this still matches the new-chat form's directory.
  catalogDefaultCwd: Record<string, string>;
}

export function createRuntime(): SessionRuntime {
  return {
    // These are authoritative engine settings.  A newly-created browser runtime
    // has not heard them yet, so keep them unknown instead of briefly claiming a
    // model, effort, or permission policy that may not match the native CLI.
    turns: [], state: "idle", mirroredRunning: false,
    model: "", effort: "", perm: "",
    collaborationMode: "default",
    fast: null,
    control: null, controlGeneration: null, hasRevisionedControl: false,
    takeoverPending: false, takeoverMessage: null,
    replaying: false, syncReady: false, truncated: false,
    historyInvalidated: false,
    historyRevision: null, pendingHistoryRevision: null,
    historyGeneration: null, historyBuildSeq: 0, historyLiveSeq: 0,
    lastLiveSeq: 0, lastLifecycleSeq: 0,
    hasLoadedOlderHistory: false,
    hydratedCacheTurnIds: [],
    historyNewestId: null,
    pendingQuestion: null, contextReport: null,
    contextRequestId: null, contextError: null, goal: null,
    statusReport: null, statusRequestId: null, statusError: null,
    notices: [],
    queue: [], pendingSend: null, acceptancePending: null,
    acceptanceHistoryBaseline: null,
  };
}

export type Action =
  | { type: "reset" }
  | { type: "event"; event: ServerEvent; ownership?: EventOwnership }
  | { type: "query_sent"; sid: string; prompt: string; msg_id: string; images?: QueryImg[]; files?: QueryFile[]; ts: number }
  | { type: "conn"; connState: ConnState; detail?: string }
  | { type: "command_error"; detail: string }
  | { type: "dismiss_banner"; banner: string }
  | { type: "enqueue"; sid?: string; query: PendingQuery }
  | { type: "dequeue_at"; sid: string; i: number }
  | { type: "set_send_mode"; mode: "interrupt" | "queue" }
  | { type: "set_pending"; sid?: string; query: PendingQuery }
  | { type: "clear_pending"; sid: string }
  | { type: "set_model"; model: string }
  | { type: "set_effort"; effort: string }
  | { type: "set_perm"; perm: string }
  | { type: "set_collaboration_mode"; mode: CollaborationModeName }
  | { type: "set_context"; report: ContextReport }
  | { type: "clear_context" }
  | { type: "begin_context_request"; sid: string; requestId: string }
  | { type: "begin_status_request"; sid: string; requestId: string }
  | { type: "set_turns"; sid: string; turns: Turn[] }
  | { type: "set_artifact"; artifact: Artifact }
  | { type: "open_artifact_loading"; file: string; sid: string | null; requestId: string }
  | { type: "open_file_loading"; file: string; sid: string | null; requestId: string; kind: "md" | "file"; line?: number }
  | { type: "start_file_save"; requestId: string; content: string }
  | { type: "clear_artifact" }
  | { type: "clear_btw" }
  | { type: "clear_session_list" }
  | { type: "restore_session_list"; sessions: SessionInfo[] }
  | { type: "set_session_pinned"; sid: string; pinned: boolean }
  | { type: "focus_session"; sid: string }
  | { type: "turn_detail_requested"; sid: string; turnId: string }
  | { type: "hydrate_cache"; sid: string; turns: Turn[]; revision: string | null; generation?: string | null; control?: SessionControl | null }
  | { type: "prune_runtimes"; protectedSids: string[] }
  | { type: "answer_question" }
  | { type: "dismiss_notice"; sid: string; noticeId: string }
  | { type: "enter_new_chat"; cwd: string; cwdSource?: "default" | "inherited" | "explicit"; model?: string | null; effort?: string | null }
  | { type: "set_new_chat_cwd"; cwd: string; cwdSource?: "default" | "inherited" | "explicit" }
  | { type: "clear_scope_cwd"; scopeKey: string }
  | { type: "set_new_chat_model"; model: string | null }
  | { type: "set_new_chat_effort"; effort: string | null }
  | { type: "exit_new_chat" };

export const initialState: AppState = {
  connState: "connecting",
  // Require a wrapper-originated frame before draining queued work. A relay
  // socket can be connected while the machine-side wrapper is still offline.
  wrapperOnline: false,
  artifact: null,
  dirPicker: null,
  cwdByScope: {},
  sendMode: "interrupt",
  newChat: null,
  sessions: [],
  pendingCatalogSids: [],
  focusedSid: null,
  runtimes: {},
  btwSid: null,
  catalog: {},
  catalogDefault: {},
  catalogDefaultEffort: {},
  catalogDefaultCwd: {},
};

function cloneTurns(turns: Turn[]): Turn[] {
  return turns.map((t) => ({ ...t, blocks: t.blocks.map((b) => ({ ...b })) }));
}

function openTurn(turns: Turn[], fallbackId: string, ts?: number): Turn {
  let turn = turns[turns.length - 1];
  if (!turn || turn.done) {
    turn = { id: fallbackId, prompt: "", blocks: [], done: false, ts };
    turns.push(turn);
  }
  return turn;
}

function eventTimestampMs(ts: number | null | undefined): number | undefined {
  return typeof ts === "number" ? Math.round(ts * 1000) : undefined;
}

function findTurnByEngineId(turns: Turn[], id: string | null | undefined): Turn | undefined {
  if (!id) return undefined;
  return [...turns].reverse().find((turn) =>
    turn.id === id || turn.forkPointId === id || turn.codexTurnId === id
    || turn.blocks.some((block) => block.kind === "process"
      && block.turn_id === id));
}

function findTurnOwningItem(turns: Turn[], id: string | null | undefined): Turn | undefined {
  if (!id) return undefined;
  return [...turns].reverse().find((turn) => turn.blocks.some((block) =>
    block.kind === "tool" ? block.tool_use_id === id
      : block.kind === "process" ? block.item_id === id
        : block.message_id === id));
}

function resolvedChannel(current: AssistantChannel | undefined, next: AssistantChannel): AssistantChannel {
  return next === "unknown" ? (current ?? "unknown") : next;
}

function terminalProcessStatus(status: ProcessStatus): boolean {
  return status === "succeeded" || status === "failed" || status === "declined"
    || status === "cancelled" || status === "interrupted";
}

function omissionBlock(): ProcessBlock {
  return {
    kind: "process",
    item_id: OMITTED_PROCESS_ITEM_ID,
    processKind: "compaction",
    phase: "snapshot",
    status: "succeeded",
    title: "较早过程已省略",
    summary: "为限制此回合的内存占用，较早的处理记录未显示。",
    done: true,
  };
}

function isOmissionBlock(block: Block): boolean {
  return block.kind === "process" && block.item_id === OMITTED_PROCESS_ITEM_ID;
}

function isFinalTextBlock(block: Block): boolean {
  return block.kind === "text" && block.channel === "final";
}

function boundedString(value: string | null | undefined, maxChars: number) {
  if (value == null || value.length <= maxChars) return value;
  return value.slice(0, maxChars);
}

function boundedRecord(
  value: Record<string, unknown> | null | undefined,
  maxChars = MAX_LIVE_DETAIL_CHARS,
): Record<string, unknown> | null | undefined {
  if (value == null) return value;
  try {
    if (JSON.stringify(value).length <= maxChars) return value;
  } catch {
    // Cache values should be JSON-compatible, but fail closed if an extension
    // or a future migration hands us a recursive object.
  }
  return { _truncated: true, summary: "输入过大，已省略" };
}

function limitedBlockPayload(block: Block): Block {
  if (block.kind === "text") {
    const text = boundedString(block.text, MAX_LIVE_TEXT_CHARS) ?? "";
    return text === block.text ? block : { ...block, text };
  }
  if (block.kind === "tool") {
    const input = boundedRecord(block.input) ?? {};
    return {
      ...block,
      input,
      progress: boundedString(block.progress, MAX_LIVE_PROGRESS_CHARS) ?? undefined,
      output: boundedString(block.output, MAX_LIVE_TOOL_OUTPUT_CHARS) ?? undefined,
      diff: boundedString(block.diff, MAX_LIVE_DIFF_CHARS) ?? undefined,
      result: block.result ? {
        ...block.result,
        content: boundedString(
          block.result.content, MAX_LIVE_TOOL_OUTPUT_CHARS) ?? "",
        summary: boundedString(
          block.result.summary, MAX_LIVE_PROGRESS_CHARS),
        diff: boundedString(block.result.diff, MAX_LIVE_DIFF_CHARS),
      } : undefined,
    };
  }
  return {
    ...block,
    title: boundedString(block.title, 1024) || "处理事件",
    summary: boundedString(block.summary, MAX_LIVE_PROGRESS_CHARS),
    detail: boundedString(block.detail, MAX_LIVE_DETAIL_CHARS),
    input: boundedRecord(block.input),
    output: boundedString(block.output, MAX_LIVE_TOOL_OUTPUT_CHARS),
    diff: boundedString(block.diff, MAX_LIVE_DIFF_CHARS),
    progress: boundedString(block.progress, MAX_LIVE_PROGRESS_CHARS),
    server: boundedString(block.server, 1024),
    tool: boundedString(block.tool, 1024),
    command: boundedString(block.command, MAX_LIVE_DETAIL_CHARS),
    cwd: boundedString(block.cwd, 16 * 1024),
    explanation: boundedString(block.explanation, MAX_LIVE_PROGRESS_CHARS),
    plan: block.plan?.slice(0, 128).map((entry) => ({
      ...entry, step: boundedString(entry.step, 16 * 1024) || "（空步骤）",
    })),
  };
}

function jsonChars(value: unknown): number {
  try { return JSON.stringify(value)?.length ?? 0; }
  catch { return MAX_TURN_BLOCK_CHARS + 1; }
}

function blockPayloadChars(block: Block): number {
  if (block.kind === "text") {
    return 128 + block.message_id.length + block.text.length;
  }
  if (block.kind === "tool") {
    return 256 + block.message_id.length + block.tool_use_id.length
      + block.tool.length + (block.title?.length ?? 0) + (block.parent_id?.length ?? 0)
      + (block.server?.length ?? 0) + (block.progress?.length ?? 0)
      + (block.output?.length ?? 0) + (block.diff?.length ?? 0)
      + (block.result?.content.length ?? 0) + (block.result?.summary?.length ?? 0)
      + (block.result?.diff?.length ?? 0) + jsonChars(block.input);
  }
  return 256 + block.item_id.length + block.title.length
    + (block.turn_id?.length ?? 0) + (block.parent_id?.length ?? 0)
    + (block.summary?.length ?? 0) + (block.detail?.length ?? 0)
    + (block.output?.length ?? 0) + (block.diff?.length ?? 0)
    + (block.progress?.length ?? 0) + (block.server?.length ?? 0)
    + (block.tool?.length ?? 0) + (block.command?.length ?? 0)
    + (block.cwd?.length ?? 0) + (block.explanation?.length ?? 0)
    + (block.plan?.reduce((sum, entry) => sum + entry.step.length + 16, 0) ?? 0)
    + jsonChars(block.input);
}

function turnBlockPayloadChars(blocks: Block[]): number {
  let total = 0;
  for (const block of blocks) {
    total += blockPayloadChars(block);
    if (total > MAX_TURN_BLOCK_CHARS) return total;
  }
  return total;
}

/** Mutate one cloned turn into a fixed-size display window.
 *
 * Keep at least the newest known final answer and newest live activity, then
 * prefer the remaining final blocks, remaining live blocks, and newest
 * completed process records in that order.  A single fixed marker replaces all
 * evicted items; it deliberately carries no ever-growing counter or id set. */
function limitTurnBlocks(turn: Turn): void {
  const markerCount = turn.blocks.reduce(
    (count, block) => count + (isOmissionBlock(block) ? 1 : 0), 0);
  if (turn.blocks.length <= MAX_TURN_BLOCKS && markerCount <= 1
      && turnBlockPayloadChars(turn.blocks) <= MAX_TURN_BLOCK_CHARS) return;

  const candidates = turn.blocks.filter((block) => !isOmissionBlock(block))
    .map(limitedBlockPayload);
  const capacity = MAX_TURN_BLOCKS - 1;
  const keep = new Set<number>();
  let retainedChars = blockPayloadChars(omissionBlock());
  const keepNewest = (
    predicate: (block: Block) => boolean,
    one = false,
    required = false,
  ) => {
    for (let index = candidates.length - 1;
      index >= 0 && keep.size < capacity; index -= 1) {
      if (keep.has(index) || !predicate(candidates[index])) continue;
      const size = blockPayloadChars(candidates[index]);
      if (!required && retainedChars + size > MAX_TURN_BLOCK_CHARS) continue;
      keep.add(index);
      retainedChars += size;
      if (one) break;
    }
  };

  // Reserve one slot for each of the two user-critical classes before either
  // class is allowed to consume the rest of the window.
  keepNewest(isFinalTextBlock, true, true);
  keepNewest((block) => !block.done, true, true);
  keepNewest(isFinalTextBlock);
  keepNewest((block) => !block.done);
  keepNewest(() => true);

  const retained = candidates.filter((_, index) => keep.has(index));
  turn.blocks = [omissionBlock(), ...retained];
}

function withLimitedTurnBlocks(turn: Turn): Turn {
  if (turn.blocks.length <= MAX_TURN_BLOCKS
      && turn.blocks.filter(isOmissionBlock).length <= 1
      && turnBlockPayloadChars(turn.blocks) <= MAX_TURN_BLOCK_CHARS) return turn;
  const limited = { ...turn, blocks: [...turn.blocks] };
  limitTurnBlocks(limited);
  return limited;
}

/** Close every still-open stream owned by a turn when its enclosing request
 * reaches a terminal state.  The relay can report an Error without a trailing
 * TurnEnd, so leaving child tools/processes open here would keep the process
 * timeline stuck on "running" forever. */
function finishOpenBlocks(
  turn: Turn,
  status: "succeeded" | "failed" | "interrupted",
  isError: boolean,
): void {
  for (const block of turn.blocks) {
    if (block.kind === "text") {
      block.done = true;
    } else if (block.kind === "process" && !block.done) {
      block.done = true;
      if (!terminalProcessStatus(block.status)) block.status = status;
    } else if (block.kind === "tool" && !block.done) {
      block.done = true;
      block.result ??= {
        content: block.output ?? "",
        is_error: isError,
        status,
      };
    }
  }
}

/** Reconcile an unfinished browser tail against a current authoritative History
 * snapshot which explicitly says the session is idle. This is a lost-terminal
 * recovery path: keep already-rendered text/process detail, but never leave its
 * timer and child blocks running forever. */
function finishOpenTurnsFromIdleHistory(
  turns: Turn[], interrupted: boolean, doneTs: number,
): Turn[] {
  return turns.map((turn) => {
    if (turn.done) return turn;
    const next = { ...turn, blocks: turn.blocks.map((block) => ({ ...block })) };
    next.done = true;
    next.doneTs ??= doneTs;
    next.progress = undefined;
    if (interrupted) next.interrupted = true;
    finishOpenBlocks(
      next, interrupted ? "interrupted" : "succeeded", interrupted);
    return next;
  });
}

const MAX_LIVE_TEXT_CHARS = 4 * 1024 * 1024;
const MAX_LIVE_TOOL_OUTPUT_CHARS = 2 * 1024 * 1024;
const MAX_LIVE_DIFF_CHARS = 2 * 1024 * 1024;
const MAX_LIVE_DETAIL_CHARS = 256 * 1024;
const MAX_LIVE_PROGRESS_CHARS = 64 * 1024;

function appendField(
  current: string | null | undefined,
  delta: string,
  maxChars: number,
): string {
  const value = current ?? "";
  if (value.length >= maxChars) return value;
  return value + delta.slice(0, maxChars - value.length);
}

function replaceWithBoundedTurns(runtime: SessionRuntime, turns: Turn[]): void {
  const itemBounded = turns.map(withLimitedTurnBlocks);
  const bounded = boundRuntimeTurns(itemBounded);
  if (bounded.length < itemBounded.length) {
    runtime.truncated = true;
    runtime.hasMore = false;
    runtime.oldestId = bounded[0]?.id ?? null;
  }
  runtime.turns = bounded;
}

function turnHasUnfinishedWork(turn: Turn): boolean {
  return !turn.done || turn.blocks.some((block) => !block.done);
}

/** Keep only work that still belongs to the live connection.
 *
 * Completed cache rows are never evidence that a turn still exists after an
 * authoritative History response: retaining them is what resurrected messages
 * removed by rollback.  An unfinished optimistic/streaming tail is different;
 * merge it by id/prompt so a history read racing the active turn stays smooth. */
function unfinishedLiveTail(turns: Turn[], hydratedCacheTurnIds: string[]): Turn[] {
  const cached = new Set(hydratedCacheTurnIds);
  return turns.filter((turn) => !cached.has(turn.id) && turnHasUnfinishedWork(turn));
}

function markTurnAsLive(
  runtime: SessionRuntime, turnId: string, liveEvent: boolean,
  eventSeq?: number | null,
): void {
  if (!liveEvent) return;
  if (typeof eventSeq === "number") {
    runtime.lastLiveSeq = Math.max(runtime.lastLiveSeq, eventSeq);
  }
  if (runtime.hydratedCacheTurnIds.length > 0) {
    runtime.hydratedCacheTurnIds = runtime.hydratedCacheTurnIds.filter(
      (cachedId) => cachedId !== turnId);
  }
}

// Patch a runtime by sid (explicit sid wins; null/undefined → focused). `create`
// creates the runtime if missing (used by snapshot for a session we haven't
// seen). Unknown sid with create=false → no-op (drop the frame: it's for a
// non-resident session the client doesn't track yet).
function patch(state: AppState, sid: string | null | undefined,
               fn: (rt: SessionRuntime) => void, create = false): AppState {
  const key = sid ?? state.focusedSid;
  if (!key) return state;
  let rt = state.runtimes[key];
  if (!rt) {
    if (!create) return state;
    rt = createRuntime();
  } else {
    rt = { ...rt };
  }
  fn(rt);
  return { ...state, runtimes: { ...state.runtimes, [key]: rt } };
}

/** Install one authoritative control value without allowing an older or
 * same-revision conflicting snapshot to resurrect a lock. */
function clearSessionControl(runtime: SessionRuntime): void {
  runtime.control = null;
  runtime.external = false;
  runtime.takeoverPending = false;
  runtime.takeoverMessage = null;
}

function switchControlGeneration(
  runtime: SessionRuntime, generation: string | null | undefined,
): void {
  if (!generation || generation === runtime.controlGeneration) return;
  clearSessionControl(runtime);
  runtime.controlGeneration = generation;
  // History pages are scoped to one wrapper generation. A cursor/page loaded
  // from the previous process must never be merged across a restart boundary.
  runtime.hasLoadedOlderHistory = false;
}

function applySessionControl(
  runtime: SessionRuntime, incoming: SessionControl,
): boolean {
  const incomingGeneration = incoming.generation ?? null;
  if (runtime.controlGeneration !== null) {
    // Generation-less migration frames are accepted only by a runtime which is
    // itself still generation-less. A delayed event from another wrapper epoch
    // cannot compete on numeric revision.
    if (incomingGeneration !== runtime.controlGeneration) return false;
  } else if (incomingGeneration !== null) {
    clearSessionControl(runtime);
    runtime.controlGeneration = incomingGeneration;
  }
  const disposition = compareSessionControl(runtime.control, incoming);
  if (disposition !== "newer") return false;
  runtime.control = incoming;
  runtime.hasRevisionedControl = true;
  runtime.external = sessionControlLocksInput(incoming);
  runtime.takeoverPending = incoming.write_state === "takeover_pending";
  runtime.takeoverMessage = runtime.takeoverPending
    ? (incoming.reason ?? null) : null;
  return true;
}

function newestSessionControl(
  current: SessionControl | null, candidate: SessionControl | null,
): SessionControl | null {
  if (!candidate) return current;
  return compareSessionControl(current, candidate) === "newer"
    ? candidate : current;
}

function mergeNotices(...groups: Notice[][]): Notice[] {
  const merged: Notice[] = [];
  for (const notice of groups.flat()) {
    const prior = merged.findIndex((item) => item.notice_id === notice.notice_id);
    if (prior >= 0) merged.splice(prior, 1);
    merged.push(notice);
  }
  return merged.slice(-MAX_SESSION_NOTICES);
}

function mergeRateWindow(
  current: StatusRateWindow | null | undefined,
  update: StatusRateWindow | null | undefined,
): StatusRateWindow | null | undefined {
  if (!update) return current;
  const next = { ...(current ?? {}) };
  if (update.used_percent != null) next.used_percent = update.used_percent;
  if (update.resets_at != null) next.resets_at = update.resets_at;
  if (update.window_duration_mins != null) {
    next.window_duration_mins = update.window_duration_mins;
  }
  return next;
}

function mergeRateLimitUpdate(
  report: StatusReport | null, update: RateLimitUpdate,
): StatusReport | null {
  if (!report) return null;
  const limits = report.rate_limits.map((limit) => ({ ...limit }));
  let index = update.limit_id
    ? limits.findIndex((limit) => limit.limit_id === update.limit_id)
    : limits.length === 1 ? 0 : -1;
  if (index < 0) {
    index = limits.length;
    limits.push({});
  }
  const current = limits[index];
  const next: StatusRateLimit = { ...current };
  if (update.limit_id != null) next.limit_id = update.limit_id;
  if (update.name != null) next.limit_name = update.name;
  if (update.plan_type != null) next.plan_type = update.plan_type;
  if (update.reached_type != null) {
    next.rate_limit_reached_type = update.reached_type;
  }
  next.primary = mergeRateWindow(current.primary, update.primary);
  next.secondary = mergeRateWindow(current.secondary, update.secondary);
  limits[index] = next;
  return { ...report, rate_limits: limits.slice(-16) };
}

export function reduce(state: AppState, action: Action): AppState {
  switch (action.type) {
    case "reset":
      return {
        ...initialState,
        sessions: [], runtimes: {}, artifact: null, dirPicker: null,
        newChat: null, btwSid: null, catalog: {}, catalogDefault: {},
        catalogDefaultEffort: {}, catalogDefaultCwd: {},
      };
    case "conn": {
      let banner = state.banner;
      if (action.connState === "connected") banner = undefined;
      else if (action.connState === "reconnecting") banner = action.detail || "正在重新连接…";
      else if (action.connState === "connecting") banner = "正在连接…";
      const runtimes = action.connState === "connected"
        ? state.runtimes
        : Object.fromEntries(Object.entries(state.runtimes).map(
            ([sid, runtime]) => [sid, {
              ...runtime, syncReady: false, replaying: false,
            }]));
      return {
        ...state,
        runtimes,
        connState: action.connState,
        // A reconnect may land on a restarted relay with no wrapper. Wait for
        // replay/snapshot proof before allowing background queue removal.
        wrapperOnline: action.connState === "connected" ? state.wrapperOnline : false,
        banner,
      };
    }
    case "command_error":
      return { ...state, banner: action.detail };
    case "dismiss_banner":
      return state.banner === action.banner
        ? { ...state, banner: undefined }
        : state;
    case "query_sent": {
      const current = state.runtimes[action.sid];
      if (!current || (current.acceptancePending
          && current.acceptancePending !== action.msg_id)) return state;
      const acceptanceHistoryBaseline = current.historyRevision
          && current.historyBuildSeq > 0
        ? {
          revision: current.historyRevision,
          generation: current.historyGeneration,
          buildSeq: current.historyBuildSeq,
          liveSeq: Math.max(current.historyLiveSeq, current.lastLiveSeq),
          newestId: current.historyNewestId,
        }
        : null;
      const turn: Turn = {
        id: action.msg_id, prompt: action.prompt, blocks: [], done: false,
        images: action.images,
        files: action.files?.map((file) => ({ filename: file.filename, data: "" })),
        ts: action.ts,
      };
      let runtimes = reduceTargetedRuntime(
        state.runtimes, action.sid, { type: "query_sent", turn });
      if (runtimes[action.sid]?.acceptancePending !== action.msg_id) {
        runtimes = {
          ...runtimes,
          [action.sid]: {
            ...runtimes[action.sid],
            acceptancePending: action.msg_id,
            acceptanceHistoryBaseline,
          },
        };
      }
      const sessions = bumpSessionActivity(state.sessions, action.sid, action.ts);
      if (runtimes === state.runtimes && sessions === state.sessions) return state;
      return { ...state, runtimes, sessions };
    }
    case "enqueue": {
      const allQueued = collectWaitingQueries(state.runtimes);
      if (!canEnqueueQuery(allQueued, action.query)) return state;
      return patch(state, action.sid ?? state.focusedSid, (rt) => {
        rt.queue = [...rt.queue, action.query];
      });
    }
    case "dequeue_at": {
      const runtimes = reduceTargetedRuntime(
        state.runtimes, action.sid, { type: "dequeue_at", i: action.i });
      return runtimes === state.runtimes ? state : { ...state, runtimes };
    }
    case "set_send_mode":
      return { ...state, sendMode: action.mode };
    case "set_pending": {
      const targetSid = action.sid ?? state.focusedSid;
      const waiting = collectWaitingQueries(state.runtimes, targetSid);
      if (!canEnqueueQuery(waiting, action.query)) return state;
      return patch(state, targetSid, (rt) => { rt.pendingSend = action.query; });
    }
    case "clear_pending": {
      const runtimes = reduceTargetedRuntime(
        state.runtimes, action.sid, { type: "clear_pending" });
      return runtimes === state.runtimes ? state : { ...state, runtimes };
    }
    case "set_model":
      return patch(state, state.focusedSid, (rt) => { rt.model = action.model; });
    case "set_effort":
      return patch(state, state.focusedSid, (rt) => { rt.effort = action.effort; });
    case "set_perm":
      return patch(state, state.focusedSid, (rt) => { rt.perm = action.perm; });
    case "set_collaboration_mode":
      return patch(state, state.focusedSid, (rt) => {
        rt.collaborationMode = action.mode;
      });
    case "set_turns":
      return patch(state, action.sid, (rt) => {
        replaceWithBoundedTurns(rt, action.turns);
      }, true);
    case "set_context":
      return patch(state, state.focusedSid, (rt) => { rt.contextReport = action.report; });
    case "clear_context":
      return patch(state, state.focusedSid, (rt) => { rt.contextReport = null; });
    case "begin_context_request":
      return patch(state, action.sid, (rt) => {
        rt.contextRequestId = action.requestId;
        rt.contextError = null;
      });
    case "begin_status_request":
      return patch(state, action.sid, (rt) => {
        rt.statusRequestId = action.requestId;
        rt.statusError = null;
      });
    case "set_artifact":
      return { ...state, artifact: action.artifact };
    case "open_artifact_loading":
      // optimistic: show the diff panel (with a spinner) instantly on click; the
      // diff_report event replaces it with the real sections when it arrives.
      return { ...state, artifact: {
        file: action.file, sid: action.sid, requestId: action.requestId,
        kind: "gitdiff", sections: [], loading: true,
      } };
    case "open_file_loading":
      return { ...state, artifact: {
        file: action.file, sid: action.sid, requestId: action.requestId,
        kind: action.kind, line: action.line, content: "", assets: {}, loading: true,
      } };
    case "start_file_save":
      if (!state.artifact || state.artifact.kind !== "md") return state;
      return { ...state, artifact: {
        ...state.artifact,
        saveRequestId: action.requestId,
        saving: true,
        saveStatus: undefined,
        saveError: undefined,
        pendingContent: action.content,
      } };
    case "clear_artifact":
      return { ...state, artifact: null };
    case "clear_btw": {
      if (!state.btwSid) return state;
      const runtimes = { ...state.runtimes };
      delete runtimes[state.btwSid];   // ephemeral: drop the fork's runtime
      return { ...state, btwSid: null, btwEngine: undefined, runtimes };
    }
    case "clear_session_list":
      return { ...state, sessions: [], pendingCatalogSids: [], focusedSid: null };
    case "restore_session_list":
      // Surface switches are view changes. Paint that surface's last accepted
      // list immediately, then let the in-flight authoritative list replace it.
      // Clearing first exposed Codex app-server startup time as a blank/frozen
      // sidebar even though the browser already had the exact rows in memory.
      return { ...state, sessions: action.sessions, focusedSid: null };
    case "set_session_pinned": {
      const sessions = setSessionPinned(state.sessions, action.sid, action.pinned);
      return sessions === state.sessions ? state : { ...state, sessions };
    }
    case "focus_session": {
      // optimistic view switch: focus the session locally right away (its runtime
      // is usually already in memory) instead of waiting for the round-trip
      // session_focus. The server's session_focus later just re-confirms.
      const sid = action.sid;
      const rt = state.runtimes[sid] ?? createRuntime();
      // if we have no turns yet, mark loading so the UI shows a spinner (not the
      // empty "send a message" prompt) until cache-hydrate or the wrapper replay lands.
      const runtimes = { ...state.runtimes, [sid]: { ...rt, loading: rt.turns.length === 0 } };
      return { ...state, focusedSid: sid, runtimes, artifact: null };
    }
    case "turn_detail_requested":
      return patch(state, action.sid, (rt) => {
        rt.turns = rt.turns.map((turn) => turn.id === action.turnId
          ? { ...turn, detailLoading: true }
          : turn);
      }, true);
    case "hydrate_cache":
      // fill a session's turns from the IndexedDB cache for an INSTANT render;
      // only if still empty (never clobber live/streaming or already-replayed turns).
      return patch(state, action.sid, (rt) => {
        if (rt.historyInvalidated) return;
        const control = action.control
          && sessionControlTargetsSid(action.control, action.sid)
          ? action.control : null;
        switchControlGeneration(
          rt, action.generation ?? control?.generation);
        if (control) applySessionControl(rt, control);
        if (rt.turns.length === 0 && action.turns.length) {
          replaceWithBoundedTurns(rt, action.turns.map((turn) => (
            !turn.forkPointId && turn.codexTurnId
              ? { ...turn, forkPointId: turn.codexTurnId }
              : turn
          )));
          rt.historyRevision = action.revision;
          rt.hydratedCacheTurnIds = action.turns.map((turn) => turn.id);
        }
        rt.loading = false;
      }, true);
    case "prune_runtimes": {
      const protectedSids = new Set(action.protectedSids);
      if (state.focusedSid) protectedSids.add(state.focusedSid);
      if (state.btwSid) protectedSids.add(state.btwSid);
      if (state.artifact?.sid) protectedSids.add(state.artifact.sid);
      const runtimes = pruneRuntimeMap(state.runtimes, protectedSids);
      return runtimes === state.runtimes ? state : { ...state, runtimes };
    }
    case "answer_question":
      return patch(state, state.focusedSid, (rt) => { rt.pendingQuestion = null; });
    case "dismiss_notice":
      return patch(state, action.sid, (rt) => {
        rt.notices = rt.notices.filter(
          (notice) => notice.notice_id !== action.noticeId);
      });
    case "enter_new_chat":
      return { ...state, newChat: {
        cwd: action.cwd,
        cwdSource: action.cwdSource ?? "default",
        model: action.model ?? null,
        effort: action.effort ?? null,
      } };
    case "set_new_chat_cwd":
      return state.newChat ? { ...state, newChat: {
        ...state.newChat,
        cwd: action.cwd,
        cwdSource: action.cwdSource ?? "explicit",
      } } : state;
    case "clear_scope_cwd": {
      if (!(action.scopeKey in state.cwdByScope)) return state;
      const cwdByScope = { ...state.cwdByScope };
      delete cwdByScope[action.scopeKey];
      return { ...state, cwdByScope };
    }
    case "set_new_chat_model":
      return state.newChat ? { ...state, newChat: { ...state.newChat, model: action.model } } : state;
    case "set_new_chat_effort":
      return state.newChat ? { ...state, newChat: { ...state.newChat, effort: action.effort } } : state;
    case "exit_new_chat":
      return { ...state, newChat: null };
    case "event":
      return reduceEvent(state, action.event, true, action.ownership);
  }
}

function reduceEvent(
  state: AppState, e: ServerEvent, boundCompletedTurns = true,
  ownership?: EventOwnership,
): AppState {
  // History is built asynchronously. Any newer replayable frame — including a
  // state/ownership update with no message block — makes an older History
  // envelope stale for control state. Narrative event reducers also advance
  // this watermark via markTurnAsLive; doing it once here covers the non-turn
  // frames which previously let stale `external=true` resurrect read-only mode.
  if (boundCompletedTurns && e.type !== "history"
      && typeof e.seq === "number") {
    state = patch(state, e.sid, (rt) => {
      rt.lastLiveSeq = Math.max(rt.lastLiveSeq, e.seq!);
    });
  }
  switch (e.type) {
    case "snapshot": {
      // Per-session: the frame's sid is the runtime key; cc_session_id is the
      // real cc id (may still be null while a brand-new session's id is captured).
      const key = e.sid ?? e.cc_session_id ?? state.focusedSid;
      if (!key) return state;
      // A snapshot may belong to a background engine/space. It hydrates that
      // runtime but never moves focus; the accepted session list drives the
      // initial explicit switch.
      return { ...patch(state, key, (rt) => {
        switchControlGeneration(rt, e.generation);
        rt.state = e.state;
        rt.syncReady = true;
        rt.ccSessionId = e.cc_session_id ?? rt.ccSessionId;
        if (e.control && sessionControlTargetsSid(e.control, key)) {
          applySessionControl(rt, e.control);
        }
      }, true), focusedSid: state.focusedSid, wrapperOnline: true };
    }
    case "session_focus": {
      // NON-destructive, focus-ONLY view change. Runtime key migration on
      // id-capture is handled by session_rekey — keeping it out of here is what
      // stops a background session's id-capture from stealing the user's view.
      const newF = e.session_id;
      // switch confirmed by the wrapper → stop the loading spinner. Essential for
      // a RESIDENT session with no replay (e.g. one that only ran /theme and has
      // no history) — otherwise it'd spin until the 6s fallback.
      const base = state.runtimes[newF] ?? createRuntime();
      const runtimes = {
        ...state.runtimes,
        [newF]: {
          ...base,
          loading: base.historyInvalidated ? true : false,
          syncReady: true,
        },
      };
      const cwdByScope = ownership && e.cwd
        ? { ...state.cwdByScope, [ownership.scopeKey]: e.cwd }
        : state.cwdByScope;
      const hasSession = state.sessions.some(
        (session) => session.session_id === newF);
      const sessions = e.request_id && ownership && !hasSession
        ? [{
          session_id: newF,
          summary: "新会话",
          last_modified: String(e.ts),
          cwd: e.cwd,
          state: base.state,
          engine: ownership.engine,
          space: ownership.space,
        } satisfies SessionInfo, ...state.sessions]
        : state.sessions;
      return {
        ...state, focusedSid: newF, runtimes, sessions,
        pendingCatalogSids: e.request_id && ownership && !hasSession
          ? [...state.pendingCatalogSids.filter((sid) => sid !== newF), newF].slice(-64)
          : state.pendingCatalogSids,
        artifact: state.focusedSid && state.focusedSid !== newF ? null : state.artifact,
        cwdByScope,
      };
    }
    case "session_rekey": {
      // A temp-keyed new session captured its real cc id. Rename the runtime
      // old_key -> session_id; focus follows ONLY if we were viewing old_key
      // (so a BACKGROUND new session's capture never yanks the current view).
      const { old_key, session_id } = e;
      if (old_key === session_id) return state;
      const runtimes = { ...state.runtimes };
      if (runtimes[old_key]) {
        const source = runtimes[old_key];
        const target = runtimes[session_id];
        if (target) {
          const seen = new Set(target.turns.map((turn) => turn.id));
          const mergedTurns = [
            ...target.turns,
            ...source.turns.filter((turn) => !seen.has(turn.id)),
          ];
          const mergedControlGeneration =
            source.controlGeneration ?? target.controlGeneration;
          const mergedControl = source.controlGeneration
              && source.controlGeneration !== target.controlGeneration
            ? source.control
            : newestSessionControl(target.control, source.control);
          const mergedRuntime: SessionRuntime = {
            ...target,
            ...source,
            control: null,
            controlGeneration: null,
            hasRevisionedControl:
              target.hasRevisionedControl || source.hasRevisionedControl,
            state: target.state,
            syncReady: target.syncReady || source.syncReady,
            historyInvalidated:
              target.historyInvalidated || source.historyInvalidated,
            historyRevision:
              source.historyRevision ?? target.historyRevision,
            pendingHistoryRevision:
              source.pendingHistoryRevision ?? target.pendingHistoryRevision,
            historyBuildSeq: source.historyRevision == null
              ? target.historyBuildSeq
              : source.historyRevision === target.historyRevision
                ? Math.max(source.historyBuildSeq, target.historyBuildSeq)
                : source.historyBuildSeq,
            historyLiveSeq: source.historyRevision == null
              ? target.historyLiveSeq
              : source.historyRevision === target.historyRevision
                ? Math.max(source.historyLiveSeq, target.historyLiveSeq)
                : source.historyLiveSeq,
            historyGeneration: source.historyRevision == null
              ? target.historyGeneration : source.historyGeneration,
            historyNewestId: source.historyRevision == null
              ? target.historyNewestId : source.historyNewestId,
            lastLiveSeq: Math.max(source.lastLiveSeq, target.lastLiveSeq),
            lastLifecycleSeq: Math.max(
              source.lastLifecycleSeq, target.lastLifecycleSeq),
            hydratedCacheTurnIds: Array.from(new Set([
              ...target.hydratedCacheTurnIds,
              ...source.hydratedCacheTurnIds,
            ])),
            ccSessionId: session_id,
            turns: mergedTurns,
            queue: [...source.queue, ...target.queue],
            pendingSend: source.pendingSend ?? target.pendingSend,
            acceptancePending:
              source.acceptancePending ?? target.acceptancePending,
            acceptanceHistoryBaseline: source.acceptancePending
              ? source.acceptanceHistoryBaseline
              : target.acceptanceHistoryBaseline,
            notices: mergeNotices(target.notices, source.notices),
          };
          switchControlGeneration(mergedRuntime, mergedControlGeneration);
          if (mergedControl) applySessionControl(mergedRuntime, mergedControl);
          replaceWithBoundedTurns(mergedRuntime, mergedTurns);
          runtimes[session_id] = mergedRuntime;
        } else {
          runtimes[session_id] = { ...source, ccSessionId: session_id };
        }
        delete runtimes[old_key];
      } else if (!runtimes[session_id]) {
        runtimes[session_id] = createRuntime();
      }
      const wasFocused = state.focusedSid === old_key;
      const sourceSession = state.sessions.find(
        (session) => session.session_id === old_key);
      const targetSession = state.sessions.find(
        (session) => session.session_id === session_id);
      const sessions = sourceSession
        ? [
          ...state.sessions.filter((session) => (
            session.session_id !== old_key
            && session.session_id !== session_id
          )),
          {
            ...sourceSession,
            ...targetSession,
            session_id,
            cwd: e.cwd ?? targetSession?.cwd ?? sourceSession.cwd,
          },
        ]
        : state.sessions;
      const cwdByScope = ownership && e.cwd
        ? { ...state.cwdByScope, [ownership.scopeKey]: e.cwd }
        : state.cwdByScope;
      return {
        ...state,
        runtimes, sessions,
        pendingCatalogSids: state.pendingCatalogSids.map(
          (sid) => sid === old_key ? session_id : sid),
        focusedSid: wasFocused ? session_id : state.focusedSid,
        cwdByScope,
      };
    }
    case "session_list": {
      // Native catalog reads may race first-turn transcript creation. An older
      // list must not erase the row confirmed by session_focus/session_rekey.
      const listed = new Set(e.sessions.map((session) => session.session_id));
      const pendingCatalogSids = state.pendingCatalogSids.filter((sid) => !listed.has(sid));
      const sessions = [...state.sessions.filter((session) =>
        pendingCatalogSids.includes(session.session_id)), ...e.sessions];
      const focusedMissing = !!state.focusedSid
        && !state.focusedSid.startsWith("tmp-")
        && !sessions.some((session) => session.session_id === state.focusedSid);
      return {
        ...state,
        // A session list is an authenticated wrapper response. It also covers
        // the cold-client race where the wrapper connected before this browser
        // and therefore no wrapper_reconnected broadcast is observed here.
        wrapperOnline: true,
        sessions, pendingCatalogSids,
        focusedSid: focusedMissing ? null : state.focusedSid,
        newChat: focusedMissing
          ? {
            cwd: (ownership
              ? state.cwdByScope[ownership.scopeKey] : "") || "~",
            cwdSource: ownership
              && !!state.cwdByScope[ownership.scopeKey]
              ? "inherited" : "default",
            model: null,
            effort: null,
          }
          : state.newChat,
      };
    }
    case "session_activity": {
      let changed = false;
      const sessions = state.sessions.map((session) => {
        if (session.session_id !== e.session_id || session.state === e.state) {
          return session;
        }
        changed = true;
        return { ...session, state: e.state };
      });
      return changed ? { ...state, sessions } : state;
    }
    case "work_dashboard":
    case "work_artifacts":
      // Work dashboard state is owned by App because it is engine-scoped and
      // artifact inventories are owned by App because both are intentionally
      // independent from the focused conversation runtime.
      return state;
    case "history_invalidated": {
      const next = patch(state, e.session_id, (rt) => {
        // This small frame is replayable even when the authoritative History
        // replacement is too large for the bounded ring. Empty stale turns
        // immediately; the following live/history refresh rebuilds from the
        // engine transcript without resurrecting removed messages.
        rt.turns = [];
        rt.pendingQuestion = null;
        rt.hasMore = false;
        rt.oldestId = null;
        rt.truncated = false;
        rt.historyInvalidated = true;
        rt.pendingHistoryRevision = e.revision;
        rt.historyNewestId = null;
        // Keep the accepted generation until replacement arrives: a slow
        // pre-rollback build from that same generation must remain rejectable.
        rt.historyBuildSeq = 0;
        rt.historyLiveSeq = 0;
        rt.hasLoadedOlderHistory = false;
        rt.hydratedCacheTurnIds = [];
        rt.loading = true;
      }, true);
      return next.artifact?.sid === e.session_id
        ? { ...next, artifact: null }
        : next;
    }
    case "artifact_invalidated":
      return state.artifact?.sid === e.session_id
        ? { ...state, artifact: null }
        : state;
    case "history": {
      // Bulk on-demand history (one frame, read from the transcript — like a web
      // chat's GET /conversation). Rebuild this session's COMPLETED turns by
      // running the events through a throwaway empty runtime: this reuses the
      // per-event reduce logic verbatim so deltas accumulate EXACTLY ONCE (never
      // double-appending over cache-hydrated or live text). Any not-yet-done turn
      // already in the runtime (an in-flight turn still streaming live, not yet in
      // the transcript) is preserved and appended after the rebuilt history.
      const sid = e.session_id;
      // Control has its own monotonic revision and remains authoritative even
      // when this History page later loses a transcript build/live race. Apply
      // it before any narrative early-return.
      if (e.control && sessionControlTargetsSid(e.control, sid)) {
        state = patch(state, sid, (rt) => {
          switchControlGeneration(rt, e.generation);
          applySessionControl(rt, e.control!);
        }, true);
      } else if (e.generation) {
        state = patch(state, sid, (rt) => {
          switchControlGeneration(rt, e.generation);
        }, true);
      }
      const base = state.runtimes[sid] ?? createRuntime();
      const sameBuildGeneration = e.generation != null
        ? base.historyGeneration === e.generation
        : base.historyGeneration == null && base.historyRevision === e.revision;
      if (!e.before && e.build_seq != null && sameBuildGeneration
          && e.build_seq < base.historyBuildSeq) return state;
      // A failed read/parse is explicitly non-authoritative. It satisfies the
      // current loading attempt but must not turn "could not read" into "the
      // conversation is empty" or clear a pending rollback barrier.
      if (e.authoritative === false) {
        const next = patch(state, sid, (rt) => {
          rt.loading = false;
        }, true);
        return next;
      }
      // build_seq orders newest-page reads only within the same boot-scoped
      // revision. A restart legitimately resets the sequence while changing
      // revision. Pagination remains revision/cursor based: another client's
      // targeted newest-page read can advance the wrapper's build sequence
      // without ever being routed to this browser.
      let scratch: AppState = {
        ...state, banner: undefined, runtimes: { [sid]: createRuntime() },
      };
      for (const ev of e.events) {
        scratch = reduceEvent(scratch, ev as ServerEvent, false);
      }
      const built = scratch.runtimes[sid] ?? createRuntime();
      if (e.detail === "summary" && Array.isArray(e.turns)) {
        built.turns = e.turns.map((turn) => ({
          ...turn,
          blocks: turn.blocks as Turn["blocks"],
          forkPointId: turn.forkPointId ?? undefined,
          checkpointId: turn.checkpointId ?? undefined,
          interrupted: turn.interrupted ?? undefined,
          error: turn.error ?? undefined,
          images: turn.images ?? undefined,
          imageRefs: turn.imageRefs ?? undefined,
          files: turn.files ?? undefined,
          ts: turn.ts ?? undefined,
          doneTs: turn.doneTs ?? undefined,
          durationMs: turn.durationMs ?? undefined,
        }));
      }
      // A pre-rollback first page and an older pagination response can arrive
      // after the replayable marker. Only the marker's exact revision may cross
      // the destructive boundary; pagination is valid only for the revision
      // whose first page is already installed.
      if (!e.before && base.pendingHistoryRevision
          && e.revision !== base.pendingHistoryRevision) return state;
      if (e.before && (base.historyInvalidated
          || !base.historyRevision || e.revision !== base.historyRevision)) {
        return state;
      }
      const pendingAcceptanceTurn = base.acceptancePending
        ? base.turns.find((turn) => turn.id === base.acceptancePending)
        : undefined;
      const acceptedNativeTurnId = pendingAcceptanceTurn
          && base.acceptanceHistoryBaseline
        ? matchQueryAcceptanceHistory(
          queryAcceptanceDescriptor(
            pendingAcceptanceTurn.id,
            pendingAcceptanceTurn.prompt,
            pendingAcceptanceTurn.images,
            pendingAcceptanceTurn.files,
          ),
          base.acceptanceHistoryBaseline,
          e,
        )
        : null;
      if (acceptedNativeTurnId && base.acceptancePending
          && acceptedNativeTurnId !== base.acceptancePending) {
        // The materialized transcript owns a native user id while live UI owns
        // the browser msg_id. The frozen-head proof above is the missing
        // TurnBinding: normalize only that exact newest row so normal history
        // merging preserves the optimistic identity and never renders twice.
        built.turns = built.turns.map((turn) => turn.id === acceptedNativeTurnId
          ? {
              ...turn,
              id: base.acceptancePending!,
              historyTurnId: acceptedNativeTurnId,
            }
          : turn);
      }
      const racedLiveEvent = !e.before && e.live_seq != null
        && base.lastLiveSeq > e.live_seq;
      const preserveStableHeadHistory = !e.before
        && base.turns.length > 0
        && (base.hasLoadedOlderHistory || e.has_more === true)
        && !base.historyInvalidated
        && base.historyRevision === e.revision
        && (e.generation != null
          ? base.historyGeneration === e.generation
          : base.historyGeneration == null);
      let turns: Turn[];
      if (e.before) {
        // pagination (load older): PREPEND the older turns ahead of what we have,
        // deduped by id — keeps the current view and in-flight turn intact.
        const haveIds = new Set(base.turns.map((t) => t.id));
        turns = [...built.turns.filter((t) => !haveIds.has(t.id)), ...base.turns];
      } else {
        // Every first page is authoritative for completed turns. Merge only the
        // genuinely unfinished local tail; arbitrary completed cache rows may
        // have been removed by rollback while this browser was offline.
        const cached = new Set(base.hydratedCacheTurnIds);
        const unfinished = unfinishedLiveTail(
          base.turns, base.hydratedCacheTurnIds);
        const newestUnfinished = [...unfinished].reverse().find(
          (turn) => turnHasUnfinishedWork(turn));
        const liveTail = preserveStableHeadHistory
          // A bounded newest page is a moving head window, not the whole
          // conversation. Keep rows already painted from live traffic or from
          // explicit older pages. This is essential when compact makes the
          // current turn itself larger than the backend byte window.
          ? base.turns
          : racedLiveEvent
          // This History started before a live event already painted by the
          // browser. Keep every non-cache local row (including a just-completed
          // TurnEnd); the stale frame may add history but cannot delete it.
          ? base.turns.filter((turn) => !cached.has(turn.id))
          : base.historyInvalidated
          // Replay gaps begin at an arbitrary ring position and can therefore
          // synthesize a prompt-less "turn" from the middle of old output.
          // Current authoritative History validates real replay tails by turn
          // identity; unmatched fragments must not survive at the newest edge.
          // Keep an optimistic query which has not yet received its UserMsg
          // echo so a History read cannot erase an in-flight send. Likewise,
          // an explicitly running snapshot may precede the transcript flush;
          // only its newest unfinished row can be the active unflushed tail.
          ? unfinished.filter((turn) =>
              turn.id === base.acceptancePending
              || historyContainsTurn(built.turns, turn)
              || (e.in_progress === true && turn === newestUnfinished))
          : unfinished;
        turns = mergeInitialHistory(
          built.turns,
          liveTail, {
          // History's final TurnEnd is synthetic: Claude transcripts do not
          // contain ResultMessage. A newer live event always wins; otherwise an
          // explicit in_progress value is authoritative, and only an older
          // wrapper without that field falls back to the local runtime state.
          preserveLiveTailOpen: racedLiveEvent || e.in_progress === true
            || (e.in_progress == null && base.state !== "idle"),
        });
        // A current first page which explicitly reports idle is the recovery
        // boundary for a lost TurnEnd. Do not close a merely optimistic local
        // query (base is still idle), or a tail advanced after this History read.
        if (e.in_progress === false && !racedLiveEvent && base.state !== "idle") {
          const wasInterrupting = base.state === "interrupting"
            || base.state === "draining";
          turns = finishOpenTurnsFromIdleHistory(
            turns, wasInterrupting,
            e.ts ? Math.round(e.ts * 1000) : Date.now());
        }
      }
      if (e.detail === "summary" && !base.historyInvalidated
          && base.historyRevision === e.revision) {
        const loadedDetail = new Map(base.turns
          .filter((turn) => turn.detailLoaded)
          .map((turn) => [turn.id, turn]));
        turns = turns.map((turn) => {
          const detail = loadedDetail.get(turn.id);
          if (!detail) return turn;
          const merged = mergeAuthoritativeTurnDetail(turn, detail);
          if (turn.done) {
            const status = turn.interrupted
              ? "interrupted" : turn.error ? "failed" : "succeeded";
            finishOpenBlocks(merged, status, status !== "succeeded");
          }
          return merged;
        });
      }
      turns = turns.map(withLimitedTurnBlocks);
      const boundedTurns = boundRuntimeTurns(turns);
      const historyTrimmed = boundedTurns.length < turns.length;
      turns = boundedTurns;
      const acceptsControlState = !e.before;
      const acceptsOwnershipState = acceptsControlState && !racedLiveEvent
        && !base.hasRevisionedControl;
      const confirmsWrapperRunning = acceptsControlState
        && e.in_progress === true
        && e.external !== true
        && !base.external
        && (!racedLiveEvent || (e.live_seq != null
          && base.lastLifecycleSeq <= e.live_seq));
      const hadModel = e.events.some((ev) => (ev as { type?: string }).type === "model");
      const hadEffort = e.events.some((ev) => (ev as { type?: string }).type === "effort");
      const acceptanceConfirmed = !!base.acceptancePending && (
        !!acceptedNativeTurnId
        || built.turns.some((turn) => turn.id === base.acceptancePending)
        || e.events.some((ev) => (
          (ev.type === "user_msg" || ev.type === "turn_binding"
            || (ev.type === "error" && ev.code !== "wrapper_offline"))
          && ev.msg_id === base.acceptancePending
        ))
      );
      return {
        ...state,
        // History can contain legacy Error rows.  They may reconstruct a
        // turn-local outcome, but must never escape as a fresh global banner.
        banner: state.banner,
        runtimes: {
          ...state.runtimes,
          [sid]: {
            ...base, turns, loading: false,
            ccSessionId: acceptsControlState ? sid : base.ccSessionId,
            state: confirmsWrapperRunning
              ? (base.state === "interrupting" || base.state === "draining"
                  ? base.state : "running")
              : acceptsControlState && !racedLiveEvent
                  && e.external !== true && !base.external
                  && e.in_progress === false
                ? "idle"
                : base.state,
            mirroredRunning: acceptsControlState && !racedLiveEvent
              ? e.external === true && e.in_progress === true
              : base.mirroredRunning,
            historyInvalidated: acceptsControlState
              ? false : base.historyInvalidated,
            historyRevision: acceptsControlState
              ? e.revision : base.historyRevision,
            pendingHistoryRevision: acceptsControlState
              ? null : base.pendingHistoryRevision,
            historyGeneration: acceptsControlState
              ? (e.generation ?? base.historyGeneration)
              : base.historyGeneration,
            historyBuildSeq: acceptsControlState
              ? (e.build_seq ?? base.historyBuildSeq)
              : base.historyBuildSeq,
            historyLiveSeq: acceptsControlState
              ? (e.live_seq ?? base.historyLiveSeq)
              : base.historyLiveSeq,
            historyNewestId: acceptsControlState
              ? (Object.prototype.hasOwnProperty.call(e, "newest_id")
                  ? (e.newest_id ?? null)
                  : base.historyNewestId)
              : base.historyNewestId,
            hasLoadedOlderHistory: e.before
              ? true
              : preserveStableHeadHistory
                ? base.hasLoadedOlderHistory
                : false,
            hydratedCacheTurnIds: acceptsControlState
              ? [] : base.hydratedCacheTurnIds,
            // A first-page History can finish after a live thread-settings
            // notification.  Its transcript snapshot then contains the old
            // model/effort even though its narrative rows are still useful.
            // Keep the live app-server/TUI setting whenever the sequence
            // watermark proves that the History build lost that race.
            model: acceptsControlState && !racedLiveEvent && hadModel
              ? built.model : base.model,
            effort: acceptsControlState && !racedLiveEvent && hadEffort
              ? built.effort : base.effort,
            // Browser retention and server pagination are independent. When
            // the newest window is bounded locally, keep the authoritative
            // backend cursor so compacted/older transcript pages remain
            // reachable through "load earlier history".
            hasMore: preserveStableHeadHistory
              ? base.hasMore : e.has_more,
            oldestId: preserveStableHeadHistory
              ? base.oldestId : (e.oldest_id ?? base.oldestId),
            truncated: base.truncated || historyTrimmed,
            // A native `claude`/`codex` in the terminal owns this session and is
            // appending to its transcript; the wrapper mirrors those appends here.
            // Render read-only — a cc session has ONE owner, and typing would fork it.
            external: acceptsOwnershipState ? !!e.external : base.external,
            takeoverPending: acceptsOwnershipState
              ? !!e.takeover_pending : base.takeoverPending,
            takeoverMessage: acceptsOwnershipState
              ? (e.takeover_pending ? base.takeoverMessage : null)
              : base.takeoverMessage,
            acceptancePending: acceptanceConfirmed
              ? null : base.acceptancePending,
            acceptanceHistoryBaseline: acceptanceConfirmed
              ? null : base.acceptanceHistoryBaseline,
          },
        },
      };
    }
    case "turn_detail": {
      const sid = e.session_id;
      const base = state.runtimes[sid];
      if (!base || base.historyRevision !== e.revision) return state;
      if (e.authoritative === false) {
        const next = patch(state, sid, (rt) => {
          rt.turns = rt.turns.map((turn) => turn.id === e.turn_id
            ? { ...turn, detailLoading: false }
            : turn);
        });
        return next;
      }
      let scratch: AppState = {
        ...state,
        banner: undefined,
        runtimes: { [sid]: createRuntime() },
      };
      for (const event of e.events) {
        scratch = reduceEvent(scratch, event as ServerEvent, false);
      }
      const detailed = (scratch.runtimes[sid]?.turns ?? []).find(
        (turn) => turn.id === e.turn_id,
      );
      if (!detailed) {
        return patch(state, sid, (rt) => {
          rt.turns = rt.turns.map((turn) => turn.id === e.turn_id
            ? { ...turn, detailLoading: false }
            : turn);
        });
      }
      return patch(state, sid, (rt) => {
        rt.turns = rt.turns.map((turn) => {
          if (turn.id !== e.turn_id) return turn;
          return withLimitedTurnBlocks({
            ...turn,
            ...detailed,
            id: turn.id,
            prompt: detailed.prompt || turn.prompt,
            images: detailed.images ?? turn.images,
            imageRefs: detailed.imageRefs ?? turn.imageRefs,
            files: detailed.files ?? turn.files,
            detailEventCount: turn.detailEventCount,
            detailLoaded: true,
            detailLoading: false,
          });
        });
      });
    }
    case "dir_list":
      return { ...state, dirPicker: { path: e.path, parent: e.parent ?? null, dirs: e.dirs } };
    // The engine's real model catalog. Empty => the wrapper couldn't read it; keep
    // what we have (data.ts's static table) rather than blanking the pickers.
    case "models": {
      const catalog = e.models.length
        ? { ...state.catalog, [e.engine]: e.models }
        : state.catalog;
      if (e.cwd && e.cwd !== state.newChat?.cwd) {
        // Cwd-aware reads run concurrently. Never let a late response for a
        // directory the user has left replace the still-current result.
        return catalog === state.catalog ? state : { ...state, catalog };
      }
      let catalogDefault = state.catalogDefault;
      let catalogDefaultEffort = state.catalogDefaultEffort;
      let catalogDefaultCwd = state.catalogDefaultCwd;
      if (e.cwd) {
        // A Claude response is authoritative even when probing failed and the
        // value is null: clear an older cwd's value instead of showing stale data.
        catalogDefault = { ...catalogDefault };
        catalogDefaultEffort = { ...catalogDefaultEffort };
        if (e.default_model) {
          catalogDefault[e.engine] = matchModelId(e.default_model, e.engine);
        } else {
          delete catalogDefault[e.engine];
        }
        if (e.default_effort) {
          catalogDefaultEffort[e.engine] = e.default_effort;
        } else {
          delete catalogDefaultEffort[e.engine];
        }
        catalogDefaultCwd = {
          ...catalogDefaultCwd, [e.engine]: e.cwd,
        };
      } else {
        if (e.default_model) {
          catalogDefault = { ...catalogDefault,
            [e.engine]: matchModelId(e.default_model, e.engine) };
        }
        if (e.default_effort) {
          catalogDefaultEffort = {
            ...catalogDefaultEffort, [e.engine]: e.default_effort,
          };
        }
      }
      return {
        ...state, catalog, catalogDefault, catalogDefaultEffort,
        catalogDefaultCwd,
      };
    }
    // App owns this on-demand, surface-keyed sheet state. Keep the event in the
    // exhaustive reducer switch so protocol drift cannot silently bypass it.
    case "engine_capabilities":
      return state;
    case "wrapper_disconnected":
      return {
        ...state,
        runtimes: Object.fromEntries(Object.entries(state.runtimes).map(
          ([sid, runtime]) => [sid, {
            ...runtime, syncReady: false, replaying: false,
          }])),
        wrapperOnline: false,
        banner: "machine offline — waiting for reconnect",
      };
    case "wrapper_reconnected":
      // Relay authentication proves transport liveness immediately. Session
      // write readiness remains separate: each runtime stays syncReady=false
      // until its replay/snapshot arrives, so queues cannot drain early.
      return {
        ...state,
        runtimes: Object.fromEntries(Object.entries(state.runtimes).map(
          ([sid, runtime]) => [sid, { ...runtime, syncReady: false }],
        )),
        wrapperOnline: true,
        banner: "machine reconnected — syncing…",
      };
    case "diff_report":
      if (!state.artifact || state.artifact.file !== e.file
          || state.artifact.requestId !== e.request_id
          || state.artifact.sid !== (e.sid ?? state.focusedSid)) return state;
      return { ...state, artifact: {
        file: e.file, sid: state.artifact.sid, requestId: e.request_id,
        kind: "gitdiff", sections: parseGitDiff(e.diff),
      } };
    case "file_preview":
      if (!state.artifact || !["md", "file", "html", "image", "pdf"].includes(state.artifact.kind)
          || state.artifact.requestId !== e.request_id
          || state.artifact.sid !== (e.sid ?? state.focusedSid)) return state;
      return { ...state, artifact: {
        file: e.path,
        sid: state.artifact.sid,
        requestId: e.request_id,
        kind: e.format === "markdown" ? "md" : e.format === "text" ? "file" : e.format,
        content: e.content,
        data: e.data ?? undefined,
        mediaType: e.media_type ?? undefined,
        convertedFrom: e.converted_from ?? undefined,
        size: e.size,
        truncated: e.truncated,
        mtimeNs: e.mtime_ns,
        revision: e.revision ?? undefined,
        line: state.artifact.line,
        error: e.error ?? undefined,
        assets: {},
      } };
    case "file_save_result":
      if (!state.artifact || state.artifact.kind !== "md"
          || state.artifact.saveRequestId !== e.request_id
          || state.artifact.sid !== (e.sid ?? state.focusedSid)) return state;
      if (e.status === "saved") {
        return { ...state, artifact: {
          ...state.artifact,
          content: state.artifact.pendingContent ?? state.artifact.content,
          size: e.size,
          mtimeNs: e.mtime_ns,
          revision: e.revision ?? state.artifact.revision,
          saving: false,
          saveStatus: "saved",
          saveError: undefined,
          pendingContent: undefined,
        } };
      }
      return { ...state, artifact: {
        ...state.artifact,
        saving: false,
        saveStatus: e.status,
        saveError: e.error || (e.status === "conflict"
          ? "文件已被其他程序修改，请重新读取后再保存。" : "保存失败。"),
        pendingContent: undefined,
      } };
    case "preview_asset":
      if (!state.artifact || state.artifact.kind !== "md"
          || state.artifact.requestId !== e.preview_id
          || state.artifact.sid !== (e.sid ?? state.focusedSid)) return state;
      return { ...state, artifact: {
        ...state.artifact,
        assets: {
          ...state.artifact.assets,
          [e.path]: {
            mediaType: e.media_type ?? undefined,
            data: e.data ?? undefined,
            error: e.error ?? undefined,
          },
        },
      } };
    case "state":
      return patch(state, e.sid, (rt) => {
        rt.state = e.state;
        if (typeof e.seq === "number") {
          rt.lastLifecycleSeq = Math.max(rt.lastLifecycleSeq, e.seq);
        }
        // A direct lifecycle frame belongs to this wrapper's resident turn and
        // supersedes any older rollout-only activity projection.
        rt.mirroredRunning = false;
        const turns = cloneTurns(rt.turns);
        const turn = e.msg_id
          ? turns.find((candidate) => candidate.id === e.msg_id)
          : turns[turns.length - 1];
        if (e.detail && turn && !turn.done) turn.progress = e.detail;
        else if (turn && (Object.hasOwn(e, "detail") || e.state !== "running")) {
          turn.progress = undefined;
        }
        if (e.state === "idle") {
          rt.pendingQuestion = null;
        }
        rt.turns = turns;
      });
    case "session_control":
      // Direct control events require an explicit runtime key. Snapshot and
      // History controls are routed by their outer envelope above.
      if (!e.sid) return state;
      return patch(state, e.sid, (rt) => {
        applySessionControl(rt, e);
      }, true);
    case "takeover_state":
      return patch(state, e.sid, (rt) => {
        // Temporary compatibility while v15 producers migrate. Once a real
        // revisioned control value exists, an unrevisioned frame can never
        // overwrite it or revive a completed takeover lock.
        if (rt.hasRevisionedControl) return;
        rt.takeoverPending = e.pending;
        rt.takeoverMessage = e.message ?? null;
      });
    case "model":
      return patch(state, e.sid, (rt) => { rt.model = matchModelId(e.model); });
    case "effort":
      return patch(state, e.sid, (rt) => { rt.effort = e.effort; });
    case "fast":
      return patch(state, e.sid, (rt) => { rt.fast = e.on; });
    case "collaboration_mode":
      return patch(state, e.sid, (rt) => {
        rt.collaborationMode = e.mode;
      });
    case "btw_opened": {
      // open the side panel + ensure a runtime for the fork; do NOT change focus
      // (the main view stays put — the fork lives only in the panel).
      const runtimes = { ...state.runtimes, [e.btw_sid]: state.runtimes[e.btw_sid] ?? createRuntime() };
      return { ...state, btwSid: e.btw_sid, btwEngine: e.engine, runtimes };
    }
    case "perm":
      return patch(state, e.sid, (rt) => { rt.perm = e.mode; });
    case "context_report":
      return patch(state, e.sid, (rt) => {
        rt.contextReport = e;
        rt.contextRequestId = null;
        rt.contextError = null;
      });
    case "ask_user":
      return patch(state, e.sid, (rt) => { rt.pendingQuestion = { ask_id: e.ask_id, header: e.header, question: e.question, options: e.options, allow_text: e.allow_text, secret: e.secret }; });
    case "goal_state":
      return patch(state, e.sid, (rt) => { rt.goal = e.goal ?? null; });
    case "rollback_result": {
      const next = patch(state, e.sid, (rt) => {
        const succeeded = [e.conversation, e.files].filter(
          (outcome) => outcome === "succeeded").length;
        const failed = [e.conversation, e.files].filter(
          (outcome) => outcome === "failed").length;
        const title = failed === 0 ? "回滚完成"
          : succeeded > 0 ? "回滚部分完成" : "回滚失败";
        const parts = [
          e.conversation !== "skipped" ? `对话：${e.conversation === "succeeded" ? "已恢复" : "失败"}` : "",
          e.files !== "skipped" ? `代码：${e.files === "succeeded" ? "已恢复" : "失败"}` : "",
        ].filter(Boolean);
        const notice: Notice = {
          v: e.v, type: "notice", ts: e.ts, sid: e.sid,
          notice_id: `rollback-${e.ts}-${e.session_id}`,
          severity: failed > 0 ? "warning" : "info",
          category: "runtime", title,
          message: e.detail || parts.join(" · ") || title,
          detail: e.conflicts.length > 0
            ? `冲突文件：${e.conflicts.slice(0, 12).join("、")}` : undefined,
          thread_id: e.session_id,
        };
        rt.notices = mergeNotices(rt.notices, [notice]);
      });
      // A files-only rollback has no HistoryInvalidated frame to close the
      // current file/diff preview.  Treat the successful result itself as the
      // authoritative byte boundary so a stale snapshot never remains visible.
      return e.files === "succeeded" && next.artifact?.sid === e.session_id
        ? { ...next, artifact: null }
        : next;
    }
    case "status_report":
      return patch(state, e.sid, (rt) => {
        rt.statusReport = e;
        rt.statusRequestId = null;
        rt.statusError = null;
      });
    case "notice":
      return patch(state, e.sid, (rt) => {
        rt.notices = mergeNotices(rt.notices, [e]);
      });
    case "rate_limit_update":
      return patch(state, e.sid, (rt) => {
        rt.statusReport = mergeRateLimitUpdate(rt.statusReport, e);
      });
    case "replay_start": {
      const needsAuthoritativeHistory = e.truncated || !!e.rebuild;
      const next = patch(state, e.sid, (rt) => {
        switchControlGeneration(rt, e.generation);
        rt.replaying = true;
        rt.syncReady = false;
        rt.truncated = e.truncated;
        // rebuild clears turns then refills — keep loading=true so the gap shows a
        // spinner rather than briefly flashing the empty "send a message" prompt.
        if (needsAuthoritativeHistory) {
          rt.turns = [];
          rt.pendingQuestion = null;
          rt.hasMore = false;
          rt.oldestId = null;
          rt.historyInvalidated = true;
          // A replay gap does not reveal which revision was missed. Accept the
          // next authoritative first page; an actual rollback marker replayed
          // inside this envelope will immediately replace this with its token.
          rt.pendingHistoryRevision = null;
          rt.hydratedCacheTurnIds = [];
          rt.hasLoadedOlderHistory = false;
          if (e.rebuild) {
            // The wrapper generation (and every SessionContext seq) restarted.
            // Never compare the new generation against old live/build watermarks.
            rt.historyBuildSeq = 0;
            rt.historyLiveSeq = 0;
            rt.historyGeneration = null;
            rt.historyNewestId = null;
            rt.lastLiveSeq = 0;
            rt.lastLifecycleSeq = 0;
          }
          rt.loading = true;
        }
      }, true);
      return needsAuthoritativeHistory && next.artifact?.sid === e.sid
        ? { ...next, artifact: null }
        : next;
    }
    case "replay_end":
      return { ...patch(state, e.sid, (rt) => {
        rt.replaying = false;
        rt.syncReady = true;
        rt.truncated = rt.truncated || e.truncated;
        // A truncated/rebuild replay is not authoritative history. Keep the
        // loading barrier until the first History page replaces the gap.
        rt.loading = rt.historyInvalidated;
      }, true), wrapperOnline: true };
    case "error": {
      // The relay has not accepted/rejected the command yet: reliable commands
      // stay in the outbox and will be retried when the wrapper returns. Keep the
      // optimistic turn pending instead of falsely marking it failed.
      if (e.code === "wrapper_offline") {
        return {
          ...state,
          runtimes: Object.fromEntries(Object.entries(state.runtimes).map(
            ([sid, runtime]) => [sid, {
              ...runtime, syncReady: false, replaying: false,
            }])),
          wrapperOnline: false,
          banner: "设备离线，正在等待重新连接…",
        };
      }
      if (e.request_id && e.sid) {
        if (state.artifact?.requestId === e.request_id
            && state.artifact.sid === e.sid) {
          return { ...state, artifact: {
            ...state.artifact, loading: false,
            error: presentCommandProblem(e),
          } };
        }
        const runtime = state.runtimes[e.sid];
        if (runtime?.contextRequestId === e.request_id) {
          return patch(state, e.sid, (rt) => {
            rt.contextRequestId = null;
            rt.contextError = presentCommandProblem(e);
          });
        }
        if (runtime?.statusRequestId === e.request_id) {
          return patch(state, e.sid, (rt) => {
            rt.statusRequestId = null;
            rt.statusError = presentCommandProblem(e);
          });
        }
      }
      if (!e.msg_id) {
        return { ...state, banner: presentCommandProblem(e) };
      }
      return patch(state, e.sid, (rt) => {
        rt.loading = false; // never leave a spinner spinning behind an error
        if (rt.acceptancePending === e.msg_id) {
          rt.acceptancePending = null;
          rt.acceptanceHistoryBaseline = null;
        }
        markTurnAsLive(rt, e.msg_id!, boundCompletedTurns, e.seq);
        const turns = cloneTurns(rt.turns);
        const t = turns.find((turn) => turn.id === e.msg_id);
        if (t) {
          t.error = presentTurnProblem(e);
          t.progress = undefined;
          t.done = true;
          t.doneTs ??= Date.now();
          finishOpenBlocks(t, "failed", true);
        }
        else turns.push({ id: e.msg_id!, prompt: "", blocks: [], done: true,
          error: presentTurnProblem(e), doneTs: Date.now() });
        if (boundCompletedTurns) replaceWithBoundedTurns(rt, turns);
        else rt.turns = turns;
        rt.pendingQuestion = null;
      }, true);
    }
    case "user_msg": {
      const next = patch(state, e.sid, (rt) => {
        if (rt.acceptancePending === e.msg_id) {
          rt.acceptancePending = null;
          rt.acceptanceHistoryBaseline = null;
        }
        markTurnAsLive(rt, e.msg_id, boundCompletedTurns, e.seq);
        const turns = cloneTurns(rt.turns);
        const existing = turns.find((t) => t.id === e.msg_id);
        const imgs = (e.images && e.images.length) ? e.images : undefined;
        const fileMeta = (e.files && e.files.length)
          ? e.files.map((file) => ({ filename: file.filename, data: "" }))
          : undefined;
        // Server time correlates the optimistic id with transcript history. The
        // client clock may drift, so authoritative echo time replaces it.
        const stamp = e.ts ? Math.round(e.ts * 1000) : undefined;
        if (existing) {
          if (!existing.prompt && e.prompt) existing.prompt = e.prompt;
          if (!existing.images && imgs) existing.images = imgs;
          if (fileMeta) existing.files = fileMeta;
          else if (existing.files) existing.files = existing.files.map(
            (file) => ({ filename: file.filename, data: "" }));
          if (stamp) existing.ts = stamp;
        } else {
          turns.push({ id: e.msg_id, prompt: e.prompt, images: imgs,
            files: fileMeta, blocks: [], done: false, ts: stamp });
        }
        rt.turns = turns;
      });
      const sessions = e.sid
        ? bumpSessionActivity(next.sessions, e.sid, Math.round(e.ts * 1000))
        : next.sessions;
      return sessions === next.sessions ? next : { ...next, sessions };
    }
    case "assistant_msg_start":
      return patch(state, e.sid, (rt) => {
        const turns = cloneTurns(rt.turns);
        const t = openTurn(turns, e.message_id, eventTimestampMs(e.ts));
        markTurnAsLive(rt, t.id, boundCompletedTurns, e.seq);
        t.progress = undefined;
        const block = t.blocks.find((b) => b.kind === "text"
          && b.message_id === e.message_id) as TextBlock | undefined;
        if (block) block.channel = resolvedChannel(block.channel, e.channel ?? "unknown");
        else {
          t.blocks.push({ kind: "text", message_id: e.message_id, text: "",
            done: false, channel: e.channel ?? "unknown" });
          limitTurnBlocks(t);
        }
        rt.turns = turns;
      });
    case "delta":
      return patch(state, e.sid, (rt) => {
        const turns = cloneTurns(rt.turns);
        const t = openTurn(turns, e.message_id, eventTimestampMs(e.ts));
        markTurnAsLive(rt, t.id, boundCompletedTurns, e.seq);
        t.progress = undefined;
        let block = t.blocks.find((b) => b.kind === "text" && b.message_id === e.message_id) as TextBlock | undefined;
        if (!block) {
          block = { kind: "text", message_id: e.message_id, text: "", done: false,
            channel: e.channel ?? "unknown" };
          t.blocks.push(block);
          limitTurnBlocks(t);
        }
        block.channel = resolvedChannel(block.channel, e.channel ?? "unknown");
        block.text = appendField(block.text, e.text, MAX_LIVE_TEXT_CHARS);
        limitTurnBlocks(t);
        rt.turns = turns;
      });
    case "tool_use":
      return patch(state, e.sid, (rt) => {
        const turns = cloneTurns(rt.turns);
        const t = openTurn(turns, e.message_id, eventTimestampMs(e.ts));
        markTurnAsLive(rt, t.id, boundCompletedTurns, e.seq);
        t.progress = undefined;
        const existing = t.blocks.find((b) => b.kind === "tool"
          && b.tool_use_id === e.tool_use_id) as ToolBlock | undefined;
        if (existing) {
          existing.tool = e.tool;
          existing.input = e.input;
          existing.category = e.category ?? "tool";
          existing.title = e.title;
          existing.parent_id = e.parent_id;
          existing.server = e.server;
        } else {
          t.blocks.push({ kind: "tool", message_id: e.message_id,
            tool_use_id: e.tool_use_id, tool: e.tool, input: e.input,
            category: e.category ?? "tool", title: e.title, parent_id: e.parent_id,
            server: e.server, done: false });
          limitTurnBlocks(t);
        }
        rt.turns = turns;
      });
    case "tool_delta":
      return patch(state, e.sid, (rt) => {
        const turns = cloneTurns(rt.turns);
        for (const t of turns) {
          const block = t.blocks.find((b) => b.kind === "tool"
            && b.tool_use_id === e.tool_use_id) as ToolBlock | undefined;
          if (!block) continue;
          markTurnAsLive(rt, t.id, boundCompletedTurns, e.seq);
          if (e.stream === "progress" || e.stream === "summary") {
            block.progress = appendField(
              block.progress, e.delta, MAX_LIVE_PROGRESS_CHARS);
          } else if (e.stream === "diff") {
            block.diff = appendField(block.diff, e.delta, MAX_LIVE_DIFF_CHARS);
          } else {
            block.output = appendField(
              block.output, e.delta, MAX_LIVE_TOOL_OUTPUT_CHARS);
          }
          t.progress = undefined;
          limitTurnBlocks(t);
          break;
        }
        rt.turns = turns;
      });
    case "tool_result":
      return patch(state, e.sid, (rt) => {
        const turns = cloneTurns(rt.turns);
        for (const t of turns) {
          const b = t.blocks.find((b) => b.kind === "tool" && b.tool_use_id === e.tool_use_id) as ToolBlock | undefined;
          if (b) {
            markTurnAsLive(rt, t.id, boundCompletedTurns, e.seq);
            b.result = { content: e.content, is_error: e.is_error,
              truncated: e.truncated ?? undefined, status: e.status,
              summary: e.summary, diff: e.diff, exit_code: e.exit_code,
              duration_ms: e.duration_ms };
            if (e.diff) b.diff = e.diff;
            b.done = true;
            t.progress = undefined;
            limitTurnBlocks(t);
            break;
          }
        }
        rt.turns = turns;
      });
    case "assistant_msg_end":
      return patch(state, e.sid, (rt) => {
        const turns = cloneTurns(rt.turns);
        for (const t of turns) {
          const b = t.blocks.find((b) => b.kind === "text" && b.message_id === e.message_id) as TextBlock | undefined;
          if (b) {
            markTurnAsLive(rt, t.id, boundCompletedTurns, e.seq);
            b.channel = resolvedChannel(b.channel, e.channel ?? "unknown");
            b.done = true;
            break;
          }
        }
        rt.turns = turns;
      });
    case "process":
      return patch(state, e.sid, (rt) => {
        const turns = cloneTurns(rt.turns);
        let owner: Turn | undefined;
        let block: ProcessBlock | undefined;
        for (const candidate of turns) {
          const found = candidate.blocks.find((b) => b.kind === "process"
            && b.item_id === e.item_id) as ProcessBlock | undefined;
          if (found) { owner = candidate; block = found; break; }
        }
        // Background task/hook events may arrive after their originating turn
        // ended and after a newer query opened. Prefer their explicit parent or
        // engine turn id before falling back to the current tail; otherwise a
        // delayed subagent update creates a phantom new turn or attaches to the
        // wrong conversation.
        if (!owner) owner = findTurnOwningItem(turns, e.parent_id);
        if (!owner) owner = findTurnByEngineId(turns, e.turn_id);
        if (!owner) {
          owner = openTurn(
            turns, e.turn_id || e.item_id, eventTimestampMs(e.ts));
        }
        markTurnAsLive(rt, owner.id, boundCompletedTurns, e.seq);
        if (!block) {
          block = { kind: "process", item_id: e.item_id, processKind: e.kind,
            phase: e.phase, status: e.status, turn_id: e.turn_id,
            parent_id: e.parent_id, title: e.title, done: false };
          owner.blocks.push(block);
        }
        block.processKind = e.kind;
        block.phase = e.phase;
        block.status = e.status;
        block.turn_id = e.turn_id ?? block.turn_id;
        block.parent_id = e.parent_id ?? block.parent_id;
        block.title = e.title || block.title;
        if (e.summary != null) block.summary = e.summary;
        if (e.detail != null) block.detail = e.detail;
        if (e.input != null) block.input = e.input;
        if (e.output != null) block.output = e.output;
        if (e.diff != null) block.diff = e.diff;
        if (e.progress != null) block.progress = e.progress;
        if (e.server != null) block.server = e.server;
        if (e.tool != null) block.tool = e.tool;
        if (e.command != null) block.command = e.command;
        if (e.cwd != null) block.cwd = e.cwd;
        if (e.exit_code != null) block.exit_code = e.exit_code;
        if (e.duration_ms != null) block.duration_ms = e.duration_ms;
        if (e.truncated != null) block.truncated = e.truncated;
        if (e.append_to && e.delta) {
          if (e.append_to === "summary") {
            block.summary = appendField(
              block.summary, e.delta, MAX_LIVE_PROGRESS_CHARS);
          } else if (e.append_to === "detail") {
            block.detail = appendField(
              block.detail, e.delta, MAX_LIVE_DETAIL_CHARS);
          } else if (e.append_to === "output") {
            block.output = appendField(
              block.output, e.delta, MAX_LIVE_TOOL_OUTPUT_CHARS);
          } else if (e.append_to === "diff") {
            block.diff = appendField(block.diff, e.delta, MAX_LIVE_DIFF_CHARS);
          } else {
            block.progress = appendField(
              block.progress, e.delta, MAX_LIVE_PROGRESS_CHARS);
          }
        }
        block.done = e.phase === "end" || terminalProcessStatus(e.status);
        owner.progress = undefined;
        limitTurnBlocks(owner);
        rt.turns = turns;
      });
    case "turn_plan":
      return patch(state, e.sid, (rt) => {
        const turns = cloneTurns(rt.turns);
        let t = findTurnOwningItem(turns, e.item_id)
          ?? findTurnByEngineId(turns, e.turn_id);
        if (!t) {
          t = openTurn(
            turns, e.turn_id || e.item_id, eventTimestampMs(e.ts));
        }
        markTurnAsLive(rt, t.id, boundCompletedTurns, e.seq);
        let block = t.blocks.find((b) => b.kind === "process"
          && b.item_id === e.item_id) as ProcessBlock | undefined;
        if (!block) {
          block = { kind: "process", item_id: e.item_id, processKind: "plan",
            phase: "snapshot", status: "running", turn_id: e.turn_id,
            title: "计划", done: false };
          t.blocks.push(block);
        }
        block.explanation = e.explanation;
        block.plan = e.plan.map((entry) => ({ ...entry }));
        block.status = e.plan.length > 0 && e.plan.every((entry) => entry.status === "completed")
          ? "succeeded" : "running";
        block.done = block.status === "succeeded";
        t.progress = undefined;
        limitTurnBlocks(t);
        rt.turns = turns;
      });
    case "turn_diff":
      return patch(state, e.sid, (rt) => {
        const turns = cloneTurns(rt.turns);
        let t = findTurnOwningItem(turns, e.item_id)
          ?? findTurnByEngineId(turns, e.turn_id);
        if (!t) {
          t = openTurn(
            turns, e.turn_id || e.item_id, eventTimestampMs(e.ts));
        }
        markTurnAsLive(rt, t.id, boundCompletedTurns, e.seq);
        let block = t.blocks.find((b) => b.kind === "process"
          && b.item_id === e.item_id) as ProcessBlock | undefined;
        if (!block) {
          block = { kind: "process", item_id: e.item_id, processKind: "diff",
            phase: "snapshot", status: "running", turn_id: e.turn_id,
            title: "代码改动", done: false };
          t.blocks.push(block);
        }
        block.diff = e.diff;
        block.truncated = e.truncated;
        t.progress = undefined;
        limitTurnBlocks(t);
        rt.turns = turns;
      });
    case "turn_binding":
      return patch(state, e.sid, (rt) => {
        if (rt.acceptancePending === e.msg_id) {
          rt.acceptancePending = null;
          rt.acceptanceHistoryBaseline = null;
        }
        const turns = cloneTurns(rt.turns);
        const optimisticIndex = turns.findIndex((turn) => turn.id === e.msg_id);
        if (optimisticIndex < 0) return;
        turns[optimisticIndex].forkPointId = e.turn_id;
        const authoritativeIndex = turns.findIndex((turn, index) => (
          index !== optimisticIndex
          && (turn.forkPointId === e.turn_id || turn.id === e.turn_id)
        ));
        if (authoritativeIndex >= 0) {
          const optimistic = turns[optimisticIndex];
          const authoritative = turns[authoritativeIndex];
          const merged = mergeInitialHistory(
            [authoritative], [optimistic])[0] ?? optimistic;
          merged.forkPointId = e.turn_id;
          const first = Math.min(optimisticIndex, authoritativeIndex);
          const second = Math.max(optimisticIndex, authoritativeIndex);
          turns.splice(second, 1);
          turns.splice(first, 1, merged);
        }
        replaceWithBoundedTurns(rt, turns);
      });
    case "turn_end":
      return patch(state, e.sid, (rt) => {
        const turns = cloneTurns(rt.turns);
        let t = findTurnByEngineId(turns, e.turn_id);
        if (!t) {
          const openTurns = turns.filter((turn) => !turn.done);
          // Claude and older producers may reveal the native id only at the
          // terminal boundary. Preserve that legacy path when there is exactly
          // one unclosed owner, while never closing an unrelated completed row.
          if (openTurns.length === 1) t = openTurns[0];
        }
        if (t) {
          markTurnAsLive(rt, t.id, boundCompletedTurns, e.seq);
          if (rt.acceptancePending === t.id) {
            rt.acceptancePending = null;
            rt.acceptanceHistoryBaseline = null;
          }
          t.done = true;
          t.durationMs = e.result.duration_ms;
          if (e.turn_id) t.forkPointId = e.turn_id;
          if (e.checkpoint_id) t.checkpointId = e.checkpoint_id;
          t.progress = undefined;
          if (e.result.subtype === "error_during_execution") t.interrupted = true;
          // Stamp completion time from the event's own server ts (seconds -> ms).
          // Robust for BOTH live turns and replayed history: the old
          // `t.ts + duration_ms` reconstruction dropped the timestamp for any turn
          // without a client-side start time (i.e. everything after a refresh,
          // where turns come from history replay). Fall back to start time, then now.
          t.doneTs = e.ts ? Math.round(e.ts * 1000) : (t.ts || Date.now());
          finishOpenBlocks(t, e.result.is_error ? "interrupted" : "succeeded",
            e.result.is_error);
        }
        if (boundCompletedTurns) replaceWithBoundedTurns(rt, turns);
        else rt.turns = turns;
        // TurnEnd closes the visible turn, but the wrapper may still be
        // draining an interrupt, finishing a checkpoint, or releasing its
        // app-server consumer. Only the following authoritative State(idle)
        // unlocks the composer and queued/pending sends.
        rt.pendingQuestion = null;
      });
    case "pong":
    case "command_ack":
    case "history_image":
    case "session_forked":
    case "hello":
      return state;
  }
}
