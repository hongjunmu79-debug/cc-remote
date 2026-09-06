import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { createServer } from "vite";

import {
  clearLegacyAuthMarkers,
  probeSession,
  shouldReconnectAfterSessionProbe,
} from "../src/session-auth.ts";
import {
  canEnqueueQuery,
  collectWaitingQueries,
  queuedQueryWireBytes,
  reduceTargetedRuntime,
  selectDrainCandidates,
} from "../src/runtime-drain.ts";
import { mergeInitialHistory } from "../src/history-merge.ts";
import { imageDimensions } from "../src/img.ts";
import {
  historyImageDisplaySource,
  TurnImagePreviewCache,
} from "../src/turn-image-previews.ts";
import { boundCachedTurns, controlForCachedSession } from "../src/cache.ts";
import {
  boundRuntimeTurns,
  MAX_RUNTIME_TURNS,
  pruneRuntimeMap,
} from "../src/runtime-bounds.ts";
import { ImeSubmitGuard, shouldSubmitTextKey } from "../src/ime-submit.ts";
import {
  classifyBtwOpened,
  consumeDiscardedBtwSnapshot,
  makeOpenBtwCommand,
  matchesBtwRequest,
  normalizeDiffTheme,
  normalizeEngine,
} from "../src/protocol.ts";
import { RelayWs } from "../src/ws.ts";
import {
  clientSlashesFor,
  commandsFor,
  isKnownCodeOnlySlash,
  matchCommands,
  modelsFor,
  permsFor,
} from "../src/data.ts";
import {
  createMobileViewportSync,
  type MobileViewportBindings,
  type MobileViewportEvent,
  type ViewportReading,
} from "../src/use-mobile-viewport.ts";
import type { ServerEvent, SessionControl } from "../src/protocol.ts";
import type { Block } from "../src/reducer.ts";
import { clampPanelWidth, resolveSidebarSwipe } from "../src/responsive-layout.ts";
import {
  classifyTurnNotification,
  turnNotificationBody,
  turnNotificationTag,
} from "../src/turn-notification.ts";
import {
  classifyBusySubmit,
  isComposerBusy,
  isInterruptSettling,
  isSettlingStopDisabled,
} from "../src/composer-submit.ts";
import { workContextMetrics } from "../src/work-context.ts";
import { processBlocks } from "../src/process-blocks.ts";
import { PointerTapGuard } from "../src/pointer-tap.ts";
import {
  constrainImageTransform,
  panImageTransform,
  pinchImageTransform,
} from "../src/image-gesture.ts";
import {
  bumpSessionActivity,
  compareSessionsByActivity,
  mergeSessionActivityState,
  orderCodeDirectoryGroups,
  sessionCommandTarget,
  setSessionPinned,
  sessionActivityTime,
  visibleDirectorySessions,
} from "../src/session-order.ts";
import {
  ComposerDraftStore,
  composerDraftKey,
} from "../src/composer-drafts.ts";
import { positionTerminalCard } from "../src/terminal-card-position.ts";

assert.deepEqual(
  positionTerminalCard(
    { bottom: 50, right: 96 },
    { height: 800, width: 980 },
    true,
  ),
  { left: 315, top: 58, width: 350 },
  "the native Android host centers the card in a legacy wide layout viewport",
);
assert.deepEqual(
  positionTerminalCard(
    { bottom: 50, right: 96 },
    { height: 800, width: 390 },
    true,
  ),
  { left: 20, top: 58, width: 350 },
  "the centered card retains safe edge gaps on narrow viewports",
);
assert.deepEqual(
  positionTerminalCard(
    { bottom: 50, right: 96 },
    { height: 800, width: 980 },
    false,
  ),
  { left: 10, top: 58, width: 350 },
  "browser positioning remains trigger-aligned while clamping to the viewport",
);

const composerDrafts = new ComposerDraftStore();
const draftA = composerDraftKey("machine-a", "code", "codex", "session-a");
const draftB = composerDraftKey("machine-a", "code", "codex", "session-b");
const draftOtherSurface = composerDraftKey(
  "machine-a", "work", "codex", "session-a",
);
composerDrafts.set(draftA, {
  input: "session A unfinished",
  images: [{ media_type: "image/png", data: "draft-image" }],
  files: [{ filename: "notes.txt", data: "draft-file" }],
});
assert.equal(composerDrafts.get(draftB).input, "",
  "switching sessions must not carry the previous composer text");
assert.equal(composerDrafts.get(draftOtherSurface).input, "",
  "the same sid on another surface must have an independent draft");
composerDrafts.set(draftB, {
  input: "session B unfinished",
  images: [],
  files: [],
});
assert.equal(composerDrafts.get(draftA).input, "session A unfinished",
  "returning to a session restores only that session's draft");
assert.equal(composerDrafts.get(draftA).images[0]?.data, "draft-image");
assert.equal(composerDrafts.get(draftA).files[0]?.filename, "notes.txt");
const tempDraft = composerDraftKey("machine-a", "code", "codex", "tmp-new");
const realDraft = composerDraftKey("machine-a", "code", "codex", "real-session");
composerDrafts.set(tempDraft, {
  input: "typed while the first turn was running",
  images: [],
  files: [],
});
composerDrafts.rekey(tempDraft, realDraft);
assert.equal(composerDrafts.get(realDraft).input,
  "typed while the first turn was running",
  "session id capture must move a temp session draft atomically");
assert.equal(composerDrafts.get(tempDraft).input, "");

assert.equal(sessionActivityTime("2026-07-17T10:00:00Z"),
  Date.parse("2026-07-17T10:00:00Z"), "ISO Claude activity timestamps must sort correctly");
assert.ok(sessionActivityTime("1752746400000") > sessionActivityTime("1752746399"),
  "millisecond and second timestamps must share one ordering scale");

const processTap = new PointerTapGuard(8);
processTap.pointerDown(1, 20, 20);
processTap.pointerMove(1, 22, 24);
processTap.pointerUp(1);
assert.equal(processTap.consumeClick(1), true,
  "a stationary touch remains a real process-header tap");
processTap.pointerDown(2, 20, 20);
processTap.pointerMove(2, 20, 36);
processTap.pointerUp(2);
assert.equal(processTap.consumeClick(1), false,
  "vertical history scrolling must not toggle the process header");
processTap.pointerDown(3, 20, 20);
processTap.pointerDown(4, 30, 20);
processTap.pointerUp(3);
processTap.pointerUp(4);
assert.equal(processTap.consumeClick(1), false,
  "multi-touch must not synthesize a process-header activation");
processTap.pointerDown(5, 20, 20);
processTap.pointerCancel(5);
assert.equal(processTap.consumeClick(1), false,
  "a cancelled touch must not toggle the process header");
assert.equal(processTap.consumeClick(0), true,
  "keyboard-generated clicks must remain accessible");

assert.deepEqual(pinchImageTransform(
  { scale: 1, x: 0, y: 0 },
  { x: 100, y: 150 }, { x: 200, y: 150 },
  { x: 70, y: 160 }, { x: 270, y: 160 },
  { width: 300, height: 300 }, { width: 300, height: 300 },
), { scale: 2, x: 20, y: 10 },
"pinch zoom follows the two-finger midpoint instead of the image center");
assert.deepEqual(panImageTransform(
  { scale: 2, x: 0, y: 0 }, 500, -500,
  { width: 300, height: 200 }, { width: 300, height: 300 },
), { scale: 2, x: 150, y: -50 },
"zoomed image panning stays inside the visible bounds");
assert.deepEqual(constrainImageTransform(
  { scale: 0.5, x: 99, y: 99 },
  { width: 300, height: 200 }, { width: 300, height: 300 },
), { scale: 1, x: 0, y: 0 },
"returning to fit scale resets stale translation");
assert.equal(mergeSessionActivityState("running", "idle"), "running",
  "catalog-native activity must not be overwritten by an idle resident runtime");
assert.equal(mergeSessionActivityState("idle", "running"), "running",
  "a locally running resident runtime remains authoritative");
assert.equal(mergeSessionActivityState("running", "interrupting"), "interrupting",
  "an explicit local transition overrides catalog activity");

assert.deepEqual(
  permsFor("claude").map(({ id, name, short }) => ({ id, name, short })),
  [
    { id: "default", name: "Default", short: "Default" },
    { id: "acceptEdits", name: "Accept Edits", short: "Accept Edits" },
    { id: "plan", name: "Plan", short: "Plan" },
    { id: "auto", name: "Auto", short: "Auto" },
    { id: "bypassPermissions", name: "Bypass Permissions", short: "Bypass Permissions" },
  ],
  "Claude permission labels should match the official English mode names",
);
assert.deepEqual(
  permsFor("codex").map(({ id, name, short }) => ({ id, name, short })),
  [
    { id: "never", name: "Never", short: "Never" },
    { id: "on-request", name: "On Request", short: "On Request" },
    { id: "untrusted", name: "Untrusted", short: "Untrusted" },
  ],
  "Codex permission labels should match the official approval-policy names",
);
for (const engine of ["claude", "codex"] as const) {
  for (const slash of ["extensions", "skills", "plugins", "apps", "mcp", "hooks"]) {
    assert.equal(clientSlashesFor(engine).has(slash), true,
      `/${slash} must remain local for ${engine}`);
    assert.equal(commandsFor(engine, "work").some(
      (entry) => "slash" in entry && entry.slash === slash), true,
      `/${slash} must be reachable from Work`);
  }
}
assert.equal(clientSlashesFor("claude").has("hook"), false,
  "Claude singular /hook remains native; Remote management uses /hooks");
const recentProject = { session_id: "project-new", cwd: "/home/nancy/project",
  last_modified: "300" };
const oldHome = { session_id: "home-old", cwd: "/home/nancy", last_modified: "100" };
const projectOld = { session_id: "project-old", cwd: "/home/nancy/project",
  last_modified: "200" };
const directoryGroups = {
  "/home/nancy": [oldHome],
  "/home/nancy/project": [projectOld, recentProject].sort(compareSessionsByActivity),
};
assert.deepEqual(directoryGroups["/home/nancy/project"].map((session) => session.session_id),
  ["project-new", "project-old"], "sessions inside a directory must be newest first");
assert.deepEqual(orderCodeDirectoryGroups(directoryGroups),
  ["/home/nancy/project", "/home/nancy"],
  "the directory containing the newest session must be first");
const bumpedSessions = bumpSessionActivity(
  [oldHome, recentProject], "home-old", 400_000);
assert.equal(bumpedSessions[0].last_modified, "400000",
  "a live message must update sidebar activity without waiting for another list request");
assert.deepEqual(orderCodeDirectoryGroups({
  "/home/nancy": [bumpedSessions[0]],
  "/home/nancy/project": [recentProject],
}), ["/home/nancy", "/home/nancy/project"],
"an optimistically updated directory must move immediately");
assert.equal(setSessionPinned([oldHome], "home-old", true)[0].pinned, true,
  "pinning must update the visible list immediately");
assert.deepEqual(sessionCommandTarget(
  { ...recentProject, engine: "codex", space: "code" }, "claude", "work"),
{ engine: "codex", space: "code" },
"session actions must target the card engine and space, not the current page");
const sevenSessions = Array.from({ length: 7 }, (_, index) => ({
  session_id: `session-${index}`, last_modified: String(100 - index),
}));
assert.equal(visibleDirectorySessions(sevenSessions, false, false).length, 5,
  "a directory must show only five sessions by default");
assert.equal(visibleDirectorySessions(sevenSessions, true, false).length, 7,
  "expanded directories must show every session");
assert.equal(visibleDirectorySessions(sevenSessions, false, true).length, 7,
  "search results must never be hidden behind the five-session limit");

const visibleAgentTimeline = processBlocks([
  { kind: "tool", message_id: "assistant-1", tool_use_id: "agent-tool",
    tool: "Agent", input: {}, category: "agent", done: false },
  { kind: "process", item_id: "agent:agent-tool", processKind: "agent",
    phase: "start", status: "running", parent_id: "agent-tool",
    title: "审查后端", done: false },
] as import("../src/reducer.ts").Block[]);
assert.deepEqual(visibleAgentTimeline.map((block) => block.kind), ["process"],
  "a dedicated live agent row must replace the duplicate generic ToolUse row");

const legacyWorkContext = workContextMetrics({
  v: 19, ts: 0, type: "context_report",
  total_tokens: 25_572, max_tokens: 1_000_000,
  percentage: 2.5572, categories: [],
});
assert.equal(legacyWorkContext.hasBreakdown, false);
assert.equal(legacyWorkContext.sessionTokens, 25_572,
  "an old wrapper must retain the honest legacy total");
assert.equal(legacyWorkContext.sessionPercentage, 2.5572);

const freshWorkContext = workContextMetrics({
  v: 19, ts: 0, type: "context_report",
  total_tokens: 25_572, max_tokens: 1_000_000,
  percentage: 2.5572, session_tokens: 72, fixed_tokens: 25_500,
  session_percentage: 0.0072, categories: [],
});
assert.equal(freshWorkContext.hasBreakdown, true);
assert.equal(freshWorkContext.sessionTokens, 72);
assert.equal(freshWorkContext.fixedTokens, 25_500);
assert.equal(freshWorkContext.sessionPercentage, 0.0072);
assert.equal(freshWorkContext.totalPercentage, 2.5572);

const derivedWorkContext = workContextMetrics({
  v: 19, ts: 0, type: "context_report",
  total_tokens: 11_194, max_tokens: 353_400,
  percentage: 3.1675, fixed_tokens: 11_000, categories: [],
});
assert.equal(derivedWorkContext.sessionTokens, 194);
assert.ok(Math.abs(derivedWorkContext.sessionPercentage - 194 / 353_400 * 100) < 1e-9);

assert.equal(resolveSidebarSwipe(12, 200, 84, 205, 390, false), "open");
assert.equal(resolveSidebarSwipe(300, 200, 230, 205, 390, false), "close");
assert.equal(resolveSidebarSwipe(12, 100, 78, 240, 390, false), null,
  "a mostly vertical gesture must not navigate");
assert.equal(resolveSidebarSwipe(12, 200, 84, 205, 390, true), null,
  "an interactive vertical scroller must own its gesture");
assert.equal(clampPanelWidth(200, 1440), 360);
assert.equal(clampPanelWidth(2_000, 1440), 1_020);
assert.equal(clampPanelWidth(600, 1_000), 580);

assert.equal(isComposerBusy("idle"), false);
assert.equal(isComposerBusy("running"), true);
assert.equal(isComposerBusy("interrupting"), true);
assert.equal(isComposerBusy("draining"), true);
assert.equal(isInterruptSettling("running"), false);
assert.equal(isInterruptSettling("interrupting"), true);
assert.equal(isInterruptSettling("draining"), true);
assert.equal(isSettlingStopDisabled("running", false), false);
assert.equal(isSettlingStopDisabled("interrupting", false), true);
assert.equal(isSettlingStopDisabled("draining", false), true);
assert.equal(isSettlingStopDisabled("interrupting", true), false);
assert.equal(classifyBusySubmit("running", "interrupt", false), "noop",
  "empty Enter must not implicitly interrupt a running turn");
assert.equal(classifyBusySubmit("interrupting", "interrupt", false), "noop");
assert.equal(classifyBusySubmit("draining", "interrupt", false), "noop");
assert.equal(classifyBusySubmit(
  "running", "interrupt", true), "interrupt-and-replace");
assert.equal(classifyBusySubmit("interrupting", "interrupt", true), "replace");
assert.equal(classifyBusySubmit("draining", "interrupt", true), "replace");
assert.equal(classifyBusySubmit("interrupting", "queue", true), "enqueue");

const loginFormSource = readFileSync(resolve(
  process.cwd(), "src/components/LoginForm.tsx"), "utf8");
assert.match(loginFormSource, /window\.location\.hostname\.toLowerCase\(\)/);
assert.match(loginFormSource, /host === "127\.0\.0\.1"[\s\S]*void startPairing\(\)/,
  "the local Windows console must render a pairing QR without an extra click");
assert.match(loginFormSource, /type=\{passwordVisible \? "text" : "password"\}/);
assert.match(loginFormSource, /name="password"/);
assert.match(loginFormSource, /autoComplete="current-password"/);
assert.match(loginFormSource, /autoCapitalize="none"/);
assert.match(loginFormSource, /autoCorrect="off"/);
assert.match(loginFormSource, /spellCheck=\{false\}/);
assert.match(loginFormSource, /enterKeyHint="go"/);
assert.match(loginFormSource, /aria-label=\{passwordVisible \? "隐藏密码" : "显示密码"\}/);
assert.match(loginFormSource, /aria-pressed=\{passwordVisible\}/);
assert.match(loginFormSource, /selectionStart/);
assert.match(loginFormSource, /selectionEnd/);
assert.match(loginFormSource, /focus\(\{ preventScroll: true \}\)/);
assert.match(loginFormSource, /setSelectionRange\(/,
  "password visibility toggles must restore the desktop caret and selection");
assert.match(loginFormSource,
  /onPointerDown=\{\(event\) => \{\s*rememberPasswordSelection\(\);/,
  "the caret must be captured before a desktop browser handles pointer focus");
assert.match(loginFormSource, /requestAnimationFrame\(\(\) => \{\s*restoreSelection\(\);/,
  "the caret must be restored after the browser commits the input type change");

const reconnectBannerSource = readFileSync(resolve(
  process.cwd(), "src/components/ReconnectBanner.tsx"), "utf8");
assert.ok(reconnectBannerSource.includes('busy && <span className="sp"'),
  "only an active reconnect/replay may render the progress indicator");

const historyAppSource = readFileSync(resolve(process.cwd(), "src/App.tsx"), "utf8");
const cacheSource = readFileSync(resolve(process.cwd(), "src/cache.ts"), "utf8");
assert.match(historyAppSource, /HISTORY_INITIAL_PAGE\s*=\s*4/,
  "the newest history page must stay small enough for an immediate first paint");
assert.match(historyAppSource, /HISTORY_MORE_PAGE\s*=\s*12/,
  "older history must be delivered in bounded follow-up pages");
const historyBeforeResume = historyAppSource.match(
  /requestHistory\([\s\S]{0,160}?HISTORY_INITIAL_PAGE\);\s*(?:ws\.|wsRef\.current(?:\?\.|\.))sendSwitchSession/g,
) ?? [];
assert.equal(historyBeforeResume.length, 3,
  "every existing-session activation must request first paint before engine resume");
assert.match(historyAppSource, /history_invalidated[\s\S]*invalidateSessionCache/,
  "history invalidation must evict the matching IndexedDB row");
assert.match(historyAppSource, /historyCacheEpochRef[\s\S]*loadSession/,
  "an IndexedDB read started before the marker must be rejected");
assert.match(historyAppSource, /history_invalidated[\s\S]*requestHistory/,
  "a visible invalidated session must request authoritative history");
assert.match(historyAppSource,
  /msg\.type === "history" && msg\.authoritative !== false[\s\S]*allowSessionCache/,
  "a failed non-authoritative History must not reopen the IndexedDB cache barrier");
assert.match(historyAppSource, /artifact_invalidated[\s\S]*sendGetWorkArtifacts/,
  "file rollback must refresh the Work artifact inventory");
assert.match(historyAppSource,
  /const effectiveState = mergeSessionActivityState\([\s\S]*focusedSessionState/,
  "catalog or mirrored native work must paint a running header without granting a local Stop action");
assert.match(historyAppSource,
  /liveStates=\{Object\.fromEntries\(state\.sessions\.map[\s\S]*mergeSessionActivityState/,
  "catalog or mirrored native work must paint a running session badge in the sidebar");
const rewindChatViewSource = readFileSync(resolve(
  process.cwd(), "src/components/ChatView.tsx"), "utf8");
const rewindComposerSource = readFileSync(resolve(
  process.cwd(), "src/components/Composer.tsx"), "utf8");
assert.equal(commandsFor("claude").some((command) => (
  "slash" in command && command.slash === "rewind"
)), false, "Claude rewind must stay hidden from the command palette");
assert.equal(commandsFor("codex").some((command) => (
  "slash" in command && command.slash === "rollback"
)), false, "Codex rollback must stay hidden from the command palette");
const claudeWorkSlashes = commandsFor("claude", "work")
  .filter((command) => "slash" in command)
  .map((command) => command.slash);
const codexWorkSlashes = commandsFor("codex", "work")
  .filter((command) => "slash" in command)
  .map((command) => command.slash);
assert.deepEqual(codexWorkSlashes, claudeWorkSlashes,
  "Work must expose one engine-neutral command surface");
for (const slash of ["model", "goal", "btw", "preview", "context", "clear"]) {
  assert.equal(claudeWorkSlashes.includes(slash), true, `Work must retain /${slash}`);
}
assert.equal(claudeWorkSlashes.includes("permissions"), false,
  "Work permissions are fixed by its private workspace and must not be presented as editable");
assert.equal(isKnownCodeOnlySlash("permissions", "claude"), true);
assert.equal(isKnownCodeOnlySlash("permissions", "codex"), true);
for (const slash of ["plan", "code-review", "security-review", "verify", "simplify", "run", "init"]) {
  assert.equal(claudeWorkSlashes.includes(slash), false, `Work must hide Code /${slash}`);
  assert.equal(isKnownCodeOnlySlash(slash, "claude"), true);
}
for (const slash of ["review", "init", "plan", "fast", "status", "compact", "rollback"]) {
  assert.equal(codexWorkSlashes.includes(slash), false, `Work must hide Codex Code /${slash}`);
  assert.equal(isKnownCodeOnlySlash(slash, "codex"), true);
}
assert.equal(isKnownCodeOnlySlash("my-personal-skill", "claude"), false,
  "unknown user skills must remain available in Work");
assert.deepEqual(matchCommands("", "claude", "work").map((command) => command.slash),
  claudeWorkSlashes);
assert.deepEqual(matchCommands("pla", "codex", "work"), []);
assert.doesNotMatch(historyAppSource, /openClaudeRewind|openLatestClaudeRewind/,
  "Claude rewind must not have an App entry point while unsupported");
assert.doesNotMatch(rewindChatViewSource, /onRewind|回到这里/,
  "Claude messages must not expose a rewind action while unsupported");
assert.match(rewindComposerSource, /case "rewind": flash\("Claude Rewind 暂未开放"\)/,
  "a manually typed hidden rewind command must be blocked locally");
assert.match(rewindComposerSource, /case "rollback": flash\("Codex Rollback 暂未开放"\)/,
  "a manually typed hidden Codex rollback command must be blocked locally");
assert.match(historyAppSource, /replay_start[\s\S]*requestHistory/,
  "a replay gap must request authoritative history instead of ending on an empty view");
assert.match(historyAppSource, /replay_start[\s\S]*setWorkArtifactsBySid/,
  "a replay gap must discard a possibly stale Work artifact inventory");
assert.match(cacheSource, /const CACHE_VER = 9/,
  "open-turn merge repair must invalidate duplicate IndexedDB projections");
assert.match(cacheSource, /objectStore\(STORE\)\.delete\(sessionId\)/);
assert.match(cacheSource, /job\.epoch !== sessionEpoch\(job\.sid\)/,
  "a debounced pre-marker write must not recreate the deleted cache row");

const viewportListeners = new Map<MobileViewportEvent, Set<() => void>>();
const viewportCss = new Map<string, string>();
const viewportFrames = new Map<number, () => void>();
const viewportDelays = new Map<number, () => void>();
let nextViewportTaskId = 1;
let viewportReading: ViewportReading = {
  height: 844, layoutHeight: 844, offsetTop: 0, scale: 1,
};
let editableFocused = false;
let layoutScrollResets = 0;
const viewportBindings: MobileViewportBindings = {
  readViewport: () => viewportReading,
  setCssProperty: (name, value) => { viewportCss.set(name, value); },
  clearCssProperty: (name) => { viewportCss.delete(name); },
  listen: (event, listener) => {
    const listeners = viewportListeners.get(event) ?? new Set();
    listeners.add(listener);
    viewportListeners.set(event, listeners);
    return () => listeners.delete(listener);
  },
  requestFrame: (listener) => {
    const id = nextViewportTaskId++;
    viewportFrames.set(id, listener);
    return id;
  },
  cancelFrame: (id) => { viewportFrames.delete(id); },
  setDelay: (listener) => {
    const id = nextViewportTaskId++;
    viewportDelays.set(id, listener);
    return id;
  },
  clearDelay: (id) => { viewportDelays.delete(id); },
  isEditableFocused: () => editableFocused,
  resetLayoutScroll: () => { layoutScrollResets += 1; },
};
const flushViewportFrames = () => {
  const frames = [...viewportFrames.values()];
  viewportFrames.clear();
  for (const frame of frames) frame();
};
const emitViewport = (event: MobileViewportEvent) => {
  for (const listener of viewportListeners.get(event) ?? []) listener();
};

const stopViewportSync = createMobileViewportSync(viewportBindings);
assert.equal(viewportCss.get("--app-height"), "844px");
assert.equal(viewportCss.get("--app-offset-top"), "0px");
assert.equal(viewportCss.get("--keyboard-inset"), "0px");

viewportReading = { height: 510.25, layoutHeight: 844, offsetTop: 0, scale: 1 };
emitViewport("viewport-resize");
assert.equal(viewportCss.get("--app-height"), "844px");
flushViewportFrames();
assert.equal(viewportCss.get("--app-height"), "510.25px");
assert.equal(viewportCss.get("--keyboard-inset"), "333.75px");

viewportReading = { height: 500, layoutHeight: 844, offsetTop: 24, scale: 1 };
emitViewport("viewport-scroll");
flushViewportFrames();
assert.equal(viewportCss.get("--app-height"), "500px");
assert.equal(viewportCss.get("--app-offset-top"), "24px");
assert.equal(viewportCss.get("--keyboard-inset"), "320px");

// Pinch zoom remains user-controlled and must not be treated as a keyboard.
viewportReading = { height: 420, layoutHeight: 844, offsetTop: 30, scale: 1.5 };
emitViewport("viewport-resize");
flushViewportFrames();
assert.equal(viewportCss.get("--app-offset-top"), "0px");
assert.equal(viewportCss.get("--keyboard-inset"), "0px");

// Safari can report its pre-blur viewport for a few animation frames. Delayed
// settling rereads it and clears only the layout-level focus pan.
viewportReading = { height: 844, layoutHeight: 844, offsetTop: 0, scale: 1 };
emitViewport("focus-out");
assert.equal(viewportDelays.size, 2);
flushViewportFrames();
for (const delayed of [...viewportDelays.values()]) delayed();
viewportDelays.clear();
flushViewportFrames();
assert.equal(layoutScrollResets, 2);
assert.equal(viewportCss.get("--app-height"), "844px");

editableFocused = true;
emitViewport("orientation-change");
for (const delayed of [...viewportDelays.values()]) delayed();
viewportDelays.clear();
flushViewportFrames();
assert.equal(layoutScrollResets, 2);
editableFocused = false;

// Repeated keyboard cycles must always settle back to the full viewport instead
// of accumulating height, offset, or bottom-inset drift.
for (let cycle = 0; cycle < 10; cycle += 1) {
  viewportReading = { height: 508, layoutHeight: 844, offsetTop: 18, scale: 1 };
  emitViewport("viewport-resize");
  flushViewportFrames();
  viewportReading = { height: 844, layoutHeight: 844, offsetTop: 0, scale: 1 };
  emitViewport("focus-out");
  flushViewportFrames();
  for (const delayed of [...viewportDelays.values()]) delayed();
  viewportDelays.clear();
  flushViewportFrames();
  assert.equal(viewportCss.get("--app-height"), "844px");
  assert.equal(viewportCss.get("--app-offset-top"), "0px");
  assert.equal(viewportCss.get("--keyboard-inset"), "0px");
}
assert.equal(layoutScrollResets, 22);

stopViewportSync();
assert.equal(viewportCss.has("--app-height"), false);
assert.equal(viewportCss.has("--app-offset-top"), false);
assert.equal(viewportCss.has("--keyboard-inset"), false);
assert.equal(viewportFrames.size, 0);
assert.equal(viewportDelays.size, 0);
for (const listeners of viewportListeners.values()) assert.equal(listeners.size, 0);

assert.equal(shouldSubmitTextKey({
  key: "Enter", shiftKey: false, isComposing: false, keyCode: 13,
}), true);
assert.equal(shouldSubmitTextKey({
  key: "Enter", shiftKey: true, isComposing: false, keyCode: 13,
}), false);
assert.equal(shouldSubmitTextKey({
  key: "Enter", shiftKey: false, isComposing: true, keyCode: 13,
}), false);
assert.equal(shouldSubmitTextKey({
  key: "Enter", shiftKey: false, isComposing: false, keyCode: 229,
}), false);
assert.equal(shouldSubmitTextKey({
  key: "Space", shiftKey: false, isComposing: false, keyCode: 32,
}), false);

const imeSubmit = new ImeSubmitGuard();
assert.equal(imeSubmit.shouldSubmitKey({
  key: "Enter", shiftKey: false, isComposing: false, keyCode: 13,
}), true);
imeSubmit.startComposition();
assert.equal(imeSubmit.shouldSubmitKey({
  key: "Enter", shiftKey: false, isComposing: false, keyCode: 13,
}), false);
assert.equal(imeSubmit.shouldCommitBeforeButtonSubmit(), true);
imeSubmit.endComposition();
assert.equal(imeSubmit.shouldSubmitKey({
  key: "Enter", shiftKey: false, isComposing: false, keyCode: 13,
}), true);
assert.equal(imeSubmit.shouldCommitBeforeButtonSubmit(), false);

// Every text-entry surface that can submit outside the three composers must use
// the same IME guard. Browser-level composition ordering remains an E2E release
// gate; this integration check prevents a raw Enter handler from bypassing the
// shared path before that suite runs.
for (const filename of [
  "QuestionSheet.tsx",
  "SessionsSidebar.tsx",
  "DirPicker.tsx",
  "LoginForm.tsx",
  "ForkWorktreeSheet.tsx",
]) {
  const source = readFileSync(resolve(process.cwd(), "src/components", filename), "utf8");
  assert.match(source, /useImeSubmit/);
  assert.match(source, /onCompositionStart/);
  assert.match(source, /nativeEvent\.isComposing/);
  assert.match(source, /nativeEvent\.keyCode/);
}

// Desktop sidebar motion must not regress to the old discrete grid-track swap.
// Chromium could strand an animated 0px/1fr grid, so the safe implementation
// slides the fixed sidebar and offsets the pane using ordinary CSS lengths.
const layoutCss = readFileSync(resolve(process.cwd(), "src/index.css"), "utf8");
assert.doesNotMatch(layoutCss, /transition\s*:\s*grid-template-columns/);
assert.match(layoutCss, /\.shell\.sidebar-open \.pane\s*\{[^}]*margin-left\s*:\s*352px/s);
assert.match(layoutCss, /\.pane\s*\{[^}]*transition\s*:[^}]*margin-left[^}]*width/s);
assert.match(layoutCss, /\.sessions\s*\{[^}]*position\s*:\s*fixed[^}]*width\s*:\s*352px/s);
assert.match(layoutCss,
  /@media \(max-width:980px\)\{\s*\.artifact-panel\{[^}]*top:calc\(var\(--app-offset-top,0px\) \+ 10px\)[^}]*bottom:auto[^}]*height:calc\(var\(--app-height,100dvh\) - 20px\)[^}]*max-height:none/s,
  "mobile artifact previews must fill the visual viewport instead of shrink-wrapping iframe or Markdown content");

let requested = "";
const authenticated = await probeSession(async (input, init) => {
  requested = input;
  assert.equal(init.credentials, "same-origin");
  assert.equal(init.cache, "no-store");
  assert.equal(init.signal.aborted, false);
  return { ok: true, status: 200 };
}, 100);
assert.equal(requested, "/api/session");
assert.equal(authenticated, "authenticated");

const unauthorized = await probeSession(
  async () => ({ ok: false, status: 401 }), 100);
assert.equal(unauthorized, "unauthorized");
assert.equal(shouldReconnectAfterSessionProbe(unauthorized), false);

const serverError = await probeSession(
  async () => ({ ok: false, status: 503 }), 100);
assert.equal(serverError, "unavailable");
assert.equal(shouldReconnectAfterSessionProbe(serverError), true);

const networkError = await probeSession(async () => {
  throw new Error("offline");
}, 100);
assert.equal(networkError, "unavailable");
assert.equal(shouldReconnectAfterSessionProbe(networkError), true);

const timeout = await probeSession(
  async () => new Promise(() => {}), 1);
assert.equal(timeout, "unavailable");

const removed: string[] = [];
clearLegacyAuthMarkers({ removeItem: (key) => { removed.push(key); } });
assert.deepEqual(removed, ["cc_remote_session", "cc_remote_authenticated"]);

const pending = {
  prompt: "pending-a",
  images: [{ media_type: "image/png", data: "img" }],
};
const queued = {
  prompt: "queued-b",
  files: [{ name: "note.txt", media_type: "text/plain", data: "file" }],
};
const runtimes = {
  a: { state: "idle", syncReady: true, pendingSend: pending, queue: [{ prompt: "later-a" }] },
  b: { state: "idle", syncReady: true, pendingSend: null, queue: [queued] },
  c: { state: "running", syncReady: true, pendingSend: null, queue: [{ prompt: "busy-c" }] },
  d: { state: "idle", syncReady: true, pendingSend: null, queue: [{ prompt: "draining-d" }] },
  e: { state: "idle", syncReady: false, pendingSend: null, queue: [{ prompt: "stale-e" }] },
  f: { state: "idle", syncReady: true, external: true, pendingSend: null, queue: [{ prompt: "external-f" }] },
};
assert.deepEqual(
  selectDrainCandidates(runtimes, new Set(["d"]), true, true),
  [
    { sid: "a", source: "pending", query: pending },
    { sid: "b", source: "queue", query: queued },
  ],
);
assert.deepEqual(selectDrainCandidates(runtimes, new Set(), false, true), []);
assert.deepEqual(selectDrainCandidates(runtimes, new Set(), true, false), []);

const sizedQuery = {
  prompt: "queued",
  files: [{ filename: "secret.txt", data: "sensitive-body" }],
};
const sizedQueryBytes = queuedQueryWireBytes(sizedQuery);
assert.equal(canEnqueueQuery([], sizedQuery, 32, sizedQueryBytes), true);
assert.equal(canEnqueueQuery([], sizedQuery, 32, sizedQueryBytes - 1), false);
assert.equal(canEnqueueQuery([sizedQuery], sizedQuery, 1, sizedQueryBytes * 3), false);
assert.equal(canEnqueueQuery(
  [sizedQuery], sizedQuery, 32, sizedQueryBytes * 2), true);
assert.deepEqual(collectWaitingQueries({
  one: { queue: ["queued"], pendingSend: "replace-me" },
  two: { queue: [], pendingSend: "other-pending" },
}, "one"), ["queued", "other-pending"]);

const a = {
  state: "idle", syncReady: true, pendingSend: null, queue: [{ prompt: "a" }],
  turns: [{ id: "a-old", prompt: "old" }],
};
const b = {
  state: "idle", syncReady: true, pendingSend: { prompt: "pending-b" },
  queue: [{ prompt: "b0" }, { prompt: "b1" }],
  turns: [{ id: "b-old", prompt: "old" }],
};
const runtimeMap = { a, b };
const withTurn = reduceTargetedRuntime(runtimeMap, "b", {
  type: "query_sent", turn: { id: "b-new", prompt: "new" },
});
assert.strictEqual(withTurn.a, a);
assert.deepEqual(withTurn.a.turns.map((turn) => turn.id), ["a-old"]);
assert.deepEqual(withTurn.b.turns.map((turn) => turn.id), ["b-old", "b-new"]);

const dequeued = reduceTargetedRuntime(withTurn, "b", { type: "dequeue_at", i: 0 });
assert.deepEqual(dequeued.a.queue.map((query) => query.prompt), ["a"]);
assert.deepEqual(dequeued.b.queue.map((query) => query.prompt), ["b1"]);

const cleared = reduceTargetedRuntime(dequeued, "b", { type: "clear_pending" });
assert.equal(cleared.a.pendingSend, null);
assert.equal(cleared.b.pendingSend, null);
assert.strictEqual(
  reduceTargetedRuntime(cleared, "missing", { type: "clear_pending" }),
  cleared,
);

const transcriptTurn = {
  id: "engine-id", prompt: "same prompt", done: true, ts: 1000,
  blocks: [{ kind: "text" as const, message_id: "engine-text", text: "answer", done: true }],
};
const optimisticTurn = {
  id: "client-id", prompt: "same prompt", done: false, ts: 1100,
  blocks: [{ kind: "text" as const, message_id: "live-text", text: "answer tail", done: false }],
};
const laggingDone = {
  id: "client-lag", prompt: "not flushed", done: true, ts: 2000, blocks: [],
};
const mergedHistory = mergeInitialHistory(
  [transcriptTurn], [optimisticTurn, laggingDone]);
assert.deepEqual(mergedHistory.map((turn) => turn.id), ["client-id", "client-lag"]);
assert.equal(mergedHistory[0].done, true);
assert.equal(mergedHistory[0].blocks.length, 1);
assert.equal(mergedHistory[1].prompt, "not flushed");

const repeatedOld = {
  id: "old-engine", prompt: "继续", done: true, ts: 10_000,
  blocks: [{ kind: "text" as const, message_id: "old-answer", text: "old", done: true }],
};
const repeatedCurrent = {
  id: "current-client", prompt: "继续", done: false, ts: 15_000, blocks: [],
};
const repeatedMerged = mergeInitialHistory([repeatedOld], [repeatedCurrent]);
assert.deepEqual(repeatedMerged.map((turn) => turn.id), ["old-engine", "current-client"]);
assert.equal(repeatedMerged[1].done, false);

const delayedEcho = mergeInitialHistory(
  [{ ...transcriptTurn, id: "engine-delayed", ts: 20_000 }],
  [{ ...optimisticTurn, id: "client-delayed", ts: 22_500 }],
);
assert.deepEqual(delayedEcho.map((turn) => turn.id), ["client-delayed"]);

// History and live ids are generated by different streams. Multiple blocks in
// the same channel must pair in order/content instead of all merging into the
// last commentary block.
const multiCommentHistory = {
  id: "multi-history", prompt: "inspect", done: true, ts: 30_000,
  blocks: [
    { kind: "text" as const, message_id: "hist-a", text: "A", done: true,
      channel: "commentary" as const },
    { kind: "tool" as const, message_id: "hist-a", tool_use_id: "shared-tool",
      tool: "shell", input: {}, done: true,
      result: { content: "ok", is_error: false } },
    { kind: "text" as const, message_id: "hist-b", text: "B", done: true,
      channel: "commentary" as const },
  ],
};
const multiCommentLive = {
  id: "multi-live", prompt: "inspect", done: true, ts: 30_100,
  blocks: [
    { kind: "text" as const, message_id: "live-a", text: "A", done: true,
      channel: "commentary" as const },
    { kind: "tool" as const, message_id: "live-a", tool_use_id: "shared-tool",
      tool: "shell", input: {}, done: true,
      result: { content: "ok", is_error: false } },
    { kind: "text" as const, message_id: "live-b", text: "B", done: true,
      channel: "commentary" as const },
  ],
};
const multiCommentMerged = mergeInitialHistory(
  [multiCommentHistory], [multiCommentLive]);
assert.deepEqual(multiCommentMerged[0].blocks.filter(
  (block) => block.kind === "text").map((block) => block.text), ["A", "B"]);

// Tool-only assistant envelopes have a start/end but no text delta. Summary
// history omits those invisible shells while loaded detail retains them. A
// completed empty shell must not consume the next same-channel summary block:
// doing so shifts every later commentary match and duplicates the final one
// after a focus-triggered History reconciliation.
const toolOnlyShellSummary = {
  id: "tool-shell-turn", prompt: "modify code", done: true, ts: 32_000,
  blocks: [
    { kind: "text" as const, message_id: "comment-a", text: "A", done: true,
      channel: "commentary" as const },
    { kind: "text" as const, message_id: "comment-b", text: "B", done: true,
      channel: "commentary" as const },
    { kind: "text" as const, message_id: "comment-c", text: "C", done: true,
      channel: "commentary" as const },
  ],
};
const toolOnlyShellDetail = {
  ...toolOnlyShellSummary,
  blocks: [
    toolOnlyShellSummary.blocks[0],
    { kind: "text" as const, message_id: "tool-envelope-a", text: "",
      done: true, channel: "commentary" as const },
    { kind: "tool" as const, message_id: "tool-envelope-a",
      tool_use_id: "edit-a", tool: "apply_patch", input: {}, done: true,
      result: { content: "updated", is_error: false } },
    toolOnlyShellSummary.blocks[1],
    { kind: "text" as const, message_id: "tool-envelope-b", text: "",
      done: true, channel: "commentary" as const },
    { kind: "tool" as const, message_id: "tool-envelope-b",
      tool_use_id: "edit-b", tool: "shell", input: {}, done: true,
      result: { content: "verified", is_error: false } },
    toolOnlyShellSummary.blocks[2],
  ],
};
let toolOnlyShellMerged = mergeInitialHistory(
  [toolOnlyShellSummary], [toolOnlyShellDetail]);
toolOnlyShellMerged = mergeInitialHistory(
  [toolOnlyShellSummary], toolOnlyShellMerged);
assert.deepEqual(toolOnlyShellMerged[0].blocks.flatMap(
  (block) => block.kind === "text" && block.text.length > 0
    ? [{ id: block.message_id, text: block.text }]
    : []),
[
  { id: "comment-a", text: "A" },
  { id: "comment-b", text: "B" },
  { id: "comment-c", text: "C" },
], "completed empty tool envelopes must not shift or duplicate commentary");

// Rollout history and the live app-server now share the authoritative
// response_item id. The live delta may still arrive before item/started and
// temporarily carry channel=unknown; identity, not text guessing, must merge it
// into the canonical commentary block.
const stableMessageHistory = {
  id: "stable-history", forkPointId: "stable-turn", prompt: "inspect",
  done: true, ts: 35_000,
  blocks: [{
    kind: "text" as const, message_id: "msg-stable",
    text: "same item", done: true, channel: "commentary" as const,
  }],
};
const stableMessageLive = {
  id: "stable-turn", forkPointId: "stable-turn", prompt: "inspect",
  done: false, ts: 35_100,
  blocks: [{
    kind: "text" as const, message_id: "msg-stable",
    text: "same item", done: false, channel: "unknown" as const,
  }],
};
const stableMessageMerged = mergeInitialHistory(
  [stableMessageHistory], [stableMessageLive],
  { preserveLiveTailOpen: true },
);
assert.equal(stableMessageMerged.length, 1);
assert.deepEqual(stableMessageMerged[0].blocks.filter(
  (block) => block.kind === "text").map((block) => ({
    id: block.message_id, text: block.text, channel: block.channel,
  })), [{
  id: "msg-stable", text: "same item", channel: "commentary",
}]);

const openEmptyMessageMerged = mergeInitialHistory(
  [{
    ...stableMessageHistory,
    blocks: [{
      kind: "text" as const, message_id: "history-before-delta",
      text: "history prefix", done: true, channel: "commentary" as const,
    }],
  }],
  [{
    ...stableMessageLive,
    blocks: [{
      kind: "text" as const, message_id: "live-before-delta",
      text: "", done: false, channel: "commentary" as const,
    }],
  }],
  { preserveLiveTailOpen: true },
);
assert.deepEqual(openEmptyMessageMerged[0].blocks.flatMap(
  (block) => block.kind === "text"
    ? [{ id: block.message_id, text: block.text }]
    : []),
[{ id: "live-before-delta", text: "history prefix" }],
"an open empty placeholder must retain the id targeted by future deltas");

// A focus-triggered History page can use a regenerated response_item id while
// the app-server keeps streaming deltas to the live id. Reconciliation must
// retain that live binding; otherwise the next delta creates a second text
// block, and every Codex -> Claude -> Codex switch paints another copy.
const regeneratedMessageHistory = {
  id: "regenerated-history", forkPointId: "regenerated-turn",
  prompt: "inspect", done: true, ts: 35_500,
  blocks: [{
    kind: "text" as const, message_id: "history-commentary",
    text: "first half", done: true, channel: "commentary" as const,
  }],
};
const regeneratedMessageLive = {
  id: "regenerated-turn", forkPointId: "regenerated-turn",
  prompt: "inspect", done: false, ts: 35_600,
  blocks: [{
    kind: "text" as const, message_id: "live-commentary",
    text: "first half", done: false, channel: "commentary" as const,
  }],
};
let regeneratedMessageMerged = mergeInitialHistory(
  [regeneratedMessageHistory], [regeneratedMessageLive],
  { preserveLiveTailOpen: true },
);
assert.equal(
  regeneratedMessageMerged[0].blocks[0]?.kind === "text"
    && regeneratedMessageMerged[0].blocks[0].message_id,
  "live-commentary",
  "an open history merge must keep the id targeted by future live deltas",
);
for (const suffix of [" plus tools", " and answer"]) {
  regeneratedMessageMerged = regeneratedMessageMerged.map((turn) => ({
    ...turn,
    blocks: turn.blocks.map((block) => block.kind === "text"
      ? { ...block, text: block.text + suffix } : block),
  }));
  regeneratedMessageMerged = mergeInitialHistory(
    [regeneratedMessageHistory], regeneratedMessageMerged,
    { preserveLiveTailOpen: true },
  );
  assert.equal(
    regeneratedMessageMerged[0].blocks.filter(
      (block) => block.kind === "text").length,
    1,
    "repeated focus History merges remain idempotent while the turn runs",
  );
  assert.equal(
    regeneratedMessageMerged[0].blocks[0]?.kind === "text"
      && regeneratedMessageMerged[0].blocks[0].message_id,
    "live-commentary",
  );
}

// Once summary history exposes payload-free image references, it replaces the
// optimistic inline bodies. Keeping both representations renders two image
// rows and leaves a large blank placeholder below the visible thumbnail.
const imageHistory = {
  id: "image-history", prompt: "看图", done: true, ts: 36_000,
  imageRefs: [{
    image_id: "image-1", media_type: "image/png" as const,
    width: 1200, height: 600, byte_size: 1024,
  }],
  blocks: [],
};
const imageLive = {
  id: "image-live", prompt: "看图", done: false, ts: 36_100,
  images: [{ media_type: "image/png" as const, data: "inline-body" }],
  blocks: [],
};
const imageMerged = mergeInitialHistory([imageHistory], [imageLive]);
assert.equal(imageMerged.length, 1);
assert.equal(imageMerged[0].images, undefined);
assert.deepEqual(imageMerged[0].imageRefs, imageHistory.imageRefs);
const turnImagePreviews = new TurnImagePreviewCache();
turnImagePreviews.update("image-session", [imageLive]);
turnImagePreviews.update("image-session", [imageMerged[0]]);
const retainedImagePreview = turnImagePreviews.get(imageMerged[0].id, 0);
assert.deepEqual(retainedImagePreview, imageLive.images[0],
  "a canonical History swap retains the already-painted optimistic preview");
assert.equal(
  historyImageDisplaySource({ status: "loading" }, retainedImagePreview),
  "data:image/png;base64,inline-body",
  "the loading thumbnail cannot blank the image during the thinking transition",
);
assert.equal(
  historyImageDisplaySource({
    status: "ready", mediaType: "image/webp", data: "canonical-thumbnail",
  }, retainedImagePreview),
  "data:image/webp;base64,canonical-thumbnail",
  "the canonical thumbnail replaces the fallback without a placeholder frame",
);
turnImagePreviews.release(imageMerged[0].id);
assert.equal(turnImagePreviews.get(imageMerged[0].id, 0), undefined);

// A complete transcript must not be reopened forever by stale cache state.
const terminalHistory = {
  id: "terminal-history", prompt: "run", done: true, ts: 40_000,
  blocks: [
    { kind: "tool" as const, message_id: "tool-msg", tool_use_id: "tool-terminal",
      tool: "shell", input: {}, done: true,
      result: { content: "ok", is_error: false } },
    { kind: "process" as const, item_id: "agent-terminal", processKind: "agent" as const,
      phase: "end" as const, status: "succeeded" as const, title: "代理完成", done: true },
  ],
};
const staleLive = {
  id: "terminal-live", prompt: "run", done: false, ts: 40_100,
  blocks: [
    { kind: "tool" as const, message_id: "tool-msg-live", tool_use_id: "tool-terminal",
      tool: "shell", input: {}, done: false },
    { kind: "process" as const, item_id: "agent-terminal", processKind: "agent" as const,
      phase: "update" as const, status: "running" as const, title: "代理运行中", done: false },
  ],
};
const terminalMerged = mergeInitialHistory([terminalHistory], [staleLive]);
assert.equal(terminalMerged[0].blocks.every((block) => block.done), true);
assert.equal(terminalMerged[0].blocks[0].kind === "tool"
  && terminalMerged[0].blocks[0].result?.content, "ok");
assert.equal(terminalMerged[0].blocks[1].kind === "process"
  && terminalMerged[0].blocks[1].title, "代理完成");
const liveDurationMerged = mergeInitialHistory(
  [{ ...terminalHistory, durationMs: 0 }],
  [{ ...staleLive, durationMs: 5000 }],
);
assert.equal(liveDurationMerged[0].durationMs, 5000,
  "a synthetic Claude history duration must not erase a real live duration");
const openTailMerged = mergeInitialHistory(
  [terminalHistory], [staleLive], { preserveLiveTailOpen: true });
assert.equal(openTailMerged[0].blocks.some((block) => !block.done), true);

// Never collapse two real repeated turns just because both prompt and response
// happen to be identical. Stable ids remain the only authoritative identity.
const repeatedAnswerTurns = [
  { id: "repeat-1", prompt: "在？", done: true, ts: 5000, doneTs: 6000,
    blocks: [{ kind: "text" as const, message_id: "repeat-answer-1",
      text: "在的", done: true }] },
  { id: "repeat-2", prompt: "在？", done: true, ts: 7000, doneTs: 8000,
    blocks: [{ kind: "text" as const, message_id: "repeat-answer-2",
      text: "在的", done: true }] },
];
assert.deepEqual(
  mergeInitialHistory(repeatedAnswerTurns, []).map((turn) => turn.id),
  ["repeat-1", "repeat-2"],
);

// Exact production race at the pure merge boundary: a focus-triggered History
// synthesizes TurnEnd while the matching live tail is still running. Preserve
// that open tail, let the live answer finish in place, then reconcile the full
// transcript without creating a second assistant-only turn.
const partialHistory = {
  id: "engine-active", prompt: "在？", done: true, ts: 10_000, doneTs: 11_000,
  forkPointId: "codex-turn-a",
  blocks: [] as Array<{ kind: "text"; message_id: string; text: string; done: boolean }>,
};
const liveActive = {
  id: "client-active", prompt: "在？", done: false, ts: 10_100,
  blocks: [{ kind: "text" as const, message_id: "live-active-answer",
    text: "", done: false }],
};
const activeMerge = mergeInitialHistory(
  [partialHistory], [liveActive], { preserveLiveTailOpen: true });
assert.equal(activeMerge.length, 1);
assert.equal(activeMerge[0].done, false);
assert.equal(activeMerge[0].doneTs, undefined);
assert.equal(activeMerge[0].forkPointId, "codex-turn-a");
const liveFinished = [{
  ...activeMerge[0], done: true, doneTs: 12_000,
  blocks: activeMerge[0].blocks.map((block) => block.kind === "text"
    ? { ...block, text: "only once", done: true } : block),
}];
const completeHistory = [{
  ...partialHistory, doneTs: 12_000,
  blocks: [{ kind: "text" as const, message_id: "engine-active-answer",
    text: "only once", done: true }],
}];
const activeReconciled = mergeInitialHistory(completeHistory, liveFinished);
assert.equal(activeReconciled.length, 1);
assert.deepEqual(activeReconciled[0].blocks.map((block) => block.kind === "text"
  ? block.text : "tool"), ["only once"]);

// A Codex goal continuation has no user prompt. Live and rollout use different
// display ids, but their authoritative app-server turn id must dedupe them.
const assistantOnlyMerged = mergeInitialHistory(
  [{ id: "history-agent-message", forkPointId: "goal-turn-1", prompt: "",
    done: true, blocks: [{ kind: "text" as const, message_id: "history-answer",
      text: "goal complete", done: true, channel: "final" as const }] }],
  [{ id: "goal-turn-1", forkPointId: "goal-turn-1", prompt: "",
    done: true, blocks: [{ kind: "text" as const, message_id: "live-answer",
      text: "goal complete", done: true, channel: "final" as const }] }],
);
assert.equal(assistantOnlyMerged.length, 1);
assert.equal(assistantOnlyMerged[0].id, "goal-turn-1");
assert.equal(assistantOnlyMerged[0].blocks.filter((block) => block.kind === "text").length, 1);

// A bounded head refresh merges against rows retained from earlier pages.
// Replayed assistant-only continuations can come from legacy/cache projections
// whose synthetic start time is newer than their authoritative terminal time.
// That impossible timestamp must not append old history after the active tail.
const impossibleReplayTime = {
  id: "old-assistant-only", prompt: "", done: true,
  ts: 20_000, doneTs: 8_000, durationMs: 1_000, blocks: [],
};
const currentHistoryTurn = {
  id: "current-history", prompt: "current", done: true,
  ts: 10_000, doneTs: 11_000, blocks: [],
};
const currentLiveTurn = {
  ...currentHistoryTurn, done: false, doneTs: undefined,
};
assert.deepEqual(mergeInitialHistory(
  [currentHistoryTurn],
  [impossibleReplayTime, currentLiveTurn],
  { preserveLiveTailOpen: true },
).map((turn) => turn.id), [
  "old-assistant-only", "current-history",
]);

// Exercise the real reducer through Vite's zero-network SSR loader. The plain
// Node test output cannot import reducer.js directly because the browser build
// intentionally uses extensionless module specifiers.
const reducerHarness = await createServer({
  root: process.cwd(),
  appType: "custom",
  logLevel: "silent",
  server: { middlewareMode: true, watch: null },
});
try {
  const {
    createRuntime, initialState, reduce, MAX_TURN_BLOCKS, MAX_TURN_BLOCK_CHARS,
    OMITTED_PROCESS_ITEM_ID,
  } = await reducerHarness.ssrLoadModule("/src/reducer.ts");
  const event = (body: Record<string, unknown>): ServerEvent => ({
    v: 19, ts: 10, ...body,
  } as ServerEvent);
  const problemSid = "safe-problem-presentation";
  let problemState = reduce({
    ...initialState, focusedSid: problemSid,
    runtimes: { [problemSid]: createRuntime() },
  }, {
    type: "query_sent", sid: problemSid, msg_id: "problem-turn",
    prompt: "hello", ts: 1,
  });
  problemState = reduce(problemState, { type: "event", event: event({
    type: "error", sid: problemSid, msg_id: "problem-turn",
    code: "cc_crash",
    message: "provider crash at /private/token; see wrapper logs",
  }) });
  assert.equal(problemState.runtimes[problemSid].turns[0].error,
    "本次回复未完成，请重试。");
  assert.doesNotMatch(problemState.runtimes[problemSid].turns[0].error ?? "",
    /cc_crash|provider|private|wrapper/i);
  const commandProblem = reduce(problemState, { type: "event", event: event({
    type: "error", sid: problemSid, code: "internal",
    message: "Traceback: secret path /private/token",
  }) });
  assert.equal(commandProblem.banner, "操作未完成，请稍后重试。");
  // Work/Code and engine switches restore the target surface's last accepted
  // list immediately. The authoritative refresh may take ~1s for Codex because
  // app-server is started on demand, but it must not blank the sidebar meanwhile.
  const cachedSessions = [{
    session_id: "cached-work-session",
    engine: "codex" as const,
    space: "work" as const,
    summary: "cached",
  }];
  const restored = reduce(
    { ...initialState, focusedSid: "old-code-session" },
    { type: "restore_session_list", sessions: cachedSessions },
  );
  assert.deepEqual(restored.sessions, cachedSessions);
  assert.equal(restored.focusedSid, null);
  const refocused = reduce(reduce(reduce(restored,
    { type: "enter_new_chat", cwd: "~" }),
  { type: "focus_session", sid: "cached-work-session" }),
  { type: "exit_new_chat" });
  assert.equal(refocused.focusedSid, "cached-work-session");
  assert.equal(refocused.newChat, null,
    "restoring a surface focus must also leave the temporary new-work page");
  const staleFocused = reduce({
    ...refocused,
    sessions: [{ session_id: "deleted-elsewhere", engine: "codex", space: "work" }],
    focusedSid: "deleted-elsewhere",
  }, { type: "event", event: event({
    type: "session_list", engine: "codex", space: "work", sessions: cachedSessions,
  }) });
  assert.equal(staleFocused.focusedSid, null,
    "an authoritative list must clear a session deleted by another client");
  assert.ok(staleFocused.newChat,
    "the removed transcript must not remain painted while a replacement focus is chosen");
  const ownerA = {
    scopeKey: "machine-a:code:claude", machineId: "machine-a",
    engine: "claude" as const, space: "code" as const,
    surfaceEpoch: 1, connectionGeneration: 1,
  };
  const ownerB = {
    scopeKey: "machine-b:code:codex", machineId: "machine-b",
    engine: "codex" as const, space: "code" as const,
    surfaceEpoch: 4, connectionGeneration: 2,
  };
  const scopedA = reduce(initialState, { type: "event", ownership: ownerA,
    event: event({ type: "session_focus", session_id: "a", cwd: "/work/a" }) });
  const scopedB = reduce(scopedA, { type: "event", ownership: ownerB,
    event: event({ type: "session_focus", session_id: "b", cwd: "/work/b" }) });
  assert.deepEqual(scopedB.cwdByScope, {
    [ownerA.scopeKey]: "/work/a", [ownerB.scopeKey]: "/work/b",
  });
  const unknownOwner = reduce(scopedB, { type: "event",
    event: event({ type: "session_focus", session_id: "unknown", cwd: "/deleted" }) });
  assert.deepEqual(unknownOwner.cwdByScope, scopedB.cwdByScope,
    "an unowned focus must never update any scoped cwd");
  const backgroundRekey = reduce(scopedB, { type: "event", ownership: ownerA,
    event: event({
      type: "session_rekey", old_key: "tmp-background",
      session_id: "real-background", cwd: "/work/a/new",
    }) });
  assert.equal(backgroundRekey.cwdByScope[ownerA.scopeKey], "/work/a/new");
  assert.equal(backgroundRekey.cwdByScope[ownerB.scopeKey], "/work/b",
    "a valid background rekey updates only its own scope");
  const clearedInherited = reduce(backgroundRekey, {
    type: "clear_scope_cwd", scopeKey: ownerA.scopeKey,
  });
  assert.equal(ownerA.scopeKey in clearedInherited.cwdByScope, false);

  const createdPlaceholder = reduce(initialState, {
    type: "event", ownership: ownerB,
    event: event({
      type: "session_focus", session_id: "tmp-created", cwd: "/work/new",
      request_id: "create-message",
    }),
  });
  assert.deepEqual(createdPlaceholder.sessions.map((session: { session_id: string }) =>
    session.session_id), ["tmp-created"],
  "a correlated create focus must paint a temporary sidebar row immediately");
  assert.equal(createdPlaceholder.sessions[0].engine, "codex");
  assert.equal(createdPlaceholder.sessions[0].space, "code");
  const rekeyedPlaceholder = reduce(createdPlaceholder, {
    type: "event", ownership: ownerB,
    event: event({
      type: "session_rekey", old_key: "tmp-created", session_id: "real-created",
      cwd: "/work/new",
    }),
  });
  assert.deepEqual(rekeyedPlaceholder.sessions.map((session: { session_id: string }) =>
    session.session_id), ["real-created"],
  "rekey must atomically migrate the temporary sidebar row");
  const racedCatalog = reduce(rekeyedPlaceholder, {
    type: "event", ownership: ownerB,
    event: event({type: "session_list", engine: "codex", space: "code", sessions: []}),
  });
  assert.equal(racedCatalog.focusedSid, "real-created",
    "a delayed catalog cannot steal focus from a newly created session");
  assert.equal(racedCatalog.sessions[0].session_id, "real-created");
  const authoritativeCreated = reduce(racedCatalog, {
    type: "event", ownership: ownerB,
    event: event({
      type: "session_list", engine: "codex", space: "code", sessions: [{
        session_id: "real-created", engine: "codex", space: "code",
        summary: "authoritative title",
      }],
    }),
  });
  assert.equal(authoritativeCreated.sessions[0].summary, "authoritative title");
  assert.deepEqual(authoritativeCreated.pendingCatalogSids, []);
  const removedAfterCatalog = reduce(authoritativeCreated, {
    type: "event", ownership: ownerB,
    event: event({type: "session_list", engine: "codex", space: "code", sessions: []}),
  });
  assert.equal(removedAfterCatalog.focusedSid, null,
    "once catalogued, authoritative removal must still work");

  const acceptanceSid = "query-acceptance";
  const acceptanceOtherSid = "query-acceptance-other";
  let acceptanceState = {
    ...initialState,
    focusedSid: acceptanceSid,
    runtimes: {
      [acceptanceSid]: { ...createRuntime(), syncReady: true },
      [acceptanceOtherSid]: { ...createRuntime(), syncReady: true },
    },
  };
  acceptanceState = reduce(acceptanceState, {
    type: "query_sent", sid: acceptanceSid, prompt: "first",
    msg_id: "acceptance-first", ts: 900,
  });
  assert.equal(
    acceptanceState.runtimes[acceptanceSid].acceptancePending,
    "acceptance-first",
  );
  acceptanceState = reduce(acceptanceState, {
    type: "query_sent", sid: acceptanceSid, prompt: "must be deferred",
    msg_id: "acceptance-second", ts: 901,
  });
  assert.deepEqual(
    acceptanceState.runtimes[acceptanceSid].turns.map(
      (turn: { id: string }) => turn.id),
    ["acceptance-first"],
    "a second idle submit cannot create another optimistic turn before acceptance",
  );
  acceptanceState = reduce(acceptanceState, {
    type: "set_pending", sid: acceptanceSid,
    query: { prompt: "must be deferred" },
  });
  acceptanceState = reduce(acceptanceState, {
    type: "set_pending", sid: acceptanceOtherSid,
    query: { prompt: "other session" },
  });
  assert.equal(
    acceptanceState.runtimes[acceptanceSid].pendingSend?.prompt,
    "must be deferred",
  );
  assert.equal(
    acceptanceState.runtimes[acceptanceOtherSid].pendingSend?.prompt,
    "other session",
    "deferred queries are targeted by sid rather than current focus",
  );
  acceptanceState = reduce(acceptanceState, { type: "event", event: event({
    type: "command_ack", client_id: "browser", cmd_id: "acceptance-command",
  }) });
  assert.equal(
    acceptanceState.runtimes[acceptanceSid].acceptancePending,
    "acceptance-first",
    "command ACK must not unlock a direct query",
  );
  acceptanceState = reduce(acceptanceState, { type: "event", event: event({
    type: "error", sid: acceptanceOtherSid, msg_id: "unrelated",
    code: "busy", message: "other",
  }) });
  assert.equal(
    acceptanceState.runtimes[acceptanceSid].acceptancePending,
    "acceptance-first",
    "another session's error cannot release this query",
  );
  acceptanceState = reduce(acceptanceState, { type: "event", event: event({
    type: "error", sid: acceptanceSid, msg_id: "acceptance-first",
    code: "busy", message: "rejected",
  }) });
  assert.equal(
    acceptanceState.runtimes[acceptanceSid].acceptancePending,
    null,
    "the exact correlated Error releases query acceptance",
  );
  acceptanceState = reduce(acceptanceState, {
    type: "query_sent", sid: acceptanceSid, prompt: "offline",
    msg_id: "acceptance-offline", ts: 902,
  });
  acceptanceState = reduce(acceptanceState, { type: "event", event: event({
    type: "error", sid: acceptanceSid, msg_id: "acceptance-offline",
    code: "wrapper_offline", message: "offline",
  }) });
  assert.equal(
    acceptanceState.runtimes[acceptanceSid].acceptancePending,
    "acceptance-offline",
    "offline/reconnect keeps the unaccepted query latched for outbox replay",
  );
  acceptanceState = reduce(acceptanceState, { type: "event", event: event({
    type: "user_msg", sid: acceptanceSid, msg_id: "acceptance-offline",
    prompt: "offline",
  }) });
  assert.equal(
    acceptanceState.runtimes[acceptanceSid].acceptancePending,
    null,
    "the authoritative user echo releases acceptance after reconnect",
  );

  // A materialized History uses transcript/native ids, not the browser msg_id.
  // Freeze the pre-send authoritative head so a genuinely appended newest turn
  // can recover a live UserMsg/TurnBinding which fell outside replay. Repeated
  // old prompts and a different newly-appended prompt are not acceptance proof.
  const nativeAcceptanceSid = "native-history-acceptance";
  const nativeOldTurn = {
    id: "native-old", prompt: "继续", blocks: [], done: true,
    ts: 1_000, doneTs: 1_100, detailEventCount: 0, detailLoaded: false,
  };
  let nativeAcceptanceState = reduce({
    ...initialState,
    focusedSid: nativeAcceptanceSid,
    runtimes: { [nativeAcceptanceSid]: createRuntime() },
  }, { type: "event", event: event({
    type: "history", sid: nativeAcceptanceSid,
    session_id: nativeAcceptanceSid, revision: "native-r1",
    generation: "native-g1", build_seq: 1, live_seq: 10,
    detail: "summary", events: [], turns: [nativeOldTurn],
    has_more: false, newest_id: "native-old", oldest_id: "native-old",
  }) });
  nativeAcceptanceState = reduce(nativeAcceptanceState, {
    type: "query_sent", sid: nativeAcceptanceSid, prompt: "继续",
    msg_id: "browser-native-pending", ts: 900_000,
    images: [{ media_type: "image/png", data: "optimistic-image" }],
  });
  nativeAcceptanceState = reduce(nativeAcceptanceState, {
    type: "event", event: event({
      type: "history", sid: nativeAcceptanceSid,
      session_id: nativeAcceptanceSid, revision: "native-r1",
      generation: "native-g1", build_seq: 1, live_seq: 10,
      detail: "summary", events: [], turns: [nativeOldTurn],
      has_more: false, newest_id: "native-old", oldest_id: "native-old",
    }),
  });
  assert.equal(
    nativeAcceptanceState.runtimes[nativeAcceptanceSid].acceptancePending,
    "browser-native-pending",
    "an old materialized row with the same prompt cannot release acceptance",
  );
  const unrelatedNativeTurn = {
    id: "native-unrelated", prompt: "another writer", blocks: [], done: true,
    ts: 2_000, doneTs: 2_100, detailEventCount: 0, detailLoaded: false,
  };
  nativeAcceptanceState = reduce(nativeAcceptanceState, {
    type: "event", event: event({
      type: "history", sid: nativeAcceptanceSid,
      session_id: nativeAcceptanceSid, revision: "native-r1",
      generation: "native-g1", build_seq: 2, live_seq: 11,
      detail: "summary", events: [],
      turns: [nativeOldTurn, unrelatedNativeTurn],
      has_more: false, newest_id: "native-unrelated", oldest_id: "native-old",
    }),
  });
  assert.equal(
    nativeAcceptanceState.runtimes[nativeAcceptanceSid].acceptancePending,
    "browser-native-pending",
    "a different newly-materialized prompt cannot release acceptance",
  );
  const acceptedNativeTurn = {
    id: "native-accepted", prompt: "继续", blocks: [], done: true,
    imageRefs: [{
      image_id: "native-accepted-image", media_type: "image/png" as const,
      width: 1200, height: 800, byte_size: 1024,
    }],
    // Deliberately far from the browser clock: the missing live echo never
    // replaced the optimistic timestamp with authoritative server time.
    ts: 3_000, doneTs: 3_100, detailEventCount: 0, detailLoaded: false,
  };
  nativeAcceptanceState = reduce(nativeAcceptanceState, {
    type: "event", event: event({
      type: "history", sid: nativeAcceptanceSid,
      session_id: nativeAcceptanceSid, revision: "native-r1",
      generation: "native-g1", build_seq: 3, live_seq: 12,
      detail: "summary", events: [],
      turns: [nativeOldTurn, unrelatedNativeTurn, acceptedNativeTurn],
      has_more: false, newest_id: "native-accepted", oldest_id: "native-old",
    }),
  });
  assert.equal(
    nativeAcceptanceState.runtimes[nativeAcceptanceSid].acceptancePending,
    null,
    "a matching appended native History head releases the browser acceptance",
  );
  assert.deepEqual(
    nativeAcceptanceState.runtimes[nativeAcceptanceSid].turns.map(
      (turn: { id: string }) => turn.id),
    ["native-old", "native-unrelated", "browser-native-pending"],
    "native History binds into the optimistic row even when client/server clocks differ",
  );
  const acceptedImageTurn =
    nativeAcceptanceState.runtimes[nativeAcceptanceSid].turns.at(-1)!;
  assert.equal(acceptedImageTurn.historyTurnId, "native-accepted",
    "canonical image requests must retain the native transcript turn id");
  assert.equal(acceptedImageTurn.images, undefined,
    "the accepted history image reference replaces its optimistic inline body");
  assert.equal(
    acceptedImageTurn.imageRefs?.[0]?.image_id,
    "native-accepted-image",
  );

  const exactEndSid = "exact-turn-end-owner";
  let exactEndState = {
    ...acceptanceState,
    focusedSid: exactEndSid,
    runtimes: {
      ...acceptanceState.runtimes,
      [exactEndSid]: {
        ...createRuntime(),
        acceptancePending: "promptless-tail",
        turns: [{
          id: "older-browser", forkPointId: "older-native",
          prompt: "older", blocks: [], done: false,
        }, {
          id: "promptless-tail", prompt: "", blocks: [], done: false,
        }],
      },
    },
  };
  exactEndState = reduce(exactEndState, { type: "event", event: event({
    type: "turn_end", sid: exactEndSid, turn_id: "older-native",
    result: { subtype: "success", duration_ms: 10, is_error: false },
  }) });
  assert.equal(exactEndState.runtimes[exactEndSid].turns[0].done, true);
  assert.equal(exactEndState.runtimes[exactEndSid].turns[1].done, false,
    "a delayed TurnEnd closes its exact owner, not the promptless live tail");
  assert.equal(
    exactEndState.runtimes[exactEndSid].acceptancePending,
    "promptless-tail",
    "a delayed older TurnEnd cannot unlock a newer query acceptance latch",
  );
  exactEndState = reduce(exactEndState, { type: "event", event: event({
    type: "turn_end", sid: exactEndSid, turn_id: "new-native",
    result: { subtype: "success", duration_ms: 11, is_error: false },
  }) });
  assert.equal(exactEndState.runtimes[exactEndSid].turns[1].done, true,
    "an unbound promptless tail retains the safe legacy TurnEnd fallback");
  assert.equal(
    exactEndState.runtimes[exactEndSid].turns[1].forkPointId,
    "new-native",
  );
  assert.equal(exactEndState.runtimes[exactEndSid].acceptancePending, null);

  const duplicateSid = "duplicate-first-message";
  let duplicateState = reduce({
    ...initialState, focusedSid: duplicateSid,
    runtimes: { [duplicateSid]: createRuntime() },
  }, {
    type: "query_sent", sid: duplicateSid, prompt: "在？测试测试",
    msg_id: "browser-message", ts: 1_000,
  });
  duplicateState = reduce(duplicateState, { type: "event", event: event({
    type: "state", sid: duplicateSid, state: "running",
  }) });
  duplicateState = reduce(duplicateState, { type: "event", event: event({
    type: "user_msg", sid: duplicateSid, msg_id: "browser-message",
    prompt: "在？测试测试", ts: 1,
  }) });
  duplicateState = reduce(duplicateState, { type: "event", event: event({
    type: "history", session_id: duplicateSid, revision: "r1",
    generation: "g1", build_seq: 1, live_seq: 0, has_more: false,
    in_progress: true, events: [
      event({ type: "user_msg", sid: duplicateSid, msg_id: "native-user",
        prompt: "在？测试测试", ts: 10 }),
      event({ type: "assistant_msg_start", sid: duplicateSid,
        message_id: "native-answer", channel: "final", ts: 11 }),
      event({ type: "delta", sid: duplicateSid, message_id: "native-answer",
        channel: "final", text: "收到", ts: 11 }),
      event({ type: "assistant_msg_end", sid: duplicateSid,
        message_id: "native-answer", channel: "final", ts: 11 }),
      event({ type: "turn_end", sid: duplicateSid, turn_id: "native-turn",
        result: { subtype: "success", duration_ms: 1, is_error: false }, ts: 11 }),
    ],
  }) });
  assert.equal(duplicateState.runtimes[duplicateSid].turns.length, 2,
    "timestamps outside the narrow legacy heuristic intentionally remain separate");
  duplicateState = reduce(duplicateState, { type: "event", event: event({
    type: "turn_binding", sid: duplicateSid,
    msg_id: "browser-message", turn_id: "native-turn",
  }) });
  assert.equal(duplicateState.runtimes[duplicateSid].turns.length, 1,
    "authoritative turn binding collapses history/live regardless of arrival order");
  assert.equal(duplicateState.runtimes[duplicateSid].turns[0].id, "browser-message");
  assert.equal(duplicateState.runtimes[duplicateSid].turns[0].forkPointId,
    "native-turn");
  assert.equal(duplicateState.runtimes[duplicateSid].turns[0].done, true);

  const summarySid = "materialized-summary";
  const summaryState = reduce({
    ...initialState, focusedSid: summarySid,
    runtimes: { [summarySid]: createRuntime() },
  }, { type: "event", event: event({
    type: "history", session_id: summarySid, revision: "summary-r1",
    generation: "summary-g1", build_seq: 1, live_seq: 0,
    detail: "summary", has_more: true, oldest_id: "summary-message",
    events: [event({ type: "model", sid: summarySid, model: "gpt-summary" })],
    turns: [{
      id: "summary-message", prompt: "inspect", done: true,
      blocks: [{ kind: "text", message_id: "summary-final",
        text: "finished", done: true, channel: "final" }],
      detailEventCount: 74, detailLoaded: false,
    }],
  }) });
  assert.equal(summaryState.runtimes[summarySid].turns.length, 1);
  assert.equal(summaryState.runtimes[summarySid].ccSessionId, summarySid,
    "authoritative history must become cacheable before a slow resume snapshot");
  assert.equal(summaryState.runtimes[summarySid].turns[0].blocks[0].kind, "text");
  assert.equal(summaryState.runtimes[summarySid].turns[0].detailEventCount, 74);
  assert.equal(summaryState.runtimes[summarySid].model, "gpt-summary");
  assert.equal(summaryState.runtimes[summarySid].hasMore, true);

  let detailState = reduce(summaryState, {
    type: "turn_detail_requested", sid: summarySid,
    turnId: "summary-message",
  });
  assert.equal(detailState.runtimes[summarySid].turns[0].detailLoading, true);
  detailState = reduce(detailState, { type: "event", event: event({
    type: "turn_detail", session_id: summarySid, turn_id: "summary-message",
    revision: "summary-r1", events: [
      event({ type: "user_msg", sid: summarySid, msg_id: "summary-message",
        prompt: "inspect" }),
      event({ type: "assistant_msg_start", sid: summarySid,
        message_id: "summary-commentary", channel: "commentary" }),
      event({ type: "delta", sid: summarySid,
        message_id: "summary-commentary", channel: "commentary",
        text: "checking" }),
      event({ type: "tool_use", sid: summarySid,
        message_id: "summary-tool-message", tool_use_id: "summary-tool",
        tool: "Read", input: { file_path: "/tmp/example" } }),
      event({ type: "tool_result", sid: summarySid,
        tool_use_id: "summary-tool", content: "ok", is_error: false }),
      event({ type: "assistant_msg_start", sid: summarySid,
        message_id: "summary-final", channel: "final" }),
      event({ type: "delta", sid: summarySid,
        message_id: "summary-final", channel: "final", text: "finished" }),
      event({ type: "assistant_msg_end", sid: summarySid,
        message_id: "summary-final", channel: "final" }),
      event({ type: "turn_end", sid: summarySid, turn_id: "summary-turn",
        result: { subtype: "success", duration_ms: 1, is_error: false } }),
    ],
  }) });
  assert.equal(detailState.runtimes[summarySid].turns.length, 1);
  assert.equal(detailState.runtimes[summarySid].turns[0].detailLoaded, true);
  assert.equal(detailState.runtimes[summarySid].turns[0].detailLoading, false);
  assert.ok(detailState.runtimes[summarySid].turns[0].blocks.some(
    (block: Block) => block.kind === "tool"));
  assert.equal(detailState.runtimes[summarySid].turns[0].blocks.filter(
    (block: Block) => block.kind === "text" && block.channel === "final").length, 1,
  "detail replaces the summary projection instead of duplicating its final text");

  const staleDetail = reduce(detailState, { type: "event", event: event({
    type: "turn_detail", session_id: summarySid, turn_id: "summary-message",
    revision: "stale-revision", events: [],
  }) });
  assert.equal(staleDetail, detailState,
    "a detail response from another history revision cannot rewrite the turn");

  const refreshedSummary = reduce(detailState, { type: "event", event: event({
    type: "history", session_id: summarySid, revision: "summary-r1",
    generation: "summary-g1", build_seq: 2, live_seq: 0,
    detail: "summary", has_more: true, oldest_id: "summary-message",
    events: [], turns: [{
      id: "summary-message", prompt: "inspect", done: true,
      blocks: [{ kind: "text", message_id: "summary-final",
        text: "finished", done: true, channel: "final" }],
      detailEventCount: 74, detailLoaded: false,
    }],
  }) });
  assert.equal(refreshedSummary.runtimes[summarySid].turns[0].detailLoaded, true);
  assert.ok(refreshedSummary.runtimes[summarySid].turns[0].blocks.some(
    (block: Block) => block.kind === "tool"),
  "a same-revision head refresh must not collapse detail the user opened");

  const backgroundRunning = reduce({
    ...initialState,
    sessions: [{
      session_id: "private-app-session",
      engine: "codex",
      space: "code",
      summary: "private app",
      state: "idle",
    }],
  }, { type: "event", event: event({
    type: "session_activity",
    engine: "codex",
    session_id: "private-app-session",
    state: "running",
  }) });
  assert.equal(backgroundRunning.sessions[0].state, "running",
    "a private Codex App turn must light the non-focused sidebar row");
  const backgroundIdle = reduce(backgroundRunning, {
    type: "event", event: event({
      type: "session_activity",
      engine: "codex",
      session_id: "private-app-session",
      state: "idle",
    }),
  });
  assert.equal(backgroundIdle.sessions[0].state, "idle",
    "the sidebar row must clear when the private App turn completes");

  const {
    buildCalendarDays, parseLocalDateTime, placeDateTimePopover, toLocalDateTime,
  } =
    await reducerHarness.ssrLoadModule("/src/date-time.ts");
  const { DateTimePicker } =
    await reducerHarness.ssrLoadModule("/src/components/DateTimePicker.tsx");
  const { CommandSheet: ModelCommandSheet } =
    await reducerHarness.ssrLoadModule("/src/components/CommandSheet.tsx");
  const sampleDate = new Date(2026, 6, 17, 9, 5);
  assert.equal(toLocalDateTime(sampleDate), "2026-07-17T09:05");
  assert.equal(parseLocalDateTime("2026-02-30T09:05"), null,
    "invalid local calendar dates must fail closed");
  const calendarDays = buildCalendarDays(new Date(2026, 6, 1), sampleDate);
  assert.equal(calendarDays.length, 42);
  assert.equal(toLocalDateTime(calendarDays[0].date).slice(0, 10), "2026-06-29",
    "the custom calendar must use a stable Monday-first six-week grid");
  assert.deepEqual(placeDateTimePopover(
    { left: 275, top: 234, bottom: 284 }, { width: 1280, height: 720 }),
  { left: 275, top: 232 },
  "a short viewport must clamp the complete popover inside the visible area");
  assert.deepEqual(placeDateTimePopover(
    { left: 548, top: 600, bottom: 650 }, { width: 1920, height: 1200 }),
  { left: 548, top: 658 },
  "a tall viewport should keep the popover directly below its trigger");
  const datePickerMarkup = renderToStaticMarkup(createElement(DateTimePicker, {
    value: "2026-07-17T09:05", onChange: () => undefined,
  }));
  assert.match(datePickerMarkup, /执行时间/);
  assert.match(datePickerMarkup, /aria-haspopup="dialog"/);
  const claudeModelsMarkup = renderToStaticMarkup(createElement(ModelCommandSheet, {
    open: true, kind: "models", engine: "claude", onClose: () => undefined,
  }));
  assert.doesNotMatch(claudeModelsMarkup, /自定义 \/ Provider 模型 ID|custom-claude-model/,
    "Claude's ordinary model sheet must not expose raw provider ids");
  const codexModelsMarkup = renderToStaticMarkup(createElement(ModelCommandSheet, {
    open: true, kind: "models", engine: "codex", onClose: () => undefined,
  }));
  assert.doesNotMatch(codexModelsMarkup, /自定义 \/ Provider 模型 ID/,
    "Codex must keep using its authoritative app-server catalog");

  const snapshotOnly = reduce(initialState, { type: "event", event: event({
    type: "snapshot", sid: "background-code", cc_session_id: "background-code",
    state: "idle", tail_text: "",
  }) });
  assert.equal(snapshotOnly.focusedSid, null,
    "a background snapshot must not focus across Code/Work surfaces");
  const unannounced = createRuntime();
  assert.equal(unannounced.model, "");
  assert.equal(unannounced.effort, "");
  assert.equal(unannounced.perm, "");
  assert.equal(unannounced.fast, null);
  const sid = "race-a";
  const otherSid = "race-b";
  let controlRequestState = {
    ...initialState,
    focusedSid: sid,
    runtimes: { [sid]: createRuntime() },
  };
  controlRequestState = reduce(controlRequestState, {
    type: "begin_context_request", sid, requestId: "context-request",
  });
  controlRequestState = reduce(controlRequestState, { type: "event", event: event({
    type: "error", sid, code: "not_running", message: "会话已离线",
    request_id: "context-request",
  }) });
  assert.equal(controlRequestState.runtimes[sid].contextRequestId, null);
  assert.equal(controlRequestState.runtimes[sid].contextError,
    "当前会话暂时不可用，请重新进入后重试。",
    "a targeted context failure must replace the infinite loading state");
  controlRequestState = reduce(controlRequestState, {
    type: "begin_status_request", sid, requestId: "status-request",
  });
  controlRequestState = reduce(controlRequestState, { type: "event", event: event({
    type: "error", sid, code: "not_running", message: "状态不可用",
    request_id: "status-request",
  }) });
  assert.equal(controlRequestState.runtimes[sid].statusRequestId, null);
  assert.equal(controlRequestState.runtimes[sid].statusError,
    "当前会话暂时不可用，请重新进入后重试。",
    "a targeted status failure must close its loading state");
  let diffRequestState = reduce(controlRequestState, {
    type: "open_artifact_loading", sid, file: "src/app.ts", requestId: "diff-request",
  });
  diffRequestState = reduce(diffRequestState, { type: "event", event: event({
    type: "diff_report", sid, file: "src/app.ts", diff: "stale",
    request_id: "older-diff-request",
  }) });
  assert.equal(diffRequestState.artifact?.loading, true,
    "a stale diff response must not replace the active request");
  diffRequestState = reduce(diffRequestState, { type: "event", event: event({
    type: "error", sid, code: "not_running", message: "差异不可用",
    request_id: "diff-request",
  }) });
  assert.equal(diffRequestState.artifact?.loading, false);
  assert.equal(diffRequestState.artifact?.error,
    "当前会话暂时不可用，请重新进入后重试。",
    "a correlated diff failure must close its loading state");
  const untouched = {
    id: "b-turn", prompt: "other", done: true, blocks: [], ts: 1000,
  };
  let state = {
    ...initialState,
    connState: "connected",
    wrapperOnline: true,
    focusedSid: sid,
    newChat: { cwd: "~", model: null, effort: null },
    runtimes: {
      [sid]: { ...createRuntime(), state: "running", syncReady: true },
      [otherSid]: { ...createRuntime(), turns: [untouched], syncReady: true },
    },
  };
  const historySid = "background-history-invalidated";
  const staleTurns = [
    { id: "kept-before-rewind", prompt: "保留", blocks: [], done: true },
    { id: "removed-by-rewind", prompt: "应删除", blocks: [], done: true },
  ];
  state = {
    ...state,
    artifact: { file: "changed.ts", kind: "file", sid: historySid },
    runtimes: {
      ...state.runtimes,
      [historySid]: {
        ...createRuntime(),
        turns: staleTurns,
        pendingQuestion: {
          ask_id: "old-question", question: "旧问题", options: [],
        },
      },
    },
  };
  state = reduce(state, { type: "event", event: event({
    type: "history_invalidated", sid: historySid,
    session_id: historySid, revision: "history-rev-2", reason: "rollback",
  }) });
  assert.deepEqual(state.runtimes[historySid].turns, []);
  assert.equal(state.runtimes[historySid].pendingQuestion, null);
  assert.equal(state.runtimes[historySid].historyInvalidated, true);
  assert.equal(state.runtimes[historySid].loading, true);
  assert.equal(state.artifact, null);

  state = {
    ...state,
    artifact: { file: "stale-after-file-rollback.ts", kind: "file", sid: historySid },
  };
  state = reduce(state, { type: "event", event: event({
    type: "artifact_invalidated", sid: historySid,
    session_id: historySid, reason: "rollback",
  }) });
  assert.equal(state.artifact, null,
    "a replayed file rollback marker must close stale previews");

  state = {
    ...state,
    artifact: { file: "stale-files-only-diff.ts", kind: "gitdiff", sid: historySid,
      sections: [] },
  };
  state = reduce(state, { type: "event", event: event({
    type: "rollback_result", sid: historySid, session_id: historySid,
    engine: "codex", restore: "files", conversation: "skipped",
    files: "succeeded", restored_turns: 1, conflicts: [],
  }) });
  assert.equal(state.artifact, null,
    "a files-only rollback result must close stale diff and preview state");

  state = {
    ...state,
    artifact: { file: "other-session.ts", kind: "file", sid: otherSid },
  };
  state = reduce(state, { type: "event", event: event({
    type: "rollback_result", sid: historySid, session_id: historySid,
    engine: "codex", restore: "files", conversation: "skipped",
    files: "succeeded", restored_turns: 1, conflicts: [],
  }) });
  assert.equal(state.artifact?.sid, otherSid,
    "a background file rollback must not close another session's preview");

  state = reduce(state, {
    type: "hydrate_cache", sid: historySid, turns: staleTurns,
    revision: "history-rev-1",
  });
  assert.deepEqual(state.runtimes[historySid].turns, [],
    "a marker must reject a late IndexedDB hydrate");
  assert.equal(state.runtimes[historySid].loading, true);

  state = reduce(state, { type: "event", event: event({
    type: "history", sid: historySid, session_id: historySid,
    revision: "history-rev-1", before: "old-page", has_more: false,
    events: [event({
      type: "user_msg", sid: historySid, msg_id: "removed-by-rewind",
      prompt: "应删除",
    })],
  }) });
  assert.deepEqual(state.runtimes[historySid].turns, [],
    "a late pagination page cannot satisfy a destructive marker");
  assert.equal(state.runtimes[historySid].historyInvalidated, true);

  state = reduce(state, { type: "event", event: event({
    type: "history", sid: historySid, session_id: historySid,
    revision: "history-rev-1", has_more: false,
    events: [event({
      type: "user_msg", sid: historySid, msg_id: "removed-by-rewind",
      prompt: "应删除",
    })],
  }) });
  assert.deepEqual(state.runtimes[historySid].turns, [],
    "a late first page from the old revision cannot satisfy the marker");
  assert.equal(state.runtimes[historySid].pendingHistoryRevision, "history-rev-2");

  state = reduce(state, { type: "event", event: event({
    type: "history", sid: historySid, session_id: historySid,
    revision: "history-rev-2", has_more: false,
    events: [
      event({ type: "user_msg", sid: historySid,
        msg_id: "kept-before-rewind", prompt: "保留" }),
      event({ type: "assistant_msg_start", sid: historySid,
        message_id: "kept-answer" }),
      event({ type: "delta", sid: historySid,
        message_id: "kept-answer", text: "已保留" }),
      event({ type: "assistant_msg_end", sid: historySid,
        message_id: "kept-answer" }),
      event({ type: "turn_end", sid: historySid,
        result: { subtype: "success", duration_ms: 1, is_error: false } }),
    ],
  }) });
  assert.deepEqual(state.runtimes[historySid].turns.map(
    (turn: { id: string }) => turn.id), ["kept-before-rewind"]);
  assert.equal(state.runtimes[historySid].historyInvalidated, false);
  assert.equal(state.runtimes[historySid].loading, false);
  assert.ok(!state.runtimes[historySid].turns.some(
    (turn: { id: string }) => turn.id === "removed-by-rewind"));

  const prunedHistorySid = "pruned-background-invalidated";
  state = reduce(state, { type: "event", event: event({
    type: "history_invalidated", sid: prunedHistorySid,
    session_id: prunedHistorySid, revision: "pruned-rev-2", reason: "rollback",
  }) });
  assert.equal(state.runtimes[prunedHistorySid].historyInvalidated, true,
    "a background marker must survive until that session is focused");
  state = reduce(state, { type: "focus_session", sid: prunedHistorySid });
  state = reduce(state, { type: "event", event: event({
    type: "session_focus", sid: prunedHistorySid,
    session_id: prunedHistorySid, cwd: "/tmp/project",
  }) });
  assert.equal(state.runtimes[prunedHistorySid].loading, true,
    "session_focus must keep loading until authoritative history arrives");

  // A wrapper restart/non-resident resume has no in-memory rollback marker.
  // The History revision must still replace stale IndexedDB-completed turns,
  // while retaining only this connection's unfinished optimistic tail.
  const restartSid = "restart-stale-idb";
  let restartState = reduce({
    ...initialState, focusedSid: restartSid,
  }, {
    type: "hydrate_cache", sid: restartSid, turns: [
      ...staleTurns,
      { id: "stale-half-turn", prompt: "旧的半截输出", blocks: [], done: false },
    ],
    revision: "boot-old:1",
  });
  restartState = reduce(restartState, {
    type: "query_sent", sid: restartSid, prompt: "正在发送",
    msg_id: "optimistic-live", ts: 20_000,
  });
  restartState = reduce(restartState, { type: "event", event: event({
    type: "history", sid: restartSid, session_id: restartSid,
    revision: "boot-new:1", has_more: false,
    events: [
      event({ type: "user_msg", sid: restartSid,
        msg_id: "kept-before-rewind", prompt: "保留" }),
      event({ type: "turn_end", sid: restartSid,
        result: { subtype: "success", duration_ms: 1, is_error: false } }),
    ],
  }) });
  assert.deepEqual(restartState.runtimes[restartSid].turns.map(
    (turn: { id: string }) => turn.id),
  ["kept-before-rewind", "optimistic-live"]);
  assert.equal(restartState.runtimes[restartSid].historyRevision, "boot-new:1");
  assert.ok(!restartState.runtimes[restartSid].turns.some(
    (turn: { id: string }) => turn.id === "removed-by-rewind"));
  assert.ok(!restartState.runtimes[restartSid].turns.some(
    (turn: { id: string }) => turn.id === "stale-half-turn"),
  "an unfinished IndexedDB row is not a genuine current-connection tail");
  assert.equal(restartState.runtimes[restartSid].turns.at(-1)?.done, false,
    "revision replacement must preserve a genuine live optimistic tail");

  const resumedLiveSid = "restart-resumed-live-tail";
  let resumedLiveState = reduce({
    ...initialState, focusedSid: resumedLiveSid,
  }, {
    type: "hydrate_cache", sid: resumedLiveSid, turns: [{
      id: "resumed-live", prompt: "仍在执行", blocks: [], done: false,
    }], revision: "boot-old:2",
  });
  resumedLiveState = reduce(resumedLiveState, { type: "event", event: event({
    type: "delta", sid: resumedLiveSid,
    message_id: "assistant-current", text: "新的实时输出",
  }) });
  resumedLiveState = reduce(resumedLiveState, { type: "event", event: event({
    type: "history", sid: resumedLiveSid, session_id: resumedLiveSid,
    revision: "boot-new:2", has_more: false, in_progress: true, events: [],
  }) });
  assert.deepEqual(resumedLiveState.runtimes[resumedLiveSid].turns.map(
    (turn: { id: string }) => turn.id), ["resumed-live"],
  "a live continuation validates the matching unfinished cached turn");

  // A reconnect may resume at any lifecycle frame, not only a text delta.
  // Every frame that proves the cached turn is live must remove its hydrated
  // marker and advance the live watermark used by History race protection.
  const assertLiveValidation = (
    name: string, blocks: Record<string, unknown>[], liveEvent: ServerEvent,
  ) => {
    const liveSid = `cached-${name}`;
    let liveState = reduce({ ...initialState, focusedSid: liveSid }, {
      type: "hydrate_cache", sid: liveSid, turns: [{
        id: "cached-live", prompt: "继续执行", blocks, done: false,
      }], revision: "live-rev",
    });
    liveState = reduce(liveState, { type: "event", event: liveEvent });
    assert.deepEqual(liveState.runtimes[liveSid].hydratedCacheTurnIds, [],
      `${name} must validate the hydrated turn`);
    assert.equal(liveState.runtimes[liveSid].lastLiveSeq, liveEvent.seq,
      `${name} must advance the live History watermark`);
  };
  assertLiveValidation("tool-delta", [{
    kind: "tool", message_id: "m", tool_use_id: "tool-1",
    tool: "Bash", input: {}, done: false,
  }], event({
    type: "tool_delta", sid: "cached-tool-delta", seq: 21,
    tool_use_id: "tool-1", stream: "output", delta: "running",
  }));
  assertLiveValidation("tool-result", [{
    kind: "tool", message_id: "m", tool_use_id: "tool-1",
    tool: "Bash", input: {}, done: false,
  }], event({
    type: "tool_result", sid: "cached-tool-result", seq: 22,
    tool_use_id: "tool-1", content: "done", is_error: false,
  }));
  assertLiveValidation("assistant-end", [{
    kind: "text", message_id: "assistant-1", text: "done", done: false,
    channel: "final",
  }], event({
    type: "assistant_msg_end", sid: "cached-assistant-end", seq: 23,
    message_id: "assistant-1", channel: "final",
  }));
  assertLiveValidation("turn-end", [], event({
    type: "turn_end", sid: "cached-turn-end", seq: 24,
    result: { subtype: "success", duration_ms: 1, is_error: false },
  }));
  assertLiveValidation("error", [], event({
    type: "error", sid: "cached-error", seq: 25, msg_id: "cached-live",
    code: "internal", message: "failed",
  }));

  // The History read began before assistant completion. Even with the same
  // revision, its empty snapshot must not delete the now-completed live turn.
  const completedRaceSid = "same-revision-completed-live-race";
  let completedRace = reduce({
    ...initialState, focusedSid: completedRaceSid,
    runtimes: { [completedRaceSid]: createRuntime() },
  }, {
    type: "query_sent", sid: completedRaceSid, prompt: "race",
    msg_id: "live-completed", ts: 30_000,
  });
  completedRace = reduce(completedRace, { type: "event", event: event({
    type: "user_msg", sid: completedRaceSid, seq: 31,
    msg_id: "live-completed", prompt: "race",
  }) });
  completedRace = reduce(completedRace, { type: "event", event: event({
    type: "turn_end", sid: completedRaceSid, seq: 32,
    result: { subtype: "success", duration_ms: 1, is_error: false },
  }) });
  completedRace = reduce(completedRace, { type: "event", event: event({
    type: "history", sid: completedRaceSid, session_id: completedRaceSid,
    revision: "race-rev", build_seq: 1, live_seq: 30,
    has_more: false, events: [],
  }) });
  assert.deepEqual(completedRace.runtimes[completedRaceSid].turns.map(
    (turn: { id: string }) => turn.id), ["live-completed"]);
  assert.equal(completedRace.runtimes[completedRaceSid].turns[0].done, true);

  // A thread/settings notification can arrive while a transcript History read
  // is already in flight.  Keep the newer live model/effort even though the
  // older History page remains useful for narrative turns.
  const settingsRaceSid = "history-cannot-revert-live-settings";
  let settingsRace = {
    ...initialState, focusedSid: settingsRaceSid,
    runtimes: { [settingsRaceSid]: createRuntime() },
  };
  settingsRace = reduce(settingsRace, { type: "event", event: event({
    type: "model", sid: settingsRaceSid, seq: 34, model: "gpt-live",
  }) });
  settingsRace = reduce(settingsRace, { type: "event", event: event({
    type: "effort", sid: settingsRaceSid, seq: 35, effort: "high",
  }) });
  settingsRace = reduce(settingsRace, { type: "event", event: event({
    type: "history", sid: settingsRaceSid, session_id: settingsRaceSid,
    revision: "settings-race-rev", build_seq: 1, live_seq: 33,
    has_more: false, events: [
      event({ type: "model", sid: settingsRaceSid, model: "gpt-stale" }),
      event({ type: "effort", sid: settingsRaceSid, effort: "low" }),
    ],
  }) });
  assert.equal(settingsRace.runtimes[settingsRaceSid].model, "gpt-live");
  assert.equal(settingsRace.runtimes[settingsRaceSid].effort, "high");

  // A current authoritative head page can repair a lost terminal event. This is
  // the exact /review interrupt failure mode: the browser saw interrupting and a
  // running process, but missed TurnEnd. Explicit in_progress=false closes the
  // retained live tail and its child activity instead of spinning forever.
  const idleRepairSid = "authoritative-idle-repairs-interrupt";
  let idleRepair = {
    ...initialState, focusedSid: idleRepairSid,
    runtimes: { [idleRepairSid]: createRuntime() },
  };
  idleRepair = reduce(idleRepair, { type: "event", event: event({
    type: "user_msg", sid: idleRepairSid, seq: 41,
    msg_id: "review-turn", prompt: "",
  }) });
  idleRepair = reduce(idleRepair, { type: "event", event: event({
    type: "process", sid: idleRepairSid, seq: 42,
    item_id: "review-hook", kind: "hook", phase: "start",
    status: "running", title: "Hook",
  }) });
  idleRepair = reduce(idleRepair, { type: "event", event: event({
    type: "state", sid: idleRepairSid, state: "interrupting",
  }) });
  const duplicateInterruptError = reduce(idleRepair, {
    type: "event", event: event({
      type: "error", sid: idleRepairSid, code: "not_running",
      message: "该会话没有正在运行的回合",
    }),
  });
  assert.equal(duplicateInterruptError.runtimes[idleRepairSid].state,
    "interrupting", "not_running alone must not guess that an interrupt is terminal");
  assert.equal(duplicateInterruptError.runtimes[idleRepairSid].turns[0].done, false);
  idleRepair = reduce(idleRepair, { type: "event", event: event({
    type: "history", sid: idleRepairSid, session_id: idleRepairSid,
    revision: "idle-repair-rev", generation: "wrapper-one",
    build_seq: 1, live_seq: 42, authoritative: true,
    in_progress: false, has_more: false, events: [],
  }) });
  const repairedRuntime = idleRepair.runtimes[idleRepairSid];
  assert.equal(repairedRuntime.state, "idle");
  assert.equal(repairedRuntime.turns[0].done, true);
  assert.equal(repairedRuntime.turns[0].interrupted, true);
  const repairedProcess = repairedRuntime.turns[0].blocks[0] as {
    done: boolean; status: string;
  };
  assert.equal(repairedProcess.done, true);
  assert.equal(repairedProcess.status, "interrupted");

  // Explicit in_progress=true remains open even when transcript EOF synthesizes
  // TurnEnd. Undefined keeps the older-wrapper fallback to local runtime state.
  const liveHistorySid = "authoritative-running-keeps-tail";
  let liveHistory = {
    ...initialState, focusedSid: liveHistorySid,
    runtimes: { [liveHistorySid]: createRuntime() },
  };
  liveHistory = reduce(liveHistory, { type: "event", event: event({
    type: "user_msg", sid: liveHistorySid, seq: 51,
    msg_id: "live-review", prompt: "",
  }) });
  liveHistory = reduce(liveHistory, { type: "event", event: event({
    type: "state", sid: liveHistorySid, state: "running",
  }) });
  const liveHistoryFrame = {
    type: "history", sid: liveHistorySid, session_id: liveHistorySid,
    revision: "live-history-rev", generation: "wrapper-one",
    build_seq: 1, live_seq: 51, authoritative: true,
    in_progress: true, has_more: false,
    events: [
      event({ type: "user_msg", sid: liveHistorySid,
        msg_id: "live-review", prompt: "" }),
      event({ type: "turn_end", sid: liveHistorySid,
        result: { subtype: "success", duration_ms: 1, is_error: false } }),
    ],
  };
  liveHistory = reduce(liveHistory, { type: "event", event: event(liveHistoryFrame) });
  assert.equal(liveHistory.runtimes[liveHistorySid].state, "running");
  assert.equal(liveHistory.runtimes[liveHistorySid].turns[0].done, false);

  // A native/external Codex turn is visually active in the sidebar but does
  // not become a wrapper-owned running state. Otherwise the composer exposes a
  // Stop action which cannot interrupt that external writer.
  const mirroredSid = "external-history-activity";
  let mirroredState = {
    ...initialState, focusedSid: mirroredSid,
    runtimes: { [mirroredSid]: createRuntime() },
  };
  mirroredState = reduce(mirroredState, { type: "event", event: event({
    type: "history", sid: mirroredSid, session_id: mirroredSid,
    revision: "external-activity-rev", build_seq: 1,
    external: true, in_progress: true, has_more: false, events: [],
  }) });
  assert.equal(mirroredState.runtimes[mirroredSid].state, "idle");
  assert.equal(mirroredState.runtimes[mirroredSid].mirroredRunning, true);
  mirroredState = reduce(mirroredState, { type: "event", event: event({
    type: "history", sid: mirroredSid, session_id: mirroredSid,
    revision: "external-activity-rev", build_seq: 2,
    external: false, in_progress: false, has_more: false, events: [],
  }) });
  assert.equal(mirroredState.runtimes[mirroredSid].state, "idle");
  assert.equal(mirroredState.runtimes[mirroredSid].mirroredRunning, false);

  // A native Codex task can accept another user message while still running.
  // The next authoritative head must close the old placeholder as `steered`
  // and leave exactly the latest prompt open instead of stacking "thinking".
  const steeredSid = "external-steering-replaces-thinking";
  let steeredState = {
    ...initialState, focusedSid: steeredSid,
    runtimes: { [steeredSid]: createRuntime() },
  };
  steeredState = reduce(steeredState, { type: "event", event: event({
    type: "history", sid: steeredSid, session_id: steeredSid,
    revision: "steered-rev", build_seq: 1,
    external: true, in_progress: true, has_more: true,
    events: [event({ type: "user_msg", sid: steeredSid,
      msg_id: "first-prompt", prompt: "first" })],
  }) });
  steeredState = reduce(steeredState, { type: "event", event: event({
    type: "history", sid: steeredSid, session_id: steeredSid,
    revision: "steered-rev", build_seq: 2,
    external: true, in_progress: true, has_more: true,
    events: [
      event({ type: "user_msg", sid: steeredSid,
        msg_id: "first-prompt", prompt: "first" }),
      event({ type: "turn_end", sid: steeredSid,
        result: { subtype: "steered", duration_ms: 0, is_error: false } }),
      event({ type: "user_msg", sid: steeredSid,
        msg_id: "second-prompt", prompt: "second" }),
    ],
  }) });
  assert.deepEqual(steeredState.runtimes[steeredSid].turns.map(
    (turn: { prompt: string; done: boolean }) => [turn.prompt, turn.done]), [
      ["first", true], ["second", false],
    ]);

  // Expanding detail before a native steer must not pin the old lifecycle.
  // The next summary is authoritative for done/error/timestamps, while its
  // lightweight commentary/compaction blocks merge with already-loaded detail.
  const detailedSteerSid = "external-steering-after-detail";
  let detailedSteerState = {
    ...initialState, focusedSid: detailedSteerSid,
    runtimes: { [detailedSteerSid]: createRuntime() },
  };
  detailedSteerState = reduce(detailedSteerState, {
    type: "event", event: event({
      type: "history", sid: detailedSteerSid,
      session_id: detailedSteerSid, revision: "detail-steer-rev",
      build_seq: 1, detail: "summary", external: true, in_progress: true,
      has_more: true, events: [], turns: [{
        id: "first-prompt", prompt: "first", done: false,
        detailEventCount: 2, detailLoaded: false, blocks: [],
      }],
    }),
  });
  detailedSteerState = reduce(detailedSteerState, {
    type: "event", event: event({
      type: "turn_detail", sid: detailedSteerSid,
      session_id: detailedSteerSid, revision: "detail-steer-rev",
      turn_id: "first-prompt", authoritative: true, events: [
        event({ type: "user_msg", sid: detailedSteerSid,
          msg_id: "first-prompt", prompt: "first" }),
        event({ type: "assistant_msg_start", sid: detailedSteerSid,
          message_id: "commentary-first", channel: "commentary" }),
        event({ type: "delta", sid: detailedSteerSid,
          message_id: "commentary-first", channel: "commentary",
          text: "first progress" }),
        event({ type: "assistant_msg_end", sid: detailedSteerSid,
          message_id: "commentary-first", channel: "commentary" }),
        event({ type: "tool_use", sid: detailedSteerSid,
          message_id: "tool-message", tool_use_id: "tool-open",
          tool: "exec_command", input: {} }),
      ],
    }),
  });
  detailedSteerState = reduce(detailedSteerState, {
    type: "event", event: event({
      type: "history", sid: detailedSteerSid,
      session_id: detailedSteerSid, revision: "detail-steer-rev",
      build_seq: 2, detail: "summary", external: true, in_progress: true,
      has_more: true, events: [], turns: [
        {
          id: "first-prompt", prompt: "first", done: true,
          doneTs: 2_000, durationMs: 0,
          detailEventCount: 2, detailLoaded: false, blocks: [{
            kind: "text", message_id: "commentary-first",
            text: "first progress", done: true, channel: "commentary",
          }],
        },
        {
          id: "second-prompt", prompt: "second", done: false,
          detailEventCount: 2, detailLoaded: false, blocks: [
            {
              kind: "text", message_id: "commentary-second",
              text: "second progress", done: true, channel: "commentary",
            },
            {
              kind: "process", item_id: "compact-second",
              processKind: "compaction", phase: "end",
              status: "succeeded", title: "压缩上下文", done: true,
            },
          ],
        },
      ],
    }),
  });
  const detailedSteerTurns = detailedSteerState.runtimes[
    detailedSteerSid
  ].turns;
  assert.deepEqual(detailedSteerTurns.map(
    (turn: { prompt: string; done: boolean }) => [turn.prompt, turn.done]), [
      ["first", true], ["second", false],
    ]);
  assert.equal(detailedSteerTurns[0].blocks.some(
    (block: { done: boolean }) => !block.done), false);
  assert.equal(detailedSteerTurns[1].blocks.some(
    (block: { kind: string; text?: string }) =>
      block.kind === "text" && block.text === "second progress"), true);
  assert.equal(detailedSteerTurns[1].blocks.some(
    (block: { kind: string; processKind?: string }) =>
      block.kind === "process" && block.processKind === "compaction"), true);

  const fallbackHistory = reduce({
    ...liveHistory,
    runtimes: { [liveHistorySid]: {
      ...liveHistory.runtimes[liveHistorySid], state: "interrupting" as const,
    } },
  }, { type: "event", event: event({
    ...liveHistoryFrame, build_seq: 2, in_progress: undefined,
  }) });
  assert.equal(fallbackHistory.runtimes[liveHistorySid].state, "interrupting");
  assert.equal(fallbackHistory.runtimes[liveHistorySid].turns[0].done, false);

  // A History read started before a newer live frame is not current control
  // state, even when it carries in_progress=false.
  const racedIdle = reduce(liveHistory, { type: "event", event: event({
    ...liveHistoryFrame, build_seq: 2, live_seq: 50, in_progress: false,
  }) });
  assert.equal(racedIdle.runtimes[liveHistorySid].state, "running");
  assert.equal(racedIdle.runtimes[liveHistorySid].turns[0].done, false);

  // A reconnect can receive a lightweight live setting frame before the
  // transcript read completes. That makes the History narrative stale, but an
  // explicit wrapper-owned in_progress=true is still a safe positive running
  // signal. Keeping the cache-hydrated idle state here makes the composer send
  // Query instead of interrupt + replacement, which the busy wrapper rejects.
  const racedRunningSid = "history-running-positive-race";
  let racedRunning = {
    ...initialState, focusedSid: racedRunningSid,
    runtimes: { [racedRunningSid]: createRuntime() },
  };
  racedRunning = reduce(racedRunning, { type: "event", event: event({
    type: "model", sid: racedRunningSid, seq: 81, model: "gpt-5",
  }) });
  racedRunning = reduce(racedRunning, { type: "event", event: event({
    type: "history", sid: racedRunningSid, session_id: racedRunningSid,
    revision: "running-positive-rev", build_seq: 1, live_seq: 80,
    external: false, in_progress: true, has_more: false, events: [],
  }) });
  assert.equal(racedRunning.runtimes[racedRunningSid].state, "running",
    "a stale narrative History must still recover positive wrapper activity");
  assert.equal(racedRunning.runtimes[racedRunningSid].mirroredRunning, false);

  let newerIdle = {
    ...initialState, focusedSid: racedRunningSid,
    runtimes: { [racedRunningSid]: createRuntime() },
  };
  newerIdle = reduce(newerIdle, { type: "event", event: event({
    type: "state", sid: racedRunningSid, seq: 82, state: "idle",
  }) });
  newerIdle = reduce(newerIdle, { type: "event", event: event({
    type: "history", sid: racedRunningSid, session_id: racedRunningSid,
    revision: "running-positive-rev", build_seq: 2, live_seq: 81,
    external: false, in_progress: true, has_more: false, events: [],
  }) });
  assert.equal(newerIdle.runtimes[racedRunningSid].state, "idle",
    "older positive History must not overwrite a newer terminal lifecycle");

  // Ownership is control state too. A History build which started before a
  // newer live state frame must never resurrect a stale terminal lock (the
  // /review false-positive), nor clear a newer takeover/ownership decision.
  const ownershipSid = "history-ownership-race";
  let ownershipRace = {
    ...initialState, focusedSid: ownershipSid,
    runtimes: { [ownershipSid]: createRuntime() },
  };
  ownershipRace = reduce(ownershipRace, { type: "event", event: event({
    type: "state", sid: ownershipSid, seq: 80, state: "running",
  }) });
  assert.equal(ownershipRace.runtimes[ownershipSid].lastLiveSeq, 80,
    "non-narrative live state must advance the History race watermark");
  ownershipRace = reduce(ownershipRace, { type: "event", event: event({
    type: "history", sid: ownershipSid, session_id: ownershipSid,
    revision: "ownership-rev", build_seq: 1, live_seq: 79,
    external: true, takeover_pending: true,
    in_progress: true, has_more: false, events: [],
  }) });
  assert.equal(Boolean(ownershipRace.runtimes[ownershipSid].external), false);
  assert.equal(ownershipRace.runtimes[ownershipSid].takeoverPending, false);

  ownershipRace = reduce(ownershipRace, { type: "event", event: event({
    type: "history", sid: ownershipSid, session_id: ownershipSid,
    revision: "ownership-rev", build_seq: 2, live_seq: 80,
    external: true, takeover_pending: true,
    in_progress: true, has_more: true, oldest_id: "cursor", events: [],
  }) });
  assert.equal(ownershipRace.runtimes[ownershipSid].external, true);
  assert.equal(ownershipRace.runtimes[ownershipSid].takeoverPending, true);
  ownershipRace = reduce(ownershipRace, { type: "event", event: event({
    type: "state", sid: ownershipSid, seq: 81, state: "running",
  }) });
  ownershipRace = reduce(ownershipRace, { type: "event", event: event({
    type: "history", sid: ownershipSid, session_id: ownershipSid,
    revision: "ownership-rev", build_seq: 3, live_seq: 80,
    external: false, takeover_pending: false,
    in_progress: false, has_more: true, oldest_id: "cursor", events: [],
  }) });
  assert.equal(ownershipRace.runtimes[ownershipSid].external, true);
  assert.equal(ownershipRace.runtimes[ownershipSid].takeoverPending, true);
  ownershipRace = reduce(ownershipRace, { type: "event", event: event({
    type: "history", sid: ownershipSid, session_id: ownershipSid,
    revision: "ownership-rev", build_seq: 3, before: "cursor",
    external: false, takeover_pending: false,
    has_more: false, events: [],
  }) });
  assert.equal(ownershipRace.runtimes[ownershipSid].external, true,
    "pagination cannot overwrite current ownership state");
  assert.equal(ownershipRace.runtimes[ownershipSid].takeoverPending, true);

  // Newest-page build ordering is independent from pagination. A late older
  // first page cannot replace build 2. Pagination remains revision/cursor based
  // because another client's targeted head build may advance the server counter
  // without ever being routed to this browser.
  const orderedHistorySid = "ordered-history";
  let orderedHistory = reduce({ ...initialState, focusedSid: orderedHistorySid }, {
    type: "event", event: event({
      type: "history", sid: orderedHistorySid, session_id: orderedHistorySid,
      revision: "ordered-rev-new", generation: "wrapper-one",
      build_seq: 2, has_more: true,
      oldest_id: "new", events: [
        event({ type: "user_msg", sid: orderedHistorySid,
          msg_id: "new", prompt: "new" }),
        event({ type: "turn_end", sid: orderedHistorySid,
          result: { subtype: "success", duration_ms: 1, is_error: false } }),
      ],
    }),
  });
  orderedHistory = reduce(orderedHistory, { type: "event", event: event({
    type: "history", sid: orderedHistorySid, session_id: orderedHistorySid,
    revision: "ordered-rev-old", generation: "wrapper-one",
    build_seq: 1, has_more: false,
    events: [event({ type: "user_msg", sid: orderedHistorySid,
      msg_id: "late-old-head", prompt: "old" })],
  }) });
  assert.deepEqual(orderedHistory.runtimes[orderedHistorySid].turns.map(
    (turn: { id: string }) => turn.id), ["new"]);
  orderedHistory = reduce(orderedHistory, { type: "event", event: event({
    type: "history", sid: orderedHistorySid, session_id: orderedHistorySid,
    revision: "ordered-rev-new", generation: "wrapper-one",
    build_seq: 2, before: "new", has_more: false,
    events: [
      event({ type: "user_msg", sid: orderedHistorySid,
        msg_id: "older-page", prompt: "older" }),
      event({ type: "turn_end", sid: orderedHistorySid,
        result: { subtype: "success", duration_ms: 1, is_error: false } }),
    ],
  }) });
  assert.deepEqual(orderedHistory.runtimes[orderedHistorySid].turns.map(
    (turn: { id: string }) => turn.id), ["older-page", "new"]);
  orderedHistory = reduce(orderedHistory, { type: "event", event: event({
    type: "history", sid: orderedHistorySid, session_id: orderedHistorySid,
    revision: "ordered-rev-new", generation: "wrapper-one",
    build_seq: 1, before: "older-page",
    has_more: false, events: [
      event({ type: "user_msg", sid: orderedHistorySid,
        msg_id: "stale-page", prompt: "stale" }),
      event({ type: "turn_end", sid: orderedHistorySid,
        result: { subtype: "success", duration_ms: 1, is_error: false } }),
    ],
  }) });
  assert.deepEqual(orderedHistory.runtimes[orderedHistorySid].turns.map(
    (turn: { id: string }) => turn.id), ["stale-page", "older-page", "new"]);

  // A targeted newest-page refresh is only the moving head window. Once this
  // browser has explicitly paged backwards, that refresh must retain the older
  // pages and their floor cursor instead of collapsing the conversation back
  // to the handful of newest events.
  const pagedHeadSid = "paged-history-survives-head-refresh";
  let pagedHead = reduce({ ...initialState, focusedSid: pagedHeadSid }, {
    type: "event", event: event({
      type: "history", sid: pagedHeadSid, session_id: pagedHeadSid,
      revision: "paged-rev", generation: "wrapper-one", build_seq: 1,
      has_more: true, oldest_id: "head-cursor", events: [
        event({ type: "user_msg", sid: pagedHeadSid,
          msg_id: "head-one", prompt: "head one", ts: 30 }),
        event({ type: "turn_end", sid: pagedHeadSid,
          result: { subtype: "success", duration_ms: 1, is_error: false } }),
      ],
    }),
  });
  pagedHead = reduce(pagedHead, { type: "event", event: event({
    type: "history", sid: pagedHeadSid, session_id: pagedHeadSid,
    revision: "paged-rev", generation: "wrapper-one", build_seq: 1,
    before: "head-cursor", has_more: false, oldest_id: "history-floor",
    events: [
      event({ type: "user_msg", sid: pagedHeadSid,
        msg_id: "older-one", prompt: "older one", ts: 10 }),
      event({ type: "turn_end", sid: pagedHeadSid,
        result: { subtype: "success", duration_ms: 1, is_error: false } }),
    ],
  }) });
  pagedHead = reduce(pagedHead, { type: "event", event: event({
    type: "history", sid: pagedHeadSid, session_id: pagedHeadSid,
    revision: "paged-rev", generation: "wrapper-one", build_seq: 2,
    has_more: true, oldest_id: "new-head-cursor", events: [
      event({ type: "user_msg", sid: pagedHeadSid,
        msg_id: "head-two", prompt: "head two", ts: 40 }),
      event({ type: "turn_end", sid: pagedHeadSid,
        result: { subtype: "success", duration_ms: 1, is_error: false } }),
    ],
  }) });
  assert.deepEqual(pagedHead.runtimes[pagedHeadSid].turns.map(
    (turn: { id: string }) => turn.id), ["older-one", "head-one", "head-two"]);
  assert.equal(pagedHead.runtimes[pagedHeadSid].hasMore, false,
    "a head refresh cannot reopen pagination after the browser reached the floor");
  assert.equal(pagedHead.runtimes[pagedHeadSid].oldestId, "history-floor");
  assert.equal(pagedHead.runtimes[pagedHeadSid].hasLoadedOlderHistory, true);

  // Even before the user explicitly paginates, a same-revision compact/head
  // refresh is only a suffix. It must not erase rows already rendered from the
  // live stream or the first bounded page.
  const compactHeadSid = "compact-head-keeps-painted-history";
  let compactHead = reduce({ ...initialState, focusedSid: compactHeadSid }, {
    type: "event", event: event({
      type: "history", sid: compactHeadSid, session_id: compactHeadSid,
      revision: "compact-rev", generation: "wrapper-one", build_seq: 1,
      has_more: true, oldest_id: "original-cursor", events: [
        event({ type: "user_msg", sid: compactHeadSid,
          msg_id: "long-turn", prompt: "original prompt", ts: 10 }),
        event({ type: "assistant_msg_start", sid: compactHeadSid,
          message_id: "before-compact", channel: "commentary" }),
        event({ type: "delta", sid: compactHeadSid,
          message_id: "before-compact", text: "before compact" }),
      ],
    }),
  });
  compactHead = reduce(compactHead, { type: "event", event: event({
    type: "history", sid: compactHeadSid, session_id: compactHeadSid,
    revision: "compact-rev", generation: "wrapper-one", build_seq: 2,
    has_more: true, oldest_id: "forced-tail-cursor", events: [
      event({ type: "user_msg", sid: compactHeadSid,
        msg_id: "long-turn", prompt: "original prompt", ts: 10 }),
      event({ type: "assistant_msg_start", sid: compactHeadSid,
        message_id: "after-compact", channel: "commentary" }),
      event({ type: "delta", sid: compactHeadSid,
        message_id: "after-compact", text: "after compact" }),
    ],
  }) });
  assert.equal(compactHead.runtimes[compactHeadSid].turns.length, 1);
  assert.equal(compactHead.runtimes[compactHeadSid].turns[0].prompt,
    "original prompt");
  assert.match(JSON.stringify(compactHead.runtimes[compactHeadSid].turns[0]),
    /before compact/);
  assert.match(JSON.stringify(compactHead.runtimes[compactHeadSid].turns[0]),
    /after compact/);
  assert.equal(compactHead.runtimes[compactHeadSid].oldestId,
    "original-cursor");

  // A newest page made entirely of genuine prompt-less background turns is
  // not evidence that the page is a byte-window suffix of the current user
  // turn. Repeated focus/history refreshes must keep those turns independent.
  const assistantPageSid = "assistant-page-does-not-attach-to-current";
  const assistantPageTurns = [
    {
      id: "background-one", prompt: "", done: true,
      blocks: [{
        kind: "text", message_id: "background-one-answer",
        text: "older background answer one", done: true, channel: "final",
      }],
      detailEventCount: 0, detailLoaded: false,
      ts: 4_000, doneTs: 5_000,
    },
    {
      id: "background-two", prompt: "", done: true,
      blocks: [{
        kind: "text", message_id: "background-two-answer",
        text: "older background answer two", done: true, channel: "final",
      }],
      detailEventCount: 0, detailLoaded: false,
      ts: 6_000, doneTs: 7_000,
    },
  ];
  let assistantPageState = reduce({
    ...initialState, focusedSid: assistantPageSid,
  }, { type: "event", event: event({
    type: "history", sid: assistantPageSid, session_id: assistantPageSid,
    revision: "assistant-page-rev", generation: "wrapper-one",
    build_seq: 1, live_seq: 0, has_more: true,
    oldest_id: "background-one", detail: "summary", events: [],
    turns: assistantPageTurns,
  }) });
  assistantPageState = reduce(assistantPageState, {
    type: "event", event: event({
      type: "user_msg", sid: assistantPageSid, seq: 1, ts: 9,
      msg_id: "current-user-turn", prompt: "current question",
    }),
  });
  assistantPageState = reduce(assistantPageState, {
    type: "event", event: event({
      type: "history", sid: assistantPageSid, session_id: assistantPageSid,
      revision: "assistant-page-rev", generation: "wrapper-one",
      build_seq: 2, live_seq: 1, has_more: true, in_progress: true,
      oldest_id: "background-one", detail: "summary", events: [],
      turns: assistantPageTurns,
    }),
  });
  assert.deepEqual(
    assistantPageState.runtimes[assistantPageSid].turns.map(
      (turn: { id: string }) => turn.id),
    ["background-one", "background-two", "current-user-turn"],
  );
  const currentAssistantPageTurn =
    assistantPageState.runtimes[assistantPageSid].turns.at(-1);
  assert.equal(currentAssistantPageTurn?.blocks.length, 0);
  assert.doesNotMatch(
    JSON.stringify(currentAssistantPageTurn),
    /older background answer/,
    "prompt-less history rows must never be stitched into a user turn",
  );

  // A real wrapper restart owns a new generation, so its build counter may
  // legitimately start from one even when the previous generation reached two.
  orderedHistory = reduce(orderedHistory, { type: "event", event: event({
    type: "history", sid: orderedHistorySid, session_id: orderedHistorySid,
    revision: "restart-rev", generation: "wrapper-two",
    build_seq: 1, has_more: false, events: [
      event({ type: "user_msg", sid: orderedHistorySid,
        msg_id: "after-restart", prompt: "restart" }),
      event({ type: "turn_end", sid: orderedHistorySid,
        result: { subtype: "success", duration_ms: 1, is_error: false } }),
    ],
  }) });
  assert.deepEqual(orderedHistory.runtimes[orderedHistorySid].turns.map(
    (turn: { id: string }) => turn.id), ["after-restart"]);

  // Browser retention must not erase the backend's pagination cursor. A
  // pathological newest page can still exceed the canonical data window while
  // the server has older transcript windows available.
  const boundedCursorSid = "bounded-history-keeps-server-cursor";
  const manyHistoryEvents = Array.from(
    { length: MAX_RUNTIME_TURNS + 40 }, (_, index) => [
    event({ type: "user_msg", sid: boundedCursorSid,
      msg_id: `bounded-${index}`, prompt: `question ${index}` }),
    event({ type: "turn_end", sid: boundedCursorSid,
      result: { subtype: "success", duration_ms: 1, is_error: false } }),
  ]).flat();
  const boundedCursorState = reduce({
    ...initialState, focusedSid: boundedCursorSid,
  }, { type: "event", event: event({
    type: "history", sid: boundedCursorSid, session_id: boundedCursorSid,
    revision: "bounded-cursor-rev", generation: "wrapper-one",
    build_seq: 1, has_more: true, oldest_id: "server-byte-cursor",
    events: manyHistoryEvents,
  }) });
  assert.equal(
    boundedCursorState.runtimes[boundedCursorSid].turns.length,
    MAX_RUNTIME_TURNS);
  assert.equal(boundedCursorState.runtimes[boundedCursorSid].hasMore, true);
  assert.equal(boundedCursorState.runtimes[boundedCursorSid].oldestId,
    "server-byte-cursor");
  assert.equal(boundedCursorState.runtimes[boundedCursorSid].truncated, true);

  const failedHistorySid = "non-authoritative-history";
  const preservedTurn = {
    id: "preserved", prompt: "keep", blocks: [], done: true,
  };
  let failedHistory = {
    ...initialState, focusedSid: failedHistorySid,
    runtimes: { [failedHistorySid]: {
      ...createRuntime(), turns: [preservedTurn], loading: true,
      historyRevision: "known-good", historyBuildSeq: 9,
    } },
  };
  failedHistory = reduce(failedHistory, { type: "event", event: event({
    type: "history", sid: failedHistorySid, session_id: failedHistorySid,
    revision: "known-good", build_seq: 10, authoritative: false,
    error: "历史暂时不可用，请稍后重试", has_more: false, events: [],
  }) });
  assert.deepEqual(failedHistory.runtimes[failedHistorySid].turns,
    [preservedTurn]);
  assert.equal(failedHistory.runtimes[failedHistorySid].historyRevision,
    "known-good");
  assert.equal(failedHistory.runtimes[failedHistorySid].historyBuildSeq, 9);
  assert.equal(failedHistory.runtimes[failedHistorySid].loading, false);
  assert.equal(failedHistory.banner, undefined,
    "a recoverable history read must stay silent while preserving the projection");

  // Even when the revision is unchanged, a first page is authoritative for
  // completed rows. This prevents generic stale-cache resurrection, not only
  // the explicit rollback-marker path.
  const sameRevisionSid = "same-revision-stale-idb";
  let sameRevisionState = reduce({
    ...initialState, focusedSid: sameRevisionSid,
  }, {
    type: "hydrate_cache", sid: sameRevisionSid, turns: staleTurns,
    revision: "boot-current:7",
  });
  sameRevisionState = reduce(sameRevisionState, {
    type: "event", event: event({
      type: "history", sid: sameRevisionSid, session_id: sameRevisionSid,
      revision: "boot-current:7", has_more: false,
      events: [
        event({ type: "user_msg", sid: sameRevisionSid,
          msg_id: "kept-before-rewind", prompt: "保留" }),
        event({ type: "turn_end", sid: sameRevisionSid,
          result: { subtype: "success", duration_ms: 1, is_error: false } }),
      ],
    }),
  });
  assert.deepEqual(sameRevisionState.runtimes[sameRevisionSid].turns.map(
    (turn: { id: string }) => turn.id), ["kept-before-rewind"]);

  // When the tiny invalidation marker itself fell out of the ring, the replay
  // gap is still a conservative invalidation boundary. Never leave a stale
  // preview open or finish loading before authoritative History arrives.
  const gapSid = "replay-gap";
  let gapState = {
    ...initialState,
    focusedSid: gapSid,
    artifact: { file: "stale.ts", kind: "file" as const, sid: gapSid },
    runtimes: {
      [gapSid]: {
        ...createRuntime(), turns: staleTurns,
        historyRevision: "boot-old:9", syncReady: true,
      },
    },
  };
  gapState = reduce(gapState, { type: "event", event: event({
    type: "replay_start", sid: gapSid, from_seq: 20, to_seq: 30,
    truncated: true, rebuild: false,
  }) });
  assert.equal(gapState.artifact, null);
  assert.deepEqual(gapState.runtimes[gapSid].turns, []);
  assert.equal(gapState.runtimes[gapSid].historyInvalidated, true);
  assert.equal(gapState.runtimes[gapSid].loading, true);
  gapState = reduce(gapState, { type: "event", event: event({
    type: "replay_end", sid: gapSid, to_seq: 30, truncated: true,
  }) });
  assert.equal(gapState.runtimes[gapSid].loading, true,
    "ReplayEnd cannot satisfy a transcript gap");
  gapState = reduce(gapState, { type: "event", event: event({
    type: "history", sid: gapSid, session_id: gapSid,
    revision: "boot-new:9", has_more: false, events: [],
  }) });
  assert.equal(gapState.runtimes[gapSid].historyInvalidated, false);
  assert.equal(gapState.runtimes[gapSid].loading, false);

  // A truncated live-tail replay can begin in the middle of an older turn.
  // Without its UserMsg/TurnEnd, that fragment looks like a new unfinished
  // assistant-only turn. The following authoritative History must discard the
  // unmatched fragment instead of sorting it after the real current turn.
  const replayFragmentSid = "replay-fragment-does-not-cross-history";
  let replayFragmentState = reduce({
    ...initialState, focusedSid: replayFragmentSid,
  }, { type: "event", event: event({
    type: "replay_start", sid: replayFragmentSid, from_seq: 70, to_seq: 74,
    truncated: true, rebuild: false,
  }) });
  replayFragmentState = reduce(replayFragmentState, {
    type: "event", event: event({
      type: "assistant_msg_start", sid: replayFragmentSid, seq: 71, ts: 7,
      message_id: "orphaned-old-commentary", channel: "commentary",
    }),
  });
  replayFragmentState = reduce(replayFragmentState, {
    type: "event", event: event({
      type: "delta", sid: replayFragmentSid, seq: 72, ts: 7.1,
      message_id: "orphaned-old-commentary",
      text: "older reasoning replayed without its user boundary",
      channel: "commentary",
    }),
  });
  replayFragmentState = reduce(replayFragmentState, {
    type: "event", event: event({
      type: "user_msg", sid: replayFragmentSid, seq: 73, ts: 9,
      msg_id: "current-turn", prompt: "current question",
    }),
  });
  replayFragmentState = reduce(replayFragmentState, {
    type: "event", event: event({
      type: "assistant_msg_start", sid: replayFragmentSid, seq: 74, ts: 9.1,
      message_id: "current-commentary", channel: "commentary",
    }),
  });
  replayFragmentState = reduce(replayFragmentState, {
    type: "event", event: event({
      type: "replay_end", sid: replayFragmentSid, to_seq: 74,
      truncated: true,
    }),
  });
  replayFragmentState = reduce(replayFragmentState, {
    type: "event", event: event({
      type: "history", sid: replayFragmentSid,
      session_id: replayFragmentSid, revision: "fragment-rev",
      generation: "wrapper-one", build_seq: 1, live_seq: 74,
      has_more: true, oldest_id: "authoritative-old",
      in_progress: true, detail: "summary", events: [],
      turns: [
        {
          id: "authoritative-old", prompt: "real older question",
          blocks: [], done: true, detailEventCount: 0,
          detailLoaded: false, ts: 5_000, doneTs: 6_000,
        },
        {
          id: "current-turn", prompt: "current question",
          blocks: [{
            kind: "text", message_id: "current-commentary",
            text: "current work", done: false, channel: "commentary",
          }],
          done: false, detailEventCount: 1,
          detailLoaded: false, ts: 9_000,
        },
      ],
    }),
  });
  assert.deepEqual(
    replayFragmentState.runtimes[replayFragmentSid].turns.map(
      (turn: { id: string }) => turn.id),
    ["authoritative-old", "current-turn"],
    "an unmatched replay prefix must not become the newest visible history",
  );
  assert.doesNotMatch(
    JSON.stringify(replayFragmentState.runtimes[replayFragmentSid].turns),
    /older reasoning replayed without its user boundary/,
  );

  // The inverse race is valid: the newest running UserMsg may have replayed
  // before its transcript row was flushed into History. Keep exactly that
  // newest unfinished tail while still removing an older orphaned prefix.
  const unflushedTailSid = "replay-gap-keeps-current-unflushed-tail";
  let unflushedTailState = reduce({
    ...initialState, focusedSid: unflushedTailSid,
  }, { type: "event", event: event({
    type: "replay_start", sid: unflushedTailSid, from_seq: 80, to_seq: 83,
    truncated: true, rebuild: false,
  }) });
  for (const replayEvent of [
    event({
      type: "assistant_msg_start", sid: unflushedTailSid, seq: 81, ts: 7,
      message_id: "unflushed-orphan", channel: "commentary",
    }),
    event({
      type: "user_msg", sid: unflushedTailSid, seq: 82, ts: 10,
      msg_id: "unflushed-current", prompt: "just accepted",
    }),
    event({
      type: "assistant_msg_start", sid: unflushedTailSid, seq: 83, ts: 10.1,
      message_id: "unflushed-current-commentary", channel: "commentary",
    }),
  ]) {
    unflushedTailState = reduce(unflushedTailState, {
      type: "event", event: replayEvent,
    });
  }
  unflushedTailState = reduce(unflushedTailState, {
    type: "event", event: event({
      type: "history", sid: unflushedTailSid,
      session_id: unflushedTailSid, revision: "unflushed-rev",
      generation: "wrapper-one", build_seq: 1, live_seq: 83,
      has_more: true, in_progress: true, detail: "summary", events: [],
      turns: [{
        id: "last-flushed", prompt: "last flushed question",
        blocks: [], done: true, detailEventCount: 0,
        detailLoaded: false, ts: 5_000, doneTs: 6_000,
      }],
    }),
  });
  assert.deepEqual(
    unflushedTailState.runtimes[unflushedTailSid].turns.map(
      (turn: { id: string }) => turn.id),
    ["last-flushed", "unflushed-current"],
  );
  assert.doesNotMatch(
    JSON.stringify(unflushedTailState.runtimes[unflushedTailSid].turns),
    /unflushed-orphan/,
  );

  const rebuildSeqSid = "wrapper-generation-rebuild";
  let rebuildSeqState = {
    ...initialState, focusedSid: rebuildSeqSid,
    runtimes: { [rebuildSeqSid]: {
      ...createRuntime(), historyBuildSeq: 8, lastLiveSeq: 200,
      lastLifecycleSeq: 199,
    } },
  };
  rebuildSeqState = reduce(rebuildSeqState, { type: "event", event: event({
    type: "replay_start", sid: rebuildSeqSid, from_seq: 1, to_seq: 2,
    truncated: false, rebuild: true,
  }) });
  assert.equal(rebuildSeqState.runtimes[rebuildSeqSid].historyBuildSeq, 0);
  assert.equal(rebuildSeqState.runtimes[rebuildSeqSid].lastLiveSeq, 0);
  assert.equal(rebuildSeqState.runtimes[rebuildSeqSid].lastLifecycleSeq, 0);

  state = { ...state, focusedSid: sid };
  // Claude has a static presentation catalog, but its explicit defaults come
  // from cwd-aware settings. An empty models array must still update them.
  state = reduce(state, { type: "event", event: event({
    type: "models", engine: "claude", models: [],
    default_model: "claude-mythos-5[1m]", default_effort: "max",
    cwd: "~",
  }) });
  assert.equal(state.catalogDefault.claude, "claude-mythos-5");
  assert.equal(state.catalogDefaultEffort.claude, "max");
  const { NewChatView } = await reducerHarness.ssrLoadModule(
    "/src/components/NewChatView.tsx");
  const { WorkArtifactsSheet } = await reducerHarness.ssrLoadModule(
    "/src/components/WorkArtifactsSheet.tsx");
  const newChatMarkup = renderToStaticMarkup(createElement(NewChatView, {
    cwd: "~", engine: "claude",
    onPickCwd: () => {},
    onSend: () => true,
  }));
  const codexNewChatMarkup = renderToStaticMarkup(createElement(NewChatView, {
    cwd: "~", engine: "codex",
    onPickCwd: () => {},
    onSend: () => true,
  }));
  for (const markup of [newChatMarkup, codexNewChatMarkup]) {
    assert.match(markup, /aria-label="添加照片"/);
    assert.match(markup, /aria-label="添加文件"/);
    assert.match(markup, /accept="image\/\*"/);
    assert.match(markup, /multiple=""/);
    assert.equal(
      (markup.match(/<button[^>]+aria-label="添加照片"/g) ?? []).length, 1);
    assert.equal(
      (markup.match(/<button[^>]+aria-label="添加文件"/g) ?? []).length, 0);
    assert.match(markup, />开始</);
    assert.doesNotMatch(markup, /本机默认|默认 ·|选择模型|思考强度/);
  }
  assert.doesNotMatch(codexNewChatMarkup, /不询问|Plan|标准/);
  const artifactsMarkup = renderToStaticMarkup(createElement(WorkArtifactsSheet, {
    open: true,
    artifacts: [
      { path: "report.md", size: 1024, modified_at: 1, kind: "document", previewable: true },
      { path: "slides/deck.pptx", size: 4096, modified_at: 2, kind: "presentation", previewable: true },
    ],
    onOpen: () => {},
    onClose: () => {},
  }));
  assert.match(artifactsMarkup, /当前工作产生的 2 个文件/);
  assert.match(artifactsMarkup, /report\.md/);
  assert.match(artifactsMarkup, /slides\/deck\.pptx/);
  assert.doesNotMatch(artifactsMarkup, /暂不可预览|disabled=""/);
  state = { ...state,
    newChat: { cwd: "/other", model: null, effort: null } };
  state = reduce(state, { type: "event", event: event({
    type: "models", engine: "claude", models: [],
    default_model: "claude-sonnet-5", default_effort: "high", cwd: "/other",
  }) });
  state = reduce(state, { type: "event", event: event({
    type: "models", engine: "claude", models: [],
    default_model: "claude-mythos-5", default_effort: "max", cwd: "~",
  }) });
  assert.equal(state.catalogDefault.claude, "claude-sonnet-5");
  assert.equal(state.catalogDefaultEffort.claude, "high");
  assert.equal(state.catalogDefaultCwd.claude, "/other");
  state = reduce(state, { type: "event", event: event({
    type: "models", engine: "claude", models: [],
    default_model: null, default_effort: null, cwd: "/other",
  }) });
  assert.equal(state.catalogDefault.claude, undefined);
  assert.equal(state.catalogDefaultEffort.claude, undefined);
  assert.equal(state.catalogDefaultCwd.claude, "/other");
  // The newest history page restores both authoritative settings. An older
  // pagination page cannot roll either one back, even if a stale server includes
  // control rows that should only appear on the newest page.
  state = reduce(state, { type: "event", event: event({
    type: "history", sid, session_id: sid, revision: "main-rev", has_more: true,
    events: [
      event({ type: "model", sid, model: "claude-mythos-5" }),
      event({ type: "effort", sid, effort: "max" }),
    ],
  }) });
  assert.equal(state.runtimes[sid].model, "claude-mythos-5");
  assert.equal(state.runtimes[sid].effort, "max");
  state = reduce(state, { type: "event", event: event({
    type: "history", sid, session_id: sid, before: "older-turn",
    revision: "main-rev", has_more: false,
    events: [
      event({ type: "model", sid, model: "claude-sonnet-5" }),
      event({ type: "effort", sid, effort: "low" }),
    ],
  }) });
  assert.equal(state.runtimes[sid].model, "claude-mythos-5");
  assert.equal(state.runtimes[sid].effort, "max");
  const approvalBeforePlan = state.runtimes[sid].perm;
  state = reduce(state, { type: "event", event: event({
    type: "collaboration_mode", sid, mode: "plan",
  }) });
  assert.equal(state.runtimes[sid].collaborationMode, "plan");
  assert.equal(state.runtimes[sid].perm, approvalBeforePlan);
  assert.equal(state.runtimes[otherSid].collaborationMode, "default");
  state = reduce(state, { type: "set_collaboration_mode", mode: "default" });
  assert.equal(state.runtimes[sid].collaborationMode, "default");
  assert.equal(state.runtimes[sid].perm, approvalBeforePlan);
  const cachedSid = "cached-v5-codex";
  state = reduce(state, { type: "hydrate_cache", sid: cachedSid, turns: [{
    id: "cached-turn", codexTurnId: "legacy-turn-id", prompt: "旧缓存",
    done: true, blocks: [],
  }], revision: "cached-rev" });
  assert.equal(state.runtimes[cachedSid].turns[0].forkPointId, "legacy-turn-id");
  state = reduce(state, {
    type: "query_sent", sid, prompt: "在？", msg_id: "client-a", ts: 10_000,
  });
  for (const live of [
    event({ type: "user_msg", sid, msg_id: "client-a", prompt: "在？", ts: 10.1 }),
    event({ type: "assistant_msg_start", sid, message_id: "live-answer" }),
  ]) state = reduce(state, { type: "event", event: live });

  state = reduce(state, { type: "event", event: event({
    type: "history", sid, session_id: sid, revision: "main-rev",
    in_progress: true, has_more: false,
    events: [
      event({ type: "user_msg", sid, msg_id: "engine-a", prompt: "在？", ts: 10 }),
      event({ type: "turn_end", sid, ts: 11, turn_id: "codex-turn-a",
        result: { subtype: "success", duration_ms: 0, is_error: false } }),
    ],
  }) });
  assert.equal(state.runtimes[sid].turns.length, 1);
  assert.equal(state.runtimes[sid].turns[0].done, false);
  assert.equal(state.runtimes[sid].turns[0].forkPointId, "codex-turn-a");

  for (const live of [
    event({ type: "delta", sid, message_id: "live-answer", text: "only once" }),
    event({ type: "assistant_msg_end", sid, message_id: "live-answer" }),
    event({ type: "turn_end", sid, ts: 12, turn_id: "codex-turn-a",
      result: { subtype: "success", duration_ms: 2000, is_error: false } }),
  ]) state = reduce(state, { type: "event", event: live });

  state = reduce(state, { type: "event", event: event({
    type: "history", sid, session_id: sid, revision: "main-rev",
    in_progress: false, has_more: false,
    events: [
      event({ type: "user_msg", sid, msg_id: "engine-a", prompt: "在？", ts: 10 }),
      event({ type: "assistant_msg_start", sid, message_id: "engine-answer" }),
      event({ type: "delta", sid, message_id: "engine-answer", text: "only once" }),
      event({ type: "assistant_msg_end", sid, message_id: "engine-answer" }),
      event({ type: "turn_end", sid, ts: 12, turn_id: "codex-turn-a",
        result: { subtype: "success", duration_ms: 2000, is_error: false } }),
    ],
  }) });
  assert.equal(state.runtimes[sid].turns.length, 1);
  assert.equal(state.runtimes[sid].turns[0].forkPointId, "codex-turn-a");
  assert.deepEqual(state.runtimes[sid].turns[0].blocks.map(
    (block: { kind: string; text?: string }) => block.kind === "text" ? block.text : "tool"),
  ["only once"]);
  assert.deepEqual(state.runtimes[otherSid].turns, [untouched]);

  const richSid = "rich-process";
  state = {
    ...state,
    focusedSid: richSid,
    runtimes: {
      ...state.runtimes,
      [richSid]: { ...createRuntime(), state: "running", syncReady: true },
    },
  };
  state = reduce(state, { type: "query_sent", sid: richSid, prompt: "实现功能",
    msg_id: "rich-turn", ts: 30_000 });
  const richEvents = [
    event({ type: "assistant_msg_start", sid: richSid, message_id: "comment-1", channel: "commentary" }),
    event({ type: "delta", sid: richSid, message_id: "comment-1", channel: "commentary", text: "先检查代码。" }),
    event({ type: "assistant_msg_end", sid: richSid, message_id: "comment-1", channel: "commentary" }),
    event({ type: "tool_use", sid: richSid, message_id: "comment-1", tool_use_id: "cmd-1",
      tool: "shell", category: "command", input: { command: "npm test" } }),
    event({ type: "tool_delta", sid: richSid, tool_use_id: "cmd-1", stream: "output", delta: "ok\n" }),
    event({ type: "tool_result", sid: richSid, tool_use_id: "cmd-1", content: "ok\n",
      is_error: false, status: "succeeded", exit_code: 0, duration_ms: 1250 }),
    event({ type: "turn_plan", sid: richSid, item_id: "plan-1", turn_id: "turn-rich",
      explanation: "执行计划", plan: [{ step: "检查", status: "completed" }] }),
    event({ type: "process", sid: richSid, item_id: "hook-1", kind: "hook", phase: "end",
      status: "succeeded", turn_id: "turn-rich", title: "Hook 完成", duration_ms: 20 }),
    event({ type: "turn_diff", sid: richSid, item_id: "diff-1", turn_id: "turn-rich",
      diff: "diff --git a/a b/a" }),
    event({ type: "assistant_msg_start", sid: richSid, message_id: "final-1", channel: "final" }),
    event({ type: "delta", sid: richSid, message_id: "final-1", channel: "final", text: "已经完成。" }),
    event({ type: "assistant_msg_end", sid: richSid, message_id: "final-1", channel: "final" }),
    event({ type: "turn_end", sid: richSid, ts: 33, turn_id: "turn-rich",
      result: { subtype: "success", duration_ms: 3000, is_error: false } }),
  ];
  for (const richEvent of richEvents) {
    state = reduce(state, { type: "event", event: richEvent });
  }
  const richTurn = state.runtimes[richSid].turns[0];
  assert.equal(richTurn.durationMs, 3000);
  assert.deepEqual(richTurn.blocks.filter((block: { kind: string }) => block.kind === "text")
    .map((block: { channel?: string }) => block.channel), ["commentary", "final"]);
  const commandBlock = richTurn.blocks.find((block: { kind: string }) => block.kind === "tool") as {
    output?: string; result?: { exit_code?: number; duration_ms?: number };
  };
  assert.equal(commandBlock.output, "ok\n");
  assert.equal(commandBlock.result?.exit_code, 0);
  assert.equal(commandBlock.result?.duration_ms, 1250);
  assert.equal(richTurn.blocks.filter((block: { kind: string }) => block.kind === "process").length, 3);

  // Claude can reveal only at Result that a stop_reason=null text block was the
  // final answer. A repeated End for the same id must reclassify in place, while
  // an End for an unknown id must never manufacture an empty text block/turn.
  const correctedSid = "claude-final-correction";
  state = {
    ...state,
    runtimes: {
      ...state.runtimes,
      [correctedSid]: { ...createRuntime(), state: "running", syncReady: true },
    },
  };
  state = reduce(state, { type: "query_sent", sid: correctedSid, prompt: "回答",
    msg_id: "corrected-turn", ts: 30_500 });
  for (const correctionEvent of [
    event({ type: "assistant_msg_start", sid: correctedSid,
      message_id: "ambiguous-answer", channel: "unknown" }),
    event({ type: "delta", sid: correctedSid,
      message_id: "ambiguous-answer", channel: "unknown", text: "真实正文" }),
    event({ type: "assistant_msg_end", sid: correctedSid,
      message_id: "ambiguous-answer", channel: "commentary" }),
    event({ type: "assistant_msg_end", sid: correctedSid,
      message_id: "ambiguous-answer", channel: "final" }),
    event({ type: "assistant_msg_end", sid: correctedSid,
      message_id: "missing-answer", channel: "final" }),
  ]) state = reduce(state, { type: "event", event: correctionEvent });
  const correctedTurns = state.runtimes[correctedSid].turns;
  assert.equal(correctedTurns.length, 1);
  assert.equal(correctedTurns[0].blocks.length, 1);
  assert.deepEqual(correctedTurns[0].blocks[0], {
    kind: "text", message_id: "ambiguous-answer", text: "真实正文",
    done: true, channel: "final",
  });

  // A compromised/noisy upstream must not grow an active turn without bound
  // before the normal completed-turn runtime eviction can run.
  const noisySid = "bounded-rich-delta";
  state = {
    ...state,
    runtimes: {
      ...state.runtimes,
      [noisySid]: { ...createRuntime(), state: "running", syncReady: true },
    },
  };
  state = reduce(state, { type: "query_sent", sid: noisySid, prompt: "stream",
    msg_id: "noisy-turn", ts: 31_000 });
  state = reduce(state, { type: "event", event: event({
    type: "tool_use", sid: noisySid, message_id: "noisy-msg",
    tool_use_id: "noisy-tool", tool: "shell", category: "command", input: {},
  }) });
  const noisyChunk = "x".repeat(512 * 1024);
  for (let index = 0; index < 6; index += 1) {
    state = reduce(state, { type: "event", event: event({
      type: "tool_delta", sid: noisySid, tool_use_id: "noisy-tool",
      stream: "output", delta: noisyChunk,
    }) });
  }
  const noisyTool = state.runtimes[noisySid].turns[0].blocks.find(
    (block: { kind: string }) => block.kind === "tool") as { output?: string };
  assert.equal(noisyTool.output?.length, 2 * 1024 * 1024);

  const { finalTextBlocks, hasActiveProcess } = await reducerHarness.ssrLoadModule(
    "/src/process-blocks.ts");
  state = reduce(state, { type: "event", event: event({
    type: "error", sid: noisySid, msg_id: "noisy-turn",
    code: "engine_failed", message: "stream stopped",
  }) });
  const failedNoisyTurn = state.runtimes[noisySid].turns[0];
  assert.equal(failedNoisyTurn.done, true);
  assert.equal(hasActiveProcess(failedNoisyTurn.blocks), false);
  const failedNoisyTool = failedNoisyTurn.blocks.find(
    (block: { kind: string }) => block.kind === "tool") as {
      done: boolean; result?: { is_error: boolean; status?: string | null };
    };
  assert.equal(failedNoisyTool.done, true);
  assert.equal(failedNoisyTool.result?.is_error, true);
  assert.equal(failedNoisyTool.result?.status, "failed");

  // A goal/automatic continuation can produce thousands of distinct items in
  // one still-running turn. Bound the item count (not only each item's bytes),
  // retain known final answers and newest live activity, and keep terminal
  // updates working for retained blocks.
  const boundedBlocksSid = "bounded-turn-blocks";
  state = {
    ...state,
    runtimes: {
      ...state.runtimes,
      [boundedBlocksSid]: { ...createRuntime(), state: "running", syncReady: true },
    },
  };
  state = reduce(state, { type: "query_sent", sid: boundedBlocksSid,
    prompt: "long goal", msg_id: "bounded-block-turn", ts: 31_500 });
  for (const finalEvent of [
    event({ type: "assistant_msg_start", sid: boundedBlocksSid,
      message_id: "early-final", channel: "final" }),
    event({ type: "delta", sid: boundedBlocksSid,
      message_id: "early-final", channel: "final", text: "必须保留的最终答案" }),
    event({ type: "assistant_msg_end", sid: boundedBlocksSid,
      message_id: "early-final", channel: "final" }),
  ]) state = reduce(state, { type: "event", event: finalEvent });

  const generatedBlocks = MAX_TURN_BLOCKS + 50;
  for (let index = 0; index < generatedBlocks; index += 1) {
    const suffix = String(index);
    const events = index % 5 === 0 ? [
      event({ type: "assistant_msg_start", sid: boundedBlocksSid,
        message_id: `comment-${suffix}`, channel: "commentary" }),
      event({ type: "delta", sid: boundedBlocksSid,
        message_id: `comment-${suffix}`, channel: "commentary", text: suffix }),
      event({ type: "assistant_msg_end", sid: boundedBlocksSid,
        message_id: `comment-${suffix}`, channel: "commentary" }),
    ] : index % 5 === 1 ? [
      event({ type: "tool_use", sid: boundedBlocksSid,
        message_id: `comment-${suffix}`, tool_use_id: `tool-${suffix}`,
        tool: "shell", category: "command", input: { command: "true" } }),
      event({ type: "tool_result", sid: boundedBlocksSid,
        tool_use_id: `tool-${suffix}`, content: "ok", is_error: false,
        status: "succeeded" }),
    ] : index % 5 === 2 ? [
      event({ type: "process", sid: boundedBlocksSid,
        item_id: `process-${suffix}`, kind: "hook", phase: "end",
        status: "succeeded", title: `Hook ${suffix}` }),
    ] : index % 5 === 3 ? [
      event({ type: "turn_plan", sid: boundedBlocksSid,
        item_id: `plan-${suffix}`, explanation: suffix,
        plan: [{ step: suffix, status: "completed" }] }),
    ] : [
      event({ type: "turn_diff", sid: boundedBlocksSid,
        item_id: `diff-${suffix}`, diff: `+${suffix}` }),
    ];
    for (const generatedEvent of events) {
      state = reduce(state, { type: "event", event: generatedEvent });
    }
  }
  state = reduce(state, { type: "event", event: event({
    type: "process", sid: boundedBlocksSid, item_id: "latest-active",
    kind: "agent", phase: "start", status: "running", title: "最新活动",
  }) });
  let boundedBlockTurn = state.runtimes[boundedBlocksSid].turns[0];
  assert.equal(boundedBlockTurn.blocks.length, MAX_TURN_BLOCKS);
  assert.equal(boundedBlockTurn.blocks.filter((block: { kind: string; item_id?: string }) =>
    block.kind === "process" && block.item_id === OMITTED_PROCESS_ITEM_ID).length, 1);
  assert.ok(boundedBlockTurn.blocks.some(
    (block: { kind: string; message_id?: string; channel?: string }) =>
      block.kind === "text" && block.message_id === "early-final"
      && block.channel === "final"));
  assert.ok(boundedBlockTurn.blocks.some(
    (block: { kind: string; item_id?: string; done: boolean }) =>
      block.kind === "process" && block.item_id === "latest-active" && !block.done));
  for (const expectedId of ["comment-305", "tool-301", "process-302", "plan-303", "diff-304"]) {
    assert.ok(boundedBlockTurn.blocks.some((block: {
      kind: string; message_id?: string; tool_use_id?: string; item_id?: string;
    }) => block.message_id === expectedId || block.tool_use_id === expectedId
      || block.item_id === expectedId));
  }

  state = reduce(state, { type: "event", event: event({
    type: "process", sid: boundedBlocksSid, item_id: "latest-active",
    kind: "agent", phase: "end", status: "succeeded", title: "最新活动完成",
  }) });
  boundedBlockTurn = state.runtimes[boundedBlocksSid].turns[0];
  const closedLatest = boundedBlockTurn.blocks.find((block: {
    kind: string; item_id?: string;
  }) => block.kind === "process" && block.item_id === "latest-active") as {
    done: boolean; status: string;
  };
  assert.equal(closedLatest.done, true);
  assert.equal(closedLatest.status, "succeeded");

  for (const finalEvent of [
    event({ type: "assistant_msg_start", sid: boundedBlocksSid,
      message_id: "late-final", channel: "final" }),
    event({ type: "delta", sid: boundedBlocksSid,
      message_id: "late-final", channel: "final", text: "最新最终答案" }),
    event({ type: "assistant_msg_end", sid: boundedBlocksSid,
      message_id: "late-final", channel: "final" }),
  ]) state = reduce(state, { type: "event", event: finalEvent });
  boundedBlockTurn = state.runtimes[boundedBlocksSid].turns[0];
  assert.equal(boundedBlockTurn.blocks.length, MAX_TURN_BLOCKS);
  assert.ok(boundedBlockTurn.blocks.some((block: {
    kind: string; message_id?: string; channel?: string;
  }) => block.kind === "text" && block.message_id === "late-final"
    && block.channel === "final"));

  const boundedPayloadSid = "bounded-turn-payload";
  state = {
    ...state,
    runtimes: {
      ...state.runtimes,
      [boundedPayloadSid]: { ...createRuntime(), state: "running", syncReady: true },
    },
  };
  state = reduce(state, { type: "query_sent", sid: boundedPayloadSid,
    prompt: "large diffs", msg_id: "bounded-payload-turn", ts: 31_700 });
  const maximumDiff = "d".repeat(2 * 1024 * 1024);
  for (let index = 0; index < 10; index += 1) {
    state = reduce(state, { type: "event", event: event({
      type: "turn_diff", sid: boundedPayloadSid, item_id: `large-diff-${index}`,
      diff: maximumDiff,
    }) });
  }
  const payloadBlocks = state.runtimes[boundedPayloadSid].turns[0].blocks;
  const retainedDiffChars = payloadBlocks.reduce((total: number, block: {
    kind: string; diff?: string | null;
  }) => total + (block.kind === "process" ? (block.diff?.length ?? 0) : 0), 0);
  assert.ok(retainedDiffChars <= MAX_TURN_BLOCK_CHARS);
  assert.ok(payloadBlocks.some((block: { kind: string; item_id?: string }) =>
    block.kind === "process" && block.item_id === "large-diff-9"));
  assert.equal(payloadBlocks.filter((block: { kind: string; item_id?: string }) =>
    block.kind === "process" && block.item_id === OMITTED_PROCESS_ITEM_ID).length, 1);

  // Authoritative History is reduced through the same bounded window and must
  // not recreate an oversized turn after a refresh.
  const boundedHistorySid = "bounded-history-blocks";
  const boundedHistoryEvents: ServerEvent[] = [
    event({ type: "user_msg", sid: boundedHistorySid,
      msg_id: "bounded-history-turn", prompt: "history" }),
    event({ type: "assistant_msg_start", sid: boundedHistorySid,
      message_id: "history-final", channel: "final" }),
    event({ type: "delta", sid: boundedHistorySid,
      message_id: "history-final", channel: "final", text: "历史最终答案" }),
    event({ type: "assistant_msg_end", sid: boundedHistorySid,
      message_id: "history-final", channel: "final" }),
  ];
  for (let index = 0; index < MAX_TURN_BLOCKS + 30; index += 1) {
    boundedHistoryEvents.push(event({
      type: "process", sid: boundedHistorySid, item_id: `history-process-${index}`,
      kind: "hook", phase: "end", status: "succeeded", title: `History ${index}`,
    }));
  }
  boundedHistoryEvents.push(event({
    type: "turn_end", sid: boundedHistorySid,
    result: { subtype: "success", duration_ms: 1, is_error: false },
  }));
  state = reduce(state, { type: "event", event: event({
    type: "history", sid: boundedHistorySid, session_id: boundedHistorySid,
    revision: "bounded-rev", in_progress: false, has_more: false,
    events: boundedHistoryEvents,
  }) });
  const boundedHistoryTurn = state.runtimes[boundedHistorySid].turns[0];
  assert.equal(boundedHistoryTurn.blocks.length, MAX_TURN_BLOCKS);
  assert.equal(boundedHistoryTurn.blocks.filter((block: {
    kind: string; item_id?: string;
  }) => block.kind === "process" && block.item_id === OMITTED_PROCESS_ITEM_ID).length, 1);
  assert.ok(boundedHistoryTurn.blocks.some((block: {
    kind: string; message_id?: string;
  }) => block.kind === "text" && block.message_id === "history-final"));

  assert.deepEqual(finalTextBlocks(richTurn.blocks).map((block: { text: string }) => block.text),
    ["已经完成。"]);
  const { presentTool, isToolFailure } = await reducerHarness.ssrLoadModule(
    "/src/tool-presentation.ts");
  assert.deepEqual(presentTool({
    kind: "tool", message_id: "m", tool_use_id: "mcp", tool: "lookup",
    input: {}, category: "mcp", title: "docs · lookup", done: false,
  }), { icon: "term", title: "docs · lookup", subtitle: "", group: "MCP" });
  assert.equal(presentTool({
    kind: "tool", message_id: "w", tool_use_id: "web", tool: "webSearch",
    input: { query: "sdk" }, category: "web_search", title: "搜索 sdk", done: false,
  }).icon, "research");
  assert.equal(isToolFailure({
    kind: "tool", message_id: "d", tool_use_id: "declined", tool: "shell",
    input: {}, done: true,
    result: { content: "", is_error: false, status: "declined" },
  }), true);

  const { ChatView } = await reducerHarness.ssrLoadModule(
    "/src/components/ChatView.tsx");
  const boundedInitialMarkup = renderToStaticMarkup(createElement(ChatView, {
    sid: "long-session",
    turns: Array.from({ length: 30 }, (_, index) => ({
      id: `long-${index}`, prompt: `prompt-${index}`, done: true, blocks: [],
    })),
    engine: "codex", onEdit: () => {}, onGetDiff: () => {},
  }));
  assert.doesNotMatch(boundedInitialMarkup, /prompt-0</,
    "switching to a long session must not synchronously render its full DOM");
  assert.match(boundedInitialMarkup, /prompt-29</);
  assert.match(boundedInitialMarkup, /virtual-thread-in/,
    "the first paint keeps only a small latest fallback before DOM measurement");
  const summaryMarkup = renderToStaticMarkup(createElement(ChatView, {
    sid: "summary-session",
    turns: [{
      id: "summary-turn", prompt: "inspect", done: true,
      blocks: [], detailEventCount: 12, detailLoaded: false,
    }],
    engine: "codex", onEdit: () => {}, onGetDiff: () => {},
    onLoadDetail: () => {},
  }));
  assert.match(summaryMarkup, /已处理/,
    "summary history must reuse the existing collapsed process affordance");
  assert.match(summaryMarkup, /12 项/,
    "the collapsed process affordance keeps the deferred detail count");
  assert.doesNotMatch(summaryMarkup, /展开完整过程/,
    "deferred detail must not add a second standalone UI control");
  const loadedDetailMarkup = renderToStaticMarkup(createElement(ChatView, {
    sid: "summary-session",
    turns: [{
      id: "summary-turn", prompt: "inspect", done: true,
      blocks: [], detailEventCount: 12, detailLoaded: true,
    }],
    engine: "codex", onEdit: () => {}, onGetDiff: () => {},
    onLoadDetail: () => {},
  }));
  assert.doesNotMatch(loadedDetailMarkup, /展开完整过程/);
  const richMarkup = renderToStaticMarkup(createElement(ChatView, {
    sid: richSid, turns: [richTurn], engine: "codex",
    onEdit: () => {}, onGetDiff: () => {},
  }));
  assert.match(richMarkup, /已处理 3s/);
  assert.match(richMarkup, /已经完成/);
  assert.doesNotMatch(richMarkup, /复制回复/,
    "reply copy keeps the original compact icon instead of adding a text action");
  assert.match(richMarkup, /class="ubub-meta ai-meta"[\s\S]*aria-label="复制"/,
    "the original reply copy icon remains in the completed-turn metadata row");
  assert.doesNotMatch(richMarkup, /先检查代码/);
  assert.doesNotMatch(richMarkup, /class="turn-working"/);

  const cachedClaudeDurationMarkup = renderToStaticMarkup(createElement(ChatView, {
    sid: "cached-claude-duration", engine: "claude",
    turns: [{
      id: "cached-claude-turn", prompt: "处理", done: true,
      ts: 1000, doneTs: 6000, durationMs: 0,
      blocks: [{
        kind: "process", item_id: "cached-tool", processKind: "hook",
        phase: "end", status: "succeeded", title: "完成", done: true,
      }],
    }],
    onEdit: () => {}, onGetDiff: () => {},
  }));
  assert.match(cachedClaudeDurationMarkup, /已处理 5s/,
    "old Claude history caches derive wall time from their timestamps");

  const cachedCodexDurationMarkup = renderToStaticMarkup(createElement(ChatView, {
    sid: "cached-codex-duration", engine: "codex",
    turns: [{
      id: "cached-codex-turn", prompt: "处理", done: true,
      ts: 1000, doneTs: 6000, durationMs: 0,
      blocks: [{
        kind: "tool", message_id: "cached-codex-message",
        tool_use_id: "cached-codex-tool", tool: "shell",
        input: {}, done: true, result: { content: "ok", is_error: false },
      }],
    }],
    onEdit: () => {}, onGetDiff: () => {},
  }));
  assert.match(cachedCodexDurationMarkup, /已处理 0s/,
    "Claude cache compatibility must not reinterpret a valid Codex duration");

  // The animated turn marker is driven by the turn lifecycle, not by an empty
  // placeholder. It must survive reasoning expansion, process activity, final
  // answer streaming, and terminal-mirrored history alike.
  const thinkingMarkup = renderToStaticMarkup(createElement(ChatView, {
    sid: richSid, turns: [{
      id: "thinking-live", prompt: "继续", done: false,
      blocks: [{ kind: "text", message_id: "thinking-text", channel: "thinking",
        text: "正在检查实现", done: false }],
    }], engine: "claude", onEdit: () => {}, onGetDiff: () => {},
  }));
  assert.match(thinkingMarkup, /class="turn-working"/);
  assert.match(thinkingMarkup, /思考中/);
  assert.match(thinkingMarkup, /正在检查实现/);

  const answerMarkup = renderToStaticMarkup(createElement(ChatView, {
    sid: richSid, turns: [{
      id: "answer-live", prompt: "继续", done: false,
      blocks: [{ kind: "text", message_id: "answer-text", channel: "final",
        text: "正在回答", done: false }],
    }], engine: "codex", onEdit: () => {}, onGetDiff: () => {},
  }));
  assert.match(answerMarkup, /class="turn-working"/);
  assert.match(answerMarkup, /回答中/);
  const { ProcessTimeline } = await reducerHarness.ssrLoadModule(
    "/src/components/ProcessTimeline.tsx");
  const declinedMarkup = renderToStaticMarkup(createElement(ProcessTimeline, {
    blocks: [{ kind: "process", item_id: "approval-denied", processKind: "hook",
      phase: "end", status: "declined", title: "Hook 已拒绝", done: false }],
    done: true,
  }));
  assert.match(declinedMarkup, /process-declined/);
  assert.match(declinedMarkup, /M18 6L6 18M6 6l12 12/);

  const codexProcessMarkup = renderToStaticMarkup(createElement(ProcessTimeline, {
    engine: "codex", done: true,
    blocks: [
      { kind: "text", message_id: "private-thought", channel: "thinking",
        text: "private reasoning must stay hidden", done: true },
      { kind: "process", item_id: "reasoning-row", processKind: "reasoning",
        phase: "end", status: "succeeded", title: "思考",
        summary: "synthetic reasoning row", done: true },
      { kind: "tool", message_id: "tool-a-message", tool_use_id: "tool-a",
        tool: "shell", input: { command: "pwd" }, done: true,
        result: { content: "/tmp", is_error: false } },
      { kind: "tool", message_id: "tool-b-message", tool_use_id: "tool-b",
        tool: "shell", input: { command: "ls" }, done: true,
        result: { content: "file", is_error: false } },
    ],
  }));
  assert.doesNotMatch(codexProcessMarkup, /private reasoning must stay hidden/);
  assert.doesNotMatch(codexProcessMarkup, /synthetic reasoning row/);
  assert.match(codexProcessMarkup, /2 个工具调用/);
  assert.doesNotMatch(codexProcessMarkup, /class="tool-group"/,
    "a completed Codex process must not render tool details until the user opens it");

  const codexHookWrappedBatchMarkup = renderToStaticMarkup(createElement(ProcessTimeline, {
    engine: "codex", done: false,
    blocks: [
      { kind: "process", item_id: "hook-a", processKind: "hook",
        phase: "end", status: "succeeded", title: "Hook · preToolUse · command",
        done: true },
      { kind: "tool", message_id: "tool-a-message", tool_use_id: "tool-a",
        tool: "shell", input: { command: "pwd" }, done: true,
        result: { content: "/tmp", is_error: false } },
      { kind: "process", item_id: "hook-b", processKind: "hook",
        phase: "end", status: "succeeded", title: "Hook · preToolUse · command",
        done: true },
      { kind: "tool", message_id: "tool-b-message", tool_use_id: "tool-b",
        tool: "shell", input: { command: "ls" }, done: true,
        result: { content: "file", is_error: false } },
    ],
  }));
  assert.match(codexHookWrappedBatchMarkup, /2 个工具调用/);
  assert.equal((codexHookWrappedBatchMarkup.match(/class="tool-group"/g) || []).length, 1,
    "successful Codex hooks must not split one tool batch into many rows");
  assert.doesNotMatch(codexHookWrappedBatchMarkup, /Hook · preToolUse/);

  const codexFailedHookMarkup = renderToStaticMarkup(createElement(ProcessTimeline, {
    engine: "codex", done: false,
    blocks: [{ kind: "process", item_id: "hook-failed", processKind: "hook",
      phase: "end", status: "failed", title: "Hook 执行失败", done: true }],
  }));
  assert.match(codexFailedHookMarkup, /Hook 执行失败/,
    "actionable Codex hook failures must remain visible");
  const { ToolGroup } = await reducerHarness.ssrLoadModule(
    "/src/components/ToolGroup.tsx");
  const collapsedToolMarkup = renderToStaticMarkup(createElement(ToolGroup, {
    tools: [
      { kind: "tool", message_id: "tool-a-message", tool_use_id: "tool-a",
        tool: "shell", input: { command: "pwd" }, done: true,
        result: { content: "/tmp", is_error: false } },
      { kind: "tool", message_id: "tool-b-message", tool_use_id: "tool-b",
        tool: "shell", input: { command: "ls" }, done: true,
        result: { content: "file", is_error: false } },
    ],
  }));
  assert.match(collapsedToolMarkup, /2 个工具调用/);
  assert.doesNotMatch(collapsedToolMarkup, /<details class="tool-group" open/,
    "Codex tool batches must stay collapsed until the user opens them");

  // A background task can be consumed after ResultMessage, when a new turn is
  // already open. Its authoritative engine turn id must route it back to the
  // old turn instead of creating a phantom third turn or attaching to the tail.
  state = reduce(state, { type: "query_sent", sid: richSid, prompt: "下一问",
    msg_id: "rich-next", ts: 34_000 });
  state = reduce(state, { type: "event", event: event({
    type: "process", sid: richSid, item_id: "late-agent", kind: "agent",
    phase: "end", status: "succeeded", turn_id: "turn-rich",
    title: "后台代理完成",
  }) });
  assert.equal(state.runtimes[richSid].turns.length, 2);
  assert.ok(state.runtimes[richSid].turns[0].blocks.some(
    (block: { kind: string; item_id?: string }) => block.kind === "process"
      && block.item_id === "late-agent"));
  assert.ok(!state.runtimes[richSid].turns[1].blocks.some(
    (block: { kind: string; item_id?: string }) => block.item_id === "late-agent"));

  // A late background update reopens only the process shell, not the completed
  // answer turn. The user can keep reading the final answer while the activity
  // header truthfully reports that work is still running.
  state = reduce(state, { type: "event", event: event({
    type: "process", sid: richSid, item_id: "late-agent", kind: "agent",
    phase: "update", status: "running", turn_id: "turn-rich",
    title: "后台代理运行中", progress: "继续检查",
  }) });
  const backgroundTurn = state.runtimes[richSid].turns[0];
  assert.equal(backgroundTurn.done, true);
  assert.equal(hasActiveProcess(backgroundTurn.blocks), true);
  const backgroundMarkup = renderToStaticMarkup(createElement(ChatView, {
    sid: richSid, turns: [backgroundTurn], engine: "codex",
    onEdit: () => {}, onGetDiff: () => {},
  }));
  assert.match(backgroundMarkup, /正在处理/);
  assert.match(backgroundMarkup, /继续检查/);
  assert.match(backgroundMarkup, /class="turn-working"/);
  assert.match(backgroundMarkup, /处理中/);
  assert.doesNotMatch(backgroundMarkup, /class="turn-done-mark"/);
  state = reduce(state, { type: "event", event: event({
    type: "process", sid: richSid, item_id: "late-agent", kind: "agent",
    phase: "end", status: "succeeded", turn_id: "turn-rich",
    title: "后台代理完成",
  }) });
  assert.equal(hasActiveProcess(state.runtimes[richSid].turns[0].blocks), false);

  const forkableTurn = {
    id: "message-a", forkPointId: "codex-turn-a", prompt: "在？",
    done: true, doneTs: 12_000,
    blocks: [{ kind: "text", message_id: "answer-a", text: "在的", done: true }],
  };
  const codexMarkup = renderToStaticMarkup(createElement(ChatView, {
    sid, turns: [forkableTurn], engine: "codex",
    onEdit: () => {}, onGetDiff: () => {}, onFork: () => {},
  }));
  assert.doesNotMatch(codexMarkup, /复制回复/);
  assert.match(codexMarkup, /class="ubub-meta ai-meta"[\s\S]*aria-label="复制"/);
  assert.match(codexMarkup, /aria-label="派生"/);
  assert.match(codexMarkup, /data-tooltip="从此回复派生新会话"/);
  assert.doesNotMatch(codexMarkup, /title="从此回复派生/);
  const assistantMeta = codexMarkup.slice(codexMarkup.indexOf('class="ubub-meta ai-meta"'));
  assert.ok(assistantMeta.indexOf('aria-label="派生"')
    > assistantMeta.indexOf('aria-label="复制"'),
  "the reply copy icon stays immediately before the fork action");
  const claudeMarkup = renderToStaticMarkup(createElement(ChatView, {
    sid, turns: [forkableTurn], engine: "claude",
    onEdit: () => {}, onGetDiff: () => {}, onFork: () => {},
  }));
  assert.match(claudeMarkup, /aria-label="派生"/);
  assert.match(claudeMarkup, /data-tooltip="从此回复派生新会话"/);
  const noForkPointMarkup = renderToStaticMarkup(createElement(ChatView, {
    sid, turns: [{ ...forkableTurn, forkPointId: undefined }], engine: "claude",
    onEdit: () => {}, onGetDiff: () => {}, onFork: () => {},
  }));
  assert.doesNotMatch(noForkPointMarkup, /aria-label="派生"/);

  state = reduce(state, { type: "event", event: event({
    type: "takeover_state", sid, pending: true, message: "等待当前回复结束",
  }) });
  assert.equal(state.runtimes[sid].takeoverPending, true);
  assert.equal(state.runtimes[sid].takeoverMessage, "等待当前回复结束");
  state = reduce(state, { type: "event", event: event({
    type: "takeover_state", sid, pending: false,
  }) });
  assert.equal(state.runtimes[sid].takeoverPending, false);
  assert.equal(state.runtimes[sid].takeoverMessage, null);

  // v15 control is authoritative and independently revisioned. Older and
  // equal-revision conflicting frames must never revive a completed lock.
  const controlSid = "revisioned-session-control";
  const control = (
    revision: number,
    control_mode: "remote" | "codex_shared" | "claude_broker" | "external_cli" | "agent_view" | "desktop",
    write_state: "writable" | "read_only" | "takeover_pending" | "input_busy",
    extra: Record<string, unknown> = {},
  ): SessionControl => event({
    type: "session_control", sid: controlSid, revision, control_mode,
    write_state, terminal_attached: false,
    generation: "control-generation", ...extra,
  }) as SessionControl;
  let controlState = reduce({
    ...initialState, focusedSid: controlSid,
    runtimes: { [controlSid]: createRuntime() },
  }, { type: "event", event: control(
    5, "external_cli", "read_only", { can_takeover: true }) });
  assert.equal(controlState.runtimes[controlSid].control?.revision, 5);
  assert.equal(controlState.runtimes[controlSid].external, true);

  controlState = reduce(controlState, { type: "event", event: control(
    4, "remote", "writable") });
  controlState = reduce(controlState, { type: "event", event: control(
    5, "desktop", "read_only") });
  assert.equal(controlState.runtimes[controlSid].control?.control_mode,
    "external_cli", "older/conflicting revisions must be rejected");
  controlState = reduce(controlState, { type: "event", event: event({
    type: "takeover_state", sid: controlSid, pending: true,
    message: "legacy frame arrived late",
  }) });
  assert.equal(controlState.runtimes[controlSid].takeoverPending, false,
    "legacy TakeoverState must be ignored after v15 control exists");

  controlState = reduce(controlState, { type: "event", event: event({
    type: "history", sid: controlSid, session_id: controlSid,
    revision: "control-history", generation: "control-generation",
    build_seq: 2, has_more: false, events: [],
    control: control(6, "remote", "writable"),
  }) });
  assert.equal(controlState.runtimes[controlSid].external, false);
  // This narrative page is stale (build 1 < build 2), but its separately
  // revisioned control value is newer and must still be installed.
  controlState = reduce(controlState, { type: "event", event: event({
    type: "history", sid: controlSid, session_id: controlSid,
    revision: "control-history", generation: "control-generation",
    build_seq: 1, has_more: false, events: [],
    control: control(7, "desktop", "read_only"),
  }) });
  assert.equal(controlState.runtimes[controlSid].control?.revision, 7);
  assert.equal(controlState.runtimes[controlSid].external, true);

  controlState = reduce(controlState, { type: "event", event: control(
    8, "remote", "writable") });
  controlState = reduce(controlState, { type: "event", event: event({
    type: "session_control", revision: 999,
    control_mode: "desktop", write_state: "read_only",
    terminal_attached: true, generation: "control-generation",
  }) });
  assert.equal(controlState.runtimes[controlSid].control?.revision, 8,
    "an unrouted direct control frame cannot target the focused runtime");
  controlState = reduce(controlState, { type: "event", event: event({
    type: "history", sid: controlSid, session_id: controlSid,
    revision: "control-history", generation: "control-generation",
    build_seq: 3, live_seq: 0, has_more: false, events: [],
    external: true, takeover_pending: true,
    control: control(7, "external_cli", "takeover_pending"),
  }) });
  assert.equal(controlState.runtimes[controlSid].control?.revision, 8);
  assert.equal(controlState.runtimes[controlSid].external, false,
    "stale modern plus legacy ownership fields cannot resurrect the lock");
  assert.equal(controlState.runtimes[controlSid].takeoverPending, false);

  controlState = reduce(controlState, { type: "event", event: event({
    type: "snapshot", sid: controlSid, cc_session_id: controlSid,
    state: "idle", tail_text: "",
    control: control(9, "codex_shared", "writable", {
      terminal_attached: true,
    }),
  }) });
  assert.equal(controlState.runtimes[controlSid].control?.control_mode,
    "codex_shared");
  assert.equal(controlState.runtimes[controlSid].external, false,
    "a terminal-attached shared Codex session remains writable");

  const crossSessionControl = control(
    100, "external_cli", "read_only", { sid: "another-session" });
  assert.equal(controlForCachedSession(controlSid, crossSessionControl), undefined,
    "a cache row must drop a control explicitly routed to another session");
  controlState = reduce(controlState, { type: "event", event: event({
    type: "snapshot", sid: controlSid, cc_session_id: controlSid,
    state: "idle", tail_text: "", generation: "control-generation",
    control: crossSessionControl,
  }) });
  assert.equal(controlState.runtimes[controlSid].control?.revision, 9,
    "a Snapshot must not install another session's nested control");
  controlState = reduce(controlState, { type: "event", event: event({
    type: "history", sid: controlSid, session_id: controlSid,
    revision: "cross-session-control-history",
    generation: "control-generation", build_seq: 4,
    has_more: false, events: [], control: crossSessionControl,
  }) });
  assert.equal(controlState.runtimes[controlSid].control?.revision, 9,
    "History must not install another session's nested control");

  let cachedControlState = reduce({
    ...initialState, focusedSid: "cached-control",
  }, {
    type: "hydrate_cache", sid: "cached-control", turns: [], revision: null,
    control: event({
      type: "session_control", sid: "cached-control", revision: 20,
      control_mode: "remote", write_state: "writable",
      terminal_attached: false, generation: "cached-generation-old",
    }) as SessionControl,
    generation: "cached-generation-old",
  });
  cachedControlState = reduce(cachedControlState, {
    type: "event", event: event({
      type: "snapshot", sid: "cached-control", cc_session_id: "cached-control",
      state: "idle", tail_text: "", generation: "cached-generation-old",
      control: event({
        type: "session_control", sid: "cached-control", revision: 19,
        control_mode: "desktop", write_state: "read_only",
        terminal_attached: true, generation: "cached-generation-old",
      }),
    }),
  });
  assert.equal(cachedControlState.runtimes["cached-control"].control?.revision, 20);
  assert.equal(cachedControlState.runtimes["cached-control"].external, false,
    "an older reconnect snapshot cannot revive a cache-superseded lock");
  const mismatchedCachedControl = reduce(initialState, {
    type: "hydrate_cache", sid: "mismatched-cache", turns: [], revision: null,
    generation: "trusted-cache-generation",
    control: event({
      type: "session_control", sid: "mismatched-cache", revision: 99,
      control_mode: "desktop", write_state: "read_only",
      terminal_attached: true, generation: "stale-cache-generation",
    }) as SessionControl,
  });
  assert.equal(mismatchedCachedControl.runtimes["mismatched-cache"].control, null,
    "a control snapshot cannot override its cache row's trusted generation");
  const crossSidCachedControl = reduce(initialState, {
    type: "hydrate_cache", sid: "cache-row-a", turns: [], revision: null,
    control: event({
      type: "session_control", sid: "cache-row-b", revision: 100,
      control_mode: "external_cli", write_state: "read_only",
      terminal_attached: true, generation: "cross-cache-generation",
    }) as SessionControl,
  });
  assert.equal(crossSidCachedControl.runtimes["cache-row-a"].control, null);
  assert.equal(crossSidCachedControl.runtimes["cache-row-a"].controlGeneration, null,
    "a mismatched cache control must not bind its generation to another row");
  cachedControlState = reduce(cachedControlState, {
    type: "event", event: event({
      type: "snapshot", sid: "cached-control", cc_session_id: "cached-control",
      state: "idle", tail_text: "", generation: "cached-generation-new",
      control: event({
        type: "session_control", sid: "cached-control", revision: 0,
        control_mode: "remote", write_state: "writable",
        terminal_attached: false, generation: "cached-generation-new",
      }),
    }),
  });
  assert.equal(cachedControlState.runtimes["cached-control"].control?.revision, 0,
    "a trusted outer generation switch starts a fresh revision epoch");
  cachedControlState = reduce(cachedControlState, {
    type: "event", event: event({
      type: "session_control", sid: "cached-control", revision: 999,
      control_mode: "desktop", write_state: "read_only",
      terminal_attached: true, generation: "cached-generation-old",
    }),
  });
  assert.equal(cachedControlState.runtimes["cached-control"].control?.control_mode,
    "remote", "a high revision from the previous generation must be rejected");

  const generationlessSid = "generationless-control-migration";
  let generationlessState = reduce({
    ...initialState, focusedSid: generationlessSid,
    runtimes: { [generationlessSid]: createRuntime() },
  }, { type: "event", event: event({
    type: "session_control", sid: generationlessSid, revision: 1,
    control_mode: "external_cli", write_state: "read_only",
    terminal_attached: true,
  }) });
  assert.equal(generationlessState.runtimes[generationlessSid].control?.revision, 1,
    "a generation-less control remains compatible with a generation-less runtime");
  generationlessState = reduce(generationlessState, {
    type: "event", event: event({
      type: "snapshot", sid: generationlessSid, cc_session_id: generationlessSid,
      state: "idle", tail_text: "", generation: "migration-generation",
      control: event({
        type: "session_control", sid: generationlessSid, revision: 0,
        control_mode: "remote", write_state: "writable",
        terminal_attached: false, generation: "migration-generation",
      }),
    }),
  });
  generationlessState = reduce(generationlessState, {
    type: "event", event: event({
      type: "session_control", sid: generationlessSid, revision: 999,
      control_mode: "desktop", write_state: "read_only",
      terminal_attached: true,
    }),
  });
  assert.equal(generationlessState.runtimes[generationlessSid].control?.control_mode,
    "remote", "generation-less control cannot overwrite a bound runtime epoch");

  const compatibilityGapSid = "revisioned-control-generation-gap";
  let compatibilityGapState = reduce({
    ...initialState, focusedSid: compatibilityGapSid,
    runtimes: { [compatibilityGapSid]: createRuntime() },
  }, { type: "event", event: event({
    type: "session_control", sid: compatibilityGapSid, revision: 8,
    control_mode: "remote", write_state: "writable",
    terminal_attached: false, generation: "gap-generation-old",
  }) });
  compatibilityGapState = reduce(compatibilityGapState, {
    type: "event", event: event({
      type: "replay_start", sid: compatibilityGapSid,
      from_seq: 0, to_seq: 0, truncated: false,
      generation: "gap-generation-new",
    }),
  });
  assert.equal(compatibilityGapState.runtimes[compatibilityGapSid].control, null);
  compatibilityGapState = reduce(compatibilityGapState, {
    type: "event", event: event({
      type: "takeover_state", sid: compatibilityGapSid, pending: true,
      message: "late legacy lock",
    }),
  });
  assert.equal(
    compatibilityGapState.runtimes[compatibilityGapSid].takeoverPending, false,
    "legacy takeover cannot revive a lock while a new revision epoch is seeding",
  );
  compatibilityGapState = reduce(compatibilityGapState, {
    type: "event", event: event({
      type: "history", sid: compatibilityGapSid,
      session_id: compatibilityGapSid, revision: "gap-history",
      generation: "gap-generation-new", build_seq: 1,
      has_more: false, events: [], external: true, takeover_pending: true,
    }),
  });
  assert.equal(compatibilityGapState.runtimes[compatibilityGapSid].external, false,
    "legacy History ownership cannot regain authority after revisioned control");

  let legacyControlState = reduce({
    ...initialState, focusedSid: "legacy-control",
    runtimes: { "legacy-control": createRuntime() },
  }, { type: "event", event: event({
    type: "takeover_state", sid: "legacy-control", pending: true,
    message: "等待旧式接管",
  }) });
  assert.equal(legacyControlState.runtimes["legacy-control"].takeoverPending, true);
  legacyControlState = reduce(legacyControlState, { type: "event", event: event({
    type: "takeover_state", sid: "legacy-control", pending: false,
  }) });
  assert.equal(legacyControlState.runtimes["legacy-control"].takeoverPending, false);

  const { presentLegacyExternalControl, presentSessionControl } = await reducerHarness.ssrLoadModule(
    "/src/session-control-ui.ts");
  const sharedControl = presentSessionControl(control(
    30, "codex_shared", "writable", { terminal_attached: true }));
  assert.equal(sharedControl.locked, false);
  assert.equal(sharedControl.title, "终端双向连接");
  assert.equal(sharedControl.tone, "attached");
  assert.match(sharedControl.detail, /浏览器与终端共享同一会话/);
  assert.doesNotMatch(sharedControl.detail, /占用/);
  const passiveSharedControl = presentSessionControl(control(
    31, "codex_shared", "writable", { terminal_attached: false }));
  assert.equal(passiveSharedControl.tone, "remote",
    "an available daemon stays visually neutral until this session has a terminal");
  assert.equal(passiveSharedControl.title, "Codex 后台通道可用");
  assert.equal(passiveSharedControl.backend, "可用");
  assert.equal(passiveSharedControl.terminal, "未检测到本机终端",
    "a passive Codex daemon is shown as backend topology, not terminal ownership");
  assert.doesNotMatch(passiveSharedControl.title, /已连接/,
    "a live shared daemon must not be presented as a live terminal connection");
  const reconnectingSharedControl = presentSessionControl(control(
    32, "codex_shared", "writable", {
      terminal_attached: true,
      reason: "Codex 共享通道连接断开；下次操作会自动重试",
    }));
  assert.equal(reconnectingSharedControl.locked, false,
    "an interrupted Codex proxy remains writable instead of becoming an external lock");
  assert.equal(reconnectingSharedControl.disconnected, true);
  assert.equal(reconnectingSharedControl.action, undefined);
  assert.doesNotMatch(reconnectingSharedControl.remote, /只读/);
  const passiveBrokerControl = presentSessionControl(control(
    33, "claude_broker", "writable", { terminal_attached: false }));
  assert.equal(passiveBrokerControl.tone, "remote",
    "Claude and Codex use the same neutral state without an attached terminal");
  assert.equal(passiveBrokerControl.title, "Claude Broker 通道可用");
  assert.equal(passiveBrokerControl.terminal, "未检测到本机终端");
  const attachedBrokerControl = presentSessionControl(control(
    34, "claude_broker", "writable", { terminal_attached: true }));
  assert.equal(attachedBrokerControl.tone, "attached");
  assert.match(attachedBrokerControl.detail, /浏览器与终端共享同一会话/);
  const externalControl = presentSessionControl(control(
    35, "external_cli", "read_only", { can_takeover: true }));
  assert.equal(externalControl.locked, true);
  assert.equal(externalControl.action, "迁移");
  assert.equal(externalControl.tone, "attention");
  assert.match(externalControl.detail, /只读.*接管/);
  const desktopControl = presentSessionControl(control(
    36, "desktop", "writable", { can_takeover: true }));
  assert.equal(desktopControl.locked, true,
    "desktop remains fail-closed even if a producer says writable");
  assert.equal(desktopControl.action, undefined);
  assert.equal(desktopControl.remote, "只读");
  assert.equal(desktopControl.title, "Codex App 使用中 · Web 只读");
  assert.equal(desktopControl.placeholder, "Codex App 使用中 — Web 只读");
  assert.equal(desktopControl.connection, "Codex App 私有 app-server");
  assert.equal(desktopControl.terminal, "Codex App 使用中");
  const exitedBrokerControl = presentSessionControl(control(
    37, "claude_broker", "input_busy", {
      terminal_attached: false,
      reason: "session_exited: Claude broker 已断开",
    }));
  assert.equal(exitedBrokerControl.tone, "disconnected");
  assert.equal(exitedBrokerControl.title, "终端连接已断开");
  assert.equal(exitedBrokerControl.remote, "等待恢复");
  assert.doesNotMatch(exitedBrokerControl.placeholder ?? "", /输入.*忙碌/,
    "a dead broker must not be presented as a merely busy input channel");
  const legacyExternalControl = presentLegacyExternalControl(
    "claude", false, "旧版外部终端");
  assert.equal(legacyExternalControl.locked, true);
  assert.equal(legacyExternalControl.action, "迁移");
  assert.match(legacyExternalControl.detail, /旧版外部终端/);

  const { TerminalControl } = await reducerHarness.ssrLoadModule(
    "/src/components/TerminalControl.tsx");
  const terminalControlMarkup = renderToStaticMarkup(createElement(TerminalControl, {
    control: control(37, "codex_shared", "writable", { terminal_attached: false }),
    engine: "codex",
    availability: "online",
  }));
  assert.match(terminalControlMarkup, /终端状态：Codex 后台通道可用/);
  assert.match(terminalControlMarkup, /tone-remote/);
  assert.doesNotMatch(terminalControlMarkup, /tone-attached/,
    "a passive Codex app-server must not use the connected accent colour");
  assert.doesNotMatch(terminalControlMarkup, /terminal-control-card/,
    "the status card stays closed until the terminal icon is clicked");
  const staleTerminalControlMarkup = renderToStaticMarkup(createElement(TerminalControl, {
    control: control(38, "claude_broker", "writable", { terminal_attached: true }),
    engine: "claude",
    availability: "offline",
  }));
  assert.match(staleTerminalControlMarkup, /终端状态：连接中断/);
  assert.match(staleTerminalControlMarkup, /tone-disconnected/);
  assert.doesNotMatch(staleTerminalControlMarkup, /tone-attached/,
    "a cached attached state must never render green while transport is offline");
  const legacyTerminalControlMarkup = renderToStaticMarkup(createElement(TerminalControl, {
    engine: "claude",
    availability: "online",
    legacyExternal: true,
  }));
  assert.match(legacyTerminalControlMarkup, /终端状态：外部 CLI 只读镜像/,
    "pre-v15 external state retains a visible terminal entry point");

  const progressSid = "progress";
  state = {
    ...state,
    focusedSid: progressSid,
    runtimes: {
      ...state.runtimes,
      [progressSid]: { ...createRuntime(), state: "running", syncReady: true },
    },
  };
  state = reduce(state, {
    type: "query_sent", sid: progressSid, prompt: "status",
    msg_id: "progress-turn", ts: 20_000,
  });
  state = reduce(state, { type: "event", event: event({
    type: "state", sid: progressSid, msg_id: "progress-turn",
    state: "running", detail: "上游服务暂时不可用（503），Codex 正在重试…",
    phase: "retrying",
  }) });
  assert.equal(state.runtimes[progressSid].turns[0].done, false);
  assert.match(state.runtimes[progressSid].turns[0].progress ?? "", /503/);
  state = reduce(state, { type: "event", event: event({
    type: "state", sid: progressSid, msg_id: "progress-turn",
    state: "running", detail: null, phase: null,
  }) });
  assert.equal(state.runtimes[progressSid].turns[0].done, false);
  assert.equal(state.runtimes[progressSid].turns[0].progress, undefined);
  state = reduce(state, { type: "event", event: event({
    type: "state", sid: progressSid, msg_id: "progress-turn",
    state: "running", detail: "上游服务暂时不可用（503），Codex 正在重试…",
    phase: "retrying",
  }) });
  state = reduce(state, { type: "event", event: event({
    type: "error", sid: progressSid, msg_id: "progress-turn",
    code: "cc_crash", message: "Codex 没有返回任何内容",
  }) });
  assert.equal(state.runtimes[progressSid].turns[0].done, true);
  assert.equal(state.runtimes[progressSid].turns[0].progress, undefined);
  assert.equal(state.runtimes[progressSid].turns[0].error,
    "本次回复未完成，请重试。");
  state = reduce(state, { type: "event", event: event({
    type: "turn_end", sid: progressSid, ts: 21,
    result: { subtype: "error", duration_ms: 237252, is_error: true },
  }) });
  assert.equal(state.runtimes[progressSid].state, "running",
    "TurnEnd closes presentation only; it must not unlock before State(idle)");
  state = reduce(state, {
    type: "set_pending", query: { prompt: "send after interrupt drain" },
  });
  assert.deepEqual(selectDrainCandidates(
    state.runtimes, new Set(), true, true), [],
  "pending work must stay blocked while the wrapper is still settling");
  state = reduce(state, { type: "event", event: event({
    type: "state", sid: progressSid, state: "idle",
  }) });
  assert.deepEqual(selectDrainCandidates(
    state.runtimes, new Set(), true, true).map(({ sid, source }) => ({ sid, source })),
  [{ sid: progressSid, source: "pending" }],
  "the authoritative idle lifecycle frame releases the pending replacement");

  const { CommandSheet } = await reducerHarness.ssrLoadModule(
    "/src/components/CommandSheet.tsx");
  const picked: string[] = [];
  const tree = CommandSheet({
    open: true, kind: "models", engine: "codex", onClose: () => {},
    onPickModel: (model: string) => { picked.push(model); },
  });
  const clickModelButtons = (node: unknown): void => {
    if (Array.isArray(node)) {
      node.forEach(clickModelButtons);
      return;
    }
    if (!node || typeof node !== "object") return;
    const element = node as {
      type?: unknown; key?: string | null;
      props?: { onClick?: () => void; children?: unknown };
    };
    if (element.type === "button"
        && ["gpt-5.6-terra", "gpt-5.6-luna"].includes(element.key ?? "")) {
      element.props?.onClick?.();
    }
    clickModelButtons(element.props?.children);
  };
  clickModelButtons(tree);
  assert.deepEqual(picked, ["gpt-5.6-terra", "gpt-5.6-luna"]);
} finally {
  await reducerHarness.close();
}

const fakePng = new Uint8Array(24);
fakePng.set([0x00, ...new TextEncoder().encode("PNG\r\n\x1a\n")], 0);
fakePng.set(new TextEncoder().encode("IHDR"), 12);
new DataView(fakePng.buffer).setUint32(16, 1);
new DataView(fakePng.buffer).setUint32(20, 1);
assert.equal(imageDimensions(fakePng, "image/png"), null);

// Keep the public RelayWs return contract checked without instantiating a
// browser WebSocket in this zero-network Node test.
const btwWs: Pick<RelayWs, "sendOpenBtw"> = {
  sendOpenBtw: (_parentSid, requestId = "generated-request") => requestId,
};
const btwRequestId = btwWs.sendOpenBtw("parent-1", "btw-request-1");
assert.equal(btwRequestId, "btw-request-1");
const openFrame = makeOpenBtwCommand("parent-1", btwRequestId, 123);
assert.equal(openFrame.type, "open_btw");
assert.equal(openFrame.sid, "parent-1");
assert.equal(openFrame.request_id, btwRequestId);
assert.equal(openFrame.ts, 123);

assert.equal(matchesBtwRequest("btw-request-1", "btw-request-1"), true);
assert.equal(matchesBtwRequest("btw-request-new", "btw-request-old"), false);
assert.equal(matchesBtwRequest(null, "btw-request-old"), false);
assert.equal(classifyBtwOpened(
  "btw-request-1", null,
  { request_id: "btw-request-1", btw_sid: "btw-1" }), "accept");
assert.equal(classifyBtwOpened(
  null, { requestId: "btw-request-1", sid: "btw-1" },
  { request_id: "btw-request-1", btw_sid: "btw-1" }), "duplicate");
assert.equal(classifyBtwOpened(
  "btw-request-new", null,
  { request_id: "btw-request-old", btw_sid: "btw-old" }), "stale");
const discardedBtwSids = new Set(["btw-stale"]);
assert.equal(consumeDiscardedBtwSnapshot(
  discardedBtwSids, { sid: "normal-session" }), false);
assert.equal(discardedBtwSids.has("btw-stale"), true);
assert.equal(consumeDiscardedBtwSnapshot(
  discardedBtwSids, { sid: "btw-stale" }), true);
assert.equal(discardedBtwSids.size, 0);

const boundedCache = boundCachedTurns(Array.from(
  { length: 120 }, (_, id) => ({ id, prompt: `turn-${id}` })));
assert.equal(boundedCache.length, 100);
assert.equal((boundedCache[0] as { id: number }).id, 20);
const skipsOneOversizedCacheTurn = boundCachedTurns([
  { id: "small", prompt: "keep" },
  { id: "huge", image: "x".repeat(2 * 1024 * 1024 + 1) },
]);
assert.deepEqual(skipsOneOversizedCacheTurn, [{ id: "small", prompt: "keep" }]);
const stripsFileBodiesFromCache = boundCachedTurns([{
  id: "file-turn",
  prompt: "upload",
  files: [{ filename: "secret.txt", data: "do-not-persist", extra: "drop-me" }],
}]);
assert.deepEqual(stripsFileBodiesFromCache, [{
  id: "file-turn",
  prompt: "upload",
  files: [{ filename: "secret.txt", data: "" }],
}]);

const boundedRuntimeTurns = boundRuntimeTurns(Array.from(
  { length: 5 }, (_, id) => ({ id: `turn-${id}`, prompt: "x", blocks: [], done: true })),
3, 10_000);
assert.deepEqual(boundedRuntimeTurns.map((turn) => turn.id), ["turn-2", "turn-3", "turn-4"]);
const retainedHistoryPrepend = boundRuntimeTurns([
  ...Array.from({ length: 12 }, (_, id) => ({
    id: `older-${id}`, prompt: "older", blocks: [], done: true,
  })),
  ...Array.from({ length: 200 }, (_, id) => ({
    id: `current-${id}`, prompt: "current", blocks: [], done: true,
  })),
]);
assert.equal(retainedHistoryPrepend[0]?.id, "older-0");
assert.equal(retainedHistoryPrepend.length, 212,
  "the former 200-turn DOM limit must not discard an accepted history page");
const cappedHistoryPrepend = boundRuntimeTurns([
  ...Array.from({ length: 12 }, (_, id) => ({
    id: `older-${id}`, prompt: "older", blocks: [], done: true,
  })),
  ...Array.from({ length: MAX_RUNTIME_TURNS }, (_, id) => ({
    id: `current-${id}`, prompt: "current", blocks: [], done: true,
  })),
]);
assert.equal(cappedHistoryPrepend[0]?.id, "current-0");
assert.equal(cappedHistoryPrepend.some((turn) => turn.id.startsWith("older-")), false,
  "the canonical data window remains bounded independently from the virtual DOM");
const activeTurn = { id: "active", prompt: "running", blocks: [], done: false };
const keepsActiveRuntimeTurns = boundRuntimeTurns([
  { id: "old", prompt: "old", blocks: [], done: true },
  activeTurn,
  { id: "newest-done", prompt: "new", blocks: [], done: true },
], 2, 10_000);
assert.deepEqual(keepsActiveRuntimeTurns.map((turn) => turn.id), ["active", "newest-done"]);
const keepsNewestOversizedTurn = boundRuntimeTurns([
  { id: "older", prompt: "a".repeat(100), blocks: [], done: true },
  { id: "newer", prompt: "b".repeat(100), blocks: [], done: true },
], 10, 10);
assert.deepEqual(keepsNewestOversizedTurn.map((turn) => turn.id), ["newer"]);
const completedAnswerWithBackgroundWork = {
  id: "background-active", prompt: "done", done: true,
  blocks: [{ kind: "process" as const, item_id: "agent-1", title: "代理",
    done: false }],
};
const keepsCompletedAnswerWithBackgroundWork = boundRuntimeTurns([
  completedAnswerWithBackgroundWork,
  { id: "newer-complete", prompt: "new", blocks: [], done: true },
], 1, 10_000);
assert.deepEqual(keepsCompletedAnswerWithBackgroundWork.map((turn) => turn.id), [
  "background-active",
]);

const idleRuntime = () => ({
  state: "idle", syncReady: true, replaying: false,
  turns: [] as Array<{ done: boolean }>, queue: [] as unknown[],
  pendingSend: null, pendingQuestion: null,
});
const prunedRuntimes = pruneRuntimeMap({
  protected: idleRuntime(), oldestIdle: idleRuntime(), newestIdle: idleRuntime(),
}, new Set(["protected"]), 2);
assert.deepEqual(Object.keys(prunedRuntimes), ["protected", "newestIdle"]);
const activeRuntime = { ...idleRuntime(), state: "running" };
const keepsConfirmedActive = pruneRuntimeMap({
  protected: idleRuntime(), active: activeRuntime,
}, new Set(["protected"]), 1);
assert.deepEqual(Object.keys(keepsConfirmedActive), ["protected", "active"]);
const backgroundRuntime = {
  ...idleRuntime(), turns: [completedAnswerWithBackgroundWork],
};
const keepsBackgroundRuntime = pruneRuntimeMap({
  protected: idleRuntime(), background: backgroundRuntime,
}, new Set(["protected"]), 1);
assert.deepEqual(Object.keys(keepsBackgroundRuntime), ["protected", "background"]);
const staleActive = { ...activeRuntime, syncReady: false };
const dropsUnconfirmedOldGeneration = pruneRuntimeMap({
  protected: idleRuntime(), stale: staleActive,
}, new Set(["protected"]), 1);
assert.deepEqual(Object.keys(dropsUnconfirmedOldGeneration), ["protected"]);

// Exercise the actual WebSocket reducer boundary: live delivery can race ahead
// of Hello replay, and a cached command response can arrive after newer state.
class FakeWebSocket {
  static readonly OPEN = 1;
  static readonly instances: FakeWebSocket[] = [];

  readonly sent: string[] = [];
  readyState = FakeWebSocket.OPEN;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onclose: ((event: { code: number }) => void) | null = null;
  onerror: (() => void) | null = null;

  constructor(readonly url: string) {
    FakeWebSocket.instances.push(this);
  }

  send(raw: string): void {
    this.sent.push(raw);
  }

  close(): void {
    this.readyState = 3;
  }

  receive(frame: Record<string, unknown>): void {
    this.onmessage?.({ data: JSON.stringify({ v: 19, ts: 1, ...frame }) });
  }
}

Object.assign(globalThis, {
  window: {
    location: { protocol: "http:", host: "relay.test", reload: () => {} },
  },
  WebSocket: FakeWebSocket,
});

const observed: ServerEvent[] = [];
let wrapperGenerationChanges = 0;
const relay = new RelayWs({
  onEvent: (event) => { observed.push(event); },
  onConnState: () => {},
  onWrapperGenerationChanged: () => { wrapperGenerationChanges += 1; },
});
relay.start();
const socket = FakeWebSocket.instances.at(-1);
assert.ok(socket);
socket.onopen?.();

// Code/Work is a hard focus boundary. Background snapshots and delayed focus
// confirmations from the previous surface must not retarget later commands.
relay.setSessionEngines([
  { session_id: "surface-code", engine: "claude", space: "code" },
  { session_id: "surface-work", engine: "claude", space: "work" },
]);
relay.setFocusedSid("surface-code", "claude", "code");
relay.setSurface("claude", "work");
socket.receive({
  type: "snapshot", sid: "surface-code", cc_session_id: "surface-code",
  state: "idle", tail_text: "",
});
relay.sendGetContext();
assert.equal("sid" in JSON.parse(socket.sent.at(-1) ?? "{}"), false);
const focusEventsBefore = observed.filter((event) => event.type === "session_focus").length;
socket.receive({ type: "session_focus", session_id: "surface-code" });
assert.equal(observed.filter((event) => event.type === "session_focus").length,
  focusEventsBefore);
relay.sendGetContext();
assert.equal("sid" in JSON.parse(socket.sent.at(-1) ?? "{}"), false);
socket.receive({ type: "session_focus", session_id: "surface-work" });
relay.sendGetContext();
assert.equal(JSON.parse(socket.sent.at(-1) ?? "{}").sid, "surface-work");
relay.setSurface("codex", "code");

relay.sendGetWorkArtifacts("claude", "surface-work");
const artifactsFrame = JSON.parse(socket.sent.at(-1) ?? "{}");
assert.equal(artifactsFrame.type, "get_work_artifacts");
assert.equal(artifactsFrame.engine, "claude");
assert.equal(artifactsFrame.session_id, "surface-work");
assert.equal(typeof artifactsFrame.client_id, "string");

const codexModels = modelsFor("codex");
assert.equal(clientSlashesFor("codex").has("plan"), true);
assert.equal(clientSlashesFor("codex").has("normal"), true);
assert.equal(commandsFor("codex").some((command) => (
  "slash" in command && command.slash === "plan"
)), true);
assert.deepEqual(matchCommands("pla", "codex").map((command) => command.slash), ["plan"]);
for (const id of ["gpt-5.6-terra", "gpt-5.6-luna"]) {
  const selected = codexModels.find((model) => model.id === id);
  assert.ok(selected);
  relay.setFocusedSid("codex-model-session", "codex");
  relay.sendSetModel(selected.id);
  const frame = JSON.parse(socket.sent.at(-1) ?? "{}");
  assert.equal(frame.type, "set_model");
  assert.equal(frame.sid, "codex-model-session");
  assert.equal(frame.model, id);
}

relay.setFocusedSid("codex-plan-session", "codex");
relay.sendSetCollaborationMode("plan");
const collaborationFrame = JSON.parse(socket.sent.at(-1) ?? "{}");
assert.equal(collaborationFrame.type, "set_collaboration_mode");
assert.equal(collaborationFrame.sid, "codex-plan-session");
assert.equal(collaborationFrame.mode, "plan");
assert.equal(typeof collaborationFrame.cmd_id, "string");
assert.equal(typeof collaborationFrame.client_id, "string");

relay.sendNewSession(
  "/tmp/project", "codex", "gpt-5.6-sol", "xhigh",
  { prompt: "先制定计划", msg_id: "first-plan-message" }, "plan",
  "on-request", "fast",
);
const newPlanFrame = JSON.parse(socket.sent.at(-1) ?? "{}");
assert.equal(newPlanFrame.type, "new_session");
assert.equal(newPlanFrame.engine, "codex");
assert.equal(newPlanFrame.collaboration_mode, "plan");
assert.equal(newPlanFrame.permission_mode, "on-request");
assert.equal(newPlanFrame.service_tier, "fast");
assert.equal(newPlanFrame.prompt, "先制定计划");

relay.sendNewSession(
  "/tmp/project", "claude", null, null,
  { prompt: "使用本机默认设置", msg_id: "default-settings-message" },
);
const defaultSessionFrame = JSON.parse(socket.sent.at(-1) ?? "{}");
assert.equal(defaultSessionFrame.type, "new_session");
assert.equal("model" in defaultSessionFrame, false);
assert.equal("effort" in defaultSessionFrame, false);

relay.sendGetModels("claude", "/tmp/project");
const claudeDefaultsFrame = JSON.parse(socket.sent.at(-1) ?? "{}");
assert.equal(claudeDefaultsFrame.type, "get_models");
assert.equal(claudeDefaultsFrame.engine, "claude");
assert.equal(claudeDefaultsFrame.cwd, "/tmp/project");
assert.equal(typeof claudeDefaultsFrame.cmd_id, "string");
assert.equal(typeof claudeDefaultsFrame.client_id, "string");

const appSource = readFileSync(resolve(process.cwd(), "src/App.tsx"), "utf8");
for (const optimisticAction of ["set_model", "set_effort", "set_perm", "set_collaboration_mode"]) {
  assert.doesNotMatch(appSource, new RegExp(`dispatch\\(\\{ type: ["']${optimisticAction}["']`));
}
assert.match(appSource, /const \{ cwd, cwdSource, model, effort \} = state\.newChat/);
assert.match(appSource, /data-lock-horizontal-swipe/);
assert.match(appSource, /surface=\{space\}/);
assert.match(appSource, /draftKey=\{focusedComposerDraftKey\}/);
assert.match(appSource, /composerDraftsRef\.current\.rekey/,
  "temp session id capture must retain the focused composer draft");
assert.match(appSource, /\{space === "work" \? "Work" : "Code"\}/);
assert.match(appSource, /<button className="engine-toggle" onClick=\{toggleEngine\}/);
assert.match(appSource, /setNewChatAutoFocus\(false\)/,
  "switching engines must not summon the new-chat keyboard");
assert.match(appSource, /prepareSurfaceSwitch\(nextEngine, space\)/,
  "engine switches must restore their own remembered surface session");
assert.match(appSource, /prepareSurfaceSwitch\(engine, next\)/,
  "Work/Code switches must share the remembered-session restoration path");
assert.match(appSource, /if \(latest && latest\.session_id !== state\.focusedSid\) \{\s*dispatch\(\{ type: "exit_new_chat" \}\)/,
  "restored focus must replace the temporary new-session page");
assert.match(appSource, /aria-label="退出登录" title="退出登录"><Icon name="logout"/);
assert.match(appSource, /rt\.replaying \|\| !rt\.syncReady \? "syncing" : "online"/,
  "cached terminal state must be downgraded until the focused session is authoritative");
assert.match(appSource, /legacyExternal=\{!rt\.control && !!rt\.external\}/,
  "rolling-deploy compatibility keeps legacy external ownership actionable");
assert.match(appSource, /sessionControlLocksInput\(rt\.control\)/,
  "Shift+Tab must not mutate controls while the authoritative session is read-only");
assert.doesNotMatch(appSource, /className="work-artifacts-btn"/);
assert.doesNotMatch(appSource, /className="work-head-manage"/);
assert.doesNotMatch(appSource, /sendSetWorkGrant|目录授权/);
const sidebarSource = readFileSync(
  resolve(process.cwd(), "src/components/SessionsSidebar.tsx"), "utf8");
assert.doesNotMatch(sidebarSource, /onGrant|目录授权/);
const newChatSource = readFileSync(
  resolve(process.cwd(), "src/components/NewChatView.tsx"), "utf8");
assert.match(newChatSource, /autoFocus=\{autoFocus\}/,
  "new-chat focus must follow the navigation intent instead of being unconditional");
assert.match(newChatSource, /<PendingImageAttachments/,
  "new-chat attachments must share the interactive image preview");
const composerSource = readFileSync(
  resolve(process.cwd(), "src/components/Composer.tsx"), "utf8");
assert.match(composerSource, /<PendingImageAttachments/,
  "session drafts must expose the shared interactive image preview");
assert.match(composerSource, /workSurface \? \(/);
assert.match(composerSource, /p\.draftStore\.get\(p\.draftKey\)/);
assert.match(composerSource, /accept="image\/\*" multiple/);
assert.match(composerSource, /aria-label="添加照片"/);
assert.match(composerSource, /aria-label="添加文件"/);
assert.match(composerSource, /className="work-compose-card"/);
assert.match(composerSource, /Artifacts · \{p\.workArtifactCount\}/);
assert.doesNotMatch(composerSource, /交付物/);
assert.doesNotMatch(composerSource, /项目与资料/);
assert.match(composerSource, /工作设置/);
assert.match(composerSource, /会话新增上下文/);
assert.match(composerSource, /workContext\.sessionPercentage\.toFixed\(0\)/);
assert.match(composerSource, /p\.contextReport\.percentage\.toFixed\(0\)/,
  "Code must retain the engine-total context reading");
assert.match(composerSource, /contextAvailable = p\.contextReport\?\.available !== false/,
  "an absent tokenUsage report must not be rendered as a real zero");
assert.match(composerSource, /尚未收到 Codex 的 tokenUsage/,
  "the context popover must explain the temporary unknown state");
assert.match(composerSource, /ref=\{workSettingsRef\}/);
assert.match(composerSource, /document\.addEventListener\("pointerdown", onPointerDown\)/);
assert.match(composerSource, /disabled=\{locked\}[\s\S]*?: "选择模型"/,
  "external read-only sessions must disable model changes");
assert.match(composerSource, /disabled=\{locked\}[\s\S]*?: "思考强度"/,
  "external read-only sessions must disable effort changes");
assert.match(composerSource, /const deferredClaudeControls = externalClaudeOwner !== null/,
  "native Claude ownership must distinguish saved takeover preferences from live controls");
assert.match(composerSource, />\s*接管后\s*<\/span>/,
  "saved Claude model and effort must be labelled as post-takeover controls");
assert.match(composerSource, /当前权限模式未公开/,
  "native Claude ownership must not present Remote's saved permission as live state");
assert.match(composerSource, /不是\$\{externalClaudeOwner\}当前模型/,
  "native Claude ownership must not present Remote's saved model as live state");
assert.match(composerSource, /不是\$\{externalClaudeOwner\}当前强度/,
  "native Claude ownership must not present Remote's saved effort as live state");
assert.doesNotMatch(composerSource, /终端占用/,
  "shared control must never be presented as exclusive terminal occupancy");
assert.match(composerSource, /presentLegacyExternalControl/);
assert.doesNotMatch(composerSource, /control-bar/,
  "terminal state no longer consumes a permanent row above the composer");
const workDashboardSource = readFileSync(
  resolve(process.cwd(), "src/components/WorkDashboardSheet.tsx"), "utf8");
assert.match(workDashboardSource, /<DateTimePicker value=\{scheduleAt\}/,
  "Work schedules must use the themed date-time picker");
assert.doesNotMatch(workDashboardSource, /datetime-local/,
  "Work schedules must not expose the browser-native date-time popup");
const dateTimePickerSource = readFileSync(
  resolve(process.cwd(), "src/components/DateTimePicker.tsx"), "utf8");
assert.match(dateTimePickerSource, /createPortal\(<>.*document\.body\)/s,
  "the date-time popover must escape the scrollable Work manager container");
const appCssSource = readFileSync(resolve(process.cwd(), "src/index.css"), "utf8");
assert.match(appCssSource,
  /\.terminal-control-card\{[^}]*right:auto; left:var\(--terminal-card-left\)/,
  "the terminal card must use its bounded left coordinate instead of an unclamped right offset");
assert.match(appCssSource, /\.capabilities-sheet>header\{[^}]*flex:none/s,
  "the Extensions header must not collapse under a long capability list");
assert.match(appCssSource, /\.capabilities-tabs\{[^}]*flex:none/s,
  "the Extensions tabs must remain visible above a long capability list");
assert.match(appCssSource, /\.capabilities-body\{[^}]*flex:1; min-height:0/s,
  "only the Extensions body may consume and scroll through remaining height");
assert.doesNotMatch(appCssSource, /\.work-form-actions button/,
  "Work action styles must not repaint nested calendar buttons");
assert.match(appCssSource, /\.work-form-actions>button/,
  "Work action styles must be limited to direct action buttons");
assert.match(appCssSource, /\.date-time-days button\{ height:32px/,
  "desktop calendar rows must stay compact enough to fit above the viewport edge");
assert.match(appCssSource,
  /@media \(max-width:640px\)\{[\s\S]*?\.work-settings\{ position:static; \}[\s\S]*?\.work-settings-pop\{ left:0; right:0; width:auto; \}/,
  "mobile Work settings must use the full composer footer instead of overflowing its trigger");
const sessionControlUiSource = readFileSync(
  resolve(process.cwd(), "src/session-control-ui.ts"), "utf8");
assert.match(sessionControlUiSource, /Codex App 使用中 · Web 只读/);
assert.match(sessionControlUiSource, /外部 CLI 控制中/);
assert.match(sessionControlUiSource, /未检测到本机终端/,
  "backend connectivity must not be presented as confirmed terminal ownership");
const terminalControlSource = readFileSync(
  resolve(process.cwd(), "src/components/TerminalControl.tsx"), "utf8");
assert.match(terminalControlSource, /control\?\.control_mode === "external_cli"/);
assert.match(terminalControlSource, /onTakeover\?\.\(\)/,
  "can_takeover remains a displayed request action, not client-side authorization");
assert.match(terminalControlSource, /aria-label="终端连接状态"/);
assert.match(terminalControlSource, /event\.key !== "Tab"/);
assert.match(terminalControlSource, /document\.contains\(trigger\).*trigger\.focus\(\)/,
  "dialog close restores focus to its terminal-status trigger");
assert.doesNotMatch(layoutCss, /\.header-secondary\{ display:none!important; \}/,
  "mobile headers must not blanket-hide Agent and notification controls");
assert.doesNotMatch(layoutCss, /\.hstat-label\{ display:none; \}/,
  "mobile headers must retain the runtime state label");
assert.doesNotMatch(layoutCss, /\.c-head \.hstat\{ display:none; \}/,
  "tiny headers must retain runtime state");
assert.doesNotMatch(appSource, /className="iconbtn header-agent"/,
  "extension management is a slash command, not permanent header chrome");
assert.match(composerSource, /case "skills": p\.onOpenExtensions\?\.\("skill"\)/,
  "Skills remain reachable from the composer on mobile");
assert.match(composerSource, /case "hooks": p\.onOpenExtensions\?\.\("hook"\)/,
  "Hooks remain reachable from the composer on mobile");
assert.match(appSource, /className=\{`iconbtn header-notify/,
  "notification settings remain reachable on mobile");
assert.match(layoutCss, /\.header-theme\{ display:none; \}/,
  "only the non-essential theme shortcut may collapse on the narrowest header");
assert.match(layoutCss, /var\(--app-height,100dvh\) - var\(--keyboard-inset,0px\)/,
  "the mobile sheet height accounts for the virtual keyboard inset");

const successfulTurn = {
  v: 19, type: "turn_end" as const, ts: 1, sid: "session-a", turn_id: "turn-a",
  result: { subtype: "success", duration_ms: 1, is_error: false },
};
const interruptedTurn = {
  ...successfulTurn, ts: 2, turn_id: "turn-b",
  result: { subtype: "error_during_execution", duration_ms: 2, is_error: true },
};
assert.equal(classifyTurnNotification(successfulTurn.result), "success");
assert.equal(classifyTurnNotification(interruptedTurn.result), "interrupted");
assert.equal(turnNotificationBody("Codex", interruptedTurn.result), "Codex 会话已中断");
assert.notEqual(turnNotificationTag(successfulTurn), turnNotificationTag(interruptedTurn),
  "successive turns in one session must not replace each other's notifications");
const chatViewSource = readFileSync(
  resolve(process.cwd(), "src/components/ChatView.tsx"), "utf8");
assert.match(chatViewSource, /surface !== "work"/);
assert.match(chatViewSource, /arr\.length === 1 && onOpenFile/);
assert.match(chatViewSource, /onOpenArtifacts\?\.\(\)/);
assert.match(layoutCss, /\.work-thread-shell \.thread-in\s*\{/);

assert.equal(relay.sendTakeover("codex-model-session"), true);
const takeoverFrame = JSON.parse(socket.sent.at(-1) ?? "{}");
assert.equal(takeoverFrame.type, "takeover");
assert.equal(takeoverFrame.sid, "codex-model-session");
assert.equal(typeof takeoverFrame.cmd_id, "string");
assert.equal(typeof takeoverFrame.client_id, "string");

// Stale/user-edited localStorage values must never reach strict Pydantic
// command literals. The App applies these guards before listing sessions or
// requesting a themed diff.
assert.equal(normalizeEngine("codex"), "codex");
assert.equal(normalizeEngine("legacy-engine"), "claude");
assert.equal(normalizeEngine(null), "claude");
assert.equal(normalizeDiffTheme("dark"), "dark");
assert.equal(normalizeDiffTheme("sepia"), "light");
assert.equal(normalizeDiffTheme(null), "light");

// These one-shot commands are just as reliable as chat mutations: each must
// carry the outbox identity needed for reconnect replay and wrapper dedupe.
relay.setFocusedSid("codex-control-session", "codex");
const reliableControlFrames: Array<[string, () => void]> = [
  ["get_status", () => relay.sendGetStatus()],
  ["get_goal", () => relay.sendGetGoal()],
  ["set_goal", () => relay.sendSetGoal("ship it", "active", 1024)],
  ["clear_goal", () => relay.sendClearGoal()],
  ["get_diff", () => relay.sendGetDiff("src/App.tsx", "dark")],
];
for (const [type, sendCommand] of reliableControlFrames) {
  sendCommand();
  const frame = JSON.parse(socket.sent.at(-1) ?? "{}");
  assert.equal(frame.type, type);
  assert.equal(frame.sid, "codex-control-session");
  assert.equal(typeof frame.cmd_id, "string");
  assert.equal(typeof frame.client_id, "string");
}

socket.receive({
  type: "snapshot", sid: "s1", cc_session_id: "s1", generation: "g1",
  state: "running", tail_text: "",
});
socket.receive({ type: "delta", sid: "s1", seq: 1, message_id: "m1", text: "X" });
socket.receive({
  type: "replay_start", sid: "s1", generation: "g1", from_seq: 1,
  to_seq: 2, truncated: false, rebuild: false,
});
socket.receive({ type: "delta", sid: "s1", seq: 1, message_id: "m1", text: "X" });
socket.receive({ type: "model", sid: "s1", seq: 2, model: "new" });
socket.receive({ type: "replay_end", sid: "s1", to_seq: 2, truncated: false });
socket.receive({ type: "model", sid: "s1", seq: 1, model: "old" });

assert.deepEqual(
  observed.filter((event) => event.type === "delta").map((event) => event.text),
  ["X"],
);
assert.deepEqual(
  observed.filter((event) => event.type === "model").map((event) => event.model),
  ["new"],
);

// Rebuild deliberately resets the seq epoch, so lower body frames must survive;
// once ReplayEnd closes it, the ordinary duplicate gate applies again.
socket.receive({ type: "delta", sid: "s1", seq: 10, message_id: "old", text: "old" });
socket.receive({
  type: "replay_start", sid: "s1", generation: "g1", from_seq: 1,
  to_seq: 1, truncated: false, rebuild: true,
});
socket.receive({
  type: "delta", sid: "s1", seq: 1, message_id: "rebuilt", text: "rebuilt",
});
socket.receive({ type: "replay_end", sid: "s1", to_seq: 1, truncated: false });
socket.receive({ type: "delta", sid: "s1", seq: 1, message_id: "stale", text: "stale" });
assert.equal(
  observed.filter((event) => event.type === "delta" && event.text === "rebuilt").length,
  1,
);
assert.equal(
  observed.filter((event) => event.type === "delta" && event.text === "stale").length,
  0,
);
assert.equal(relay.lastSeqFor("s1"), 1);

// The same numeric seq belongs to a different cursor domain after generation
// change and must not be mistaken for a duplicate.
// BtwOpened deliberately has no generation. Losing its following Snapshot used
// to leave the old fork stuck open because a normal session's g1 -> g2 change
// only notified the App when a per-btw generation had already been recorded.
socket.receive({
  type: "btw_opened", request_id: "btw-gap", btw_sid: "btw-old",
  parent_sid: "s1", engine: "claude",
});
socket.receive({
  type: "snapshot", sid: "s1", cc_session_id: "s1", generation: "g2",
  state: "running", tail_text: "",
});
socket.receive({ type: "model", sid: "s1", seq: 1, model: "next-generation" });
assert.equal(
  observed.filter((event) => event.type === "model"
    && event.model === "next-generation").length,
  1,
);
assert.equal(wrapperGenerationChanges, 1);
socket.receive({
  type: "snapshot", sid: "s2", cc_session_id: "s2", generation: "g2",
  state: "idle", tail_text: "",
});
assert.equal(wrapperGenerationChanges, 1); // one notice per wrapper generation
relay.stop();

// A separate socket exercises the v15 control watermark without perturbing
// the narrative generation assertions above.
const controlObserved: ServerEvent[] = [];
const controlRelay = new RelayWs({
  onEvent: (event) => { controlObserved.push(event); },
  onConnState: () => {},
});
controlRelay.start();
const controlSocket = FakeWebSocket.instances.at(-1);
assert.ok(controlSocket);
controlSocket.onopen?.();
const wireControlSid = "wire-control-revision";
controlRelay.seedReplayState({}, {}, {
  [wireControlSid]: {
    v: 19, ts: 1, type: "session_control", sid: wireControlSid,
    control_mode: "remote", write_state: "writable",
    terminal_attached: false, generation: "wire-generation", revision: 10,
  },
});
controlSocket.receive({
  type: "session_control", sid: wireControlSid,
  control_mode: "desktop", write_state: "read_only",
  terminal_attached: true, generation: "wire-generation", revision: 9,
});
assert.equal(controlObserved.filter(
  (event) => event.type === "session_control").length, 0,
"the WS watermark must drop an older direct control frame");
controlSocket.receive({
  type: "history", sid: wireControlSid, session_id: wireControlSid,
  revision: "wire-history", generation: "wire-generation",
  has_more: false, events: [],
  control: {
    v: 19, ts: 1, type: "session_control", sid: wireControlSid,
    control_mode: "external_cli", write_state: "read_only",
    terminal_attached: true, generation: "wire-generation", revision: 9,
  },
});
const strippedHistory = controlObserved.at(-1);
assert.equal(strippedHistory?.type, "history");
assert.equal(strippedHistory?.type === "history" && strippedHistory.control,
  undefined, "WS must retain History while stripping only stale control");
controlSocket.receive({
  type: "history", sid: wireControlSid, session_id: wireControlSid,
  revision: "wire-cross-session-history", generation: "wire-generation",
  has_more: false, events: [],
  control: {
    v: 19, ts: 1, type: "session_control", sid: "wire-other-session",
    control_mode: "external_cli", write_state: "read_only",
    terminal_attached: true, generation: "wire-generation", revision: 100,
  },
});
const crossSessionHistory = controlObserved.at(-1);
assert.equal(crossSessionHistory?.type, "history");
assert.equal(crossSessionHistory?.type === "history"
  && crossSessionHistory.control, undefined,
"WS must strip a History control routed to another session");
controlSocket.receive({
  type: "snapshot", sid: wireControlSid, cc_session_id: wireControlSid,
  state: "idle", tail_text: "", generation: "wire-generation",
  control: {
    v: 19, ts: 1, type: "session_control", sid: "wire-other-session",
    control_mode: "external_cli", write_state: "read_only",
    terminal_attached: true, generation: "wire-generation", revision: 100,
  },
});
const crossSessionSnapshot = controlObserved.at(-1);
assert.equal(crossSessionSnapshot?.type, "snapshot");
assert.equal(crossSessionSnapshot?.type === "snapshot"
  && crossSessionSnapshot.control, undefined,
"WS must strip a Snapshot control routed to another session");
controlSocket.receive({
  type: "session_control", sid: wireControlSid,
  control_mode: "external_cli", write_state: "read_only",
  terminal_attached: true, generation: "wire-generation",
  revision: 11, can_takeover: true,
});
assert.equal(controlObserved.at(-1)?.type, "session_control");
controlSocket.receive({
  type: "session_control", sid: wireControlSid,
  control_mode: "remote", write_state: "writable",
  terminal_attached: false, generation: "wire-generation", revision: 11,
});
assert.equal(controlObserved.filter(
  (event) => event.type === "session_control").length, 1,
"an equal-revision conflicting control frame must be dropped");
controlSocket.receive({
  type: "snapshot", sid: wireControlSid, cc_session_id: wireControlSid,
  state: "idle", tail_text: "", generation: "wire-generation-next",
  control: {
    v: 19, ts: 2, type: "session_control", sid: wireControlSid,
    control_mode: "remote", write_state: "writable",
    terminal_attached: false, generation: "wire-generation-next", revision: 0,
  },
});
const nextGenerationSnapshot = controlObserved.at(-1);
assert.equal(nextGenerationSnapshot?.type, "snapshot");
assert.equal(nextGenerationSnapshot?.type === "snapshot"
  && nextGenerationSnapshot.control?.revision, 0);
controlSocket.receive({
  type: "session_control", sid: wireControlSid,
  control_mode: "desktop", write_state: "read_only",
  terminal_attached: true, generation: "wire-generation", revision: 999,
});
assert.equal(controlObserved.at(-1), nextGenerationSnapshot,
  "old-generation direct control must be rejected regardless of revision");

const beforeUnroutedControl = controlObserved.length;
controlSocket.receive({
  type: "session_control", control_mode: "desktop", write_state: "read_only",
  terminal_attached: true, generation: "wire-generation-next", revision: 1000,
});
assert.equal(controlObserved.length, beforeUnroutedControl,
  "an unrouted direct control frame must be dropped at the transport boundary");

// A temp runtime can capture a real id which still has a stale cache watermark.
// The live temp cursor wins together with its generation and control revision.
const aliasOld = "wire-control-temp";
const aliasReal = "wire-control-real";
controlRelay.seedReplayState(
  { [aliasOld]: 3, [aliasReal]: 2 },
  { [aliasOld]: "wire-alias-live", [aliasReal]: "wire-alias-cache" },
  {
    [aliasOld]: {
      v: 19, ts: 3, type: "session_control", sid: aliasOld,
      control_mode: "remote", write_state: "writable",
      terminal_attached: false, generation: "wire-alias-live", revision: 2,
    },
    [aliasReal]: {
      v: 19, ts: 2, type: "session_control", sid: aliasReal,
      control_mode: "desktop", write_state: "read_only",
      terminal_attached: true, generation: "wire-alias-cache", revision: 50,
    },
  },
);
controlSocket.receive({
  type: "session_rekey", old_key: aliasOld, session_id: aliasReal,
});
assert.equal(controlRelay.generationFor(aliasReal), "wire-alias-live");
assert.equal(controlRelay.lastSeqFor(aliasReal), 3);
const afterAliasRekey = controlObserved.length;
controlSocket.receive({
  type: "session_control", sid: aliasReal,
  control_mode: "desktop", write_state: "read_only", terminal_attached: true,
  generation: "wire-alias-cache", revision: 999,
});
assert.equal(controlObserved.length, afterAliasRekey,
  "rekey must not retain the stale real-id generation watermark");
controlRelay.stop();

// Cwd ownership is local transport metadata. A create response freezes the
// surface epoch at send time; a later temp rekey cannot borrow the epoch of a
// surface selected after the request was sent.
const scopedObserved: Array<{ event: ServerEvent; ownership?: {
  scopeKey: string; surfaceEpoch: number; connectionGeneration: number;
} }> = [];
const scopedRelay = new RelayWs({
  onEvent: (event, ownership) => { scopedObserved.push({ event, ownership }); },
  onConnState: () => {},
}, "machine-scope");
scopedRelay.start();
const scopedSocket = FakeWebSocket.instances.at(-1);
assert.ok(scopedSocket);
scopedSocket.onopen?.();
assert.equal(scopedRelay.sendNewSession(
  "/work/a", "claude", null, null,
  { prompt: "hi", msg_id: "create-scope-a" },
), true);
scopedSocket.receive({
  type: "session_focus", session_id: "tmp-scope-a",
  request_id: "create-scope-a", cwd: "/work/a",
});
const createdScoped = scopedObserved.at(-1);
assert.equal(createdScoped?.event.type, "session_focus");
assert.equal(createdScoped?.ownership?.scopeKey, "machine-scope:code:claude");
assert.equal(createdScoped?.ownership?.connectionGeneration, 1);
const originalEpoch = createdScoped?.ownership?.surfaceEpoch;

scopedRelay.setSurface("codex", "code");
scopedRelay.setSurface("claude", "code");
scopedSocket.receive({
  type: "session_rekey", old_key: "tmp-scope-a",
  session_id: "real-scope-a", cwd: "/work/a",
});
const staleRekey = scopedObserved.at(-1);
assert.equal(staleRekey?.event.type, "session_rekey");
assert.equal(staleRekey?.ownership, undefined,
  "A→B→A must not relabel an old temp rekey with the current surface epoch");
assert.ok(originalEpoch != null);

scopedSocket.receive({
  type: "session_rekey", old_key: "unknown-temp",
  session_id: "unknown-real", cwd: "/deleted",
});
assert.equal(scopedObserved.at(-1)?.ownership, undefined,
  "an unknown temp key has no cwd ownership");

const beforeReconnectFrame = scopedObserved.length;
(scopedRelay as unknown as { connect: () => void }).connect();
const replacementSocket = FakeWebSocket.instances.at(-1);
assert.ok(replacementSocket && replacementSocket !== scopedSocket);
replacementSocket.onopen?.();
scopedSocket.receive({
  type: "session_rekey", old_key: "real-scope-a",
  session_id: "too-late", cwd: "/deleted/old-socket",
});
assert.equal(scopedObserved.length, beforeReconnectFrame,
  "a delayed frame from an older underlying WebSocket generation is dropped");
scopedRelay.stop();

// Query acceptance is a per-session lifecycle barrier, not an outbox ACK bit.
// It survives this RelayWs instance's automatic reconnect and only correlated
// authoritative narrative proof may release it.
const acceptanceRelay = new RelayWs({
  onEvent: () => {},
  onConnState: () => {},
});
acceptanceRelay.start();
const acceptanceSocket = FakeWebSocket.instances.at(-1);
assert.ok(acceptanceSocket);
acceptanceSocket.onopen?.();
acceptanceRelay.setFocusedSid("accept-wire-a", "claude", "code");
assert.equal(acceptanceRelay.sendQuery(
  "first", "accept-wire-message-a"), true);
const acceptanceFrameA = JSON.parse(acceptanceSocket.sent.at(-1) ?? "{}");
assert.equal(acceptanceRelay.sendQuery(
  "must defer", "accept-wire-message-a2"), false,
  "a second direct query to one sid is rejected before it enters the outbox");
acceptanceRelay.setFocusedSid("accept-wire-b", "claude", "code");
acceptanceSocket.receive({
  type: "history", sid: "accept-wire-b", session_id: "accept-wire-b",
  revision: "accept-wire-history", generation: "accept-wire-generation",
  build_seq: 1, live_seq: 20, detail: "summary", events: [],
  turns: [{
    id: "accept-wire-native-old", prompt: "other session", blocks: [],
    done: true, detailEventCount: 0, detailLoaded: false,
  }],
  has_more: false, oldest_id: "accept-wire-native-old",
  newest_id: "accept-wire-native-old",
});
assert.equal(acceptanceRelay.sendQuery(
  "other session", "accept-wire-message-b"), true);
acceptanceSocket.receive({
  type: "command_ack",
  client_id: acceptanceFrameA.client_id,
  cmd_id: acceptanceFrameA.cmd_id,
});
assert.equal(
  acceptanceRelay.pendingQueryFor("accept-wire-a"),
  "accept-wire-message-a",
  "command ACK alone cannot release query acceptance",
);

(acceptanceRelay as unknown as { connect: () => void }).connect();
const acceptanceReconnectSocket = FakeWebSocket.instances.at(-1);
assert.ok(acceptanceReconnectSocket
  && acceptanceReconnectSocket !== acceptanceSocket);
acceptanceReconnectSocket.onopen?.();
assert.equal(
  acceptanceRelay.pendingQueryFor("accept-wire-a"),
  "accept-wire-message-a",
  "the ACKed latch survives automatic WebSocket reconnect",
);
assert.ok(acceptanceReconnectSocket.sent.some((raw) => {
  const frame = JSON.parse(raw);
  return frame.type === "query" && frame.msg_id === "accept-wire-message-b";
}), "the unacknowledged session-B query is replayed after reconnect");

acceptanceReconnectSocket.receive({
  type: "error", sid: "accept-wire-a", msg_id: "wrong-message",
  code: "busy", message: "wrong",
});
assert.equal(
  acceptanceRelay.pendingQueryFor("accept-wire-a"),
  "accept-wire-message-a",
);
acceptanceReconnectSocket.receive({
  type: "error", sid: "accept-wire-a", msg_id: "accept-wire-message-a",
  code: "busy", message: "rejected",
});
assert.equal(acceptanceRelay.pendingQueryFor("accept-wire-a"), null);
assert.equal(
  acceptanceRelay.pendingQueryFor("accept-wire-b"),
  "accept-wire-message-b",
  "session-A correlation cannot release session B",
);
acceptanceReconnectSocket.receive({
  type: "history", sid: "accept-wire-b", session_id: "accept-wire-b",
  revision: "accept-wire-history", generation: "accept-wire-generation",
  build_seq: 1, live_seq: 20, detail: "summary", events: [],
  turns: [{
    id: "accept-wire-native-old", prompt: "other session", blocks: [],
    done: true, detailEventCount: 0, detailLoaded: false,
  }],
  has_more: false, oldest_id: "accept-wire-native-old",
  newest_id: "accept-wire-native-old",
});
assert.equal(
  acceptanceRelay.pendingQueryFor("accept-wire-b"),
  "accept-wire-message-b",
  "a delayed old History with the same prompt is not query acceptance",
);
acceptanceReconnectSocket.receive({
  type: "history", sid: "accept-wire-b", session_id: "accept-wire-b",
  revision: "accept-wire-history", generation: "accept-wire-generation",
  build_seq: 2, live_seq: 21, detail: "summary", events: [],
  turns: [{
    id: "accept-wire-native-page", prompt: "other session", blocks: [],
    done: true, detailEventCount: 0, detailLoaded: false,
  }],
  before: "accept-wire-native-old", has_more: true,
  oldest_id: "accept-wire-native-page", newest_id: "accept-wire-native-page",
});
assert.equal(
  acceptanceRelay.pendingQueryFor("accept-wire-b"),
  "accept-wire-message-b",
  "an older pagination page cannot release query acceptance",
);
acceptanceReconnectSocket.receive({
  type: "history", sid: "accept-wire-b", session_id: "accept-wire-b",
  revision: "accept-wire-history", generation: "accept-wire-generation",
  build_seq: 2, live_seq: 21, detail: "summary", events: [],
  turns: [{
    id: "accept-wire-native-wrong", prompt: "unrelated", blocks: [],
    done: true, detailEventCount: 0, detailLoaded: false,
  }],
  has_more: false, oldest_id: "accept-wire-native-wrong",
  newest_id: "accept-wire-native-wrong",
});
assert.equal(
  acceptanceRelay.pendingQueryFor("accept-wire-b"),
  "accept-wire-message-b",
  "a different appended native turn cannot release query acceptance",
);
acceptanceReconnectSocket.receive({
  type: "history", sid: "accept-wire-b", session_id: "accept-wire-b",
  revision: "accept-wire-history", generation: "accept-wire-generation",
  build_seq: 3, live_seq: 22, detail: "summary", events: [],
  turns: [{
    id: "accept-wire-native-new", prompt: "other session", blocks: [],
    done: true, detailEventCount: 0, detailLoaded: false,
  }],
  has_more: false, oldest_id: "accept-wire-native-new",
  newest_id: "accept-wire-native-new",
});
assert.equal(acceptanceRelay.pendingQueryFor("accept-wire-b"), null,
  "a matching appended native History head recovers an echo missed during reconnect");
acceptanceRelay.stop();

console.log("web reliability tests passed");
