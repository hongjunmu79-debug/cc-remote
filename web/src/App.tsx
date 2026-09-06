import { lazy, Suspense, useCallback, useEffect, useReducer, useRef, useState, type TouchEvent } from "react";
import { RelayWs, sessionScopeKey, type EventOwnership } from "./ws";
import { reduce, initialState, createRuntime, type Turn } from "./reducer";
import { uuid } from "./util";
import { Icon } from "./icons";
import { ChatView } from "./components/ChatView";
import { Composer } from "./components/Composer";
import { ReconnectBanner } from "./components/ReconnectBanner";
import { NoticeStack } from "./components/NoticeStack";
import { presentCommandProblem } from "./problem-presentation";
import { LoginForm } from "./components/LoginForm";
import { SessionsSidebar } from "./components/SessionsSidebar";
import { DirPicker } from "./components/DirPicker";
import { NewChatView } from "./components/NewChatView";
import { BtwPanel } from "./components/BtwPanel";
import { QuestionSheet } from "./components/QuestionSheet";
import { GoalPanel } from "./components/GoalPanel";
import { StatusSheet } from "./components/StatusSheet";
import { ForkWorktreeSheet } from "./components/ForkWorktreeSheet";
import { WorkDashboardSheet } from "./components/WorkDashboardSheet";
import { WorkArtifactsSheet } from "./components/WorkArtifactsSheet";
import { CapabilitiesSheet, type HookDraft, type SkillDraft } from "./components/CapabilitiesSheet";
import { TerminalControl } from "./components/TerminalControl";
import { DeviceSheet, type PairingState, type RemoteDevice } from "./components/DeviceSheet";
import { parseGoalCommand } from "./goal-command";
import { shouldOpenCodexStatus } from "./status-capabilities";
import { permsFor } from "./data";
import { shouldAcceptSessionList } from "./session-list";
import { PAIRED_CLIENT_ID_KEY, clearLegacyAuthMarkers, probeSession, syncPairedClientId } from "./session-auth";
import {
  canEnqueueQuery,
  collectWaitingQueries,
  selectDrainCandidates,
} from "./runtime-drain";
import { MAX_RUNTIME_SESSIONS } from "./runtime-bounds";
import { isTerminalWorktreeForkError, matchesSessionForkRequest,
  matchesWorktreeForkRequest, type PendingSessionFork,
  type PendingWorktreeFork } from "./session-worktree";
import { classifyBtwOpened, consumeDiscardedBtwSnapshot, matchesBtwRequest,
  normalizeDiffTheme, normalizeEngine, type Snapshot, type QueryImg,
  type QueryFile, type SessionInfo, type CodexPermissionMode,
  type CodexServiceTier, type CollaborationModeName,
  type DiffTheme, type Engine, type Space,
  type SessionControl, sessionControlLocksInput } from "./protocol";
import type { EngineCapabilities, EngineCapabilityItem, EngineCapabilityKind, WorkArtifactInfo, WorkDashboard } from "./protocol";
import { isMarkdownPath } from "./preview-path";
import { parseGitDiff } from "./diff";
import { resolveSidebarSwipe } from "./responsive-layout";
import {
  bumpSessionActivity,
  compareSessionsByActivity,
  mergeSessionActivityState,
  sessionCommandTarget,
  setSessionPinned,
} from "./session-order";
import { disableRemotePush, enableRemotePush } from "./push";
import { turnNotificationBody, turnNotificationTag } from "./turn-notification";
import { HistoryRequestCoordinator } from "./history-requests";
import { RecoverableReadCoordinator } from "./recoverable-read";
import { InlineImageAssetCache } from "./inline-image-assets";
import { HistoryImageAssetCache } from "./history-image-assets";
import { ComposerDraftStore, composerDraftKey } from "./composer-drafts";
import { NativePagerBridge } from "./native-pager/NativePagerBridge";
import { projectNativePagerSnapshot } from "./native-pager/projector";
import { findNativeSession, nativeCodeCatalog } from "./native-pager/catalog";
import type {
  NativeCommandEnvelope, NativeCommandResult,
} from "./native-pager/contract";

const THEME_KEY = "cc_remote_theme";
const ENGINE_KEY = "cc_remote_engine";  // which backend the NEXT new session uses
const SPACE_KEY = "cc_remote_space";
const NOTIFY_KEY = "cc_remote_notifications";
const MACHINE_KEY = "cc_remote_machine";
// Paint the newest few turns first. Older history is intentionally fetched in
// follow-up pages so one tool-heavy conversation cannot monopolize the socket,
// reducer, and main thread before the current answer becomes usable.
const HISTORY_INITIAL_PAGE = 4;
const HISTORY_MORE_PAGE = 12;

// The sidebar is an overlay on mobile (<980px, matches index.css) but a
// persistent grid column on desktop. So auto-close it after picking a session
// ONLY on mobile; on desktop keep it open.
const isMobile = () => window.matchMedia("(max-width: 979px)").matches;
const hasNativePagerHost = () =>
  typeof window.ccRemoteNative?.postMessage === "function";

// ArtifactPanel (diff/preview) is only mounted when an artifact is open; load it
// on demand so its chunk is not part of the critical first-paint bundle.
const ArtifactPanel = lazy(() =>
  import("./components/ArtifactPanel").then((module) => ({ default: module.ArtifactPanel })));

export default function App() {
  const [theme, setTheme] = useState<DiffTheme>(
    () => normalizeDiffTheme(localStorage.getItem(THEME_KEY)));
  const [engine, setEngine] = useState<Engine>(
    () => normalizeEngine(localStorage.getItem(ENGINE_KEY)));
  const [space, setSpace] = useState<Space>(
    () => localStorage.getItem(SPACE_KEY) === "work" ? "work" : "code");
  const [authed, setAuthed] = useState(false);
  const [authReady, setAuthReady] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [dirPickerOpen, setDirPickerOpen] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);
  const [newChatAutoFocus, setNewChatAutoFocus] = useState(true);
  const [editPrompt, setEditPrompt] = useState<string | null>(null);
  // right slot is shared by diff + /btw; rightView picks which shows.
  const [rightView, setRightView] = useState<"diff" | "btw">("diff");
  // true from the moment /btw is clicked until the fork's btw_opened arrives — so
  // the panel appears instantly (spinner) instead of waiting ~1s for the fork.
  const [btwOpening, setBtwOpening] = useState(false);
  // Goal is deliberately opt-in UI: no empty bar and no RPC until /goal runs.
  // Keep reveal/editor state per session so switching sessions never leaks it.
  const [goalUiBySid, setGoalUiBySid] = useState<Record<string, { revealed: boolean; open: boolean }>>({});
  const [statusOpenSid, setStatusOpenSid] = useState<string | null>(null);
  const [forkWorktreeSession, setForkWorktreeSession] = useState<SessionInfo | null>(null);
  const [forkWorktreeCreating, setForkWorktreeCreating] = useState(false);
  const [forkWorktreeError, setForkWorktreeError] = useState<string | null>(null);
  const [forkingPointId, setForkingPointId] = useState<string | null>(null);
  const [workManagerOpen, setWorkManagerOpen] = useState(false);
  const [workArtifactsOpen, setWorkArtifactsOpen] = useState(false);
  const [capabilitiesOpen, setCapabilitiesOpen] = useState(false);
  const [capabilitiesKind, setCapabilitiesKind] = useState<EngineCapabilityKind | "all">("all");
  const [capabilitiesLoading, setCapabilitiesLoading] = useState(false);
  const [deviceSheetOpen, setDeviceSheetOpen] = useState(false);
  const [capabilitiesBySurface, setCapabilitiesBySurface] = useState<Record<string, EngineCapabilities>>({});
  const [notificationsEnabled, setNotificationsEnabled] = useState(
    () => localStorage.getItem(NOTIFY_KEY) === "1"
      && typeof Notification !== "undefined" && Notification.permission === "granted");
  const [machineId, setMachineId] = useState(
    () => localStorage.getItem(MACHINE_KEY) || "default");
  const [remoteDevices, setRemoteDevices] = useState<RemoteDevice[]>([]);
  const [devicePairing, setDevicePairing] = useState<PairingState>({
    enabled: false, expires_at: null,
  });
  const [workProjectId, setWorkProjectId] = useState<string | null>(null);
  const [workDashboards, setWorkDashboards] = useState<Partial<Record<Engine, WorkDashboard>>>({});
  const [workArtifactsBySid, setWorkArtifactsBySid] = useState<Record<string, WorkArtifactInfo[]>>({});
  const [state, dispatch] = useReducer(reduce, initialState);
  const inlineImageAssetsRef = useRef(new InlineImageAssetCache());
  const [, bumpInlineImageRevision] = useReducer((value: number) => value + 1, 0);
  const historyImageAssetsRef = useRef(new HistoryImageAssetCache());
  const composerDraftsRef = useRef(new ComposerDraftStore());
  const [, bumpHistoryImageRevision] = useReducer((value: number) => value + 1, 0);
  const dismissBanner = useCallback((banner: string) => {
    dispatch({ type: "dismiss_banner", banner });
  }, []);
  const remotePushActiveRef = useRef(false);
  const stateRef = useRef(state);
  stateRef.current = state;
  const wsRef = useRef<RelayWs | null>(null);
  const historyRequestsRef = useRef(new HistoryRequestCoordinator());
  const drainingRef = useRef<Set<string>>(new Set());
  const pendingCreateRef = useRef<string | null>(null);
  const createRequestsRef = useRef<Map<string, {
    scopeKey: string;
    cwdSource: "default" | "inherited" | "explicit";
  }>>(new Map());
  const pendingBtwRef = useRef<string | null>(null);
  const pendingSessionForkRef = useRef<PendingSessionFork | null>(null);
  const pendingWorktreeForkRef = useRef<PendingWorktreeFork | null>(null);
  const sessionListsBySurfaceRef = useRef<Record<string, SessionInfo[]>>({});
  const [nativeCatalogRevision, bumpNativeCatalogRevision] = useReducer(
    (value: number) => value + 1,
    0,
  );
  // Cached lists are paint-only during a surface switch. A surface may choose
  // its remembered/latest focus only after a fresh wrapper list is accepted.
  const authoritativeSurfaceListsRef = useRef<Set<string>>(new Set());
  const sessionActivityPendingRef = useRef<Set<string>>(new Set());
  const prefetchedSurfacesRef = useRef<Set<string>>(new Set());
  const lastFocusBySurfaceRef = useRef<Record<string, string>>({});
  const preferredSurfaceFocusRef = useRef<{ key: string; sid: string } | null>(null);
  const activeBtwRef = useRef<{ requestId: string; sid: string } | null>(null);
  // Retain recently cancelled ids so a late response can be identified and
  // discarded (and a late successful fork can be closed) without disturbing a
  // newer opening spinner. Bounded because a peer may disappear permanently.
  const btwRequestIdsRef = useRef<Set<string>>(new Set());
  const discardedBtwSidsRef = useRef<Set<string>>(new Set());
  // A marker may arrive while its session is in the background and while an
  // IndexedDB read is already in flight. The set blocks new cache use; the
  // epoch rejects reads that started before the destructive mutation.
  // sid -> exact revision required by a rollback marker. null means a replay
  // gap hid the revision, so the next authoritative first page may satisfy it.
  const historyInvalidationsRef = useRef<Map<string, string | null>>(new Map());
  const historyCacheEpochRef = useRef<Map<string, number>>(new Map());
  const previousMachineRef = useRef(machineId);
  const touchStartX = useRef(0);
  const touchStartY = useRef(0);
  const touchSwipeLocked = useRef(false);
  const artifactDirtyRef = useRef(false);
  const setArtifactDirty = useCallback((dirty: boolean) => {
    artifactDirtyRef.current = dirty;
  }, []);
  const confirmArtifactDiscard = useCallback(() => {
    if (!artifactDirtyRef.current) return true;
    if (!window.confirm("Markdown 有未保存的修改，确定放弃吗？")) return false;
    artifactDirtyRef.current = false;
    return true;
  }, []);
  const requestHistory = useCallback((
    sid: string,
    before: string | null | undefined,
    limit: number,
    generation?: string | null,
    revision?: string | null,
  ) => {
    const ws = wsRef.current;
    if (!ws) return false;
    return historyRequestsRef.current.request({
      sid, before, limit,
      generation: generation ?? ws.generationFor(sid),
      revision,
    }, () => ws.sendGetHistory(sid, before, limit));
  }, []);
  // guards the once-per-connection "land on the latest session" auto-focus below
  const didInitFocusRef = useRef(false);
  const shortcutRef = useRef<{
    artifact: typeof state.artifact;
    btwSid: string | null;
    rightView: "diff" | "btw";
    getDiff: (file: string) => void;
    openBtw: () => void;
    closeBtw: () => void;
  }>({ artifact: null, btwSid: null, rightView: "diff",
    getDiff: () => {}, openBtw: () => {}, closeBtw: () => {} });

  useEffect(() => {
    const previous = previousMachineRef.current;
    if (previous === machineId) return;
    previousMachineRef.current = machineId;
    localStorage.setItem(MACHINE_KEY, machineId);
    pendingCreateRef.current = null;
    createRequestsRef.current.clear();
    pendingBtwRef.current = null;
    activeBtwRef.current = null;
    sessionListsBySurfaceRef.current = {};
    authoritativeSurfaceListsRef.current.clear();
    sessionActivityPendingRef.current.clear();
    historyRequestsRef.current.clear();
    prefetchedSurfacesRef.current.clear();
    historyInvalidationsRef.current.clear();
    historyCacheEpochRef.current.clear();
    inlineImageAssetsRef.current.clear();
    historyImageAssetsRef.current.clear();
    bumpInlineImageRevision();
    bumpHistoryImageRevision();
    dispatch({ type: "reset" });
    void import("./cache").then((module) => module.clearCache());
  }, [machineId]);

  // The focused session's runtime (turns/state/model/perm/queue/...). Falls back
  // to an empty runtime before any session is focused.
  const focusedSid = state.focusedSid;
  const activeScopeKey = sessionScopeKey(machineId, engine, space);
  const currentCwd = state.cwdByScope[activeScopeKey] ?? "";
  const rt = state.runtimes[focusedSid ?? ""] ?? createRuntime();
  const focusedEngine = (state.sessions.find(
    (session) => session.session_id === focusedSid)?.engine ?? engine) as "claude" | "codex";
  const focusedComposerDraftKey = composerDraftKey(
    machineId, space, focusedEngine, focusedSid ?? "",
  );
  const inlineImageAssets = focusedSid
    ? inlineImageAssetsRef.current.forSession(focusedSid) : {};
  const historyImageAssets = focusedSid
    ? historyImageAssetsRef.current.forSession(focusedSid) : {};
  const currentWorkArtifacts = focusedSid ? (workArtifactsBySid[focusedSid] ?? []) : [];
  const allQueued = collectWaitingQueries(state.runtimes);
  const replaceableQueued = collectWaitingQueries(state.runtimes, focusedSid);

  const goalUi = focusedSid ? goalUiBySid[focusedSid] : undefined;
  const loadMessageImage = useCallback((sid: string, path: string): boolean => {
    const ws = wsRef.current;
    if (!ws || stateRef.current.focusedSid !== sid) return false;
    const cache = inlineImageAssetsRef.current;
    if (cache.has(sid, path)) return true;
    const previewId = uuid();
    const requestId = uuid();
    if (!cache.begin({ sid, path, previewId, requestId })) return false;
    if (!ws.sendGetPreviewAsset(path, previewId, requestId)) {
      cache.cancel(requestId);
      return false;
    }
    bumpInlineImageRevision();
    return true;
  }, []);
  const loadFocusedMessageImage = useCallback((path: string) => (
    focusedSid ? loadMessageImage(focusedSid, path) : false
  ), [focusedSid, loadMessageImage]);
  const loadHistoryImage = useCallback((
    turnId: string,
    imageId: string,
    variant: "thumbnail" | "full",
  ): boolean => {
    const sid = stateRef.current.focusedSid;
    const ws = wsRef.current;
    if (!sid || !ws) return false;
    const revision = stateRef.current.runtimes[sid]?.historyRevision;
    if (!revision) return false;
    const cache = historyImageAssetsRef.current;
    if (cache.has(sid, turnId, imageId, variant)) return true;
    const requestId = uuid();
    if (!cache.begin({
      sid, turnId, imageId, variant, requestId, revision,
    })) return false;
    if (!ws.sendGetHistoryImage(
      sid, turnId, imageId, variant, requestId, revision,
    )) {
      cache.cancel(requestId);
      return false;
    }
    bumpHistoryImageRevision();
    return true;
  }, []);

  // HttpOnly cookies can't be inspected from JS. Ask the relay whether this
  // browser session is still registered before opening a WebSocket; this also
  // makes relay restarts (which intentionally revoke old sessions) fail closed.
  useEffect(() => {
    // Never retain credentials/markers from the pre-HttpOnly implementation.
    clearLegacyAuthMarkers(localStorage);
    let cancelled = false;
    let timer: number | null = null;
    let backoff = 1000;
    const check = async () => {
      const result = await probeSession();
      if (cancelled) return;
      if (result === "unavailable") {
        setAuthReady(false);
        timer = window.setTimeout(check, backoff);
        backoff = Math.min(backoff * 2, 5000);
        return;
      }
      if (result === "unauthorized") {
        clearLegacyAuthMarkers(localStorage);
        localStorage.removeItem(PAIRED_CLIENT_ID_KEY);
        // Do not expose the login form until prior-session prompts and
        // attachments are gone. A fast login must not race cache hydration.
        try { await import("./cache").then((module) => module.clearCache()); }
        catch { /* best-effort local cleanup */ }
        if (cancelled) return;
      }
      if (result === "authenticated") {
        try { await syncPairedClientId(localStorage); }
        catch { /* a later WS auth probe remains authoritative */ }
        if (cancelled) return;
      }
      setAuthed(result === "authenticated");
      setAuthReady(true);
    };
    void check();
    return () => {
      cancelled = true;
      if (timer !== null) window.clearTimeout(timer);
    };
  }, []);

  useEffect(() => {
    if (!authed) { setRemoteDevices([]); return; }
    let cancelled = false;
    void fetch("/api/devices", {
      credentials: "same-origin", cache: "no-store",
    }).then(async (response) => response.ok ? response.json() : null)
      .then((payload) => {
        if (cancelled || !payload || !Array.isArray(payload.devices)) return;
        const validDevices: RemoteDevice[] = (payload.devices as unknown[]).filter((value: unknown): value is RemoteDevice => {
          if (!value || typeof value !== "object") return false;
          const item = value as Partial<RemoteDevice>;
          return typeof item.machine_id === "string"
            && /^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$/.test(item.machine_id)
            && typeof item.label === "string" && typeof item.online === "boolean";
        });
        const available = validDevices.map((device) => device.machine_id);
        setRemoteDevices(validDevices);
        setDevicePairing(payload.pairing ?? { enabled: false, expires_at: null });
        if (available.length && !available.includes(machineId)) {
          setMachineId(available[0]);
        }
      }).catch(() => undefined);
    return () => { cancelled = true; };
  }, [authed, machineId]);

  useEffect(() => {
    let cancelled = false;
    if (!authed || !notificationsEnabled) {
      remotePushActiveRef.current = false;
      return;
    }
    void enableRemotePush(machineId).then((enabled) => {
      if (!cancelled) remotePushActiveRef.current = enabled;
    });
    return () => { cancelled = true; };
  }, [authed, machineId, notificationsEnabled]);

  // Swipe right -> open sidebar, swipe left -> close (mobile). Interactive
  // vertical scrollers opt out so a diagonal scroll never becomes navigation.
  const onTouchStart = (e: TouchEvent) => {
    const touch = e.touches[0];
    touchStartX.current = touch.clientX;
    touchStartY.current = touch.clientY;
    touchSwipeLocked.current = e.target instanceof Element
      && !!e.target.closest("[data-lock-horizontal-swipe]");
  };
  const onTouchEnd = (e: TouchEvent) => {
    const touch = e.changedTouches[0];
    const action = resolveSidebarSwipe(
      touchStartX.current,
      touchStartY.current,
      touch.clientX,
      touch.clientY,
      window.innerWidth,
      touchSwipeLocked.current,
    );
    if (action === "open") setSidebarOpen(true);
    else if (action === "close") setSidebarOpen(false);
  };

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    localStorage.setItem(THEME_KEY, theme);
  }, [theme]);
  const toggleTheme = () => setTheme((t) => (t === "dark" ? "light" : "dark"));

  // `engine` selects the backend (Claude Code / Codex): the whole UI re-skins via
  // data-engine, and the sidebar re-lists that engine's own sessions.
  const engineRef = useRef(engine);
  engineRef.current = engine;
  const spaceRef = useRef(space);
  spaceRef.current = space;
  useEffect(() => {
    document.documentElement.setAttribute("data-engine", engine);
    localStorage.setItem(ENGINE_KEY, engine);
    wsRef.current?.setSurface(engine, space);
    wsRef.current?.sendListSessions(engine, space);
    if (space === "work") wsRef.current?.sendGetWorkDashboard(engine);
  }, [engine, space]);
  useEffect(() => {
    document.documentElement.setAttribute("data-space", space);
    localStorage.setItem(SPACE_KEY, space);
  }, [space]);
  const rememberSurfaceFocus = (currentEngine: Engine, currentSpace: Space) => {
    if (focusedSid && !state.newChat) {
      lastFocusBySurfaceRef.current[`${currentSpace}:${currentEngine}`] = focusedSid;
    }
  };

  const prepareSurfaceSwitch = (nextEngine: Engine, nextSpace: Space) => {
    rememberSurfaceFocus(engine, space);
    const surfaceKey = `${nextSpace}:${nextEngine}`;
    authoritativeSurfaceListsRef.current.delete(surfaceKey);
    dispatch({
      type: "restore_session_list",
      sessions: sessionListsBySurfaceRef.current[surfaceKey] ?? [],
    });
    const remembered = lastFocusBySurfaceRef.current[surfaceKey];
    preferredSurfaceFocusRef.current = remembered
      ? { key: surfaceKey, sid: remembered } : null;
    didInitFocusRef.current = false;
    wsRef.current?.setSurface(nextEngine, nextSpace);
    wsRef.current?.setFocusedSid(null);
    // Keep the previous surface's transcript out of view while its accepted
    // list is restored. The focus effect below exits this temporary new page as
    // soon as the remembered (or latest valid) session is available.
    dispatch({ type: "enter_new_chat", cwd: "~", cwdSource: "default" });
    setNewChatAutoFocus(false);
  };

  // Engine and Work/Code switches are navigation. Each surface restores the
  // session that was last open there instead of silently starting a new one.
  const toggleEngine = () => {
    const nextEngine: Engine = engine === "codex" ? "claude" : "codex";
    pendingCreateRef.current = null;
    setCreateError(null);
    setStatusOpenSid(null);
    setWorkArtifactsOpen(false);
    setWorkProjectId(null);
    prepareSurfaceSwitch(nextEngine, space);
    setEngine(nextEngine);
    if (isMobile()) setSidebarOpen(false);
  };

  const switchSpace = (next: Space) => {
    if (next === space || !confirmArtifactDiscard()) return;
    pendingCreateRef.current = null;
    setCreateError(null);
    setStatusOpenSid(null);
    setForkWorktreeSession(null);
    setForkWorktreeError(null);
    setWorkArtifactsOpen(false);
    prepareSurfaceSwitch(engine, next);
    setSpace(next);
  };

  // WebSocket lifecycle
  useEffect(() => {
    if (!authed) return;
    const draining = drainingRef.current;
    const historyRequests = historyRequestsRef.current;
    didInitFocusRef.current = false;  // re-arm initial-focus for this connection lifecycle
    authoritativeSurfaceListsRef.current.delete(`${spaceRef.current}:${engineRef.current}`);

    let cancelled = false;
    const recoverableReads = new RecoverableReadCoordinator(
      (callback, delayMs) => window.setTimeout(callback, delayMs),
      (timer) => window.clearTimeout(timer),
    );

    // A snapshot announces a session (cc_session_id/state/cwd). We do NOT reset
    // the cursor here anymore — cursors are seeded from the IndexedDB cache before
    // connecting, so hello asks the wrapper only for the DELTA instead of a full
    // history replay of every resident session (that flood wedged reconnect).
    function handleSnapshot(e: Snapshot, ownership?: EventOwnership) {
      dispatch({ type: "event", event: e, ownership });
    }

    (async () => {
      let seeded = { cursors: {} as Record<string, number>,
        generations: {} as Record<string, string>,
        controls: {} as Record<string, SessionControl> };
      try { seeded = await import("./cache").then((m) => m.loadAllReplayState()); } catch { /* best-effort */ }
      if (cancelled) return;
      const ws = new RelayWs({
        onEvent: (msg, ownership) => {
          if (msg.type === "preview_asset"
              && inlineImageAssetsRef.current.accept(msg)) {
            bumpInlineImageRevision();
          }
          if (msg.type === "history_image"
              && historyImageAssetsRef.current.accept(msg)) {
            bumpHistoryImageRevision();
          }
          if ((msg.type === "user_msg" || msg.type === "turn_end") && msg.sid) {
            const activityMs = Math.round(msg.ts * 1000);
            let changed = false;
            for (const [key, listed] of Object.entries(
              sessionListsBySurfaceRef.current)) {
              const updated = bumpSessionActivity(listed, msg.sid, activityMs);
              if (updated !== listed) {
                sessionListsBySurfaceRef.current[key] = updated;
                changed = true;
              }
            }
            if (msg.type === "user_msg" && changed) {
              sessionActivityPendingRef.current.add(msg.sid);
            }
            if (changed) bumpNativeCatalogRevision();
          }
          if (msg.type === "history_invalidated") {
            const sid = msg.session_id;
            if (inlineImageAssetsRef.current.dropSession(sid)) {
              bumpInlineImageRevision();
            }
            if (historyImageAssetsRef.current.dropSession(sid)) {
              bumpHistoryImageRevision();
            }
            if (historyInvalidationsRef.current.get(sid) !== msg.revision) {
              historyInvalidationsRef.current.set(sid, msg.revision);
              historyCacheEpochRef.current.set(
                sid, (historyCacheEpochRef.current.get(sid) ?? 0) + 1);
              void import("./cache").then((module) =>
                module.invalidateSessionCache(sid));
            }
            // The marker is deliberately tiny/replayable; the full replacement
            // is one-shot and may have been dropped by a disconnect or frame
            // size bound. Fetch immediately when visible; a background session
            // gets the same authoritative request when it is later focused.
            if (stateRef.current.focusedSid === sid) {
              requestHistory(
                sid, undefined, HISTORY_INITIAL_PAGE, undefined, msg.revision);
            }
          } else if (msg.type === "artifact_invalidated") {
            const sid = msg.session_id;
            setWorkArtifactsBySid((current) => {
              if (!(sid in current)) return current;
              const next = { ...current };
              delete next[sid];
              return next;
            });
            const session = stateRef.current.sessions.find(
              (candidate) => candidate.session_id === sid);
            if (session?.space === "work"
                || (spaceRef.current === "work"
                  && stateRef.current.focusedSid === sid)) {
              ws.sendGetWorkArtifacts(
                (session?.engine as Engine | undefined) ?? engineRef.current,
                sid,
              );
            }
          } else if (msg.type === "replay_start" && msg.sid
              && (msg.truncated || msg.rebuild)) {
            const sid = msg.sid;
            // If a marker is still retained inside this replay it will follow
            // ReplayStart and replace null with its exact revision.
            if (!historyInvalidationsRef.current.has(sid)) {
              historyInvalidationsRef.current.set(sid, null);
              historyCacheEpochRef.current.set(
                sid, (historyCacheEpochRef.current.get(sid) ?? 0) + 1);
              void import("./cache").then((module) =>
                module.invalidateSessionCache(sid));
            }
            setWorkArtifactsBySid((current) => {
              if (!(sid in current)) return current;
              const next = { ...current };
              delete next[sid];
              return next;
            });
            if (stateRef.current.focusedSid === sid) {
              requestHistory(
                sid, undefined, HISTORY_INITIAL_PAGE, msg.generation);
            }
          } else if (msg.type === "history" && msg.authoritative !== false && !msg.before
              && historyInvalidationsRef.current.has(msg.session_id)) {
            const expected = historyInvalidationsRef.current.get(msg.session_id);
            if (expected === null || expected === msg.revision) {
              // A late first page from an older revision must not re-enable the
              // cache behind a newer destructive marker.
              historyInvalidationsRef.current.delete(msg.session_id);
              void import("./cache").then((module) =>
                module.allowSessionCache(msg.session_id));
            }
          }
          if (msg.type === "history") {
            historyRequestsRef.current.complete(msg);
            const retryKey = ["history", msg.session_id, msg.before ?? "",
              msg.revision ?? ""].join("\u0000");
            if (msg.authoritative === false) {
              recoverableReads.retry(retryKey, () => {
                if (cancelled) return;
                if (stateRef.current.focusedSid !== msg.session_id) return;
                requestHistory(
                  msg.session_id, msg.before,
                  msg.before ? HISTORY_MORE_PAGE : HISTORY_INITIAL_PAGE,
                  msg.generation, msg.revision,
                );
              });
            } else {
              recoverableReads.complete(retryKey);
            }
          }
          if (msg.type === "turn_detail") {
            const retryKey = ["detail", msg.session_id, msg.turn_id].join("\u0000");
            if (msg.authoritative === false) {
              recoverableReads.retry(retryKey, () => {
                if (cancelled) return;
                if (stateRef.current.focusedSid !== msg.session_id) return;
                const current = stateRef.current.runtimes[msg.session_id];
                const turn = current?.turns.find((item) => item.id === msg.turn_id);
                if (!turn || turn.detailLoaded) return;
                const sent = ws.sendGetTurnDetail(
                  msg.session_id, msg.turn_id,
                  current.historyRevision ?? msg.revision,
                );
                if (sent) dispatch({
                  type: "turn_detail_requested", sid: msg.session_id,
                  turnId: msg.turn_id,
                });
              });
            } else {
              recoverableReads.complete(retryKey);
            }
          }
          if (msg.type === "rollback_result" && msg.files === "succeeded"
              && stateRef.current.artifact?.sid === msg.session_id) {
            // Diff/file previews are snapshots of bytes that have just changed.
            // Close them instead of leaving a convincing but stale panel open.
            dispatch({ type: "clear_artifact" });
          }
          if (msg.type === "rollback_result" && msg.prefill_text
              && stateRef.current.focusedSid === msg.session_id) {
            setEditPrompt(msg.prefill_text);
          }
          if (msg.type === "btw_opened") {
            const disposition = classifyBtwOpened(
              pendingBtwRef.current, activeBtwRef.current, msg);
            if (disposition === "duplicate") {
              return; // cached replay after a lost ACK; the fork is already open
            }
            if (disposition === "stale") {
              // The user cancelled, navigated, or started a newer request while
              // this fork was connecting. Never let the stale response open the
              // panel, and tear down the now-unowned ephemeral session.
              const discarded = discardedBtwSidsRef.current;
              discarded.add(msg.btw_sid);
              while (discarded.size > 64) {
                const oldest = discarded.values().next().value as string | undefined;
                if (!oldest) break;
                discarded.delete(oldest);
              }
              ws.sendCloseBtw(msg.btw_sid);
              return;
            }
            pendingBtwRef.current = null;
            activeBtwRef.current = {
              requestId: msg.request_id,
              sid: msg.btw_sid,
            };
            setBtwOpening(false);
          } else if (msg.type === "error" && msg.request_id
              && btwRequestIdsRef.current.has(msg.request_id)) {
            const matches = matchesBtwRequest(
              pendingBtwRef.current, msg.request_id);
            if (!matches) return; // obsolete /btw failure; keep any newer spinner
            pendingBtwRef.current = null;
            setBtwOpening(false);
          }
          if (msg.type === "session_forked") {
            const pendingMessageFork = pendingSessionForkRef.current;
            const matchesMessageFork = msg.target === "same_cwd"
              && matchesSessionForkRequest(
              pendingMessageFork, msg.request_id,
              msg.parent_session_id, msg.last_turn_id);
            const matchesWorktreeFork = msg.target === "worktree"
              && matchesWorktreeForkRequest(
              pendingWorktreeForkRef.current, msg.request_id,
              msg.parent_session_id);
            if (!matchesMessageFork && !matchesWorktreeFork) return;
            const targetEngine = matchesMessageFork
              ? pendingMessageFork!.engine
              : "codex";
            if (matchesMessageFork) {
              pendingSessionForkRef.current = null;
              setForkingPointId(null);
            }
            if (matchesWorktreeFork) {
              pendingWorktreeForkRef.current = null;
              setForkWorktreeCreating(false);
              setForkWorktreeError(null);
              setForkWorktreeSession(null);
            }
            setEngine(targetEngine);
            setSpace("code");
            dispatch({ type: "exit_new_chat" });
            dispatch({ type: "focus_session", sid: msg.session_id });
            ws.setSessionEngines([{ session_id: msg.session_id, engine: targetEngine, space: "code" }]);
            ws.setFocusedSid(msg.session_id, targetEngine, "code");
            ws.sendListSessions(targetEngine, "code");
            requestHistory(
              msg.session_id, undefined, HISTORY_INITIAL_PAGE);
            ws.sendSwitchSession(msg.session_id, targetEngine, "code");
            if (isMobile()) setSidebarOpen(false);
            return;
          }
          if (msg.type === "error" && isTerminalWorktreeForkError(msg.code)
              && matchesSessionForkRequest(
                pendingSessionForkRef.current, msg.request_id)) {
            pendingSessionForkRef.current = null;
            setForkingPointId(null);
            dispatch({ type: "command_error", detail: presentCommandProblem(msg) });
            return;
          }
          if (msg.type === "error" && isTerminalWorktreeForkError(msg.code)
              && matchesWorktreeForkRequest(
              pendingWorktreeForkRef.current, msg.request_id)) {
            pendingWorktreeForkRef.current = null;
            setForkWorktreeCreating(false);
            setForkWorktreeError(presentCommandProblem(msg));
            return;
          }
          const createResponseRequestId = (msg.type === "session_focus"
              || msg.type === "error") ? msg.request_id : null;
          const createRequest = createResponseRequestId
            ? createRequestsRef.current.get(createResponseRequestId) : undefined;
          if (createRequest && (msg.type === "session_focus"
              || (msg.type === "error" && msg.code !== "wrapper_offline"))) {
            createRequestsRef.current.delete(createResponseRequestId!);
            if (createResponseRequestId !== pendingCreateRef.current) return;
            pendingCreateRef.current = null;
            const currentScopeKey = sessionScopeKey(
              machineId, engineRef.current, spaceRef.current);
            if (createRequest.scopeKey !== currentScopeKey) return;
            if (msg.type === "session_focus") {
              setCreateError(null);
              dispatch({ type: "exit_new_chat" });
            } else {
              if (msg.code === "invalid_cwd"
                  && createRequest.cwdSource === "inherited") {
                dispatch({
                  type: "clear_scope_cwd",
                  scopeKey: createRequest.scopeKey,
                });
                dispatch({
                  type: "set_new_chat_cwd",
                  cwd: "~",
                  cwdSource: "default",
                });
              }
              setCreateError(presentCommandProblem(msg));
              return;
            }
          }
          if (msg.type === "snapshot") {
            if (consumeDiscardedBtwSnapshot(discardedBtwSidsRef.current, msg)) return;
            handleSnapshot(msg, ownership);
            return;
          }
          if (msg.type === "session_rekey") {
            if (ownership) {
              composerDraftsRef.current.rekey(
                composerDraftKey(
                  ownership.machineId, ownership.space, ownership.engine,
                  msg.old_key,
                ),
                composerDraftKey(
                  ownership.machineId, ownership.space, ownership.engine,
                  msg.session_id,
                ),
              );
            }
            setWorkArtifactsBySid((current) => {
              const prior = current[msg.old_key];
              if (!prior) return current;
              const next = { ...current, [msg.session_id]: prior };
              delete next[msg.old_key];
              return next;
            });
            if (stateRef.current.focusedSid === msg.old_key
                && ownership?.engine === engineRef.current
                && ownership.space === spaceRef.current) {
              // The reducer has already got enough correlated metadata to paint
              // a temp sidebar row. Rekey is the durability boundary: refresh
              // the active surface so its title/status comes from the native
              // catalog without making the user toggle or reload the page.
              ws.sendListSessions(engineRef.current, spaceRef.current);
              if (spaceRef.current === "work") {
                ws.sendGetWorkArtifacts(engineRef.current, msg.session_id);
              }
            }
            setGoalUiBySid((current) => {
              const prior = current[msg.old_key];
              if (!prior) return current;
              const next = { ...current, [msg.session_id]: prior };
              delete next[msg.old_key];
              return next;
            });
          }
          if (msg.type === "session_list") {
            ws.setSessionEngines(msg.sessions);
            const listedSpace = msg.space ?? "code";
            const surfaceKey = `${listedSpace}:${msg.engine}`;
            sessionListsBySurfaceRef.current[surfaceKey] = msg.sessions.map(
              (session) => ({ ...session, engine: msg.engine, space: listedSpace }),
            );
            bumpNativeCatalogRevision();
            authoritativeSurfaceListsRef.current.add(surfaceKey);
            prefetchedSurfacesRef.current.add(surfaceKey);
            // Warm the sibling Work/Code surface once per page lifetime. Codex
            // reuses the just-read native catalog in the wrapper, so this does
            // not start a second app-server and the user's first toggle is fast.
            const siblingSpace: Space = listedSpace === "work" ? "code" : "work";
            const siblingKey = `${siblingSpace}:${msg.engine}`;
            if (msg.engine === engineRef.current
                && !prefetchedSurfacesRef.current.has(siblingKey)) {
              prefetchedSurfacesRef.current.add(siblingKey);
              ws.sendListSessions(msg.engine, siblingSpace);
            }
          }
          if (msg.type === "session_activity") {
            for (const [surfaceKey, listed] of Object.entries(
              sessionListsBySurfaceRef.current,
            )) {
              if (!surfaceKey.endsWith(`:${msg.engine}`)) continue;
              sessionListsBySurfaceRef.current[surfaceKey] = listed.map(
                (session) => session.session_id === msg.session_id
                  ? { ...session, state: msg.state }
                  : session,
              );
            }
            bumpNativeCatalogRevision();
          }
          if (msg.type === "work_dashboard") {
            setWorkDashboards((current) => ({ ...current, [msg.engine]: msg }));
            setWorkProjectId((current) => current && msg.projects.some(
              (project) => project.project_id === current) ? current : null);
          }
          if (msg.type === "work_artifacts") {
            setWorkArtifactsBySid((current) => ({
              ...current, [msg.session_id]: msg.artifacts,
            }));
          }
          if (msg.type === "engine_capabilities") {
            setCapabilitiesBySurface((current) => ({
              ...current, [`${msg.space}:${msg.engine}`]: msg,
            }));
            if (msg.space === spaceRef.current && msg.engine === engineRef.current) {
              setCapabilitiesLoading(false);
            }
          }
          if (msg.type === "turn_end" && msg.sid && document.hidden
              && localStorage.getItem(NOTIFY_KEY) === "1"
              && !remotePushActiveRef.current
              && typeof Notification !== "undefined"
              && Notification.permission === "granted") {
            const session = stateRef.current.sessions.find(
              (candidate) => candidate.session_id === msg.sid);
            const label = session?.engine === "codex" ? "Codex" : "Claude";
            const body = turnNotificationBody(label, msg.result);
            void navigator.serviceWorker?.ready.then((registration) =>
              registration.showNotification("cc-remote", {
                body, icon: "/icon-192.png", badge: "/favicon.svg",
                tag: turnNotificationTag(msg), data: { url: "/" },
              })).catch(() => undefined);
          }
          if (msg.type === "session_focus" && spaceRef.current === "work"
              && !msg.session_id.startsWith("tmp-")) {
            ws.sendGetWorkArtifacts(engineRef.current, msg.session_id);
          }
          if (msg.type === "session_list"
              && !shouldAcceptSessionList(engineRef.current, spaceRef.current, msg)) return;
          if (msg.type === "session_list") {
            const currentSid = stateRef.current.focusedSid;
            if (currentSid && !currentSid.startsWith("tmp-")
                && !stateRef.current.pendingCatalogSids.includes(currentSid)
                && !msg.sessions.some((session) => session.session_id === currentSid)) {
              didInitFocusRef.current = false;
              preferredSurfaceFocusRef.current = null;
            }
          }
          if ((msg.type === "turn_end"
              || (msg.type === "error" && msg.code !== "wrapper_offline"))
              && msg.sid) {
            draining.delete(msg.sid);
          }
          dispatch({ type: "event", event: msg, ownership });
          if (msg.type === "wrapper_reconnected") {
            if (hasNativePagerHost()) {
              ws.sendListSessions("claude", "code");
              ws.sendListSessions("codex", "code");
              if (spaceRef.current === "work") {
                ws.sendListSessions(engineRef.current, "work");
              }
            } else {
              ws.sendListSessions(engineRef.current, spaceRef.current);
            }
            if (spaceRef.current === "work") {
              ws.sendGetWorkDashboard(engineRef.current);
            }
            ws.sendGetModels("codex");
            const currentSid = stateRef.current.focusedSid;
            if (currentSid) requestHistory(
              currentSid, undefined, HISTORY_INITIAL_PAGE, msg.generation);
          }
          // refresh the context ring after each turn (local SDK query, no model tokens)
          if (msg.type === "turn_end" && msg.sid) {
            ws.sendGetContextTo(msg.sid);
            if (sessionActivityPendingRef.current.delete(msg.sid)) {
              const listed = Object.values(sessionListsBySurfaceRef.current)
                .flat().find((session) => session.session_id === msg.sid);
              ws.sendListSessions(
                (listed?.engine as Engine | undefined) ?? engineRef.current,
                listed?.space ?? spaceRef.current,
              );
            }
            const session = stateRef.current.sessions.find(
              (candidate) => candidate.session_id === msg.sid);
            if (session?.space === "work"
                || (spaceRef.current === "work" && stateRef.current.focusedSid === msg.sid)) {
              ws.sendGetWorkArtifacts(
                (session?.engine as Engine | undefined) ?? engineRef.current, msg.sid);
            }
          }
        },
        onConnState: (s, detail) => {
          dispatch({ type: "conn", connState: s, detail });
          if (s === "connected") {
            recoverableReads.clear();
            historyRequestsRef.current.beginConnection();
            // The native dashboard is engine-neutral. Warm both Code catalogs
            // on the same socket; the active Web surface still accepts only its
            // own response into the reducer.
            if (hasNativePagerHost()) {
              ws.sendListSessions("claude", "code");
              ws.sendListSessions("codex", "code");
              if (spaceRef.current === "work") {
                ws.sendListSessions(engineRef.current, "work");
              }
            } else {
              ws.sendListSessions(engineRef.current, spaceRef.current);
            }
            if (spaceRef.current === "work") {
              ws.sendGetWorkDashboard(engineRef.current);
              const currentSid = stateRef.current.focusedSid;
              if (currentSid) ws.sendGetWorkArtifacts(engineRef.current, currentSid);
            }
            // Always fetch codex's catalog, not just when codex is the active engine:
            // the engine pill switches instantly and must render real models/efforts.
            // The wrapper caches it, so a refresh doesn't respawn an app-server.
            ws.sendGetModels("codex");
          }
        },
        onAuthFail: () => {
          setAuthReady(false);
          clearLegacyAuthMarkers(localStorage);
          localStorage.removeItem(PAIRED_CLIENT_ID_KEY);
          pendingCreateRef.current = null;
          createRequestsRef.current.clear();
          pendingBtwRef.current = null;
          pendingSessionForkRef.current = null;
          pendingWorktreeForkRef.current = null;
          activeBtwRef.current = null;
          btwRequestIdsRef.current.clear();
          discardedBtwSidsRef.current.clear();
          historyInvalidationsRef.current.clear();
          historyCacheEpochRef.current.clear();
          inlineImageAssetsRef.current.clear();
          historyImageAssetsRef.current.clear();
          bumpInlineImageRevision();
          bumpHistoryImageRevision();
          historyRequestsRef.current.clear();
          setBtwOpening(false);
          setForkingPointId(null);
          setForkWorktreeSession(null);
          setForkWorktreeCreating(false);
          setForkWorktreeError(null);
          setGoalUiBySid({});
          setStatusOpenSid(null);
          setWorkArtifactsOpen(false);
          setWorkArtifactsBySid({});
          dispatch({ type: "reset" });
          setAuthed(false);
          void (async () => {
            try { await import("./cache").then((module) => module.clearCache()); }
            catch { /* best-effort local cleanup */ }
            setAuthReady(true);
          })();
        },
        onCommandError: (detail) => dispatch({ type: "command_error", detail }),
        onOutboxChanged: (protectedSids) => {
          dispatch({ type: "prune_runtimes", protectedSids });
        },
        onWrapperGenerationChanged: () => {
          inlineImageAssetsRef.current.clear();
          historyImageAssetsRef.current.clear();
          bumpInlineImageRevision();
          bumpHistoryImageRevision();
          discardedBtwSidsRef.current.clear();
          if (stateRef.current.btwSid || pendingBtwRef.current
              || activeBtwRef.current) {
            pendingBtwRef.current = null;
            activeBtwRef.current = null;
            setBtwOpening(false);
            if (stateRef.current.btwSid) dispatch({ type: "clear_btw" });
            dispatch({ type: "command_error",
              detail: "服务已重新连接，临时 /btw 会话已关闭，请重新打开。" });
          }
        },
      }, machineId);
      ws.setSurface(engineRef.current, spaceRef.current);
      // Seed both transport and reducer watermarks before Hello. This prevents
      // an older replay/snapshot from reviving a lock already superseded in the
      // last authoritative control snapshot.
      ws.seedReplayState(seeded.cursors, seeded.generations, seeded.controls);
      for (const [sid, control] of Object.entries(seeded.controls)) {
        dispatch({
          type: "hydrate_cache", sid, turns: [], revision: null,
          generation: seeded.generations[sid] ?? control.generation, control,
        });
      }
      wsRef.current = ws;
      ws.start();
    })();

    return () => {
      cancelled = true;
      wsRef.current?.stop();
      wsRef.current = null;
      historyRequests.clear();
      recoverableReads.clear();
      draining.clear();
    };
  }, [authed, machineId, requestHistory]);

  // Land on the preferred/recent session only after an accepted list for the
  // active engine+space arrives. Background snapshots never pick focus.
  useEffect(() => {
    if (didInitFocusRef.current || !wsRef.current) return;
    const surfaceKey = `${spaceRef.current}:${engineRef.current}`;
    if (!authoritativeSurfaceListsRef.current.has(surfaceKey)) return;
    if (state.sessions.length === 0) {
      preferredSurfaceFocusRef.current = null;
      didInitFocusRef.current = true;
      return;
    }
    const preferred = preferredSurfaceFocusRef.current?.key === surfaceKey
      ? state.sessions.find((session) => (
          session.session_id === preferredSurfaceFocusRef.current?.sid
          && (session.space ?? "code") === spaceRef.current
          && (session.engine ?? "claude") === engineRef.current
        ))
      : undefined;
    preferredSurfaceFocusRef.current = null;
    const latest = preferred ?? [...state.sessions]
      .filter((s) => s.tag !== "archived")
      .sort(compareSessionsByActivity)[0]
      ?? state.sessions[0];
    didInitFocusRef.current = true;
    if (latest && latest.session_id !== state.focusedSid) {
      dispatch({ type: "exit_new_chat" });
      dispatch({ type: "focus_session", sid: latest.session_id });
      const latestEngine = (latest.engine as "claude" | "codex") || engineRef.current;
      wsRef.current.setFocusedSid(latest.session_id, latestEngine, spaceRef.current);
      requestHistory(
        latest.session_id, undefined, HISTORY_INITIAL_PAGE);
      wsRef.current.sendSwitchSession(latest.session_id, latestEngine, spaceRef.current);
    }
  }, [state.sessions, state.focusedSid, requestHistory]);

  // Direct sidebar selection and newly-created sessions both update the
  // per-surface bookmark. A later Work/Code or engine toggle can therefore
  // restore the exact view without relying on whichever list row happens to be
  // newest at that moment.
  useEffect(() => {
    if (!focusedSid || state.newChat) return;
    const selected = state.sessions.find((session) => session.session_id === focusedSid);
    if (!selected) return;
    const selectedEngine = (selected.engine as Engine | undefined) ?? engine;
    const selectedSpace: Space = selected.space === "work" ? "work" : "code";
    lastFocusBySurfaceRef.current[`${selectedSpace}:${selectedEngine}`] = focusedSid;
  }, [focusedSid, state.newChat, state.sessions, engine]);

  // Drain every resident session, not just the one currently visible. A queued
  // background turn must resume when that runtime becomes idle even if the user
  // has switched elsewhere. Never remove work merely because the socket or
  // wrapper is offline; accepted commands are retained by RelayWs's outbox.
  useEffect(() => {
    const draining = drainingRef.current;
    for (const sid of draining) {
      const runtime = state.runtimes[sid];
      if (!runtime || runtime.state !== "idle") draining.delete(sid);
    }

    const ws = wsRef.current;
    if (!ws) return;
    const candidates = selectDrainCandidates(
      state.runtimes,
      draining,
      state.connState === "connected",
      state.wrapperOnline,
    );
    for (const { sid, source, query } of candidates) {
      const msg_id = uuid();
      if (!ws.sendQueryTo(sid, query.prompt, msg_id, query.images, query.files)) continue;
      draining.add(sid);
      dispatch({ type: "query_sent", sid, prompt: query.prompt, msg_id,
        images: query.images, files: query.files, ts: Date.now() });
      if (source === "pending") dispatch({ type: "clear_pending", sid });
      else dispatch({ type: "dequeue_at", sid, i: 0 });
    }
  }, [state.runtimes, state.connState, state.wrapperOnline]);

  // Keep a long-lived tab bounded without evicting anything that can still be
  // acted on. ACK callbacks run the same prune when an outbox target becomes
  // reclaimable; otherwise an idle runtime protected during retry would linger.
  useEffect(() => {
    if (Object.keys(state.runtimes).length <= MAX_RUNTIME_SESSIONS) return;
    dispatch({
      type: "prune_runtimes",
      protectedSids: wsRef.current?.pendingSessionIds() ?? [],
    });
  }, [state.runtimes, focusedSid, state.btwSid, state.artifact?.sid]);

  // Persist the focused session's turns to IndexedDB (Phase-2 will write through
  // background sessions too). Coalesced in cache.ts.
  useEffect(() => {
    const sid = rt.ccSessionId;
    const revision = rt.historyRevision;
    if (!sid || !revision
        || historyInvalidationsRef.current.has(sid)) return;
    import("./cache").then(({ saveSession }) => {
      const live = wsRef.current?.lastSeqFor(sid) || 0;
      saveSession(
        sid, rt.turns, live, revision,
        wsRef.current?.generationFor(sid),
        rt.control,
      );
    });
  }, [focusedSid, rt.turns, rt.ccSessionId, rt.historyRevision, rt.control]);

  // Paint IndexedDB before starting the newest-page network read.  A browser
  // can otherwise receive a very fast summary response in the same task that
  // opened the session, leaving no frame in which the local projection is
  // visible.  The next animation frame is the cache-first boundary; the wrapper
  // then validates/replaces it in the background. A 6s fallback clears the
  // spinner only when both cache and wrapper stay silent.
  useEffect(() => {
    const sid = focusedSid;
    if (!sid) return;
    let cancelled = false;
    let requestFrame: number | null = null;
    const cacheEpoch = historyCacheEpochRef.current.get(sid) ?? 0;
    void import("./cache").then(({ loadSession }) => loadSession(sid)).then((cached) => {
      const valid = !cancelled
          && cacheEpoch === (historyCacheEpochRef.current.get(sid) ?? 0)
          && !historyInvalidationsRef.current.has(sid)
          && cached && Array.isArray(cached.turns)
          && (cached.turns.length || cached.control);
      if (valid && cached) {
        dispatch({
          type: "hydrate_cache", sid,
          turns: (cached.turns as Turn[]).map((turn) => ({
            ...turn, detailLoading: false,
          })),
          revision: cached.revision,
          generation: cached.generation ?? cached.control?.generation,
          control: cached.control,
        });
      }
      if (!cancelled && state.connState === "connected") {
        requestFrame = window.requestAnimationFrame(() => {
          requestFrame = null;
          if (!cancelled) {
            requestHistory(sid, undefined, HISTORY_INITIAL_PAGE);
          }
        });
      }
    });
    const t = window.setTimeout(() => dispatch({
      type: "hydrate_cache", sid, turns: [], revision: null,
    }), 6000);
    return () => {
      cancelled = true;
      window.clearTimeout(t);
      if (requestFrame != null) window.cancelAnimationFrame(requestFrame);
    };
  }, [focusedSid, requestHistory, state.connState]);

  // Cmd/Ctrl+B => toggle sidebar; Cmd/Ctrl+Shift+B => open latest turn's diff
  useEffect(() => {
    if (!authed) return;
    const onKey = (e: KeyboardEvent) => {
      if (!(e.metaKey || e.ctrlKey)) return;
      const k = e.key.toLowerCase();
      if (k === "b" && e.shiftKey) {           // diff (shared right slot)
        e.preventDefault();
        const latest = shortcutRef.current;
        if (latest.artifact?.kind === "gitdiff" && latest.rightView === "diff") dispatch({ type: "clear_artifact" });
        else latest.getDiff("");
      } else if (k === "b") {                    // toggle sidebar
        e.preventDefault();
        setSidebarOpen((v) => !v);
      } else if (k === "k" && e.shiftKey) {      // /btw side panel (shared right slot)
        e.preventDefault();
        const latest = shortcutRef.current;
        if (latest.btwSid && latest.rightView === "btw") latest.closeBtw();
        else latest.openBtw();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [authed]);

  // Shift+Tab follows each engine's real mode control: Claude cycles permission
  // modes; Codex toggles collaboration mode without touching approvalPolicy.
  useEffect(() => {
    if (!authed) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Tab" && e.shiftKey) {
        e.preventDefault();
        if ((rt.control && sessionControlLocksInput(rt.control))
            || (!rt.control && rt.external)) return;
        if (focusedEngine === "codex") {
          setCollaborationMode(
            rt.collaborationMode === "plan" ? "default" : "plan");
          return;
        }
        const modes = permsFor(focusedEngine).map((p) => p.id);
        const current = modes.indexOf(rt.perm);
        setPerm(modes[current < 0 ? 0 : (current + 1) % modes.length]);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [authed, focusedSid, rt.perm, rt.collaborationMode, rt.control,
    rt.external, focusedEngine]);

  // ---- /btw effects ----
  // These MUST stay ABOVE the `!authed` early return below. Hooks have to run
  // unconditionally and in the same order on every render; putting them after the
  // return meant logging out (authed -> false) rendered fewer hooks than the
  // previous render, and React blew up with #300 ("rendered fewer hooks than
  // expected"). Logging back in tripped the mirror image. Refreshing "fixed" it
  // only because a fresh mount has no previous render to disagree with.
  //
  // A /btw fork belongs to the session it was forked from. When you switch session
  // or toggle engine, discard it — else a codex btw would linger while you view a
  // cc session ("cc shows codex btw"). Read via ref so opening btw (no focus
  // change) doesn't trip this, and it only fires on actual navigation.
  const btwSidRef = useRef<string | null>(null);
  btwSidRef.current = state.btwSid;
  useEffect(() => {
    pendingBtwRef.current = null;
    activeBtwRef.current = null;
    setBtwOpening(false);
    const s = btwSidRef.current;
    if (s) { wsRef.current?.sendCloseBtw(s); dispatch({ type: "clear_btw" }); }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [focusedSid, engine]);

  if (!authReady) {
    return <div className="login" aria-busy="true">正在连接中继…</div>;
  }

  if (!authed) {
    return <LoginForm onLogin={() => { dispatch({ type: "reset" }); setAuthed(true); }} theme={theme} onToggleTheme={toggleTheme} />;
  }

  const sendQuery = (prompt: string, images?: QueryImg[], files?: QueryFile[]): boolean => {
    const ws = wsRef.current;
    if (!ws || !focusedSid) return false;
    const query = { prompt, images, files };
    const currentState = stateRef.current;
    if (ws.pendingQueryFor(focusedSid)
        || currentState.runtimes[focusedSid]?.acceptancePending) {
      const waiting = collectWaitingQueries(
        currentState.runtimes,
        currentState.sendMode === "interrupt" ? focusedSid : undefined,
      );
      if (!canEnqueueQuery(waiting, query)) {
        dispatch({
          type: "command_error",
          detail: "排队已满（最多 32 条 / 64 MiB），请先等待发送。",
        });
        return false;
      }
      dispatch(currentState.sendMode === "queue"
        ? { type: "enqueue", sid: focusedSid, query }
        : { type: "set_pending", sid: focusedSid, query });
      return true;
    }
    const msg_id = uuid();
    if (!ws.sendQueryTo(focusedSid, prompt, msg_id, images, files)) return false;
    drainingRef.current.add(focusedSid);
    const activityMs = Date.now();
    const surfaceKey = `${space}:${engine}`;
    const cached = sessionListsBySurfaceRef.current[surfaceKey];
    if (cached) {
      sessionListsBySurfaceRef.current[surfaceKey] = bumpSessionActivity(
        cached, focusedSid, activityMs);
      bumpNativeCatalogRevision();
    }
    sessionActivityPendingRef.current.add(focusedSid);
    dispatch({ type: "query_sent", sid: focusedSid, prompt, msg_id, images, files,
      ts: activityMs });
    return true;
  };
  // One command creates the session and starts its first query atomically. The
  // wrapper targets the new temp-keyed ctx directly; no later focus event is used
  // to route or trigger this message.
  const sendFirstMessage = (prompt: string, images?: QueryImg[], files?: QueryFile[],
                            collaborationMode?: CollaborationModeName,
                            permissionMode?: CodexPermissionMode,
                            serviceTier?: CodexServiceTier): boolean => {
    if (!wsRef.current || !state.newChat) return false;
    const { cwd, cwdSource, model, effort } = state.newChat;
    // Null is meaningful: let the local CLI/app-server use its configured defaults.
    // Only explicit user choices cross the wire; otherwise a stale fallback catalog
    // could silently override the machine's real model or reasoning configuration.
    const msg_id = uuid();
    const queued = wsRef.current.sendNewSession(
      space === "work" ? null : cwd, engine, model, effort,
      { prompt, msg_id, images, files },
      engine === "codex" ? collaborationMode : undefined,
      engine === "codex"
        ? (space === "work" ? "on-request" : permissionMode)
        : undefined,
      engine === "codex" ? serviceTier : undefined,
      space, space === "work" ? workProjectId : undefined);
    if (queued) {
      pendingCreateRef.current = msg_id;
      createRequestsRef.current.set(msg_id, {
        scopeKey: sessionScopeKey(machineId, engine, space),
        cwdSource,
      });
      while (createRequestsRef.current.size > 64) {
        const oldest = createRequestsRef.current.keys().next().value;
        if (!oldest) break;
        createRequestsRef.current.delete(oldest);
      }
      setCreateError(null);
    }
    return queued;
  };
  const interrupt = () => wsRef.current?.sendInterrupt();
  const setModel = (model: string) => {
    wsRef.current?.sendSetModel(model);
  };
  const setEffort = (effort: string) => {
    wsRef.current?.sendSetEffort(effort);
  };
  // Codex Fast mode is persisted by app-server per thread. The runtime's Fast
  // event owns the chip state; here we only forward the requested transition.
  const setServiceTier = (tier: string) => {
    wsRef.current?.sendSetServiceTier(tier);
  };
  const setPerm = (perm: string) => {
    wsRef.current?.sendSetPerm(perm);
  };
  const setCollaborationMode = (mode: CollaborationModeName) => {
    wsRef.current?.sendSetCollaborationMode(mode);
  };
  const setGoalUi = (patch: Partial<{ revealed: boolean; open: boolean }>) => {
    if (!focusedSid) return;
    setGoalUiBySid((current) => {
      const previous = current[focusedSid] ?? { revealed: false, open: false };
      return { ...current, [focusedSid]: { ...previous, ...patch } };
    });
  };
  const runGoal = (args: string) => {
    if (!focusedSid) return;
    const command = parseGoalCommand(args);
    if (command.kind === "clear") {
      wsRef.current?.sendClearGoal();
      setGoalUi({ revealed: false, open: false });
      return;
    }
    setGoalUi({ revealed: true, open: true });
    if (command.kind === "show") wsRef.current?.sendGetGoal();
    else wsRef.current?.sendSetGoal(command.objective, "active", null);
  };
  const openStatus = () => {
    if (!focusedSid) return;
    setStatusOpenSid(focusedSid);
    const requestId = wsRef.current?.sendGetStatus();
    if (requestId) {
      dispatch({ type: "begin_status_request", sid: focusedSid, requestId });
    }
  };
  const requestContext = () => {
    if (!focusedSid) return;
    const requestId = wsRef.current?.sendGetContext();
    if (requestId) {
      dispatch({ type: "begin_context_request", sid: focusedSid, requestId });
    }
  };
  const forkFromTurn = (forkPointId: string) => {
    if (!focusedSid
        || pendingSessionForkRef.current || pendingWorktreeForkRef.current) return;
    const requestId = wsRef.current?.sendForkSession(
      focusedSid, forkPointId) ?? null;
    if (!requestId) {
      dispatch({ type: "command_error",
        detail: "派生请求未发送，请等待连接恢复后重试。" });
      return;
    }
    pendingSessionForkRef.current = {
      requestId,
      parentSessionId: focusedSid,
      forkPointId,
      engine: focusedEngine,
    };
    setForkingPointId(forkPointId);
  };
  const openForkWorktree = (session: SessionInfo) => {
    if (pendingSessionForkRef.current || pendingWorktreeForkRef.current) return;
    setForkWorktreeError(null);
    setForkWorktreeSession(session);
  };
  const submitForkWorktree = (name: string) => {
    const source = forkWorktreeSession;
    if (!source || pendingWorktreeForkRef.current) return;
    setForkWorktreeError(null);
    const requestId = wsRef.current?.sendForkSessionWorktree(source.session_id, name) ?? null;
    if (!requestId) {
      setForkWorktreeError("请求未发送，请等待连接恢复后重试。");
      return;
    }
    pendingWorktreeForkRef.current = {
      requestId,
      parentSessionId: source.session_id,
    };
    setForkWorktreeCreating(true);
  };
  const closeForkWorktree = () => {
    if (pendingWorktreeForkRef.current) return;
    setForkWorktreeSession(null);
    setForkWorktreeError(null);
  };
  const getDiff = (file: string) => {
    if (!confirmArtifactDiscard()) return;
    const requestId = wsRef.current?.sendGetDiff(file, theme) ?? null;
    if (!requestId) return;
    setRightView("diff");
    dispatch({ type: "open_artifact_loading", file, sid: focusedSid, requestId });
  };
  const openTurnDiff = (files: string[], diff: string) => {
    if (!diff || !confirmArtifactDiscard()) return;
    setRightView("diff");
    dispatch({ type: "set_artifact", artifact: {
      file: files.length === 1 ? files[0] : `本轮改动 · ${files.length} 个文件`,
      sid: focusedSid,
      kind: "gitdiff",
      sections: parseGitDiff(diff),
    } });
  };
  const previewFile = (file: string, line?: number) => {
    if (!focusedSid) return;
    if (!confirmArtifactDiscard()) return;
    const requestId = wsRef.current?.sendGetFilePreview(file) ?? null;
    if (!requestId) return;
    setRightView("diff");
    dispatch({
      type: "open_file_loading",
      file,
      sid: focusedSid,
      requestId,
      kind: isMarkdownPath(file) ? "md" : "file",
      line,
    });
  };
  const previewMarkdown = (file: string) => previewFile(file);
  const loadPreviewAsset = (file: string, previewId: string): boolean =>
    !!wsRef.current?.sendGetPreviewAsset(file, previewId);
  const saveMarkdown = (file: string, content: string, expectedSize: number,
                        expectedMtimeNs: string, expectedRevision: string): string | null => {
    const requestId = wsRef.current?.sendSaveMarkdown(
      file, content, expectedSize, expectedMtimeNs, expectedRevision) ?? null;
    if (requestId) dispatch({ type: "start_file_save", requestId, content });
    return requestId;
  };
  // /btw: fork the focused session into an ephemeral side panel (wrapper replies
  // BtwOpened → reducer opens the panel). Send/close target the fork by its sid.
  const openBtw = () => {
    if (!confirmArtifactDiscard()) return;
    setRightView("btw");
    if (!focusedSid || state.btwSid || pendingBtwRef.current) return;
    const requestId = wsRef.current?.sendOpenBtw(focusedSid) ?? null;
    if (!requestId) { setBtwOpening(false); return; }
    pendingBtwRef.current = requestId;
    const requestIds = btwRequestIdsRef.current;
    requestIds.add(requestId);
    while (requestIds.size > 64) {
      const oldest = requestIds.values().next().value as string | undefined;
      if (!oldest) break;
      requestIds.delete(oldest);
    }
    setBtwOpening(true);
  };
  const sendBtw = (prompt: string) => { if (state.btwSid) wsRef.current?.sendQueryTo(state.btwSid, prompt, uuid()); };
  const closeBtw = () => {
    pendingBtwRef.current = null;
    activeBtwRef.current = null;
    setBtwOpening(false);
    if (state.btwSid) {
      wsRef.current?.sendCloseBtw(state.btwSid);
      dispatch({ type: "clear_btw" });
    }
  };
  // Header tab switch between the two right-slot views (opening the target lazily).
  const switchRight = (v: "diff" | "btw") => {
    if (v === "diff") {
      setRightView("diff");
      if (!state.artifact) getDiff("");
    } else openBtw();
  };
  shortcutRef.current = {
    artifact: state.artifact, btwSid: state.btwSid, rightView,
    getDiff, openBtw, closeBtw,
  };
  const logout = async () => {
    try {
      const response = await fetch("/api/logout", {
        method: "POST", credentials: "same-origin", cache: "no-store",
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      await import("./cache").then((module) => module.clearCache());
      wsRef.current?.stop();
      pendingCreateRef.current = null;
      createRequestsRef.current.clear();
      pendingBtwRef.current = null;
      pendingSessionForkRef.current = null;
      pendingWorktreeForkRef.current = null;
      sessionActivityPendingRef.current.clear();
      activeBtwRef.current = null;
      btwRequestIdsRef.current.clear();
      discardedBtwSidsRef.current.clear();
      historyInvalidationsRef.current.clear();
      historyCacheEpochRef.current.clear();
      composerDraftsRef.current.clear();
      setCreateError(null);
      setForkingPointId(null);
      setForkWorktreeSession(null);
      setForkWorktreeCreating(false);
      setForkWorktreeError(null);
      dispatch({ type: "reset" });
      localStorage.removeItem(PAIRED_CLIENT_ID_KEY);
      setAuthed(false);
    } catch {
      dispatch({ type: "command_error", detail: "退出失败：服务暂不可用，请稍后重试" });
    }
  };
  const activeDevice = remoteDevices.find(
    (device) => device.machine_id === machineId);
  const activeDeviceOnline = state.connState === "connected" && state.wrapperOnline;
  // A native client can advance the transcript without a wrapper-owned turn.
  // Present that mirrored activity as running in every status surface while
  // leaving Composer on the authoritative write state (so a read-only App turn
  // never gains a Stop button it cannot actually control).
  const focusedSessionState = state.sessions.find(
    (session) => session.session_id === focusedSid)?.state;
  const effectiveState = mergeSessionActivityState(
    focusedSessionState, rt.state, rt.mirroredRunning,
  ) ?? rt.state;

  const selectSession = (id: string, candidate?: SessionInfo): boolean => {
    const selected = candidate
      ?? state.sessions.find((session) => session.session_id === id)
      ?? findNativeSession(sessionListsBySurfaceRef.current, id);
    if (!selected || !confirmArtifactDiscard()) return false;
    pendingCreateRef.current = null;
    setCreateError(null);
    setStatusOpenSid(null);
    setWorkArtifactsOpen(false);
    const selectedEngine = (selected.engine as "claude" | "codex") || engine;
    const selectedSpace: Space = selected.space === "work" ? "work" : "code";
    if (selectedEngine !== engine || selectedSpace !== space) {
      prepareSurfaceSwitch(selectedEngine, selectedSpace);
      preferredSurfaceFocusRef.current = {
        key: `${selectedSpace}:${selectedEngine}`,
        sid: id,
      };
      setEngine(selectedEngine);
      setSpace(selectedSpace);
    }
    dispatch({ type: "exit_new_chat" });
    dispatch({ type: "focus_session", sid: id });
    wsRef.current?.setFocusedSid(id, selectedEngine, selectedSpace);
    requestHistory(id, undefined, HISTORY_INITIAL_PAGE);
    wsRef.current?.sendSwitchSession(id, selectedEngine, selectedSpace);
    if (selectedSpace === "work") {
      wsRef.current?.sendGetWorkArtifacts(selectedEngine, id);
    }
    if (isMobile()) setSidebarOpen(false);
    return true;
  };

  const setPinned = (session: SessionInfo, pinned: boolean): void => {
    const target = sessionCommandTarget(session, engine, space);
    const surfaceKey = `${target.space}:${target.engine}`;
    const cached = sessionListsBySurfaceRef.current[surfaceKey];
    if (cached) {
      sessionListsBySurfaceRef.current[surfaceKey] = setSessionPinned(
        cached, session.session_id, pinned);
      bumpNativeCatalogRevision();
    }
    dispatch({ type: "set_session_pinned", sid: session.session_id, pinned });
    wsRef.current?.sendPinSession(
      session.session_id, pinned, target.engine, target.space);
  };

  const handleNativeCommand = (
    command: NativeCommandEnvelope,
  ): NativeCommandResult => {
    const action = command.action;
    if (action.kind === "refreshSessions") {
      wsRef.current?.sendListSessions("claude", "code");
      wsRef.current?.sendListSessions("codex", "code");
      return { accepted: true };
    }
    const session = state.sessions.find((item) => item.session_id === action.taskId)
      ?? findNativeSession(sessionListsBySurfaceRef.current, action.taskId);
    if (!session) return { accepted: false, message: "任务已不存在，请刷新看板" };
    if (action.kind === "focusTask") {
      if (artifactDirtyRef.current) {
        return { accepted: false, message: "聊天页有未保存的文件修改" };
      }
      return selectSession(action.taskId, session)
        ? { accepted: true } : { accepted: false, message: "无法切换任务" };
    }
    if (action.kind === "setPinned") {
      setPinned(session, action.pinned);
      return { accepted: true };
    }
    if (action.taskId !== focusedSid) {
      return { accepted: false, message: "请先打开该任务再执行操作" };
    }
    const runtime = state.runtimes[action.taskId];
    if (action.kind === "interruptTask") {
      const writable = runtime?.control
        ? runtime.control.write_state === "writable" : !runtime?.external;
      if (!runtime || runtime.state === "idle" || !writable) {
        return { accepted: false, message: "当前任务不可中断" };
      }
      wsRef.current?.sendInterrupt();
      return { accepted: true };
    }
    if (action.kind === "answerQuestion") {
      const question = runtime?.pendingQuestion;
      if (!question) return { accepted: false, message: "问题已失效" };
      wsRef.current?.sendAnswerQuestion(question.ask_id, action.answer);
      dispatch({ type: "answer_question" });
      return { accepted: true };
    }
    return { accepted: false, message: "不支持的操作" };
  };

  const nativeSessions = nativeCodeCatalog(sessionListsBySurfaceRef.current);
  // Reading the revision makes background catalog updates an explicit render
  // dependency without copying large task arrays into React state.
  void nativeCatalogRevision;
  const nativePagerSnapshot = projectNativePagerSnapshot(state, {
    machineId,
    sessions: nativeSessions,
  });

  return (
    <div className={"shell" + (sidebarOpen ? " sidebar-open" : "") + ((state.artifact || state.btwSid || btwOpening) ? " panel-open" : "")} onTouchStart={onTouchStart} onTouchEnd={onTouchEnd}>
      <NativePagerBridge snapshot={nativePagerSnapshot}
        onCommand={handleNativeCommand} />
      <SessionsSidebar
        open={sidebarOpen}
        space={space}
        onSpaceChange={switchSpace}
        sessions={state.sessions}
        liveStates={Object.fromEntries(state.sessions.map((session) => {
          const runtime = state.runtimes[session.session_id];
          return [session.session_id, mergeSessionActivityState(
            session.state,
            runtime?.state,
            runtime?.mirroredRunning,
          ) ?? "idle"];
        }))}
        activeSessionId={focusedSid}
        onSelect={(id) => { selectSession(id); }}
        onNew={() => { if (!confirmArtifactDiscard()) return; pendingCreateRef.current = null; setCreateError(null); setStatusOpenSid(null); setNewChatAutoFocus(true); wsRef.current?.setFocusedSid(null); dispatch({ type: "enter_new_chat", cwd: "~", cwdSource: "default" }); if (isMobile()) setSidebarOpen(false); }}
        onNewInDir={(cwd) => { if (!confirmArtifactDiscard()) return; pendingCreateRef.current = null; setCreateError(null); setStatusOpenSid(null); setNewChatAutoFocus(true); wsRef.current?.setFocusedSid(null); dispatch({ type: "enter_new_chat", cwd, cwdSource: "explicit" }); if (isMobile()) setSidebarOpen(false); }}
        onClose={() => setSidebarOpen(false)}
        onRename={(id, title) => wsRef.current?.sendRenameSession(id, title, engine, space)}
        onArchive={(id, archived) => { wsRef.current?.sendArchiveSession(id, archived, engine, space); }}
        onPin={setPinned}
        onDelete={(id) => {
          const warning = space === "work"
            ? "删除后将永久移除这项工作及其私有文件，确定继续吗？"
            : "删除后将永久移除这条会话历史；代码文件不会被删除，确定继续吗？";
          if (!window.confirm(warning)) return;
          const deleted = state.sessions.find(
            (session) => session.session_id === id);
          const target = deleted
            ? sessionCommandTarget(deleted, engine, space)
            : { engine, space };
          composerDraftsRef.current.delete(composerDraftKey(
            machineId, target.space, target.engine, id,
          ));
          if (focusedSid === id) dispatch({ type: "enter_new_chat", cwd: "~", cwdSource: "default" });
          wsRef.current?.sendDeleteSession(id, engine, space);
        }}
        onForkWorktree={openForkWorktree}
      />
      <DirPicker
        open={dirPickerOpen}
        path={state.dirPicker?.path ?? null}
        parent={state.dirPicker?.parent ?? null}
        dirs={state.dirPicker?.dirs ?? []}
        onBrowse={(p) => wsRef.current?.sendListDir(p)}
        onConfirm={(cwd) => { if (state.newChat) dispatch({ type: "set_new_chat_cwd", cwd, cwdSource: "explicit" }); setDirPickerOpen(false); }}
        onClose={() => setDirPickerOpen(false)}
      />
      <section className={`pane ${space}-pane`}>
        <header className={`c-head ${space}-head`}>
          <div className="titlewrap">
            <div className="ttl">
              <button className="surface-head-title" onClick={() => setSidebarOpen(true)}>
                <span className="surface-head-mark"><Icon name={space === "work" ? "work" : "code"} size={18} /></span>
                <span>{space === "work" ? "Work" : "Code"}</span>
              </button>
            </div>
            <div className="sub">{space === "work" ? "私有工作区 · " : ""}{rt.ccSessionId ? `session ${rt.ccSessionId.slice(0, 8)}` : "connected"}</div>
          </div>
          <span className={`hstat ${effectiveState}`}><span className="sd" />
            <span className="hstat-label">{effectiveState}</span></span>
          {space === "code" && focusedSid && !state.newChat && (
            <TerminalControl control={rt.control} engine={focusedEngine}
              availability={state.connState !== "connected" || !state.wrapperOnline
                ? "offline" : rt.replaying || !rt.syncReady ? "syncing" : "online"}
              legacyExternal={!rt.control && !!rt.external}
              legacyTakeoverPending={rt.takeoverPending}
              legacyMessage={rt.takeoverMessage}
              onTakeover={() => wsRef.current?.sendTakeover(focusedSid)} />
          )}
          <button className={`device-trigger${activeDeviceOnline ? " online" : ""}`}
            onClick={() => setDeviceSheetOpen(true)} aria-label="设备中心"
            title={`${activeDevice?.label ?? machineId} · ${activeDeviceOnline ? "在线" : "离线"}`}>
            <Icon name="devices" size={18} />
            <span>{activeDevice?.label ?? machineId}</span><i />
          </button>
          <button className="engine-toggle" onClick={toggleEngine} aria-label="切换新会话引擎"
            title="新建会话使用的引擎">{engine === "codex" ? "◇ Codex" : "✳ Claude"}</button>
          {typeof Notification !== "undefined" && <button
            className={`iconbtn header-notify${notificationsEnabled ? " notify-on" : ""}`}
            onClick={() => { void (async () => {
              if (notificationsEnabled) {
                localStorage.removeItem(NOTIFY_KEY);
                setNotificationsEnabled(false);
                remotePushActiveRef.current = false;
                await disableRemotePush();
                return;
              }
              const permission = await Notification.requestPermission();
              const enabled = permission === "granted";
              if (enabled) localStorage.setItem(NOTIFY_KEY, "1");
              else localStorage.removeItem(NOTIFY_KEY);
              setNotificationsEnabled(enabled);
              remotePushActiveRef.current = enabled
                ? await enableRemotePush(machineId) : false;
            })(); }} aria-label="完成提醒"
            title={notificationsEnabled ? "后台完成提醒已开启" : "开启后台完成提醒"}>
            <Icon name="notify" />
          </button>}
          <button className="iconbtn header-theme" onClick={toggleTheme} aria-label="切换主题">
            <Icon name={theme === "dark" ? "sun" : "moon"} />
          </button>
          <button className="iconbtn" onClick={() => void logout()}
            aria-label="退出登录" title="退出登录"><Icon name="logout" /></button>
        </header>

        <ReconnectBanner banner={state.banner} replaying={rt.replaying}
          truncated={rt.truncated}
          busy={state.connState !== "connected" || !state.wrapperOnline || rt.replaying}
          onDismiss={dismissBanner} />
        <NoticeStack notices={rt.notices}
          onDismiss={(noticeId) => {
            if (focusedSid) dispatch({ type: "dismiss_notice", sid: focusedSid, noticeId });
          }} />

        {state.newChat ? (
          <NewChatView cwd={state.newChat.cwd} space={space}
            createError={createError}
            autoFocus={newChatAutoFocus}
            engine={engine}
            workDashboard={workDashboards[engine] ?? null}
            selectedProjectId={workProjectId}
            onSelectProject={setWorkProjectId}
            onManageWork={() => setWorkManagerOpen(true)}
            onPickCwd={() => setDirPickerOpen(true)}
            onSend={sendFirstMessage} />
        ) : (
          <>
            <ChatView sid={focusedSid} turns={rt.turns} loading={!!rt.loading}
              surface={space}
              engine={focusedEngine} forkingPointId={forkingPointId}
              hasMore={!!rt.hasMore}
              historyRevision={rt.historyRevision}
              historyCursor={rt.oldestId}
              onLoadMore={() => focusedSid ? requestHistory(
                focusedSid, rt.oldestId, HISTORY_MORE_PAGE) : false}
              onLoadDetail={(turnId) => {
                if (!focusedSid) return;
                const sent = wsRef.current?.sendGetTurnDetail(
                  focusedSid, turnId, rt.historyRevision) ?? false;
                if (sent) dispatch({
                  type: "turn_detail_requested", sid: focusedSid, turnId,
                });
              }}
              onEdit={(prompt) => setEditPrompt(prompt)} onGetDiff={getDiff}
              onOpenTurnDiff={openTurnDiff}
              onPreviewMarkdown={previewMarkdown}
              onOpenFile={previewFile}
              onOpenArtifacts={() => {
                if (focusedSid) {
                  wsRef.current?.sendGetWorkArtifacts(focusedEngine, focusedSid);
                }
                setWorkArtifactsOpen(true);
              }}
              imageAssets={inlineImageAssets}
              onLoadImage={loadFocusedMessageImage}
              historyImageAssets={historyImageAssets}
              onLoadHistoryImage={loadHistoryImage}
              onFork={space === "code" ? forkFromTurn : undefined} />

            <GoalPanel engine={engine} goal={rt.goal}
              revealed={!!goalUi?.revealed} open={!!goalUi?.open}
              onOpen={() => { wsRef.current?.sendGetGoal(); setGoalUi({ revealed: true, open: true }); }}
              onClose={() => setGoalUi({ open: false })}
              onDismiss={() => setGoalUi({ revealed: false, open: false })}
              onSave={(objective, status, budget) => {
                wsRef.current?.sendSetGoal(objective, status, engine === "codex" ? budget : null);
                setGoalUi({ revealed: true, open: false });
              }}
              onClear={() => {
                wsRef.current?.sendClearGoal();
                setGoalUi({ revealed: false, open: false });
              }} />

            <Composer
          draftKey={focusedComposerDraftKey}
          draftStore={composerDraftsRef.current}
          surface={space}
          state={rt.state}
          catalog={state.catalog}
          connState={state.connState}
          wrapperOnline={state.wrapperOnline}
          syncReady={rt.syncReady}
          sendMode={state.sendMode}
          setSendMode={(m) => dispatch({ type: "set_send_mode", mode: m })}
          queue={rt.queue}
          allQueued={allQueued}
          replaceableQueued={replaceableQueued}
          model={rt.model}
          effort={rt.effort}
          perm={rt.perm}
          collaborationMode={rt.collaborationMode}
          fast={rt.fast}
          control={rt.control}
          external={rt.external}
          takeoverPending={rt.takeoverPending}
          takeoverMessage={rt.takeoverMessage}
          engine={focusedEngine}
          editPrompt={editPrompt}
          onEditConsumed={() => setEditPrompt(null)}
          onSendQuery={sendQuery}
          onInterrupt={interrupt}
          onEnqueue={(query) => dispatch({ type: "enqueue", query })}
          onSetPending={(query) => dispatch({ type: "set_pending", query })}
          onDequeue={(i) => { if (focusedSid) dispatch({ type: "dequeue_at", sid: focusedSid, i }); }}
          onSetModel={setModel}
          onSetEffort={setEffort}
          onSetServiceTier={setServiceTier}
          onSetPerm={setPerm}
          onSetCollaborationMode={setCollaborationMode}
          onClear={() => dispatch({
            type: "enter_new_chat",
            cwd: space === "work" ? "~" : (currentCwd || "~"),
            cwdSource: space === "work" || !currentCwd ? "default" : "inherited",
          })}
          onContext={requestContext}
          onOpenBtw={openBtw}
          onPreview={previewMarkdown}
          onGoal={runGoal}
          onStatus={openStatus}
          onReview={(target, value) => {
            if (focusedSid) wsRef.current?.sendStartReview(focusedSid, target, value);
          }}
          onCompact={() => {
            if (focusedSid) wsRef.current?.sendCompactSession(focusedSid);
          }}
          onOpenExtensions={(kind) => {
            setCapabilitiesKind(kind);
            setCapabilitiesOpen(true);
            setCapabilitiesLoading(true);
            wsRef.current?.sendGetEngineCapabilities(
              focusedEngine, space, state.newChat?.cwd ?? currentCwd);
          }}
          workArtifactCount={space === "work" ? currentWorkArtifacts.length : 0}
          onOpenArtifacts={() => {
            if (focusedSid) {
              wsRef.current?.sendGetWorkArtifacts(focusedEngine, focusedSid);
            }
            setWorkArtifactsOpen(true);
          }}
          contextReport={rt.contextReport}
          contextError={rt.contextError}
        />
          </>
        )}
        {/* context usage now lives in the composer's ring popover (see Composer) */}
      </section>
      {/* Shared right slot: diff and /btw take turns; header tabs switch. */}
      {(() => {
        const btwShowing = !!state.btwSid || btwOpening;
        const view = rightView === "btw" && btwShowing ? "btw"
          : state.artifact ? "diff" : btwShowing ? "btw" : null;
        if (view === "btw")
          return <BtwPanel sid={state.btwSid ?? undefined} rt={state.btwSid ? state.runtimes[state.btwSid] : undefined}
            engine={state.btwEngine} opening={btwOpening && !state.btwSid}
            active="btw" hasArtifact={!!state.artifact} artifactKind={state.artifact?.kind} onTab={switchRight}
            onSend={sendBtw} onOpenFile={previewFile} onClose={closeBtw}
            onDismissNotice={(noticeId) => {
              if (state.btwSid) dispatch({ type: "dismiss_notice", sid: state.btwSid, noticeId });
            }} />;
        if (view === "diff" && state.artifact)
          return <Suspense fallback={null}>
            <ArtifactPanel artifact={state.artifact} active="diff" hasBtw={!!state.btwSid}
              onTab={switchRight} onRefresh={previewFile}
              onOpenFile={previewFile} onLoadPreviewAsset={loadPreviewAsset}
              onSaveMarkdown={saveMarkdown} onDirtyChange={setArtifactDirty}
              onClose={() => dispatch({ type: "clear_artifact" })} />
          </Suspense>;
        return null;
      })()}
      {rt.pendingQuestion && (
        <QuestionSheet
          header={rt.pendingQuestion.header}
          question={rt.pendingQuestion.question}
          options={rt.pendingQuestion.options}
          allowText={rt.pendingQuestion.allow_text}
          secret={rt.pendingQuestion.secret}
          onAnswer={(answer) => {
            wsRef.current?.sendAnswerQuestion(rt.pendingQuestion!.ask_id, answer);
            dispatch({ type: "answer_question" });
          }}
        />
      )}
      <StatusSheet open={shouldOpenCodexStatus(statusOpenSid, focusedSid, focusedEngine)} report={rt.statusReport}
        notices={rt.notices}
        error={rt.statusError}
        onClose={() => setStatusOpenSid(null)}
        onRefresh={openStatus}
        onDismissNotice={(noticeId) => {
          if (focusedSid) dispatch({ type: "dismiss_notice", sid: focusedSid, noticeId });
        }} />
      <ForkWorktreeSheet open={forkWorktreeSession !== null} session={forkWorktreeSession}
        creating={forkWorktreeCreating} error={forkWorktreeError}
        onConfirm={submitForkWorktree} onClose={closeForkWorktree} />
      <WorkDashboardSheet open={workManagerOpen && space === "work"}
        dashboard={workDashboards[engine] ?? null}
        selectedProjectId={workProjectId}
        onSelectProject={setWorkProjectId}
        onClose={() => setWorkManagerOpen(false)}
        onCreateProject={(name, description) => !!wsRef.current?.sendCreateWorkProject(engine, name, description)}
        onDeleteProject={(projectId) => !!wsRef.current?.sendDeleteWorkProject(engine, projectId)}
        onAddSource={(projectId, kind, title, uri, file) => !!wsRef.current?.sendAddWorkSource(engine, projectId, kind, title, uri, file)}
        onDeleteSource={(sourceId) => !!wsRef.current?.sendDeleteWorkSource(engine, sourceId)}
        onCreateSchedule={(title, prompt, nextRunAt, repeatSeconds, projectId) => !!wsRef.current?.sendCreateWorkSchedule(engine, title, prompt, nextRunAt, repeatSeconds, projectId)}
        onDeleteSchedule={(scheduleId) => !!wsRef.current?.sendDeleteWorkSchedule(engine, scheduleId)}
        onCreatePlugin={(name, instructions, projectId) => !!wsRef.current?.sendCreateWorkPlugin(engine, name, instructions, projectId)}
        onDeletePlugin={(pluginId) => !!wsRef.current?.sendDeleteWorkPlugin(engine, pluginId)} />
      <WorkArtifactsSheet open={workArtifactsOpen && space === "work"
          && !state.newChat && currentWorkArtifacts.length > 0}
        artifacts={currentWorkArtifacts}
        onOpen={(path) => { setWorkArtifactsOpen(false); previewFile(path); }}
        onClose={() => setWorkArtifactsOpen(false)} />
      <CapabilitiesSheet open={capabilitiesOpen}
        engine={focusedEngine}
        activeKind={capabilitiesKind}
        readOnly={space === "work"}
        report={capabilitiesBySurface[`${space}:${focusedEngine}`] ?? null}
        loading={capabilitiesLoading}
        onKindChange={setCapabilitiesKind}
        onRefresh={() => {
          setCapabilitiesLoading(true);
          wsRef.current?.sendGetEngineCapabilities(
            focusedEngine, space, state.newChat?.cwd ?? currentCwd);
        }}
        onManagePlugin={(item, action) => {
          const verb = action === "install" ? "安装" : "卸载";
          if (!window.confirm(`${verb}插件「${item.name}」将修改本机 ${focusedEngine === "codex" ? "Codex" : "Claude"} 配置，确定继续吗？`)) return;
          setCapabilitiesLoading(true);
          wsRef.current?.sendManageEnginePlugin(
            focusedEngine, space, action, item.id,
            state.newChat?.cwd ?? currentCwd);
        }}
        onManageSkill={(item: EngineCapabilityItem, action) => {
          const labels = { enable: "启用", disable: "停用", remove: "删除" } as const;
          if (!window.confirm(`${labels[action]} Skill「${item.name}」？${action === "remove" ? "删除会移动到本机可恢复回收目录。" : ""}`)) return;
          setCapabilitiesLoading(true);
          wsRef.current?.sendManageEngineSkill(
            focusedEngine, space, action, { skillId: item.id },
            state.newChat?.cwd ?? currentCwd);
        }}
        onCreateSkill={(draft: SkillDraft) => {
          setCapabilitiesLoading(true);
          wsRef.current?.sendManageEngineSkill(
            focusedEngine, space, "create", draft,
            state.newChat?.cwd ?? currentCwd);
        }}
        onRemoveHook={(item: EngineCapabilityItem) => {
          if (!window.confirm(`删除 Hook「${item.name}」？配置文件中的其他内容会原样保留。`)) return;
          setCapabilitiesLoading(true);
          wsRef.current?.sendManageEngineHook(
            focusedEngine, space, "remove", { hookId: item.id },
            state.newChat?.cwd ?? currentCwd);
        }}
        onCreateHook={(draft: HookDraft) => {
          setCapabilitiesLoading(true);
          wsRef.current?.sendManageEngineHook(
            focusedEngine, space, "create", draft,
            state.newChat?.cwd ?? currentCwd);
        }}
        onClose={() => setCapabilitiesOpen(false)} />
      <DeviceSheet open={deviceSheetOpen}
        currentId={machineId}
        devices={remoteDevices}
        pairing={devicePairing}
        onDevices={(nextDevices, nextPairing) => {
          setRemoteDevices(nextDevices);
          setDevicePairing(nextPairing);
        }}
        onSelect={(nextMachineId) => {
          if (nextMachineId !== machineId) setMachineId(nextMachineId);
          setDeviceSheetOpen(false);
        }}
        onClose={() => setDeviceSheetOpen(false)} />
    </div>
  );
}
