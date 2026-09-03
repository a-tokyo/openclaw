import { uniqueStrings } from "@openclaw/normalization-core/string-normalization";
import { executeSqliteQuerySync } from "../../infra/kysely-sync.js";
import { runWithSqliteBusyTimeout } from "../../infra/sqlite-busy-timeout.js";
import { getChildLogger } from "../../logging/logger.js";
import {
  collectActiveSessionWorkAdmissions,
  runExclusiveSessionLifecycleMutation,
} from "../../sessions/session-lifecycle-admission.js";
import {
  openOpenClawAgentDatabase,
  resolveOpenClawAgentSqlitePath,
  runOpenClawAgentWriteTransaction,
  type OpenClawAgentDatabase,
} from "../../state/openclaw-agent-db.js";
import { measureSessionPhysicalDiskUsage, type SessionPhysicalDiskUsage } from "./disk-budget.js";
import { publishSessionStateArchives } from "./session-accessor.sqlite-archive-store.js";
import {
  materializeSessionStateDeletePlans,
  type SessionStateDeletePlan,
} from "./session-accessor.sqlite-archive.js";
import type { SessionLifecycleArchivedTranscript } from "./session-accessor.sqlite-contract.js";
import { readSessionEntryCount } from "./session-accessor.sqlite-entry-store.js";
import { emitArchivedTranscriptUpdates } from "./session-accessor.sqlite-events.js";
import { emitCommittedSessionEntryRemovals } from "./session-accessor.sqlite-identity.js";
import {
  assertPlannedLifecycleArtifactEntriesUnchanged,
  collectProjectedReferencedSessionIds,
  collectSessionStateIdsForEntry,
  deleteMaterializedSessionStatePlans,
  deletePlannedLifecycleArtifactEntries,
  planSessionStateDeleteIfUnreferenced,
  readSessionGenerationIdsForKeys,
} from "./session-accessor.sqlite-lifecycle-state.js";
import type {
  SessionEntryMaintenancePlan,
  SessionEntryMaintenanceResult,
  SessionEntryRemovalPlan,
} from "./session-accessor.sqlite-lifecycle-types.js";
import {
  cloneSessionEntry,
  getSessionKysely,
  runExclusiveSqliteSessionWrite,
  toDatabaseOptions,
  type ResolvedSqliteReadScope,
} from "./session-accessor.sqlite-scope.js";
import { parseSessionEntryJson as parseSessionEntryRow } from "./session-accessor.sqlite-status.js";
import { normalizeStoreSessionKey } from "./store-entry.js";
import {
  collectSessionMaintenancePreserveKeys,
  collectSessionMaintenancePreserveKeysForStore,
} from "./store-maintenance-preserve.js";
import { resolveMaintenanceConfig } from "./store-maintenance-runtime.js";
import {
  capEntryCount,
  pruneStaleModelRunEntries,
  pruneStaleEntries,
  shouldPreserveMaintenanceEntry,
  shouldRunModelRunPrune,
  shouldRunSessionEntryMaintenance,
  type ResolvedSessionMaintenanceConfig,
} from "./store-maintenance.js";
import type { SessionEntry } from "./types.js";

// Live-entry pruning owner. Produces plans inside writes; finalizes archives afterward.

const SESSION_PLANNER_ANALYSIS_MIN_DELETED_ENTRIES = 64;
const SESSION_PLANNER_ANALYSIS_LIMIT = 1_000;
const plannerMaintenanceByStore = new Map<string, Promise<void>>();

/** Coalesce bounded planner-statistics refreshes behind the per-store writer lane. */
export async function refreshSqliteSessionPlannerStatisticsBestEffort(
  scope: Pick<ResolvedSqliteReadScope, "agentId" | "env" | "path">,
  deletedEntries: number,
  options: { isCurrent?: () => boolean } = {},
): Promise<void> {
  const isCurrent = options.isCurrent ?? (() => true);
  if (deletedEntries < SESSION_PLANNER_ANALYSIS_MIN_DELETED_ENTRIES || !isCurrent()) {
    return;
  }
  const storePath = resolveOpenClawAgentSqlitePath(toDatabaseOptions(scope));
  const active = plannerMaintenanceByStore.get(storePath);
  if (active) {
    await active;
    return;
  }
  const completion = runExclusiveSqliteSessionWrite(scope, async () => {
    if (!isCurrent()) {
      return;
    }
    const database = openOpenClawAgentDatabase(toDatabaseOptions(scope));
    runWithSqliteBusyTimeout(database.db, 0, () => {
      const row = database.db.prepare("PRAGMA analysis_limit").get() as
        | { analysis_limit?: unknown }
        | undefined;
      const previousLimit = Number(row?.analysis_limit ?? 0);
      try {
        database.db.exec(
          `PRAGMA analysis_limit = ${SESSION_PLANNER_ANALYSIS_LIMIT}; ANALYZE main;`,
        );
      } finally {
        database.db.exec(`PRAGMA analysis_limit = ${previousLimit};`);
      }
    });
  })
    .catch((error: unknown) => {
      getChildLogger({ subsystem: "session-sqlite" }).warn(
        "SQLite session planner-statistics refresh failed",
        { agentId: scope.agentId, error, path: storePath },
      );
    })
    .finally(() => {
      plannerMaintenanceByStore.delete(storePath);
    });
  plannerMaintenanceByStore.set(storePath, completion);
  await completion;
}

function collectSqliteSessionMaintenanceBaseKeys(
  store: Record<string, SessionEntry>,
  activeSessionKey: string,
): string[] {
  const keys: string[] = [];
  const seen = new Set<string>();
  let currentKey = normalizeStoreSessionKey(activeSessionKey);
  while (currentKey && !seen.has(currentKey)) {
    seen.add(currentKey);
    keys.push(currentKey);
    currentKey = normalizeStoreSessionKey(store[currentKey]?.parentSessionKey ?? "");
  }
  return keys;
}

function hasStaleSqliteSessionEntryCandidate(
  database: OpenClawAgentDatabase,
  pruneAfterMs: number,
  preserveKeys: ReadonlySet<string> | undefined,
  preserveRecentMs: number | null,
): boolean {
  const cutoffMs = Date.now() - pruneAfterMs;
  const db = getSessionKysely(database.db);
  const rows = executeSqliteQuerySync(
    database.db,
    db
      .selectFrom("session_nodes")
      .select(["entry_json", "session_key"])
      .where("updated_at", "<", cutoffMs)
      .where("archived_at", "is", null)
      .orderBy("updated_at", "asc"),
  ).rows;
  return rows.some((row) => {
    const entry = parseSessionEntryRow(row);
    if (!entry) {
      return false;
    }
    return !shouldPreserveMaintenanceEntry({
      key: normalizeStoreSessionKey(row.session_key),
      entry,
      preserveKeys,
      preserveRecentMs,
    });
  });
}

function loadSqliteSessionMaintenanceStore(
  database: OpenClawAgentDatabase,
): Record<string, SessionEntry> {
  const db = getSessionKysely(database.db);
  const rows = executeSqliteQuerySync(
    database.db,
    db.selectFrom("session_nodes").select(["session_key", "entry_json"]).orderBy("session_key"),
  ).rows;
  const store: Record<string, SessionEntry> = {};
  for (const row of rows) {
    const entry = parseSessionEntryRow(row);
    if (entry) {
      store[row.session_key] = entry;
    }
  }
  return store;
}

export function applySessionEntryMaintenance(
  database: OpenClawAgentDatabase,
  params: {
    activeSessionKey: string;
    archiveDirectory: string;
    forceMaintenance?: boolean;
    maintenanceConfig?: ResolvedSessionMaintenanceConfig;
    skipMaintenance?: boolean;
    storePath: string;
  },
): SessionEntryMaintenancePlan {
  if (params.skipMaintenance) {
    return { entryRemovals: [], stateDeletePlans: [], modelRunPruned: 0, pruned: 0, capped: 0 };
  }
  const maintenance = params.maintenanceConfig ?? resolveMaintenanceConfig();
  if (maintenance.mode === "warn") {
    return { entryRemovals: [], stateDeletePlans: [], modelRunPruned: 0, pruned: 0, capped: 0 };
  }

  // Count all rows before loading their payloads. Protection controls eviction candidates, not
  // whether a row consumes maxEntries; the full snapshot is needed only when maintenance runs.
  const entryCount = readSessionEntryCount(database);
  const preserveCandidateKeys = collectSessionMaintenancePreserveKeys([params.activeSessionKey]);
  const hasStaleCandidate = hasStaleSqliteSessionEntryCandidate(
    database,
    maintenance.pruneAfterMs,
    preserveCandidateKeys,
    maintenance.preserveRecentMs ?? null,
  );
  const shouldMaintainStore =
    params.forceMaintenance === true ||
    entryCount > maintenance.maxEntries ||
    hasStaleCandidate ||
    shouldRunModelRunPrune({
      maintenance,
      entryCount,
      force: params.forceMaintenance,
    }) ||
    shouldRunSessionEntryMaintenance({
      entryCount,
      maxEntries: maintenance.maxEntries,
      force: params.forceMaintenance,
    });
  if (!shouldMaintainStore) {
    return { entryRemovals: [], stateDeletePlans: [], modelRunPruned: 0, pruned: 0, capped: 0 };
  }

  const store = loadSqliteSessionMaintenanceStore(database);
  const preserveKeys = collectCapacityEligibleLivePreserveKeys({
    baseKeys: collectSqliteSessionMaintenanceBaseKeys(store, params.activeSessionKey),
    database,
    store,
    storePath: params.storePath,
  });
  const removedKeys = new Set<string>();
  const removedEntriesByKey = new Map<string, SessionEntry>();
  const rememberRemovedEntry = (removed: { key: string; entry: SessionEntry }) => {
    removedKeys.add(removed.key);
    removedEntriesByKey.set(removed.key, cloneSessionEntry(removed.entry));
  };
  let remainingEntryCount = entryCount;
  let modelRunPruned = 0;
  if (
    shouldRunModelRunPrune({
      maintenance,
      entryCount: remainingEntryCount,
      force: params.forceMaintenance,
    })
  ) {
    modelRunPruned = pruneStaleModelRunEntries(store, maintenance.modelRunPruneAfterMs, {
      log: false,
      onPruned: rememberRemovedEntry,
      preserveKeys,
      preserveRecentMs: maintenance.preserveRecentMs,
    });
    remainingEntryCount -= modelRunPruned;
  }
  let pruned = 0;
  if (
    params.forceMaintenance === true ||
    hasStaleCandidate ||
    remainingEntryCount > maintenance.maxEntries
  ) {
    pruned = pruneStaleEntries(store, maintenance.pruneAfterMs, {
      log: false,
      onPruned: rememberRemovedEntry,
      preserveKeys,
      preserveRecentMs: maintenance.preserveRecentMs,
    });
    remainingEntryCount -= pruned;
  }
  let capped = 0;
  if (
    shouldRunSessionEntryMaintenance({
      entryCount: remainingEntryCount,
      maxEntries: maintenance.maxEntries,
      force: params.forceMaintenance,
    })
  ) {
    capped = capEntryCount(store, maintenance.maxEntries, {
      log: false,
      onCapped: rememberRemovedEntry,
      preserveKeys,
      preserveRecentMs: maintenance.preserveRecentMs,
    });
  }
  return {
    ...planSqliteLiveEntryRemovals({
      archiveDirectory: params.archiveDirectory,
      database,
      projectedStore: store,
      removedEntriesByKey,
      removedKeys,
    }),
    modelRunPruned,
    pruned,
    capped,
  };
}

function planSqliteLiveEntryRemovals(params: {
  archiveDirectory: string;
  database: OpenClawAgentDatabase;
  projectedStore: Record<string, SessionEntry>;
  removedEntriesByKey: Map<string, SessionEntry>;
  removedKeys: Set<string>;
}): SessionEntryMaintenancePlan {
  const removedSessionIds = new Set<string>();
  for (const entry of params.removedEntriesByKey.values()) {
    for (const sessionId of collectSessionStateIdsForEntry(entry)) {
      removedSessionIds.add(sessionId);
    }
  }
  for (const sessionId of readSessionGenerationIdsForKeys(params.database, params.removedKeys)) {
    removedSessionIds.add(sessionId);
  }
  const referencedSessionIds = collectProjectedReferencedSessionIds({
    database: params.database,
    excludedSessionKeys: params.removedKeys,
    projectedStore: params.projectedStore,
  });
  const deletePlans: SessionStateDeletePlan[] = [];
  for (const sessionId of removedSessionIds) {
    const plan = planSessionStateDeleteIfUnreferenced({
      archiveTranscript: true,
      archiveDirectory: params.archiveDirectory,
      database: params.database,
      referencedSessionIds,
      sessionId,
    });
    if (plan) {
      deletePlans.push(plan);
    }
  }
  return {
    entryRemovals: [...params.removedEntriesByKey].map(([sessionKey, entry]) => ({
      expectedEntry: entry,
      sessionKey,
    })),
    stateDeletePlans: deletePlans,
    modelRunPruned: 0,
    pruned: 0,
    capped: 0,
  };
}

/** Session ids owned by in-flight work admissions, without live-reference protection. */
export function collectAdmissionProtectedSessionIds(params: {
  database: OpenClawAgentDatabase;
  storePath: string;
}): Set<string> {
  const protectedSessionIds = new Set<string>();
  const admissionIdentities =
    collectActiveSessionWorkAdmissions().get(params.storePath) ?? new Set<string>();
  if (admissionIdentities.size === 0) {
    return protectedSessionIds;
  }

  // Admissions may carry either the backing session id or its live session key. Protect both,
  // then resolve admitted keys through their entries so cleanup cannot reclaim active work.
  for (const identity of admissionIdentities) {
    protectedSessionIds.add(identity);
  }
  const normalizedAdmissionKeys = new Set(
    [...admissionIdentities].map((identity) => normalizeStoreSessionKey(identity)),
  );
  const db = getSessionKysely(params.database.db);
  const rows = executeSqliteQuerySync(
    params.database.db,
    db.selectFrom("session_nodes").select(["entry_json", "current_session_id", "session_key"]),
  ).rows;
  for (const row of rows) {
    if (!normalizedAdmissionKeys.has(normalizeStoreSessionKey(row.session_key))) {
      continue;
    }
    protectedSessionIds.add(row.current_session_id);
    const entry = parseSessionEntryRow(row);
    if (entry) {
      for (const sessionId of collectSessionStateIdsForEntry(entry)) {
        protectedSessionIds.add(sessionId);
      }
    }
  }
  // Key-scoped admissions must survive rollover: an in-flight run admitted by
  // key may still write to a generation the entry no longer references, so
  // every generation of an admitted key stays off-limits.
  const generationRows = executeSqliteQuerySync(
    params.database.db,
    db.selectFrom("session_windows").select(["session_id", "session_key"]),
  ).rows;
  for (const row of generationRows) {
    if (normalizedAdmissionKeys.has(normalizeStoreSessionKey(row.session_key))) {
      protectedSessionIds.add(row.session_id);
    }
  }
  return protectedSessionIds;
}

/** Live session_node keys that own any admission-protected generation. */
function collectAdmissionProtectedStoreKeys(params: {
  database: OpenClawAgentDatabase;
  storePath: string;
}): Set<string> {
  const protectedSessionIds = collectAdmissionProtectedSessionIds(params);
  if (protectedSessionIds.size === 0) {
    return new Set();
  }
  const keys = new Set<string>();
  const db = getSessionKysely(params.database.db);
  for (const row of executeSqliteQuerySync(
    params.database.db,
    db.selectFrom("session_nodes").select(["current_session_id", "session_key"]),
  ).rows) {
    if (
      protectedSessionIds.has(row.session_key) ||
      protectedSessionIds.has(row.current_session_id)
    ) {
      keys.add(row.session_key);
    }
  }
  for (const row of executeSqliteQuerySync(
    params.database.db,
    db.selectFrom("session_windows").select(["session_id", "session_key"]),
  ).rows) {
    if (protectedSessionIds.has(row.session_id)) {
      keys.add(row.session_key);
    }
  }
  return keys;
}

function collectCapacityEligibleLivePreserveKeys(params: {
  baseKeys?: Iterable<string | undefined>;
  database: OpenClawAgentDatabase;
  skipSessionKeys?: ReadonlySet<string>;
  store: Record<string, SessionEntry>;
  storePath: string;
}): Set<string> {
  const preserveKeys =
    collectSessionMaintenancePreserveKeysForStore({
      baseKeys: params.baseKeys,
      storePath: params.storePath,
      store: params.store,
    }) ?? new Set<string>();
  for (const key of params.skipSessionKeys ?? []) {
    preserveKeys.add(key);
  }
  // Admissions may name a non-current generation id. Map those ids back to the
  // live session_node so capacity eviction cannot cascade-delete in-flight work.
  for (const key of collectAdmissionProtectedStoreKeys({
    database: params.database,
    storePath: params.storePath,
  })) {
    preserveKeys.add(key);
  }
  return preserveKeys;
}

/** Plans at most one oldest capacity-eligible live session_node removal. */
export function planOldestCapacityEligibleSqliteLiveEntryRemoval(params: {
  archiveDirectory: string;
  database: OpenClawAgentDatabase;
  skipSessionKeys?: ReadonlySet<string>;
  storePath: string;
  preserveRecentMs?: number | null;
}): SessionEntryMaintenancePlan {
  // simplification: rescan session_nodes per victim; index by updated_at if stores grow huge
  const store = loadSqliteSessionMaintenanceStore(params.database);
  const preserveKeys = collectCapacityEligibleLivePreserveKeys({
    database: params.database,
    skipSessionKeys: params.skipSessionKeys,
    store,
    storePath: params.storePath,
  });
  const projectedStore = { ...store };
  const removedKeys = new Set<string>();
  const removedEntriesByKey = new Map<string, SessionEntry>();
  capEntryCount(projectedStore, Math.max(0, Object.keys(store).length - 1), {
    log: false,
    onCapped: (removed) => {
      removedKeys.add(removed.key);
      removedEntriesByKey.set(removed.key, cloneSessionEntry(removed.entry));
    },
    preserveKeys,
    preserveRecentMs: resolveLivePreserveRecentMs(params.preserveRecentMs),
  });
  return planSqliteLiveEntryRemovals({
    archiveDirectory: params.archiveDirectory,
    database: params.database,
    projectedStore,
    removedEntriesByKey,
    removedKeys,
  });
}

function sqliteSessionNodeExists(database: OpenClawAgentDatabase, sessionKey: string): boolean {
  const db = getSessionKysely(database.db);
  return (
    executeSqliteQuerySync(
      database.db,
      db
        .selectFrom("session_nodes")
        .select("session_key")
        .where("session_key", "=", sessionKey)
        .limit(1),
    ).rows.length > 0
  );
}

function resolveLivePreserveRecentMs(preserveRecentMs?: number | null): number | null {
  return preserveRecentMs === undefined
    ? (resolveMaintenanceConfig().preserveRecentMs ?? null)
    : preserveRecentMs;
}

function areCapacityEligibleLiveEntryRemovals(params: {
  database: OpenClawAgentDatabase;
  entryRemovals: readonly SessionEntryRemovalPlan[];
  storePath: string;
  preserveRecentMs?: number | null;
}): boolean {
  if (params.entryRemovals.length === 0) {
    return false;
  }
  const store = loadSqliteSessionMaintenanceStore(params.database);
  const preserveKeys = collectCapacityEligibleLivePreserveKeys({
    database: params.database,
    store,
    storePath: params.storePath,
  });
  return params.entryRemovals.every(
    (removal) =>
      !shouldPreserveMaintenanceEntry({
        key: removal.sessionKey,
        entry: store[removal.sessionKey] ?? removal.expectedEntry,
        preserveKeys,
        preserveRecentMs: resolveLivePreserveRecentMs(params.preserveRecentMs),
        scope: "capacity",
      }),
  );
}

/** Last-resort live-node disk eviction. Historical generations must already be exhausted. */
export async function reclaimSqliteLiveSessionEntriesToHighWater(params: {
  archiveDirectory: string;
  database: OpenClawAgentDatabase;
  highWaterBytes: number;
  pruneArchivesToHighWater: () => Promise<{
    removedFiles: number;
    usage: SessionPhysicalDiskUsage;
  }>;
  reclaimFreePages: (database: OpenClawAgentDatabase) => void;
  resolved: Pick<ResolvedSqliteReadScope, "agentId" | "env" | "path">;
  storePath: string;
  usage: SessionPhysicalDiskUsage;
  preserveRecentMs?: number | null;
}): Promise<{
  removedEntries: number;
  removedFiles: number;
  usage: SessionPhysicalDiskUsage;
}> {
  let { usage } = params;
  let removedEntries = 0;
  let removedFiles = 0;
  const skipSessionKeys = new Set<string>();
  const livePlanParams = {
    archiveDirectory: params.archiveDirectory,
    database: params.database,
    skipSessionKeys,
    storePath: params.storePath,
    preserveRecentMs: params.preserveRecentMs,
  };
  while (usage.totalBytes > params.highWaterBytes) {
    const livePlan = planOldestCapacityEligibleSqliteLiveEntryRemoval(livePlanParams);
    const victim = livePlan.entryRemovals[0];
    if (!victim) {
      break;
    }
    const identities = uniqueStrings(
      [
        victim.sessionKey,
        victim.expectedEntry?.sessionId,
        ...readSessionGenerationIdsForKeys(params.database, [victim.sessionKey]),
      ].filter(
        (identity): identity is string => typeof identity === "string" && identity.length > 0,
      ),
    );
    let retargeted = false;
    const published = await runExclusiveSessionLifecycleMutation({
      scope: params.storePath,
      identities,
      run: async () => {
        const fencedPlan = planOldestCapacityEligibleSqliteLiveEntryRemoval(livePlanParams);
        if (fencedPlan.entryRemovals[0]?.sessionKey !== victim.sessionKey) {
          retargeted = true;
          return null;
        }
        return await finalizeSessionEntryMaintenancePlansAfterWriterReleaseBestEffort(
          params.resolved,
          [fencedPlan],
          { storePath: params.storePath, preserveRecentMs: params.preserveRecentMs },
        );
      },
    });
    if (retargeted) {
      continue;
    }
    if (!published || sqliteSessionNodeExists(params.database, victim.sessionKey)) {
      skipSessionKeys.add(victim.sessionKey);
      continue;
    }
    removedEntries += 1;
    emitArchivedTranscriptUpdates(published.archivedTranscripts);
    try {
      params.reclaimFreePages(params.database);
    } catch {
      // Best-effort reclamation only.
    }
    usage = await measureSessionPhysicalDiskUsage(params.storePath);
    if (usage.totalBytes > params.highWaterBytes) {
      const repruned = await params.pruneArchivesToHighWater();
      removedFiles += repruned.removedFiles;
      usage = repruned.usage;
    }
  }
  return { removedEntries, removedFiles, usage };
}

type SessionEntryMaintenanceFinalizeOptions = {
  storePath?: string;
  preserveRecentMs?: number | null;
};

export async function finalizeSessionEntryMaintenancePlansBestEffort(
  scope: Pick<ResolvedSqliteReadScope, "agentId" | "env" | "path">,
  plans: readonly SessionEntryMaintenancePlan[],
  options?: SessionEntryMaintenanceFinalizeOptions,
): Promise<SessionEntryMaintenanceResult> {
  return await finalizeSqliteSessionEntryMaintenancePlansWithCommit(
    scope,
    plans,
    async (commit) => commit(),
    options,
  );
}

/** Finalizes maintenance after its caller releases the per-store writer lane. */
export async function finalizeSessionEntryMaintenancePlansAfterWriterReleaseBestEffort(
  scope: Pick<ResolvedSqliteReadScope, "agentId" | "env" | "path">,
  plans: readonly SessionEntryMaintenancePlan[],
  options?: SessionEntryMaintenanceFinalizeOptions,
): Promise<SessionEntryMaintenanceResult> {
  return await finalizeSqliteSessionEntryMaintenancePlansWithCommit(
    scope,
    plans,
    async (commit) => await runExclusiveSqliteSessionWrite(scope, async () => commit()),
    options,
  );
}

async function finalizeSqliteSessionEntryMaintenancePlansWithCommit(
  scope: Pick<ResolvedSqliteReadScope, "agentId" | "env" | "path">,
  plans: readonly SessionEntryMaintenancePlan[],
  commit: (
    fn: () => SessionLifecycleArchivedTranscript[],
  ) => Promise<SessionLifecycleArchivedTranscript[]>,
  options?: SessionEntryMaintenanceFinalizeOptions,
): Promise<SessionEntryMaintenanceResult> {
  const entryRemovals = plans.flatMap((plan) => plan.entryRemovals);
  const stateDeletePlans = plans.flatMap((plan) => plan.stateDeletePlans);
  const warn = (message: string, error: unknown) => {
    getChildLogger({ subsystem: "session-sqlite" }).warn(message, {
      agentId: scope.agentId,
      error,
      path: scope.path,
      sessionIds: uniqueStrings(stateDeletePlans.map((plan) => plan.sessionId)),
    });
  };
  const emptyResult: SessionEntryMaintenanceResult = {
    archivedTranscripts: [],
    modelRunPruned: 0,
    pruned: 0,
    capped: 0,
  };
  if (entryRemovals.length === 0 && stateDeletePlans.length === 0) {
    return emptyResult;
  }
  let archivedTranscripts: SessionLifecycleArchivedTranscript[];
  try {
    const materializedPlans = await materializeSessionStateDeletePlans(stateDeletePlans);
    let aborted = false;
    archivedTranscripts = await commit(() => {
      let committed: SessionLifecycleArchivedTranscript[] = [];
      runOpenClawAgentWriteTransaction((database) => {
        assertPlannedLifecycleArtifactEntriesUnchanged(database, entryRemovals);
        if (
          options?.storePath &&
          !areCapacityEligibleLiveEntryRemovals({
            database,
            entryRemovals,
            storePath: options.storePath,
            preserveRecentMs: options.preserveRecentMs,
          })
        ) {
          aborted = true;
          return;
        }
        committed = deleteMaterializedSessionStatePlans(
          database,
          materializedPlans,
          undefined,
          new Set(entryRemovals.map((removal) => removal.sessionKey)),
        );
        deletePlannedLifecycleArtifactEntries(database, entryRemovals);
      }, toDatabaseOptions(scope));
      return committed;
    });
    if (aborted) {
      return emptyResult;
    }
  } catch (error) {
    warn("SQLite session maintenance cleanup failed", error);
    return emptyResult;
  }
  const committedCounts = plans.reduce(
    (counts, plan) => ({
      modelRunPruned: counts.modelRunPruned + plan.modelRunPruned,
      pruned: counts.pruned + plan.pruned,
      capped: counts.capped + plan.capped,
    }),
    { modelRunPruned: 0, pruned: 0, capped: 0 },
  );
  emitCommittedSessionEntryRemovals(entryRemovals);
  try {
    return {
      archivedTranscripts: await publishSessionStateArchives(scope, archivedTranscripts),
      ...committedCounts,
    };
  } catch (error) {
    warn("SQLite session maintenance archive publication failed", error);
    return { archivedTranscripts: [], ...committedCounts };
  }
}
