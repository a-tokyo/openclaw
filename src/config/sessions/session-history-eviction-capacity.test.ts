import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { beginSessionEffect } from "../../sessions/session-controller.lifecycle.js";
import * as diskBudget from "./disk-budget.js";
import { measureSessionPhysicalDiskUsage } from "./disk-budget.js";
import {
  appendTranscriptMessage,
  patchSessionEntryCore,
  replaceSessionEntry,
  resetSessionEntryLifecycle,
} from "./session-accessor.js";
import { finalizeSessionEntryMaintenancePlansAfterWriterReleaseBestEffort } from "./session-accessor.sqlite-maintenance.js";
import * as archivePruning from "./session-history-archive-pruning.js";
import type { SessionHistoryDiskBudgetParams } from "./session-history-budget-state.js";
import { installHistoryBudgetState } from "./session-history-budget-state.test-support.js";
import { createSessionHistoryBudgetFixture } from "./session-history-budget.test-support.js";
import {
  enforceSqliteSessionHistoryDiskBudget,
  inspectSqliteSessionHistoryDiskBudget,
} from "./session-history-eviction.js";
import { planLiveEvictionInDatabase } from "./session-live-eviction-plan.worker.js";
import { resolveSqliteTargetFromSessionStorePath } from "./session-sqlite-target.js";
import {
  prepareSessionMaintenancePreservation,
  registerSessionMaintenancePreserveKeysProvider,
} from "./store-maintenance-preserve.js";
import { resolveMaintenanceConfigFromInput } from "./store-maintenance.js";

const BIG = (tag: string) => `${tag} ${"x".repeat(64 * 1024)}`;

describe("SQLite live-node disk budget eviction", () => {
  let tempDir: string;
  let storePath: string;
  const { createHistoricalTranscript, database, sessionExists, settlePhysicalUsage } =
    createSessionHistoryBudgetFixture(() => ({ storePath, tempDir }));

  installHistoryBudgetState("openclaw-session-live-budget-", (_state, dir, store) => {
    tempDir = dir;
    storePath = store;
  });

  async function seed(
    sessionKey: string,
    sessionId: string,
    updatedAt: number,
    content = BIG(sessionId),
    extra = {},
  ) {
    await replaceSessionEntry({ sessionKey, storePath }, { sessionId, updatedAt, ...extra });
    await appendTranscriptMessage(
      { sessionId, sessionKey, storePath },
      { message: { role: "user", content } },
    );
  }

  /** Explicit budget one byte under current usage, unless overridden. */
  async function explicitBudget(
    overrides: Partial<SessionHistoryDiskBudgetParams["maintenance"]> = {},
    activeSessionKey?: string,
  ): Promise<SessionHistoryDiskBudgetParams> {
    settlePhysicalUsage();
    const { totalBytes } = await measureSessionPhysicalDiskUsage(storePath);
    return {
      ...(activeSessionKey ? { activeSessionKey } : {}),
      storePath,
      mode: "enforce",
      maintenance: {
        maxDiskBytes: totalBytes - 1,
        maxDiskBytesExplicit: true,
        highWaterBytes: Math.max(1, totalBytes - 1),
        ...overrides,
      },
    };
  }

  async function withAdmission<T>(identity: string, run: () => Promise<T>): Promise<T> {
    const admission = await beginSessionEffect({
      scope: storePath,
      identities: [identity],
      assertAllowed: () => {},
    });
    try {
      return await run();
    } finally {
      admission.release();
    }
  }

  it.each([
    { name: "after historical reclaim", withHistory: true },
    { name: "when no historical generations remain", withHistory: false },
  ])("inspects and evicts idle durable live nodes $name", async ({ withHistory }) => {
    const idleKey = "agent:main:slack:channel:c1:thread:1";
    const admittedKey = "agent:main:slack:channel:c2:thread:2";
    await seed("agent:main:main", "live-main", 40, "main keep");
    if (withHistory) {
      await createHistoricalTranscript({
        content: "tiny history",
        nextSessionId: "scratch-live",
        sessionId: "scratch-old",
        sessionKey: "agent:main:scratch",
        updatedAt: 1,
      });
    }
    await seed(idleKey, "idle-thread-live", 10);
    await seed(admittedKey, "admitted-thread", 5, "admitted keep");
    await withAdmission(admittedKey, async () => {
      const budget = await explicitBudget();
      if (!withHistory) {
        expect(countHistoricalSessionIds()).toBe(0);
      }
      const inspected = await inspectSqliteSessionHistoryDiskBudget(budget);
      const result = await enforceSqliteSessionHistoryDiskBudget(budget);

      expect(inspected.wouldMutate).toBe(true);
      expect(result?.removedEntries).toBeGreaterThanOrEqual(1);
      expect(sessionExists("idle-thread-live")).toBe(false);
      expect(sessionNodeExists(idleKey)).toBe(false);
      expect(sessionExists("live-main")).toBe(true);
      expect(sessionExists("admitted-thread")).toBe(true);
      expect(sessionNodeExists(admittedKey)).toBe(true);
    });
  });

  it("preserves recently active live nodes under physical pressure", async () => {
    const now = Date.now();
    const recentKey = "agent:main:slack:channel:c6:thread:6";
    const idleKey = "agent:main:slack:channel:c5:thread:5";
    await seed("agent:main:main", "live-main", now, "main keep");
    await seed(recentKey, "recent-live", now, "recent keep");
    await seed(idleKey, "idle-thread-live", 10);
    expect(countHistoricalSessionIds()).toBe(0);
    const budget = await explicitBudget({ preserveRecentMs: 7 * 24 * 60 * 60 * 1000 });
    const inspected = await inspectSqliteSessionHistoryDiskBudget(budget);
    const result = await enforceSqliteSessionHistoryDiskBudget(budget);

    expect(inspected.wouldMutate).toBe(true);
    expect(result?.removedEntries).toBeGreaterThanOrEqual(1);
    expect(sessionNodeExists(idleKey)).toBe(false);
    expect(sessionNodeExists(recentKey)).toBe(true);
    expect(sessionExists("live-main")).toBe(true);
  });

  it("keeps a provider-protected thread that appears while live eviction waits", async () => {
    const oldestKey = "agent:main:slack:channel:c10:thread:10";
    const newerKey = "agent:main:slack:channel:c11:thread:11";
    await seed(oldestKey, "provider-oldest", 5);
    await seed(newerKey, "provider-newer", 15);
    const budget = await explicitBudget();
    let preserveCalls = 0;
    let underBudget = false;
    const realMeasure = diskBudget.measureSessionPhysicalDiskUsage;
    vi.spyOn(diskBudget, "measureSessionPhysicalDiskUsage").mockImplementation(
      async (targetPath) => {
        const usage = await realMeasure(targetPath);
        return underBudget ? { ...usage, totalBytes: 1 } : usage;
      },
    );
    const unregister = registerSessionMaintenancePreserveKeysProvider(async () => ({
      capture: () => {
        preserveCalls += 1;
        if (preserveCalls < 2) {
          return [];
        }
        underBudget = true;
        return [oldestKey];
      },
      dispose() {},
    }));
    try {
      const result = await enforceSqliteSessionHistoryDiskBudget(budget);
      expect(preserveCalls).toBeGreaterThanOrEqual(2);
      expect(result?.removedEntries ?? 0).toBe(0);
      expect(sessionNodeExists(oldestKey)).toBe(true);
      expect(sessionNodeExists(newerKey)).toBe(true);
    } finally {
      unregister();
    }
  });

  it("stops live eviction when the post-delete checkpoint is blocked", async () => {
    const oldestKey = "agent:main:slack:channel:c12:thread:12";
    const newerKey = "agent:main:slack:channel:c13:thread:13";
    await seed(oldestKey, "checkpoint-oldest", 5);
    await seed(newerKey, "checkpoint-newer", 15);
    const budget = await explicitBudget();
    const realReclaim = archivePruning.reclaimSqliteFreePages;
    vi.spyOn(archivePruning, "reclaimSqliteFreePages").mockImplementation(
      async (databaseOptions, diagnostics, limits) =>
        diagnostics?.trigger === "after-eviction"
          ? false
          : await realReclaim(databaseOptions, diagnostics, limits),
    );
    const result = await enforceSqliteSessionHistoryDiskBudget(budget);
    const after = await measureSessionPhysicalDiskUsage(storePath);

    expect(result?.removedEntries).toBe(1);
    expect(result?.totalBytesAfter).toBe(after.totalBytes);
    expect(result?.walBytesAfter).toBe(after.databaseWalBytes);
    expect(sessionNodeExists(oldestKey)).toBe(false);
    expect(sessionNodeExists(newerKey)).toBe(true);
  });

  it("keeps idle durable live nodes under the default disk budget", async () => {
    const threadKey = "agent:main:slack:channel:c14:thread:14";
    await seed(threadKey, "default-budget-thread", 5);
    const budget = await explicitBudget({ maxDiskBytesExplicit: undefined });
    const inspected = await inspectSqliteSessionHistoryDiskBudget(budget);
    const result = await enforceSqliteSessionHistoryDiskBudget(budget);

    expect(inspected.wouldMutate).toBe(false);
    expect(result?.removedEntries ?? 0).toBe(0);
    expect(sessionNodeExists(threadKey)).toBe(true);
  });

  it("keeps the operator active key and evicts the next idle thread", async () => {
    const activeKey = "agent:main:slack:channel:c17:thread:17";
    const idleKey = "agent:main:slack:channel:c18:thread:18";
    await seed(activeKey, "active-oldest", 5);
    await seed(idleKey, "idle-newer", 15);
    const result = await enforceSqliteSessionHistoryDiskBudget(
      await explicitBudget({}, "agent:main:slack:channel:C17:thread:17"),
    );

    expect(result?.removedEntries).toBe(1);
    expect(sessionNodeExists(activeKey)).toBe(true);
    expect(sessionNodeExists(idleKey)).toBe(false);
  });

  it("previews and applies no eviction when only the active key is eligible", async () => {
    const activeKey = "agent:main:slack:channel:c19:thread:19";
    await seed(activeKey, "active-only", 5);
    const budget = await explicitBudget({}, "agent:main:slack:channel:C19:thread:19");

    await expect(inspectSqliteSessionHistoryDiskBudget(budget)).resolves.toMatchObject({
      wouldMutate: false,
    });
    expect((await enforceSqliteSessionHistoryDiskBudget(budget))?.removedEntries).toBe(0);
    expect(sessionNodeExists(activeKey)).toBe(true);
  });

  it.each([
    { name: "whose transcript is cold", coldGeneration: false },
    { name: "whose older generation is cold while the current is hot", coldGeneration: true },
  ])("skips an idle thread $name", async ({ coldGeneration }) => {
    const coldKey = "agent:main:slack:channel:c15:thread:15";
    const hotKey = "agent:main:slack:channel:c16:thread:16";
    await seed(coldKey, "cold-oldest", 5);
    if (coldGeneration) {
      await resetSessionEntryLifecycle({
        storePath,
        target: { canonicalKey: coldKey, storeKeys: [coldKey] },
        buildNextEntry: () => ({ sessionId: "hot-current", updatedAt: 6 }),
      });
      await appendTranscriptMessage(
        { sessionId: "hot-current", sessionKey: coldKey, storePath },
        { message: { role: "user", content: "current" } },
      );
    }
    await seed(hotKey, "hot-newer", 15);
    markCold("cold-oldest");
    const result = await enforceSqliteSessionHistoryDiskBudget(await explicitBudget());

    expect(result?.removedEntries).toBe(1);
    expect(sessionNodeExists(coldKey)).toBe(true);
    expect(sessionExists("cold-oldest")).toBe(true);
    expect(sessionExists("hot-current")).toBe(coldGeneration);
    expect(sessionNodeExists(hotKey)).toBe(false);
  });

  it("keeps a thread whose transcript goes cold before the eviction commits", async () => {
    const threadKey = "agent:main:slack:channel:c20:thread:20";
    await seed(threadKey, "late-cold", 5);
    const prepared = await prepareSessionMaintenancePreservation(storePath);
    const snapshot = prepared.capture();
    prepared.dispose();
    const live = planLiveEvictionInDatabase(database(), {
      archiveDirectory: path.join(tempDir, "archives"),
      baseKeys: [],
      preserveRecentMs: null,
      skipSessionKeys: [],
      snapshot,
      unprotectSessionKeys: [],
    });
    expect(live.plan.entryRemovals.map((removal) => removal.sessionKey)).toEqual([threadKey]);
    markCold("late-cold");
    const target = resolveSqliteTargetFromSessionStorePath(storePath);
    const removed: string[] = [];
    await finalizeSessionEntryMaintenancePlansAfterWriterReleaseBestEffort(
      { agentId: target.agentId ?? "main", env: process.env, path: target.path ?? "" },
      [live.plan],
      { onEntryRemoved: (sessionKey) => removed.push(sessionKey) },
    );

    expect(removed).toEqual([]);
    expect(sessionNodeExists(threadKey)).toBe(true);
  });

  it("keeps a node whose admission names a prior generation from live eviction and capping", async () => {
    const sessionKey = "agent:main:slack:channel:c8:thread:8";
    const fillerKey = "agent:main:history-filler";
    await replaceSessionEntry(
      { sessionKey: fillerKey, storePath },
      { sessionId: "filler-live", updatedAt: 0 },
    );
    await seed(sessionKey, "admitted-history", 1);
    await resetSessionEntryLifecycle({
      storePath,
      target: { canonicalKey: sessionKey, storeKeys: [sessionKey] },
      buildNextEntry: () => ({ sessionId: "live-history", updatedAt: 2 }),
    });
    await appendTranscriptMessage(
      { sessionId: "live-history", sessionKey, storePath },
      { message: { role: "user", content: "live keep" } },
    );
    await withAdmission("admitted-history", async () => {
      const budget = await explicitBudget();
      budget.maintenance.highWaterBytes = Math.max(
        1,
        (budget.maintenance.maxDiskBytes ?? 0) - 32 * 1024,
      );
      expect((await enforceSqliteSessionHistoryDiskBudget(budget))?.removedEntries ?? 0).toBe(0);
      await patchSessionEntryCore({ sessionKey: fillerKey, storePath }, () => ({ updatedAt: 4 }), {
        maintenanceConfig: resolveMaintenanceConfigFromInput({
          maxEntries: 1,
          mode: "enforce",
          pruneAfter: "365d",
        }),
      });
      expect(sessionExists("admitted-history")).toBe(true);
      expect(sessionExists("live-history")).toBe(true);
      expect(sessionNodeExists(sessionKey)).toBe(true);
    });
  });

  it("marks only an operator-set budget as explicit", () => {
    const explicit = (maxDiskBytes?: string | false) =>
      resolveMaintenanceConfigFromInput(maxDiskBytes === undefined ? undefined : { maxDiskBytes })
        .maxDiskBytesExplicit;
    expect([explicit(), explicit("400mb"), explicit(false), explicit("big")]).toEqual([
      false,
      true,
      false,
      false,
    ]);
  });

  it("does not wipe live durables when highWaterBytes is 0", async () => {
    const threadKey = "agent:main:slack:channel:c9:thread:9";
    await seed(threadKey, "live-durable", 2, "live durable keep");
    await createHistoricalTranscript({
      content: BIG("oldest"),
      nextSessionId: "newer-history",
      sessionId: "oldest-history",
      sessionKey: "agent:main:history-order",
      updatedAt: 10,
    });
    expect(sessionExists("oldest-history")).toBe(true);
    const result = await enforceSqliteSessionHistoryDiskBudget(
      await explicitBudget({ highWaterBytes: 0 }),
    );

    expect(sessionExists("live-durable")).toBe(true);
    expect(sessionNodeExists(threadKey)).toBe(true);
    expect(result?.overBudget).toBe(true);
  });

  function markCold(sessionId: string): void {
    database()
      .db.prepare(
        `INSERT INTO session_transcript_cold_archives
          (session_id, generation, archive_name, archive_sha256, event_count, raw_bytes,
           archive_bytes, last_seq, archived_at, storage, archive_blob)
         VALUES (?, 'test', ?, ?, 1, 1, 1, 1, ?, 'sqlite', x'00')`,
      )
      .run(sessionId, `${sessionId}.cold.zst`, "0".repeat(64), Date.now());
  }

  function sessionNodeExists(sessionKey: string): boolean {
    return (
      database().db.prepare("SELECT 1 FROM session_nodes WHERE session_key = ?").get(sessionKey) !==
      undefined
    );
  }

  function countHistoricalSessionIds(): number {
    const row = database()
      .db.prepare(
        "SELECT count(*) AS n FROM session_windows WHERE session_id NOT IN (SELECT current_session_id FROM session_nodes)",
      )
      .get() as { n: number };
    return row.n;
  }
});
