/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { fakeAsync, tick } from "@angular/core/testing";
import { vi } from "vitest";
import { DataGuardRowNavigatorService, DataGuardRowNavRequest } from "./data-guard-row-navigator.service";

describe("DataGuardRowNavigatorService", () => {
  describe("pageIndexFor", () => {
    it("returns page 1 for the first row regardless of page size", () => {
      expect(DataGuardRowNavigatorService.pageIndexFor(0, 5)).toBe(1);
      expect(DataGuardRowNavigatorService.pageIndexFor(0, 25)).toBe(1);
    });

    it("computes 1-based page index for arbitrary row indices", () => {
      expect(DataGuardRowNavigatorService.pageIndexFor(4, 5)).toBe(1);
      expect(DataGuardRowNavigatorService.pageIndexFor(5, 5)).toBe(2);
      expect(DataGuardRowNavigatorService.pageIndexFor(11, 5)).toBe(3);
      expect(DataGuardRowNavigatorService.pageIndexFor(99, 10)).toBe(10);
    });

    it("guards against degenerate page sizes (returns page 1)", () => {
      expect(DataGuardRowNavigatorService.pageIndexFor(7, 0)).toBe(1);
    });

    it("clamps negative indices to page 1 and logs a warning", () => {
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
      expect(DataGuardRowNavigatorService.pageIndexFor(-3, 5)).toBe(1);
      expect(warnSpy).toHaveBeenCalled();
      warnSpy.mockRestore();
    });
  });

  describe("nextCycleStep", () => {
    it("walks the cycle and wraps to the start", () => {
      const indices = [3, 7, 12];
      // start: cursor=0 → value=3, next=1
      let step = DataGuardRowNavigatorService.nextCycleStep(indices, 0);
      expect(step).toEqual({ value: 3, nextCursor: 1 });
      // cursor=1 → value=7, next=2
      step = DataGuardRowNavigatorService.nextCycleStep(indices, step.nextCursor);
      expect(step).toEqual({ value: 7, nextCursor: 2 });
      // cursor=2 → value=12, next=3
      step = DataGuardRowNavigatorService.nextCycleStep(indices, step.nextCursor);
      expect(step).toEqual({ value: 12, nextCursor: 3 });
      // cursor=3 wraps → value=3, next=1 (3 % 3 = 0)
      step = DataGuardRowNavigatorService.nextCycleStep(indices, step.nextCursor);
      expect(step).toEqual({ value: 3, nextCursor: 1 });
      // cursor=1 again → value=7, next=2
      step = DataGuardRowNavigatorService.nextCycleStep(indices, step.nextCursor);
      expect(step).toEqual({ value: 7, nextCursor: 2 });
    });

    it("treats a single-element array as a no-op cycle that re-emits", () => {
      const indices = [42];
      expect(DataGuardRowNavigatorService.nextCycleStep(indices, 0)).toEqual({ value: 42, nextCursor: 1 });
      expect(DataGuardRowNavigatorService.nextCycleStep(indices, 1)).toEqual({ value: 42, nextCursor: 1 });
      expect(DataGuardRowNavigatorService.nextCycleStep(indices, 7)).toEqual({ value: 42, nextCursor: 1 });
    });

    it("coerces corrupted cursors to 0", () => {
      const indices = [10, 20];
      expect(DataGuardRowNavigatorService.nextCycleStep(indices, -5)).toEqual({ value: 10, nextCursor: 1 });
      expect(DataGuardRowNavigatorService.nextCycleStep(indices, NaN)).toEqual({ value: 10, nextCursor: 1 });
    });

    it("throws when affectedRowIndices is empty (callers must toast first)", () => {
      expect(() => DataGuardRowNavigatorService.nextCycleStep([], 0)).toThrow();
    });
  });

  describe("purgeStaleCursors", () => {
    // Regression: the checklist's locate cursors used to be cleared wholesale
    // whenever the results-service emitted, which wiped the per-row cursor
    // mid-cycle after benign re-emits (verdict toggles, status messages).
    // The fix swaps clear() for a per-key purge so live ids survive.
    it("keeps cursors for ids that are still live across a re-emit", () => {
      const cursors = new Map<string, number>([
        ["issue-a", 2],
        ["issue-b", 1],
      ]);
      // Same id set re-emits → no mutations expected.
      DataGuardRowNavigatorService.purgeStaleCursors(cursors, new Set(["issue-a", "issue-b"]));
      expect(cursors.get("issue-a")).toBe(2);
      expect(cursors.get("issue-b")).toBe(1);
      expect(cursors.size).toBe(2);
    });

    it("regression: a 4-step cycle survives an unrelated updateEntry re-emit", () => {
      // Simulates: user clicks 📍 twice on issue-a (cursor reaches 2), then
      // toggles issue-b's verdict (which fires updateEntry → setState with the
      // same id set), then clicks 📍 on issue-a a third time. The third click
      // MUST advance from 2 → 3, not snap back to 0 → 1.
      const cursors = new Map<string, number>([["issue-a", 2]]);
      // Re-emit after the benign verdict toggle. Both ids still live.
      DataGuardRowNavigatorService.purgeStaleCursors(cursors, new Set(["issue-a", "issue-b"]));
      expect(cursors.get("issue-a")).toBe(2);
      // Next click reads this and feeds it into nextCycleStep.
      const indices = [3, 7, 12, 15];
      const step = DataGuardRowNavigatorService.nextCycleStep(indices, cursors.get("issue-a")!);
      expect(step.value).toBe(12);
      expect(step.nextCursor).toBe(3);
    });

    it("drops cursors for ids that disappeared (fresh scan)", () => {
      const cursors = new Map<string, number>([
        ["issue-a", 2],
        ["issue-b", 1],
      ]);
      // Fresh scan: completely different id set.
      DataGuardRowNavigatorService.purgeStaleCursors(cursors, new Set(["issue-c", "issue-d"]));
      expect(cursors.has("issue-a")).toBe(false);
      expect(cursors.has("issue-b")).toBe(false);
      expect(cursors.size).toBe(0);
    });

    it("preserves the intersection when the id set partially changes", () => {
      const cursors = new Map<string, number>([
        ["issue-a", 2],
        ["issue-b", 1],
        ["issue-c", 3],
      ]);
      DataGuardRowNavigatorService.purgeStaleCursors(cursors, new Set(["issue-b", "issue-d"]));
      expect(cursors.has("issue-a")).toBe(false);
      expect(cursors.get("issue-b")).toBe(1);
      expect(cursors.has("issue-c")).toBe(false);
      // Live ids without prior cursors are not auto-added — that's the
      // checklist's `.get(...) ?? 0` job at click time.
      expect(cursors.has("issue-d")).toBe(false);
    });

    it("handles an empty live set by purging everything", () => {
      const cursors = new Map<string, number>([["issue-a", 2]]);
      DataGuardRowNavigatorService.purgeStaleCursors(cursors, new Set());
      expect(cursors.size).toBe(0);
    });

    it("is a no-op on an empty cursors map", () => {
      const cursors = new Map<string, number>();
      DataGuardRowNavigatorService.purgeStaleCursors(cursors, new Set(["x"]));
      expect(cursors.size).toBe(0);
    });
  });

  describe("rowFingerprint", () => {
    // Mirror of the agent-service profile-dataset.test.ts contract. These two
    // suites are the cross-language locks that catch JSON.stringify drift
    // between Bun (server) and V8 (browser).
    it("produces identical keys for identical content", () => {
      const a = DataGuardRowNavigatorService.rowFingerprint({ age: 25, name: "Alice" }, ["age", "name"]);
      const b = DataGuardRowNavigatorService.rowFingerprint({ age: 25, name: "Alice" }, ["age", "name"]);
      expect(a).toBe(b);
    });

    it("canonicalises column order (sort) so display-side reordering doesn't matter", () => {
      const a = DataGuardRowNavigatorService.rowFingerprint({ age: 25, name: "Alice" }, ["age", "name"]);
      const b = DataGuardRowNavigatorService.rowFingerprint({ age: 25, name: "Alice" }, ["name", "age"]);
      expect(a).toBe(b);
    });

    it("treats missing key and explicit null the same way", () => {
      const a = DataGuardRowNavigatorService.rowFingerprint({ age: null, name: "Alice" }, ["age", "name"]);
      const b = DataGuardRowNavigatorService.rowFingerprint({ name: "Alice" }, ["age", "name"]);
      expect(a).toBe(b);
    });

    // Contract example: the exact same input/output as the agent-service
    // counterpart test ("contract example: known input produces known output").
    // If either side drifts, both tests fail and the diff makes the cause obvious.
    // Format: JSON.stringify(String(value)) per non-null cell; null/undefined
    // → bare `null` literal (no quotes).
    it("contract example: matches agent-service rowFingerprint byte-for-byte", () => {
      const row = { glucose: 180, patient_id: "p-7", group: null };
      const key = DataGuardRowNavigatorService.rowFingerprint(row, ["patient_id", "group", "glucose"]);
      // glucose: JSON.stringify(String(180)) = "\"180\""
      // group:   null                         = "null"
      // patient_id: JSON.stringify(String("p-7")) = "\"p-7\""
      expect(key).toBe('"180"' + "null" + '"p-7"');
    });

    // Bug repro: Texera's JSONLScanSourceOpExec widens mixed-type columns to
    // String via parseField(stringValue, schemaType), while DataGuard's own
    // parseJsonl keeps native JSON types. Without the String() coercion, the
    // profiler-side number `45` and display-side string `"45"` fingerprint
    // differently and findRowByKey misses every row → wrong cell highlight.
    it("regression: number and numeric-string cells fingerprint identically (JSONL mixed-type)", () => {
      const fromAgent = DataGuardRowNavigatorService.rowFingerprint({ age: 45, sample_id: "J001" }, [
        "age",
        "sample_id",
      ]);
      const fromTexera = DataGuardRowNavigatorService.rowFingerprint({ age: "45", sample_id: "J001" }, [
        "age",
        "sample_id",
      ]);
      expect(fromAgent).toBe(fromTexera);
      // And the resulting token is the quoted-string form.
      expect(fromAgent).toBe('"45"' + '"J001"');
    });

    it("regression: float values round-trip via String() identically on V8/Bun", () => {
      const a = DataGuardRowNavigatorService.rowFingerprint({ x: 28.1 }, ["x"]);
      const b = DataGuardRowNavigatorService.rowFingerprint({ x: "28.1" }, ["x"]);
      expect(a).toBe(b);
      expect(a).toBe('"28.1"');
    });
  });

  describe("findRowByKey", () => {
    const rows = [
      { id: "a", v: 1 },
      { id: "b", v: 2 },
      { id: "c", v: 3 },
    ];
    const columns = ["id", "v"];

    it("finds a row in the middle of the array", () => {
      const key = DataGuardRowNavigatorService.rowFingerprint({ id: "b", v: 2 }, columns);
      expect(DataGuardRowNavigatorService.findRowByKey(rows, columns, key)).toBe(1);
    });

    it("returns -1 when no row matches", () => {
      const key = DataGuardRowNavigatorService.rowFingerprint({ id: "z", v: 99 }, columns);
      expect(DataGuardRowNavigatorService.findRowByKey(rows, columns, key)).toBe(-1);
    });

    it("returns -1 for empty inputs", () => {
      expect(DataGuardRowNavigatorService.findRowByKey([], columns, "anything")).toBe(-1);
      expect(DataGuardRowNavigatorService.findRowByKey(rows, [], "anything")).toBe(-1);
    });

    it("survives display-side column reordering (caller's column list can be in any order)", () => {
      // Caller passes columns in a different order than the producer side did
      // — the fingerprint sort canonicalises, so we still match.
      const key = DataGuardRowNavigatorService.rowFingerprint({ id: "c", v: 3 }, ["v", "id"]);
      expect(DataGuardRowNavigatorService.findRowByKey(rows, ["id", "v"], key)).toBe(2);
    });
  });

  describe("findNthRowByKey", () => {
    // Regression: duplicate-row issues emit N identical fingerprints in
    // affectedRowKeys (every dup row has the same content by definition).
    // findRowByKey returns the first display match for every click, so 4
    // dup clicks would collapse to 2 visible flashes if we did not cycle by
    // occurrence. findNthRowByKey returns the Nth match so the checklist can
    // hand a different occurrence to each click and land on a distinct row.
    const dupRows = [
      { id: "X", group: "g1" }, // 0: unrelated
      { id: "J001", group: "g1" }, // 1: dup A, first match
      { id: "Y", group: "g2" }, // 2: unrelated
      { id: "J001", group: "g1" }, // 3: dup A, second match
    ];
    const columns = ["id", "group"];

    it("returns the index of the Nth match (0-indexed) and -1 once exhausted", () => {
      const dupKey = DataGuardRowNavigatorService.rowFingerprint({ id: "J001", group: "g1" }, columns);
      expect(DataGuardRowNavigatorService.findNthRowByKey(dupRows, columns, dupKey, 0)).toBe(1);
      expect(DataGuardRowNavigatorService.findNthRowByKey(dupRows, columns, dupKey, 1)).toBe(3);
      expect(DataGuardRowNavigatorService.findNthRowByKey(dupRows, columns, dupKey, 2)).toBe(-1);
    });

    it("returns -1 on empty inputs / no matches", () => {
      const dupKey = DataGuardRowNavigatorService.rowFingerprint({ id: "J001", group: "g1" }, columns);
      expect(DataGuardRowNavigatorService.findNthRowByKey([], columns, dupKey, 0)).toBe(-1);
      expect(DataGuardRowNavigatorService.findNthRowByKey(dupRows, [], dupKey, 0)).toBe(-1);
      const missing = DataGuardRowNavigatorService.rowFingerprint({ id: "ZZZ", group: "g9" }, columns);
      expect(DataGuardRowNavigatorService.findNthRowByKey(dupRows, columns, missing, 0)).toBe(-1);
    });

    it("coerces negative / non-finite occurrence to 0 (defensive)", () => {
      const dupKey = DataGuardRowNavigatorService.rowFingerprint({ id: "J001", group: "g1" }, columns);
      expect(DataGuardRowNavigatorService.findNthRowByKey(dupRows, columns, dupKey, -1)).toBe(1);
      expect(DataGuardRowNavigatorService.findNthRowByKey(dupRows, columns, dupKey, NaN)).toBe(1);
    });

    it("findRowByKey is a thin wrapper over findNthRowByKey(..., 0)", () => {
      const dupKey = DataGuardRowNavigatorService.rowFingerprint({ id: "J001", group: "g1" }, columns);
      expect(DataGuardRowNavigatorService.findRowByKey(dupRows, columns, dupKey)).toBe(
        DataGuardRowNavigatorService.findNthRowByKey(dupRows, columns, dupKey, 0)
      );
    });
  });

  describe("countMatchesByKey", () => {
    it("counts every row whose fingerprint matches the key", () => {
      const rows = [
        { id: "J001", v: 1 },
        { id: "J002", v: 2 },
        { id: "J001", v: 1 },
        { id: "J001", v: 1 },
      ];
      const columns = ["id", "v"];
      const key = DataGuardRowNavigatorService.rowFingerprint({ id: "J001", v: 1 }, columns);
      expect(DataGuardRowNavigatorService.countMatchesByKey(rows, columns, key)).toBe(3);
    });

    it("returns 0 on empty inputs / no match", () => {
      expect(DataGuardRowNavigatorService.countMatchesByKey([], ["id"], "x")).toBe(0);
      expect(DataGuardRowNavigatorService.countMatchesByKey([{ id: "a" }], [], "x")).toBe(0);
    });
  });

  describe("navigate / getNav$", () => {
    it("multicasts the request to nav$ subscribers, stamping a requestId, and resolves Promise<boolean>", async () => {
      const svc = new DataGuardRowNavigatorService();
      const received: DataGuardRowNavRequest[] = [];
      svc.getNav$().subscribe(r => {
        received.push(r);
        // Auto-respond so navigate() resolves.
        svc.reportFlashResult({ requestId: r.requestId, flashed: true });
      });
      const flashed = await svc.navigate({ operatorId: "op-1", rowIndex: 42, column: "age" });
      expect(flashed).toBe(true);
      expect(received.length).toBe(1);
      expect(received[0].operatorId).toBe("op-1");
      expect(received[0].rowIndex).toBe(42);
      expect(received[0].column).toBe("age");
      expect(typeof received[0].requestId).toBe("number");
    });

    it("carries rowKey alongside rowIndex so subscribers can prefer the key", async () => {
      const svc = new DataGuardRowNavigatorService();
      const received: DataGuardRowNavRequest[] = [];
      svc.getNav$().subscribe(r => {
        received.push(r);
        svc.reportFlashResult({ requestId: r.requestId, flashed: true });
      });
      await svc.navigate({
        operatorId: "op-1",
        rowIndex: 4,
        rowKey: '"alice"25',
        column: "age",
      });
      expect(received[0].rowKey).toBe('"alice"25');
      expect(received[0].rowIndex).toBe(4);
    });

    it("assigns monotonically increasing requestIds across calls", async () => {
      const svc = new DataGuardRowNavigatorService();
      const seen: number[] = [];
      svc.getNav$().subscribe(r => {
        seen.push(r.requestId);
        svc.reportFlashResult({ requestId: r.requestId, flashed: true });
      });
      await svc.navigate({ operatorId: "op-1", rowIndex: 0 });
      await svc.navigate({ operatorId: "op-1", rowIndex: 1 });
      await svc.navigate({ operatorId: "op-1", rowIndex: 2 });
      expect(seen[1]).toBeGreaterThan(seen[0]);
      expect(seen[2]).toBeGreaterThan(seen[1]);
    });

    // R3: cold-mount replay. The ResultTableFrameComponent mounts lazily after
    // openResultPanel(), so a plain Subject would lose the emission. The
    // ReplaySubject(1) ensures the late subscriber still sees the most recent
    // request — this is the buffer behaviour reviewers asked us to lock down.
    it("replays the most recent request to a late subscriber (cold-mount)", () => {
      const svc = new DataGuardRowNavigatorService();
      // Fire-and-forget — we're testing the nav$ replay, not the awaiter.
      void svc.navigate({ operatorId: "op-1", rowIndex: 7, column: "x" });
      const received: DataGuardRowNavRequest[] = [];
      svc.getNav$().subscribe(r => received.push(r));
      expect(received.length).toBe(1);
      expect(received[0].rowIndex).toBe(7);
    });

    // R3 followup: the replay window is bounded (500 ms) so a request from a
    // long-ago click doesn't leak into a much later mount.
    it("does not replay requests older than the buffer window", fakeAsync(() => {
      const svc = new DataGuardRowNavigatorService();
      void svc.navigate({ operatorId: "op-1", rowIndex: 7 });
      tick(1000);
      const received: DataGuardRowNavRequest[] = [];
      svc.getNav$().subscribe(r => received.push(r));
      expect(received).toEqual([]);
    }));
  });

  describe("navigate() flash-confirmed Promise (rounds 3 + 4)", () => {
    // Round-3 mechanism: result-table-frame emits {requestId, flashed} on
    // flashResult$ and the checklist only advances its locate cursor on
    // `true`. Cures: silent skips (4 clicks → flashes at [0,1,(skip),3]
    // + 5th click wraps to 0) and rapid-click cursor drift.
    //
    // Round-4 fix: navigate() returns Promise<boolean> directly and subscribes
    // to flashResult$ BEFORE publishing on nav$. Without that ordering, a
    // SYNCHRONOUS fast-path emission (target row already on current page →
    // table-frame calls reportFlashResult inside the next() chain) would be
    // dropped by the Subject and the awaiter would hang the full safety
    // timeout — leaving the cursor stuck on indices[0] for every cycle.

    // CRITICAL round-4 regression lock — production order.
    //
    // The fake frame subscribes to nav$ FIRST (mimicking the real
    // ResultTableFrame's ngOnInit subscription) and on every emission calls
    // reportFlashResult SYNCHRONOUSLY inside the next() chain — exactly what
    // tryFlashOnCurrentPage produces on the fast path. Then the caller does
    // `await navigate()`. Without subscribe-before-next inside navigate(),
    // this Promise would hang until the 36 s safety timeout and resolve
    // false. This test would have caught the round-3 bug.
    it("regression: navigate resolves true when the frame reports synchronously inside the next() chain (fast path)", async () => {
      const svc = new DataGuardRowNavigatorService();
      svc.getNav$().subscribe(req => {
        // Synchronous emission — exactly what the fast path produces.
        svc.reportFlashResult({ requestId: req.requestId, flashed: true });
      });
      const flashed = await svc.navigate({ operatorId: "op-1", rowIndex: 1 });
      expect(flashed).toBe(true);
    });

    it("regression: a cycle of synchronous fast-path clicks advances each time", async () => {
      // Direct simulation of the user's scenario: 4 clicks on an issue with
      // 4 affected rows, each landing on the already-rendered page (fast
      // path = synchronous reportFlashResult). All 4 must resolve `true`
      // so the cursor advances through [0,1,2,3] without skipping.
      const svc = new DataGuardRowNavigatorService();
      svc.getNav$().subscribe(req => {
        svc.reportFlashResult({ requestId: req.requestId, flashed: true });
      });
      const results: boolean[] = [];
      for (let i = 0; i < 4; i++) {
        results.push(await svc.navigate({ operatorId: "op-1", rowIndex: i }));
      }
      expect(results).toEqual([true, true, true, true]);
    });

    it("navigate resolves false when the table-frame reports flashed=false", async () => {
      const svc = new DataGuardRowNavigatorService();
      svc.getNav$().subscribe(req => {
        svc.reportFlashResult({ requestId: req.requestId, flashed: false });
      });
      const flashed = await svc.navigate({ operatorId: "op-1", rowIndex: 1 });
      expect(flashed).toBe(false);
    });

    it("navigate ignores results stamped with an unrelated requestId", async () => {
      const svc = new DataGuardRowNavigatorService();
      svc.getNav$().subscribe(req => {
        // Wrong-id result first (would hang the awaiter if we didn't filter)
        // then the real one.
        svc.reportFlashResult({ requestId: req.requestId + 999, flashed: true });
        svc.reportFlashResult({ requestId: req.requestId, flashed: false });
      });
      const flashed = await svc.navigate({ operatorId: "op-1", rowIndex: 1 });
      expect(flashed).toBe(false);
    });

    // Required test 1: rapid two-click race. Click A's walk gets superseded;
    // cursor advances EXACTLY once (for B's success), not twice. The fake
    // frame emits A's flashed=false and B's flashed=true.
    it("rapid two-click race: superseded older walk does not advance the cursor", async () => {
      const svc = new DataGuardRowNavigatorService();
      let advances = 0;
      const seenRequests: number[] = [];
      // Don't auto-respond — we'll emit manually to control ordering.
      svc.getNav$().subscribe(req => seenRequests.push(req.requestId));
      const pA = svc.navigate({ operatorId: "op-1", rowIndex: 0 }).then(f => {
        if (f) advances++;
      });
      const pB = svc.navigate({ operatorId: "op-1", rowIndex: 1 }).then(f => {
        if (f) advances++;
      });
      // Report: A superseded (false), B succeeded (true).
      svc.reportFlashResult({ requestId: seenRequests[0], flashed: false });
      svc.reportFlashResult({ requestId: seenRequests[1], flashed: true });
      await Promise.all([pA, pB]);
      expect(advances).toBe(1);
    });

    // Required test 2: empty-click — the table-frame never reports anything.
    // navigate() must resolve `false` via the safety timeout so the cursor
    // stays put. Timeout is 10 * 3500 + 1000 = 36000 ms (round-4 concern #2).
    it("empty-click safety timeout resolves false so the cursor stays put", async () => {
      vi.useFakeTimers();
      try {
        const svc = new DataGuardRowNavigatorService();
        // No nav$ subscriber → no reportFlashResult ever fires. Wedged-frame
        // simulation.
        let resolved: boolean | undefined;
        const promise = svc.navigate({ operatorId: "op-1", rowIndex: 7 }).then(r => {
          resolved = r;
        });
        // Advance past the safety timeout (36000 ms).
        await vi.advanceTimersByTimeAsync(36500);
        await promise;
        expect(resolved).toBe(false);
      } finally {
        vi.useRealTimers();
      }
    });

    it("getFlashResult$ multicasts {requestId, flashed} to subscribers", () => {
      const svc = new DataGuardRowNavigatorService();
      const seen: { requestId: number; flashed: boolean }[] = [];
      svc.getFlashResult$().subscribe(r => seen.push(r));
      svc.reportFlashResult({ requestId: 1, flashed: true });
      svc.reportFlashResult({ requestId: 2, flashed: false });
      expect(seen).toEqual([
        { requestId: 1, flashed: true },
        { requestId: 2, flashed: false },
      ]);
    });
  });
});
