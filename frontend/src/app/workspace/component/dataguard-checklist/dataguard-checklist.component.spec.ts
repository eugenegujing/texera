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

import { vi } from "vitest";
import { BehaviorSubject } from "rxjs";
import { DataGuardChecklistComponent } from "./dataguard-checklist.component";
import { ChecklistEntry, DataGuardScanResult } from "../../service/agent/data-guard-results.service";
import type { DataGuardRowNavRequest } from "../../service/agent/data-guard-row-navigator.service";

type NavRequest = Omit<DataGuardRowNavRequest, "requestId">;

/**
 * Operator-type-aware locate branching: CSV scans take the synchronous index
 * path (cursor advances immediately, no await), while JSONL keeps the
 * flash-confirmed Promise contract introduced in round 4. The tests construct
 * the component directly with stub collaborators so we exercise just
 * `onShowInResultPanel` — TestBed would drag in change detection and the full
 * results-service wiring we don't need here.
 */
describe("DataGuardChecklistComponent.onShowInResultPanel — locate branching", () => {
  const SOURCE_OP_ID = "scan-1";

  function makeEntry(rowIndices: number[], rowKeys?: string[]): ChecklistEntry {
    return {
      issueId: "issue-1",
      issue: {
        issueId: "issue-1",
        issueType: "missing_value",
        column: "age",
        description: "x",
        evidence: "y",
        affectedRowCount: rowIndices.length,
        affectedRowIndices: rowIndices,
        affectedRowKeys: rowKeys,
        detectedAt: "now",
      },
      proposal: null,
      error: null,
      verdict: "pending",
    };
  }

  function makeComponent(operatorType: string, navigateImpl: () => Promise<boolean>) {
    const scanState: DataGuardScanResult = {
      agentId: "a",
      state: "ready",
      entries: [],
      datasetSource: "demo.csv",
      datasetRows: 10,
      datasetColumns: 3,
      sourceOperatorId: SOURCE_OP_ID,
    };
    const state$ = new BehaviorSubject<DataGuardScanResult>(scanState);

    const results = {
      getState$: () => state$.asObservable(),
      getState: () => state$.value,
      updateEntry: vi.fn(),
      reset: vi.fn(),
    } as any;

    const autoTrigger = {
      startOrchestration: () => ({ unsubscribe: () => {} }),
      rescanAny: vi.fn(),
      rescanCurrent: vi.fn(),
      applyBatch: vi.fn(),
    } as any;

    const settings = { isEnabled: () => true } as any;

    const navigateSpy = vi.fn((_req: NavRequest) => navigateImpl());
    const rowNavigator = { navigate: navigateSpy } as any;

    const jointGraph = {
      getCurrentHighlightedOperatorIDs: () => [SOURCE_OP_ID],
      unhighlightOperators: vi.fn(),
      highlightOperators: vi.fn(),
    };
    const texeraGraph = {
      getAllOperators: () => [{ operatorID: SOURCE_OP_ID, operatorType }],
      getOperator: (id: string) => (id === SOURCE_OP_ID ? { operatorID: SOURCE_OP_ID, operatorType } : undefined),
    };
    const workflowActionService = {
      getJointGraphWrapper: () => jointGraph,
      getTexeraGraph: () => texeraGraph,
      getWorkflowMetadata: () => ({ wid: 1 }),
      openResultPanel: vi.fn(),
    } as any;

    const notificationService = {
      info: vi.fn(),
      warning: vi.fn(),
    } as any;

    const component = new DataGuardChecklistComponent(
      results,
      autoTrigger,
      settings,
      workflowActionService,
      rowNavigator,
      notificationService
    );
    // Hydrate the component's view of state without going through ngOnInit
    // (which would also subscribe to the auto-trigger orchestration stream we
    // don't need here).
    (component as any).scan = scanState;
    return { component, navigateSpy, notificationService };
  }

  it("CSV path: advances cursor synchronously and fires navigate without awaiting", async () => {
    // Pending Promise — if the component were awaiting it, the cursor would
    // not be advanced by the time onShowInResultPanel resolves to us. (It
    // resolves after a single microtask defer.)
    let resolveNavigate!: (v: boolean) => void;
    const pending = new Promise<boolean>(r => (resolveNavigate = r));
    const { component, navigateSpy } = makeComponent("CSVFileScan", () => pending);

    const entry = makeEntry([3, 7, 12, 18]);
    const click = component.onShowInResultPanel(entry);
    await click; // returns once cursor advance + void navigate has been issued

    // navigate fired exactly once, WITHOUT a rowKey — that's the CSV signal
    // to result-table-frame to take the simple index path.
    expect(navigateSpy).toHaveBeenCalledTimes(1);
    const payload = navigateSpy.mock.calls[0]![0]!;
    expect(payload.operatorId).toBe(SOURCE_OP_ID);
    expect(payload.rowIndex).toBe(3);
    expect(payload.rowKey).toBeUndefined();

    // Cursor advanced immediately — without resolving the Promise.
    expect((component as any).locateCursors.get("issue-1")).toBe(1);

    // Clean up the dangling Promise so vitest doesn't complain.
    resolveNavigate(true);
  });

  it("JSONL path: computes rowKeyOccurrence as the count of prior identical keys in the cursor walk", async () => {
    // Simulates a duplicate-row issue: 4 affected rows, 2 unique keys. The
    // checklist must hand the table-frame the occurrence index so each click
    // lands on a distinct display row even though all 4 fingerprints are
    // pairwise identical for the same dup group.
    //
    // Pattern [k1, k2, k1, k2] mimics two J001 dups interleaved with two J004
    // dups (worker-shuffled JSONL). The expected occurrences are:
    //   click 1 (cursor=0, key=k1): no prior k1 → 0
    //   click 2 (cursor=1, key=k2): no prior k2 → 0
    //   click 3 (cursor=2, key=k1): one prior k1 → 1
    //   click 4 (cursor=3, key=k2): one prior k2 → 1
    const captured: Array<{ rowKey?: string; rowKeyOccurrence?: number }> = [];
    const { component } = makeComponent("JSONLFileScan", () => Promise.resolve(true));
    // Hijack navigate to record what payload it sees (the default spy is fine
    // but we want both fields in one place per call).
    const realNavigate = (component as any).rowNavigator.navigate as ReturnType<typeof vi.fn>;
    realNavigate.mockImplementation((req: NavRequest) => {
      captured.push({ rowKey: req.rowKey, rowKeyOccurrence: req.rowKeyOccurrence });
      return Promise.resolve(true);
    });

    const entry = makeEntry([1, 2, 3, 4], ["k1", "k2", "k1", "k2"]);
    for (let i = 0; i < 4; i++) await component.onShowInResultPanel(entry);

    expect(captured.map(c => c.rowKey)).toEqual(["k1", "k2", "k1", "k2"]);
    expect(captured.map(c => c.rowKeyOccurrence)).toEqual([0, 0, 1, 1]);
  });

  it("JSONL path: four dup clicks (all identical keys) yield occurrences 0..3 and advance the cursor each time", async () => {
    // True duplicate_id case: every affectedRowKey is the SAME string (k0).
    // findRowByKey would return the same display index for all 4 clicks; the
    // occurrence parameter is what saves us. Each click resolves true so the
    // cursor advances 0→1→2→3 and the requested occurrence walks 0→1→2→3.
    const captured: Array<{ rowKeyOccurrence?: number }> = [];
    const { component } = makeComponent("JSONLFileScan", () => Promise.resolve(true));
    const realNavigate = (component as any).rowNavigator.navigate as ReturnType<typeof vi.fn>;
    realNavigate.mockImplementation((req: NavRequest) => {
      captured.push({ rowKeyOccurrence: req.rowKeyOccurrence });
      return Promise.resolve(true);
    });

    const sameKey = "dup-key";
    const entry = makeEntry([10, 11, 12, 13], [sameKey, sameKey, sameKey, sameKey]);
    for (let i = 0; i < 4; i++) await component.onShowInResultPanel(entry);

    expect(captured.map(c => c.rowKeyOccurrence)).toEqual([0, 1, 2, 3]);
    expect((component as any).locateCursors.get("issue-1")).toBe(4);
  });

  it("CSV path: rowKeyOccurrence is irrelevant — no rowKey is sent, occurrence defaults on the table side", async () => {
    // The CSV branch deliberately skips fingerprint matching entirely (single
    // worker → display order matches profiler order). It must NOT set rowKey
    // or rowKeyOccurrence, so the result-table-frame routes to
    // handleLocateByIndex unchanged.
    const captured: NavRequest[] = [];
    const { component } = makeComponent("CSVFileScan", () => Promise.resolve(true));
    const realNavigate = (component as any).rowNavigator.navigate as ReturnType<typeof vi.fn>;
    realNavigate.mockImplementation((req: NavRequest) => {
      captured.push(req);
      return Promise.resolve(true);
    });
    const entry = makeEntry([0, 1, 2, 3], ["k0", "k0", "k0", "k0"]);
    await component.onShowInResultPanel(entry);
    expect(captured[0].rowKey).toBeUndefined();
    expect(captured[0].rowKeyOccurrence).toBeUndefined();
  });

  it("JSONL path: cursor advances only after navigate() resolves true, stays put on false", async () => {
    // First click: navigate resolves true → cursor advances.
    const { component, navigateSpy } = makeComponent("JSONLFileScan", () => Promise.resolve(true));
    const entry = makeEntry([3, 7, 12, 18], ["k0", "k1", "k2", "k3"]);

    const clickPromise = component.onShowInResultPanel(entry);
    // Before the await settles, the cursor should still be at its starting
    // position — proves we did not eagerly write it like the CSV branch does.
    expect((component as any).locateCursors.get("issue-1")).toBeUndefined();
    await clickPromise;
    expect((component as any).locateCursors.get("issue-1")).toBe(1);
    // rowKey was passed through — that's the JSONL signal to the table side.
    expect(navigateSpy.mock.calls[0]![0]!.rowKey).toBe("k0");

    // Second click: navigate resolves false → cursor stays at 1, next click
    // will retry the same target rather than skipping it. Because a rowKey
    // was sent (JSONL path), a not-found toast must fire so the user knows
    // the data has drifted from the scan instead of being silently dropped
    // on a wrong byte-order row.
    const { component: c2, navigateSpy: spy2, notificationService: notif2 } = makeComponent(
      "JSONLFileScan",
      () => Promise.resolve(false)
    );
    (c2 as any).locateCursors.set("issue-1", 1);
    await c2.onShowInResultPanel(entry);
    expect((c2 as any).locateCursors.get("issue-1")).toBe(1);
    expect(spy2).toHaveBeenCalledTimes(1);
    expect(spy2.mock.calls[0]![0]!.rowIndex).toBe(7); // step.value at cursor=1
    expect(notif2.info).toHaveBeenCalledTimes(1);
    expect(notif2.info.mock.calls[0]![0]).toMatch(/couldn't find this row/i);
  });

  it("CSV path: no toast on navigate() false (legitimate index fallback, not a drift signal)", async () => {
    // CSV is single-worker, so the index path is the intended target — no
    // rowKey is sent. A false outcome there means the navigate timed out
    // mid-page-render or was superseded by a newer click, not that the row
    // can't be found. Toasting here would be noisy on every rapid double-click.
    const { component, notificationService } = makeComponent(
      "CSVFileScan",
      () => Promise.resolve(false)
    );
    const entry = makeEntry([0, 1, 2, 3], ["k0", "k0", "k0", "k0"]);
    await component.onShowInResultPanel(entry);
    expect(notificationService.info).not.toHaveBeenCalled();
  });
});
