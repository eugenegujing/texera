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
    return { component, navigateSpy };
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
    // will retry the same target rather than skipping it.
    const { component: c2, navigateSpy: spy2 } = makeComponent("JSONLFileScan", () => Promise.resolve(false));
    (c2 as any).locateCursors.set("issue-1", 1);
    await c2.onShowInResultPanel(entry);
    expect((c2 as any).locateCursors.get("issue-1")).toBe(1);
    expect(spy2).toHaveBeenCalledTimes(1);
    expect(spy2.mock.calls[0]![0]!.rowIndex).toBe(7); // step.value at cursor=1
  });
});
