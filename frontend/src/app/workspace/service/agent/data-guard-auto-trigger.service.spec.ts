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

import { DataGuardAutoTriggerService } from "./data-guard-auto-trigger.service";
import { OperatorPredicate } from "../../types/workflow-common.interface";

/**
 * Pure-logic tests for the rescan target resolver. Exercising the static
 * helper avoids a full TestBed harness for what is, fundamentally, a switch
 * over (priorOpId × operator graph). The behaviour we care about:
 *
 *  - Floater click after the panel was reset (priorOpId undefined) must still
 *    pick up the dataset operator on the canvas — the user-reported symptom
 *    was the second click silently doing nothing.
 *  - When the prior operator is gone but a different dataset operator exists,
 *    fall through to the candidate rather than warning "nothing on canvas".
 *  - When nothing on the canvas is a dataset operator, return "none" so the
 *    caller can surface the right toast.
 */
describe("DataGuardAutoTriggerService.resolveRescanTarget", () => {
  function op(operatorID: string, operatorType: string): OperatorPredicate {
    return {
      operatorID,
      operatorType,
      operatorProperties: {},
      inputPorts: [],
      outputPorts: [],
      showAdvanced: false,
      isDisabled: false,
      customDisplayName: operatorID,
      operatorVersion: "0",
    } as unknown as OperatorPredicate;
  }
  const isDataset = (t: string) => t === "CSVFileScan" || t === "ParallelCSVFileScan";

  it("returns 'prior' when the prior operator is still on the graph", () => {
    const a = op("op-1", "CSVFileScan");
    const b = op("op-2", "ViewResult");
    const result = DataGuardAutoTriggerService.resolveRescanTarget("op-1", [a, b], isDataset);
    expect(result.kind).toBe("prior");
    if (result.kind === "prior") expect(result.operator.operatorID).toBe("op-1");
  });

  it("falls through to a 'candidate' when priorOpId is undefined", () => {
    // Symptom of the floater bug: after onClose(), results.reset() sets
    // sourceOperatorId=undefined. The floater click must still find the
    // dataset operator that's sitting on the canvas.
    const a = op("op-1", "CSVFileScan");
    const result = DataGuardAutoTriggerService.resolveRescanTarget(undefined, [a], isDataset);
    expect(result.kind).toBe("candidate");
    if (result.kind === "candidate") expect(result.operator.operatorID).toBe("op-1");
  });

  it("falls through to a 'candidate' when the prior operator was removed", () => {
    // User deleted the originally-scanned operator and dropped a new one. We
    // shouldn't warn "nothing to rescan" just because the prior id is stale.
    const replacement = op("op-2", "CSVFileScan");
    const result = DataGuardAutoTriggerService.resolveRescanTarget(
      "op-1-gone",
      [replacement],
      isDataset
    );
    expect(result.kind).toBe("candidate");
    if (result.kind === "candidate") expect(result.operator.operatorID).toBe("op-2");
  });

  it("returns 'none' when no dataset operator is on the canvas", () => {
    const a = op("op-1", "ViewResult");
    const result = DataGuardAutoTriggerService.resolveRescanTarget(undefined, [a], isDataset);
    expect(result.kind).toBe("none");
  });

  it("returns 'none' on an empty canvas", () => {
    const result = DataGuardAutoTriggerService.resolveRescanTarget(undefined, [], isDataset);
    expect(result.kind).toBe("none");
  });

  it("returns 'prior' even when the survivor is no longer a dataset operator", () => {
    // Edge case: results state somehow has a sourceOperatorId pointing at an
    // operator that is no longer a dataset operator (e.g., the user replaced
    // the CSVFileScan's contents). 'prior' branch will return it, runPipeline
    // will then read no fileName and bail with the userInitiated message —
    // this is acceptable; the helper isn't responsible for re-filtering by
    // type once it has a live match. Test pins behaviour.
    const stale = op("op-1", "ViewResult");
    const result = DataGuardAutoTriggerService.resolveRescanTarget("op-1", [stale], isDataset);
    expect(result.kind).toBe("prior");
  });
});

/**
 * Concurrency control: a user-initiated rescan that arrives while another
 * pipeline is already in flight must SERIALIZE behind it — never fire a
 * second concurrent /scan. This test exercises the runPipeline Promise-
 * tracking field (`currentPipeline`) by constructing the service with
 * collaborator stubs and gating its first pipeline on a deferred promise.
 */
describe("DataGuardAutoTriggerService concurrent pipeline serialization", () => {
  function op(operatorID: string, operatorType: string, fileName = "/o/d/v/x.csv"): OperatorPredicate {
    return {
      operatorID,
      operatorType,
      operatorProperties: { fileName },
      inputPorts: [],
      outputPorts: [],
      showAdvanced: false,
      isDisabled: false,
      customDisplayName: operatorID,
      operatorVersion: "0",
    } as unknown as OperatorPredicate;
  }

  function deferred<T>(): { promise: Promise<T>; resolve: (v: T) => void } {
    let resolve!: (v: T) => void;
    const promise = new Promise<T>(r => {
      resolve = r;
    });
    return { promise, resolve };
  }

  /**
   * Reach into the private slot the production code uses. We're testing
   * the concurrency invariant ("at most one pipeline running at a time
   * for a user click"), so peeking at the field is the most direct way
   * to assert what we care about without an integration harness.
   */
  function pipelineSlot(svc: unknown): Promise<void> | null {
    return (svc as { currentPipeline: Promise<void> | null }).currentPipeline;
  }

  it("a user-initiated rescan waits for the in-flight pipeline instead of running concurrently", async () => {
    // Gate the first pipeline's load step on a deferred promise so we can
    // hold it open and observe what happens when a second user click arrives
    // while it's still in flight.
    const loadGate = deferred<{ source: string; rows: number; columns: number }>();
    let loadCallCount = 0;
    let scanCallCount = 0;

    const opA = op("op-1", "CSVFileScan", "/o/d/v/a.csv");

    // Minimal stubs — just enough for runPipeline to reach the gated load
    // step and stop, and for rescanAny to discover opA and resolve.
    const graphStub = {
      getOperator: (id: string) => (id === "op-1" ? opA : undefined),
      getAllOperators: () => [opA],
      getOperatorAddStream: () => ({ pipe: () => ({ subscribe: () => ({ unsubscribe() {} }) }) }),
      getOperatorPropertyChangeStream: () => ({ pipe: () => ({ subscribe: () => ({ unsubscribe() {} }) }) }),
    };
    const workflowActionService = {
      getTexeraGraph: () => graphStub,
      getWorkflowMetadata: () => ({ wid: 7 }),
    };
    const agentService = {
      getAllAgents: () => ({ subscribe: (o: { next: (v: unknown[]) => void; complete?: () => void }) => { o.next([{ id: "agent-1", delegate: { workflowId: 7 } }]); o.complete?.(); return { unsubscribe() {} }; } }),
    };
    const notificationService = {
      info: () => {},
      warning: () => {},
      error: () => {},
      success: () => {},
    };
    const settings = { isEnabled: () => true };

    // BehaviorSubject-shaped enough for runPipeline.results.setState +
    // rescanAny's read of state.sourceOperatorId.
    const stateHolder: { [k: string]: unknown } = { state: "idle", entries: [], sourceOperatorId: undefined };
    const results = {
      getState: () => stateHolder,
      setState: (patch: Record<string, unknown>) => Object.assign(stateHolder, patch),
    };

    const datasetService = {
      retrieveDatasetVersionSingleFile: () => ({ subscribe: () => ({ unsubscribe() {} }) }),
    };
    const executeWorkflowService = { executeWorkflow: () => {} };
    const http = {
      post: () => ({ subscribe: () => ({ unsubscribe() {} }) }),
    };

    // Construct the service. Cast everything; this test only depends on
    // the concurrency-control surface, not on the deps' real behaviour.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const svc = new DataGuardAutoTriggerService(
      workflowActionService as any,
      agentService as any,
      notificationService as any,
      settings as any,
      results as any,
      datasetService as any,
      executeWorkflowService as any,
      http as any
    );

    // Replace loadFromOperatorFile with the gated stub so the first pipeline
    // suspends predictably.
    (svc as unknown as { loadFromOperatorFile: unknown }).loadFromOperatorFile = () => {
      loadCallCount++;
      return loadGate.promise;
    };
    // Replace ensureAgent so we don't depend on agentService.createAgent.
    (svc as unknown as { ensureAgent: unknown }).ensureAgent = async () => "agent-1";
    // Spy the /scan POST. If a second pipeline ran concurrently, this would
    // observe a second call before loadGate.resolve() fires.
    (svc as unknown as { http: { post: () => unknown } }).http = {
      post: () => {
        scanCallCount++;
        return { subscribe: (o: { next: (v: unknown) => void; complete?: () => void }) => { o.next({ issueCount: 0, issues: [], proposals: [] }); o.complete?.(); return { unsubscribe() {} }; } };
      },
    };

    // Start pipeline #1 (simulating an auto-trigger) — it will suspend on the load.
    const first = (svc as unknown as { runPipeline: (op: OperatorPredicate, o: { userInitiated: boolean }) => Promise<void> })
      .runPipeline(opA, { userInitiated: false });

    // Yield once so runPipeline reaches the load await.
    await Promise.resolve();
    expect(loadCallCount).toBe(1);
    expect(scanCallCount).toBe(0);
    expect(pipelineSlot(svc)).not.toBeNull();

    // Now: user clicks the floater while #1 is suspended.
    const second = svc.rescanAny();

    // Let microtasks drain. The second call MUST NOT start a parallel
    // load or /scan while #1 is gated.
    await Promise.resolve();
    await Promise.resolve();
    expect(loadCallCount).toBe(1); // still only the first
    expect(scanCallCount).toBe(0); // no parallel /scan POSTed

    // Release the gate. Pipeline #1 completes, currentPipeline.finally
    // clears the slot, and pipeline #2 proceeds — loadCallCount becomes 2.
    loadGate.resolve({ source: "x", rows: 0, columns: 0 });

    await first;
    await second;

    // Both ran, but serialized — second only started after first finished.
    expect(loadCallCount).toBe(2);
    expect(scanCallCount).toBe(2);
    expect(pipelineSlot(svc)).toBeNull();
  });
});
