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

import { DataGuardAutoTriggerService, PARSERS, parseCsv, ParserOptions } from "./data-guard-auto-trigger.service";
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
  const isDataset = (t: string) => t === "CSVFileScan" || t === "CSVOldFileScan";

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
    const result = DataGuardAutoTriggerService.resolveRescanTarget("op-1-gone", [replacement], isDataset);
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

  it("treats CSVOldFileScan as a dataset operator alongside CSVFileScan", () => {
    // Regression: CSVOldFileScan was added in the parser-dispatcher refactor
    // so the auto-trigger fires for it too. Reusing the dispatcher's own
    // PARSERS map as the source of truth keeps the operator-type set and the
    // parser table from drifting apart silently.
    const isDatasetByPARSERS = (t: string) => Object.prototype.hasOwnProperty.call(PARSERS, t);
    const csvOld = op("op-csvold", "CSVOldFileScan");
    const result = DataGuardAutoTriggerService.resolveRescanTarget(undefined, [csvOld], isDatasetByPARSERS);
    expect(result.kind).toBe("candidate");
    if (result.kind === "candidate") expect(result.operator.operatorID).toBe("op-csvold");
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
      getAllAgents: () => ({
        subscribe: (o: { next: (v: unknown[]) => void; complete?: () => void }) => {
          o.next([{ id: "agent-1", delegate: { workflowId: 7 } }]);
          o.complete?.();
          return { unsubscribe() {} };
        },
      }),
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

    // Construct the service. Cast everything through `unknown`; this test
    // only depends on the concurrency-control surface, not on the deps' real
    // behaviour. `unknown` keeps the ESLint no-explicit-any rule happy and is
    // standard pattern for test-double injection.
    const asDep = <T>(x: unknown) => x as T;
    const svc = new DataGuardAutoTriggerService(
      asDep(workflowActionService),
      asDep(agentService),
      asDep(notificationService),
      asDep(settings),
      asDep(results),
      asDep(datasetService),
      asDep(executeWorkflowService),
      asDep(http)
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
        return {
          subscribe: (o: { next: (v: unknown) => void; complete?: () => void }) => {
            o.next({ issueCount: 0, issues: [], proposals: [] });
            o.complete?.();
            return { unsubscribe() {} };
          },
        };
      },
    };

    // Start pipeline #1 (simulating an auto-trigger) — it will suspend on the load.
    const first = (
      svc as unknown as { runPipeline: (op: OperatorPredicate, o: { userInitiated: boolean }) => Promise<void> }
    ).runPipeline(opA, { userInitiated: false });

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

/**
 * Parser-dispatch tests. The dispatcher is what protects us from
 * `Papa.parse`-ing JSON / Parquet bytes; it's the contract between the
 * auto-trigger and the per-format readers (CSV today, JSONL once dev-B's
 * module lands). These tests run against the exported `PARSERS` table and
 * `parseCsv` helper directly — no service harness required.
 */
describe("DataGuardAutoTriggerService parser dispatch", () => {
  it("registers CSVFileScan and CSVOldFileScan against parseCsv", () => {
    // CSVOld's Scala impl uses scala-csv's DefaultCSVFormat (RFC-4180), so
    // its bytes are identical-shape to CSVFileScan — they legitimately share
    // the same parser. ParallelCSVFileScan was removed because Texera has
    // disabled it in the operator registry (LogicalOp.scala:171).
    expect(PARSERS["CSVFileScan"]).toBe(parseCsv);
    expect(PARSERS["CSVOldFileScan"]).toBe(parseCsv);
    expect(PARSERS["ParallelCSVFileScan"]).toBeUndefined();
  });

  it("registers JSONLFileScan as a distinct parser (not parseCsv)", () => {
    // Dev-B's parseJsonl module has now landed and is wired in. The key
    // invariant we care about for the dispatcher contract: JSONL must NOT
    // dispatch through parseCsv — feeding NDJSON bytes to Papa would
    // produce one row with a useless single column. As long as the entry
    // exists and isn't parseCsv, the dispatcher is honest.
    expect(PARSERS["JSONLFileScan"]).toBeDefined();
    expect(PARSERS["JSONLFileScan"]).not.toBe(parseCsv);
  });

  it("parseCsv extracts headers and dynamically-typed rows from RFC-4180 bytes", async () => {
    const csv = "id,name,score\n1,Alice,9.5\n2,Bob,7\n";
    const blob = new Blob([csv], { type: "text/csv" });
    const out = await parseCsv(blob, "demo.csv");
    expect(out.columns).toEqual(["id", "name", "score"]);
    expect(out.rows).toEqual([
      { id: 1, name: "Alice", score: 9.5 },
      { id: 2, name: "Bob", score: 7 },
    ]);
  });

  it("parseCsv handles a CSVOld-style blob identically to a canonical CSV blob", async () => {
    // Same bytes, two operator types. Since both CSVFileScan and
    // CSVOldFileScan dispatch to parseCsv, the detector pipeline sees an
    // identical {columns, rows} structure regardless of which operator
    // produced it — which is the contract the 5 detector categories rely on.
    const csv = "x,y\nfoo,1\nbar,2\n";
    const blob1 = new Blob([csv], { type: "text/csv" });
    const blob2 = new Blob([csv], { type: "text/csv" });
    const viaCsv = await PARSERS["CSVFileScan"](blob1, "a.csv");
    const viaCsvOld = await PARSERS["CSVOldFileScan"](blob2, "a.csv");
    expect(viaCsvOld).toEqual(viaCsv);
  });

  it("parseCsv honors an explicit `;` delimiter for CSVOld-style bytes", async () => {
    // CSVOld exposes a `customDelimiter` operator property. The Scala impl
    // overrides scala-csv's delimiter with whatever the user picked (`,` /
    // `;` / `\t` / …). Papa's autodetect *usually* gets this right, but
    // we now pass the operator's choice through explicitly so parsing is
    // byte-for-byte equivalent to the operator's own reader.
    const csv = "id;name;score\n1;Alice;9.5\n2;Bob;7\n";
    const blob = new Blob([csv], { type: "text/csv" });
    const out = await parseCsv(blob, "demo.csv", { delimiter: ";" });
    expect(out.columns).toEqual(["id", "name", "score"]);
    expect(out.rows).toEqual([
      { id: 1, name: "Alice", score: 9.5 },
      { id: 2, name: "Bob", score: 7 },
    ]);
  });

  it("parseCsv honors an explicit tab delimiter", async () => {
    // Tab-delimited (`\t`) is the other realistic CSVOld customDelimiter
    // value. Asserting it separately because Papa's autodetect handles
    // tabs differently from semicolons.
    const csv = "id\tname\n1\tAlice\n2\tBob\n";
    const blob = new Blob([csv], { type: "text/csv" });
    const out = await parseCsv(blob, "demo.tsv", { delimiter: "\t" });
    expect(out.columns).toEqual(["id", "name"]);
    expect(out.rows).toEqual([
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ]);
  });

  it("parseCsv throws a descriptive error on malformed input", async () => {
    // Mismatched quotes — Papa surfaces an error; our wrapper rethrows with
    // a row reference so the toast/log is actionable.
    const bad = 'col1,col2\n"unterminated,oops\n';
    const blob = new Blob([bad], { type: "text/csv" });
    let err: unknown = null;
    try {
      await parseCsv(blob, "bad.csv");
    } catch (e) {
      err = e;
    }
    expect(err).not.toBeNull();
    expect((err as Error).message).toMatch(/CSV parse failed/);
  });
});

/**
 * Pure tests for the parser-options extractor. This is the bridge between an
 * operator's properties (`op.operatorProperties`) and a parser's runtime
 * options (`ParserOptions`). Keeping the mapping in a static helper means
 * adding a new operator-specific knob = one line here + one line in the
 * parser, no TestBed needed to assert it.
 */
describe("DataGuardAutoTriggerService.extractParserOptions", () => {
  function op(operatorType: string, properties: Record<string, unknown> = {}): OperatorPredicate {
    return {
      operatorID: "op-1",
      operatorType,
      operatorProperties: properties,
      inputPorts: [],
      outputPorts: [],
      showAdvanced: false,
      isDisabled: false,
      customDisplayName: operatorType,
      operatorVersion: "0",
    } as unknown as OperatorPredicate;
  }

  it("pulls customDelimiter off a CSVOldFileScan", () => {
    const opts: ParserOptions = DataGuardAutoTriggerService.extractParserOptions(
      op("CSVOldFileScan", { customDelimiter: ";" })
    );
    expect(opts.delimiter).toBe(";");
  });

  it("omits delimiter when CSVOldFileScan has no customDelimiter", () => {
    // The Scala side defaults an empty/missing customDelimiter to "," — and
    // Papa's autodetect handles the common case well — so we deliberately
    // leave `delimiter` undefined and let Papa decide rather than hard-code.
    const opts = DataGuardAutoTriggerService.extractParserOptions(op("CSVOldFileScan", {}));
    expect(opts.delimiter).toBeUndefined();
  });

  it("ignores customDelimiter on non-CSVOld operators", () => {
    // A stray `customDelimiter` on a CSVFileScan op should NOT leak through.
    // Only CSVOldFileScan opts in to the property (its Scala desc declares
    // it); we don't want to silently inherit it elsewhere.
    const opts = DataGuardAutoTriggerService.extractParserOptions(op("CSVFileScan", { customDelimiter: ";" }));
    expect(opts.delimiter).toBeUndefined();
  });

  it("returns an empty options object for an unknown operator type", () => {
    expect(DataGuardAutoTriggerService.extractParserOptions(op("JSONLFileScan"))).toEqual({});
  });
});
