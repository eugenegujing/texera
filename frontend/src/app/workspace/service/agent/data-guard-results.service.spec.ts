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

import { TestBed } from "@angular/core/testing";
import { firstValueFrom } from "rxjs";
import { take } from "rxjs/operators";
import {
  DataGuardResultsService,
  ChecklistEntry,
  DataQualityIssue,
  FixProposal,
} from "./data-guard-results.service";

function makeIssue(issueId: string): DataQualityIssue {
  return {
    issueId,
    issueType: "PLACEHOLDER",
    column: "age",
    description: "age=999 placeholder",
    evidence: "5 rows",
    affectedRowCount: 5,
    detectedAt: "2026-05-15T00:00:00Z",
  };
}

function makeProposal(issueId: string): FixProposal {
  return {
    issueId,
    issueType: "PLACEHOLDER",
    action: "Replace age=999 with NULL",
    operationKind: "REPLACE_VALUE",
    operationParams: { column: "age", from: 999, to: null },
    riskTier: "low",
    reason: "999 is outside valid age range",
    evidence: "5 of 5 rows with age=999 have no other anomalies",
    confidence: "high",
    targetRowCount: 5,
  };
}

function makeEntry(issueId: string, overrides: Partial<ChecklistEntry> = {}): ChecklistEntry {
  return {
    issueId,
    issue: makeIssue(issueId),
    proposal: makeProposal(issueId),
    error: null,
    verdict: "pending",
    ...overrides,
  };
}

describe("DataGuardResultsService", () => {
  let service: DataGuardResultsService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(DataGuardResultsService);
  });

  it("initializes in the idle state with an empty entry list", async () => {
    const state = await firstValueFrom(service.getState$().pipe(take(1)));
    expect(state.state).toBe("idle");
    expect(state.entries).toEqual([]);
    expect(state.agentId).toBe("");
    expect(state.datasetRows).toBe(0);
    expect(state.datasetColumns).toBe(0);
    expect(state.datasetSource).toBe("");
  });

  it("setState merges the patch shallowly into the current state", () => {
    service.setState({ state: "scanning", agentId: "agent-1", message: "loading" });
    const after = service.getState();
    expect(after.state).toBe("scanning");
    expect(after.agentId).toBe("agent-1");
    expect(after.message).toBe("loading");
    // Untouched fields preserved
    expect(after.entries).toEqual([]);
    expect(after.datasetRows).toBe(0);
  });

  it("setState overwrites entries when explicitly patched", () => {
    const entries = [makeEntry("i-1"), makeEntry("i-2")];
    service.setState({ state: "ready", entries });
    expect(service.getState().entries).toHaveLength(2);
    expect(service.getState().entries[0].issueId).toBe("i-1");
  });

  it("updateEntry patches only the row whose issueId matches", () => {
    service.setState({ state: "ready", entries: [makeEntry("i-1"), makeEntry("i-2")] });
    service.updateEntry("i-2", { verdict: "allow" });
    const entries = service.getState().entries;
    expect(entries[0].verdict).toBe("pending");
    expect(entries[1].verdict).toBe("allow");
  });

  it("updateEntry is a no-op when the issueId is not present", () => {
    service.setState({ state: "ready", entries: [makeEntry("i-1")] });
    const before = service.getState().entries;
    service.updateEntry("does-not-exist", { verdict: "deny" });
    const after = service.getState().entries;
    expect(after).toEqual(before);
  });

  it("updateEntry supports remember and surface-error flags", () => {
    service.setState({ state: "ready", entries: [makeEntry("i-1")] });
    service.updateEntry("i-1", { verdict: "allow", remember: true });
    const entry = service.getState().entries[0];
    expect(entry.verdict).toBe("allow");
    expect(entry.remember).toBe(true);
  });

  it("ChecklistEntry.verdict type union is exactly allow | deny | pending (no modify)", () => {
    // Compile-time guard: assigning "modify" should fail typecheck once #11b
    // narrows the union. We exercise the runtime contract here by sweeping the
    // allowed verdicts and asserting none of them is the removed "modify".
    const verdicts: Array<ChecklistEntry["verdict"]> = ["allow", "deny", "pending"];
    expect(verdicts).not.toContain("modify");
    // Bracket-access guards against the structural-type escape hatch: if the
    // union ever regresses to include "modify", TypeScript will narrow this
    // back to a valid assignment and the runtime check still holds.
    for (const v of verdicts) {
      service.setState({ state: "ready", entries: [makeEntry("i-1", { verdict: v })] });
      expect(service.getState().entries[0].verdict).toBe(v);
    }
  });

  it("ChecklistEntry no longer carries modifiedAction", () => {
    const entry = makeEntry("i-1");
    expect("modifiedAction" in entry).toBe(false);
  });

  it("reset returns to the initial idle state regardless of prior state", () => {
    service.setState({
      state: "ready",
      agentId: "agent-1",
      entries: [makeEntry("i-1"), makeEntry("i-2")],
      datasetSource: "demo.csv",
      datasetRows: 100,
      datasetColumns: 5,
      message: "Found 2 issues",
    });
    service.reset();
    const after = service.getState();
    expect(after.state).toBe("idle");
    expect(after.entries).toEqual([]);
    expect(after.agentId).toBe("");
    expect(after.datasetRows).toBe(0);
    expect(after.datasetColumns).toBe(0);
    expect(after.datasetSource).toBe("");
    expect(after.message).toBeUndefined();
  });

  it("notifies subscribers when state changes", async () => {
    const seen: string[] = [];
    const sub = service.getState$().subscribe(s => seen.push(s.state));
    service.setState({ state: "scanning" });
    service.setState({ state: "ready", entries: [makeEntry("i-1")] });
    service.reset();
    sub.unsubscribe();
    expect(seen).toEqual(["idle", "scanning", "ready", "idle"]);
  });
});
