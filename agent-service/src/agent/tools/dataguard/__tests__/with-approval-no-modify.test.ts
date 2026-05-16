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

// Post-cut behavior pin for the with-approval permission gate (#11a).
// The legacy chat-flow gate (`requestApproval` in with-approval.ts) is still
// wired for any future caller (e.g. WorkflowGuard). After Modify is cut, the
// gate must:
//
//   • still resolve cleanly on "allow" and "deny" verdicts arriving over WS
//   • never receive a "modify" verdict — and the type system enforces this
//     (the @ts-expect-error below proves `verdict: "modify"` won't compile)
//
// The pre-existing test in `with-approval.test.ts` named
// "'modify' verdict carries through with the modifiedAction" is expected to
// be removed by backend during #15 (it constructs a PermissionDecision
// literal containing `verdict: "modify"`, which will be a type error after
// the cut).

import { describe, expect, test } from "bun:test";
import { requestApproval, type ApprovalGateway } from "../with-approval";
import type { FixProposal, IssueType, PermissionDecision } from "../../../../types/dataguard";

function makeProposal(overrides: Partial<FixProposal> = {}): FixProposal {
  return {
    issueId: "iss-1",
    issueType: "placeholder_value",
    action: "Replace age=999 with NULL",
    operationKind: "replace_value",
    operationParams: { column: "age", match: 999, replacement: null },
    riskTier: "medium",
    reason: "test",
    evidence: "test",
    confidence: "high",
    targetRowCount: 5,
    ...overrides,
  };
}

class MockGateway implements ApprovalGateway {
  rules: Set<IssueType> = new Set();
  emitted: Array<{ stepId: string; proposal: FixProposal }> = [];
  private decisions: Map<string, PermissionDecision> = new Map();
  private waiters: Map<string, (d: PermissionDecision) => void> = new Map();
  private counter = 0;

  matchesAutoAllowRule(issueType: IssueType): boolean {
    return this.rules.has(issueType);
  }
  generateStepId(): string {
    this.counter += 1;
    return `mock-step-${this.counter}`;
  }
  emitPendingApproval(stepId: string, proposal: FixProposal): void {
    this.emitted.push({ stepId, proposal });
  }
  awaitDecision(stepId: string): Promise<PermissionDecision> {
    if (this.decisions.has(stepId)) return Promise.resolve(this.decisions.get(stepId)!);
    return new Promise(resolve => this.waiters.set(stepId, resolve));
  }
  resolveLater(stepId: string, decision: PermissionDecision): void {
    const w = this.waiters.get(stepId);
    if (w) {
      this.waiters.delete(stepId);
      w(decision);
    } else {
      this.decisions.set(stepId, decision);
    }
  }
}

describe("requestApproval after Modify cut (#11a)", () => {
  test("medium-risk allow flow round-trips with no modifiedAction in sight", async () => {
    const gw = new MockGateway();
    const promise = requestApproval(gw, makeProposal({ riskTier: "medium" }));
    expect(gw.emitted).toHaveLength(1);
    gw.resolveLater("mock-step-1", { stepId: "mock-step-1", verdict: "allow" });
    const decision = await promise;
    expect(decision.verdict).toBe("allow");
    expect(decision).not.toHaveProperty("modifiedAction");
  });

  test("high-risk deny flow round-trips with no modifiedAction in sight", async () => {
    const gw = new MockGateway();
    const promise = requestApproval(gw, makeProposal({ riskTier: "high" }));
    expect(gw.emitted).toHaveLength(1);
    gw.resolveLater("mock-step-1", { stepId: "mock-step-1", verdict: "deny" });
    const decision = await promise;
    expect(decision.verdict).toBe("deny");
    expect(decision).not.toHaveProperty("modifiedAction");
  });

  test('type system rejects a PermissionDecision literal with verdict: "modify"', () => {
    // @ts-expect-error "modify" is no longer a Verdict member after #11a.
    const bad: PermissionDecision = { stepId: "x", verdict: "modify" };
    expect(bad.stepId).toBe("x");
  });
});
