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

import { describe, expect, test } from "bun:test";
import { requestApproval, type ApprovalGateway } from "../with-approval";
import type { FixProposal, IssueType, PermissionDecision, RiskTier } from "../../../../types/dataguard";

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
  decisions: Map<string, PermissionDecision> = new Map();
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
    if (this.decisions.has(stepId)) {
      return Promise.resolve(this.decisions.get(stepId)!);
    }
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

describe("requestApproval", () => {
  test("auto-allows low-risk fixes without prompting", async () => {
    const gw = new MockGateway();
    const decision = await requestApproval(gw, makeProposal({ riskTier: "low" }));
    expect(decision.verdict).toBe("auto_allow_low_risk");
    expect(gw.emitted).toHaveLength(0);
  });

  test("auto-allows when the issueType matches a remembered rule", async () => {
    const gw = new MockGateway();
    gw.rules.add("placeholder_value");
    const decision = await requestApproval(gw, makeProposal({ riskTier: "medium" }));
    expect(decision.verdict).toBe("auto_allow_remembered");
    expect(gw.emitted).toHaveLength(0);
  });

  test("medium risk without remembered rule → emits pending and waits", async () => {
    const gw = new MockGateway();
    const promise = requestApproval(gw, makeProposal({ riskTier: "medium" }));
    // Pending emitted synchronously before the promise resolves.
    expect(gw.emitted).toHaveLength(1);
    expect(gw.emitted[0].stepId).toBe("mock-step-1");

    // Simulate user clicking Allow.
    gw.resolveLater("mock-step-1", { stepId: "mock-step-1", verdict: "allow" });

    const decision = await promise;
    expect(decision.verdict).toBe("allow");
    expect(decision.stepId).toBe("mock-step-1");
  });

  test("high risk: prompts every time even with a remembered rule", async () => {
    const gw = new MockGateway();
    gw.rules.add("outlier");
    const promise = requestApproval(gw, makeProposal({ issueType: "outlier", riskTier: "high" }));
    expect(gw.emitted).toHaveLength(1);
    gw.resolveLater("mock-step-1", { stepId: "mock-step-1", verdict: "deny" });
    const decision = await promise;
    expect(decision.verdict).toBe("deny");
  });

  test("'modify' verdict carries through with the modifiedAction", async () => {
    const gw = new MockGateway();
    const promise = requestApproval(gw, makeProposal({ riskTier: "medium" }));
    gw.resolveLater("mock-step-1", {
      stepId: "mock-step-1",
      verdict: "modify",
      modifiedAction: "Flag instead of replace",
    });
    const decision = await promise;
    expect(decision.verdict).toBe("modify");
    expect(decision.modifiedAction).toBe("Flag instead of replace");
  });

  test("a decision that arrives before the tool awaits is buffered and delivered", async () => {
    const gw = new MockGateway();
    // The decision is pre-recorded BEFORE the tool starts awaiting. This
    // matches a race where the user clicks before the agent has finished
    // emitting the pending step on this side.
    gw.resolveLater("mock-step-1", { stepId: "mock-step-1", verdict: "allow" });
    const decision = await requestApproval(gw, makeProposal({ riskTier: "medium" }));
    expect(decision.verdict).toBe("allow");
  });
});
