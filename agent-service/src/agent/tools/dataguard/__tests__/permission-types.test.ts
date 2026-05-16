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

// Type-level contract tests for the Modify-verdict cut (task #11a / #15).
//
// `tsc --noEmit` is part of the QA gate (bun run typecheck). Any one of the
// ts-expect-error directives below failing to fire WILL produce a compile
// error of the form "Unused 'ts-expect-error' directive". (Avoiding the @
// prefix in this comment so tsc does not parse it as a directive itself.)
// Backend's job (in #15) is to narrow `Verdict` to "allow" | "deny" (plus the
// two internal auto_allow_* sentinels) and to drop `modifiedAction` from
// `PermissionDecision`, `DecisionLogEntry`, and `RecordDecisionInput`.

import { describe, expect, test } from "bun:test";
import type {
  DecisionLogEntry,
  PermissionDecision,
  Verdict,
} from "../../../../types/dataguard";
import type { RecordDecisionInput } from "../dataguard-session";

describe("Verdict type — Modify is gone (#11a)", () => {
  test("a value of \"modify\" is NOT assignable to Verdict", () => {
    // @ts-expect-error "modify" is removed from the Verdict union by #11a.
    const v: Verdict = "modify";
    // The .toBe argument is also "modify" and that argument is statically
    // checked against the Verdict union — silence the same error here too.
    // @ts-expect-error
    expect(v).toBe("modify");
  });

  test("\"allow\" and \"deny\" remain valid Verdict members", () => {
    const allow: Verdict = "allow";
    const deny: Verdict = "deny";
    expect(allow).toBe("allow");
    expect(deny).toBe("deny");
  });

  test("the two internal auto_allow_* sentinels remain valid", () => {
    const low: Verdict = "auto_allow_low_risk";
    const remembered: Verdict = "auto_allow_remembered";
    expect(low).toBe("auto_allow_low_risk");
    expect(remembered).toBe("auto_allow_remembered");
  });
});

describe("PermissionDecision — modifiedAction is gone (#11a)", () => {
  test("constructing a PermissionDecision with `modifiedAction` is a type error", () => {
    const d: PermissionDecision = {
      stepId: "step-1",
      verdict: "allow",
      // @ts-expect-error `modifiedAction` is removed from PermissionDecision by #11a.
      modifiedAction: "Flag instead of replace",
    };
    expect(d.stepId).toBe("step-1");
  });

  test("a minimal PermissionDecision with only stepId + verdict still type-checks", () => {
    const d: PermissionDecision = { stepId: "step-1", verdict: "deny" };
    expect(d.verdict).toBe("deny");
  });
});

describe("DecisionLogEntry — modifiedAction is gone (#11a)", () => {
  test("constructing a DecisionLogEntry with `modifiedAction` is a type error", () => {
    const e: DecisionLogEntry = {
      decisionId: "dec-1",
      timestamp: "2026-05-15T00:00:00.000Z",
      issueType: "placeholder_value",
      targetRowCount: 5,
      proposedAction: "Replace age=999 with NULL",
      userDecision: "allow",
      // @ts-expect-error `modifiedAction` is removed from DecisionLogEntry by #11a.
      modifiedAction: "Flag instead of replace",
      reason: "test",
      confidence: "high",
    };
    expect(e.decisionId).toBe("dec-1");
  });
});

describe("RecordDecisionInput — modifiedAction is gone (#11a)", () => {
  test("DataGuardSession.recordDecision callers can no longer pass `modifiedAction`", () => {
    const proposal = {
      issueId: "iss-1",
      issueType: "placeholder_value" as const,
      action: "Replace age=999 with NULL",
      operationKind: "replace_value" as const,
      operationParams: {},
      riskTier: "medium" as const,
      reason: "test",
      evidence: "test",
      confidence: "high" as const,
      targetRowCount: 5,
    };
    const input: RecordDecisionInput = {
      proposal,
      verdict: "allow",
      // @ts-expect-error `modifiedAction` is removed from RecordDecisionInput by #11a.
      modifiedAction: "Flag instead of replace",
      applied: true,
    };
    expect(input.verdict).toBe("allow");
  });
});
