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
import { DataGuardSession } from "../dataguard-session";
import type { DatasetView } from "../dataset";
import type { DataQualityIssue, FixProposal } from "../../../../types/dataguard";

function makeIssue(): DataQualityIssue {
  return {
    issueId: "iss-1",
    issueType: "placeholder_value",
    column: "age",
    description: "5 rows have age=999",
    evidence: "5 of 5 placeholder-only.",
    affectedRowCount: 5,
    detectedAt: "2026-05-14T12:00:00.000Z",
  };
}

function makeProposal(): FixProposal {
  return {
    issueId: "iss-1",
    issueType: "placeholder_value",
    action: "Replace age=999 with NULL",
    operationKind: "replace_value",
    operationParams: { column: "age", match: 999, replacement: null },
    riskTier: "medium",
    reason: "out of range",
    evidence: "5 rows",
    confidence: "high",
    targetRowCount: 5,
  };
}

describe("DataGuardSession", () => {
  test("setDataset stores the dataset and resets per-run state", () => {
    const s = new DataGuardSession();
    const ds: DatasetView = { columns: ["a"], rows: [{ a: 1 }] };
    s.recordIssue(makeIssue());
    s.setDataset(ds);
    expect(s.getDataset()).toBe(ds);
    expect(s.getIssues()).toEqual([]);
    expect(s.getDecisionLog()).toEqual([]);
  });

  test("recordIssue accumulates and dedupes by issueId", () => {
    const s = new DataGuardSession();
    s.recordIssue(makeIssue());
    s.recordIssue(makeIssue()); // same issueId — should not duplicate
    expect(s.getIssues()).toHaveLength(1);
  });

  test("recordDecision appends a DecisionLogEntry", () => {
    const s = new DataGuardSession();
    s.setDataset({ columns: [], rows: [] });
    s.recordDecision({
      proposal: makeProposal(),
      verdict: "allow",
      applied: true,
    });
    const log = s.getDecisionLog();
    expect(log).toHaveLength(1);
    expect(log[0].userDecision).toBe("allow");
    expect(log[0].issueType).toBe("placeholder_value");
    expect(log[0].appliedAt).toBeDefined();
  });

  test("recordDecision with denied: no appliedAt", () => {
    const s = new DataGuardSession();
    s.recordDecision({ proposal: makeProposal(), verdict: "deny", applied: false });
    expect(s.getDecisionLog()[0].appliedAt).toBeUndefined();
  });

  test("addAutoAllowRule registers, matchesAutoAllowRule returns true", () => {
    const s = new DataGuardSession();
    s.addAutoAllowRule("placeholder_value");
    expect(s.matchesAutoAllowRule("placeholder_value")).toBe(true);
    expect(s.matchesAutoAllowRule("outlier")).toBe(false);
  });

  test("addAutoAllowRule is idempotent (does not duplicate)", () => {
    const s = new DataGuardSession();
    s.addAutoAllowRule("placeholder_value");
    s.addAutoAllowRule("placeholder_value");
    expect(s.getAutoAllowRules()).toHaveLength(1);
  });

  test("removeAutoAllowRule clears the rule by id", () => {
    const s = new DataGuardSession();
    const rule = s.addAutoAllowRule("placeholder_value");
    expect(s.removeAutoAllowRule(rule.ruleId)).toBe(true);
    expect(s.matchesAutoAllowRule("placeholder_value")).toBe(false);
  });
});
