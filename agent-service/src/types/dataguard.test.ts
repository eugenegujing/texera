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
import type {
  AutoAllowRule,
  Confidence,
  DataQualityIssue,
  DecisionLogEntry,
  FixOperationKind,
  FixProposal,
  IssueType,
  PermissionDecision,
  RiskTier,
  Verdict,
} from "./dataguard";

// These tests double as runtime fixtures for downstream tools. Each shape is
// instantiated with realistic data drawn from the design doc's §5 storyboard
// so that any drift in the type definitions surfaces here first.

describe("DataGuard type shapes", () => {
  test("DataQualityIssue: placeholder-value example (high-affinity, small N)", () => {
    const issue: DataQualityIssue = {
      issueId: "iss-1",
      issueType: "placeholder_value",
      column: "age",
      description: "5 rows have age = 999",
      evidence: "5 of 5 rows with age=999 have no other anomalies.",
      affectedRowCount: 5,
      affectedRowIndices: [10, 42, 77, 199, 412],
      detectedAt: "2026-05-14T12:00:00.000Z",
    };
    expect(issue.issueType).toBe("placeholder_value");
    expect(issue.affectedRowCount).toBe(5);
    expect(issue.affectedRowIndices).toHaveLength(5);
  });

  test("DataQualityIssue: missing-value example without row indices (large N)", () => {
    const issue: DataQualityIssue = {
      issueId: "iss-2",
      issueType: "missing_value",
      column: "glucose",
      description: "17 missing glucose values, 14 in Group A",
      evidence: "Group A: 14 of 200 rows missing; Group B: 3 of 200 rows missing.",
      affectedRowCount: 17,
      detectedAt: "2026-05-14T12:00:01.000Z",
    };
    expect(issue.affectedRowIndices).toBeUndefined();
  });

  test("FixProposal: replace-value, medium risk, high confidence", () => {
    const proposal: FixProposal = {
      issueId: "iss-1",
      issueType: "placeholder_value",
      action: "Replace age = 999 with NULL",
      operationKind: "replace_value",
      operationParams: { column: "age", match: 999, replacement: null },
      riskTier: "medium",
      reason: "999 is outside the valid human-age range and appears to be a placeholder.",
      evidence: "5 of 5 rows with age=999 have no other anomalies.",
      confidence: "high",
      targetRowCount: 5,
    };
    expect(proposal.riskTier).toBe("medium");
    expect(proposal.operationKind).toBe("replace_value");
    expect(proposal.operationParams).toMatchObject({ column: "age", match: 999 });
  });

  test("FixProposal: drop-rows, high risk (the storyboard 'deny' case)", () => {
    const proposal: FixProposal = {
      issueId: "iss-3",
      issueType: "outlier",
      action: "Drop 3 rows with BMI > 60",
      operationKind: "drop_rows",
      operationParams: { rowIndices: [55, 211, 433] },
      riskTier: "high",
      reason: "Extreme outliers may be data-entry errors.",
      evidence: "3 rows have BMI > 60 (clinical maximum ~70).",
      confidence: "low",
      targetRowCount: 3,
    };
    expect(proposal.riskTier).toBe("high");
  });

  test("DecisionLogEntry: allowed and applied", () => {
    const entry: DecisionLogEntry = {
      decisionId: "dec-1",
      timestamp: "2026-05-14T12:00:30.000Z",
      issueType: "placeholder_value",
      targetRowCount: 5,
      proposedAction: "Replace age = 999 with NULL",
      userDecision: "allow",
      reason: "999 outside valid age range.",
      confidence: "high",
      appliedAt: "2026-05-14T12:00:31.123Z",
    };
    expect(entry.userDecision).toBe("allow");
    expect(entry.appliedAt).toBeDefined();
    // @ts-expect-error modifiedAction was cut from DecisionLogEntry by #11a;
    // this assertion locks the absence of the property at the type level.
    expect(entry.modifiedAction).toBeUndefined();
  });

  test("DecisionLogEntry: denied — no appliedAt", () => {
    const entry: DecisionLogEntry = {
      decisionId: "dec-2",
      timestamp: "2026-05-14T12:01:00.000Z",
      issueType: "outlier",
      targetRowCount: 3,
      proposedAction: "Drop 3 rows with BMI > 60",
      userDecision: "deny",
      reason: "User flagged these as meaningful clinical cases.",
      confidence: "low",
    };
    expect(entry.userDecision).toBe("deny");
    expect(entry.appliedAt).toBeUndefined();
  });

  test("AutoAllowRule: per-issue-type policy", () => {
    const rule: AutoAllowRule = {
      ruleId: "rule-1",
      issueType: "placeholder_value",
      createdAt: "2026-05-14T12:00:30.000Z",
    };
    expect(rule.issueType).toBe("placeholder_value");
  });

  test("PermissionDecision: allow with remember=true triggers a rule write", () => {
    const decision: PermissionDecision = {
      stepId: "step-43",
      verdict: "allow",
      remember: true,
    };
    expect(decision.remember).toBe(true);
  });

  test("PermissionDecision: deny", () => {
    const decision: PermissionDecision = {
      stepId: "step-44",
      verdict: "deny",
    };
    expect(decision.verdict).toBe("deny");
  });

  test("Literal unions accept all documented members", () => {
    const risks: RiskTier[] = ["low", "medium", "high", "warning"];
    const confidences: Confidence[] = ["low", "medium", "high"];
    const issueTypes: IssueType[] = [
      "placeholder_value",
      "missing_value",
      "duplicate_id",
      "outlier",
      "inconsistent_label",
    ];
    const opKinds: FixOperationKind[] = [
      "replace_value",
      "drop_rows",
      "impute",
      "standardize",
      "trim_whitespace",
      "rename_column",
    ];
    const verdicts: Verdict[] = [
      "allow",
      "deny",
      "auto_allow_low_risk",
      "auto_allow_remembered",
    ];
    expect(risks).toHaveLength(4);
    expect(confidences).toHaveLength(3);
    expect(issueTypes).toHaveLength(5);
    expect(opKinds).toHaveLength(6);
    expect(verdicts).toHaveLength(4);
  });
});
