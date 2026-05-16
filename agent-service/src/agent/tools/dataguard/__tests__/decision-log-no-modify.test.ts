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

// Decision-log contract tests for the Modify-verdict cut (task #11a / #15).
// The CSV-shaped audit trail in §4.4 of the design doc currently carries a
// `modified_action` column. After #11a that column is gone and so is the
// `modifiedAction` property on each in-memory DecisionLogEntry produced by
// DataGuardSession.recordDecision.

import { describe, expect, test } from "bun:test";
import { serializeDecisionLogCsv } from "../decision-log";
import { DataGuardSession } from "../dataguard-session";
import type { DecisionLogEntry, FixProposal } from "../../../../types/dataguard";

function entry(overrides: Partial<DecisionLogEntry> = {}): DecisionLogEntry {
  return {
    decisionId: "dec-1",
    timestamp: "2026-05-15T12:00:00.000Z",
    issueType: "placeholder_value",
    targetRowCount: 5,
    proposedAction: "Replace age=999 with NULL",
    userDecision: "allow",
    reason: "out of valid range",
    confidence: "high",
    appliedAt: "2026-05-15T12:00:01.000Z",
    ...overrides,
  };
}

function proposal(overrides: Partial<FixProposal> = {}): FixProposal {
  return {
    issueId: "iss-1",
    issueType: "placeholder_value",
    action: "Replace age=999 with NULL",
    operationKind: "replace_value",
    operationParams: { column: "age", match: 999, replacement: null },
    riskTier: "medium",
    reason: "out of valid range",
    evidence: "5 of 5 placeholder rows",
    confidence: "high",
    targetRowCount: 5,
    ...overrides,
  };
}

describe("serializeDecisionLogCsv header — modified_action column removed (#11a)", () => {
  test("empty-log header is the 9-column schema with no `modified_action`", () => {
    const csv = serializeDecisionLogCsv([]);
    const header = csv.split("\n")[0];
    expect(header).toBe(
      "decision_id,timestamp,issue_type,target_rows,proposed_action,user_decision,reason,confidence,applied_at"
    );
    expect(header).not.toContain("modified_action");
  });

  test("data rows have exactly 9 fields (matching the 9-column header)", () => {
    const csv = serializeDecisionLogCsv([entry()]);
    const lines = csv.split("\n");
    const headerCols = lines[0].split(",").length;
    // Use a CSV-aware split for the data row to handle quoted fields safely;
    // here the fixture has no commas inside fields so a plain split is fine.
    const dataCols = lines[1].split(",").length;
    expect(headerCols).toBe(9);
    expect(dataCols).toBe(9);
  });
});

describe("DataGuardSession.recordDecision — modifiedAction is gone (#11a)", () => {
  test("written entries never carry a `modifiedAction` field", () => {
    const session = new DataGuardSession();
    session.recordProposal(proposal());
    session.recordDecision({
      proposal: proposal(),
      verdict: "allow",
      applied: true,
    });
    const log = session.getDecisionLog();
    expect(log).toHaveLength(1);
    expect(log[0]).not.toHaveProperty("modifiedAction");
  });

  test("a denied decision likewise has no `modifiedAction`", () => {
    const session = new DataGuardSession();
    session.recordProposal(proposal());
    session.recordDecision({
      proposal: proposal(),
      verdict: "deny",
      applied: false,
    });
    const log = session.getDecisionLog();
    expect(log[0]).not.toHaveProperty("modifiedAction");
  });

  test("auto_allow_low_risk and auto_allow_remembered entries also lack `modifiedAction`", () => {
    const session = new DataGuardSession();
    session.recordProposal(proposal());
    session.recordDecision({
      proposal: proposal(),
      verdict: "auto_allow_low_risk",
      applied: true,
    });
    session.recordDecision({
      proposal: proposal({ issueId: "iss-2" }),
      verdict: "auto_allow_remembered",
      applied: true,
    });
    const log = session.getDecisionLog();
    expect(log).toHaveLength(2);
    expect(log[0]).not.toHaveProperty("modifiedAction");
    expect(log[1]).not.toHaveProperty("modifiedAction");
  });
});
