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
import { serializeDecisionLogCsv } from "../decision-log";
import type { DecisionLogEntry } from "../../../../types/dataguard";

function entry(overrides: Partial<DecisionLogEntry> = {}): DecisionLogEntry {
  return {
    decisionId: "dec-1",
    timestamp: "2026-05-14T12:00:30.000Z",
    issueType: "placeholder_value",
    targetRowCount: 5,
    proposedAction: "Replace age=999 with NULL",
    userDecision: "allow",
    reason: "out of valid range",
    confidence: "high",
    appliedAt: "2026-05-14T12:00:31.000Z",
    ...overrides,
  };
}

describe("serializeDecisionLogCsv", () => {
  test("empty log returns header only", () => {
    const csv = serializeDecisionLogCsv([]);
    expect(csv.split("\n")).toEqual([
      "decision_id,timestamp,issue_type,target_rows,proposed_action,user_decision,modified_action,reason,confidence,applied_at",
    ]);
  });

  test("single row: header + one data row", () => {
    const csv = serializeDecisionLogCsv([entry()]);
    const lines = csv.split("\n");
    expect(lines).toHaveLength(2);
    expect(lines[1]).toContain("dec-1");
    expect(lines[1]).toContain("placeholder_value");
    expect(lines[1]).toContain("allow");
  });

  test("escapes commas, quotes, and newlines in fields per RFC 4180", () => {
    const csv = serializeDecisionLogCsv([
      entry({
        proposedAction: 'Replace "999" with NULL, including row 3',
        reason: "line1\nline2",
      }),
    ]);
    const dataRow = csv.split("\n").slice(1).join("\n");
    expect(dataRow).toContain('"Replace ""999"" with NULL, including row 3"');
    expect(dataRow).toContain('"line1\nline2"');
  });

  test("missing appliedAt and modifiedAction render as empty fields", () => {
    const csv = serializeDecisionLogCsv([
      entry({ userDecision: "deny", appliedAt: undefined }),
    ]);
    const row = csv.split("\n")[1];
    expect(row.endsWith(",")).toBe(true); // appliedAt is the last column and is empty
    expect(row).toContain(",,"); // modifiedAction is empty between reason+confidence's neighbors
  });

  test("multiple rows preserve insertion order", () => {
    const csv = serializeDecisionLogCsv([
      entry({ decisionId: "dec-1", issueType: "placeholder_value" }),
      entry({ decisionId: "dec-2", issueType: "missing_value" }),
      entry({ decisionId: "dec-3", issueType: "outlier", userDecision: "deny" }),
    ]);
    const lines = csv.split("\n").slice(1);
    expect(lines[0]).toContain("dec-1");
    expect(lines[1]).toContain("dec-2");
    expect(lines[2]).toContain("dec-3");
    expect(lines[2]).toContain("deny");
  });

  test("auto_allow_low_risk and auto_allow_remembered survive the round trip", () => {
    const csv = serializeDecisionLogCsv([
      entry({ userDecision: "auto_allow_low_risk" }),
      entry({ userDecision: "auto_allow_remembered" }),
    ]);
    expect(csv).toContain("auto_allow_low_risk");
    expect(csv).toContain("auto_allow_remembered");
  });
});
