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
import { suggestFix, type LlmCallFn } from "../suggest-fix";
import type { DataQualityIssue } from "../../../../types/dataguard";

function makeIssue(overrides: Partial<DataQualityIssue> = {}): DataQualityIssue {
  return {
    issueId: "iss-test-1",
    issueType: "placeholder_value",
    column: "age",
    description: "5 rows have age = 999",
    evidence: "5 of 5 rows with age=999 have no other anomalies.",
    affectedRowCount: 5,
    affectedRowIndices: [10, 42, 77, 199, 412],
    detectedAt: "2026-05-14T12:00:00.000Z",
    ...overrides,
  };
}

const VALID_RAW_JSON = JSON.stringify({
  action: "Replace age = 999 with NULL",
  operationKind: "replace_value",
  operationParams: { column: "age", match: 999, replacement: null },
  riskTier: "medium",
  reason: "999 is outside the valid human-age range and appears to be a placeholder.",
  evidence: "5 of 5 rows with age=999 have no other anomalies.",
  confidence: "high",
  targetRowCount: 5,
});

function constantLlm(payload: string): LlmCallFn {
  return async () => payload;
}

describe("suggestFix", () => {
  test("parses a valid LLM JSON payload into a FixProposal", async () => {
    const issue = makeIssue();
    const proposal = await suggestFix(issue, { llmCall: constantLlm(VALID_RAW_JSON) });
    expect(proposal.issueId).toBe(issue.issueId);
    expect(proposal.issueType).toBe("placeholder_value");
    expect(proposal.operationKind).toBe("replace_value");
    expect(proposal.riskTier).toBe("medium");
    expect(proposal.confidence).toBe("high");
    expect(proposal.targetRowCount).toBe(5);
  });

  test("strips ```json``` code fences before parsing", async () => {
    const fenced = "```json\n" + VALID_RAW_JSON + "\n```";
    const proposal = await suggestFix(makeIssue(), { llmCall: constantLlm(fenced) });
    expect(proposal.operationKind).toBe("replace_value");
  });

  test("strips bare ``` fences before parsing", async () => {
    const fenced = "```\n" + VALID_RAW_JSON + "\n```";
    const proposal = await suggestFix(makeIssue(), { llmCall: constantLlm(fenced) });
    expect(proposal.riskTier).toBe("medium");
  });

  test("issueId and issueType are set from the issue, not the LLM", async () => {
    // The LLM payload claims a different issueType — we ignore it and use the
    // server-side issue's type to keep the contract honest.
    const proposalIgnoredFields = {
      ...JSON.parse(VALID_RAW_JSON),
      issueId: "wrong-id-from-llm",
      issueType: "outlier",
    };
    const issue = makeIssue({ issueId: "iss-real-7", issueType: "missing_value" });
    const proposal = await suggestFix(issue, {
      llmCall: constantLlm(JSON.stringify(proposalIgnoredFields)),
    });
    expect(proposal.issueId).toBe("iss-real-7");
    expect(proposal.issueType).toBe("missing_value");
  });

  test("throws on invalid JSON", async () => {
    await expect(suggestFix(makeIssue(), { llmCall: constantLlm("not json at all") })).rejects.toThrow(/invalid JSON/);
  });

  test("throws when required field is missing", async () => {
    const bad = { ...JSON.parse(VALID_RAW_JSON) };
    delete bad.operationKind;
    await expect(suggestFix(makeIssue(), { llmCall: constantLlm(JSON.stringify(bad)) })).rejects.toThrow(
      /schema validation/
    );
  });

  test("throws when operationKind is not a known enum member", async () => {
    const bad = { ...JSON.parse(VALID_RAW_JSON), operationKind: "delete_database" };
    await expect(suggestFix(makeIssue(), { llmCall: constantLlm(JSON.stringify(bad)) })).rejects.toThrow(
      /schema validation/
    );
  });

  test("throws when riskTier is not low|medium|high", async () => {
    const bad = { ...JSON.parse(VALID_RAW_JSON), riskTier: "critical" };
    await expect(suggestFix(makeIssue(), { llmCall: constantLlm(JSON.stringify(bad)) })).rejects.toThrow(
      /schema validation/
    );
  });

  test("passes issue details into the prompt for the LLM", async () => {
    let captured = "";
    const issue = makeIssue({
      issueType: "duplicate_id",
      column: "sample_id",
      description: "3 duplicate sample IDs",
    });
    const proposal = await suggestFix(issue, {
      llmCall: async prompt => {
        captured = prompt;
        return VALID_RAW_JSON;
      },
    });
    expect(captured).toContain("duplicate_id");
    expect(captured).toContain("sample_id");
    expect(captured).toContain("3 duplicate sample IDs");
    expect(proposal).toBeDefined();
  });

  test("propagates LLM transport errors", async () => {
    const issue = makeIssue();
    await expect(
      suggestFix(issue, {
        llmCall: async () => {
          throw new Error("connection refused");
        },
      })
    ).rejects.toThrow(/connection refused/);
  });
});
