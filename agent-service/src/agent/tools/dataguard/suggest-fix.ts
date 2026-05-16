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

// Takes a DataQualityIssue from profile_dataset and asks an LLM for a single
// concrete FixProposal. Read-only with respect to the dataset: it only
// proposes, never applies.

import { z } from "zod";
import type { DataQualityIssue, FixProposal, RiskTier } from "../../../types/dataguard";

export type LlmCallFn = (prompt: string) => Promise<string>;

export interface SuggestFixOptions {
  llmCall: LlmCallFn;
}

const fixProposalSchema = z.object({
  action: z.string().min(1),
  operationKind: z.enum([
    "replace_value",
    "drop_rows",
    "impute",
    "standardize",
    "trim_whitespace",
    "rename_column",
  ]),
  operationParams: z.record(z.string(), z.unknown()),
  riskTier: z.enum(["low", "medium", "high", "warning"]),
  reason: z.string().min(1),
  evidence: z.string().min(1),
  confidence: z.enum(["low", "medium", "high"]),
  targetRowCount: z.number().int().nonnegative(),
});

// `warning` here = "we have a concrete fix but recommend a human eyeball it
// first" — the checklist UI defaults these to unchecked. Used for cases where
// auto-applying could destroy meaningful signal (outliers that might be real
// extremes, biologically plausible out-of-range values).
const DEFAULT_RISK_TIER_BY_ISSUE: Record<string, RiskTier> = {
  placeholder_value: "medium",
  missing_value: "medium",
  duplicate_id: "high",
  // `outlier` is the validRanges-based detector — see profile-dataset.ts. The
  // earlier z-score outlier detector was removed.
  outlier: "warning",
  inconsistent_label: "medium",
};

export async function suggestFix(
  issue: DataQualityIssue,
  options: SuggestFixOptions
): Promise<FixProposal> {
  const prompt = buildPrompt(issue);
  const rawResponse = await options.llmCall(prompt);
  const cleaned = stripCodeFences(rawResponse);

  let parsed: unknown;
  try {
    parsed = JSON.parse(cleaned);
  } catch (e) {
    throw new Error(
      `suggest_fix: LLM returned invalid JSON for issue ${issue.issueId}: ${(e as Error).message}`
    );
  }

  const validated = fixProposalSchema.safeParse(parsed);
  if (!validated.success) {
    throw new Error(
      `suggest_fix: LLM proposal failed schema validation for issue ${issue.issueId}: ${validated.error.message}`
    );
  }

  // Override LLM-supplied issueId/issueType with the server-side values to
  // keep the contract honest: the LLM can suggest *what* to do, but it does
  // not control *which* issue this proposal is bound to.
  return {
    issueId: issue.issueId,
    issueType: issue.issueType,
    ...validated.data,
  };
}

export function buildPrompt(issue: DataQualityIssue): string {
  const defaultTier = DEFAULT_RISK_TIER_BY_ISSUE[issue.issueType] ?? "medium";
  const indicesLine =
    issue.affectedRowIndices && issue.affectedRowIndices.length > 0
      ? `\n- affectedRowIndices: [${issue.affectedRowIndices.join(", ")}]`
      : "";
  return `You are a data-cleaning assistant. Propose a single concrete fix for the following data-quality issue. Reply with one JSON object only — no prose, no markdown, no fences.

Issue:
- type: ${issue.issueType}
- column: ${issue.column}
- description: ${issue.description}
- evidence: ${issue.evidence}
- affectedRowCount: ${issue.affectedRowCount}${indicesLine}

Required JSON shape:
{
  "action": "<one-sentence human-readable description of the fix>",
  "operationKind": "replace_value | drop_rows | impute | standardize | trim_whitespace | rename_column",
  "operationParams": { ...operation-specific params... },
  "riskTier": "low | medium | high | warning",
  "reason": "<one-sentence justification>",
  "evidence": "<one-sentence supporting data from the issue>",
  "confidence": "low | medium | high",
  "targetRowCount": ${issue.affectedRowCount}
}

operationParams by kind:
- replace_value: { "column": string, "replacement": any, "rowIndices"?: number[], "match"?: any }
    Use "rowIndices" when the issue gives you affectedRowIndices — index targeting
    is deterministic. Use "match" only for value-based swaps (e.g. replace every
    literal "unknown" with null) when you don't have indices. Never invent a
    "match" value from aggregate stats — silent no-ops happen when match doesn't
    equal the cell exactly (e.g. 950 vs 950.0 vs "950").
- drop_rows: { "rowIndices": number[] }
- impute: { "column": string, "strategy": "mean" | "median" | "mode" }
- standardize: { "column": string, "mapping": { [from: string]: string } }
- trim_whitespace: { "column": string }
- rename_column: { "from": string, "to": string }

Rules:
- Every proposal must be a concrete, applicable fix — no "flag-and-do-nothing" options.
- For outlier issues (values outside the valid range stated in description / evidence): use replace_value with "rowIndices" set to the affectedRowIndices and "replacement" set to the nearest valid bound (min or max from the range), or to null if no bound is sensible. Never use "match" for outliers — always use rowIndices.
- Set riskTier="warning" when you have a concrete fix but the user really should eyeball it before applying — typical for outlier values that are unusual but possibly real (extreme biological readings, etc.). The UI defaults these to unchecked so the user makes an explicit call.
- Prefer impute over drop_rows for missing values.

Default risk tier for ${issue.issueType}: ${defaultTier}. Override only with a strong reason.`;
}

function stripCodeFences(s: string): string {
  const trimmed = s.trim();
  if (!trimmed.startsWith("```")) return trimmed;
  const lines = trimmed.split("\n");
  const last = lines[lines.length - 1]?.trim() ?? "";
  const sliced = last === "```" ? lines.slice(1, -1) : lines.slice(1);
  return sliced.join("\n").trim();
}
