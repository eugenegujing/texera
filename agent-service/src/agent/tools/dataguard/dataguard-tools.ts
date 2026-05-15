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

// Vercel AI SDK tool definitions for DataGuard.
// Three tools exposed to the LLM:
//   - profile_dataset (read-only)
//   - suggest_fix    (read-only)
//   - apply_fix      (mutating — gated by requestApproval)
// The decision log is written automatically inside apply_fix; an explicit
// write_decision_log tool (Step 10) exports the log to CSV at session end.

import { z } from "zod";
import { tool } from "ai";
import { profileDataset, type ProfileOptions } from "./profile-dataset";
import { suggestFix, type LlmCallFn } from "./suggest-fix";
import { applyFix } from "./apply-fix";
import { requestApproval, type ApprovalGateway } from "./with-approval";
import type { DataGuardSession } from "./dataguard-session";
import { createWriteDecisionLogTool, TOOL_NAME_WRITE_DECISION_LOG } from "./decision-log";
import { createBiasCheckTool, TOOL_NAME_BIAS_CHECK } from "./bias-check";

export const TOOL_NAME_PROFILE_DATASET = "profile_dataset";
export const TOOL_NAME_SUGGEST_FIX = "suggest_fix";
export const TOOL_NAME_APPLY_FIX = "apply_fix";

export interface DataGuardToolContext {
  session: DataGuardSession;
  gateway: ApprovalGateway;
  llmCall: LlmCallFn;
}

export function createDataGuardTools(ctx: DataGuardToolContext): Record<string, unknown> {
  return {
    [TOOL_NAME_PROFILE_DATASET]: createProfileDatasetTool(ctx),
    [TOOL_NAME_SUGGEST_FIX]: createSuggestFixTool(ctx),
    [TOOL_NAME_APPLY_FIX]: createApplyFixTool(ctx),
    [TOOL_NAME_WRITE_DECISION_LOG]: createWriteDecisionLogTool(ctx.session),
    [TOOL_NAME_BIAS_CHECK]: createBiasCheckTool(ctx.session),
  };
}

function createProfileDatasetTool(ctx: DataGuardToolContext) {
  return tool({
    description: `Scan the loaded dataset for quality issues. Read-only.

Detects four categories:
- missing_value: null / empty / configured missing tokens
- placeholder_value: numeric (999, -1) or string sentinels
- duplicate_id: requires idColumn hint
- out_of_range: requires validRanges hint per column

Call this once at the start of a DataGuard run. Returns a JSON array of DataQualityIssue records.`,
    inputSchema: z.object({
      idColumn: z
        .string()
        .optional()
        .describe("Column name to treat as the unique row identifier. If omitted, no duplicate_id detection runs."),
      validRanges: z
        .record(z.string(), z.object({ min: z.number(), max: z.number() }))
        .optional()
        .describe("Per-column valid numeric range. Values outside are flagged as out_of_range."),
      placeholderValues: z
        .array(z.union([z.string(), z.number()]))
        .optional()
        .describe("Override the default placeholder list (default: [999, -1, 'unknown', 'Unknown'])."),
      missingTokens: z
        .array(z.string())
        .optional()
        .describe("Override the default missing-token list (default: ['NA', 'N/A', 'n/a', 'null', 'NULL', 'None'])."),
    }),
    execute: async (input) => {
      const dataset = ctx.session.getDataset();
      if (!dataset) {
        return "[ERROR] No dataset loaded into DataGuard session. The frontend must call setDataset before invoking profile_dataset.";
      }
      const options: ProfileOptions = {
        idColumn: input.idColumn,
        validRanges: input.validRanges,
        placeholderValues: input.placeholderValues,
        missingTokens: input.missingTokens,
      };
      const issues = profileDataset(dataset, options);
      for (const issue of issues) ctx.session.recordIssue(issue);
      return JSON.stringify({
        datasetRowCount: dataset.rows.length,
        datasetColumnCount: dataset.columns.length,
        issueCount: issues.length,
        issues,
      });
    },
  });
}

function createSuggestFixTool(ctx: DataGuardToolContext) {
  return tool({
    description: `Propose a single concrete fix for a previously-detected issue. Read-only.

Call after profile_dataset. Pass the issueId from one of the returned issues. Returns a FixProposal that you can then pass to apply_fix.`,
    inputSchema: z.object({
      issueId: z.string().describe("The issueId of a DataQualityIssue returned by profile_dataset."),
    }),
    execute: async (input) => {
      const issue = ctx.session.getIssue(input.issueId);
      if (!issue) {
        return `[ERROR] No issue with id "${input.issueId}". Call profile_dataset first.`;
      }
      try {
        const proposal = await suggestFix(issue, { llmCall: ctx.llmCall });
        ctx.session.recordProposal(proposal);
        return JSON.stringify(proposal);
      } catch (e) {
        return `[ERROR] suggest_fix failed: ${(e as Error).message}`;
      }
    },
  });
}

function createApplyFixTool(ctx: DataGuardToolContext) {
  return tool({
    description: `Apply a previously-proposed fix to the dataset. MUTATING — gated by user approval.

Pass the issueId. The proposal stored from suggest_fix is looked up automatically. For risk tier "low" the fix is auto-applied with a summary line; for "medium" / "high" the user must approve through the chat panel. The result includes the user's verdict.`,
    inputSchema: z.object({
      issueId: z.string().describe("The issueId whose proposal should be applied."),
    }),
    execute: async (input) => {
      const proposal = ctx.session.getProposal(input.issueId);
      if (!proposal) {
        return `[ERROR] No proposal for issueId "${input.issueId}". Call suggest_fix first.`;
      }
      const dataset = ctx.session.getDataset();
      if (!dataset) {
        return `[ERROR] No dataset loaded.`;
      }

      const decision = await requestApproval(ctx.gateway, proposal);

      if (decision.verdict === "deny") {
        ctx.session.recordDecision({ proposal, verdict: "deny", applied: false });
        return JSON.stringify({
          verdict: "deny",
          rowsAffected: 0,
          message: "User denied the fix. No changes made.",
        });
      }

      // For modify, MVP keeps the original operationKind/params but records the
      // user's free-text override in the log. Future iteration can parse the
      // modifiedAction back into a structured proposal override.
      const modifiedAction = decision.verdict === "modify" ? decision.modifiedAction : undefined;

      try {
        const result = applyFix(dataset, proposal);
        ctx.session.updateDataset(result.dataset);
        if (result.flaggedRows.length > 0) ctx.session.addFlaggedRows(result.flaggedRows);
        ctx.session.recordDecision({
          proposal,
          verdict: decision.verdict,
          modifiedAction,
          applied: true,
        });
        return JSON.stringify({
          verdict: decision.verdict,
          rowsAffected: result.rowsAffected,
          flaggedRows: result.flaggedRows,
          datasetRowCount: result.dataset.rows.length,
          message: `Applied ${proposal.operationKind}. Rows affected: ${result.rowsAffected}.`,
        });
      } catch (e) {
        return `[ERROR] apply_fix failed: ${(e as Error).message}`;
      }
    },
  });
}
