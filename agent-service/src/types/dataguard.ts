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

// Shared types for the DataGuard agent: the contract between the four
// DataGuard tools (profile_dataset / suggest_fix / apply_fix / write_decision_log),
// the agent's permission-gating layer, and the chat-panel approval UI.

// `warning` marks a fix the agent thinks is risky enough to recommend manual
// review — UI defaults the checkbox to unchecked and renders an orange badge.
// The fix itself is still concrete and applicable (no more no-op "flag" ops).
export type RiskTier = "low" | "medium" | "high" | "warning";

export type Confidence = "low" | "medium" | "high";

// `outlier` is the validRanges-based detector. Earlier there was a separate
// z-score "outlier" detector that flagged anything beyond ±3σ — too aggressive
// (it removed legitimately large but consecutive readings), so it was dropped.
// The remaining detector requires the user to supply a hard min/max per column.
export type IssueType =
  | "placeholder_value"
  | "missing_value"
  | "duplicate_id"
  | "outlier"
  | "inconsistent_label";

export type FixOperationKind =
  | "replace_value"
  | "drop_rows"
  | "impute"
  | "standardize"
  | "trim_whitespace"
  | "rename_column";

// "modify" was removed for MVP (#11a) to avoid silent fallback — the legacy
// handler recorded a user-supplied action override in the log but always
// executed the original proposal.operationParams. Revisit post-hackathon
// with a real natural-language → operationParams parser.
export type Verdict =
  | "allow"
  | "deny"
  | "auto_allow_low_risk"
  | "auto_allow_remembered";

export interface DataQualityIssue {
  issueId: string;
  issueType: IssueType;
  column: string;
  description: string;
  evidence: string;
  affectedRowCount: number;
  // Present only when the affected set is small enough to enumerate; otherwise
  // omit and rely on `evidence` for a sample / aggregate description.
  affectedRowIndices?: number[];
  detectedAt: string;
}

export interface FixProposal {
  issueId: string;
  issueType: IssueType;
  action: string;
  operationKind: FixOperationKind;
  operationParams: Record<string, unknown>;
  riskTier: RiskTier;
  reason: string;
  evidence: string;
  confidence: Confidence;
  targetRowCount: number;
}

export interface PermissionDecision {
  stepId: string;
  verdict: Verdict;
  remember?: boolean;
}

export interface DecisionLogEntry {
  decisionId: string;
  timestamp: string;
  issueType: IssueType;
  targetRowCount: number;
  proposedAction: string;
  userDecision: Verdict;
  reason: string;
  confidence: Confidence;
  appliedAt?: string;
}

export interface AutoAllowRule {
  ruleId: string;
  issueType: IssueType;
  createdAt: string;
}
