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

export type RiskTier = "low" | "medium" | "high";

export type Confidence = "low" | "medium" | "high";

export type IssueType =
  | "placeholder_value"
  | "missing_value"
  | "duplicate_id"
  | "out_of_range"
  | "outlier"
  | "inconsistent_label";

export type FixOperationKind =
  | "replace_value"
  | "drop_rows"
  | "impute"
  | "flag"
  | "standardize"
  | "trim_whitespace"
  | "rename_column";

export type Verdict =
  | "allow"
  | "deny"
  | "modify"
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
  modifiedAction?: string;
  remember?: boolean;
}

export interface DecisionLogEntry {
  decisionId: string;
  timestamp: string;
  issueType: IssueType;
  targetRowCount: number;
  proposedAction: string;
  userDecision: Verdict;
  modifiedAction?: string;
  reason: string;
  confidence: Confidence;
  appliedAt?: string;
}

export interface AutoAllowRule {
  ruleId: string;
  issueType: IssueType;
  createdAt: string;
}
