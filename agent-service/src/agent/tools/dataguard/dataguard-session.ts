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

// Per-agent DataGuard run state. One DataGuardSession lives on each
// TexeraAgent (lazy-initialized when the first DataGuard tool fires) and
// holds the working dataset, accumulated issues, decision log, flagged rows,
// and auto-allow rules. Independent of the workflow state so resetting one
// does not affect the other.

import type {
  AutoAllowRule,
  DataQualityIssue,
  DecisionLogEntry,
  FixProposal,
  IssueType,
  Verdict,
} from "../../../types/dataguard";
import type { DatasetView } from "./dataset";

export interface RecordDecisionInput {
  proposal: FixProposal;
  verdict: Verdict;
  modifiedAction?: string;
  applied: boolean;
}

export class DataGuardSession {
  private dataset: DatasetView | undefined;
  private issues: Map<string, DataQualityIssue> = new Map();
  private proposals: Map<string, FixProposal> = new Map();
  private decisionLog: DecisionLogEntry[] = [];
  private flaggedRows: Set<number> = new Set();
  private autoAllowRules: Map<string, AutoAllowRule> = new Map();
  private decisionCounter = 0;
  private ruleCounter = 0;

  setDataset(dataset: DatasetView): void {
    this.dataset = dataset;
    // A new dataset means a fresh DataGuard run — clear the per-run state.
    // Auto-allow rules persist (they're a user preference, not run state).
    this.issues.clear();
    this.proposals.clear();
    this.decisionLog = [];
    this.flaggedRows.clear();
  }

  recordProposal(proposal: FixProposal): void {
    this.proposals.set(proposal.issueId, proposal);
  }

  getProposal(issueId: string): FixProposal | undefined {
    return this.proposals.get(issueId);
  }

  getIssue(issueId: string): DataQualityIssue | undefined {
    return this.issues.get(issueId);
  }

  getDataset(): DatasetView | undefined {
    return this.dataset;
  }

  updateDataset(dataset: DatasetView): void {
    this.dataset = dataset;
  }

  recordIssue(issue: DataQualityIssue): void {
    this.issues.set(issue.issueId, issue);
  }

  getIssues(): DataQualityIssue[] {
    return Array.from(this.issues.values());
  }

  recordDecision(input: RecordDecisionInput): DecisionLogEntry {
    this.decisionCounter += 1;
    const now = new Date().toISOString();
    const entry: DecisionLogEntry = {
      decisionId: `dec-${this.decisionCounter}`,
      timestamp: now,
      issueType: input.proposal.issueType,
      targetRowCount: input.proposal.targetRowCount,
      proposedAction: input.proposal.action,
      userDecision: input.verdict,
      modifiedAction: input.modifiedAction,
      reason: input.proposal.reason,
      confidence: input.proposal.confidence,
      appliedAt: input.applied ? now : undefined,
    };
    this.decisionLog.push(entry);
    return entry;
  }

  getDecisionLog(): DecisionLogEntry[] {
    return [...this.decisionLog];
  }

  addFlaggedRows(indices: number[]): void {
    for (const i of indices) this.flaggedRows.add(i);
  }

  getFlaggedRows(): number[] {
    return Array.from(this.flaggedRows).sort((a, b) => a - b);
  }

  addAutoAllowRule(issueType: IssueType): AutoAllowRule {
    // Idempotent: if a rule already exists for this issueType, return it.
    for (const r of this.autoAllowRules.values()) {
      if (r.issueType === issueType) return r;
    }
    this.ruleCounter += 1;
    const rule: AutoAllowRule = {
      ruleId: `rule-${this.ruleCounter}`,
      issueType,
      createdAt: new Date().toISOString(),
    };
    this.autoAllowRules.set(rule.ruleId, rule);
    return rule;
  }

  removeAutoAllowRule(ruleId: string): boolean {
    return this.autoAllowRules.delete(ruleId);
  }

  matchesAutoAllowRule(issueType: IssueType): boolean {
    for (const r of this.autoAllowRules.values()) {
      if (r.issueType === issueType) return true;
    }
    return false;
  }

  getAutoAllowRules(): AutoAllowRule[] {
    return Array.from(this.autoAllowRules.values());
  }
}
