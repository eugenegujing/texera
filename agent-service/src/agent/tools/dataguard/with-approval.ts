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

// The permission gate: every mutating DataGuard tool calls requestApproval()
// before doing anything. The function returns a PermissionDecision that the
// tool inspects to know whether to apply, skip, or transform its input.

import type { FixProposal, IssueType, PermissionDecision } from "../../../types/dataguard";

// The set of operations the gate needs from its host. Implemented by
// TexeraAgent in production and by a mock in tests, so the gating logic
// itself can be unit-tested without a full agent or a websocket.
export interface ApprovalGateway {
  // Does this issueType have a standing "auto-allow" rule?
  matchesAutoAllowRule(issueType: IssueType): boolean;
  // Mint a fresh step id used to correlate the emitted pending step with the
  // user's eventual decision.
  generateStepId(): string;
  // Add a "pending approval" step into the conversation history and broadcast
  // it to subscribed websocket clients. The frontend renders the prompt UI.
  emitPendingApproval(stepId: string, proposal: FixProposal): void;
  // Resolve when the decision for this step arrives via a websocket message.
  awaitDecision(stepId: string): Promise<PermissionDecision>;
}

export async function requestApproval(gateway: ApprovalGateway, proposal: FixProposal): Promise<PermissionDecision> {
  // `high` and `warning` ALWAYS prompt — the "remember" rule does not apply.
  // This is the same shape Claude Code uses for destructive Bash operations.
  //
  // `warning` exists specifically because the agent is *not* confident enough
  // to act without a human eyeball (e.g., outliers that might be real extreme
  // values). Letting an "Allow & remember placeholder_value" rule from earlier
  // in the session auto-approve a warning-tier fix would defeat the whole
  // point of the tier.
  const alwaysPrompt = proposal.riskTier === "high" || proposal.riskTier === "warning";
  if (!alwaysPrompt && gateway.matchesAutoAllowRule(proposal.issueType)) {
    return { stepId: "", verdict: "auto_allow_remembered" };
  }
  if (proposal.riskTier === "low") {
    return { stepId: "", verdict: "auto_allow_low_risk" };
  }
  const stepId = gateway.generateStepId();
  gateway.emitPendingApproval(stepId, proposal);
  return gateway.awaitDecision(stepId);
}
