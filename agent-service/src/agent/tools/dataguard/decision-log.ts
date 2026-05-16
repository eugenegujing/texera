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

// RFC-4180 CSV serializer for the DataGuard decision log. Schema matches
// §4.4 of README_DataGuard_Texera.md exactly so a reviewer can open the
// downloaded CSV and trace every applied/denied/modified fix.

import { z } from "zod";
import { tool } from "ai";
import type { DecisionLogEntry } from "../../../types/dataguard";
import type { DataGuardSession } from "./dataguard-session";

const HEADER_COLUMNS = [
  "decision_id",
  "timestamp",
  "issue_type",
  "target_rows",
  "proposed_action",
  "user_decision",
  "reason",
  "confidence",
  "applied_at",
] as const;

export const TOOL_NAME_WRITE_DECISION_LOG = "write_decision_log";

export function serializeDecisionLogCsv(entries: DecisionLogEntry[]): string {
  const header = HEADER_COLUMNS.join(",");
  const rows = entries.map(rowToCsv);
  return [header, ...rows].join("\n");
}

function rowToCsv(e: DecisionLogEntry): string {
  return [
    csvField(e.decisionId),
    csvField(e.timestamp),
    csvField(e.issueType),
    csvField(String(e.targetRowCount)),
    csvField(e.proposedAction),
    csvField(e.userDecision),
    csvField(e.reason),
    csvField(e.confidence),
    csvField(e.appliedAt ?? ""),
  ].join(",");
}

// RFC 4180: a field MUST be quoted if it contains a comma, double-quote, or
// line break. Quotes within a quoted field are escaped by doubling.
function csvField(value: string): string {
  if (value === "") return "";
  const needsQuoting = /[",\r\n]/.test(value);
  if (!needsQuoting) return value;
  return `"${value.replace(/"/g, '""')}"`;
}

// ----- AI SDK tool (exposed to the LLM) -----

export function createWriteDecisionLogTool(session: DataGuardSession) {
  return tool({
    description: `Export the DataGuard decision log to CSV. Returns the CSV text. Call this at the end of a DataGuard run to give the user an audit trail of every Allow / Deny / Modify they made.`,
    inputSchema: z.object({}),
    execute: async () => {
      const csv = serializeDecisionLogCsv(session.getDecisionLog());
      return JSON.stringify({
        rows: session.getDecisionLog().length,
        bytes: csv.length,
        csv,
      });
    },
  });
}
