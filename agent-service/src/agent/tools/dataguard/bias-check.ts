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

// Compares per-group row counts in the before / after dataset and flags skew
// — the closing demo beat: "Group A retention: 96%. Group B retention: 95%.
// ✓ No skew introduced."

import { z } from "zod";
import { tool } from "ai";
import type { DatasetView } from "./dataset";
import type { DataGuardSession } from "./dataguard-session";

export interface BiasCheckResult {
  groupColumn: string;
  perGroup: Record<string, { before: number; after: number; retentionPct: number }>;
  maxRetentionGapPct: number;
  skewThresholdPct: number;
  skewDetected: boolean;
}

export interface BiasCheckOptions {
  skewThresholdPct?: number;
}

const DEFAULT_SKEW_THRESHOLD = 10;

export function computeBiasCheck(
  before: DatasetView,
  after: DatasetView,
  groupColumn: string,
  options: BiasCheckOptions = {}
): BiasCheckResult {
  const threshold = options.skewThresholdPct ?? DEFAULT_SKEW_THRESHOLD;
  const perGroup: BiasCheckResult["perGroup"] = {};

  if (!before.columns.includes(groupColumn) || before.rows.length === 0) {
    return {
      groupColumn,
      perGroup,
      maxRetentionGapPct: 0,
      skewThresholdPct: threshold,
      skewDetected: false,
    };
  }

  const beforeCounts = countByGroup(before, groupColumn);
  const afterCounts = countByGroup(after, groupColumn);

  for (const [group, beforeN] of beforeCounts) {
    const afterN = afterCounts.get(group) ?? 0;
    perGroup[group] = {
      before: beforeN,
      after: afterN,
      retentionPct: beforeN > 0 ? (afterN / beforeN) * 100 : 0,
    };
  }

  const retentions = Object.values(perGroup).map(g => g.retentionPct);
  const maxGap = retentions.length > 0 ? Math.max(...retentions) - Math.min(...retentions) : 0;

  return {
    groupColumn,
    perGroup,
    maxRetentionGapPct: maxGap,
    skewThresholdPct: threshold,
    skewDetected: maxGap > threshold,
  };
}

function countByGroup(ds: DatasetView, col: string): Map<string, number> {
  const counts = new Map<string, number>();
  for (const r of ds.rows) {
    const v = r[col];
    if (v === undefined || v === null) continue;
    const key = String(v);
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  return counts;
}

// ----- AI SDK tool -----

export const TOOL_NAME_BIAS_CHECK = "bias_check";

export function createBiasCheckTool(session: DataGuardSession) {
  return tool({
    description: `Compare row counts per group in the current dataset vs the pre-cleanup snapshot. Returns retention% per group plus a skew flag. Read-only.`,
    inputSchema: z.object({
      groupColumn: z.string().describe("Column whose distinct values define the groups (e.g., 'group', 'cohort')."),
      skewThresholdPct: z
        .number()
        .optional()
        .describe("If max(retention%) - min(retention%) exceeds this, skewDetected = true. Default 10."),
      beforeDataset: z
        .object({
          columns: z.array(z.string()),
          rows: z.array(z.record(z.string(), z.unknown())),
        })
        .optional()
        .describe("Optional explicit 'before' dataset; if omitted, the tool cannot compute bias and returns an error."),
    }),
    execute: async input => {
      const after = session.getDataset();
      if (!after) return "[ERROR] No dataset in session; load one before calling bias_check.";
      const before = input.beforeDataset ?? null;
      if (!before) {
        return "[ERROR] bias_check requires a beforeDataset (the pre-cleanup snapshot). Pass it explicitly.";
      }
      const result = computeBiasCheck(before, after, input.groupColumn, {
        skewThresholdPct: input.skewThresholdPct,
      });
      return JSON.stringify(result);
    },
  });
}
