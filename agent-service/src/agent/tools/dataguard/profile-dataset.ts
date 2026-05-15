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

// Read-only scanner that returns DataQualityIssue[] for a dataset.
// No LLM calls — pure heuristics. Safe to auto-run on dataset-add.

import type { DataQualityIssue } from "../../../types/dataguard";
import type { DatasetView } from "./dataset";

export interface ProfileOptions {
  // Column to treat as a unique identifier; duplicates flagged as duplicate_id.
  idColumn?: string;
  // Hard valid range per numeric column (e.g., { age: { min: 0, max: 130 } }).
  validRanges?: Record<string, { min: number; max: number }>;
  // Sentinel values that should be treated as placeholders rather than data.
  placeholderValues?: Array<string | number>;
  // String tokens that should be treated as missing in addition to null/undefined/NaN/"".
  missingTokens?: string[];
  // Above this row count, affectedRowIndices is omitted from the issue.
  maxIndicesInIssue?: number;
}

// Placeholders are sentinel *values* that look like data but are actually
// "no value." Kept distinct from missing-tokens to avoid double-flagging
// the same cell under two issue types.
const DEFAULT_PLACEHOLDERS: Array<string | number> = [
  999,
  -1,
  "unknown",
  "Unknown",
];

// Tokens that mean "no data was recorded." Empty string is always treated
// as missing without needing to be listed.
const DEFAULT_MISSING_TOKENS: string[] = [
  "NA",
  "N/A",
  "n/a",
  "null",
  "NULL",
  "None",
];

const DEFAULT_MAX_INDICES_IN_ISSUE = 50;

let issueCounter = 0;
function nextIssueId(): string {
  issueCounter += 1;
  return `iss-${Date.now()}-${issueCounter}`;
}

function nowIso(): string {
  return new Date().toISOString();
}

function isMissing(value: unknown, missingTokens: string[]): boolean {
  if (value === null || value === undefined) return true;
  if (typeof value === "number" && Number.isNaN(value)) return true;
  if (typeof value === "string") {
    if (value === "") return true;
    if (missingTokens.includes(value)) return true;
  }
  return false;
}

function toNumber(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return undefined;
}

function placeholderHit(
  value: unknown,
  placeholders: Array<string | number>
): string | number | undefined {
  for (const p of placeholders) {
    if (typeof p === "string" && typeof value === "string" && p === value) return p;
    if (typeof p === "number") {
      const n = toNumber(value);
      if (n !== undefined && n === p) return p;
    }
  }
  return undefined;
}

function maybeIndices(
  indices: number[],
  cap: number
): number[] | undefined {
  return indices.length <= cap ? indices : undefined;
}

export function profileDataset(
  dataset: DatasetView,
  options: ProfileOptions = {}
): DataQualityIssue[] {
  const placeholders = options.placeholderValues ?? DEFAULT_PLACEHOLDERS;
  const missingTokens = options.missingTokens ?? DEFAULT_MISSING_TOKENS;
  const indexCap = options.maxIndicesInIssue ?? DEFAULT_MAX_INDICES_IN_ISSUE;
  const detectedAt = nowIso();
  const issues: DataQualityIssue[] = [];

  // Pre-compute placeholder hits per row/column so out_of_range can avoid
  // double-counting and missing-value can avoid flagging a row that has a
  // string placeholder like "N/A" twice.
  const placeholderHitByColRow = new Map<string, Map<number, string | number>>();
  for (const col of dataset.columns) {
    const map = new Map<number, string | number>();
    for (let i = 0; i < dataset.rows.length; i++) {
      const hit = placeholderHit(dataset.rows[i][col], placeholders);
      if (hit !== undefined) map.set(i, hit);
    }
    placeholderHitByColRow.set(col, map);
  }

  // Missing-value detector.
  for (const col of dataset.columns) {
    const missingIndices: number[] = [];
    for (let i = 0; i < dataset.rows.length; i++) {
      if (isMissing(dataset.rows[i][col], missingTokens)) missingIndices.push(i);
    }
    if (missingIndices.length === 0) continue;
    const pct = (missingIndices.length / Math.max(dataset.rows.length, 1)) * 100;
    issues.push({
      issueId: nextIssueId(),
      issueType: "missing_value",
      column: col,
      description: `${missingIndices.length} row(s) have missing ${col}`,
      evidence: `Missing: ${missingIndices.length} of ${dataset.rows.length} (${pct.toFixed(1)}%)`,
      affectedRowCount: missingIndices.length,
      affectedRowIndices: maybeIndices(missingIndices, indexCap),
      detectedAt,
    });
  }

  // Placeholder-value detector.
  for (const col of dataset.columns) {
    const hits = placeholderHitByColRow.get(col)!;
    if (hits.size === 0) continue;
    const indices = Array.from(hits.keys()).sort((a, b) => a - b);
    const distinctValues = Array.from(new Set(hits.values()));
    issues.push({
      issueId: nextIssueId(),
      issueType: "placeholder_value",
      column: col,
      description: `${indices.length} row(s) in ${col} contain placeholder value(s): ${distinctValues.join(", ")}`,
      evidence: `Placeholder(s) ${distinctValues.join(", ")} appear ${indices.length} time(s) in ${col}.`,
      affectedRowCount: indices.length,
      affectedRowIndices: maybeIndices(indices, indexCap),
      detectedAt,
    });
  }

  // Duplicate-ID detector (only when idColumn is configured and exists).
  if (options.idColumn && dataset.columns.includes(options.idColumn)) {
    const idCol = options.idColumn;
    const positions = new Map<string, number[]>();
    for (let i = 0; i < dataset.rows.length; i++) {
      const v = dataset.rows[i][idCol];
      if (v === null || v === undefined) continue;
      const key = String(v);
      const existing = positions.get(key);
      if (existing) existing.push(i);
      else positions.set(key, [i]);
    }
    const duplicateIndices: number[] = [];
    const duplicateKeys: string[] = [];
    for (const [key, rows] of positions) {
      if (rows.length > 1) {
        duplicateIndices.push(...rows);
        duplicateKeys.push(key);
      }
    }
    if (duplicateIndices.length > 0) {
      duplicateIndices.sort((a, b) => a - b);
      issues.push({
        issueId: nextIssueId(),
        issueType: "duplicate_id",
        column: idCol,
        description: `${duplicateKeys.length} duplicate ID(s) in ${idCol} affecting ${duplicateIndices.length} row(s)`,
        evidence: `Duplicate keys (showing up to 5): ${duplicateKeys.slice(0, 5).join(", ")}`,
        affectedRowCount: duplicateIndices.length,
        affectedRowIndices: maybeIndices(duplicateIndices, indexCap),
        detectedAt,
      });
    }
  }

  // Out-of-range detector — skips rows already flagged as placeholders so we
  // don't surface the same row under two issue types.
  if (options.validRanges) {
    for (const [col, range] of Object.entries(options.validRanges)) {
      if (!dataset.columns.includes(col)) continue;
      const placeholderHits = placeholderHitByColRow.get(col)!;
      const oorIndices: number[] = [];
      for (let i = 0; i < dataset.rows.length; i++) {
        if (placeholderHits.has(i)) continue;
        const v = toNumber(dataset.rows[i][col]);
        if (v === undefined) continue;
        if (v < range.min || v > range.max) oorIndices.push(i);
      }
      if (oorIndices.length === 0) continue;
      issues.push({
        issueId: nextIssueId(),
        issueType: "out_of_range",
        column: col,
        description: `${oorIndices.length} row(s) in ${col} fall outside [${range.min}, ${range.max}]`,
        evidence: `Valid range: [${range.min}, ${range.max}]; violations: ${oorIndices.length}.`,
        affectedRowCount: oorIndices.length,
        affectedRowIndices: maybeIndices(oorIndices, indexCap),
        detectedAt,
      });
    }
  }

  return issues;
}
