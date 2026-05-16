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
import {
  DEFAULT_MISSING_TOKENS,
  DEFAULT_PLACEHOLDERS,
  isMissing as isCellMissing,
  placeholderHit,
  toNumber,
} from "./missing-detection";

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
  // Max distinct values a string column may have before inconsistent-label
  // detection skips it (free-text columns will hit this and be ignored).
  // Default 20. Set to 0 to disable label detection entirely.
  inconsistentLabelMaxCardinality?: number;
}

const DEFAULT_MAX_INDICES_IN_ISSUE = 50;

let issueCounter = 0;
function nextIssueId(): string {
  issueCounter += 1;
  return `iss-${Date.now()}-${issueCounter}`;
}

function nowIso(): string {
  return new Date().toISOString();
}

function isMissing(value: unknown, missingTokens: ReadonlyArray<string>): boolean {
  return isCellMissing(value, missingTokens);
}

function maybeIndices(
  indices: number[],
  cap: number
): number[] | undefined {
  return indices.length <= cap ? indices : undefined;
}

// Guess which column is the row identifier when the caller didn't specify one.
// Conservative: only matches columns whose names look unambiguously like IDs.
// Tries the cheapest, most-specific patterns first so e.g. `sample_id` wins
// over a generic `id_card` column elsewhere in the schema. Returns undefined
// when nothing matches — the caller skips dup-ID detection in that case.
function inferIdColumn(columns: ReadonlyArray<string>): string | undefined {
  const matchers: Array<(name: string) => boolean> = [
    name => /^id$/i.test(name),
    name => /_id$/i.test(name),
    name => /^id_/i.test(name),
    name => /Id$/.test(name),
    name => /^.+_uid$/i.test(name) || /^uid$/i.test(name),
  ];
  for (const m of matchers) {
    const hit = columns.find(c => m(c));
    if (hit) return hit;
  }
  return undefined;
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

  // Pre-compute placeholder hits per row/column so outlier-detection can
  // skip rows already flagged elsewhere and missing-value can avoid flagging
  // a row that has a string placeholder like "N/A" twice.
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

  // Duplicate-ID detector. Honors options.idColumn when set; otherwise tries
  // to infer one from column names (e.g. "sample_id" → use it). Without this
  // inference the auto-trigger's empty-body /scan would never find dup IDs in
  // user datasets — users don't configure scan options through the checklist UI.
  const idCol = options.idColumn && dataset.columns.includes(options.idColumn)
    ? options.idColumn
    : inferIdColumn(dataset.columns);
  if (idCol) {
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

  // Inconsistent-label detector. For each low-cardinality string column,
  // group raw values by a normalized key (trim + lowercase). If two or more
  // raw spellings collapse to the same key, every row using a non-canonical
  // spelling is flagged. Example: "Male"/"male"/"M" all map to "m"/"male"
  // depending on key choice. We use trim+lowercase so "Yes"/"yes"/" yes "
  // collide as expected.
  const labelMaxCardinality = options.inconsistentLabelMaxCardinality ?? 20;
  if (labelMaxCardinality > 0) {
    for (const col of dataset.columns) {
      const placeholderHits = placeholderHitByColRow.get(col)!;
      // Count distinct non-missing, non-placeholder string values.
      const raw = new Map<string, number[]>();
      let nonStringSeen = false;
      for (let i = 0; i < dataset.rows.length; i++) {
        if (placeholderHits.has(i)) continue;
        const v = dataset.rows[i][col];
        if (isMissing(v, missingTokens)) continue;
        if (typeof v !== "string") {
          nonStringSeen = true;
          continue;
        }
        const list = raw.get(v);
        if (list) list.push(i);
        else raw.set(v, [i]);
      }
      if (nonStringSeen || raw.size === 0 || raw.size > labelMaxCardinality) continue;

      // Group by normalized key. A key with >1 raw spelling = inconsistent.
      const groups = new Map<string, { canonical: string; spellings: Set<string>; rows: number[] }>();
      for (const [rawValue, rows] of raw) {
        const key = rawValue.trim().toLowerCase();
        const existing = groups.get(key);
        if (existing) {
          existing.spellings.add(rawValue);
          existing.rows.push(...rows);
          // Prefer the most common spelling as canonical (heuristic).
          if (rows.length > (raw.get(existing.canonical)?.length ?? 0)) {
            existing.canonical = rawValue;
          }
        } else {
          groups.set(key, { canonical: rawValue, spellings: new Set([rawValue]), rows: [...rows] });
        }
      }
      const inconsistentRows: number[] = [];
      const examples: string[] = [];
      for (const g of groups.values()) {
        if (g.spellings.size > 1) {
          // Flag every row that uses a non-canonical spelling.
          for (const [rawValue, rows] of raw) {
            if (rawValue !== g.canonical && g.spellings.has(rawValue)) {
              inconsistentRows.push(...rows);
            }
          }
          examples.push(`{${Array.from(g.spellings).join(" / ")}}`);
        }
      }
      if (inconsistentRows.length === 0) continue;
      inconsistentRows.sort((a, b) => a - b);
      issues.push({
        issueId: nextIssueId(),
        issueType: "inconsistent_label",
        column: col,
        description: `${inconsistentRows.length} row(s) in ${col} use non-canonical label spellings`,
        evidence: `Mixed spellings (showing up to 3): ${examples.slice(0, 3).join(", ")}`,
        affectedRowCount: inconsistentRows.length,
        affectedRowIndices: maybeIndices(inconsistentRows, indexCap),
        detectedAt,
      });
    }
  }

  // Outlier detector — values that fall outside a user-supplied hard range.
  // We deliberately do NOT auto-detect outliers via z-score: legitimate
  // consecutive large readings (e.g. clusters of high glucose in a clinical
  // dataset) would be flagged en masse. Requires the caller to opt in by
  // providing validRanges per column. Skips rows already flagged as
  // placeholders so the same row doesn't surface under two issue types.
  if (options.validRanges) {
    for (const [col, range] of Object.entries(options.validRanges)) {
      if (!dataset.columns.includes(col)) continue;
      const placeholderHits = placeholderHitByColRow.get(col)!;
      const outlierIndices: number[] = [];
      for (let i = 0; i < dataset.rows.length; i++) {
        if (placeholderHits.has(i)) continue;
        const v = toNumber(dataset.rows[i][col]);
        if (v === undefined) continue;
        if (v < range.min || v > range.max) outlierIndices.push(i);
      }
      if (outlierIndices.length === 0) continue;
      issues.push({
        issueId: nextIssueId(),
        issueType: "outlier",
        column: col,
        description: `${outlierIndices.length} row(s) in ${col} fall outside the valid range [${range.min}, ${range.max}]`,
        evidence: `Valid range: [${range.min}, ${range.max}]; violations: ${outlierIndices.length}.`,
        affectedRowCount: outlierIndices.length,
        affectedRowIndices: maybeIndices(outlierIndices, indexCap),
        detectedAt,
      });
    }
  }

  return issues;
}
