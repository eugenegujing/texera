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
  // Tukey's IQR fence multiplier for auto outlier detection. Default 1.5
  // (the classical "mild outlier" threshold). Bump to 3.0 for "extreme
  // outliers only" or set very high to effectively disable auto-IQR while
  // still honoring per-column `validRanges`.
  outlierIqrMultiplier?: number;
  // Minimum number of numeric, non-missing, non-placeholder observations a
  // column needs before auto-IQR runs. Below this, quartiles aren't trustworthy
  // — skip the column silently. Default 10. validRanges still fires at any size.
  outlierMinObservations?: number;
  // Whether to run the outlier detector. Default false — disabled because
  // the IQR-based detector converges to no-op fixes after a few iterative
  // Apply rounds (capping outliers to the fence eventually produces cells
  // already at the fence, and the LLM keeps proposing replace_value with
  // the same fence value, which `apply-fix` correctly treats as a no-op
  // but the user perceives as "stuck"). validRanges still works as a
  // per-column override when the caller opts into outlier detection.
  //
  // Gating spec:
  //   !enableOutlierDetection && !validRanges → skip outlier entirely
  //   !enableOutlierDetection &&  validRanges → run validRange-only (no auto IQR)
  //    enableOutlierDetection &&  validRanges → validRange + auto IQR for cols w/o ranges
  //    enableOutlierDetection && !validRanges → auto IQR for every numeric column
  // Detector code (IQR + range-violation paths) is kept intact so the option
  // is a flip-of-a-switch — useful when we re-enable after fixing convergence.
  enableOutlierDetection?: boolean;
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

function maybeIndices(indices: number[], cap: number): number[] | undefined {
  return indices.length <= cap ? indices : undefined;
}

/**
 * Deterministic, content-based fingerprint for a dataset row.
 *
 * Used to align profiler issues (which see file-byte-order rows) with the
 * Texera result panel (which may show rows in a worker-shuffled order — the
 * JSONL multi-worker scan is the motivating case). The frontend recomputes the
 * same fingerprint over the rows it has loaded and matches by string equality.
 *
 * Contract (must be byte-identical to `findRowByKey` in
 * `data-guard-row-navigator.service.ts`):
 *   - Columns are sorted alphabetically (`Array#sort()` default locale-agnostic
 *     compare on the UTF-16 code-unit order) to canonicalize the schema. This
 *     guarantees the same key regardless of column-display reordering on
 *     either side.
 *   - For each column in canonical order: read the cell value; `undefined`
 *     (incl. completely-missing keys) is coerced to `null` so it produces the
 *     same string as an explicit null cell.
 *   - Each non-null cell is normalised to its string form via `String(v)`
 *     before `JSON.stringify`, so number `45` and string `"45"` produce the
 *     same token. This matters because Texera's JSONL scan widens mixed-type
 *     columns to String (`JSONToMap` + `parseField(stringValue, schemaType)`),
 *     while DataGuard's own `parseJsonl` keeps native JSON types — without
 *     the coercion the two sides fingerprint differently and `findRowByKey`
 *     misses every row in mixed-type columns.
 *   - **Round 6 — missing-token canonicalization.** Any cell `isMissing()`
 *     considers absent (`null`, `undefined`, `NaN`, `""`, and the
 *     case-insensitive trimmed tokens `na`/`n/a`/`null`/`none`/`nan`) emits
 *     the same bare `null` token. This closes a JSONL locate bug: Texera's
 *     `JSONToMap` calls Jackson's `JsonNode#asText()` on a `NullNode`, which
 *     returns the literal STRING `"null"` (not Java null). Without the
 *     canonicalization the profiler-side null cell fingerprints as bare
 *     `null` while the Texera-side cell fingerprints as `"\"null\""` and the
 *     locate match silently misses, falling back to the byte-order index path
 *     that lands on whatever shuffled display row happens to sit at that
 *     position.
 *   - The individual JSON tokens are concatenated with an empty separator;
 *     because each token is self-delimited (`"…"` or the literal `null`),
 *     no ambiguity is introduced.
 *
 * Edge cases handled:
 *   - Missing key vs explicit null vs the string `"null"` (Jackson asText on
 *     a JsonNullNode) → identical fingerprint (`null`).
 *   - JSON-stringify special characters (quotes, backslashes, unicode) → the
 *     standard JSON.stringify escapes apply identically on V8 (Texera frontend)
 *     and on Bun (agent-service).
 *   - Floats round-trip through `String()` identically on V8 and Bun (both
 *     implement IEEE-754 ToString per ECMA-262 §7.1.17).
 */
function fingerprintCell(v: unknown): string {
  if (isCellMissing(v)) return "null";
  return JSON.stringify(String(v));
}

export function rowFingerprint(row: Record<string, unknown>, columns: ReadonlyArray<string>): string {
  const canonical = [...columns].sort();
  let out = "";
  for (const c of canonical) {
    out += fingerprintCell(row[c]);
  }
  return out;
}

function maybeKeys(
  indices: number[] | undefined,
  rows: ReadonlyArray<Record<string, unknown>>,
  columns: ReadonlyArray<string>
): string[] | undefined {
  // Mirror the contract: only emit keys when indices are present. Large-issue
  // path (indices === undefined) stays key-less so we don't waste payload
  // bytes when the frontend can't display them all anyway.
  if (indices === undefined) return undefined;
  return indices.map(i => rowFingerprint(rows[i], columns));
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
    // JSONL flatten produces dot-notation column names like `user.id` or
    // `customer.uid`. The underscore-based matchers above don't catch these
    // (the dot is not an underscore, and `Id$` is case-sensitive so a
    // lowercase `id` at the trailing segment misses too). Add dot-anchored
    // patterns so the auto-trigger's dup-ID detection still works on
    // JSONL-loaded data.
    name => /\.id$/i.test(name),
    name => /\.uid$/i.test(name),
  ];
  for (const m of matchers) {
    const hit = columns.find(c => m(c));
    if (hit) return hit;
  }
  return undefined;
}

export function profileDataset(dataset: DatasetView, options: ProfileOptions = {}): DataQualityIssue[] {
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
    const idx = maybeIndices(missingIndices, indexCap);
    issues.push({
      issueId: nextIssueId(),
      issueType: "missing_value",
      column: col,
      description: `${missingIndices.length} row(s) have missing ${col}`,
      evidence: `Missing: ${missingIndices.length} of ${dataset.rows.length} (${pct.toFixed(1)}%)`,
      affectedRowCount: missingIndices.length,
      affectedRowIndices: idx,
      affectedRowKeys: maybeKeys(idx, dataset.rows, dataset.columns),
      detectedAt,
    });
  }

  // Placeholder-value detector.
  for (const col of dataset.columns) {
    const hits = placeholderHitByColRow.get(col)!;
    if (hits.size === 0) continue;
    const indices = Array.from(hits.keys()).sort((a, b) => a - b);
    const distinctValues = Array.from(new Set(hits.values()));
    const idx = maybeIndices(indices, indexCap);
    issues.push({
      issueId: nextIssueId(),
      issueType: "placeholder_value",
      column: col,
      description: `${indices.length} row(s) in ${col} contain placeholder value(s): ${distinctValues.join(", ")}`,
      evidence: `Placeholder(s) ${distinctValues.join(", ")} appear ${indices.length} time(s) in ${col}.`,
      affectedRowCount: indices.length,
      affectedRowIndices: idx,
      affectedRowKeys: maybeKeys(idx, dataset.rows, dataset.columns),
      detectedAt,
    });
  }

  // Duplicate-ID detector. Honors options.idColumn when set; otherwise tries
  // to infer one from column names (e.g. "sample_id" → use it). Without this
  // inference the auto-trigger's empty-body /scan would never find dup IDs in
  // user datasets — users don't configure scan options through the checklist UI.
  const idCol =
    options.idColumn && dataset.columns.includes(options.idColumn) ? options.idColumn : inferIdColumn(dataset.columns);
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
      const idx = maybeIndices(duplicateIndices, indexCap);
      issues.push({
        issueId: nextIssueId(),
        issueType: "duplicate_id",
        column: idCol,
        description: `${duplicateKeys.length} duplicate ID(s) in ${idCol} affecting ${duplicateIndices.length} row(s)`,
        evidence: `Duplicate keys (showing up to 5): ${duplicateKeys.slice(0, 5).join(", ")}`,
        affectedRowCount: duplicateIndices.length,
        affectedRowIndices: idx,
        affectedRowKeys: maybeKeys(idx, dataset.rows, dataset.columns),
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
      const idx = maybeIndices(inconsistentRows, indexCap);
      issues.push({
        issueId: nextIssueId(),
        issueType: "inconsistent_label",
        column: col,
        description: `${inconsistentRows.length} row(s) in ${col} use non-canonical label spellings`,
        evidence: `Mixed spellings (showing up to 3): ${examples.slice(0, 3).join(", ")}`,
        affectedRowCount: inconsistentRows.length,
        affectedRowIndices: idx,
        affectedRowKeys: maybeKeys(idx, dataset.rows, dataset.columns),
        detectedAt,
      });
    }
  }

  // Outlier detector — two-mode:
  //
  // 1. Caller supplied `validRanges` for a column → use that hard range
  //    (authoritative; user-known domain limits).
  // 2. Otherwise → auto IQR (Tukey's 1.5× fence) on numeric columns. IQR
  //    uses Q1/Q3 which are robust to a few extreme values AND to small
  //    clusters of large readings (Q3 absorbs them), so unlike the earlier
  //    z-score variant it doesn't over-flag legitimate biological clusters.
  //
  // Skip in either mode:
  //   • rows already flagged as placeholder (avoid double-counting)
  //   • rows already flagged as missing
  //   • mixed-type columns (require ≥ 80% numeric)
  //   • too-small samples (need ≥ outlierMinObservations data points)
  //   • degenerate distributions (IQR === 0, all values clustered)
  const validRanges = options.validRanges ?? {};
  const iqrMultiplier = options.outlierIqrMultiplier ?? 1.5;
  const outlierMinObs = options.outlierMinObservations ?? 10;
  const enableOutlier = options.enableOutlierDetection ?? false;
  const hasValidRanges = Object.keys(validRanges).length > 0;
  // Top-level gate: skip the entire outlier loop only when both auto-IQR is
  // off AND the caller didn't supply any validRanges. Per spec, an explicit
  // validRanges override should still fire even when enableOutlierDetection
  // is false — otherwise users who carefully configured hard ranges would be
  // silently surprised.
  if (enableOutlier || hasValidRanges) {
    for (const col of dataset.columns) {
      const placeholderHits = placeholderHitByColRow.get(col)!;

      // Collect numeric, non-placeholder, non-missing values with their row index.
      const values: Array<{ i: number; v: number }> = [];
      let nonMissingCount = 0;
      for (let i = 0; i < dataset.rows.length; i++) {
        if (placeholderHits.has(i)) continue;
        const raw = dataset.rows[i][col];
        if (isMissing(raw, missingTokens)) continue;
        nonMissingCount++;
        const v = toNumber(raw);
        if (v === undefined) continue;
        values.push({ i, v });
      }

      let outlierIndices: number[] = [];
      let mode: "validRange" | "iqr" | null = null;
      let evidenceParts = "";

      if (validRanges[col]) {
        // Mode 1: user-supplied hard range wins per-column. Runs regardless
        // of enableOutlierDetection so an explicit override is always honored.
        mode = "validRange";
        const range = validRanges[col];
        outlierIndices = values.filter(p => p.v < range.min || p.v > range.max).map(p => p.i);
        evidenceParts = `Valid range: [${range.min}, ${range.max}]; violations: ${outlierIndices.length}.`;
      } else if (enableOutlier) {
        // Mode 2: auto IQR — only when the caller opted in. Guard against
        // false-positives:
        //   - too few observations → can't trust quartiles
        //   - mostly-non-numeric column → skip (can't compare apples to oranges)
        //   - all values clustered (IQR === 0) → no outliers possible
        if (values.length < outlierMinObs) continue;
        if (values.length / Math.max(nonMissingCount, 1) < 0.8) continue;

        const sorted = [...values].sort((a, b) => a.v - b.v);
        const q1 = quantile(
          sorted.map(p => p.v),
          0.25
        );
        const q3 = quantile(
          sorted.map(p => p.v),
          0.75
        );
        const iqr = q3 - q1;
        if (iqr === 0) continue;

        const lowerFence = q1 - iqrMultiplier * iqr;
        const upperFence = q3 + iqrMultiplier * iqr;
        outlierIndices = values.filter(p => p.v < lowerFence || p.v > upperFence).map(p => p.i);
        if (outlierIndices.length === 0) continue;
        mode = "iqr";
        evidenceParts =
          `Q1=${q1.toFixed(2)}, Q3=${q3.toFixed(2)}, IQR=${iqr.toFixed(2)}, ` +
          `fence=[${lowerFence.toFixed(2)}, ${upperFence.toFixed(2)}] ` +
          `(Tukey's ${iqrMultiplier}× rule); ${outlierIndices.length} value(s) outside fence.`;
      } else {
        // Auto-IQR disabled and no per-column validRange → silently skip.
        continue;
      }

      if (mode === null || outlierIndices.length === 0) continue;

      outlierIndices.sort((a, b) => a - b);
      const idx = maybeIndices(outlierIndices, indexCap);
      const desc =
        mode === "validRange"
          ? `${outlierIndices.length} row(s) in ${col} fall outside the valid range [${validRanges[col].min}, ${validRanges[col].max}]`
          : `${outlierIndices.length} row(s) in ${col} are statistical outliers (Tukey's ${iqrMultiplier}× IQR fence)`;
      issues.push({
        issueId: nextIssueId(),
        issueType: "outlier",
        column: col,
        description: desc,
        evidence: evidenceParts,
        affectedRowCount: outlierIndices.length,
        affectedRowIndices: idx,
        affectedRowKeys: maybeKeys(idx, dataset.rows, dataset.columns),
        detectedAt,
      });
    }
  }

  return issues;
}

/**
 * Linear-interpolation quantile (R-7 / numpy default), Q1 = quantile(0.25),
 * Q3 = quantile(0.75). Input must be sorted ascending. `q` in [0, 1].
 * Pure helper — exported for testability.
 */
export function quantile(sortedValues: ReadonlyArray<number>, q: number): number {
  if (sortedValues.length === 0) return NaN;
  if (sortedValues.length === 1) return sortedValues[0];
  const pos = (sortedValues.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  if (base + 1 < sortedValues.length) {
    return sortedValues[base] + rest * (sortedValues[base + 1] - sortedValues[base]);
  }
  return sortedValues[base];
}
