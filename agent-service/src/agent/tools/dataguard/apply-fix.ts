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

// Applies one approved FixProposal to an in-memory DatasetView.
// Pure function: returns a new dataset; never mutates the input.
// This is the mutating boundary of DataGuard — every call here must have been
// authorized through the permission scaffolding (see with-approval.ts).

import type { FixProposal } from "../../../types/dataguard";
import type { DatasetView } from "./dataset";
import { isMissing as isCellMissing } from "./missing-detection";

export interface ApplyResult {
  dataset: DatasetView;
  rowsAffected: number;
}

export interface ApplyOptions {
  // Extra tokens — beyond the DEFAULT_MISSING_TOKENS — that should also be
  // treated as missing. Threaded through from the user's /scan call so that
  // a session configured with custom `missingTokens` (e.g., ["xyz"]) sees
  // those same tokens treated as missing during `impute`. Without this,
  // apply-fix and the profiler would silently disagree on what's missing.
  missingTokens?: string[];
}

export function applyFix(
  dataset: DatasetView,
  proposal: FixProposal,
  options: ApplyOptions = {}
): ApplyResult {
  const rows = dataset.rows.map(r => ({ ...r }));
  let columns = [...dataset.columns];
  const params = proposal.operationParams;

  switch (proposal.operationKind) {
    case "replace_value": {
      const column = params.column as string;
      const replacement = params.replacement;
      // Two targeting modes:
      //   - rowIndices: deterministic, used for outlier / placeholder
      //     where the profiler already knows exactly which rows are wrong.
      //   - match: value-based, used for cases like "replace every 'unknown' with null".
      // rowIndices wins when both are present. Without rowIndices support,
      // replace_value silently no-ops whenever the LLM-supplied `match` is
      // slightly off (e.g. 950 vs 950.0 vs "950") — that turned every outlier
      // proposal into a byte-identical re-export, which then made LakeFS abort
      // the version commit with "No changes detected in dataset".
      //
      // Critically, we only count a row as "affected" when the replacement
      // actually changes the cell. This matters for iterative cleanup: after
      // v1→v2→v3 of capping outliers to the IQR fence, the proposal "replace
      // these 3 rows with fence value X" hits cells that are *already* X.
      // Without the equality guard, `applied` would be > 0, the frontend would
      // try to write back a byte-identical CSV, and LakeFS would reject the
      // commit with "No changes detected." With the guard, applied === 0,
      // the frontend skips the upload, and the user sees "Nothing to apply."
      const targetIndices = params.rowIndices as number[] | undefined;
      let affected = 0;
      if (targetIndices && targetIndices.length > 0) {
        const indexSet = new Set(targetIndices);
        for (let i = 0; i < rows.length; i++) {
          if (indexSet.has(i) && !cellEquals(rows[i][column], replacement)) {
            rows[i][column] = replacement;
            affected++;
          }
        }
      } else {
        const match = params.match;
        for (const r of rows) {
          if (cellEquals(r[column], match) && !cellEquals(r[column], replacement)) {
            r[column] = replacement;
            affected++;
          }
        }
      }
      return { dataset: { columns, rows }, rowsAffected: affected };
    }

    case "drop_rows": {
      const drop = new Set(params.rowIndices as number[]);
      const kept = rows.filter((_, i) => !drop.has(i));
      return {
        dataset: { columns, rows: kept },
        rowsAffected: rows.length - kept.length,
      };
    }

    case "impute": {
      const column = params.column as string;
      const strategy = params.strategy as "mean" | "median" | "mode";
      const fill = computeImputeValue(rows, column, strategy, options.missingTokens);
      let affected = 0;
      for (const r of rows) {
        if (isCellMissing(r[column], options.missingTokens)) {
          r[column] = fill;
          affected++;
        }
      }
      return { dataset: { columns, rows }, rowsAffected: affected };
    }

    case "trim_whitespace": {
      const column = params.column as string;
      let affected = 0;
      for (const r of rows) {
        const v = r[column];
        if (typeof v === "string") {
          const trimmed = v.trim();
          if (trimmed !== v) {
            r[column] = trimmed;
            affected++;
          }
        }
      }
      return { dataset: { columns, rows }, rowsAffected: affected };
    }

    case "standardize": {
      const column = params.column as string;
      const mapping = params.mapping as Record<string, string>;
      let affected = 0;
      for (const r of rows) {
        const v = r[column];
        if (typeof v === "string" && Object.prototype.hasOwnProperty.call(mapping, v)) {
          r[column] = mapping[v];
          affected++;
        }
      }
      return { dataset: { columns, rows }, rowsAffected: affected };
    }

    case "rename_column": {
      const from = params.from as string;
      const to = params.to as string;
      columns = columns.map(c => (c === from ? to : c));
      let affected = 0;
      for (const r of rows) {
        if (Object.prototype.hasOwnProperty.call(r, from)) {
          r[to] = r[from];
          delete r[from];
          affected++;
        }
      }
      return { dataset: { columns, rows }, rowsAffected: affected };
    }

    default:
      throw new Error(
        `apply_fix: unknown operationKind: ${(proposal as unknown as { operationKind: string }).operationKind}`
      );
  }
}

function cellEquals(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a === "number" && typeof b === "number" && Number.isNaN(a) && Number.isNaN(b)) {
    return true;
  }
  return false;
}

function computeImputeValue(
  rows: Record<string, unknown>[],
  column: string,
  strategy: "mean" | "median" | "mode",
  missingTokens?: string[]
): unknown {
  const numericValues: number[] = [];
  const stringCounts = new Map<string, number>();
  for (const r of rows) {
    const v = r[column];
    // Honor the session's missingTokens override so impute treats the exact
    // same set of cells as missing that the profiler flagged. Without this,
    // the user sees "NULL"/"N/A" still in the cleaned CSV after Fix-and-run.
    if (isCellMissing(v, missingTokens)) continue;
    if (typeof v === "number" && Number.isFinite(v)) {
      numericValues.push(v);
    } else if (typeof v === "string") {
      stringCounts.set(v, (stringCounts.get(v) ?? 0) + 1);
    }
  }

  if (strategy === "mean") {
    if (numericValues.length === 0) return null;
    return numericValues.reduce((s, n) => s + n, 0) / numericValues.length;
  }
  if (strategy === "median") {
    if (numericValues.length === 0) return null;
    const sorted = [...numericValues].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
  }
  // mode: prefer strings if any non-missing strings exist; else fall back to numbers
  if (stringCounts.size > 0) {
    let mode = "";
    let max = -1;
    for (const [k, count] of stringCounts) {
      if (count > max) {
        max = count;
        mode = k;
      }
    }
    return mode;
  }
  if (numericValues.length === 0) return null;
  const numCounts = new Map<number, number>();
  for (const n of numericValues) numCounts.set(n, (numCounts.get(n) ?? 0) + 1);
  let mode = numericValues[0];
  let max = -1;
  for (const [k, count] of numCounts) {
    if (count > max) {
      max = count;
      mode = k;
    }
  }
  return mode;
}
