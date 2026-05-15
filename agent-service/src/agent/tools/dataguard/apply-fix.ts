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

export interface ApplyResult {
  dataset: DatasetView;
  rowsAffected: number;
  flaggedRows: number[];
}

export function applyFix(dataset: DatasetView, proposal: FixProposal): ApplyResult {
  const rows = dataset.rows.map(r => ({ ...r }));
  let columns = [...dataset.columns];
  const params = proposal.operationParams;

  switch (proposal.operationKind) {
    case "replace_value": {
      const column = params.column as string;
      const match = params.match;
      const replacement = params.replacement;
      let affected = 0;
      for (const r of rows) {
        if (cellEquals(r[column], match)) {
          r[column] = replacement;
          affected++;
        }
      }
      return { dataset: { columns, rows }, rowsAffected: affected, flaggedRows: [] };
    }

    case "drop_rows": {
      const drop = new Set(params.rowIndices as number[]);
      const kept = rows.filter((_, i) => !drop.has(i));
      return {
        dataset: { columns, rows: kept },
        rowsAffected: rows.length - kept.length,
        flaggedRows: [],
      };
    }

    case "impute": {
      const column = params.column as string;
      const strategy = params.strategy as "mean" | "median" | "mode";
      const fill = computeImputeValue(rows, column, strategy);
      let affected = 0;
      for (const r of rows) {
        if (isMissing(r[column])) {
          r[column] = fill;
          affected++;
        }
      }
      return { dataset: { columns, rows }, rowsAffected: affected, flaggedRows: [] };
    }

    case "flag": {
      const indices = (params.rowIndices as number[]).slice();
      return {
        dataset: { columns, rows },
        rowsAffected: indices.length,
        flaggedRows: indices,
      };
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
      return { dataset: { columns, rows }, rowsAffected: affected, flaggedRows: [] };
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
      return { dataset: { columns, rows }, rowsAffected: affected, flaggedRows: [] };
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
      return { dataset: { columns, rows }, rowsAffected: affected, flaggedRows: [] };
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

function isMissing(v: unknown): boolean {
  if (v === null || v === undefined) return true;
  if (typeof v === "number" && Number.isNaN(v)) return true;
  if (typeof v === "string" && v === "") return true;
  return false;
}

function computeImputeValue(
  rows: Record<string, unknown>[],
  column: string,
  strategy: "mean" | "median" | "mode"
): unknown {
  const numericValues: number[] = [];
  const stringCounts = new Map<string, number>();
  for (const r of rows) {
    const v = r[column];
    if (isMissing(v)) continue;
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
