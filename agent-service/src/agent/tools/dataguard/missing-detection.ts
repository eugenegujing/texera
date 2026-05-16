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

// Single source of truth for "is this cell missing / placeholder?" Used by both
// the profiler (which flags issues) and the applier (which fills/replaces).
// Why: when the two disagree, impute silently leaves cells that the profiler
// flagged as missing — the user sees "NULL"/"N/A" still in the cleaned CSV.

export const DEFAULT_PLACEHOLDERS: ReadonlyArray<string | number> = [999, -1, "unknown", "Unknown"];

// Case-insensitive set of tokens that mean "no value was recorded." Compared
// against the *trimmed*, lowercased cell so whitespace and case can't smuggle
// a missing cell past the check.
const MISSING_TOKENS_LOWER: ReadonlySet<string> = new Set(["na", "n/a", "null", "none", "nan"]);

// Kept for places that still want the raw token list (e.g., the profiler's
// ProfileOptions API surface).
export const DEFAULT_MISSING_TOKENS: ReadonlyArray<string> = ["NA", "N/A", "n/a", "null", "NULL", "None"];

export function isMissing(value: unknown, extraTokens: ReadonlyArray<string> = []): boolean {
  if (value === null || value === undefined) return true;
  if (typeof value === "number" && Number.isNaN(value)) return true;
  if (typeof value !== "string") return false;
  const trimmed = value.trim();
  if (trimmed === "") return true;
  if (MISSING_TOKENS_LOWER.has(trimmed.toLowerCase())) return true;
  if (extraTokens.includes(value) || extraTokens.includes(trimmed)) return true;
  return false;
}

export function toNumber(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return undefined;
}

export function placeholderHit(
  value: unknown,
  placeholders: ReadonlyArray<string | number>
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
