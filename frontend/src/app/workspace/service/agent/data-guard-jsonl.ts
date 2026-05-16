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

/**
 * JSONL parser for DataGuard auto-trigger.
 *
 * Produces the same `{columns, rows}` shape that `loadFromOperatorFile` would
 * have produced via Papa.parse — so the five detectors in `profile_dataset`
 * fire identically on a JSONL-loaded `DatasetView` as on a CSV-loaded one.
 *
 * Flatten policy:
 *   - Nested objects → dot-notation column names. `{address:{street:"x"}}`
 *     produces a single column `address.street`.
 *   - Arrays → `JSON.stringify(arr)` as a single cell (one row per JSONL line;
 *     we never explode arrays across rows — that would change row indices and
 *     break the locate / apply-fix rowIndices contract).
 *   - Lines that aren't JSON *objects* (bare strings, numbers, top-level
 *     arrays, booleans, null) → skipped with a console.warn.
 *   - Blank lines, CRLF, trailing newlines → tolerated silently.
 *   - Column set = union of keys across all parsed rows; rows missing a column
 *     get `null` for that cell.
 *   - Collision (literal top-level dotted key vs nested-key flatten producing
 *     the same final name): the **nested-key value always wins**, regardless
 *     of which key appears first in JSON source order. A warning is logged
 *     once per colliding path per parse. The implementation uses two passes
 *     (collect every nested path first; then write leaves, blocking literal-
 *     dotted top-level writes that land on a nested-owned slot) so source
 *     order does not matter.
 */

export interface ParsedJsonl {
  columns: string[];
  rows: Record<string, unknown>[];
}

const isPlainObject = (v: unknown): v is Record<string, unknown> =>
  v !== null && typeof v === "object" && !Array.isArray(v);

/**
 * Flatten one JSON object into a `Record<string, unknown>` whose keys are
 * dot-notation paths and whose values are leaves (primitives, null, or
 * stringified arrays).
 *
 * Collision rule: when a literal top-level dotted key (e.g. `"address.street"`)
 * collides with a path produced by descending into a nested object (e.g.
 * `address: {street: ...}`), the **nested** value always wins, regardless of
 * which appeared first in JSON source order. Pass 1 collects every nested
 * path; pass 2 writes leaves and skips literal-dotted top-level keys whose
 * path is already owned by a nested descent. Nested-wins is defensible: the
 * nested structure is the "real" hierarchy; a literal-dotted top-level key
 * is the edge case.
 */
function flattenObject(obj: Record<string, unknown>, warnedCollisions: Set<string>): Record<string, unknown> {
  // Pass 1: collect every path produced by descending into a nested object.
  // Literal-dotted top-level KEYS are not visited here (they are leaves at
  // the top level even if their name contains a dot).
  const nestedPaths = new Set<string>();
  const collectNested = (node: Record<string, unknown>, prefix: string): void => {
    for (const key of Object.keys(node)) {
      const value = node[key];
      const path = prefix ? `${prefix}.${key}` : key;
      if (isPlainObject(value)) {
        // Descending into a nested object — every leaf path reached this way
        // is "owned" by the nested structure and must beat any literal-dotted
        // top-level write.
        collectNested(value, path);
      } else if (prefix !== "") {
        // A leaf reached *via* a nested descent (prefix non-empty).
        nestedPaths.add(path);
      }
    }
  };
  collectNested(obj, "");

  // Pass 2: write every leaf. Nested leaves always go in. Literal-dotted
  // top-level leaves are blocked if a nested path already owns the slot.
  const out: Record<string, unknown> = {};
  const writeLeaves = (node: Record<string, unknown>, prefix: string, isTopLevel: boolean): void => {
    for (const key of Object.keys(node)) {
      const value = node[key];
      const path = prefix ? `${prefix}.${key}` : key;
      if (isPlainObject(value)) {
        writeLeaves(value, path, false);
        continue;
      }
      // Array → JSON-stringified single cell. Primitives / null / undefined
      // pass through as-is; `null` is meaningful for missing-value detection.
      const leaf = Array.isArray(value) ? JSON.stringify(value) : value;
      if (isTopLevel && nestedPaths.has(path)) {
        // Literal-dotted top-level key colliding with a nested path. Skip the
        // write entirely (nested wins). Warn once per path per parse.
        if (!warnedCollisions.has(path)) {
          // eslint-disable-next-line no-console
          console.warn(`[DataGuard JSONL] column name collision on "${path}"; nested-key value wins.`);
          warnedCollisions.add(path);
        }
        continue;
      }
      out[path] = leaf;
    }
  };
  writeLeaves(obj, "", true);
  return out;
}

export async function parseJsonl(blob: Blob, fileName: string): Promise<ParsedJsonl> {
  const text = await blob.text();
  // CRLF tolerance: normalize first. Splitting on /\r?\n/ would also work but
  // a single replace keeps the offset arithmetic obvious if we ever surface
  // per-line errors with line numbers.
  const normalized = text.replace(/\r\n/g, "\n");
  const lines = normalized.split("\n");

  const rows: Record<string, unknown>[] = [];
  const columnOrder: string[] = [];
  const seenColumns = new Set<string>();
  const warnedCollisions = new Set<string>();
  let skippedNonObject = 0;
  let skippedParseError = 0;

  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    if (raw.length === 0 || raw.trim().length === 0) continue;
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch (e: unknown) {
      skippedParseError++;
      // eslint-disable-next-line no-console
      console.warn(
        `[DataGuard JSONL] ${fileName}: line ${i + 1} is not valid JSON, skipping.`,
        e instanceof Error ? e.message : e
      );
      continue;
    }
    if (!isPlainObject(parsed)) {
      skippedNonObject++;
      // eslint-disable-next-line no-console
      console.warn(
        `[DataGuard JSONL] ${fileName}: line ${i + 1} is not a JSON object (got ${
          parsed === null ? "null" : Array.isArray(parsed) ? "array" : typeof parsed
        }), skipping.`
      );
      continue;
    }
    const flat = flattenObject(parsed, warnedCollisions);
    for (const key of Object.keys(flat)) {
      if (!seenColumns.has(key)) {
        seenColumns.add(key);
        columnOrder.push(key);
      }
    }
    rows.push(flat);
  }

  // Fill missing keys with null so downstream detectors see a consistent
  // schema (the profiler iterates columns × rows; an undefined cell would
  // skew its missing-value counts vs. an explicit null).
  for (const row of rows) {
    for (const col of columnOrder) {
      if (!Object.prototype.hasOwnProperty.call(row, col)) {
        row[col] = null;
      }
    }
  }

  if (skippedNonObject > 0 || skippedParseError > 0) {
    // eslint-disable-next-line no-console
    console.warn(
      `[DataGuard JSONL] ${fileName}: skipped ${skippedNonObject} non-object line(s), ${skippedParseError} parse error(s).`
    );
  }

  return { columns: columnOrder, rows };
}
