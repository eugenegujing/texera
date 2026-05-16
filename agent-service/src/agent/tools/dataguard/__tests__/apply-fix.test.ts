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

import { describe, expect, test } from "bun:test";
import { applyFix } from "../apply-fix";
import type { DatasetView } from "../dataset";
import type { FixProposal } from "../../../../types/dataguard";

function makeProposal(overrides: Partial<FixProposal> = {}): FixProposal {
  return {
    issueId: "iss-test",
    issueType: "placeholder_value",
    action: "test action",
    operationKind: "replace_value",
    operationParams: {},
    riskTier: "medium",
    reason: "test",
    evidence: "test",
    confidence: "high",
    targetRowCount: 0,
    ...overrides,
  };
}

describe("applyFix", () => {
  test("replace_value: swaps matching cells, leaves rest", () => {
    const ds: DatasetView = {
      columns: ["age"],
      rows: [{ age: 25 }, { age: 999 }, { age: 30 }, { age: 999 }],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "replace_value",
        operationParams: { column: "age", match: 999, replacement: null },
      })
    );
    expect(result.rowsAffected).toBe(2);
    expect(result.dataset.rows[0].age).toBe(25);
    expect(result.dataset.rows[1].age).toBe(null);
    expect(result.dataset.rows[3].age).toBe(null);
  });

  test("replace_value with rowIndices: targets rows by index, ignores cell value", () => {
    // Regression for the "No changes detected in dataset" LakeFS error: when
    // the LLM proposed `match` that didn't equal the cell exactly (rounding,
    // string-vs-number, etc.), cellEquals silently no-op'd and the exported
    // CSV was byte-identical → version commit aborted. rowIndices is the
    // deterministic escape hatch used by outlier proposals.
    const ds: DatasetView = {
      columns: ["glucose"],
      rows: [{ glucose: 100 }, { glucose: 949.7 }, { glucose: 120 }, { glucose: 815.3 }],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "replace_value",
        operationParams: { column: "glucose", rowIndices: [1, 3], replacement: 110 },
      })
    );
    expect(result.rowsAffected).toBe(2);
    expect(result.dataset.rows[0].glucose).toBe(100);
    expect(result.dataset.rows[1].glucose).toBe(110);
    expect(result.dataset.rows[2].glucose).toBe(120);
    expect(result.dataset.rows[3].glucose).toBe(110);
  });

  test("replace_value: rowIndices wins when both rowIndices and match are present", () => {
    // Deterministic precedence: if both are supplied (legacy LLM output),
    // honor the index-based targeting since match is the fragile path.
    const ds: DatasetView = {
      columns: ["x"],
      rows: [{ x: 1 }, { x: 2 }, { x: 3 }],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "replace_value",
        operationParams: {
          column: "x",
          rowIndices: [0],
          match: 999, // would match nothing — only rowIndices should win
          replacement: 99,
        },
      })
    );
    expect(result.rowsAffected).toBe(1);
    expect(result.dataset.rows[0].x).toBe(99);
    expect(result.dataset.rows[1].x).toBe(2);
  });

  test("replace_value with rowIndices skips no-op writes (cell already equals replacement)", () => {
    // Iterative cleanup regression: after v1→v2→v3 capping outliers to the
    // IQR fence, the LLM proposes "replace these rows with fence value X"
    // but those rows already hold X from the previous round. Without the
    // equality guard, rowsAffected would be 3, the frontend would push a
    // byte-identical CSV, and LakeFS would abort with "No changes detected."
    // With the guard, rowsAffected === 0 → frontend skips the upload.
    const ds: DatasetView = {
      columns: ["bmi"],
      rows: [
        { bmi: 28.1 },
        { bmi: 35.74 }, // already at fence
        { bmi: 27.5 },
        { bmi: 35.74 }, // already at fence
        { bmi: 35.74 }, // already at fence
      ],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "replace_value",
        operationParams: { column: "bmi", rowIndices: [1, 3, 4], replacement: 35.74 },
      })
    );
    expect(result.rowsAffected).toBe(0);
    expect(result.dataset.rows[1].bmi).toBe(35.74);
    expect(result.dataset.rows[3].bmi).toBe(35.74);
  });

  test("replace_value with rowIndices: mixed (some cells already match, some don't)", () => {
    // Same scenario but row 3 still has a genuine outlier (75.2). Only that
    // row should count as affected; the others are already at the fence.
    const ds: DatasetView = {
      columns: ["bmi"],
      rows: [
        { bmi: 35.74 },
        { bmi: 35.74 },
        { bmi: 35.74 },
        { bmi: 75.2 }, // real outlier
      ],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "replace_value",
        operationParams: { column: "bmi", rowIndices: [0, 1, 2, 3], replacement: 35.74 },
      })
    );
    expect(result.rowsAffected).toBe(1);
    expect(result.dataset.rows[3].bmi).toBe(35.74);
  });

  test("replace_value: original dataset is not mutated", () => {
    const ds: DatasetView = {
      columns: ["age"],
      rows: [{ age: 999 }, { age: 30 }],
    };
    const before = JSON.stringify(ds);
    applyFix(
      ds,
      makeProposal({
        operationKind: "replace_value",
        operationParams: { column: "age", match: 999, replacement: null },
      })
    );
    expect(JSON.stringify(ds)).toBe(before);
  });

  test("drop_rows: removes rows at given indices", () => {
    const ds: DatasetView = {
      columns: ["x"],
      rows: [{ x: 0 }, { x: 1 }, { x: 2 }, { x: 3 }],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "drop_rows",
        operationParams: { rowIndices: [1, 3] },
      })
    );
    expect(result.rowsAffected).toBe(2);
    expect(result.dataset.rows).toHaveLength(2);
    expect(result.dataset.rows[0].x).toBe(0);
    expect(result.dataset.rows[1].x).toBe(2);
  });

  test("impute mean: fills missing with column mean", () => {
    const ds: DatasetView = {
      columns: ["v"],
      rows: [{ v: 10 }, { v: 20 }, { v: null }, { v: 30 }, { v: null }],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "impute",
        operationParams: { column: "v", strategy: "mean" },
      })
    );
    expect(result.rowsAffected).toBe(2);
    expect(result.dataset.rows[2].v).toBe(20);
    expect(result.dataset.rows[4].v).toBe(20);
  });

  test("impute median (odd count): fills missing with middle value", () => {
    const ds: DatasetView = {
      columns: ["v"],
      rows: [{ v: 1 }, { v: 3 }, { v: null }, { v: 100 }],
    };
    // Non-missing values [1, 3, 100], sorted → median = 3
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "impute",
        operationParams: { column: "v", strategy: "median" },
      })
    );
    expect(result.dataset.rows[2].v).toBe(3);
  });

  test("impute median (even count): fills missing with mean of two middle", () => {
    const ds: DatasetView = {
      columns: ["v"],
      rows: [{ v: 1 }, { v: 3 }, { v: null }, { v: 5 }, { v: 100 }],
    };
    // Non-missing values [1, 3, 5, 100], sorted → (3 + 5) / 2 = 4
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "impute",
        operationParams: { column: "v", strategy: "median" },
      })
    );
    expect(result.dataset.rows[2].v).toBe(4);
  });

  test("impute: treats profiler missing-tokens (N/A, NULL, null, Unknown, whitespace) as missing", () => {
    // Regression: apply-fix's isMissing used to only recognize null/undefined/NaN/""
    // while the profiler flagged "N/A", "NULL", "null", "Unknown" as missing. Result:
    // after Fix-and-run, the cleaned CSV still contained those literal tokens because
    // impute silently skipped them. Both sides must agree.
    const ds: DatasetView = {
      columns: ["glucose"],
      rows: [
        { glucose: 100 },
        { glucose: "NULL" },
        { glucose: 120 },
        { glucose: "null" },
        { glucose: "N/A" },
        { glucose: " " },
        { glucose: 140 },
      ],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "impute",
        operationParams: { column: "glucose", strategy: "median" },
      })
    );
    // 4 missing tokens replaced; non-missing observations [100, 120, 140] → median 120.
    expect(result.rowsAffected).toBe(4);
    expect(result.dataset.rows[1].glucose).toBe(120);
    expect(result.dataset.rows[3].glucose).toBe(120);
    expect(result.dataset.rows[4].glucose).toBe(120);
    expect(result.dataset.rows[5].glucose).toBe(120);
  });

  test("impute mode: missing-token strings are not counted as mode candidates", () => {
    // "NULL" appearing twice must not be voted the mode just because it's the
    // most frequent string — it's a missing-marker, not data.
    const ds: DatasetView = {
      columns: ["group"],
      rows: [
        { group: "A" },
        { group: "NULL" },
        { group: "A" },
        { group: "NULL" },
        { group: "B" },
        { group: null },
      ],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "impute",
        operationParams: { column: "group", strategy: "mode" },
      })
    );
    expect(result.dataset.rows[1].group).toBe("A");
    expect(result.dataset.rows[3].group).toBe("A");
    expect(result.dataset.rows[5].group).toBe("A");
  });

  test("impute respects session-supplied missingTokens override", () => {
    // Regression for the missingTokens-not-threaded bug: a user who set
    // {missingTokens: ["xyz"]} at scan time would have rows whose value is
    // literally "xyz" flagged by the profiler but silently skipped by impute
    // (which only knew about the default tokens). Threading ApplyOptions
    // through fixes this.
    const ds: DatasetView = {
      columns: ["v"],
      rows: [{ v: 1 }, { v: "xyz" }, { v: 3 }, { v: "xyz" }, { v: 5 }],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "impute",
        operationParams: { column: "v", strategy: "median" },
      }),
      { missingTokens: ["xyz"] }
    );
    // Non-missing values [1, 3, 5] → median 3, both "xyz" cells replaced.
    expect(result.rowsAffected).toBe(2);
    expect(result.dataset.rows[1].v).toBe(3);
    expect(result.dataset.rows[3].v).toBe(3);
  });

  test("impute mode: fills missing with most common string", () => {
    const ds: DatasetView = {
      columns: ["c"],
      rows: [
        { c: "A" }, { c: "A" }, { c: "B" }, { c: null }, { c: "" },
      ],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "impute",
        operationParams: { column: "c", strategy: "mode" },
      })
    );
    expect(result.rowsAffected).toBe(2);
    expect(result.dataset.rows[3].c).toBe("A");
    expect(result.dataset.rows[4].c).toBe("A");
  });

  test("trim_whitespace: trims string cells in target column", () => {
    const ds: DatasetView = {
      columns: ["name"],
      rows: [{ name: " Alice " }, { name: "Bob" }, { name: "\tCharlie\n" }],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "trim_whitespace",
        operationParams: { column: "name" },
      })
    );
    expect(result.rowsAffected).toBe(2);
    expect(result.dataset.rows[0].name).toBe("Alice");
    expect(result.dataset.rows[1].name).toBe("Bob");
    expect(result.dataset.rows[2].name).toBe("Charlie");
  });

  test("standardize: maps values per mapping dict", () => {
    const ds: DatasetView = {
      columns: ["yn"],
      rows: [{ yn: "Y" }, { yn: "yes" }, { yn: "n" }, { yn: "N" }, { yn: "unknown" }],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "standardize",
        operationParams: {
          column: "yn",
          mapping: { Y: "yes", N: "no", n: "no" },
        },
      })
    );
    expect(result.rowsAffected).toBe(3);
    expect(result.dataset.rows[0].yn).toBe("yes");
    expect(result.dataset.rows[1].yn).toBe("yes"); // unchanged (no mapping)
    expect(result.dataset.rows[2].yn).toBe("no");
    expect(result.dataset.rows[3].yn).toBe("no");
    expect(result.dataset.rows[4].yn).toBe("unknown");
  });

  test("rename_column: updates columns array and per-row keys", () => {
    const ds: DatasetView = {
      columns: ["sample_id", "value"],
      rows: [{ sample_id: "S1", value: 1 }, { sample_id: "S2", value: 2 }],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "rename_column",
        operationParams: { from: "sample_id", to: "subjectId" },
      })
    );
    expect(result.dataset.columns).toEqual(["subjectId", "value"]);
    expect(result.dataset.rows[0].subjectId).toBe("S1");
    expect(result.dataset.rows[0].sample_id).toBeUndefined();
  });

  test("empty dataset: returns empty dataset and zero rowsAffected", () => {
    const ds: DatasetView = { columns: [], rows: [] };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "replace_value",
        operationParams: { column: "x", match: 1, replacement: 0 },
      })
    );
    expect(result.rowsAffected).toBe(0);
    expect(result.dataset.rows).toEqual([]);
  });

  test("unknown operationKind: throws", () => {
    const bad = makeProposal({
      operationKind: "nuke_database" as unknown as FixProposal["operationKind"],
      operationParams: {},
    });
    expect(() => applyFix({ columns: [], rows: [] }, bad)).toThrow(/unknown operationKind/);
  });

  test("realistic diabetes flow: replace age=999 with NULL leaves other columns intact", () => {
    const ds: DatasetView = {
      columns: ["sample_id", "age", "glucose"],
      rows: [
        { sample_id: "S1", age: 45, glucose: 110 },
        { sample_id: "S2", age: 999, glucose: 130 },
        { sample_id: "S3", age: 999, glucose: 140 },
      ],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "replace_value",
        operationParams: { column: "age", match: 999, replacement: null },
      })
    );
    expect(result.rowsAffected).toBe(2);
    expect(result.dataset.rows[1].age).toBeNull();
    expect(result.dataset.rows[1].glucose).toBe(130);
    expect(result.dataset.rows[1].sample_id).toBe("S2");
  });
});
