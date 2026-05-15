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
import { applyFix } from "./apply-fix";
import type { DatasetView } from "./dataset";
import type { FixProposal } from "../../../types/dataguard";

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

  test("flag: does not mutate rows, populates flaggedRows", () => {
    const ds: DatasetView = {
      columns: ["x"],
      rows: [{ x: 1 }, { x: 2 }, { x: 3 }],
    };
    const result = applyFix(
      ds,
      makeProposal({
        operationKind: "flag",
        operationParams: { rowIndices: [0, 2] },
      })
    );
    expect(result.rowsAffected).toBe(2);
    expect(result.flaggedRows).toEqual([0, 2]);
    expect(result.dataset.rows[0].x).toBe(1);
    expect(result.dataset.rows[2].x).toBe(3);
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
