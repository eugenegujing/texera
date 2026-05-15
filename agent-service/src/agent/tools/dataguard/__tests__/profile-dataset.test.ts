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
import { profileDataset } from "../profile-dataset";
import type { DatasetView } from "../dataset";

describe("profileDataset", () => {
  test("clean dataset → empty issue list", () => {
    const ds: DatasetView = {
      columns: ["age", "name"],
      rows: [
        { age: 25, name: "Alice" },
        { age: 30, name: "Bob" },
      ],
    };
    expect(profileDataset(ds)).toEqual([]);
  });

  test("empty dataset → empty issue list", () => {
    expect(profileDataset({ columns: [], rows: [] })).toEqual([]);
  });

  test("detects missing values per column (null + empty string)", () => {
    const ds: DatasetView = {
      columns: ["age", "name"],
      rows: [
        { age: 25, name: "Alice" },
        { age: null, name: "Bob" },
        { age: 30, name: "" },
      ],
    };
    const issues = profileDataset(ds);
    const ageMiss = issues.find(i => i.issueType === "missing_value" && i.column === "age");
    const nameMiss = issues.find(i => i.issueType === "missing_value" && i.column === "name");
    expect(ageMiss).toBeDefined();
    expect(ageMiss!.affectedRowCount).toBe(1);
    expect(nameMiss).toBeDefined();
    expect(nameMiss!.affectedRowCount).toBe(1);
  });

  test("treats configured missing tokens as missing", () => {
    const ds: DatasetView = {
      columns: ["x"],
      rows: [{ x: "ok" }, { x: "N/A" }, { x: "NA" }, { x: "ok" }],
    };
    const issues = profileDataset(ds);
    const miss = issues.find(i => i.issueType === "missing_value");
    expect(miss).toBeDefined();
    expect(miss!.affectedRowCount).toBe(2);
  });

  test("NaN counts as missing", () => {
    const ds: DatasetView = {
      columns: ["x"],
      rows: [{ x: 1 }, { x: Number.NaN }, { x: 3 }],
    };
    const issues = profileDataset(ds);
    const miss = issues.find(i => i.issueType === "missing_value");
    expect(miss).toBeDefined();
    expect(miss!.affectedRowCount).toBe(1);
  });

  test("detects 999 as placeholder in numeric column (default placeholder list)", () => {
    const ds: DatasetView = {
      columns: ["age"],
      rows: [
        { age: 25 }, { age: 999 }, { age: 30 }, { age: 999 }, { age: 999 },
      ],
    };
    const issues = profileDataset(ds);
    const ph = issues.find(i => i.issueType === "placeholder_value");
    expect(ph).toBeDefined();
    expect(ph!.column).toBe("age");
    expect(ph!.affectedRowCount).toBe(3);
  });

  test("custom placeholder list overrides default", () => {
    const ds: DatasetView = {
      columns: ["status"],
      rows: [
        { status: "ok" }, { status: "missing" }, { status: "ok" }, { status: "missing" },
      ],
    };
    const issues = profileDataset(ds, { placeholderValues: ["missing"] });
    const ph = issues.find(i => i.issueType === "placeholder_value");
    expect(ph).toBeDefined();
    expect(ph!.affectedRowCount).toBe(2);
  });

  test("idColumn → detects duplicate IDs", () => {
    const ds: DatasetView = {
      columns: ["sample_id", "value"],
      rows: [
        { sample_id: "S1", value: 1 },
        { sample_id: "S2", value: 2 },
        { sample_id: "S1", value: 99 },
        { sample_id: "S3", value: 3 },
      ],
    };
    const issues = profileDataset(ds, { idColumn: "sample_id" });
    const dup = issues.find(i => i.issueType === "duplicate_id");
    expect(dup).toBeDefined();
    expect(dup!.column).toBe("sample_id");
    expect(dup!.affectedRowCount).toBe(2);
  });

  test("no idColumn → no duplicate_id issue even with repeated values", () => {
    const ds: DatasetView = {
      columns: ["x"],
      rows: [{ x: 1 }, { x: 1 }, { x: 1 }],
    };
    const issues = profileDataset(ds);
    expect(issues.find(i => i.issueType === "duplicate_id")).toBeUndefined();
  });

  test("validRanges → detects out-of-range values", () => {
    const ds: DatasetView = {
      columns: ["bmi"],
      rows: [
        { bmi: 25.5 }, { bmi: 65 }, { bmi: 72 }, { bmi: 22 },
      ],
    };
    const issues = profileDataset(ds, { validRanges: { bmi: { min: 10, max: 60 } } });
    const oor = issues.find(i => i.issueType === "out_of_range");
    expect(oor).toBeDefined();
    expect(oor!.affectedRowCount).toBe(2);
  });

  test("placeholder values are not double-counted as out_of_range", () => {
    const ds: DatasetView = {
      columns: ["age"],
      rows: [{ age: 25 }, { age: 999 }, { age: 30 }],
    };
    const issues = profileDataset(ds, { validRanges: { age: { min: 0, max: 130 } } });
    expect(issues.find(i => i.issueType === "out_of_range")).toBeUndefined();
    expect(issues.find(i => i.issueType === "placeholder_value")).toBeDefined();
  });

  test("affectedRowIndices included when small (≤50)", () => {
    const ds: DatasetView = {
      columns: ["age"],
      rows: [{ age: 999 }, { age: 25 }, { age: 999 }],
    };
    const issues = profileDataset(ds);
    const ph = issues.find(i => i.issueType === "placeholder_value")!;
    expect(ph.affectedRowIndices).toEqual([0, 2]);
  });

  test("affectedRowIndices omitted for large sets (>50)", () => {
    const rows = Array.from({ length: 100 }, (_, i) => ({ x: i < 60 ? null : i }));
    const ds: DatasetView = { columns: ["x"], rows };
    const issues = profileDataset(ds);
    const miss = issues.find(i => i.issueType === "missing_value")!;
    expect(miss.affectedRowCount).toBe(60);
    expect(miss.affectedRowIndices).toBeUndefined();
  });

  test("each emitted issue has a distinct issueId", () => {
    const ds: DatasetView = {
      columns: ["a", "b"],
      rows: [{ a: null, b: null }],
    };
    const issues = profileDataset(ds);
    const ids = new Set(issues.map(i => i.issueId));
    expect(ids.size).toBe(issues.length);
    expect(issues.length).toBeGreaterThan(0);
  });

  test("realistic diabetes fixture surfaces 4 issue categories", () => {
    // Mirrors the §5 storyboard's diabetes_messy.csv: placeholder ages,
    // missing glucose, duplicate sample IDs, and biologically impossible BMI.
    const ds: DatasetView = {
      columns: ["sample_id", "age", "glucose", "bmi", "group"],
      rows: [
        { sample_id: "S1", age: 45, glucose: 110, bmi: 28, group: "A" },
        { sample_id: "S2", age: 999, glucose: null, bmi: 30, group: "A" },
        { sample_id: "S1", age: 45, glucose: 120, bmi: 27, group: "A" }, // dup ID
        { sample_id: "S3", age: 50, glucose: null, bmi: 65, group: "B" }, // impossible BMI
        { sample_id: "S4", age: 999, glucose: 140, bmi: 31, group: "B" },
      ],
    };
    const issues = profileDataset(ds, {
      idColumn: "sample_id",
      validRanges: { age: { min: 0, max: 120 }, bmi: { min: 10, max: 60 } },
    });
    const kinds = new Set(issues.map(i => i.issueType));
    expect(kinds.has("placeholder_value")).toBe(true);
    expect(kinds.has("missing_value")).toBe(true);
    expect(kinds.has("duplicate_id")).toBe(true);
    expect(kinds.has("out_of_range")).toBe(true);
  });
});
