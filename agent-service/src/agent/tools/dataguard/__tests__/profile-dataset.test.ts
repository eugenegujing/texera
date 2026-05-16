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
import { profileDataset, rowFingerprint } from "../profile-dataset";
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
      rows: [{ age: 25 }, { age: 999 }, { age: 30 }, { age: 999 }, { age: 999 }],
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
      rows: [{ status: "ok" }, { status: "missing" }, { status: "ok" }, { status: "missing" }],
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

  test("auto-infers idColumn from name patterns (sample_id) when caller omits it", () => {
    // The auto-trigger pipeline POSTs /scan with an empty body — without this
    // inference, dup-ID detection would never fire on user files. Match must
    // be conservative (id-like column names only), not value-based.
    const ds: DatasetView = {
      columns: ["sample_id", "age"],
      rows: [
        { sample_id: "S1", age: 30 },
        { sample_id: "S2", age: 40 },
        { sample_id: "S1", age: 30 },
      ],
    };
    const issues = profileDataset(ds);
    const dup = issues.find(i => i.issueType === "duplicate_id");
    expect(dup).toBeDefined();
    expect(dup!.column).toBe("sample_id");
    expect(dup!.affectedRowCount).toBe(2);
  });

  test("auto-infer recognizes bare `id`, `*Id`, `id_*` patterns too", () => {
    const cases: Array<{ col: string }> = [{ col: "id" }, { col: "userId" }, { col: "id_card" }, { col: "ID" }];
    for (const { col } of cases) {
      const ds: DatasetView = {
        columns: [col, "value"],
        rows: [
          { [col]: "a", value: 1 },
          { [col]: "a", value: 2 },
          { [col]: "b", value: 3 },
        ],
      };
      const issues = profileDataset(ds);
      const dup = issues.find(i => i.issueType === "duplicate_id");
      expect(dup).toBeDefined();
      expect(dup!.column).toBe(col);
    }
  });

  test("auto-infer recognizes dotted JSONL-flatten names (`user.id`, `customer.uid`, nested)", () => {
    // JSONL flatten produces dot-notation column names. The underscore-based
    // matchers used to miss these (`.` is not `_`, and `Id$` is case-sensitive),
    // so dup-ID detection silently no-op'd on JSONL-loaded data. Regression
    // lock for F1 from the round-2 review.
    const cases: Array<{ col: string }> = [
      { col: "user.id" },
      { col: "customer.uid" },
      { col: "nested.user.id" },
      { col: "Account.ID" }, // case-insensitive
    ];
    for (const { col } of cases) {
      const ds: DatasetView = {
        columns: [col, "value"],
        rows: [
          { [col]: "a", value: 1 },
          { [col]: "a", value: 2 },
          { [col]: "b", value: 3 },
        ],
      };
      const issues = profileDataset(ds);
      const dup = issues.find(i => i.issueType === "duplicate_id");
      expect(dup).toBeDefined();
      expect(dup!.column).toBe(col);
    }
  });

  test("auto-infer does NOT fire when no column name looks like an ID", () => {
    // Conservative: just having repeated values isn't enough — the user's
    // workflow may legitimately have duplicate categorical labels.
    const ds: DatasetView = {
      columns: ["color", "qty"],
      rows: [
        { color: "red", qty: 1 },
        { color: "red", qty: 2 },
      ],
    };
    const issues = profileDataset(ds);
    expect(issues.find(i => i.issueType === "duplicate_id")).toBeUndefined();
  });

  test("no idColumn → no duplicate_id issue even with repeated values", () => {
    const ds: DatasetView = {
      columns: ["x"],
      rows: [{ x: 1 }, { x: 1 }, { x: 1 }],
    };
    const issues = profileDataset(ds);
    expect(issues.find(i => i.issueType === "duplicate_id")).toBeUndefined();
  });

  test("validRanges → detects outlier values", () => {
    const ds: DatasetView = {
      columns: ["bmi"],
      rows: [{ bmi: 25.5 }, { bmi: 65 }, { bmi: 72 }, { bmi: 22 }],
    };
    const issues = profileDataset(ds, { validRanges: { bmi: { min: 10, max: 60 } } });
    const outlier = issues.find(i => i.issueType === "outlier");
    expect(outlier).toBeDefined();
    expect(outlier!.affectedRowCount).toBe(2);
  });

  test("placeholder values are not double-counted as outliers", () => {
    const ds: DatasetView = {
      columns: ["age"],
      rows: [{ age: 25 }, { age: 999 }, { age: 30 }],
    };
    const issues = profileDataset(ds, { validRanges: { age: { min: 0, max: 130 } } });
    expect(issues.find(i => i.issueType === "outlier")).toBeUndefined();
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
    expect(kinds.has("outlier")).toBe(true);
  });

  describe("outlier detector (IQR auto + validRanges override)", () => {
    // Auto-IQR is off by default (see ProfileOptions.enableOutlierDetection
    // doc-comment for why). Tests that exercise the IQR branch opt in
    // explicitly with `enableOutlierDetection: true` so the same coverage
    // survives a future re-enable: flip the default and these tests stop
    // needing the override.

    test("auto-fires on a clear outlier via IQR (Tukey 1.5× fence) — no hint needed", () => {
      // 19 values near 50, one extreme reading at 500. IQR Q1/Q3 are
      // unaffected by the single outlier, so the fence excludes 500.
      const rows: Array<Record<string, unknown>> = [];
      for (let i = 0; i < 19; i++) rows.push({ v: 50 + (i % 3) });
      rows.push({ v: 500 });
      const issues = profileDataset({ columns: ["v"], rows }, { enableOutlierDetection: true });
      const outlier = issues.find(i => i.issueType === "outlier" && i.column === "v");
      expect(outlier).toBeDefined();
      expect(outlier!.affectedRowIndices).toEqual([19]);
      expect(outlier!.description).toContain("Tukey");
    });

    test("does NOT flag clusters of consecutive large readings — IQR Q3 absorbs them", () => {
      // 10 normals at 100 + 10 sustained-high at 400. Q1=100, Q3=400, IQR=300,
      // upper fence = 400 + 450 = 850. All values fit, no outliers.
      // This is exactly the false-positive case the user wanted protection from.
      const rows: Array<Record<string, unknown>> = [];
      for (let i = 0; i < 10; i++) rows.push({ glucose: 100 });
      for (let i = 0; i < 10; i++) rows.push({ glucose: 400 });
      const issues = profileDataset({ columns: ["glucose"], rows }, { enableOutlierDetection: true });
      expect(issues.find(i => i.issueType === "outlier")).toBeUndefined();
    });

    test("validRanges override wins per-column over auto IQR", () => {
      // age column gets a hard range; bmi column has no range so falls back
      // to IQR. The 500 in age should be flagged via validRanges (not IQR
      // semantics).
      const rows: Array<Record<string, unknown>> = [];
      for (let i = 0; i < 19; i++) rows.push({ age: 30 + (i % 5), bmi: 25 + (i % 3) });
      rows.push({ age: 500, bmi: 27 });
      const issues = profileDataset(
        { columns: ["age", "bmi"], rows },
        { enableOutlierDetection: true, validRanges: { age: { min: 0, max: 120 } } }
      );
      const ageOutlier = issues.find(i => i.issueType === "outlier" && i.column === "age");
      expect(ageOutlier).toBeDefined();
      expect(ageOutlier!.affectedRowIndices).toEqual([19]);
      expect(ageOutlier!.description).toContain("valid range");
      // bmi column has no validRanges; IQR sees a tight cluster, no outlier.
      expect(issues.find(i => i.issueType === "outlier" && i.column === "bmi")).toBeUndefined();
    });

    test("skips column with fewer than outlierMinObservations numeric values", () => {
      // Quartiles aren't meaningful on a tiny sample, so the auto-IQR branch
      // bails. Default min-obs is 10; we feed 5.
      const rows: Array<Record<string, unknown>> = [{ v: 1 }, { v: 2 }, { v: 3 }, { v: 4 }, { v: 1000 }];
      const issues = profileDataset({ columns: ["v"], rows }, { enableOutlierDetection: true });
      expect(issues.find(i => i.issueType === "outlier")).toBeUndefined();
    });

    test("skips placeholder rows so they don't surface under two issue types", () => {
      // age=999 is a default placeholder, so the placeholder detector owns
      // that row. The IQR pass must skip it.
      const rows: Array<Record<string, unknown>> = [];
      for (let i = 0; i < 19; i++) rows.push({ age: 30 + (i % 5) });
      rows.push({ age: 999 }); // placeholder, not outlier
      const issues = profileDataset({ columns: ["age"], rows }, { enableOutlierDetection: true });
      expect(issues.find(i => i.issueType === "placeholder_value")).toBeDefined();
      expect(issues.find(i => i.issueType === "outlier")).toBeUndefined();
    });

    test("skips mostly-non-numeric columns (require ≥ 80% numeric)", () => {
      // 18 strings + 2 numbers → 10% numeric → skip; otherwise the 2 numbers
      // would form a degenerate quartile distribution and flag spuriously.
      const rows: Array<Record<string, unknown>> = [];
      for (let i = 0; i < 18; i++) rows.push({ mixed: "label-" + i });
      rows.push({ mixed: 5 });
      rows.push({ mixed: 9999 });
      const issues = profileDataset({ columns: ["mixed"], rows }, { enableOutlierDetection: true });
      expect(issues.find(i => i.issueType === "outlier" && i.column === "mixed")).toBeUndefined();
    });

    test("custom outlierIqrMultiplier bumps the fence to suppress mild outliers", () => {
      // With default 1.5×, value 500 is flagged. Bumping high enough that the
      // fence comfortably covers 500 → no outlier. Q1=50, Q3=52, IQR=2,
      // multiplier 1000 → fence = 52 + 2000 = 2052, so 500 fits. Useful for
      // users who want IQR off entirely without supplying validRanges.
      const rows: Array<Record<string, unknown>> = [];
      for (let i = 0; i < 19; i++) rows.push({ v: 50 + (i % 3) });
      rows.push({ v: 500 });
      const issues = profileDataset(
        { columns: ["v"], rows },
        { enableOutlierDetection: true, outlierIqrMultiplier: 1000 }
      );
      expect(issues.find(i => i.issueType === "outlier")).toBeUndefined();
    });

    test("does not double-count: IQR skips column when validRanges already set", () => {
      // Mode-1 is exclusive per column: a column with validRanges goes through
      // the hard-range path only — IQR is silently bypassed for that column.
      const rows: Array<Record<string, unknown>> = [];
      for (let i = 0; i < 19; i++) rows.push({ age: 30 + (i % 5) });
      rows.push({ age: 500 });
      const issues = profileDataset(
        { columns: ["age"], rows },
        { enableOutlierDetection: true, validRanges: { age: { min: 0, max: 120 } } }
      );
      // Exactly one outlier issue for age, not two.
      const ageOutliers = issues.filter(i => i.issueType === "outlier" && i.column === "age");
      expect(ageOutliers).toHaveLength(1);
    });

    // -------- Default-off coverage (Part 1 of the round-5 patch) --------

    test("auto-IQR is OFF by default — same input that would flag with the option is silent", () => {
      // Same 19+1=500 fixture as the "auto-fires" test above, but without the
      // `enableOutlierDetection: true` option. Locks the default to false.
      const rows: Array<Record<string, unknown>> = [];
      for (let i = 0; i < 19; i++) rows.push({ v: 50 + (i % 3) });
      rows.push({ v: 500 });
      const issues = profileDataset({ columns: ["v"], rows });
      expect(issues.find(i => i.issueType === "outlier")).toBeUndefined();
    });

    test("validRanges still fires when enableOutlierDetection is false (explicit override)", () => {
      // Per spec: a caller who explicitly supplied validRanges shouldn't be
      // surprised by the new gating. The validRange-based mode runs even with
      // auto-IQR off. Only the auto branch is gated.
      const ds: DatasetView = {
        columns: ["bmi"],
        rows: [{ bmi: 25 }, { bmi: 75 }, { bmi: 30 }, { bmi: 100 }],
      };
      const issues = profileDataset(ds, { validRanges: { bmi: { min: 10, max: 60 } } });
      const outlier = issues.find(i => i.issueType === "outlier" && i.column === "bmi");
      expect(outlier).toBeDefined();
      expect(outlier!.affectedRowCount).toBe(2);
      expect(outlier!.description).toContain("valid range");
    });
  });

  describe("inconsistent_label detector", () => {
    test("flags rows using non-canonical spellings of the same label", () => {
      const ds: DatasetView = {
        columns: ["gender"],
        rows: [
          { gender: "Male" },
          { gender: "Male" },
          { gender: "Male" },
          { gender: "male" },
          { gender: "MALE" },
          { gender: "Female" },
          { gender: "Female" },
          { gender: "female" },
        ],
      };
      const issues = profileDataset(ds);
      const ilab = issues.find(i => i.issueType === "inconsistent_label" && i.column === "gender");
      expect(ilab).toBeDefined();
      // "male" and "MALE" are non-canonical variants of "Male" (most common);
      // "female" is non-canonical variant of "Female". Total: 3 rows.
      expect(ilab!.affectedRowCount).toBe(3);
    });

    test("ignores columns that are pure data (every value unique)", () => {
      const ds: DatasetView = {
        columns: ["note"],
        rows: Array.from({ length: 30 }, (_, i) => ({ note: `note-${i}` })),
      };
      const issues = profileDataset(ds);
      expect(issues.find(i => i.issueType === "inconsistent_label")).toBeUndefined();
    });

    test("ignores columns above cardinality cap (likely free text)", () => {
      const rows: Array<Record<string, unknown>> = [];
      for (let i = 0; i < 25; i++) rows.push({ tag: `tag${i}` });
      // Add a clear inconsistency for the 26th distinct group — but we exceed the cap.
      rows.push({ tag: "tag1 " }); // would collide with "tag1" if checked
      const issues = profileDataset({ columns: ["tag"], rows }, { inconsistentLabelMaxCardinality: 20 });
      expect(issues.find(i => i.issueType === "inconsistent_label")).toBeUndefined();
    });

    test("disabled when inconsistentLabelMaxCardinality = 0", () => {
      const ds: DatasetView = {
        columns: ["g"],
        rows: [{ g: "A" }, { g: "a" }, { g: "A" }],
      };
      const issues = profileDataset(ds, { inconsistentLabelMaxCardinality: 0 });
      expect(issues.find(i => i.issueType === "inconsistent_label")).toBeUndefined();
    });

    test("does not flag when all spellings agree (genuine low-cardinality categorical)", () => {
      const ds: DatasetView = {
        columns: ["group"],
        rows: [{ group: "A" }, { group: "A" }, { group: "A" }, { group: "B" }, { group: "B" }],
      };
      const issues = profileDataset(ds);
      expect(issues.find(i => i.issueType === "inconsistent_label")).toBeUndefined();
    });
  });

  // ---------------------------------------------------------------------------
  // rowFingerprint — the contract that lets the frontend `findRowByKey` match
  // a profiler-emitted key against rows whose display-order has been shuffled
  // by Texera's multi-worker JSONL scan. Lock the algorithm down here.
  // ---------------------------------------------------------------------------
  describe("rowFingerprint", () => {
    test("identical content → identical key", () => {
      const a = rowFingerprint({ age: 25, name: "Alice" }, ["age", "name"]);
      const b = rowFingerprint({ age: 25, name: "Alice" }, ["age", "name"]);
      expect(a).toBe(b);
    });

    test("different content → different key", () => {
      const a = rowFingerprint({ age: 25, name: "Alice" }, ["age", "name"]);
      const b = rowFingerprint({ age: 26, name: "Alice" }, ["age", "name"]);
      expect(a).not.toBe(b);
    });

    test("column order in the input is canonicalized (sort)", () => {
      // Same row content, two different schema orderings — should match.
      const a = rowFingerprint({ age: 25, name: "Alice" }, ["age", "name"]);
      const b = rowFingerprint({ age: 25, name: "Alice" }, ["name", "age"]);
      expect(a).toBe(b);
    });

    test("missing key and explicit null fingerprint identically", () => {
      const a = rowFingerprint({ age: null, name: "Alice" }, ["age", "name"]);
      // Note: `age` key not present on the second row at all.
      const b = rowFingerprint({ name: "Alice" } as Record<string, unknown>, ["age", "name"]);
      expect(a).toBe(b);
    });

    test("undefined value treated as null", () => {
      const a = rowFingerprint({ age: undefined, name: "Alice" }, ["age", "name"]);
      const b = rowFingerprint({ age: null, name: "Alice" }, ["age", "name"]);
      expect(a).toBe(b);
    });

    test("JSON-special characters survive round-trip (quotes, backslashes, unicode)", () => {
      // The contract is "String() then JSON.stringify", so the only thing to
      // verify here is that the helper does *use* JSON.stringify (after the
      // String() coercion that's a no-op on strings) — otherwise quote
      // escaping would be lost and a string containing a field separator
      // would mis-fingerprint. For strings, String(s) === s, so the expected
      // output is JSON.stringify(s) per cell.
      const row = { label: 'he said "hi"\\nbye', emoji: "🎉" };
      const expected = JSON.stringify("🎉") + JSON.stringify('he said "hi"\\nbye');
      // canonical column order is alphabetical: emoji, label
      expect(rowFingerprint(row, ["label", "emoji"])).toBe(expected);
    });

    test("numbers and numeric strings ARE equivalent after String() coercion", () => {
      // Texera's JSONL scan widens mixed-type columns to String (via
      // `parseField(stringValue, schemaType)` in JSONLScanSourceOpExec), while
      // DataGuard's parseJsonl preserves native JSON types. To make matches
      // survive that schema-widening, both sides String()-coerce the cell
      // before JSON.stringify — so `25` and `"25"` fingerprint identically.
      const a = rowFingerprint({ x: 25 }, ["x"]);
      const b = rowFingerprint({ x: "25" }, ["x"]);
      expect(a).toBe(b);
      // And the token shape is the quoted-string form.
      expect(a).toBe('"25"');
    });

    test("floats fingerprint identically whether typed as number or string", () => {
      // IEEE-754 ToString (ECMA-262 §7.1.17) is the same on V8 and Bun, so
      // `String(28.1)` is "28.1" on both. The string side is trivially "28.1".
      const a = rowFingerprint({ x: 28.1 }, ["x"]);
      const b = rowFingerprint({ x: "28.1" }, ["x"]);
      expect(a).toBe(b);
      expect(a).toBe('"28.1"');
    });

    // Cross-language fingerprint contract: this is the *exact* string the
    // frontend `findRowByKey` must produce for the same input. If the JSON
    // tokens here drift between V8 and Bun, this test catches it before
    // production. Format: JSON.stringify(String(value)) per non-null cell;
    // null/undefined → bare `null` literal (no quotes).
    test("contract example: known input produces known output (V8/Bun parity)", () => {
      const row = { glucose: 180, patient_id: "p-7", group: null };
      // canonical sort: glucose, group, patient_id
      const key = rowFingerprint(row, ["patient_id", "group", "glucose"]);
      // glucose: JSON.stringify(String(180)) = "\"180\""
      // group:   null                         = "null"
      // patient_id: JSON.stringify(String("p-7")) = "\"p-7\""
      expect(key).toBe('"180"' + "null" + '"p-7"');
    });

    // Round-6 regression — JSONL null-cell locate bug.
    //
    // Texera's `JSONLScanSourceOpExec` reads each line via Jackson and pipes
    // it through `JSONUtils.JSONToMap`, which calls `JsonNode#asText()` on
    // every value node. For a JsonNullNode, Jackson's `asText()` returns the
    // literal STRING `"null"` (4 chars n-u-l-l), not Java null. So when the
    // result panel renders a row that the source file had as `{score: null}`,
    // the cell value the frontend sees is the string `"null"`, while the
    // profiler-side parseJsonl preserved a real JS `null`.
    //
    // Pre-fix the profiler emitted bare `null` for the cell, the frontend
    // emitted `"\"null\""` (the quoted form), the fingerprints diverged,
    // `findRowByKey` missed every row, and the silent index-fallback path
    // flashed whatever shuffled display row sat at the byte-order index.
    //
    // The fix collapses both representations to the bare `null` token via
    // the shared `isMissing` predicate so the two sides agree.
    test('regression: explicit-null cell and Jackson-asText `"null"` string fingerprint identically (JSONL round 6)', () => {
      const profilerRow = { score: null as unknown, user: "Grace" };
      const texeraRow = { score: "null", user: "Grace" };
      const a = rowFingerprint(profilerRow, ["score", "user"]);
      const b = rowFingerprint(texeraRow, ["score", "user"]);
      expect(a).toBe(b);
    });

    test("regression: standard missing-token spellings all fingerprint to the bare null token", () => {
      const expected = rowFingerprint({ x: null }, ["x"]);
      for (const token of ["null", "NULL", "Null", "NA", "n/a", "N/A", "None", "NONE", "nan", "NaN", "", "  "]) {
        expect(rowFingerprint({ x: token }, ["x"])).toBe(expected);
      }
    });
  });

  // ---------------------------------------------------------------------------
  // Per-detector affectedRowKeys emission — must be 1-to-1 aligned with
  // affectedRowIndices, and absent when indices are absent (large-issue path).
  // ---------------------------------------------------------------------------
  describe("affectedRowKeys integration", () => {
    test("missing-value issue carries keys aligned with indices", () => {
      const ds: DatasetView = {
        columns: ["age", "name"],
        rows: [
          { age: 25, name: "Alice" },
          { age: null, name: "Bob" },
          { age: 30, name: "Carol" },
        ],
      };
      const issues = profileDataset(ds);
      const miss = issues.find(i => i.issueType === "missing_value" && i.column === "age");
      expect(miss!.affectedRowIndices).toEqual([1]);
      expect(miss!.affectedRowKeys).toHaveLength(1);
      // Re-fingerprint the same row to confirm match.
      expect(miss!.affectedRowKeys![0]).toBe(rowFingerprint(ds.rows[1], ds.columns));
    });

    test("large-issue path omits both indices and keys", () => {
      // Force the missing-value detector well over the cap, then assert that
      // neither indices nor keys are emitted — preserves the existing
      // maybeIndices behaviour.
      const rows = Array.from({ length: 100 }, () => ({ x: null }));
      const issues = profileDataset({ columns: ["x"], rows }, { maxIndicesInIssue: 10 });
      const miss = issues.find(i => i.issueType === "missing_value");
      expect(miss!.affectedRowIndices).toBeUndefined();
      expect(miss!.affectedRowKeys).toBeUndefined();
    });
  });
});
