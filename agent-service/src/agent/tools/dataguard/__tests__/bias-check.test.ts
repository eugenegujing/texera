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
import { computeBiasCheck } from "../bias-check";
import type { DatasetView } from "../dataset";

describe("computeBiasCheck", () => {
  test("identical before/after → 100% retention, no skew", () => {
    const ds: DatasetView = {
      columns: ["group"],
      rows: [{ group: "A" }, { group: "A" }, { group: "B" }, { group: "B" }],
    };
    const r = computeBiasCheck(ds, ds, "group");
    expect(r.skewDetected).toBe(false);
    expect(r.perGroup.A.retentionPct).toBe(100);
    expect(r.perGroup.B.retentionPct).toBe(100);
  });

  test("flags skew when one group loses much more than another", () => {
    const before: DatasetView = {
      columns: ["group"],
      rows: [
        { group: "A" }, { group: "A" }, { group: "A" }, { group: "A" }, { group: "A" },
        { group: "B" }, { group: "B" }, { group: "B" }, { group: "B" }, { group: "B" },
      ],
    };
    const after: DatasetView = {
      columns: ["group"],
      rows: [
        { group: "A" },
        { group: "B" }, { group: "B" }, { group: "B" }, { group: "B" }, { group: "B" },
      ],
    };
    // A retains 20%, B retains 100% → 80-point gap → skew
    const r = computeBiasCheck(before, after, "group");
    expect(r.skewDetected).toBe(true);
    expect(r.perGroup.A.retentionPct).toBe(20);
    expect(r.perGroup.B.retentionPct).toBe(100);
  });

  test("balanced cleanup (5%/4% loss across groups) → no skew", () => {
    // Mirrors the §5 storyboard closing beat — 4-5% loss per group.
    const before: DatasetView = {
      columns: ["group"],
      rows: Array.from({ length: 200 }, (_, i) => ({ group: i < 100 ? "A" : "B" })),
    };
    const after: DatasetView = {
      columns: ["group"],
      rows: [
        ...Array.from({ length: 96 }, () => ({ group: "A" })),
        ...Array.from({ length: 95 }, () => ({ group: "B" })),
      ],
    };
    const r = computeBiasCheck(before, after, "group");
    expect(r.skewDetected).toBe(false);
    expect(Math.round(r.perGroup.A.retentionPct)).toBe(96);
    expect(Math.round(r.perGroup.B.retentionPct)).toBe(95);
  });

  test("groupColumn missing from dataset: returns empty perGroup, no crash", () => {
    const ds: DatasetView = { columns: ["x"], rows: [{ x: 1 }] };
    const r = computeBiasCheck(ds, ds, "group");
    expect(r.perGroup).toEqual({});
    expect(r.skewDetected).toBe(false);
  });

  test("custom skewThreshold widens / narrows the trigger", () => {
    const before: DatasetView = {
      columns: ["g"],
      rows: Array.from({ length: 100 }, (_, i) => ({ g: i < 50 ? "A" : "B" })),
    };
    const after: DatasetView = {
      columns: ["g"],
      rows: [
        ...Array.from({ length: 45 }, () => ({ g: "A" })), // 90%
        ...Array.from({ length: 40 }, () => ({ g: "B" })), // 80%
      ],
    };
    // 10-point gap: skew with threshold=5, no skew with threshold=15
    expect(computeBiasCheck(before, after, "g", { skewThresholdPct: 5 }).skewDetected).toBe(true);
    expect(computeBiasCheck(before, after, "g", { skewThresholdPct: 15 }).skewDetected).toBe(false);
  });

  test("empty before → no groups, no skew", () => {
    const r = computeBiasCheck({ columns: [], rows: [] }, { columns: [], rows: [] }, "g");
    expect(r.perGroup).toEqual({});
    expect(r.skewDetected).toBe(false);
  });
});
