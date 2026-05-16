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

import { vi } from "vitest";
import { parseJsonl } from "./data-guard-jsonl";

function jsonlBlob(text: string): Blob {
  return new Blob([text], { type: "application/jsonl" });
}

describe("parseJsonl", () => {
  it("parses a single JSON object line into one row with the expected column", async () => {
    const { columns, rows } = await parseJsonl(jsonlBlob('{"a":1,"b":"x"}'), "single.jsonl");
    expect(columns).toEqual(["a", "b"]);
    expect(rows).toEqual([{ a: 1, b: "x" }]);
  });

  it("returns empty columns and rows for an empty file", async () => {
    const { columns, rows } = await parseJsonl(jsonlBlob(""), "empty.jsonl");
    expect(columns).toEqual([]);
    expect(rows).toEqual([]);
  });

  it("skips blank lines and a trailing newline silently", async () => {
    const { columns, rows } = await parseJsonl(jsonlBlob('{"a":1}\n\n{"a":2}\n   \n{"a":3}\n'), "blanks.jsonl");
    expect(columns).toEqual(["a"]);
    expect(rows).toEqual([{ a: 1 }, { a: 2 }, { a: 3 }]);
  });

  it("tolerates CRLF line endings", async () => {
    const { rows } = await parseJsonl(jsonlBlob('{"a":1}\r\n{"a":2}\r\n'), "crlf.jsonl");
    expect(rows).toEqual([{ a: 1 }, { a: 2 }]);
  });

  it("flattens nested objects into dot-notation columns", async () => {
    const { columns, rows } = await parseJsonl(
      jsonlBlob('{"id":1,"address":{"street":"Main","city":"Irvine"}}'),
      "nested.jsonl"
    );
    expect(columns).toEqual(["id", "address.street", "address.city"]);
    expect(rows[0]).toEqual({ id: 1, "address.street": "Main", "address.city": "Irvine" });
  });

  it("stringifies arrays as a single cell instead of exploding rows", async () => {
    const { columns, rows } = await parseJsonl(jsonlBlob('{"tags":["a","b","c"]}'), "arr.jsonl");
    expect(columns).toEqual(["tags"]);
    expect(rows).toEqual([{ tags: '["a","b","c"]' }]);
  });

  it("skips a bare-string line with a console warning", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { rows } = await parseJsonl(jsonlBlob('"just a string"\n{"a":1}'), "bare.jsonl");
    expect(rows).toEqual([{ a: 1 }]);
    expect(warn).toHaveBeenCalled();
  });

  it("skips a bare-number, top-level-array, and boolean line with warnings", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { rows } = await parseJsonl(jsonlBlob('42\n[1,2,3]\ntrue\n{"a":1}'), "nonobjects.jsonl");
    expect(rows).toEqual([{ a: 1 }]);
    expect(warn).toHaveBeenCalled();
  });

  it("fills missing keys in a row with null when other rows have that key", async () => {
    const { columns, rows } = await parseJsonl(jsonlBlob('{"a":1,"b":2}\n{"a":3}'), "ragged.jsonl");
    expect(columns).toEqual(["a", "b"]);
    expect(rows).toEqual([
      { a: 1, b: 2 },
      { a: 3, b: null },
    ]);
  });

  it("uses union-of-keys column ordering (first-seen wins)", async () => {
    const { columns } = await parseJsonl(jsonlBlob('{"b":1}\n{"a":2}\n{"c":3,"a":4}'), "order.jsonl");
    expect(columns).toEqual(["b", "a", "c"]);
  });

  it("handles a 100-line input with consistent shape", async () => {
    const lines: string[] = [];
    for (let i = 0; i < 100; i++) {
      lines.push(JSON.stringify({ idx: i, kind: i % 2 === 0 ? "even" : "odd" }));
    }
    const { columns, rows } = await parseJsonl(jsonlBlob(lines.join("\n")), "big.jsonl");
    expect(columns).toEqual(["idx", "kind"]);
    expect(rows.length).toBe(100);
    expect(rows[0]).toEqual({ idx: 0, kind: "even" });
    expect(rows[99]).toEqual({ idx: 99, kind: "odd" });
  });

  it("does not throw on lines that aren't valid JSON; it warns and continues", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { rows } = await parseJsonl(jsonlBlob('{"a":1}\nnot json at all\n{"a":2}'), "broken.jsonl");
    expect(rows).toEqual([{ a: 1 }, { a: 2 }]);
    expect(warn).toHaveBeenCalled();
  });

  it("prefers nested-key value on column-name collision and warns once", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    // Top-level literal "address.street" key appears first, nested second:
    // the nested {address:{street}} value must win regardless of order.
    const { columns, rows } = await parseJsonl(
      jsonlBlob('{"address.street":"OLD","address":{"street":"NEW"}}'),
      "collide.jsonl"
    );
    expect(columns).toEqual(["address.street"]);
    expect(rows).toEqual([{ "address.street": "NEW" }]);
    expect(warn).toHaveBeenCalled();
  });

  it("prefers nested-key value even when nested appears FIRST in source order (F2 regression)", async () => {
    // Round-2 reviewer found that the previous last-write-wins implementation
    // produced "OLD" here because the literal-dotted key was iterated after
    // the nested object. The two-pass fix must make nested win in BOTH source
    // orderings.
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { columns, rows } = await parseJsonl(
      jsonlBlob('{"address":{"street":"NEW"},"address.street":"OLD"}'),
      "collide-reverse.jsonl"
    );
    expect(columns).toEqual(["address.street"]);
    expect(rows).toEqual([{ "address.street": "NEW" }]);
    expect(warn).toHaveBeenCalled();
  });

  it("collision warning fires only once per colliding path across many rows", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const lines = ['{"a":{"b":1},"a.b":99}', '{"a":{"b":2},"a.b":99}', '{"a":{"b":3},"a.b":99}'].join("\n");
    const { rows } = await parseJsonl(jsonlBlob(lines), "collide-many.jsonl");
    expect(rows).toEqual([{ "a.b": 1 }, { "a.b": 2 }, { "a.b": 3 }]);
    // Exactly one collision warning for "a.b" across all three rows.
    const collisionWarns = warn.mock.calls.filter(
      args => typeof args[0] === "string" && args[0].includes('collision on "a.b"')
    );
    expect(collisionWarns.length).toBe(1);
  });
});
