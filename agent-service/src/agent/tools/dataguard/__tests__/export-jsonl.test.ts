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

// Route shape contract for GET /dataguard/export-jsonl. The frontend
// write-back path uses this when the source operator is JSONLFileScan.
// Each line is a JSON object whose keys follow `dataset.columns` order;
// missing/null cells round-trip as JSON `null`.

import { beforeEach, describe, expect, test } from "bun:test";
import { buildApp, _resetAgentStoreForTests, _getAgentForTests } from "../../../../server";
import { env } from "../../../../config/env";

const API = env.API_PREFIX;
const app = buildApp();

function url(path: string): string {
  return `http://localhost${path}`;
}

async function postJson(path: string, body: unknown): Promise<Response> {
  return app.handle(
    new Request(url(path), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
  );
}

async function getRaw(path: string): Promise<Response> {
  return app.handle(new Request(url(path), { method: "GET" }));
}

async function createAgent(): Promise<string> {
  const res = await postJson(`${API}/agents`, { modelType: "test-model" });
  const body = (await res.json()) as { id: string };
  return body.id;
}

function parseJsonlBody(text: string): unknown[] {
  return text
    .split("\n")
    .filter(l => l.length > 0)
    .map(l => JSON.parse(l));
}

beforeEach(() => {
  _resetAgentStoreForTests();
});

describe(`GET ${API}/agents/:id/dataguard/export-jsonl`, () => {
  test("404 when no dataset is loaded", async () => {
    const id = await createAgent();
    const res = await getRaw(`${API}/agents/${id}/dataguard/export-jsonl`);
    expect(res.status).toBe(404);
  });

  test("empty session (columns set, zero rows) returns empty body", async () => {
    const id = await createAgent();
    const agent = _getAgentForTests(id)!;
    agent.getDataGuardSession().setDataset({ columns: ["a", "b"], rows: [] });
    const res = await getRaw(`${API}/agents/${id}/dataguard/export-jsonl`);
    expect(res.status).toBe(200);
    const text = await res.text();
    expect(text).toBe("");
    expect(res.headers.get("content-type") || "").toContain("application/x-ndjson");
  });

  test("multi-row session emits one JSON object per line in columns order", async () => {
    const id = await createAgent();
    const agent = _getAgentForTests(id)!;
    agent.getDataGuardSession().setDataset({
      columns: ["id", "name"],
      rows: [
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
      ],
    });
    const res = await getRaw(`${API}/agents/${id}/dataguard/export-jsonl`);
    expect(res.status).toBe(200);
    const text = await res.text();
    // Trailing newline so the file is canonical-JSONL.
    expect(text.endsWith("\n")).toBe(true);
    const lines = text.split("\n").filter(l => l.length > 0);
    expect(lines.length).toBe(2);
    // Key order must follow `columns`, not insertion order of the row map.
    expect(lines[0]).toBe(`{"id":1,"name":"Alice"}`);
    expect(lines[1]).toBe(`{"id":2,"name":"Bob"}`);
  });

  test("null cells round-trip as JSON null, not omitted", async () => {
    const id = await createAgent();
    const agent = _getAgentForTests(id)!;
    agent.getDataGuardSession().setDataset({
      columns: ["a", "b"],
      rows: [
        { a: 1, b: null },
        { a: null, b: 2 },
      ],
    });
    const res = await getRaw(`${API}/agents/${id}/dataguard/export-jsonl`);
    const text = await res.text();
    const rows = parseJsonlBody(text) as Array<Record<string, unknown>>;
    expect(rows).toEqual([
      { a: 1, b: null },
      { a: null, b: 2 },
    ]);
  });

  test("missing keys on a row are emitted as JSON null (not dropped)", async () => {
    const id = await createAgent();
    const agent = _getAgentForTests(id)!;
    agent.getDataGuardSession().setDataset({
      columns: ["a", "b"],
      // Second row omits `b` entirely (undefined). Must surface as null so
      // the column doesn't silently disappear from that row's output.
      rows: [{ a: 1, b: 2 }, { a: 3 }],
    });
    const res = await getRaw(`${API}/agents/${id}/dataguard/export-jsonl`);
    const text = await res.text();
    const rows = parseJsonlBody(text) as Array<Record<string, unknown>>;
    expect(rows).toEqual([
      { a: 1, b: 2 },
      { a: 3, b: null },
    ]);
  });

  test("values with newlines and quotes are escaped by JSON encoding", async () => {
    const id = await createAgent();
    const agent = _getAgentForTests(id)!;
    agent.getDataGuardSession().setDataset({
      columns: ["text"],
      rows: [{ text: 'line1\nline2 with "quotes"' }, { text: "tab\there" }],
    });
    const res = await getRaw(`${API}/agents/${id}/dataguard/export-jsonl`);
    const text = await res.text();
    const lines = text.split("\n").filter(l => l.length > 0);
    // Each line must itself be one valid JSON object — embedded \n in the
    // value must NOT split the row across multiple JSONL lines.
    expect(lines.length).toBe(2);
    const rows = lines.map(l => JSON.parse(l)) as Array<Record<string, unknown>>;
    expect(rows[0]).toEqual({ text: 'line1\nline2 with "quotes"' });
    expect(rows[1]).toEqual({ text: "tab\there" });
  });
});
