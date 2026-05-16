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

// Contract tests for cutting the Modify verdict (task #11a / #15) and for
// the remember-flag scope rule (task #12). The HTTP body schema on
// POST /api/agents/:id/dataguard/apply-batch must:
//
//   • reject verdict: "modify"
//   • reject the modifiedAction field (no longer part of the contract)
//   • reject { verdict: "deny", remember: true } — remember is only meaningful for "allow"
//   • still accept { verdict: "allow", remember: true } and { verdict: "deny" }
//
// All assertions are at the schema layer (Elysia body validation runs before
// the handler), so we don't need a real loaded dataset or LLM-derived
// proposals to exercise them.

import { beforeEach, describe, expect, test } from "bun:test";
import { buildApp, _resetAgentStoreForTests } from "../../../../server";
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

async function readJson<T = unknown>(res: Response): Promise<T> {
  return (await res.json()) as T;
}

async function createAgent(): Promise<string> {
  const res = await postJson(`${API}/agents`, { modelType: "test-model" });
  const body = await readJson<{ id: string }>(res);
  return body.id;
}

beforeEach(() => {
  _resetAgentStoreForTests();
});

describe(`POST ${API}/agents/:id/dataguard/apply-batch — Modify verdict cut (#11a)`, () => {
  test("rejects verdict: \"modify\" with a 4xx body-schema error", async () => {
    const id = await createAgent();
    const res = await postJson(`${API}/agents/${id}/dataguard/apply-batch`, {
      decisions: [{ issueId: "iss-1", verdict: "modify" }],
    });
    expect(res.status).toBeGreaterThanOrEqual(400);
    expect(res.status).toBeLessThan(500);
  });

  test("rejects unknown field `modifiedAction` on a decision entry", async () => {
    const id = await createAgent();
    const res = await postJson(`${API}/agents/${id}/dataguard/apply-batch`, {
      decisions: [
        { issueId: "iss-1", verdict: "allow", modifiedAction: "Flag instead of replace" },
      ],
    });
    expect(res.status).toBeGreaterThanOrEqual(400);
    expect(res.status).toBeLessThan(500);
  });

  test("still accepts verdict: \"allow\" (baseline — parity check that the cut didn't over-reach)", async () => {
    const id = await createAgent();
    const res = await postJson(`${API}/agents/${id}/dataguard/apply-batch`, {
      decisions: [{ issueId: "iss-not-loaded", verdict: "allow" }],
    });
    // No proposal recorded for this issueId, so the handler returns 200 with
    // a per-result error string — the SCHEMA accepted the body, which is the
    // point of this test.
    expect(res.status).toBe(200);
  });

  test("still accepts verdict: \"deny\" (baseline)", async () => {
    const id = await createAgent();
    const res = await postJson(`${API}/agents/${id}/dataguard/apply-batch`, {
      decisions: [{ issueId: "iss-not-loaded", verdict: "deny" }],
    });
    expect(res.status).toBe(200);
  });

  test("rejects a mixed batch where ANY decision uses verdict: \"modify\"", async () => {
    const id = await createAgent();
    const res = await postJson(`${API}/agents/${id}/dataguard/apply-batch`, {
      decisions: [
        { issueId: "iss-1", verdict: "allow" },
        { issueId: "iss-2", verdict: "modify" },
        { issueId: "iss-3", verdict: "deny" },
      ],
    });
    expect(res.status).toBeGreaterThanOrEqual(400);
    expect(res.status).toBeLessThan(500);
  });
});

describe(`POST ${API}/agents/:id/dataguard/apply-batch — remember flag scope (#12)`, () => {
  test("rejects { verdict: \"deny\", remember: true } — remember only applies to allow", async () => {
    const id = await createAgent();
    const res = await postJson(`${API}/agents/${id}/dataguard/apply-batch`, {
      decisions: [{ issueId: "iss-1", verdict: "deny", remember: true }],
    });
    expect(res.status).toBeGreaterThanOrEqual(400);
    expect(res.status).toBeLessThan(500);
  });

  test("accepts { verdict: \"allow\", remember: true } (baseline)", async () => {
    const id = await createAgent();
    const res = await postJson(`${API}/agents/${id}/dataguard/apply-batch`, {
      decisions: [{ issueId: "iss-not-loaded", verdict: "allow", remember: true }],
    });
    // Same as above — handler can't find the proposal but the schema accepted.
    expect(res.status).toBe(200);
  });

  test("accepts { verdict: \"deny\", remember: false } — only `remember: true` + deny is the forbidden combo", async () => {
    const id = await createAgent();
    const res = await postJson(`${API}/agents/${id}/dataguard/apply-batch`, {
      decisions: [{ issueId: "iss-not-loaded", verdict: "deny", remember: false }],
    });
    expect(res.status).toBe(200);
  });

  test("accepts { verdict: \"deny\" } with `remember` omitted entirely", async () => {
    const id = await createAgent();
    const res = await postJson(`${API}/agents/${id}/dataguard/apply-batch`, {
      decisions: [{ issueId: "iss-not-loaded", verdict: "deny" }],
    });
    expect(res.status).toBe(200);
  });
});
