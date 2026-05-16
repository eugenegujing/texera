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

// Verification re-scan contract: POST /apply-batch must re-run profileDataset
// on the cleaned dataset and return residualIssues. The UI uses this to show
// users honest residue instead of silently claiming success.

import { beforeEach, describe, expect, test } from "bun:test";
import { buildApp, _resetAgentStoreForTests, _getAgentForTests } from "../../../../server";
import { env } from "../../../../config/env";
import type { DataQualityIssue } from "../../../../types/dataguard";

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

describe(`POST ${API}/agents/:id/dataguard/apply-batch — verification re-scan`, () => {
  test("response includes residualIssues + residualCount fields", async () => {
    const id = await createAgent();
    const agent = _getAgentForTests(id)!;
    agent.getDataGuardSession().setDataset({
      columns: ["x"],
      rows: [{ x: 1 }],
    });
    const res = await postJson(`${API}/agents/${id}/dataguard/apply-batch`, {
      decisions: [],
    });
    expect(res.status).toBe(200);
    const body = await readJson<{ residualIssues: unknown; residualCount: unknown }>(res);
    expect(Array.isArray(body.residualIssues)).toBe(true);
    expect(typeof body.residualCount).toBe("number");
  });

  test("residualIssues empty when cleaned dataset has nothing left to flag", async () => {
    const id = await createAgent();
    const agent = _getAgentForTests(id)!;
    const session = agent.getDataGuardSession();
    // Pristine dataset → no proposals to apply, profiler finds nothing.
    session.setDataset({
      columns: ["age"],
      rows: [{ age: 30 }, { age: 40 }, { age: 50 }],
    });
    const res = await postJson(`${API}/agents/${id}/dataguard/apply-batch`, {
      decisions: [],
    });
    const body = await readJson<{ residualCount: number; residualIssues: DataQualityIssue[] }>(res);
    expect(body.residualCount).toBe(0);
    expect(body.residualIssues).toEqual([]);
  });

  test("residualIssues surfaces leftovers when proposals leave data dirty", async () => {
    const id = await createAgent();
    const agent = _getAgentForTests(id)!;
    const session = agent.getDataGuardSession();
    // Dataset with a placeholder "999" the user denied — re-scan should still
    // flag it because nothing was fixed.
    session.setDataset({
      columns: ["age"],
      rows: [{ age: 30 }, { age: 999 }, { age: 40 }],
    });
    const res = await postJson(`${API}/agents/${id}/dataguard/apply-batch`, {
      decisions: [],
    });
    const body = await readJson<{ residualCount: number; residualIssues: DataQualityIssue[] }>(res);
    expect(body.residualCount).toBeGreaterThan(0);
    expect(body.residualIssues.some(i => i.issueType === "placeholder_value")).toBe(true);
  });
});
