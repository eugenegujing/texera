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

import { Elysia, t } from "elysia";
import { cors } from "@elysiajs/cors";
import { createOpenAI } from "@ai-sdk/openai";
import { TexeraAgent } from "./agent/texera-agent";
import { getBackendConfig } from "./api/backend-api";
import { extractUserFromToken, validateToken } from "./api/auth-api";
import { retrieveWorkflow } from "./api/workflow-api";
import { WorkflowSystemMetadata } from "./agent/util/workflow-system-metadata";
import { env } from "./config/env";
import { createLogger } from "./logger";

const log = createLogger("Server");
const wsLog = createLogger("WS");
import type { DataQualityIssue } from "./types/dataguard";
import type {
  AgentInfo,
  AgentDelegateConfig,
  CreateAgentRequest,
  UpdateAgentSettingsRequest,
  AgentSettingsApi,
  ReActStep,
} from "./types/agent";
import { OperatorResultSerializationMode } from "./types/agent";

const agentStore = new Map<string, TexeraAgent>();
let agentCounter = 0;

async function createAgentInstance(
  modelType: string,
  customName?: string,
  delegateConfig?: AgentDelegateConfig
): Promise<{ agentId: string; agent: TexeraAgent }> {
  const agentId = `agent-${++agentCounter}`;
  const config = getBackendConfig();

  const openai = createOpenAI({
    baseURL: `${config.modelsEndpoint}/api`,
    apiKey: env.LLM_API_KEY,
  });

  // Reasoning effort variants are configured as separate model entries in litellm-config.yaml
  // with extra_body to inject reasoning_effort, bypassing LiteLLM's param validation.
  const agent = new TexeraAgent({
    model: openai.chat(modelType),
    modelType,
    agentId,
    agentName: customName || "Bob",
  });

  await agent.initialize();

  if (delegateConfig?.workflowId && delegateConfig.userToken) {
    try {
      const workflow = await retrieveWorkflow(delegateConfig.userToken, delegateConfig.workflowId);
      delegateConfig.workflowName = workflow.name;

      const workflowState = agent.getWorkflowState();
      workflowState.setWorkflowContent(workflow.content);

      agent.setDelegateConfig({
        userToken: delegateConfig.userToken,
        userInfo: delegateConfig.userInfo,
        workflowId: delegateConfig.workflowId,
        workflowName: delegateConfig.workflowName,
        computingUnitId: delegateConfig.computingUnitId,
      });

      log.info({ agentId, workflowId: delegateConfig.workflowId }, "loaded workflow for agent");
    } catch (error) {
      log.warn({ agentId, workflowId: delegateConfig.workflowId, err: error }, "failed to load workflow");
    }
  }

  agentStore.set(agentId, agent);
  log.info({ agentId, delegate: !!delegateConfig }, "created agent");

  return { agentId, agent };
}

function getAgentInfo(agentId: string, agent: TexeraAgent): AgentInfo {
  const agentSettings = agent.getSettings();
  const settingsApi: AgentSettingsApi = {
    maxOperatorResultCharLimit: agentSettings.maxOperatorResultCharLimit,
    maxOperatorResultCellCharLimit: agentSettings.maxOperatorResultCellCharLimit,
    operatorResultSerializationMode: agentSettings.operatorResultSerializationMode,
    toolTimeoutSeconds: Math.round(agentSettings.toolTimeoutMs / 1000),
    executionTimeoutMinutes: Math.round(agentSettings.executionTimeoutMs / 60000),
    disabledTools: Array.from(agentSettings.disabledTools),
    maxSteps: agentSettings.maxSteps,
    allowedOperatorTypes: agentSettings.allowedOperatorTypes,
  };

  const delegateConfig = agent.getDelegateConfig();

  return {
    id: agentId,
    name: agent.agentName,
    modelType: agent.modelType,
    state: agent.getState(),
    createdAt: agent.createdAt,
    delegate: delegateConfig
      ? {
          userToken: "***",
          userInfo: delegateConfig.userInfo,
          workflowId: delegateConfig.workflowId,
          workflowName: delegateConfig.workflowName,
          computingUnitId: delegateConfig.computingUnitId,
        }
      : undefined,
    settings: settingsApi,
  };
}

function getAgent(agentId: string): TexeraAgent {
  const agent = agentStore.get(agentId);
  if (!agent) {
    throw new Error("Agent not found");
  }
  return agent;
}

// `normalize: false` keeps unknown fields in the parsed body so additionalProperties:false
// schemas can reject them (Elysia 1.4 strips by default otherwise — see #11a tests).
const agentsRouter = new Elysia({ prefix: "/agents", normalize: false })
  // Error handler must live on the same Elysia instance whose routes throw, or
  // its scope will not see the errors. Elysia 1.x defaults to local scoping for
  // .onError, so attach here rather than on the outer app.
  .onError(({ error, code, set }) => {
    log.error({ err: error }, "request error");
    // Elysia body-schema rejection — surface as 400 so callers can distinguish
    // bad input from server bugs. Without this, every typebox validation error
    // ends up as a 500 and the modify-cut tests can't tell whether the route
    // rejected the bad verdict or crashed.
    if (code === "VALIDATION") {
      set.status = 400;
      return { error: error instanceof Error ? error.message : String(error) };
    }
    const errorMessage = error instanceof Error ? error.message : String(error);
    if (errorMessage === "Agent not found") {
      set.status = 404;
      return { error: "Agent not found" };
    }
    if (errorMessage === "Invalid or expired token") {
      set.status = 401;
      return { error: "Invalid or expired token" };
    }
    if (errorMessage === "modelType is required") {
      set.status = 400;
      return { error: "modelType is required" };
    }
    set.status = 500;
    return { error: errorMessage || "Internal server error" };
  })
  .get("/", () => {
    const agentList = Array.from(agentStore.entries()).map(([id, agent]) => getAgentInfo(id, agent));
    return { agents: agentList };
  })

  .post(
    "/",
    async ({ body }) => {
      const { modelType, name, userToken, workflowId, computingUnitId, settings } = body as CreateAgentRequest;

      if (!modelType) {
        throw new Error("modelType is required");
      }

      let delegateConfig: AgentDelegateConfig | undefined;
      if (userToken) {
        if (!validateToken(userToken)) {
          throw new Error("Invalid or expired token");
        }

        const userInfo = extractUserFromToken(userToken);
        delegateConfig = {
          userToken,
          userInfo,
          workflowId,
          computingUnitId,
        };
      }

      const { agentId, agent } = await createAgentInstance(modelType, name, delegateConfig);

      if (settings) {
        log.info(
          {
            agentId,
            maxOperatorResultCharLimit: settings.maxOperatorResultCharLimit,
            maxOperatorResultCellCharLimit: settings.maxOperatorResultCellCharLimit,
          },
          "applying initial agent settings"
        );
        agent.updateSettings({
          maxOperatorResultCharLimit: settings.maxOperatorResultCharLimit,
          maxOperatorResultCellCharLimit: settings.maxOperatorResultCellCharLimit,
          operatorResultSerializationMode: settings.operatorResultSerializationMode
            ? (settings.operatorResultSerializationMode as OperatorResultSerializationMode)
            : undefined,
          toolTimeoutMs: settings.toolTimeoutSeconds ? settings.toolTimeoutSeconds * 1000 : undefined,
          executionTimeoutMs: settings.executionTimeoutMinutes ? settings.executionTimeoutMinutes * 60000 : undefined,
          disabledTools: settings.disabledTools ? new Set(settings.disabledTools) : undefined,
          maxSteps: settings.maxSteps,
          allowedOperatorTypes: settings.allowedOperatorTypes,
        });
      }

      return getAgentInfo(agentId, agent);
    },
    {
      body: t.Object({
        modelType: t.String(),
        name: t.Optional(t.String()),
        userToken: t.Optional(t.String()),
        workflowId: t.Optional(t.Number()),
        computingUnitId: t.Optional(t.Number()),
        settings: t.Optional(
          t.Object({
            maxOperatorResultCharLimit: t.Optional(t.Number()),
            maxOperatorResultCellCharLimit: t.Optional(t.Number()),
            operatorResultSerializationMode: t.Optional(t.Literal("tsv")),
            toolTimeoutSeconds: t.Optional(t.Number()),
            executionTimeoutMinutes: t.Optional(t.Number()),
            disabledTools: t.Optional(t.Array(t.String())),
            maxSteps: t.Optional(t.Number()),
            allowedOperatorTypes: t.Optional(t.Array(t.String())),
          })
        ),
      }),
    }
  )

  .get("/:id", ({ params: { id } }) => {
    const agent = getAgent(id);
    return {
      ...getAgentInfo(id, agent),
      workflow: agent.getWorkflowState().getWorkflowContent(),
      stepCount: agent.getReActSteps().length,
    };
  })

  .delete("/:id", ({ params: { id }, set }) => {
    const agent = agentStore.get(id);
    if (!agent) {
      set.status = 404;
      return { error: "Agent not found" };
    }

    agent.destroy();
    agentStore.delete(id);
    return { deleted: true };
  })

  .get("/:id/react-steps", ({ params: { id } }) => {
    const agent = getAgent(id);
    return { steps: agent.getReActSteps(), state: agent.getState() };
  })

  .get("/:id/operator-results", ({ params: { id } }) => {
    const agent = getAgent(id);
    return { results: getOperatorResultSummaries(agent) };
  })

  .post(
    "/:id/steps-by-operators",
    ({ params: { id }, body }) => {
      const agent = getAgent(id);
      const { operatorIds } = body;
      return { steps: agent.getReActStepsByOperatorIds(operatorIds || []) };
    },
    {
      body: t.Object({
        operatorIds: t.Array(t.String()),
      }),
    }
  )

  .get("/:id/system-info", ({ params: { id } }) => {
    const agent = getAgent(id);
    return agent.getSystemInfo();
  })

  .post("/:id/stop", ({ params: { id } }) => {
    const agent = getAgent(id);
    agent.stop();
    return { status: "stopping" };
  })

  .post("/:id/clear", ({ params: { id } }) => {
    const agent = getAgent(id);
    agent.clearHistory();
    return { status: "cleared" };
  })

  .post("/:id/checkout", ({ params: { id }, body }) => {
    const agent = getAgent(id);
    const { stepId } = body as { stepId: string };
    if (!stepId) throw new Error("stepId is required");

    const success = agent.checkout(stepId);
    if (!success) throw new Error(`Step ${stepId} not found or checkout failed`);

    const allSteps = agent.getAllSteps();
    const workflowContent = agent.getWorkflowState().getWorkflowContent();

    broadcastToAgent(id, {
      type: "headChange",
      headId: stepId,
      steps: allSteps,
      workflowContent,
      operatorResults: getOperatorResultSummaries(agent),
    });

    return {
      status: "checked out",
      headId: stepId,
    };
  })

  // ---------- DataGuard endpoints ----------

  .post(
    "/:id/dataguard/dataset",
    ({ params: { id }, body }) => {
      const agent = getAgent(id);
      agent.setDataGuardDataset({
        columns: body.columns,
        rows: body.rows,
      });
      return { ok: true, columns: body.columns.length, rows: body.rows.length };
    },
    {
      body: t.Object({
        columns: t.Array(t.String()),
        rows: t.Array(t.Record(t.String(), t.Any())),
      }),
    }
  )

  // Server-driven DataGuard scan. Runs profile_dataset + suggest_fix entirely
  // server-side (no chat / no LLM tool loop), returns a flat list of issues
  // each paired with a FixProposal. The checklist UI consumes this directly.
  //
  // Body (optional): { idColumn?, validRanges?, placeholderValues?, missingTokens? }
  // — same profile_dataset options, all optional.
  .post(
    "/:id/dataguard/scan",
    async ({ params: { id }, body }) => {
      const agent = getAgent(id);
      const session = agent.getDataGuardSession();
      const dataset = session.getDataset();
      if (!dataset) {
        return { error: "No dataset loaded. Call /dataguard/dataset first." };
      }
      const { profileDataset } = await import("./agent/tools/dataguard/profile-dataset");
      const { suggestFix } = await import("./agent/tools/dataguard/suggest-fix");
      const scanOptions = {
        idColumn: body?.idColumn,
        validRanges: body?.validRanges,
        placeholderValues: body?.placeholderValues,
        missingTokens: body?.missingTokens,
      };
      session.setScanOptions(scanOptions);
      const issues = profileDataset(dataset, scanOptions);
      for (const issue of issues) session.recordIssue(issue);

      // Generate a proposal per issue in parallel. Each calls the LLM once.
      const llmCall = (prompt: string) => agent.callLlm(prompt);
      const proposals = await Promise.all(
        issues.map(async issue => {
          try {
            const p = await suggestFix(issue, { llmCall });
            session.recordProposal(p);
            return { issueId: issue.issueId, proposal: p, error: null };
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            return { issueId: issue.issueId, proposal: null, error: msg };
          }
        })
      );
      return { issueCount: issues.length, issues, proposals };
    },
    {
      body: t.Optional(
        t.Object({
          idColumn: t.Optional(t.String()),
          validRanges: t.Optional(t.Record(t.String(), t.Object({ min: t.Number(), max: t.Number() }))),
          placeholderValues: t.Optional(t.Array(t.Union([t.String(), t.Number()]))),
          missingTokens: t.Optional(t.Array(t.String())),
        })
      ),
    }
  )

  // Apply a user-selected batch of FixProposals (the checklist UI sends this
  // when the user clicks "Apply Selected"). Each entry in `decisions` is one
  // checkbox row: { issueId, verdict, remember? }.
  .post(
    "/:id/dataguard/apply-batch",
    async ({ params: { id }, body, set }) => {
      const agent = getAgent(id);
      const session = agent.getDataGuardSession();
      const { applyFix } = await import("./agent/tools/dataguard/apply-fix");

      // `remember` only applies when the user approves a fix — it adds the
      // issueType to autoAllowRules so future similar fixes are pre-approved.
      // Pairing it with deny is nonsense and would silently teach the agent
      // an unintended rule, so we reject the whole batch (#12).
      const badRemember = body.decisions.find(d => d.verdict === "deny" && d.remember === true);
      if (badRemember) {
        set.status = 400;
        return {
          error: `decision for issueId="${badRemember.issueId}" combines verdict="deny" with remember=true; remember only applies to allow.`,
        };
      }

      // Belt-and-suspenders rejection of legacy fields (e.g., #11a's
      // `modifiedAction`). The typebox schema sets additionalProperties:false,
      // but Elysia's body parser sometimes strips unknown keys before
      // validation; this explicit check guarantees an honest 400.
      const KNOWN_KEYS = new Set(["issueId", "verdict", "remember"]);
      const rawBody = body as unknown as { decisions: Array<Record<string, unknown>> };
      for (let i = 0; i < rawBody.decisions.length; i++) {
        const entry = rawBody.decisions[i];
        const extras = Object.keys(entry).filter(k => !KNOWN_KEYS.has(k));
        if (extras.length > 0) {
          set.status = 400;
          return {
            error: `decision at index ${i} has unknown field(s): ${extras.join(", ")}. Allowed: issueId, verdict, remember.`,
          };
        }
      }

      const results: Array<{
        issueId: string;
        verdict: string;
        applied: boolean;
        rowsAffected: number;
        error?: string;
      }> = [];

      let dataset = session.getDataset();
      if (!dataset) return { error: "No dataset loaded." };

      for (const decision of body.decisions) {
        const proposal = session.getProposal(decision.issueId);
        if (!proposal) {
          results.push({
            issueId: decision.issueId,
            verdict: decision.verdict,
            applied: false,
            rowsAffected: 0,
            error: "no proposal for this issueId — call /scan first",
          });
          continue;
        }
        if (decision.verdict === "deny") {
          session.recordDecision({ proposal, verdict: "deny", applied: false });
          results.push({ issueId: decision.issueId, verdict: "deny", applied: false, rowsAffected: 0 });
          continue;
        }
        try {
          // Thread the session's scan-time missingTokens into applyFix so that
          // user-configured tokens (e.g. ["xyz"]) are treated as missing by
          // impute, matching what the profiler flagged.
          const out = applyFix(dataset, proposal, {
            missingTokens: session.getScanOptions().missingTokens,
          });
          dataset = out.dataset;
          session.updateDataset(dataset);
          session.recordDecision({
            proposal,
            verdict: decision.verdict,
            applied: true,
          });
          if (decision.remember) {
            session.addAutoAllowRule(proposal.issueType);
          }
          results.push({
            issueId: decision.issueId,
            verdict: decision.verdict,
            applied: true,
            rowsAffected: out.rowsAffected,
          });
        } catch (e: unknown) {
          const msg = e instanceof Error ? e.message : String(e);
          results.push({
            issueId: decision.issueId,
            verdict: decision.verdict,
            applied: false,
            rowsAffected: 0,
            error: msg,
          });
        }
      }
      // Verification pass: re-run the profiler on the cleaned dataset so the
      // UI can surface anything the proposals didn't actually fix. These are
      // genuine leftovers (denied fixes, anything impute couldn't compute,
      // etc.). The earlier split into "acknowledged" (flag-for-review) is
      // gone because the flag op kind is gone — every proposal now produces a
      // real concrete change.
      let residualIssues: DataQualityIssue[] = [];
      if (dataset) {
        const { profileDataset } = await import("./agent/tools/dataguard/profile-dataset");
        residualIssues = profileDataset(dataset, session.getScanOptions());
      }
      return {
        applied: results.filter(r => r.applied).length,
        denied: results.filter(r => r.verdict === "deny").length,
        failed: results.filter(r => !r.applied && r.verdict !== "deny").length,
        datasetRowCount: dataset?.rows.length ?? 0,
        results,
        residualIssues,
        residualCount: residualIssues.length,
      };
    },
    {
      body: t.Object({
        decisions: t.Array(
          t.Object(
            {
              issueId: t.String(),
              // "modify" was cut by #11a — body schema must reject it (the
              // legacy handler executed the original proposalParams anyway and
              // only logged the user's free-text, which silently lied to users).
              verdict: t.Union([t.Literal("allow"), t.Literal("deny")]),
              remember: t.Optional(t.Boolean()),
            },
            // Reject legacy fields like `modifiedAction` outright instead of
            // silently dropping them — callers that still send them are buggy.
            { additionalProperties: false }
          )
        ),
      }),
    }
  )

  // Return the in-memory cleaned dataset as a CSV blob. The frontend uses this
  // after "Apply selected" to upload the cleaned data back as a new dataset
  // version, then auto-runs the workflow.
  .get("/:id/dataguard/export-csv", ({ params: { id }, set }) => {
    const agent = getAgent(id);
    const dataset = agent.getDataGuardSession().getDataset();
    if (!dataset) {
      set.status = 404;
      return "No dataset loaded.";
    }
    const escape = (v: unknown): string => {
      if (v === null || v === undefined) return "";
      const s = String(v);
      return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const lines: string[] = [];
    lines.push(dataset.columns.map(escape).join(","));
    for (const row of dataset.rows) {
      lines.push(dataset.columns.map(c => escape(row[c])).join(","));
    }
    set.headers["content-type"] = "text/csv; charset=utf-8";
    return lines.join("\n");
  })

  .get("/:id/dataguard/session", ({ params: { id } }) => {
    const agent = getAgent(id);
    const session = agent.getDataGuardSession();
    const dataset = session.getDataset();
    return {
      datasetRowCount: dataset?.rows.length ?? 0,
      datasetColumnCount: dataset?.columns.length ?? 0,
      issues: session.getIssues(),
      decisionLog: session.getDecisionLog(),
      autoAllowRules: session.getAutoAllowRules(),
    };
  })

  .get("/:id/operator-types", ({ params: { id } }) => {
    const agent = getAgent(id);
    const metadataStore = agent.getMetadataStore();
    const allTypes = metadataStore.getAllOperatorTypes();
    return Object.entries(allTypes).map(([type, description]) => ({ type, description }));
  })

  .get("/:id/settings", ({ params: { id } }) => {
    const agent = getAgent(id);
    const agentSettings = agent.getSettings();
    return {
      maxOperatorResultCharLimit: agentSettings.maxOperatorResultCharLimit,
      maxOperatorResultCellCharLimit: agentSettings.maxOperatorResultCellCharLimit,
      operatorResultSerializationMode: agentSettings.operatorResultSerializationMode,
      toolTimeoutSeconds: Math.round(agentSettings.toolTimeoutMs / 1000),
      executionTimeoutMinutes: Math.round(agentSettings.executionTimeoutMs / 60000),
      disabledTools: Array.from(agentSettings.disabledTools),
      maxSteps: agentSettings.maxSteps,
      allowedOperatorTypes: agentSettings.allowedOperatorTypes,
    };
  })

  .patch(
    "/:id/settings",
    ({ params: { id }, body }) => {
      const agent = getAgent(id);
      const settings = body as UpdateAgentSettingsRequest;

      log.info(
        {
          agentId: id,
          maxOperatorResultCharLimit: settings.maxOperatorResultCharLimit,
          maxOperatorResultCellCharLimit: settings.maxOperatorResultCellCharLimit,
        },
        "updating agent settings"
      );

      agent.updateSettings({
        maxOperatorResultCharLimit: settings.maxOperatorResultCharLimit,
        maxOperatorResultCellCharLimit: settings.maxOperatorResultCellCharLimit,
        operatorResultSerializationMode: settings.operatorResultSerializationMode
          ? (settings.operatorResultSerializationMode as OperatorResultSerializationMode)
          : undefined,
        toolTimeoutMs: settings.toolTimeoutSeconds !== undefined ? settings.toolTimeoutSeconds * 1000 : undefined,
        executionTimeoutMs:
          settings.executionTimeoutMinutes !== undefined ? settings.executionTimeoutMinutes * 60000 : undefined,
        disabledTools: settings.disabledTools ? new Set(settings.disabledTools) : undefined,
        maxSteps: settings.maxSteps,
        allowedOperatorTypes: settings.allowedOperatorTypes,
      });

      const agentSettings = agent.getSettings();
      return {
        maxOperatorResultCharLimit: agentSettings.maxOperatorResultCharLimit,
        maxOperatorResultCellCharLimit: agentSettings.maxOperatorResultCellCharLimit,
        operatorResultSerializationMode: agentSettings.operatorResultSerializationMode,
        toolTimeoutSeconds: Math.round(agentSettings.toolTimeoutMs / 1000),
        executionTimeoutMinutes: Math.round(agentSettings.executionTimeoutMs / 60000),
        disabledTools: Array.from(agentSettings.disabledTools),
        maxSteps: agentSettings.maxSteps,
        allowedOperatorTypes: agentSettings.allowedOperatorTypes,
      };
    },
    {
      body: t.Object({
        maxOperatorResultCharLimit: t.Optional(t.Number()),
        maxOperatorResultCellCharLimit: t.Optional(t.Number()),
        operatorResultSerializationMode: t.Optional(t.Literal("tsv")),
        toolTimeoutSeconds: t.Optional(t.Number()),
        executionTimeoutMinutes: t.Optional(t.Number()),
        maxSteps: t.Optional(t.Number()),
        disabledTools: t.Optional(t.Array(t.String())),
        allowedOperatorTypes: t.Optional(t.Array(t.String())),
      }),
    }
  );

interface WsMessage {
  type: "message" | "stop" | "decision";
  content?: string;
  messageSource?: "chat" | "feedback";
  // Fields below carry the user's verdict on a pending-approval step.
  // Used when type === "decision". See agent/tools/dataguard/with-approval.ts.
  // "modify" verdict was cut by #11a (it silently lied — the handler ran the
  // original proposalParams and just logged the user's free-text).
  stepId?: string;
  verdict?: "allow" | "deny";
  remember?: boolean;
}

interface OperatorResultSummaryWs {
  state: string;
  inputTuples: number;
  outputTuples: number;
  inputPortShapes?: { portIndex: number; rows: number; columns: number }[];
  outputColumns?: number;
  error?: string;
  warnings?: string[];
  consoleLogCount?: number;
  totalRowCount?: number;
  sampleRecords?: Record<string, any>[];
  resultStatistics?: Record<string, string>;
}

interface WsOutgoingMessage {
  type: "step" | "state" | "error" | "complete" | "init" | "headChange";
  step?: ReActStep;
  state?: string;
  error?: string;
  steps?: ReActStep[];
  headId?: string;
  operatorResults?: Record<string, OperatorResultSummaryWs>;
  workflowContent?: any;
}

function getOperatorResultSummaries(agent: TexeraAgent): Record<string, OperatorResultSummaryWs> {
  const resultState = agent.getWorkflowResultState();
  const visible = resultState.getAllVisible();
  const results: Record<string, OperatorResultSummaryWs> = {};
  for (const [opId, entry] of visible) {
    const info = entry.operatorInfo;
    results[opId] = {
      state: info.state,
      inputTuples: info.inputTuples,
      outputTuples: info.outputTuples,
      inputPortShapes: info.inputPortShapes,
      outputColumns:
        info.result && info.result.length > 0
          ? Object.keys(info.result[0]).filter(k => k !== "__row_index__").length
          : undefined,
      error: info.error,
      warnings: info.warnings,
      consoleLogCount: info.consoleLogs?.length,
      totalRowCount: info.totalRowCount,
      sampleRecords: info.result,
      resultStatistics: info.resultStatistics,
    };
  }
  return results;
}

function broadcastToAgent(agentId: string, message: WsOutgoingMessage): void {
  const agent = agentStore.get(agentId);
  if (!agent) return;

  const jsonMessage = JSON.stringify(message);
  for (const ws of agent.getWebsockets()) {
    try {
      ws.send(jsonMessage);
    } catch (error) {
      wsLog.error({ agentId, err: error }, "failed to send message to client");
      agent.removeWebsocket(ws);
    }
  }
}

export function buildApp() {
  // `normalize: false` so body schemas with additionalProperties:false can
  // reject unknown fields (Elysia 1.4 silently strips them by default — see
  // the #11a modify-reject tests).
  return new Elysia({ normalize: false })
    .use(cors())
    .group(env.API_PREFIX, app =>
      app
        .get("/healthcheck", () => ({
          status: "ok",
          timestamp: new Date().toISOString(),
        }))
        .use(agentsRouter)
    )
    .ws(`${env.API_PREFIX}/agents/:id/react`, {
      open(ws) {
        const agentId = (ws.data as any).params?.id;
        wsLog.info({ agentId }, "client connected");

        const agent = agentStore.get(agentId);
        if (!agent) {
          ws.send(JSON.stringify({ type: "error", error: "Agent not found" }));
          ws.close();
          return;
        }

        agent.addWebsocket(ws);

        const initMessage: WsOutgoingMessage = {
          type: "init",
          state: agent.getState(),
          steps: agent.getAllSteps(),
          headId: agent.getHead(),
          operatorResults: getOperatorResultSummaries(agent),
        };
        ws.send(JSON.stringify(initMessage));
      },

      async message(ws, messageData) {
        const agentId = (ws.data as any).params?.id;
        const agent = agentStore.get(agentId);

        if (!agent) {
          ws.send(JSON.stringify({ type: "error", error: "Agent not found" }));
          return;
        }

        let msg: WsMessage;
        try {
          msg = typeof messageData === "string" ? JSON.parse(messageData) : (messageData as WsMessage);
        } catch {
          ws.send(JSON.stringify({ type: "error", error: "Invalid message format" }));
          return;
        }

        if (msg.type === "stop") {
          agent.stop();
          broadcastToAgent(agentId, { type: "state", state: "STOPPING" });
          return;
        }

        if (msg.type === "decision") {
          if (!msg.stepId || !msg.verdict) {
            ws.send(
              JSON.stringify({
                type: "error",
                error: "decision requires stepId and verdict",
              })
            );
            return;
          }
          if (msg.verdict !== "allow" && msg.verdict !== "deny") {
            ws.send(
              JSON.stringify({
                type: "error",
                error: `verdict must be "allow" or "deny" (got "${msg.verdict}")`,
              })
            );
            return;
          }
          if (msg.verdict === "deny" && msg.remember === true) {
            ws.send(
              JSON.stringify({
                type: "error",
                error: "remember=true only applies to allow decisions",
              })
            );
            return;
          }
          const resolved = agent.resolveDecision(msg.stepId, {
            stepId: msg.stepId,
            verdict: msg.verdict,
            remember: msg.remember,
          });
          wsLog.info(
            { agentId, stepId: msg.stepId, verdict: msg.verdict, resolved },
            "received user decision"
          );
          return;
        }

        if (msg.type === "message") {
          if (!msg.content || typeof msg.content !== "string") {
            ws.send(JSON.stringify({ type: "error", error: "Message content is required" }));
            return;
          }

          wsLog.info({ agentId, preview: msg.content.substring(0, 50) }, "received message");

          agent.setStepCallback((step: ReActStep) => {
            const hasToolCalls = step.toolCalls && step.toolCalls.length > 0;
            broadcastToAgent(agentId, {
              type: "step",
              step,
              ...(hasToolCalls ? { operatorResults: getOperatorResultSummaries(agent) } : {}),
            });
          });

          broadcastToAgent(agentId, { type: "state", state: "GENERATING" });

          try {
            const result = await agent.sendMessage(msg.content, msg.messageSource);

            agent.setStepCallback(null);

            const allSteps = agent.getReActSteps();
            const lastStep = allSteps[allSteps.length - 1];
            if (lastStep && lastStep.isEnd) {
              broadcastToAgent(agentId, { type: "step", step: lastStep });
            }

            broadcastToAgent(agentId, {
              type: "complete",
              state: agent.getState(),
              operatorResults: getOperatorResultSummaries(agent),
            });

            wsLog.info({ agentId, steps: result.messages.length }, "agent run complete");
          } catch (error: any) {
            agent.setStepCallback(null);
            broadcastToAgent(agentId, { type: "error", error: error.message });
          }
        }
      },

      close(ws) {
        const agentId = (ws.data as any).params?.id;
        wsLog.info({ agentId }, "client disconnected");

        const agent = agentStore.get(agentId);
        if (agent) {
          agent.removeWebsocket(ws);
        }
      },
    })
    .onError(({ error, set }) => {
      // Catch-all for non-router routes such as /api/healthcheck and the websocket route.
      log.error({ err: error }, "request error");
      set.status = 500;
      return { error: error instanceof Error ? error.message : String(error) };
    });
}

// Reset module-level state. Used by tests to start each case from a clean store.
export function _resetAgentStoreForTests(): void {
  agentStore.clear();
  agentCounter = 0;
}

// Reach an agent by id from a test so a test can seed its DataGuardSession
// directly (avoids running the LLM-backed /scan to set up state).
export function _getAgentForTests(id: string): TexeraAgent | undefined {
  return agentStore.get(id);
}

function printStartupMessage(app: ReturnType<typeof buildApp>) {
  const LINE = "=".repeat(60);
  console.log(LINE);
  console.log("Texera Agent Service (Elysia.js + RxJS)");
  console.log(LINE);
  console.log(`Server running at http://localhost:${env.PORT}`);
  console.log("");

  console.log("Registered Routes:");
  const routes = app.routes;

  const httpRoutes = routes.filter(r => r.method !== "WS");
  const wsRoutes = routes.filter(r => r.method === "WS");

  for (const route of httpRoutes) {
    const method = route.method.padEnd(6);
    console.log(`  ${method} ${route.path}`);
  }

  if (wsRoutes.length > 0) {
    console.log("");
    console.log("WebSocket Endpoints:");
    for (const route of wsRoutes) {
      console.log(`  WS     ${route.path}`);
    }
    console.log("         Send: { type: 'message', content: '...' }");
    console.log("         Send: { type: 'stop' }");
    console.log("         Recv: { type: 'step' | 'state' | 'complete' | 'error' | 'init', ... }");
  }

  console.log("");
  console.log("Environment:");
  console.log(`  LLM_API_KEY: ${env.LLM_API_KEY === "dummy" ? "dummy (default)" : "set"}`);
  console.log(`  LLM_ENDPOINT: ${getBackendConfig().modelsEndpoint}`);
  console.log(`  WORKFLOW_COMPILING_SERVICE_ENDPOINT: ${getBackendConfig().compileEndpoint}`);
  console.log(`  TEXERA_DASHBOARD_SERVICE_ENDPOINT: ${getBackendConfig().apiEndpoint}`);
  console.log("");
  console.log("Features:");
  console.log("  - Auto-persistence with debounce (500ms)");
  console.log(LINE);
}

async function initializeServices() {
  try {
    log.info("initializing global workflow system metadata");
    const metadata = await WorkflowSystemMetadata.initializeGlobal();
    log.info({ operatorCount: metadata.getOperatorCount() }, "loaded operators into global metadata");
  } catch (error) {
    log.warn({ err: error }, "failed to initialize global metadata; agents will initialize individually");
  }
}

export async function start() {
  await initializeServices();
  const app = buildApp().listen(env.PORT);
  printStartupMessage(app);
  return app;
}

// Run the server only when this file is the entry point, not when it is
// imported by tests or other modules.
if (import.meta.main) {
  start();
}
