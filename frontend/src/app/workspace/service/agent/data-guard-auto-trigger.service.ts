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

import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { Observable, Subscription, debounceTime, filter, firstValueFrom, map, merge } from "rxjs";
import * as Papa from "papaparse";
import { OperatorPredicate } from "../../types/workflow-common.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { AgentService, AgentInfo } from "./agent.service";
import { DataGuardSettingsService } from "./data-guard-settings.service";
import { DataGuardResultsService, ChecklistEntry, DataQualityIssue, FixProposal } from "./data-guard-results.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { parseJsonl } from "./data-guard-jsonl";

/**
 * DataGuard auto-trigger orchestration — checklist-driven flow.
 *
 * Pipeline when a dataset-reading operator is added (or its file property is set):
 *   1. Resolve workflow context (id, settings on/off).
 *   2. Find or create an agent bound to this workflow.
 *   3. Load the dataset into the agent's DataGuardSession:
 *      - if operator has a `fileName` → fetch via DatasetService + papaparse
 *      - else → server-side load of bundled demo CSV
 *   4. Call server-side `/dataguard/scan` (NOT the chat — this bypasses the
 *      LLM ReAct loop, so the LLM can't decide to call `deleteOperator` and
 *      vaporize the user's workflow). The endpoint runs profile_dataset +
 *      suggest_fix server-side and returns issues + proposals in one shot.
 *   5. Publish results into DataGuardResultsService → the
 *      DataGuardChecklistComponent re-renders as a checklist with
 *      checkboxes. The user picks what to apply.
 *
 * Toggle: DataGuardSettingsService gates the pipeline (per-workflow,
 * default ON, controlled by toolbar 🛡 shield button).
 */
/**
 * Per-parser options. Optional; each parser ignores keys it doesn't care about.
 *  - `delimiter`: explicit field separator to override Papa's autodetect. Used
 *    by `CSVOldFileScan`, whose Scala impl overrides scala-csv's delimiter via
 *    the operator's `customDelimiter` property (`,` / `;` / `\t` / …). Without
 *    this, Papa's autodetect *usually* picks the right one but isn't strict
 *    equivalence — a `;`-delimited file would round-trip differently than the
 *    operator's own reader.
 */
export interface ParserOptions {
  delimiter?: string;
}

/**
 * A `DatasetParser` converts a raw file Blob into the {columns, rows} shape the
 * agent-service `/dataguard/dataset` endpoint expects. Each format gets its own
 * parser so we don't `Papa.parse` JSON / Parquet / TSV bytes and produce
 * garbage rows.
 *
 * Returning `Promise` lets a parser do streaming or chunked reads (Parquet
 * decoders, JSONL line readers) without changing the caller contract.
 */
export type DatasetParser = (
  blob: Blob,
  fileName: string,
  options?: ParserOptions
) => Promise<{ columns: string[]; rows: Record<string, unknown>[] }>;

/**
 * Parse a Blob assumed to be RFC-4180 CSV with a header row. Shared by every
 * CSV-shaped operator (`CSVFileScan`, `CSVOldFileScan`). CSVOld's Scala impl
 * (CSVOldScanSourceOpExec) uses scala-csv with `DefaultCSVFormat`, which is
 * RFC-4180 — so the bytes are byte-for-byte the same shape, only the runtime
 * wrapper differs. No CSVOld-specific variant is needed; we just register
 * `parseCsv` for both keys.
 *
 * If `options.delimiter` is provided we pass it through to Papa instead of
 * relying on Papa's autodetect — necessary for CSVOldFileScan, which exposes
 * a `customDelimiter` operator property (`,` is the default but the user can
 * set `;`, `\t`, or anything else).
 *
 * `ParallelCSVFileScan` was removed from this dispatcher because Texera has
 * disabled it in the operator registry (see LogicalOp.scala:171 — the type
 * registration is commented out so users can't drop one onto the canvas).
 * If it gets re-enabled later, add the key back here in one line.
 */
export const parseCsv: DatasetParser = async (blob, _fileName, options) => {
  const text = await blob.text();
  const parseConfig: Papa.ParseConfig<Record<string, unknown>> = {
    header: true,
    skipEmptyLines: true,
    dynamicTyping: true,
  };
  if (options?.delimiter) {
    parseConfig.delimiter = options.delimiter;
  }
  const parsed = Papa.parse<Record<string, unknown>>(text, parseConfig);
  if (parsed.errors.length > 0) {
    throw new Error(`CSV parse failed: ${parsed.errors[0].message} (row ${parsed.errors[0].row})`);
  }
  return {
    columns: parsed.meta.fields ?? [],
    rows: parsed.data,
  };
};

/**
 * Operator-type → parser dispatch table. Exported so tests can assert the
 * mapping is correct without instantiating the full service.
 */
export const PARSERS: Record<string, DatasetParser> = {
  CSVFileScan: parseCsv,
  CSVOldFileScan: parseCsv,
  // JSONL flattens nested objects into dot-notation columns so the five
  // detectors fire identically on JSONL-loaded data as on CSV-loaded data.
  // See data-guard-jsonl.ts for the full flatten policy + collision rule.
  JSONLFileScan: parseJsonl,
};

@Injectable({ providedIn: "root" })
export class DataGuardAutoTriggerService {
  // Auto-trigger fires only for operators whose type is a key in `PARSERS`.
  // Adding a new format = add to PARSERS + this set in lockstep (see post-MVP
  // §19 of README_DataGuard_Texera.md for the broader format roadmap).
  private static readonly DATASET_OPERATOR_TYPES = new Set<string>(["CSVFileScan", "CSVOldFileScan", "JSONLFileScan"]);

  /** Dedup: re-orchestrate only if (operatorID, filePath) changes. */
  private readonly lastOrchestratedFile = new Map<string, string>();
  /**
   * The currently-running pipeline, if any. Replaces the old boolean `busy`
   * flag so a user-initiated rescan can *await* the in-flight scan instead
   * of either silently dropping (old behaviour) or double-firing a parallel
   * /scan against agent-service (which would race results.setState calls and
   * double LLM cost). Auto-trigger paths still treat a non-null pipeline as
   * "busy" and drop — they have no UX cost to skipping a redundant scan.
   */
  private currentPipeline: Promise<void> | null = null;

  constructor(
    private readonly workflowActionService: WorkflowActionService,
    private readonly agentService: AgentService,
    private readonly notificationService: NotificationService,
    private readonly settings: DataGuardSettingsService,
    private readonly results: DataGuardResultsService,
    private readonly datasetService: DatasetService,
    private readonly executeWorkflowService: ExecuteWorkflowService,
    private readonly http: HttpClient
  ) {}

  public isDatasetOperatorType(operatorType: string): boolean {
    return DataGuardAutoTriggerService.DATASET_OPERATOR_TYPES.has(operatorType);
  }

  /**
   * Force a re-scan of whichever operator the current panel state was tied
   * to. After an Apply, the user might notice the AI missed something on
   * the cleaned version and want another pass. The auto-trigger dedup would
   * normally block this (same opId + same fileName), so we clear the dedup
   * entry before running. A subsequent Apply produces a new timestamped
   * dataset version (v3, v4, …).
   */
  public async rescanCurrent(): Promise<void> {
    const state = this.results.getState();
    const opId = state.sourceOperatorId;
    if (!opId) {
      this.notificationService.warning("DataGuard: nothing to re-scan yet — drop a dataset operator first.");
      return;
    }
    const op = this.workflowActionService.getTexeraGraph().getOperator(opId);
    if (!op) {
      this.notificationService.warning("DataGuard: the original operator is gone — can't re-scan.");
      return;
    }
    // Bypass the per-(opId, filePath) dedup so the pipeline runs even though
    // nothing about the file path changed since the last scan.
    this.lastOrchestratedFile.delete(opId);
    await this.runPipeline(op, { userInitiated: true });
  }

  /**
   * Re-scan whatever is currently on the canvas. Used by the floating
   * DataGuard icon (when the panel is closed) and any "scan anytime" entry
   * point — works even if the panel was previously reset to idle (i.e., the
   * sourceOperatorId is gone).
   *
   * An explicit user click should always win:
   *  - The panel is forced into the "scanning" state immediately so the user
   *    sees feedback even if a downstream guard bails (every bail-out path
   *    now overwrites this with a meaningful "error" / "idle" message instead
   *    of leaving the panel hanging on "Re-scanning…").
   *  - The (opId, filePath) dedup is cleared wholesale — the dedup exists to
   *    suppress auto-debounce, not to block deliberate user actions.
   *  - If a pipeline is genuinely still in flight (HTTP /scan mid-call), we
   *    *await* it before starting the new one. Forcing `busy=false` instead
   *    would let two `/scan` POSTs race, doubling LLM cost and clobbering
   *    `results.setState` in undefined order.
   *
   * Resolution order:
   *  1. If results state still has a live sourceOperatorId → re-scan it.
   *  2. Else pick the first dataset-reading operator on the canvas.
   *  3. Else surface a panel "idle" with a clear message and toast.
   */
  public async rescanAny(): Promise<void> {
    // Always show the panel immediately — even if a guard below bails, the
    // user must see *something* happen. Every bail-out path overwrites this.
    this.results.setState({ state: "scanning", entries: [], message: "Re-scanning…" });
    // Wipe the dedup map — an explicit click should never be suppressed.
    this.lastOrchestratedFile.clear();
    // Serialize behind any in-flight pipeline. The setState above gives the
    // user immediate visual feedback while we wait. runPipeline itself also
    // awaits currentPipeline on the userInitiated path; awaiting here too
    // keeps the resolveRescanTarget call below from racing a pipeline that's
    // mid-mutation of results state.
    if (this.currentPipeline) {
      try {
        await this.currentPipeline;
      } catch {
        /* swallow — the prior pipeline's own error UX already fired */
      }
    }

    const target = DataGuardAutoTriggerService.resolveRescanTarget(
      this.results.getState().sourceOperatorId,
      this.workflowActionService.getTexeraGraph().getAllOperators(),
      t => this.isDatasetOperatorType(t)
    );
    if (target.kind === "none") {
      this.results.setState({
        state: "idle",
        message: undefined,
      });
      this.notificationService.warning("DataGuard: drop a dataset operator on the canvas first.");
      return;
    }
    await this.runPipeline(target.operator, { userInitiated: true });
  }

  /**
   * Pure helper: pick which operator a user-initiated rescan should target.
   *  - "prior"     — the operator tied to the current panel state is still on the graph.
   *  - "candidate" — that operator is gone (or never set); fall back to the first
   *                  dataset operator on the canvas.
   *  - "none"      — no dataset operator exists; caller must surface a message.
   * Extracted as a static so it's exercisable without a TestBed harness.
   */
  public static resolveRescanTarget(
    priorOpId: string | undefined,
    allOperators: OperatorPredicate[],
    isDatasetType: (operatorType: string) => boolean
  ):
    | { kind: "prior"; operator: OperatorPredicate }
    | { kind: "candidate"; operator: OperatorPredicate }
    | { kind: "none" } {
    if (priorOpId) {
      const prior = allOperators.find(o => o.operatorID === priorOpId);
      if (prior) return { kind: "prior", operator: prior };
    }
    const candidate = allOperators.find(o => isDatasetType(o.operatorType));
    if (candidate) return { kind: "candidate", operator: candidate };
    return { kind: "none" };
  }

  /** Subscribe both operator-add and operator-property-change. */
  public startOrchestration(): Subscription {
    const graph = this.workflowActionService.getTexeraGraph();
    const addStream$ = graph.getOperatorAddStream();
    const propertyStream$ = graph.getOperatorPropertyChangeStream().pipe(
      debounceTime(500),
      map(event => event.operator)
    );

    return merge(addStream$, propertyStream$)
      .pipe(filter(op => this.isDatasetOperatorType(op.operatorType)))
      .subscribe(op => {
        void this.runPipeline(op, { userInitiated: false });
      });
  }

  /**
   * Apply a batch of user decisions. Triggered by the checklist "Apply" button.
   *
   * Pipeline:
   *   1. POST /apply-batch — agent-service mutates its in-memory dataset.
   *   2. GET  /export-csv  — pull the cleaned dataset back as a CSV Blob.
   *   3. Upload that CSV to the source dataset (multipart-upload) and commit a
   *      new dataset version named "DataGuard cleaned".
   *   4. Rewrite the operator's `fileName` to point at the new version.
   *   5. Auto-run the workflow so the result panel populates.
   *
   * If we can't write back (no write access, can't locate the dataset, etc.)
   * we surface a friendly message and stop — the in-memory fix is still
   * recorded server-side, just not reflected on the canvas.
   */
  public async applyBatch(
    decisions: Array<{ issueId: string; verdict: "allow" | "deny"; remember?: boolean }>
  ): Promise<void> {
    const state = this.results.getState();
    if (!state.agentId) {
      this.notificationService.error("DataGuard: no active scan.");
      return;
    }
    this.results.setState({ state: "applying", message: `Cleaning ${decisions.length} item(s)…` });

    // Step 1 — apply in memory
    let applyResp: {
      applied: number;
      denied: number;
      failed: number;
      datasetRowCount: number;
      residualIssues?: DataQualityIssue[];
      residualCount?: number;
    };
    try {
      applyResp = await firstValueFrom(
        this.http.post<typeof applyResp>(`/api/agents/${state.agentId}/dataguard/apply-batch`, { decisions })
      );
    } catch (e: unknown) {
      const msg = this.extractMessage(e);
      this.results.setState({ state: "error", message: `Couldn't apply fixes: ${msg}` });
      this.notificationService.error(`DataGuard: ${msg}`);
      return;
    }

    const { applied, denied } = applyResp;
    const residualCount = applyResp.residualCount ?? applyResp.residualIssues?.length ?? 0;
    // Verification re-scan: surface a quiet notification if anything was left
    // behind. We deliberately keep residualCount accurate (= what's still in
    // the data) rather than excluding issues the agent couldn't propose a fix
    // for — hiding those would make the toast lie. Calling out the user's own
    // skips separately when relevant tells them which leftovers are their
    // call vs. which are limitations of the auto-fix pass.
    if (residualCount > 0) {
      const skippedNote = denied > 0 ? ` (${denied} you skipped)` : "";
      this.notificationService.warning(
        `DataGuard applied ${applied} fix${applied === 1 ? "" : "es"}, but ${residualCount} issue${residualCount === 1 ? " is" : "s are"} still present${skippedNote}.`
      );
    }
    if (applied === 0) {
      // Nothing actually changed — skip the upload + rerun dance.
      this.results.setState({
        state: "done",
        message: denied > 0 ? `Skipped ${denied} item(s). Nothing changed.` : "Nothing to apply.",
      });
      return;
    }

    // Step 2 — write the cleaned file back as a new dataset version, then run.
    const sourceFile = state.sourceFilePath;
    const opId = state.sourceOperatorId;
    if (!sourceFile || !opId) {
      this.results.setState({
        state: "done",
        message: `Cleaned ${applied} item(s). (Source file unknown — couldn't save back automatically.)`,
      });
      return;
    }

    try {
      this.results.setState({ message: "Saving cleaned data as a new version…" });
      // Look up the source operator's type so writeBackAsNewVersion picks the
      // matching export endpoint (export-csv vs export-jsonl). If the operator
      // has been deleted while we were applying, fall back to CSV — that's the
      // historical default and downstream still rejects the path if needed.
      const sourceOp = this.workflowActionService.getTexeraGraph().getOperator(opId);
      const sourceOpType = sourceOp?.operatorType ?? "";
      const newPath = await this.writeBackAsNewVersion(state.agentId, sourceFile, sourceOpType);

      // Step 4 — repoint the operator at the cleaned version
      const graph = this.workflowActionService.getTexeraGraph();
      const op = graph.getOperator(opId);
      if (op) {
        this.workflowActionService.setOperatorProperty(opId, {
          ...op.operatorProperties,
          fileName: newPath,
        });
      }
      // Prevent the auto-trigger pipeline from re-scanning this fresh file
      this.lastOrchestratedFile.set(opId, newPath);

      // Step 5 — auto-run
      this.results.setState({
        state: "done",
        message: `Cleaned ${applied} item(s). Running workflow with fixed data…`,
      });
      this.notificationService.success(`DataGuard: cleaned ${applied} item(s). Running workflow…`);
      this.executeWorkflowService.executeWorkflow("DataGuard cleaned run");

      // Step 6 — auto-rescan the cleaned version so the checklist reflects
      // reality (typically empty, or whatever residue is left). Without this
      // step the panel keeps showing the previous scan's entries even after
      // the user pressed Fix — confusing, since each row's fix has already
      // been applied to a NEW dataset version that's now what the operator
      // points at. The workflow execute above is independent; we don't wait.
      void this.rescanCurrent();
    } catch (e: unknown) {
      const msg = this.extractMessage(e);
      this.results.setState({
        state: "error",
        message: `Cleaned ${applied} item(s) in memory, but couldn't save back: ${msg}`,
      });
      this.notificationService.error(`DataGuard: couldn't save cleaned data — ${msg}`);
    }
  }

  /**
   * Download the cleaned CSV from agent-service, push it to the source
   * dataset, commit a new version, and return the new fileName path so the
   * operator can be re-pointed.
   *
   * `sourceFile` is the path the operator was reading from, in the canonical
   * format `/ownerEmail/datasetName/versionName/fileRelativePath`.
   */
  private async writeBackAsNewVersion(agentId: string, sourceFile: string, sourceOpType: string): Promise<string> {
    // 1. Pull cleaned data from agent-service. The endpoint + MIME type +
    // fallback extension depend on the source operator type. Everything else
    // in this method (LakeFS commit, operator-repoint, auto-rescan) is
    // format-agnostic and stays untouched.
    //
    // Filename policy: we PRESERVE the source file's original name (extension
    // included). The `fallbackName` below is used only when the source path
    // is degenerate (no trailing segment). We deliberately do NOT rewrite the
    // extension based on body format — LakeFS doesn't care about extensions,
    // and rewriting "data.json → data.jsonl" would silently change the
    // operator's perceived file identity. Reuse-in-place is the contract.
    const isJsonl = sourceOpType === "JSONLFileScan";
    const exportPath = isJsonl ? "export-jsonl" : "export-csv";
    const mimeType = isJsonl ? "application/x-ndjson" : "text/csv";
    const fallbackName = isJsonl ? "cleaned.jsonl" : "cleaned.csv";

    const dataBlob = await firstValueFrom(
      this.http.get(`/api/agents/${agentId}/dataguard/${exportPath}`, { responseType: "blob" })
    );
    const fileName = sourceFile.split("/").pop() || fallbackName;
    const cleanedFile = new File([dataBlob], fileName, { type: mimeType });

    // 2. Parse source path: /ownerEmail/datasetName/versionName/fileRelative...
    const parts = sourceFile.replace(/^\/+/, "").split("/");
    if (parts.length < 4) {
      throw new Error(`source path "${sourceFile}" is not /owner/dataset/version/file`);
    }
    const [ownerEmail, datasetName, , ...fileRel] = parts;
    const fileRelativePath = fileRel.join("/");

    // 3. Find dataset (need its did + write access)
    const datasets = await firstValueFrom(this.datasetService.retrieveAccessibleDatasets());
    const match = datasets.find(d => d.ownerEmail === ownerEmail && d.dataset.name === datasetName);
    if (!match || !match.dataset.did) {
      throw new Error(`dataset "${ownerEmail}/${datasetName}" not accessible to you`);
    }
    if (match.accessPrivilege !== "WRITE" && !match.isOwner) {
      throw new Error(`you don't have write access to "${ownerEmail}/${datasetName}"`);
    }

    // 4. Multipart-upload as a single part (cleaned files are small for a hackathon demo)
    const partSize = Math.max(cleanedFile.size, 5 * 1024 * 1024); // LakeFS likes ≥5MB parts but accepts last
    await firstValueFrom(
      this.datasetService
        .multipartUpload(ownerEmail, datasetName, fileRelativePath, cleanedFile, partSize, 1, true)
        .pipe(filter(p => p.status === "finished"))
    );

    // 5. Commit a new version. Append a timestamp so re-running on the same
    // source version (e.g., user picks v1 again after we already produced a
    // "DataGuard cleaned" v2) doesn't collide on the version name — every run
    // gets a fresh unique version instead of failing.
    const versionLabel = `DataGuard cleaned ${this.timestampSuffix()}`;
    const newVersion = await firstValueFrom(this.datasetService.createDatasetVersion(match.dataset.did, versionLabel));

    return `/${ownerEmail}/${datasetName}/${newVersion.name}/${fileRelativePath}`;
  }

  /** YYYY-MM-DD HH:mm:ss in local time — unique per second, human-readable in
   *  the dataset version dropdown. */
  private timestampSuffix(): string {
    const d = new Date();
    const pad = (n: number) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }

  /** Pull a readable message out of any throwable. Angular's HttpErrorResponse
   *  is a class but does NOT extend Error, so the prior `instanceof Error`
   *  check fell through to String(e) and printed "[object Object]". Try the
   *  obvious shapes in order. */
  private extractMessage(e: unknown): string {
    if (typeof e === "string") return e;
    if (e instanceof Error) return e.message;
    if (e && typeof e === "object") {
      const obj = e as Record<string, unknown>;
      // Angular HttpErrorResponse: { error: { message } | string, message, statusText }
      const inner = obj["error"];
      if (typeof inner === "string" && inner) return inner;
      if (inner && typeof inner === "object") {
        const im = (inner as Record<string, unknown>)["message"];
        if (typeof im === "string" && im) return im;
      }
      if (typeof obj["message"] === "string" && obj["message"]) return obj["message"] as string;
      if (typeof obj["statusText"] === "string" && obj["statusText"]) return obj["statusText"] as string;
      try {
        return JSON.stringify(e);
      } catch {
        return "unknown error";
      }
    }
    return String(e);
  }

  // ============================================================
  //   Pipeline
  // ============================================================

  private async runPipeline(
    op: OperatorPredicate,
    options: { userInitiated: boolean } = { userInitiated: false }
  ): Promise<void> {
    const filePath = this.readFilePath(op);
    // Don't auto-trigger until the user actually picks a file. Otherwise the
    // panel pops open against a bundled demo dataset that has no relation to
    // the empty operator on the canvas — confusing, and the workflow can't
    // even run yet (Texera throws FileNotFoundException on an unset fileName).
    if (!filePath) {
      if (options.userInitiated) {
        // Floater click against an unconfigured operator: surface a visible
        // panel state + toast rather than silently no-op'ing.
        this.results.setState({
          state: "idle",
          message: undefined,
        });
        this.notificationService.warning("DataGuard: set the operator's file path before scanning.");
      }
      return;
    }
    // Auto-trigger dedup. A userInitiated call already cleared the map in
    // rescanAny / rescanCurrent, so this only suppresses property-change spam.
    if (!options.userInitiated && this.lastOrchestratedFile.get(op.operatorID) === filePath) return;
    // Concurrency control. Two callers compete for the slot:
    //   - Auto-trigger (userInitiated=false): a slot conflict means a redundant
    //     scan, so we drop silently. Property-change events fire in bursts and
    //     we don't want every one of them tail-piling.
    //   - User click  (userInitiated=true ): a slot conflict means the user
    //     deliberately asked for a fresh scan while one is running. We serialize
    //     by AWAITING the in-flight pipeline before starting ours. This avoids
    //     two parallel /scan POSTs (which would double LLM cost and race
    //     results.setState in undefined order) while still honouring the click.
    if (this.currentPipeline) {
      if (!options.userInitiated) return;
      this.notificationService.info("DataGuard: queuing your scan behind one already in progress…");
      try {
        await this.currentPipeline;
      } catch {
        /* prior pipeline's own error UX already fired */
      }
      // currentPipeline.finally has cleared the slot by the time we resume.
    }

    // Register THIS pipeline as the in-flight one. We assign the promise
    // before awaiting it (synchronously visible to any rescanAny caller that
    // checks `currentPipeline` in the same tick), then await ourselves.
    // _runPipelineBody clears `currentPipeline` in its own finally.
    const promise = this._runPipelineBody(op, filePath, options);
    this.currentPipeline = promise;
    await promise;
  }

  /** The actual pipeline body. Split out from runPipeline so the concurrency
   *  control above is straight-line: assign currentPipeline once, await it,
   *  done. The finally clears the slot so the next caller (auto-trigger or
   *  user click) doesn't see a stale Promise. */
  private async _runPipelineBody(
    op: OperatorPredicate,
    filePath: string,
    options: { userInitiated: boolean }
  ): Promise<void> {
    try {
      const workflowId = this.workflowActionService.getWorkflowMetadata()?.wid;
      if (!workflowId) {
        if (options.userInitiated) {
          this.results.setState({ state: "idle", message: undefined });
        }
        this.notificationService.warning("DataGuard: save the workflow first.");
        return;
      }

      if (!this.settings.isEnabled(workflowId)) {
        if (options.userInitiated) {
          this.results.setState({ state: "idle", message: undefined });
        }
        this.notificationService.info(
          "DataGuard is OFF for this workflow. Click the 🛡 shield in the toolbar to re-enable."
        );
        return;
      }

      // Phase 1: agent + dataset
      const agentId = await this.ensureAgent(workflowId);
      this.results.setState({
        agentId,
        state: "scanning",
        entries: [],
        message: "Loading dataset…",
      });

      const loaded = await this.loadFromOperatorFile(agentId, filePath, op);

      this.results.setState({
        datasetSource: loaded.source,
        datasetRows: loaded.rows,
        datasetColumns: loaded.columns,
        message: `Checking ${loaded.rows} rows for problems…`,
      });
      this.notificationService.info(`DataGuard is checking ${loaded.rows} rows from ${loaded.source}…`);

      // Phase 2: server-side scan (NO chat involved — bypasses LLM ReAct loop)
      const scan: {
        issueCount: number;
        issues: DataQualityIssue[];
        proposals: Array<{ issueId: string; proposal: FixProposal | null; error: string | null }>;
      } = await firstValueFrom(this.http.post<any>(`/api/agents/${agentId}/dataguard/scan`, {}));

      const entries: ChecklistEntry[] = scan.issues.map(issue => {
        const p = scan.proposals.find(x => x.issueId === issue.issueId);
        return {
          issueId: issue.issueId,
          issue,
          proposal: p?.proposal ?? null,
          error: p?.error ?? null,
          // Default: low-risk = "allow" pre-checked. medium/high/warning all
          // start "pending" so the user makes an explicit call — especially
          // important for "warning" where we deliberately want manual review.
          verdict: p?.proposal?.riskTier === "low" ? "allow" : "pending",
        };
      });

      this.results.setState({
        state: "ready",
        entries,
        message:
          scan.issueCount === 0
            ? "Your data looks good — nothing to fix."
            : `Found ${scan.issueCount} thing${scan.issueCount === 1 ? "" : "s"} we can clean up. Pick which to fix.`,
        sourceOperatorId: op.operatorID,
        sourceFilePath: filePath,
      });
      this.lastOrchestratedFile.set(op.operatorID, filePath);
      if (scan.issueCount > 0) {
        this.notificationService.success(
          `DataGuard found ${scan.issueCount} thing${scan.issueCount === 1 ? "" : "s"} to clean up. Review and click Fix.`
        );
      } else {
        this.notificationService.info("DataGuard: your data looks good.");
      }
    } catch (e: unknown) {
      const msg = this.extractMessage(e);
      this.results.setState({ state: "error", message: msg });
      this.notificationService.error(`DataGuard auto-trigger failed: ${msg}`);
    } finally {
      this.currentPipeline = null;
    }
  }

  private readFilePath(op: OperatorPredicate): string {
    const props = (op.operatorProperties ?? {}) as Record<string, unknown>;
    const v = props["fileName"];
    return typeof v === "string" ? v.trim() : "";
  }

  /**
   * Load a dataset file into the agent's DataGuardSession. Dispatches to a
   * per-format parser via the `PARSERS` table so we don't `Papa.parse` JSON or
   * Parquet bytes. If no parser is registered for the operator type we surface
   * a visible error (toast + thrown) — silent no-op was the pre-dispatcher
   * footgun.
   *
   * Passes the operator's relevant properties through to the parser as
   * `ParserOptions`. Today the only consumer is CSVOldFileScan's
   * `customDelimiter`; future formats can extend `ParserOptions` (and this
   * extractor) without churning the parser signature.
   */
  private async loadFromOperatorFile(
    agentId: string,
    filePath: string,
    op: OperatorPredicate
  ): Promise<{ source: string; rows: number; columns: number }> {
    const operatorType = op.operatorType;
    const parser = PARSERS[operatorType];
    if (!parser) {
      const msg =
        `DataGuard: no parser registered for operator type "${operatorType}". ` +
        `Supported types: ${Object.keys(PARSERS).join(", ")}.`;
      this.notificationService.error(msg);
      throw new Error(msg);
    }
    const parserOptions = DataGuardAutoTriggerService.extractParserOptions(op);
    const fileName = filePath.split("/").pop() || filePath;
    const blob = await firstValueFrom(this.datasetService.retrieveDatasetVersionSingleFile(filePath));
    const { columns, rows } = await parser(blob, fileName, parserOptions);
    await firstValueFrom(this.http.post(`/api/agents/${agentId}/dataguard/dataset`, { columns, rows }));
    return { source: filePath, rows: rows.length, columns: columns.length };
  }

  /**
   * Pure helper: pull format-specific knobs off an operator's properties into
   * a `ParserOptions`. Exposed as static so tests can exercise the mapping
   * without a TestBed harness.
   *
   * Today this only handles `CSVOldFileScan.customDelimiter`. Empty / missing
   * delimiter falls back to Papa autodetect (matches the Scala side, which
   * also defaults to `,` when `customDelimiter` is empty).
   */
  public static extractParserOptions(op: OperatorPredicate): ParserOptions {
    const opts: ParserOptions = {};
    if (op.operatorType === "CSVOldFileScan") {
      const props = (op.operatorProperties ?? {}) as Record<string, unknown>;
      const raw = props["customDelimiter"];
      if (typeof raw === "string" && raw.length > 0) {
        opts.delimiter = raw;
      }
    }
    return opts;
  }

  private async ensureAgent(workflowId: number): Promise<string> {
    const all: AgentInfo[] = await firstValueFrom(this.agentService.getAllAgents());
    const match = all.find(a => a.delegate?.workflowId === workflowId);
    if (match) return match.id;
    const created = await firstValueFrom(this.agentService.createAgent("claude-haiku-4.5", "DataGuard", workflowId));
    return created.id;
  }
}
