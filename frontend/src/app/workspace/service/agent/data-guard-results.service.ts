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
import { BehaviorSubject, Observable } from "rxjs";

/** Shape of a DataGuard quality issue (mirrors agent-service/src/types/dataguard.ts). */
export interface DataQualityIssue {
  issueId: string;
  issueType: string;
  column: string;
  description: string;
  evidence: string;
  affectedRowCount: number;
  affectedRowIndices?: number[];
  detectedAt: string;
}

export interface FixProposal {
  issueId: string;
  issueType: string;
  action: string;
  operationKind: string;
  operationParams: Record<string, unknown>;
  // `warning` = fix is concrete but agent recommends manual review (e.g.,
  // outliers that might be real extremes). UI defaults these unchecked.
  riskTier: "low" | "medium" | "high" | "warning";
  reason: string;
  evidence: string;
  confidence: "low" | "medium" | "high";
  targetRowCount: number;
}

/** One row in the checklist UI: issue + (optional) proposal + (optional) error. */
export interface ChecklistEntry {
  issueId: string;
  issue: DataQualityIssue;
  proposal: FixProposal | null;
  /** Error generating the proposal (LLM failure, etc.) */
  error: string | null;
  /** User's selection for the apply-batch call. */
  verdict: "allow" | "deny" | "pending";
  /** When verdict === "allow", remember this issueType for next time. */
  remember?: boolean;
}

export type DataGuardPanelState = "idle" | "scanning" | "ready" | "applying" | "done" | "error";

export interface DataGuardScanResult {
  agentId: string;
  state: DataGuardPanelState;
  entries: ChecklistEntry[];
  datasetSource: string; // e.g., "bundled demo (diabetes_messy.csv)" or filePath
  datasetRows: number;
  datasetColumns: number;
  message?: string; // status message, e.g., "applied 3 fixes" or error text
  /** Operator + file the current scan was triggered against — used by Apply
   *  to write the cleaned CSV back into the same dataset as a new version. */
  sourceOperatorId?: string;
  sourceFilePath?: string;
  // Post-apply verification re-scan is read directly from the HTTP response
  // in DataGuardAutoTriggerService — it produces a toast and is not held on
  // state. If a future UI needs to display residual leftovers in the panel,
  // add the field back here.
}

/**
 * Per-page-session DataGuard state. Drives the DataGuardChecklistComponent.
 *
 * - `DataGuardAutoTriggerService` writes here after loading a dataset and
 *   running /scan.
 * - `DataGuardChecklistComponent` subscribes and re-renders.
 * - Clearing the workflow / closing the panel resets to `idle`.
 */
@Injectable({ providedIn: "root" })
export class DataGuardResultsService {
  private readonly state$ = new BehaviorSubject<DataGuardScanResult>({
    agentId: "",
    state: "idle",
    entries: [],
    datasetSource: "",
    datasetRows: 0,
    datasetColumns: 0,
  });

  public getState$(): Observable<DataGuardScanResult> {
    return this.state$.asObservable();
  }

  public getState(): DataGuardScanResult {
    return this.state$.value;
  }

  public setState(patch: Partial<DataGuardScanResult>): void {
    this.state$.next({ ...this.state$.value, ...patch });
  }

  public updateEntry(issueId: string, patch: Partial<ChecklistEntry>): void {
    const current = this.state$.value;
    const entries = current.entries.map(e => (e.issueId === issueId ? { ...e, ...patch } : e));
    this.state$.next({ ...current, entries });
  }

  public reset(): void {
    this.state$.next({
      agentId: "",
      state: "idle",
      entries: [],
      datasetSource: "",
      datasetRows: 0,
      datasetColumns: 0,
      sourceOperatorId: undefined,
      sourceFilePath: undefined,
    });
  }
}
