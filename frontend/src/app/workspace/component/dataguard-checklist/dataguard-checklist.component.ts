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

import { Component, OnDestroy, OnInit } from "@angular/core";
import { NgFor, NgIf } from "@angular/common";
import { Subscription } from "rxjs";
import { CdkDrag, CdkDragHandle } from "@angular/cdk/drag-drop";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzCheckboxComponent } from "ng-zorro-antd/checkbox";
import {
  DataGuardResultsService,
  DataGuardScanResult,
  ChecklistEntry,
} from "../../service/agent/data-guard-results.service";
import { DataGuardAutoTriggerService } from "../../service/agent/data-guard-auto-trigger.service";
import { DataGuardSettingsService } from "../../service/agent/data-guard-settings.service";
import { DataGuardRowNavigatorService } from "../../service/agent/data-guard-row-navigator.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../common/service/notification/notification.service";

/**
 * Standalone DataGuard checklist panel.
 *
 * UX (per user request):
 *   - When DataGuard auto-trigger detects a dataset and runs /scan, the
 *     issues + proposals land in DataGuardResultsService.
 *   - This component renders them as a checklist with per-row Allow / Deny
 *     pickers (low-risk pre-checked Allow).
 *   - Click "Apply Selected" to send the batch decision to
 *     /api/agents/:id/dataguard/apply-batch. The agent's chat is NOT
 *     involved — so the LLM can't accidentally call deleteOperator or modify
 *     the workflow canvas.
 *
 * Visibility:
 *   - Floating panel docked bottom-right, slides up when state ≠ "idle".
 *   - Collapsed via close button → state goes back to "idle".
 */
@Component({
  selector: "texera-dataguard-checklist",
  standalone: true,
  imports: [NgIf, NgFor, NzIconDirective, NzButtonComponent, NzCheckboxComponent, CdkDrag, CdkDragHandle],
  templateUrl: "./dataguard-checklist.component.html",
  styleUrls: ["./dataguard-checklist.component.scss"],
})
export class DataGuardChecklistComponent implements OnInit, OnDestroy {
  public scan: DataGuardScanResult = {
    agentId: "",
    state: "idle",
    entries: [],
    datasetSource: "",
    datasetRows: 0,
    datasetColumns: 0,
  };

  // Cached row-verdict tallies. Recomputed once per state push (see ngOnInit)
  // rather than three times per Angular change-detection tick. Default-CD
  // means every event walks the template, so a get() that filters the entries
  // array fires for *each* row in *each* tick — quadratic with a fat list.
  public selectedCount = 0;
  public deniedCount = 0;
  public pendingCount = 0;

  private sub?: Subscription;
  // Owns the workflow-graph subscription that powers auto-trigger orchestration.
  // Lives here (not in agent-panel) so the gate is "is the checklist mounted?"
  // — the only consumer of the auto-trigger output.
  private orchestrationSub?: Subscription;

  // Per-row cursor for the "📍" locate-cycle affordance. Keyed by `issueId`
  // so each detector row gets an independent cursor. Cleared on every fresh
  // scan push — different issueIds means stale keys would just become
  // garbage, but a hard reset keeps memory bounded and the behaviour
  // predictable. The cursor value is the index of the *next* click —
  // i.e., on entry it is 0, and after navigating to indices[0] it becomes 1.
  private locateCursors = new Map<string, number>();

  constructor(
    private readonly results: DataGuardResultsService,
    private readonly autoTrigger: DataGuardAutoTriggerService,
    private readonly settings: DataGuardSettingsService,
    private readonly workflowActionService: WorkflowActionService,
    private readonly rowNavigator: DataGuardRowNavigatorService,
    private readonly notificationService: NotificationService
  ) {}

  // ---------------- floating reopen button ----------------

  /** Shield toggle in the toolbar gates the floater. When the user explicitly
   *  turns DataGuard off for this workflow, the floater must disappear too —
   *  otherwise the "OFF" toggle would be a lie. */
  public get shieldOn(): boolean {
    const wid = this.workflowActionService.getWorkflowMetadata()?.wid;
    if (wid === undefined) return true;
    return this.settings.isEnabled(wid);
  }

  /** Visible iff: panel is closed (idle) AND the shield toggle is on. */
  public get showFloater(): boolean {
    return this.scan.state === "idle" && this.shieldOn;
  }

  /** User clicked the floating DataGuard icon. Always triggers a fresh scan
   *  of whatever dataset operator is on the canvas — that's what the user
   *  picked when we asked "click behavior?". */
  public onFloaterClick(): void {
    void this.autoTrigger.rescanAny();
  }

  ngOnInit(): void {
    // Track the issueId set of the previous push. `updateEntry` rebuilds the
    // entries array on every verdict toggle (`.map(...)`), so identity-compare
    // would spuriously reset cursors mid-review. Instead, reset only when the
    // *set of issueIds* changes — that's the actual "fresh scan" signal.
    let lastIssueIdsKey: string | undefined;
    this.sub = this.results.getState$().subscribe(s => {
      const key = s.entries.map(e => e.issueId).join("|");
      if (key !== lastIssueIdsKey) {
        this.locateCursors.clear();
        lastIssueIdsKey = key;
      }
      this.scan = s;
      // Tally once per state push instead of three full-walks per CD tick.
      let allow = 0,
        deny = 0,
        pending = 0;
      for (const entry of s.entries) {
        if (entry.verdict === "allow") allow++;
        else if (entry.verdict === "deny") deny++;
        else pending++;
      }
      this.selectedCount = allow;
      this.deniedCount = deny;
      this.pendingCount = pending;
    });
    // Subscribe to operator-add / property-change so dropping a dataset
    // operator on the canvas triggers /scan via the auto-trigger pipeline.
    // Without this, the checklist never opens on its own.
    this.orchestrationSub = this.autoTrigger.startOrchestration();
  }

  ngOnDestroy(): void {
    this.sub?.unsubscribe();
    this.orchestrationSub?.unsubscribe();
  }

  // ---------------- visibility helpers ----------------

  public get isOpen(): boolean {
    return this.scan.state !== "idle";
  }

  // ---------------- row actions ----------------

  public onToggleAllow(entry: ChecklistEntry, checked: boolean): void {
    this.results.updateEntry(entry.issueId, { verdict: checked ? "allow" : "pending" });
  }

  public onDeny(entry: ChecklistEntry): void {
    this.results.updateEntry(entry.issueId, { verdict: "deny" });
  }

  public onToggleRemember(entry: ChecklistEntry, checked: boolean): void {
    this.results.updateEntry(entry.issueId, { remember: checked });
  }

  // ---------------- panel actions ----------------

  /** Mark every pending row as Allow. Skips already-denied. */
  public onSelectAll(): void {
    for (const e of this.scan.entries) {
      if (e.verdict === "pending") this.results.updateEntry(e.issueId, { verdict: "allow" });
    }
  }

  /** Mark every pending row as Deny. */
  public onDenyAll(): void {
    for (const e of this.scan.entries) {
      if (e.verdict === "pending") this.results.updateEntry(e.issueId, { verdict: "deny" });
    }
  }

  /** Apply the user's selection. Backend bypasses the LLM / chat entirely. */
  public async onApplySelected(): Promise<void> {
    const decisions = this.scan.entries
      .filter((e): e is ChecklistEntry & { verdict: "allow" | "deny" } => e.verdict !== "pending")
      .map(e => ({
        issueId: e.issueId,
        verdict: e.verdict,
        remember: e.remember,
      }));
    await this.autoTrigger.applyBatch(decisions);
  }

  public onClose(): void {
    this.results.reset();
  }

  public isRescanning = false;

  /** "Scan again": re-runs DataGuard on the current dataset version. After
   *  a previous Apply created v2, a re-scan + Apply produces v3 — letting the
   *  user iterate when the AI missed an issue on the first pass. */
  public async onRescan(): Promise<void> {
    if (this.isRescanning) return;
    this.isRescanning = true;
    try {
      await this.autoTrigger.rescanCurrent();
    } finally {
      this.isRescanning = false;
    }
  }

  // ---------------- show-in-result-panel ----------------

  /**
   * Tooltip text for the "📍" button. Shows "Show next affected row (i of N)"
   * where i is the 1-based index that the *next* click will navigate to. Falls
   * back to a static label when row indices are unknown or empty.
   */
  public locateTooltip(entry: ChecklistEntry): string {
    const rowIndices = entry.issue.affectedRowIndices;
    if (!rowIndices || rowIndices.length === 0) return "Show this row in the result panel";
    if (rowIndices.length === 1) return "Show the affected row in the result panel";
    const cursor = this.locateCursors.get(entry.issueId) ?? 0;
    const next = (cursor % rowIndices.length) + 1;
    return `Show next affected row (${next} of ${rowIndices.length})`;
  }

  /**
   * "📍" affordance: focus the source operator's result panel and jump to the
   * next affected row, cycling through `affectedRowIndices` with a per-row
   * cursor. Repeated clicks walk every affected row and wrap to the first.
   * Length-1 rows re-emit the same navigator event so the result-panel pulse
   * re-fires — the user has to *feel* that the click did something.
   *
   * Operator highlight + result-panel open are dispatched synchronously so the
   * ResultTableFrameComponent has time to mount before we publish the row-nav
   * event in a microtask — otherwise our subscriber on the table side may not
   * exist yet on the first click after a panel close.
   */
  public onShowInResultPanel(entry: ChecklistEntry): void {
    const opId = this.scan.sourceOperatorId;
    if (!opId) {
      this.notificationService.warning("DataGuard: no source operator recorded for this scan.");
      return;
    }
    const jointGraph = this.workflowActionService.getJointGraphWrapper();
    const operatorExists = this.workflowActionService
      .getTexeraGraph()
      .getAllOperators()
      .some(op => op.operatorID === opId);
    if (!operatorExists) {
      this.notificationService.warning("DataGuard: the source operator was removed from the canvas.");
      return;
    }

    const currentlyHighlighted = jointGraph.getCurrentHighlightedOperatorIDs();
    if (!(currentlyHighlighted.length === 1 && currentlyHighlighted[0] === opId)) {
      jointGraph.unhighlightOperators(...currentlyHighlighted);
      jointGraph.highlightOperators(opId);
    }
    this.workflowActionService.openResultPanel();

    const rowIndices = entry.issue.affectedRowIndices;
    if (rowIndices === undefined) {
      this.notificationService.info(
        "DataGuard: opened the result panel — row indices weren't recorded for this issue."
      );
      return;
    }
    if (rowIndices.length === 0) {
      this.notificationService.info(
        "DataGuard: opened the result panel — no rows are affected by this issue."
      );
      return;
    }
    // Cycle through affectedRowIndices: each click advances this row's cursor
    // by one and wraps modulo length. Length-1 rows still emit so the result
    // panel re-pulses on every click. Pure helper for testability.
    const cursor = this.locateCursors.get(entry.issueId) ?? 0;
    const step = DataGuardRowNavigatorService.nextCycleStep(rowIndices, cursor);
    this.locateCursors.set(entry.issueId, step.nextCursor);
    // Defer one microtask so the table frame mounts before we ask it to page.
    queueMicrotask(() =>
      this.rowNavigator.navigate({
        operatorId: opId,
        rowIndex: step.value,
        column: entry.issue.column,
      })
    );
  }

  // ---------------- display helpers ----------------

  public riskTierLabel(entry: ChecklistEntry): string {
    return entry.proposal?.riskTier ?? "—";
  }

  /** Human-readable category name for the row title. Maps the raw issueType
   *  enum to plain English so non-technical users see "Missing value" instead
   *  of "missing_value". */
  public categoryLabel(entry: ChecklistEntry): string {
    return this.categoryLabelForType(entry.issue.issueType);
  }

  /** Aggregate the current entries by category — drives the at-a-glance
   *  "2 Missing values · 3 Placeholder values" summary above the row list. */
  public categorySummary(): Array<{ label: string; count: number }> {
    const counts = new Map<string, number>();
    for (const entry of this.scan.entries) {
      const label = this.categoryLabel(entry);
      counts.set(label, (counts.get(label) ?? 0) + 1);
    }
    return Array.from(counts.entries()).map(([label, count]) => ({ label, count }));
  }

  private categoryLabelForType(issueType: string): string {
    switch (issueType) {
      case "missing_value":
        return "Missing value";
      case "placeholder_value":
        return "Placeholder value";
      case "duplicate_id":
        return "Duplicate row";
      case "outlier":
        // The validRanges-based outlier — the earlier z-score variant was
        // removed and "out_of_range" was renamed into this one.
        return "Outlier";
      case "inconsistent_label":
        return "Inconsistent label";
      default:
        return issueType;
    }
  }

  public statusBadge(): string {
    switch (this.scan.state) {
      case "scanning":
        return "Checking…";
      case "applying":
        return "Fixing…";
      case "ready":
        return `${this.scan.entries.length} to review`;
      case "done":
        return "Done";
      case "error":
        return "Problem";
      default:
        return "";
    }
  }
}
