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

import { ChangeDetectorRef, Component, Input, OnChanges, OnDestroy, OnInit, SimpleChanges } from "@angular/core";
import { Subject, race, timer } from "rxjs";
import { filter, take } from "rxjs/operators";
import { NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import {
  NzTableQueryParams,
  NzTableComponent,
  NzTheadComponent,
  NzTrDirective,
  NzTableCellDirective,
  NzThMeasureDirective,
  NzTbodyComponent,
  NzCellEllipsisDirective,
} from "ng-zorro-antd/table";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../../../service/workflow-result/workflow-result.service";
import { PanelResizeService } from "../../../service/workflow-result/panel-resize/panel-resize.service";
import { isWebPaginationUpdate, OperatorState } from "../../../types/execute-workflow.interface";
import { IndexableObject, TableColumn } from "../../../types/result-table.interface";
import { RowModalComponent } from "../result-panel-modal.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DomSanitizer, SafeHtml } from "@angular/platform-browser";
import { ResultExportationComponent } from "../../result-exportation/result-exportation.component";
import { WorkflowStatusService } from "../../../service/workflow-status/workflow-status.service";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { DataGuardRowNavigatorService } from "../../../service/agent/data-guard-row-navigator.service";
import { NgIf, NgFor, NgClass } from "@angular/common";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";

/**
 * The Component will display the result in an excel table format,
 *  where each row represents a result from the workflow,
 *  and each column represents the type of result the workflow returns.
 *
 * Clicking each row of the result table will create an pop-up window
 *  and display the detail of that row in a pretty json format.
 */
@UntilDestroy()
@Component({
  selector: "texera-result-table-frame",
  templateUrl: "./result-table-frame.component.html",
  styleUrls: ["./result-table-frame.component.scss"],
  imports: [
    NgIf,
    NzSpaceCompactItemDirective,
    NzInputDirective,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzTableComponent,
    NzTheadComponent,
    NzTrDirective,
    NgFor,
    NzTableCellDirective,
    NzThMeasureDirective,
    NgClass,
    NzTbodyComponent,
    NzCellEllipsisDirective,
  ],
})
export class ResultTableFrameComponent implements OnInit, OnChanges, OnDestroy {
  // DataGuard locate-row highlight duration. Must match the SCSS animation
  // (which is set to iteration-count: infinite + clears when the class drops).
  private static readonly HIGHLIGHT_DURATION_MS = 2000;
  @Input() operatorId?: string;

  // display result table
  currentColumns?: TableColumn[];
  currentResult: IndexableObject[] = [];
  //   for more details
  //   see https://ng.ant.design/components/table/en#components-table-demo-ajax
  isFrontPagination: boolean = true;

  isLoadingResult: boolean = false;

  // paginator section, used when displaying rows

  // this attribute stores whether front-end should handle pagination
  //   if false, it means the pagination is managed by the server
  // this starts from **ONE**, not zero
  currentPageIndex: number = 1;
  totalNumTuples: number = 0;
  pageSize = 5;
  currentColumnOffset = 0;
  columnLimit = 25;
  columnSearch = "";
  panelHeight = 0;
  tableStats: Record<string, Record<string, number>> = {};
  prevTableStats: Record<string, Record<string, number>> = {};
  widthPercent: string = "";
  isOperatorFinished: boolean = false;

  // DataGuard "show this row" highlight. Indexed in *page* coordinates so we
  // can match against the same `let i = index` the *ngFor uses.
  highlightedRowIndexInPage: number | null = null;
  highlightedColumn: string | null = null;
  private highlightTimer: ReturnType<typeof setTimeout> | null = null;
  // Fires (with pageIndex) after a paginated page has been rendered into the
  // table. The DataGuard locate-flow waits on this — semantically "I want to
  // know when the page is shown" rather than "when the HTTP responds" — so
  // we never double-subscribe to the cold `selectPage` Observable.
  private readonly pageRendered$ = new Subject<number>();
  // DataGuard locate-flow token. Each new navigate request bumps this; every
  // async branch in handleLocateByKey / handleLocateByIndex captures the value
  // at request time and bails (emitting `flashed: false` exactly once) if the
  // captured token no longer matches `currentLocateToken` by the time the
  // branch resumes. This kills the rapid-click race where an older walk would
  // otherwise complete after a newer one and flash the wrong row, and also
  // guarantees the checklist's cursor-advancement Promise resolves for the
  // superseded request instead of hanging until the 6 s safety timeout.
  private currentLocateToken: number = 0;

  constructor(
    private modalService: NzModalService,
    private workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService,
    private resizeService: PanelResizeService,
    private changeDetectorRef: ChangeDetectorRef,
    private sanitizer: DomSanitizer,
    private workflowStatusService: WorkflowStatusService,
    private guiConfigService: GuiConfigService,
    private dataGuardRowNavigator: DataGuardRowNavigatorService
  ) {}

  // DataGuard locate-by-key: max pages to walk before giving up. Without a cap
  // a malformed key could keep selectPage'ing forever; 10 covers typical
  // demo / hackathon datasets (e.g. 5 rows/page × 10 pages = 50 rows, larger
  // than every fixture we ship).
  private static readonly LOCATE_BY_KEY_MAX_PAGES = 10;

  ngOnChanges(changes: SimpleChanges): void {
    this.operatorId = changes.operatorId?.currentValue;
    if (this.operatorId) {
      const paginatedResultService = this.workflowResultService.getPaginatedResultService(this.operatorId);
      if (paginatedResultService) {
        this.isFrontPagination = false;
        this.totalNumTuples = paginatedResultService.getCurrentTotalNumTuples();
        this.currentPageIndex = paginatedResultService.getCurrentPageIndex();
        this.changePaginatedResultData();

        this.tableStats = paginatedResultService.getStats();
        this.prevTableStats = this.tableStats;
      }
    }
  }

  ngOnInit(): void {
    this.workflowStatusService
      .getStatusUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(statusMap => {
        if (this.operatorId && statusMap[this.operatorId]?.operatorState === OperatorState.Completed) {
          this.isOperatorFinished = true;
          this.changeDetectorRef.detectChanges();
        } else {
          this.isOperatorFinished = false;
        }
      });

    this.columnLimit = this.guiConfigService.env.limitColumns;

    this.workflowResultService
      .getResultUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(update => {
        if (!this.operatorId) {
          return;
        }
        const opUpdate = update[this.operatorId];
        if (!opUpdate || !isWebPaginationUpdate(opUpdate)) {
          return;
        }
        let columnCount = this.currentColumns?.length;
        if (columnCount) this.widthPercent = (1 / columnCount) * 100 + "%";
        this.isFrontPagination = false;
        this.totalNumTuples = opUpdate.totalNumTuples;
        if (opUpdate.dirtyPageIndices.includes(this.currentPageIndex)) {
          this.changePaginatedResultData();
        }
        this.changeDetectorRef.detectChanges();
      });

    this.workflowResultService
      .getResultTableStats()
      .pipe(untilDestroyed(this))
      .subscribe(([prevStats, currentStats]) => {
        if (!this.operatorId) {
          return;
        }

        if (currentStats[this.operatorId]) {
          this.tableStats = currentStats[this.operatorId];
          if (prevStats[this.operatorId] && this.checkKeys(this.tableStats, prevStats[this.operatorId])) {
            this.prevTableStats = prevStats[this.operatorId];
          } else {
            this.prevTableStats = this.tableStats;
          }
        }
      });

    this.resizeService.currentSize.pipe(untilDestroyed(this)).subscribe(size => {
      this.panelHeight = size.height;
      this.adjustPageSizeBasedOnPanelSize(size.height);
      let currentPageNum: number = Math.ceil(this.totalNumTuples / this.pageSize);
      while (this.currentPageIndex > currentPageNum && this.currentPageIndex > 1) {
        this.currentPageIndex -= 1;
      }
    });

    if (this.operatorId) {
      const paginatedResultService = this.workflowResultService.getPaginatedResultService(this.operatorId);
      if (paginatedResultService) {
      }
    }

    // DataGuard checklist row click → page to + flash the affected row.
    // Only honored for this frame's operator; other ResultTableFrames ignore.
    this.dataGuardRowNavigator
      .getNav$()
      .pipe(untilDestroyed(this))
      .subscribe(req => {
        if (!this.operatorId || req.operatorId !== this.operatorId) {
          // Wrong operator: this frame is not the target. Don't emit a
          // flashResult — some other frame (the right one) will handle it.
          // Note: if there is NO right frame at all (operator was deleted),
          // the checklist's awaitFlashResult timeout (6 s) catches it.
          return;
        }
        // Token-cancel any in-flight walk from a prior request. The old walk's
        // captured token no longer matches currentLocateToken, so its next
        // async resumption will short-circuit + emit `flashed: false` for its
        // own requestId. This is what makes rapid clicks safe.
        this.currentLocateToken = req.requestId;
        // Branch on rowKey: if the server included a fingerprint, prefer it
        // (handles Texera's JSONL multi-worker row-shuffle). Fall back to the
        // raw index path when rowKey is absent or no row matches anywhere.
        if (req.rowKey !== undefined) {
          this.handleLocateByKey(
            req.operatorId,
            req.requestId,
            req.rowKey,
            req.column,
            req.rowIndex,
            req.rowKeyOccurrence ?? 0
          );
        } else {
          this.handleLocateByIndex(req.operatorId, req.requestId, req.rowIndex, req.column);
        }
      });
  }

  /**
   * Centralised flash-result reporter. Wraps the navigator service call so
   * every exit point in the locate flow goes through one place — easier to
   * audit "did we report on every branch?".
   */
  private reportFlashResult(requestId: number, flashed: boolean): void {
    this.dataGuardRowNavigator.reportFlashResult({ requestId, flashed });
  }

  /**
   * Locate a row by its content fingerprint (`rowKey`). Scans the currently
   * loaded page first (fast path — common when the issue is on the first
   * page); on a miss walks subsequent pages up to LOCATE_BY_KEY_MAX_PAGES.
   * When no match is found anywhere, emits `flashed: false` — the checklist
   * caller observes the false outcome and surfaces a "row not found" toast,
   * since at that point the data has drifted from the scan and silently
   * highlighting a byte-order-index row would mislead the user.
   *
   * Same operator-id captured-at-click-time guard as the index path — bails
   * silently if the user switched operators mid-page-load.
   */
  private handleLocateByKey(
    requestOperatorId: string,
    requestId: number,
    rowKey: string,
    column: string | undefined,
    fallbackIndex: number,
    occurrence: number
  ): void {
    // Every async resumption MUST first check that our captured requestId is
    // still the current one. If a newer click superseded us, emit
    // `flashed: false` (so the older request's cursor stays put) and bail.
    // Returns `true` when the caller should bail.
    const isSuperseded = (): boolean => this.currentLocateToken !== requestId;

    // Cumulative count of matches we've already walked past in earlier pages.
    // The `occurrence` semantic is global (across the full result set), so the
    // per-page lookup is `findNthRowByKey(...,occurrence - matchesSeenBeforeCurrentPage)`.
    // When that returns -1, the page has K matches but they're earlier
    // occurrences than the one we want — we advance the counter by K and move
    // to the next page. This is critical for the duplicate-row case where the
    // 4 dup rows can land on any 4 of the result-panel's display rows
    // (potentially split across multiple pages by Texera's parallel scan).
    let matchesSeenBeforeCurrentPage = 0;

    const tryFlashOnCurrentPage = (): { flashed: boolean; matchesOnThisPage: number } => {
      if (this.operatorId !== requestOperatorId) {
        // bail, but treat as "handled" — return matchesOnThisPage=0 so the
        // caller doesn't bump the cumulative counter on a stale frame.
        return { flashed: true, matchesOnThisPage: 0 };
      }
      const columns = this.currentColumns?.map(c => c.columnDef) ?? [];
      const rows = this.currentResult as ReadonlyArray<Record<string, unknown>>;
      const wantWithinPage = occurrence - matchesSeenBeforeCurrentPage;
      const rowInPage = DataGuardRowNavigatorService.findNthRowByKey(rows, columns, rowKey, wantWithinPage);
      if (rowInPage >= 0) {
        this.flashRow(rowInPage, column);
        return { flashed: true, matchesOnThisPage: 0 };
      }
      // No match (or not enough matches) on this page: tally how many matches
      // ARE on this page so the walker can advance the cumulative counter.
      const matchesOnThisPage = DataGuardRowNavigatorService.countMatchesByKey(rows, columns, rowKey);
      return { flashed: false, matchesOnThisPage };
    };

    // 1. Fast path — only safe for `occurrence === 0` (the pre-occurrence
    //    behaviour). When occurrence > 0 we can't trust a fast-path match on
    //    the current page: the user may be on page 5 of 10 and the first
    //    match here might be the 2nd or 3rd global occurrence, not the 1st.
    //    Force the slow walk so the canonical page order is honoured.
    if (occurrence === 0) {
      const fast = tryFlashOnCurrentPage();
      if (fast.flashed) {
        this.reportFlashResult(requestId, true);
        return;
      }
    }
    // The slow walk below starts from page 1 unconditionally and counts
    // matches in canonical page order via matchesSeenBeforeCurrentPage, so it
    // gives a deterministic answer regardless of which page is currently
    // displayed.

    // 2. Walk subsequent pages. Start from page 1 (not currentPageIndex+1)
    //    because the user may not be on page 1 when they click — the affected
    //    row could be earlier. Stop at LOCATE_BY_KEY_MAX_PAGES or when we run
    //    out of tuples.
    const totalPages = Math.max(1, Math.ceil(this.totalNumTuples / Math.max(this.pageSize, 1)));
    const lastPage = Math.min(totalPages, ResultTableFrameComponent.LOCATE_BY_KEY_MAX_PAGES);
    const walkPage = (pageIndex: number) => {
      if (isSuperseded()) {
        this.reportFlashResult(requestId, false);
        return;
      }
      if (this.operatorId !== requestOperatorId) {
        // User switched operators mid-walk. Treat as silent skip — the new
        // operator's frame (if any) handles its own requests, this request
        // never produced a flash here.
        this.reportFlashResult(requestId, false);
        return;
      }
      if (pageIndex > lastPage) {
        // Exhausted the search window. We deliberately do NOT fall back to the
        // file-byte-order index here — that path is correct only for single-
        // worker output, and on multi-worker JSONL it silently lands on the
        // wrong cell (worker-shuffle puts an unrelated row at the byte-order
        // position). Earlier versions toasted on every click because the
        // pre-`isCellMissing`-fingerprint contract mismatched 100% of the time
        // for null cells; with the round-6 fingerprint normalisation in place
        // a miss now only happens when the data genuinely no longer matches
        // the scan (post-Apply drift, schema change, etc.) — surfacing that
        // is exactly what we want. Emit `flashed: false`; the checklist
        // caller decides whether to toast based on whether a `rowKey` was in
        // the request.
        this.reportFlashResult(requestId, false);
        return;
      }
      this.currentPageIndex = pageIndex;
      race(
        this.pageRendered$.pipe(
          filter(p => p === pageIndex),
          take(1)
        ),
        timer(3000)
      )
        .pipe(take(1), untilDestroyed(this))
        .subscribe(() => {
          if (isSuperseded()) {
            this.reportFlashResult(requestId, false);
            return;
          }
          const attempt = tryFlashOnCurrentPage();
          if (attempt.flashed) {
            this.reportFlashResult(requestId, true);
            return;
          }
          matchesSeenBeforeCurrentPage += attempt.matchesOnThisPage;
          walkPage(pageIndex + 1);
        });
      this.changePaginatedResultData();
    };
    walkPage(1);
  }

  /**
   * Legacy index-based path. Used as a fallback when `rowKey` is absent
   * (CSV single-worker, older agent-service builds) or when no key match is
   * found anywhere in the result panel within LOCATE_BY_KEY_MAX_PAGES.
   */
  private handleLocateByIndex(
    requestOperatorId: string,
    requestId: number,
    rowIndex: number,
    column: string | undefined
  ): void {
    const isSuperseded = (): boolean => this.currentLocateToken !== requestId;
    // applyFlash returns true if it actually flashed, false if the index path
    // bailed (operator switched, totalNumTuples=0, rowIndex out-of-bounds).
    // The "no rows" guard catches the empty-click test case: index fallback
    // can't flash anything when totalNumTuples is 0.
    const applyFlash = (): boolean => {
      if (this.operatorId !== requestOperatorId) return false;
      if (rowIndex < 0 || (this.totalNumTuples > 0 && rowIndex >= this.totalNumTuples)) {
        return false;
      }
      if (this.totalNumTuples === 0 || this.currentResult.length === 0) {
        return false;
      }
      const targetPage = DataGuardRowNavigatorService.pageIndexFor(rowIndex, this.pageSize);
      const rowInPage = rowIndex - (targetPage - 1) * this.pageSize;
      // The page may have fewer rows than expected (last page short-fill); if
      // the computed in-page index is out of range, don't flash a phantom row.
      if (rowInPage < 0 || rowInPage >= this.currentResult.length) {
        return false;
      }
      this.flashRow(rowInPage, column);
      return true;
    };
    const targetPage = DataGuardRowNavigatorService.pageIndexFor(rowIndex, this.pageSize);
    if (this.currentPageIndex !== targetPage) {
      this.currentPageIndex = targetPage;
      race(
        this.pageRendered$.pipe(
          filter(p => p === targetPage),
          take(1)
        ),
        timer(3000)
      )
        .pipe(take(1), untilDestroyed(this))
        .subscribe(() => {
          if (isSuperseded()) {
            this.reportFlashResult(requestId, false);
            return;
          }
          this.reportFlashResult(requestId, applyFlash());
        });
      this.changePaginatedResultData();
    } else {
      if (isSuperseded()) {
        this.reportFlashResult(requestId, false);
        return;
      }
      this.reportFlashResult(requestId, applyFlash());
    }
  }

  /** Shared flash routine: sets highlight state for HIGHLIGHT_DURATION_MS. */
  private flashRow(rowInPage: number, column: string | undefined): void {
    this.highlightedRowIndexInPage = rowInPage;
    this.highlightedColumn = column ?? null;
    this.changeDetectorRef.detectChanges();
    if (this.highlightTimer !== null) {
      clearTimeout(this.highlightTimer);
    }
    this.highlightTimer = setTimeout(() => {
      this.highlightedRowIndexInPage = null;
      this.highlightedColumn = null;
      this.highlightTimer = null;
      this.changeDetectorRef.detectChanges();
    }, ResultTableFrameComponent.HIGHLIGHT_DURATION_MS);
  }

  ngOnDestroy(): void {
    // @UntilDestroy handles RxJS subs but not raw timers — clear so the late
    // callback can't fire detectChanges() on a destroyed view (NG0911).
    if (this.highlightTimer !== null) {
      clearTimeout(this.highlightTimer);
      this.highlightTimer = null;
    }
  }

  checkKeys(
    currentStats: Record<string, Record<string, number>>,
    prevStats: Record<string, Record<string, number>>
  ): boolean {
    let firstSet = Object.keys(currentStats);
    let secondSet = Object.keys(prevStats);

    if (firstSet.length != secondSet.length) {
      return false;
    }

    for (let i = 0; i < firstSet.length; i++) {
      if (firstSet[i] != secondSet[i]) {
        return false;
      }
    }

    return true;
  }

  compare(field: string, stats: string): SafeHtml {
    let current = this.tableStats[field][stats];
    let previous = this.prevTableStats[field][stats];
    let currentStr: string;
    let previousStr: string;

    if (typeof current === "number" && typeof previous === "number") {
      currentStr = current.toFixed(2);
      previousStr = previous !== undefined ? previous.toFixed(2) : currentStr;
    } else {
      currentStr = current.toLocaleString();
      previousStr = previous !== undefined ? previous.toLocaleString() : currentStr;
    }
    let styledValue = "";

    if (this.isOperatorFinished) {
      for (let i = 0; i < currentStr.length; i++) {
        styledValue += `<span style="color: black">${currentStr[i]}</span>`;
      }
      return this.sanitizer.bypassSecurityTrustHtml(styledValue);
    }

    for (let i = 0; i < currentStr.length; i++) {
      const char = currentStr[i];
      const prevChar = previousStr[i];

      if (char !== prevChar) {
        styledValue += `<span style="color: blue">${char}</span>`;
      } else {
        styledValue += `<span style="color: black">${char}</span>`;
      }
    }

    return this.sanitizer.bypassSecurityTrustHtml(styledValue);
  }

  /**
   * Adjusts the number of result rows displayed per page based on the
   * available vertical space of the Texera results panel.
   *
   * The method accounts for fixed UI elements within the panel—such as
   * headers, column navigation controls, pagination, and the search bar—
   * to determine the remaining space available for rendering result rows.
   * The page size is then recalculated using the height of a single table row.
   *
   * To maintain a stable user experience during panel resizes, the current
   * page index is recomputed so that the previously visible results remain
   * in view and the user does not experience an abrupt jump in the dataset.
   *
   * @param panelHeight - The total height (in pixels) of the results panel.
   */
  private adjustPageSizeBasedOnPanelSize(panelHeight: number) {
    const TABLE_HEADER_HEIGHT = 38.62;
    const PANEL_HEADER_HEIGHT = 64.27; // Includes panel title and tab bar
    const COLUMN_NAVIGATION_HEIGHT = 56.6; // Previous/Next columns controls
    const PAGINATION_HEIGHT = 32.63;
    const SEARCH_BAR_HEIGHT_WITH_MARGIN = 77; // Approximate height for search bar and margins
    const ROW_HEIGHT = 38.62;

    const usedHeight =
      TABLE_HEADER_HEIGHT +
      PANEL_HEADER_HEIGHT +
      COLUMN_NAVIGATION_HEIGHT +
      PAGINATION_HEIGHT +
      SEARCH_BAR_HEIGHT_WITH_MARGIN;

    const newPageSize = Math.max(1, Math.floor((panelHeight - usedHeight) / ROW_HEIGHT));

    const oldOffset = (this.currentPageIndex - 1) * this.pageSize;

    this.pageSize = newPageSize;
    this.resizeService.pageSize = newPageSize;

    this.currentPageIndex = Math.floor(oldOffset / newPageSize) + 1;
  }

  /**
   * Callback function for table query params changed event
   *   params containing new page index, new page size, and more
   *   (this function will be called when user switch page)
   *
   * @param params new parameters
   */
  onTableQueryParamsChange(params: NzTableQueryParams) {
    if (this.isFrontPagination) {
      return;
    }
    if (!this.operatorId) {
      return;
    }
    this.currentPageIndex = params.pageIndex;

    this.changePaginatedResultData();
  }

  /**
   * Opens the model to display the row details in
   *  pretty json format when clicked. User can view the details
   *  in a larger, expanded format.
   */
  open(indexInPage: number, rowData: IndexableObject): void {
    const currentRowIndex = indexInPage + (this.currentPageIndex - 1) * this.pageSize;
    // open the modal component
    const modalRef: NzModalRef<RowModalComponent> = this.modalService.create({
      // modal title
      nzTitle: "Row Details",
      nzContent: RowModalComponent,
      nzData: { operatorId: this.operatorId, rowIndex: currentRowIndex }, // set the index value and page size to the modal for navigation
      // prevent browser focusing close button (ugly square highlight)
      nzAutofocus: null,
      // modal footer buttons
      nzFooter: [
        {
          label: "<",
          onClick: () => {
            const component = modalRef.componentInstance;
            if (component) {
              component.rowIndex -= 1;
              this.currentPageIndex = Math.floor(component.rowIndex / this.pageSize) + 1;
              component.ngOnChanges();
            }
          },
          disabled: () => modalRef.componentInstance?.rowIndex === 0,
        },
        {
          label: ">",
          onClick: () => {
            const component = modalRef.componentInstance;
            if (component) {
              component.rowIndex += 1;
              this.currentPageIndex = Math.floor(component.rowIndex / this.pageSize) + 1;
              component.ngOnChanges();
            }
          },
          disabled: () => modalRef.componentInstance?.rowIndex === this.totalNumTuples - 1,
        },
        {
          label: "OK",
          onClick: () => {
            modalRef.destroy();
          },
          type: "primary",
        },
      ],
    });
  }

  // frontend table data must be changed, because:
  // 1. result panel is opened - must display currently selected page
  // 2. user selects a new page - must display new page data
  // 3. current page is dirty - must re-fetch data
  //
  changePaginatedResultData(): void {
    if (!this.operatorId) {
      return;
    }
    const paginatedResultService = this.workflowResultService.getPaginatedResultService(this.operatorId);
    if (!paginatedResultService) {
      return;
    }
    this.isLoadingResult = true;
    paginatedResultService
      .selectPage(this.currentPageIndex, this.pageSize, this.currentColumnOffset, this.columnLimit, this.columnSearch)
      .pipe(untilDestroyed(this))
      .subscribe(pageData => {
        if (this.currentPageIndex === pageData.pageIndex) {
          this.setupResultTable(pageData.table, paginatedResultService.getCurrentTotalNumTuples());
          this.changeDetectorRef.detectChanges();
          // Signal page-rendered AFTER setup + CD so the locate-flow's flash
          // lands on the freshly rendered rows, not stale ones.
          this.pageRendered$.next(pageData.pageIndex);
        }
      });
  }

  /**
   * Updates all the result table properties based on the execution result,
   *  displays a new data table with a new paginator on the result panel.
   *
   * @param resultData rows of the result (may not be all rows if displaying result for workflow completed event)
   * @param totalRowCount
   */
  setupResultTable(resultData: ReadonlyArray<IndexableObject>, totalRowCount: number) {
    if (!this.operatorId) {
      return;
    }
    if (resultData.length < 1) {
      return;
    }

    this.isLoadingResult = false;
    this.changeDetectorRef.detectChanges();

    // creates a shallow copy of the readonly response.result,
    //  this copy will be has type object[] because MatTableDataSource's input needs to be object[]
    this.currentResult = resultData.slice();

    //  1. Get all the column names except '_id', using the first tuple
    //  2. Use those names to generate a list of display columns
    //  3. Pass the result data as array to generate a new data table

    let columns: { columnKey: any; columnText: string }[];

    const columnKeys = Object.keys(resultData[0]).filter(x => x !== "_id");
    columns = columnKeys.map(v => ({ columnKey: v, columnText: v }));

    // generate columnDef from first row, column definition is in order
    this.currentColumns = this.generateColumns(columns);
    this.totalNumTuples = totalRowCount;
  }

  /**
   * Generates all the column information for the result data table
   *
   * @param columns
   */
  generateColumns(columns: { columnKey: any; columnText: string }[]): TableColumn[] {
    return columns.map((col, index) => ({
      columnDef: col.columnKey,
      header: col.columnText,
      getCell: (row: IndexableObject) => row[col.columnKey].toString(),
    }));
  }

  downloadData(data: any, rowIndex: number, columnIndex: number, columnName: string): void {
    const realRowNumber = (this.currentPageIndex - 1) * this.pageSize + rowIndex;
    const defaultFileName = `${columnName}_${realRowNumber}`;
    const modal = this.modalService.create({
      nzTitle: "Export Data and Save to a Dataset",
      nzContent: ResultExportationComponent,
      nzData: {
        exportType: "data",
        workflowName: this.workflowActionService.getWorkflowMetadata.name,
        defaultFileName: defaultFileName,
        rowIndex: realRowNumber,
        columnIndex: columnIndex,
      },
      nzFooter: null,
    });
  }

  onColumnShiftLeft(): void {
    if (this.currentColumnOffset > 0) {
      this.currentColumnOffset = Math.max(0, this.currentColumnOffset - this.columnLimit);
      this.changePaginatedResultData();
    }
  }

  onColumnShiftRight(): void {
    if (this.currentColumns && this.currentColumns.length === this.columnLimit) {
      this.currentColumnOffset += this.columnLimit;
      this.changePaginatedResultData();
    }
  }

  onColumnSearch(event: Event): void {
    const input = event.target as HTMLInputElement;
    this.columnSearch = input.value;
    this.currentColumnOffset = 0;
    this.changePaginatedResultData();
  }
}
