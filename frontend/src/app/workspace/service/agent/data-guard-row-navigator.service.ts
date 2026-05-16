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
import { Observable, ReplaySubject } from "rxjs";

/** Payload of a "show this row in the result panel" request. */
export interface DataGuardRowNavRequest {
  operatorId: string;
  /** 0-based row index in the operator's full result set. */
  rowIndex: number;
  /** Optional column to focus / highlight inside the row. */
  column?: string;
}

/**
 * Tiny pub/sub for the "click an issue → jump to that row in the result panel"
 * affordance. The checklist publishes here; ResultTableFrameComponent subscribes
 * and pages to the right offset + flashes the row.
 *
 * Why ReplaySubject(1, 500ms): the result panel mounts `ResultTableFrameComponent`
 * lazily via NgComponentOutlet — when the checklist click triggers
 * `openResultPanel()`, the frame is created on the next CD tick, *after* the
 * navigator emit fires. A plain Subject would drop the emission. ReplaySubject
 * with a 1-element / 500 ms window replays the most recent request to a
 * cold-mounted subscriber, but only briefly so stale requests don't bleed into
 * unrelated later mounts (e.g., user switches operators a minute later).
 */
@Injectable({ providedIn: "root" })
export class DataGuardRowNavigatorService {
  // TODO: 500 ms is empirical — covers the openResultPanel → NgComponentOutlet
  // mount on a warm laptop. Bump if QA reports drops on slow CPUs / first
  // contentful paint stalls.
  private readonly nav$ = new ReplaySubject<DataGuardRowNavRequest>(1, 500);

  public getNav$(): Observable<DataGuardRowNavRequest> {
    return this.nav$.asObservable();
  }

  public navigate(req: DataGuardRowNavRequest): void {
    this.nav$.next(req);
  }

  /**
   * Pure helper, broken out for unit testability. Pages are 1-based.
   * Logs a warning (does not throw) on negative `rowIndex` — a silent clamp
   * would hide caller bugs.
   */
  public static pageIndexFor(rowIndex: number, pageSize: number): number {
    if (rowIndex < 0) {
      // eslint-disable-next-line no-console
      console.warn(`DataGuardRowNavigatorService: negative rowIndex=${rowIndex}, clamping to page 1`);
      return 1;
    }
    if (pageSize <= 0) return 1;
    return Math.floor(rowIndex / pageSize) + 1;
  }

  /**
   * Advance a per-row cycle cursor through `affectedRowIndices` and return the
   * `{ value, nextCursor }` pair for the click that just happened. Used by
   * the checklist so repeated clicks on the same "📍" button walk every
   * affected row in turn and wrap to the start. Modulo on `length` so length-0
   * inputs are rejected (caller toasts before calling). Negative or NaN
   * cursors are coerced to 0 — defensive against a corrupted Map entry.
   */
  public static nextCycleStep(
    affectedRowIndices: ReadonlyArray<number>,
    cursor: number
  ): { value: number; nextCursor: number } {
    const len = affectedRowIndices.length;
    if (len === 0) {
      throw new Error("DataGuardRowNavigatorService.nextCycleStep: empty affectedRowIndices");
    }
    const safe = Number.isFinite(cursor) && cursor >= 0 ? Math.floor(cursor) : 0;
    const idx = safe % len;
    return { value: affectedRowIndices[idx], nextCursor: idx + 1 };
  }
}
