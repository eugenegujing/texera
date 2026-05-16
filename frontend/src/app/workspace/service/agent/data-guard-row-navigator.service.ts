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
import { Observable, ReplaySubject, Subject, firstValueFrom, race, timer } from "rxjs";
import { filter, map, take } from "rxjs/operators";

/**
 * Outcome of a navigate() request, emitted on `flashResult$` once the
 * result-table-frame either successfully flashed a row or aborted the walk
 * (superseded by a newer request, page-render timeout, or no row found).
 *
 * The checklist component subscribes filtered to its own `requestId` so it can
 * advance its per-row locate cursor ONLY when a flash truly landed — silent
 * skips (e.g. rapid double-clicks where the older walk gets cancelled, or
 * empty pages where neither key nor index lookup matches) leave the cursor
 * unchanged so the next click retries the same target instead of skipping it.
 */
export interface DataGuardRowNavResult {
  requestId: number;
  flashed: boolean;
}

/** Payload of a "show this row in the result panel" request. */
export interface DataGuardRowNavRequest {
  /**
   * Monotonically increasing id assigned by `navigate()`. The result-table-frame
   * captures the latest id on each request and bails out of older async walks
   * whose captured id no longer matches the current one. Surfaced on the
   * request payload so subscribers can correlate the request to its
   * `flashResult$` emission.
   */
  requestId: number;
  operatorId: string;
  /**
   * 0-based row index in the operator's full result set, as seen by the
   * agent-service profiler (file-byte order). Used as a fall-back target for
   * CSV single-worker flows where the result panel order matches the
   * profiler order; ignored when `rowKey` is set and matched successfully.
   */
  rowIndex: number;
  /**
   * Content-based fingerprint of the affected row. Produced by the
   * agent-service `rowFingerprint` helper. Texera's multi-worker JSONL scan
   * shuffles rows into worker-arrival order in the result panel, so the
   * profiler-side `rowIndex` no longer reliably points at the right cell.
   * Matching by `rowKey` instead lets the result-table-frame find the
   * affected row regardless of display order.
   *
   * Fingerprint contract (must match `rowFingerprint` in
   * `agent-service/src/agent/tools/dataguard/profile-dataset.ts`):
   *   - Sort the dataset's column names alphabetically.
   *   - For each column in canonical order, `JSON.stringify` the cell value
   *     (treat `undefined` and missing keys as `null`).
   *   - Concatenate the per-cell JSON tokens with an empty separator.
   */
  rowKey?: string;
  /** Optional column to focus / highlight inside the row. */
  column?: string;
  /**
   * For duplicate-row issues every affected row has IDENTICAL content, so all
   * entries of `affectedRowKeys` point to the SAME fingerprint string. A naive
   * `findRowByKey` would return the first matching display row on every click,
   * collapsing 4 dup clicks into 2 visible flashes. This field lets the caller
   * say "I want the Nth display match of this fingerprint" — the checklist
   * counts how many times the current key appeared earlier in the cursor walk
   * and passes that as the occurrence so each click lands on a distinct row.
   *
   * 0-indexed (0 = first display match, 1 = second, …). Defaults to 0 when
   * omitted, preserving the pre-existing single-match semantics for unique
   * fingerprints (the common case — missing values, placeholders, outliers,
   * inconsistent labels are all per-row unique keys).
   *
   * Crucially, the result-table-frame's page walk treats this occurrence
   * **cumulatively across pages**: if page 1 has 1 match and the user wanted
   * occurrence=1, the walker advances past page 1 and looks for the first
   * match on subsequent pages — it does NOT pick the first match on page 2.
   * See `handleLocateByKey` for the matchesSeenBeforeCurrentPage accumulator.
   */
  rowKeyOccurrence?: number;
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
  // Multi-cast pipe of {requestId, flashed} completion signals. Plain Subject
  // (not Replay) because navigate() now wires the awaiter subscription BEFORE
  // it publishes the request — see the round-4 fix below — so synchronous
  // fast-path emissions from the table-frame are caught. A ReplaySubject would
  // also work but would leak previous-request results into new awaiters with
  // colliding ids if the buffer didn't drain in time. Cleaner to subscribe
  // first.
  private readonly flashResult$ = new Subject<DataGuardRowNavResult>();
  // Monotonic request-id source. Incremented on every navigate() call so the
  // result-table-frame can identify the latest request and bail older walks.
  private requestSeq = 0;
  // Safety timeout for the navigate() Promise. The table-frame's locate-by-key
  // walk can chain up to LOCATE_BY_KEY_MAX_PAGES (10) page-render races at
  // 3 s each = 30 s worst case; we add a 5 s buffer for setup/teardown and the
  // optional handleLocateByIndex fallback that runs AFTER the walk exhausts.
  // The timeout exists ONLY as a deadlock guard against a wedged table-frame
  // that mounts but never reports; in the normal case the frame always
  // reports promptly. Derived from a single explicit constant (rather than
  // importing the table-frame's own constants) to keep this service free of
  // any reverse dependency on the result-panel component layer.
  // Formula: LOCATE_BY_KEY_MAX_PAGES (10) * pageRendered$ race cap (3500 ms,
  // includes a small slack over the 3 s race) + 1000 ms component-setup slack.
  private static readonly FLASH_RESULT_TIMEOUT_MS = 10 * 3500 + 1000; // 36000

  public getNav$(): Observable<DataGuardRowNavRequest> {
    return this.nav$.asObservable();
  }

  /**
   * Hot stream of `{requestId, flashed}` completions. Subscribers correlate
   * by `requestId` because the table-frame may emit out-of-order results when
   * an older walk completes after a newer click superseded it (the older walk
   * gets `flashed: false`, the newer one `flashed: true`).
   */
  public getFlashResult$(): Observable<DataGuardRowNavResult> {
    return this.flashResult$.asObservable();
  }

  /**
   * Publish a navigate request AND return a Promise that resolves to the
   * flash outcome (`true` = a row actually pulsed on screen, `false` = the
   * request was superseded by a newer click / timed out / found nothing).
   *
   * CRITICAL ordering (round-4 regression fix): the awaiter subscription is
   * wired to `flashResult$` BEFORE `nav$.next()` fires the request. If the
   * target row is already on the rendered page, the table-frame's
   * `tryFlashOnCurrentPage` calls `reportFlashResult` synchronously inside
   * the `next()` chain. With subscribe-after-next (the old shape) that
   * emission would be lost to a plain Subject and the awaiter would hang
   * until the 36 s safety timeout, leaving the locate cursor stuck on
   * `indices[0]` for every cycle. By subscribing first we catch the
   * synchronous emission.
   *
   * Returns a `Promise<boolean>` directly; callers do `const flashed =
   * await navigator.navigate(req)` and only advance their cursor on `true`.
   */
  public navigate(req: Omit<DataGuardRowNavRequest, "requestId">): Promise<boolean> {
    const requestId = ++this.requestSeq;
    const result$ = this.flashResult$.pipe(
      filter(r => r.requestId === requestId),
      take(1),
      map(r => r.flashed)
    );
    const timeout$ = timer(DataGuardRowNavigatorService.FLASH_RESULT_TIMEOUT_MS).pipe(map(() => false));
    // firstValueFrom subscribes synchronously, so by the time the next line
    // runs the subscription to flashResult$ is already in place.
    const settled = firstValueFrom(race(result$, timeout$));
    this.nav$.next({ ...req, requestId });
    return settled;
  }

  /**
   * Called by the result-table-frame when a walk concludes — either by
   * actually flashing the row (`flashed: true`) or by being superseded /
   * timing out / failing to find the target anywhere (`flashed: false`).
   * Idempotent emits are safe; the navigate() awaiter uses `take(1)`.
   */
  public reportFlashResult(result: DataGuardRowNavResult): void {
    this.flashResult$.next(result);
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

  /**
   * Purge keys from a per-row cursor Map that no longer appear in `liveIds`.
   * Used by the checklist component on every results-service push: a benign
   * re-emit (e.g., `updateEntry` after a verdict toggle) leaves the live ids
   * unchanged, so this is a no-op and the user's 📍 click cursors survive.
   * On a fresh scan the issueId set is replaced wholesale, so every old key
   * is dropped — bounded memory, no leak. Mutates `cursors` in place so the
   * caller's Map reference stays stable.
   *
   * Extracted as a static helper so the checklist subscribe callback stays
   * trivially testable without spinning up a TestBed.
   */
  public static purgeStaleCursors(cursors: Map<string, number>, liveIds: ReadonlySet<string>): void {
    for (const k of cursors.keys()) {
      if (!liveIds.has(k)) cursors.delete(k);
    }
  }

  /**
   * Case-insensitive set of string tokens that mean "no value was recorded."
   * MUST stay in sync with `MISSING_TOKENS_LOWER` in
   * `agent-service/src/agent/tools/dataguard/missing-detection.ts`. We
   * deliberately inline the set rather than import — frontend and
   * agent-service are separate build targets with no shared module path —
   * and the contract is enforced by parallel unit tests using the same
   * fixtures (see the round-6 regression test for JSONL `null` cells).
   */
  private static readonly MISSING_TOKENS_LOWER: ReadonlySet<string> = new Set([
    "na",
    "n/a",
    "null",
    "none",
    "nan",
  ]);

  /**
   * Mirror of `isMissing` in agent-service `missing-detection.ts`. Treats
   * `null`, `undefined`, `NaN`, empty / whitespace-only strings, and the
   * case-insensitive trimmed missing-token set above as missing. Used by
   * `fingerprintCell` so a profiler-side null cell and a Texera-side
   * `"null"` string cell collapse to the same fingerprint token (see
   * round-6 doc on `fingerprintCell` below).
   */
  private static isMissingCell(v: unknown): boolean {
    if (v === null || v === undefined) return true;
    if (typeof v === "number" && Number.isNaN(v)) return true;
    if (typeof v !== "string") return false;
    const trimmed = v.trim();
    if (trimmed === "") return true;
    return DataGuardRowNavigatorService.MISSING_TOKENS_LOWER.has(trimmed.toLowerCase());
  }

  /**
   * Compute the fingerprint of a result-panel row. Implementation MUST stay
   * byte-identical to the agent-service `rowFingerprint` helper, otherwise the
   * locate-by-key match will silently fail.
   *
   * Contract (mirrored from agent-service):
   *   - Canonicalize column order by alphabetical sort.
   *   - Each non-missing cell is normalised to a string via `String(v)`
   *     before `JSON.stringify`, so number `45` and string `"45"` produce the
   *     same token. This is the fix for the JSONL-multi-worker mixed-type
   *     case: Texera's `JSONLScanSourceOpExec` widens mixed columns to String
   *     via `parseField(stringValue, schemaType)`, while DataGuard's
   *     `parseJsonl` keeps native JSON types. Without coercion the two sides
   *     fingerprint differently and `findRowByKey` misses every row.
   *   - **Round 6 — missing-token canonicalization.** Any cell `isMissingCell`
   *     considers absent (`null`, `undefined`, `NaN`, `""`/whitespace, and
   *     the case-insensitive trimmed tokens `na`/`n/a`/`null`/`none`/`nan`)
   *     emits the same bare `null` token. This closes the JSONL locate bug
   *     where Texera's `JSONToMap` calls Jackson's `JsonNode#asText()` on a
   *     `NullNode`, returning the literal STRING `"null"` rather than Java
   *     null — without canonicalization the profiler-side cell fingerprints
   *     as bare `null` while the Texera-side cell fingerprints as
   *     `"\"null\""`, the locate match silently misses, and the byte-order
   *     index fallback lands on whatever shuffled display row happens to sit
   *     at that position.
   *   - Concatenate JSON tokens with an empty separator (each token is
   *     self-delimited as `"…"` or the literal `null`).
   *
   * Reviewers note: we deliberately do NOT share this code between the
   * frontend and the agent-service — they're separate build targets with no
   * common module path. The contract is enforced by parallel unit tests on
   * each side using the same input fixtures.
   */
  private static fingerprintCell(v: unknown): string {
    if (DataGuardRowNavigatorService.isMissingCell(v)) return "null";
    return JSON.stringify(String(v));
  }

  public static rowFingerprint(row: Record<string, unknown>, columns: ReadonlyArray<string>): string {
    const canonical = [...columns].sort();
    let out = "";
    for (const c of canonical) {
      out += DataGuardRowNavigatorService.fingerprintCell(row[c]);
    }
    return out;
  }

  /**
   * Linear-scan find of the **Nth** row whose fingerprint matches `targetKey`.
   * Returns the 0-based display index of the `occurrence`-th match (0 = first,
   * 1 = second, …) or -1 if there are fewer matches than requested.
   *
   * Duplicate-row issues are why this exists. Every affected row in a
   * `duplicate_id` issue has the SAME fingerprint by definition, so the
   * checklist's cursor walk hands the same `targetKey` to every click and
   * relies on `occurrence` to walk distinct display rows.
   *
   * `columns` is the schema seen by the caller (display-side); it can be in
   * any order — the fingerprint canonicalises it. Empty arrays return -1.
   * Negative or non-finite `occurrence` is coerced to 0 (defensive).
   */
  public static findNthRowByKey(
    rows: ReadonlyArray<Record<string, unknown>>,
    columns: ReadonlyArray<string>,
    targetKey: string,
    occurrence: number
  ): number {
    if (rows.length === 0 || columns.length === 0) return -1;
    const want = Number.isFinite(occurrence) && occurrence >= 0 ? Math.floor(occurrence) : 0;
    let seen = 0;
    for (let i = 0; i < rows.length; i++) {
      if (DataGuardRowNavigatorService.rowFingerprint(rows[i], columns) === targetKey) {
        if (seen === want) return i;
        seen++;
      }
    }
    return -1;
  }

  /**
   * Thin wrapper preserving the pre-occurrence call sites (and tests). Returns
   * the first display-row index whose fingerprint matches `targetKey`, or -1.
   */
  public static findRowByKey(
    rows: ReadonlyArray<Record<string, unknown>>,
    columns: ReadonlyArray<string>,
    targetKey: string
  ): number {
    return DataGuardRowNavigatorService.findNthRowByKey(rows, columns, targetKey, 0);
  }

  /**
   * Count how many rows on the supplied page match `targetKey`. Used by the
   * result-table-frame's page walker to maintain a cumulative "matches seen
   * before the current page" counter so an `occurrence` request lands on the
   * correct display row even when matches straddle a page boundary.
   */
  public static countMatchesByKey(
    rows: ReadonlyArray<Record<string, unknown>>,
    columns: ReadonlyArray<string>,
    targetKey: string
  ): number {
    if (rows.length === 0 || columns.length === 0) return 0;
    let count = 0;
    for (let i = 0; i < rows.length; i++) {
      if (DataGuardRowNavigatorService.rowFingerprint(rows[i], columns) === targetKey) {
        count++;
      }
    }
    return count;
  }
}
