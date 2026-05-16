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

import { fakeAsync, tick } from "@angular/core/testing";
import { vi } from "vitest";
import { DataGuardRowNavigatorService, DataGuardRowNavRequest } from "./data-guard-row-navigator.service";

describe("DataGuardRowNavigatorService", () => {
  describe("pageIndexFor", () => {
    it("returns page 1 for the first row regardless of page size", () => {
      expect(DataGuardRowNavigatorService.pageIndexFor(0, 5)).toBe(1);
      expect(DataGuardRowNavigatorService.pageIndexFor(0, 25)).toBe(1);
    });

    it("computes 1-based page index for arbitrary row indices", () => {
      expect(DataGuardRowNavigatorService.pageIndexFor(4, 5)).toBe(1);
      expect(DataGuardRowNavigatorService.pageIndexFor(5, 5)).toBe(2);
      expect(DataGuardRowNavigatorService.pageIndexFor(11, 5)).toBe(3);
      expect(DataGuardRowNavigatorService.pageIndexFor(99, 10)).toBe(10);
    });

    it("guards against degenerate page sizes (returns page 1)", () => {
      expect(DataGuardRowNavigatorService.pageIndexFor(7, 0)).toBe(1);
    });

    it("clamps negative indices to page 1 and logs a warning", () => {
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
      expect(DataGuardRowNavigatorService.pageIndexFor(-3, 5)).toBe(1);
      expect(warnSpy).toHaveBeenCalled();
      warnSpy.mockRestore();
    });
  });

  describe("nextCycleStep", () => {
    it("walks the cycle and wraps to the start", () => {
      const indices = [3, 7, 12];
      // start: cursor=0 → value=3, next=1
      let step = DataGuardRowNavigatorService.nextCycleStep(indices, 0);
      expect(step).toEqual({ value: 3, nextCursor: 1 });
      // cursor=1 → value=7, next=2
      step = DataGuardRowNavigatorService.nextCycleStep(indices, step.nextCursor);
      expect(step).toEqual({ value: 7, nextCursor: 2 });
      // cursor=2 → value=12, next=3
      step = DataGuardRowNavigatorService.nextCycleStep(indices, step.nextCursor);
      expect(step).toEqual({ value: 12, nextCursor: 3 });
      // cursor=3 wraps → value=3, next=1 (3 % 3 = 0)
      step = DataGuardRowNavigatorService.nextCycleStep(indices, step.nextCursor);
      expect(step).toEqual({ value: 3, nextCursor: 1 });
      // cursor=1 again → value=7, next=2
      step = DataGuardRowNavigatorService.nextCycleStep(indices, step.nextCursor);
      expect(step).toEqual({ value: 7, nextCursor: 2 });
    });

    it("treats a single-element array as a no-op cycle that re-emits", () => {
      const indices = [42];
      expect(DataGuardRowNavigatorService.nextCycleStep(indices, 0)).toEqual({ value: 42, nextCursor: 1 });
      expect(DataGuardRowNavigatorService.nextCycleStep(indices, 1)).toEqual({ value: 42, nextCursor: 1 });
      expect(DataGuardRowNavigatorService.nextCycleStep(indices, 7)).toEqual({ value: 42, nextCursor: 1 });
    });

    it("coerces corrupted cursors to 0", () => {
      const indices = [10, 20];
      expect(DataGuardRowNavigatorService.nextCycleStep(indices, -5)).toEqual({ value: 10, nextCursor: 1 });
      expect(DataGuardRowNavigatorService.nextCycleStep(indices, NaN)).toEqual({ value: 10, nextCursor: 1 });
    });

    it("throws when affectedRowIndices is empty (callers must toast first)", () => {
      expect(() => DataGuardRowNavigatorService.nextCycleStep([], 0)).toThrow();
    });
  });

  describe("navigate / getNav$", () => {
    it("multicasts the request to subscribers", () => {
      const svc = new DataGuardRowNavigatorService();
      const req: DataGuardRowNavRequest = { operatorId: "op-1", rowIndex: 42, column: "age" };
      const received: DataGuardRowNavRequest[] = [];
      svc.getNav$().subscribe(r => received.push(r));
      svc.navigate(req);
      expect(received).toEqual([req]);
    });

    // R3: cold-mount replay. The ResultTableFrameComponent mounts lazily after
    // openResultPanel(), so a plain Subject would lose the emission. The
    // ReplaySubject(1) ensures the late subscriber still sees the most recent
    // request — this is the buffer behaviour reviewers asked us to lock down.
    it("replays the most recent request to a late subscriber (cold-mount)", () => {
      const svc = new DataGuardRowNavigatorService();
      const req: DataGuardRowNavRequest = { operatorId: "op-1", rowIndex: 7, column: "x" };
      svc.navigate(req);
      const received: DataGuardRowNavRequest[] = [];
      svc.getNav$().subscribe(r => received.push(r));
      expect(received).toEqual([req]);
    });

    // R3 followup: the replay window is bounded (500 ms) so a request from a
    // long-ago click doesn't leak into a much later mount.
    it("does not replay requests older than the buffer window", fakeAsync(() => {
      const svc = new DataGuardRowNavigatorService();
      svc.navigate({ operatorId: "op-1", rowIndex: 7 });
      tick(1000);
      const received: DataGuardRowNavRequest[] = [];
      svc.getNav$().subscribe(r => received.push(r));
      expect(received).toEqual([]);
    }));
  });
});
