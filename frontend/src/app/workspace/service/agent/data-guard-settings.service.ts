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
import { BehaviorSubject, Observable, distinctUntilChanged, map } from "rxjs";

/**
 * Per-workflow DataGuard auto-trigger enable/disable.
 *
 * - Default is **ON** for every new workflow (matches the storyboard:
 *   "drag a dataset → DataGuard auto-pops").
 * - Per-workflow because some users will want to scan one workflow but not
 *   another. Keyed by the workflow id in `localStorage` so the choice
 *   persists across page reloads but is not synced server-side.
 * - The state is exposed as an Observable so the toolbar's shield button can
 *   reactively recolor itself, and `DataGuardAutoTriggerService` reads it
 *   synchronously in `runPipeline` to gate orchestration.
 */
@Injectable({ providedIn: "root" })
export class DataGuardSettingsService {
  private static readonly STORAGE_KEY_PREFIX = "dataguard.enabled.wid.";
  private static readonly OFF = "off";
  private static readonly ON = "on";

  // Cache of in-memory state, in addition to localStorage. Map<workflowId, enabled>.
  // localStorage is the source of truth across reloads; the cache is just
  // here so subscribers get instant updates without re-reading.
  private readonly cache$ = new BehaviorSubject<ReadonlyMap<number, boolean>>(new Map());

  /** True if DataGuard auto-trigger should fire for this workflow. */
  public isEnabled(workflowId: number): boolean {
    const cached = this.cache$.value.get(workflowId);
    if (cached !== undefined) return cached;
    const stored = localStorage.getItem(this.key(workflowId));
    // Absent key = default-on. Only an explicit "off" disables.
    return stored !== DataGuardSettingsService.OFF;
  }

  /** Reactive view of the enabled flag for a specific workflow. */
  public isEnabled$(workflowId: number): Observable<boolean> {
    return this.cache$.asObservable().pipe(
      map(m => {
        const v = m.get(workflowId);
        if (v !== undefined) return v;
        return localStorage.getItem(this.key(workflowId)) !== DataGuardSettingsService.OFF;
      }),
      distinctUntilChanged()
    );
  }

  /** Persist + broadcast a new enabled state for this workflow. */
  public setEnabled(workflowId: number, enabled: boolean): void {
    localStorage.setItem(this.key(workflowId), enabled ? DataGuardSettingsService.ON : DataGuardSettingsService.OFF);
    const next = new Map(this.cache$.value);
    next.set(workflowId, enabled);
    this.cache$.next(next);
  }

  /** Convenience toggle. Returns the new state. */
  public toggle(workflowId: number): boolean {
    const next = !this.isEnabled(workflowId);
    this.setEnabled(workflowId, next);
    return next;
  }

  private key(workflowId: number): string {
    return DataGuardSettingsService.STORAGE_KEY_PREFIX + workflowId;
  }
}
