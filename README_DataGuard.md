# DataGuard — Permission-Gated Data Cleaning for Texera

> **Tagline:** AI suggests. Humans authorize. Texera records.
>
> **One-sentence pitch:** DataGuard is a conversational agent inside Texera that proposes data-cleaning actions one at a time and asks the user's permission before applying each — the Claude Code experience, but for data instead of code.

---

## 1. Problem

Data cleaning is rarely "just technical." Cleaning decisions can introduce bias, remove rare-but-meaningful cases, or silently change the meaning of a dataset — especially dangerous in scientific or high-stakes data.

Typical pain points:
- A missing glucose value may not be random.
- An `age = 999` value may be a placeholder, not a real age.
- A duplicate sample ID with conflicting labels may need expert review.
- A statistical outlier may be a meaningful rare case.

Today, two common workflows both fail in different ways:

| Approach | Failure mode |
|---|---|
| Manual scripts (pandas in a notebook) | Opaque, hard to audit, no provenance, doesn't scale beyond one person |
| Auto-clean tools | Black-box decisions, no explanation, no human control over high-impact actions |

**DataGuard's claim:** the *interaction model* is the missing piece, not the algorithms. Treat data-cleaning decisions the way Claude Code treats file edits — **ask permission, explain reasoning, log every decision.**

---

## 2. Why this design

Texera's execution engine (Amber) is a streaming, actor-based system. It does **not** natively support pausing a workflow mid-execution to wait for a user click on an in-canvas approval table. Building such a "pause-and-await-user" operator would require changes in three places (Amber engine, gRPC control protocol, Angular result panel).

DataGuard sidesteps all of this by living in `agent-service/` rather than in the workflow graph:

| Layer | Reused |
|---|---|
| Conversation state | Existing agent-service session management |
| Permission UX | Same UI pattern Claude Code uses for tool authorization |
| LLM gateway | The existing `LLM_ENDPOINT` (OpenAI-compatible) wired into `agent-service` |
| Data processing | TypeScript pure functions in agent-service — no new operators required |
| Workflow execution | Existing Texera workflow API (no new gRPC, no Amber changes) |

**No Amber changes, no new operators, no new protocols.**

---

## 3. User experience

### 3.1 Trigger

The user does **nothing special**. When a dataset-reading operator is added to the workflow canvas — currently `CSVFileScan`, `CSVOldFileScan`, or `JSONLFileScan` — the auto-trigger fires:

1. Resolves the workflow context (id, per-workflow shield setting).
2. Finds or creates a per-workflow agent on agent-service.
3. Reads the operator's `fileName`, fetches the bytes via `DatasetService`, parses with `papaparse`.
4. POSTs `{columns, rows}` to `/dataguard/dataset` so the agent has the data in memory.
5. POSTs `/dataguard/scan` — the server runs `profile_dataset` then `suggest_fix` per issue, **bypassing the LLM ReAct loop** (so the LLM can't decide to call `deleteOperator` and vaporize the user's workflow).
6. Publishes the scan result to `DataGuardResultsService`.

The dedicated `<texera-dataguard-checklist>` floating panel slides in. The chat panel is not involved.

### 3.2 The checklist panel

- **Floating, draggable** — `cdkDrag` on the panel, header is the `cdkDragHandle`. Position is session-only (no localStorage persistence). `cdkDragBoundary="body"` keeps the panel inside the viewport so it can't get lost behind the toolbar.
- **Risk-tier chip** per row (LOW / MEDIUM / HIGH / WARNING) — color coded.
- **Default verdict per row** — `low` is pre-checked Allow; everything else (`medium` / `high` / `warning`) starts `pending` so the user makes an explicit call.
- **Per-row controls**: checkbox to allow, "Skip" button to deny, "Always do this" remember toggle (hidden for `high` and `warning`, and hidden whenever there's no proposal at all).
- **Bulk actions**: "Fix all" / "Skip all" buttons in the row header.
- **Apply button**: `Fix N & run` posts the batch to `/dataguard/apply-batch`, writes the cleaned data back as a new dataset version, repoints the operator, and auto-runs the workflow. **After Apply succeeds, the panel automatically re-scans the new dataset version** so the user sees real residue (or "all clean") instead of stale entries from before the fix.
- **"Scan again"** footer button (visible when state is `done` or `error`) re-runs DataGuard on the *current* dataset version — supports iterative cleanup (v1 → Apply → v2 → Scan again → Apply → v3 → …).

### 3.3 Locate this issue → result panel

Each row's `In column X · affects N row(s)` line is a clickable **📍 locate** button. Clicking it:

1. Highlights the source operator on the graph.
2. Opens / focuses the Result Panel for that operator.
3. Navigates to the page containing the next affected row and flashes the cell.

The button **cycles** — every click advances a per-row cursor through `affectedRowIndices`, wrapping back to the first after the last. Each row owns its own cursor (`Map<issueId, number>` on the checklist component). Cursors survive benign state re-emits via `purgeStaleCursors` (only ids no longer in the live entry set get evicted, never wholesale clear); they only reset on a genuine fresh scan with a different issueId set. The tooltip previews the next position (`Show next affected row (i of N)`).

**Two locate paths split by operator type** because they have different reordering semantics:

| Source operator | Path | Cursor advance | Why |
|---|---|---|---|
| `CSVFileScan` / `CSVOldFileScan` | **Sync index** — `handleLocateByIndex(rowIndex)` directly | Synchronously, *before* `navigate` is called | Texera CSV scan is single-worker → display order = file-byte source order. The index DataGuard computed against `parseCsv`'s output is correct as-is. No fingerprint dance needed; simpler code, lower latency. |
| `JSONLFileScan` | **Fingerprint match + flash-confirmed Promise** — `handleLocateByKey(rowKey, …)` | Only on `flashed === true` (await the Promise) | Texera JSONL scan is parallelizable → display order can shuffle relative to source order. The index is no longer trustworthy. We compute a content-stable fingerprint per row in the profiler and match it against rendered display rows. |

The JSONL path additionally has:

- **Fingerprint contract.** `rowFingerprint(row, columns)` is byte-identical on agent-service and frontend: sort columns alphabetically, for each cell `String(v) ` then `JSON.stringify(...)` (the `String()` step is critical — Texera's runtime sometimes coerces numbers to strings during schema-widened display, so naked `JSON.stringify(45)` vs `JSON.stringify("45")` would mismatch). Both sides have contract-example tests.
- **`currentLocateToken` cancellation.** Each click bumps a monotonic counter; every async resumption in `handleLocateByKey` (page-render race, walkPage recursion) checks the captured token before mutating state. Older walks bail and emit `flashResult: false` exactly once so the awaiting Promise resolves rather than hanging.
- **Subscribe-before-publish navigate Promise.** `navigate()` returns `Promise<boolean>`. Internally: `firstValueFrom(race(filtered, timer(36s)))` subscribes synchronously to `flashResult$`, then `nav$.next(req)` publishes. So even a synchronous fast-path emit (target row already on current page) is caught.
- **Walk up to `LOCATE_BY_KEY_MAX_PAGES = 10` pages** before falling back to the index path. The fingerprint match falls back silently to index if it can't find the row — no nagging toast.
- **36 s safety timeout** derived from `LOCATE_BY_KEY_MAX_PAGES × 3500 ms + 1000 ms` so a legitimately slow walk doesn't time out and skip the cursor.

The result-panel side uses an internal `pageRendered$ Subject` so the flash lands AFTER the new page renders (not after a 100 ms guess). `dg-row-highlight` + `dg-cell-highlight` apply for 2 s, matched to the SCSS pulse animation. Cross-operator races, viewport resize during page swap, columnDef-vs-header naming drift, and stale closures on a destroyed view are all guarded.

### 3.4 Toolbar shield + floating icon

A `🛡` button in the workflow toolbar toggles DataGuard per-workflow (persisted in localStorage). When the panel is closed (`state === "idle"`) but the shield is ON, a small floating DataGuard icon appears on the canvas. Clicking it always triggers a fresh scan of whatever dataset operator is on the canvas (via `DataGuardAutoTriggerService.rescanAny()`).

Resolution order for "what to scan?":
1. The previously-scanned operator if it still exists on the canvas.
2. Otherwise the first dataset-reading operator on the canvas.
3. Otherwise warn the user: "drop a dataset operator first."

**Concurrency control.** The pipeline is gated by `currentPipeline: Promise<void> | null` instead of a boolean. Two regimes:
- **Auto-trigger** (`userInitiated: false`, driven by operator-add / property-change debounce): if the slot is occupied, **drop silently** to preserve the original spam-suppression semantics.
- **User-initiated** (`userInitiated: true`, the floater click or the panel's Scan-again button): if the slot is occupied, **await** the in-flight pipeline (with a "queuing your scan…" toast) then start a fresh one. The panel state flips to `"scanning"` immediately so the user never sees a dead click. At most one `/scan` POST is in flight at a time.

This means rescan works at any time, even after the user explicitly closed the panel — `state.sourceOperatorId` being gone after `reset()` doesn't break the flow, and a click during a slow scan doesn't double-fire LLM cost.

---

## 4. System architecture

```
┌───────────────────────────┐          ┌──────────────────────────────────────┐
│  Texera frontend (Angular) │   chat   │       agent-service (Bun / TS)      │
│                            │◄─── ws ──┤                                      │
│  ┌─────────────────────┐   │          │   TexeraAgent (DataGuard host)      │
│  │ DataGuard checklist │   │          │   ├── DataGuardSession              │
│  │ + floating reopen   │   │  REST    │   │   • dataset (in-memory)         │
│  │ + toolbar shield    │◄──┼──────────┤   │   • issues / proposals / log    │
│  └─────────────────────┘   │          │   │   • auto-allow rules            │
│  ┌─────────────────────┐   │          │   ├── Tools:                         │
│  │ Auto-trigger        │   │          │   │   • profile_dataset (read-only) │
│  │  service            │   │          │   │   • suggest_fix     (read-only) │
│  │ + DataGuard results │   │          │   │   • apply_fix       (mutating)  │
│  │  state              │   │          │   │   • write_decision_log          │
│  └─────────────────────┘   │          │   │   • bias_check                  │
└───────────────────────────┘          │   └── LLM gateway (LiteLLM)         │
                                       └──────────────┬───────────────────────┘
                                                      │
                                              ┌───────▼────────┐
                                              │ Texera storage │
                                              │ (dataset/      │
                                              │  file-service) │
                                              └────────────────┘
```

**Two entry points to the same backend:**

- **REST (the primary path):** `/scan` and `/apply-batch` are server-driven — no LLM in the loop during the user's interaction. The LLM is invoked exactly once per issue inside `/scan`, to render a structured `FixProposal` from the raw `DataQualityIssue`. This keeps the user's flow deterministic and fast.
- **WS (the chat path):** `<texera-permission-prompt>` still works if the agent is prompted via chat and the ReAct loop reaches `apply_fix` for a mutating action. The shape is `{type: "decision", stepId, verdict, remember?}`. This path is not used by the auto-trigger but is kept for any future chat-driven DataGuard flow.

---

## 5. Detectors (five categories)

`profile_dataset` runs entirely without an LLM and emits `DataQualityIssue` records of five types:

| Type | Detection |
|---|---|
| `missing_value` | `null` / empty / configured missing tokens (default: `na`, `n/a`, `null`, `none`, `nan` — case-insensitive, whitespace-trimmed) |
| `placeholder_value` | Numeric sentinels (`999`, `-1`) or string sentinels (`unknown`, `Unknown`) — overridable via `placeholderValues` |
| `duplicate_id` | Honors `idColumn` hint; **falls back to a column-name heuristic** (`id`, `*_id`, `*Id`, `id_*`, `uid`) when no hint is supplied so the auto-trigger's empty-body `/scan` still catches duplicates. Flags repeated IDs (with or without conflicting labels) |
| `outlier` | Requires `validRanges` hint per column (`{min, max}`); flags numeric values outside the range. Skips rows already flagged as placeholders to avoid double-counting |
| `inconsistent_label` | Low-cardinality string columns where `trim+lowercase` keys collide on multiple raw spellings (e.g., `Male` / `male` / `MALE`); picks the most-frequent spelling as canonical |

> **Note on outlier semantics.** An earlier z-score based detector (flag anything with `|z| > 3`) was deliberately removed: clustered legitimate extremes (e.g. sustained high glucose readings in a clinical dataset) were being flagged en masse and the user had no good way to tell the agent "those are real". The current `outlier` detector requires the caller to opt in by stating a hard range per column — the user owns the definition.

The `missing` detector is centralized in `missing-detection.ts` — `profile_dataset` and `apply_fix` (impute) share it, so what the profiler flags is exactly what imputation treats as missing.

---

## 6. Risk tiers (four levels)

`suggest_fix` annotates every proposal with a `RiskTier`. The tier governs both the UI (pre-check / badge color / "remember" availability) and the permission gate:

| Tier | Color | Default checkbox | "Allow & remember"? | Use case |
|---|---|---|---|---|
| `low` | green | pre-checked Allow | yes | Trim whitespace, standardize column names, drop fully empty rows |
| `medium` | yellow | pending (unchecked) | yes | Impute missing values, standardize inconsistent labels |
| `high` | red | pending (unchecked) | **no** | Drop rows, resolve conflicting duplicate IDs |
| `warning` | orange | pending (unchecked) | **no** | Concrete fix exists but the agent specifically wants a human to eyeball it — e.g. clamping an outlier that might be a real extreme value |

The `warning` tier was introduced to replace the earlier `flag` operation kind (which was a no-op against the data and just recorded row indices on the session). Every proposal now produces a real concrete change to the data; "please review this one manually" is conveyed through the warning tier instead of a no-op operation. This also fixes a downstream LakeFS bug: when every "applied" fix is a real mutation, the exported CSV genuinely differs from the source and the version-create commit succeeds.

`profile_dataset` default tiers:

| Issue type | Default tier |
|---|---|
| `missing_value` | medium |
| `placeholder_value` | medium |
| `inconsistent_label` | medium |
| `duplicate_id` | high |
| `outlier` | **warning** |

`suggest_fix` is allowed to override the default with a strong reason; the LLM prompt explicitly instructs it to prefer clamping via `replace_value` over destructive `drop_rows`, and to set `riskTier="warning"` when the user really should eyeball the fix.

---

## 7. Fix operation kinds (six)

`FixOperationKind` is the closed enum of mutations `apply_fix` knows how to execute:

| Kind | Params | Effect |
|---|---|---|
| `replace_value` | `{column, replacement, rowIndices?, match?}` | Swap cells. Two targeting modes: `rowIndices` (deterministic, used by outlier proposals) wins when both are present. `match` (value-based) is for cases like "replace every `unknown` with null". `rowIndices` was added because LLM-generated `match` values for numeric outliers silently no-op'd when the LLM rounded the cell value (e.g. `match: 950` vs cell `949.7`), producing byte-identical exports that LakeFS rejected. |
| `drop_rows` | `{rowIndices: number[]}` | Remove rows by index |
| `impute` | `{column, strategy: "mean" \| "median" \| "mode"}` | Fill missing cells; honors session `missingTokens` override |
| `standardize` | `{column, mapping: {from: to}}` | Replace cell values via explicit mapping |
| `trim_whitespace` | `{column}` | Strip leading/trailing whitespace |
| `rename_column` | `{from, to}` | Rename column and rewrite per-row keys |

`apply_fix` is a **pure function** — it never mutates the input `DatasetView`, just returns a new one. Optional `ApplyOptions.missingTokens` is threaded through from the session's scan options so `impute` treats the same set of cells as missing that the profiler flagged.

---

## 8. Permission model

Every mutating tool call passes through `requestApproval(gateway, proposal)`:

```
verdict resolution:
  if riskTier is "high" or "warning":
      always prompt the user
  else if issueType has an autoAllowRule in the session:
      return auto_allow_remembered
  else if riskTier is "low":
      return auto_allow_low_risk
  else:
      emit pendingApproval step, wait for user decision via WS
```

The `Verdict` union: `"allow" | "deny" | "auto_allow_low_risk" | "auto_allow_remembered"`. **`"modify"` was deliberately cut** — the legacy handler recorded a user-supplied free-text override but still executed the original `operationParams`, which silently lied to users. Modify will return only with a real natural-language → operationParams parser (post-MVP).

### 8.1 Contract enforcement

The HTTP and WS handlers strictly enforce this contract:

- `/apply-batch` body schema: `verdict: t.Union([t.Literal("allow"), t.Literal("deny")])` with `additionalProperties: false` on each decision object. Unknown fields (e.g., legacy `modifiedAction`) cause a typebox validation rejection.
- The Elysia app is built with `normalize: false` so unknown fields aren't silently stripped before validation hits.
- A global `onError` handler converts `code === "VALIDATION"` to HTTP 400 instead of the default 500.
- A runtime check on `/apply-batch` rejects `{verdict: "deny", remember: true}` with a friendly 400 — `remember` only applies when the user approves a fix.
- The WS `decision` handler narrows the same way: `verdict?: "allow" | "deny"` only, `modifiedAction` removed, and the handler explicitly rejects an invalid verdict and `deny+remember=true` with an error message.

---

## 9. Decision log

Every approved or denied action is appended to a structured per-session log. The log serializes to RFC-4180 CSV with the 9-column schema:

```
decision_id, timestamp, issue_type, target_rows, proposed_action,
user_decision, reason, confidence, applied_at
```

`applied_at` is empty for denied entries. The CSV is exported by the `write_decision_log` tool (LLM-invocable) or read directly from `session.getDecisionLog()`.

The `modified_action` column was cut alongside the `"modify"` verdict.

---

## 10. Backend API surface

All under `${API_PREFIX}/agents/:id`:

| Method + Path | Purpose |
|---|---|
| `POST /dataguard/dataset` | Load `{columns: string[], rows: Record<string, unknown>[]}` into the agent's `DataGuardSession`. Resets per-run state (issues, proposals, decision log). |
| `POST /dataguard/scan` | Run `profile_dataset` then `suggest_fix` per issue. Body: optional `{idColumn?, validRanges?, placeholderValues?, missingTokens?}`. Persists scan options on the session for the verification re-scan. Returns `{issueCount, issues, proposals}`. |
| `POST /dataguard/apply-batch` | Apply the user's checked decisions. Body: `{decisions: [{issueId, verdict: "allow"|"deny", remember?}]}`. Runs `apply_fix` per allowed proposal, records every decision (including denies). Re-profiles the cleaned dataset and returns `{applied, denied, failed, datasetRowCount, results, residualIssues, residualCount}`. |
| `GET  /dataguard/export-csv` | Return the in-memory cleaned dataset as a CSV blob. Used by the frontend to push the new version back to the source dataset. |
| `GET  /dataguard/session` | Inspect session state (issue list, decision log, auto-allow rules). |
| `WS   /agents/:id/react` `{type:"decision",…}` | Resolve a pending-approval step. Used by the chat flow only; the checklist path uses `/apply-batch`. |

---

## 11. Frontend components

### 11.1 Services (DI singletons, `providedIn: 'root'`)

| Service | Responsibility |
|---|---|
| `DataGuardAutoTriggerService` | Owns the operator-add / property-change subscription, runs the orchestration pipeline (resolve workflow → load dataset → scan → publish). Exposes `startOrchestration()`, `applyBatch(decisions)`, `rescanCurrent()`, `rescanAny()`. Concurrency-gated by `currentPipeline: Promise<void> \| null` (see §3.4). |
| `DataGuardResultsService` | `BehaviorSubject<DataGuardScanResult>` that drives the checklist UI. States: `idle → scanning → ready → applying → done / error`. Exposes `setState(patch)`, `updateEntry(issueId, patch)`, `reset()`. |
| `DataGuardSettingsService` | Per-workflow shield ON/OFF, persisted in `localStorage` (`dataguard.enabled.wid.<wid>`). Default ON. |
| `DataGuardRowNavigatorService` | `ReplaySubject<DataGuardRowNavRequest>(1, 500ms)` driving the 📍 locate flow. Includes pure helpers `pageIndexFor(rowIndex, pageSize)` and `nextCycleStep(indices, cursor)`. |
| `AgentService.sendDecision(agentId, stepId, verdict, options)` | WS sender for the chat flow's `{type:"decision",…}` message. |

### 11.2 Components

| Component | Role |
|---|---|
| `<texera-dataguard-checklist>` (standalone) | The floating, draggable checklist panel. Subscribes to `DataGuardResultsService`, owns the orchestration subscription (so it lives whenever the checklist itself can render), renders rows / risk-tier badges / Apply button / Scan-again footer / floating reopen icon. Holds the per-row `locateCursors: Map<issueId, number>` driving cyclic 📍 navigation. |
| `<texera-permission-prompt>` (standalone) | The inline chat-bubble approval prompt — used by the chat-driven path in `agent-chat`. Allow / Deny / Allow-&-remember (the last is hidden for `high` and `warning`). |
| `<texera-menu>` (modified) | Toolbar 🛡 shield button — toggles `DataGuardSettingsService.isEnabled(wid)`. |
| `<texera-result-table-frame>` (modified) | Subscribes to `DataGuardRowNavigatorService`. On a locate request, navigates the paginator to the target page and chains off an internal `pageRendered$ Subject` so `applyFlash()` only fires after the new page actually renders. 2 s highlight via `HIGHLIGHT_DURATION_MS`; `ngOnDestroy` clears the timer to avoid NG0911. |

The checklist component lives at `bottom: 100px; right: 80px` by default. When dragged elsewhere it stays where the user put it for the session; refreshing returns to the default anchor. The floating reopen icon occupies the same default position.

---

## 12. Auto-trigger dataset operator set

```ts
private static readonly DATASET_OPERATOR_TYPES = new Set<string>([
  "CSVFileScan",
  "CSVOldFileScan",
  "JSONLFileScan",
]);
```

`loadFromOperatorFile` dispatches by operator type to a parser registry:

```ts
type DatasetParser = (blob, fileName, options?: {delimiter?}) => Promise<{columns, rows}>;
const PARSERS: Record<string, DatasetParser> = {
  CSVFileScan:     parseCsv,
  CSVOldFileScan:  parseCsv,    // honors options.delimiter (CSVOld customDelimiter)
  JSONLFileScan:   parseJsonl,  // nested flatten + array stringify + collision rule
};
```

**CSV variants** share `parseCsv`. CSVOld's Scala impl uses scala-csv's `DefaultCSVFormat` (RFC-4180-equivalent bytes) but exposes a `customDelimiter` operator property; `extractParserOptions` reads it from `op.operatorProperties.customDelimiter` and threads it into Papa as the `delimiter` option, so `;`, `\t`, or any non-comma separator is honored.

**JSONL** goes to `parseJsonl` (in `data-guard-jsonl.ts`). Flatten policy:
- Nested objects → dot-notation columns (`address.street`). Pre-scan collects all nested-owned paths; second pass emits leaves and skips (with single warning per path) literal-dotted top-level keys whose path is nested-owned. **Nested always wins regardless of JSON source order.**
- Arrays → `JSON.stringify(arr)` as a single cell (never explodes rows; preserves row indices for `apply_fix` rowIndices contract).
- Non-object lines (bare strings/numbers/booleans/arrays/null) → `console.warn` and skip.
- Blank lines + CRLF tolerated.
- Server-side `GET /dataguard/export-jsonl` round-trips JSONL after Apply (iterates `dataset.columns` for canonical key order; `undefined` → `null` for lossless round-trip).

**Out of trigger set** (intentional): `ArrowFileScan`, `FileLister`, `FileScan`, `FileScanFromInput`, `TextInput`. Adding a format = register a parser in `PARSERS`, add the operator type to `DATASET_OPERATOR_TYPES`, ensure write-back format-awareness if the operator has its own file format (JSONL has `/export-jsonl`, CSV uses default `/export-csv`).

`ParallelCSVFileScan` is intentionally omitted: Texera disables it in the operator registry (`LogicalOp.scala:171` commented out, "so that it does not confuse user"). If re-enabled, one-line add to `PARSERS` and `DATASET_OPERATOR_TYPES`.

---

## 13. File map

### 13.1 agent-service

```
src/types/dataguard.ts                          Shared types: RiskTier, Confidence, IssueType,
                                                FixOperationKind, Verdict, DataQualityIssue,
                                                FixProposal, PermissionDecision, DecisionLogEntry,
                                                AutoAllowRule
src/types/dataguard.test.ts                     Fixture tests (literal-union shapes)
src/types/agent.ts                              ReActStep.pendingApproval + PendingApproval interface

src/agent/tools/dataguard/
  dataset.ts                                    DatasetView type
  missing-detection.ts                          Shared isMissing / placeholderHit / toNumber
  profile-dataset.ts                            Five-detector profiler, no LLM;
                                                inferIdColumn helper for auto-detecting
                                                ID columns by name pattern
  suggest-fix.ts                                LLM-driven proposal generator, zod-validated;
                                                prompt passes affectedRowIndices and instructs
                                                rowIndices-based replace_value for outliers
  apply-fix.ts                                  Pure-function applier for the six op kinds.
                                                replace_value supports rowIndices targeting;
                                                ApplyOptions.missingTokens honored by impute
  with-approval.ts                              Permission gate (handles low/medium/high/warning;
                                                warning never auto-allows, even with remember rules)
  dataguard-session.ts                          Per-agent state: dataset, issues, proposals,
                                                decisionLog, autoAllowRules, ScanOptions
  decision-log.ts                               9-col CSV serializer + AI-SDK tool
                                                (modified_action column removed by #11a)
  bias-check.ts                                 Per-group retention diff + AI-SDK tool
  dataguard-tools.ts                            AI SDK tool({...}) definitions (5 tools)
  __tests__/*.test.ts                           Test files — apply-fix, suggest-fix, profile-dataset,
                                                with-approval, dataguard-session, decision-log,
                                                bias-check, apply-batch-rescan, plus the
                                                contract-lock files: permission-types,
                                                apply-batch-modify-reject, decision-log-no-modify,
                                                with-approval-no-modify

src/agent/texera-agent.ts                       Implements ApprovalGateway; registers DataGuard
                                                tools; exposes public callLlm(prompt) used by /scan
src/server.ts                                   Elysia routes (app built with normalize:false so
                                                additionalProperties:false on body schemas
                                                actually rejects legacy fields):
                                                  POST   /dataguard/dataset
                                                  POST   /dataguard/scan
                                                  POST   /dataguard/apply-batch  (rejects "modify"
                                                         verdict, modifiedAction field, and
                                                         {verdict:"deny", remember:true})
                                                  GET    /dataguard/export-csv
                                                  GET    /dataguard/session
                                                  WS     decision message branch
                                                onError: VALIDATION → 400
```

### 13.2 frontend

```
src/app/workspace/
  service/agent/
    agent-types.ts                              ReActStep.pendingApproval field (mirror of backend);
                                                riskTier includes "warning"
    agent.service.ts                            sendDecision(agentId, stepId, verdict, {remember})
                                                — WS sender for the chat flow
    data-guard-auto-trigger.service.ts          Orchestration pipeline (scan / apply-batch /
                                                rescanAny / rescanCurrent), debounced operator-add
                                                + operator-property-change subscription.
                                                Concurrency-gated via currentPipeline Promise:
                                                user-initiated awaits in-flight, auto-trigger
                                                drops silently. After-Apply auto-rescan.
                                                Includes pure helper resolveRescanTarget(state, graph).
    data-guard-auto-trigger.service.spec.ts     Tests for resolveRescanTarget decision tree
                                                + serialization test (no concurrent /scan)
    data-guard-results.service.ts               BehaviorSubject<DataGuardScanResult> driving the UI
    data-guard-results.service.spec.ts          State-shape tests
    data-guard-settings.service.ts              Per-workflow shield ON/OFF (localStorage)
    data-guard-row-navigator.service.ts         ReplaySubject for 📍 locate flow;
                                                pageIndexFor + nextCycleStep pure helpers
    data-guard-row-navigator.service.spec.ts    Cycle-walk + ReplaySubject TTL + negative-cursor
                                                coercion + serialization edge cases
  component/
    dataguard-checklist/                        <texera-dataguard-checklist> — floating draggable
                                                panel (cdkDrag + cdkDragHandle + cdkDragBoundary),
                                                row checklist, 📍 locate button (cyclic),
                                                category roll-up, Scan-again, floating reopen icon
    result-panel/result-table-frame/            Modified to subscribe to row-navigator and flash
                                                cells via pageRendered$ Subject (waits for new
                                                page render, not arbitrary timeout)
    agent/agent-panel/
      permission-prompt/                        <texera-permission-prompt> — inline approval UI
                                                used by the chat-driven path
      agent-chat/                               Renders <texera-permission-prompt> inside the
                                                step loop when pendingApproval is set
      agent-panel.component.ts                  No longer owns the auto-trigger subscription —
                                                that moved to the checklist component
    menu/                                       Toolbar 🛡 shield button (per-workflow toggle)
    workspace.component.{ts,html}               Mounts <texera-dataguard-checklist *ngIf="copilotEnabled">
```

---

## 14. End-to-end flow

1. Confirm the 🛡 shield is ON (toolbar — twotone icon = ON, outline = OFF).
2. Drop a `CSVFileScan` operator and point it at any dataset in the system.
3. The checklist panel slides in. While `/scan` runs, a loading message replaces the row list (typically a few seconds — one LLM call per issue).
4. Review each row. `LOW` rows are pre-checked; `MEDIUM` / `HIGH` / `WARNING` need an explicit Allow. Click the **📍** on any row to jump to the affected row in the Result Panel — clicking again cycles to the next affected row, wrapping after the last.
5. Click **Fix N & run** — the cleaned data is written back as a new dataset version with a timestamp-suffixed name, the operator is repointed at the new version, and the workflow auto-runs. The panel then **auto-re-scans** the new version so you immediately see whether anything is still left.
6. Click **Scan again** at any time to iterate against the current version.
7. Close the panel. The floating DataGuard icon appears (if the shield is ON) — click it any time to trigger a fresh scan; the panel re-opens immediately even if another scan is in flight (queued, never concurrent).

---

## 15. Testing

```bash
cd agent-service
bun run typecheck   # exit 0
bun test            # 217 pass / 0 fail (457 expect calls)

cd frontend
npx tsc --noEmit    # exit 0
ng test --watch=false   # runs Karma harness (the same one CONTRIBUTING.md requires)
```

Test coverage spans:

- **Types fixtures** (12) — verifies the literal unions accept and reject the right members.
- **Profile** (28+) — per-detector cases including the validRanges-based outlier, the explicit "clustered large readings are NOT auto-outliers" assertion, the `inferIdColumn` heuristic across all id-name patterns (`id`, `*_id`, `*Id`, `id_*`, **dotted JSONL flatten names like `user.id` / `customer.uid`**), `rowFingerprint` contract example + number-vs-string equivalence + float round-trip.
- **Suggest** (10+) — LLM-response schema validation; outlier/out-of-range proposals must use `rowIndices` not `match`.
- **Apply** (16) — every op kind round-trips; original dataset never mutated; `replace_value` with `rowIndices` regression-locks the LakeFS "no changes detected" bug; `missingTokens` override threads through to impute.
- **With-approval** (7) — low/medium/high/warning gating, `warning`-with-remembered-rule, buffered-decision race.
- **Session** (8) — recordIssue/recordDecision/auto-allow lifecycle.
- **Decision log** (6) + **decision-log-no-modify** (2) — RFC-4180 CSV shape and the post-#11a 9-column schema lock.
- **Apply-batch end-to-end** (12+) — Modify-verdict rejection, `additionalProperties` rejection, `verdict==="deny" && remember===true` rejection, residual re-scan correctness.
- **Permission-types** (4) — `@ts-expect-error` locks that `"modify"` and `modifiedAction` cannot type-check anywhere.
- **Export-jsonl** (6) — empty session, multi-row, special chars, null round-trip.
- **Frontend specs:**
  - `DataGuardRowNavigatorService` (36): cycle math, fingerprint algorithm parity, `findRowByKey`, `nextCycleStep` cycle, `purgeStaleCursors` survival semantics, **`navigate()` Promise round-3+4 contract** (rapid-click race resolves only the survivor, empty-click timeout leaves cursor put, synchronous fast-path emit-before-await race fixed).
  - `DataGuardAutoTriggerService` (7): `resolveRescanTarget` decision tree + pipeline serialization proof.
  - `DataGuardChecklistComponent` (2): **CSV-path advances cursor synchronously**, JSONL-path waits for flash-confirmed Promise (only advances on `true`).
  - `data-guard-jsonl` (15): parser edge cases — nested flatten, array stringify, blank lines, CRLF, malformed JSON skip, collision rule (nested wins regardless of source order), 100-line bulk.

The locate feature alone went through four iterative rounds — cursor preservation (purgeStaleCursors), fingerprint type-coercion (`String()` before stringify), token cancellation (`currentLocateToken`), subscribe-before-publish Promise (`firstValueFrom(race(...))` subscribes before `.next()`) — each round caught by a tightly-scoped reviewer pass. See git log on `feat/dataguard-mvp` for the full arc.

---

## 16. Differentiation

Closest UX overlap among Texera AI proposals is `mengw15`'s **UDF Copilot** (Claude-Code-style permission UX), but it operates on **code in the Monaco editor**, not data. Complementary, not competing.

| Project | Object of AI | User | Surface |
|---|---|---|---|
| UDF Copilot | Code | Developer | Monaco editor |
| Macro Operators | Workflow structure | Workflow author | Canvas |
| Self-healing workflows | Workflow JSON | Workflow author | Canvas |
| **DataGuard** | **Data** | **Domain expert / scientist** | **Chat panel + checklist** |

**Theme positioning:** the only proposal targeting the *Data / AI for Science* track.

**Sibling feature: WorkflowGuard.** Applies the same permission UX to *workflow edits* (`addOperator` / `modifyOperator` / `deleteOperator`). Independent feature, shared `pendingApproval` mechanism. See `README_WorkflowGuard_Texera.md`.

---

## 17. Why it matters

Pure AI automation in data cleaning is risky:
- AI may silently remove scientifically meaningful outliers.
- AI may introduce bias by removing data from one group more than another.
- AI may misinterpret placeholder values as real values.
- AI may make irreversible transformations the user never sanctioned.

DataGuard treats the human as the **decision-maker**, not the **reviewer of a finished job**:
- AI provides suggestions, not final decisions.
- Every mutating action requires explicit authorization (or pre-authorization via "remember", but only at the user's request and only for tiers ≤ medium).
- Each decision is supported by evidence and confidence.
- Every step is logged for audit and reproducibility.
- The workflow is fully replayable from the decision log.

**The interaction model is the contribution.** DataGuard demonstrates that Claude Code's permission-based UX — already proven for code — translates naturally to data work, and is *especially* valuable in scientific contexts where reversibility and trust matter most.

---

## 18. HCI contribution

DataGuard contributes a concrete instance of:

```
AI detects   →   AI explains with evidence   →   AI proposes specific action
       ↓
Human decides (Allow / Allow & remember / Deny)
       ↓
System applies (only if approved)   →   System records   →   Continue
```

Relevant concepts:
- Permission-based AI agency (Claude Code, MCP tool authorization).
- Trust calibration through evidence + confidence display.
- Risk-tiered auto-apply (low-risk transparency vs. high-risk gating, plus the `warning` tier for "concrete fix, but verify").
- Decision provenance and audit trail.
- Reproducibility via replayable decision logs.
- Mixed-initiative interaction with the human always at the final boundary.

---

## 19. Post-MVP follow-ups

- **Arrow / `File Scan` / `Text Input` operator support.** Auto-trigger is currently `CSVFileScan` + `CSVOldFileScan` + `JSONLFileScan`. Adding Arrow needs a binary IPC parser (`apache-arrow` package). `File Scan` / `File Scan From Input` need a suffix-based dispatcher to pick the right parser. `Text Input` has no file — would need a property-driven adapter and probably can't do the cleaned-version write-back, so DataGuard would be read-only there.
- **Modify verdict.** Currently cut. Returns only with a real natural-language → `operationParams` parser; the legacy "modify" recorded a free-text override but executed the original params, which silently lied.
- **Iceberg-backed decision log.** Via the existing Lakekeeper integration. Currently CSV.
- **`run_cleaning_workflow` tool.** Distributed cleaning by delegating to a Texera workflow for datasets that don't fit in memory.
- **`--replay decision_log.csv`.** Reproduce a cleaned dataset from a saved log without LLM calls.
- **System-prompt switch when DataGuard is active.** If we want the chat path back, the agent's system prompt should temporarily become DataGuard-focused (currently workflow-centric).
- **Disabled tools per agent.** Pass `disabledTools: ["addOperator", …]` when the auto-trigger creates an agent, so even an accidental chat doesn't risk workflow mutation.
- **Bias-check banner in the panel.** Currently `bias_check.ts` produces structured output but only the chat path consumes it.
- **Persist drag position across refresh.** Today it resets to bottom-right on reload.
- **`pageRendered$` integration cleanup.** The result-table-frame change exposes a private completion signal; a small refactor could publish it as a public `Observable` so other consumers (e.g. a "scroll to row N" feature) can subscribe.
