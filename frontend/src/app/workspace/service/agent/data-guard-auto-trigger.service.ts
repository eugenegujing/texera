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
import { Observable, filter, map } from "rxjs";
import { OperatorPredicate } from "../../types/workflow-common.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";

/**
 * DataGuard auto-trigger.
 *
 * Watches the texera-graph for newly-added dataset-reading operators
 * (CSVFileScan, TableFileScan, JSONFileScan, …) and emits a hint that the
 * agent-panel should auto-launch a DataGuard agent and open the chat panel.
 *
 * The actual agent-creation flow (POST /agents → POST /agents/:id/dataguard/dataset
 * → activate websocket → send "scan this dataset") is owned by the panel
 * component. This service is the *event source*, not the orchestrator.
 *
 * Wire from a panel:
 *
 *   constructor(private trigger: DataGuardAutoTriggerService) {}
 *   ngOnInit() {
 *     this.trigger.getDatasetAddedStream().pipe(untilDestroyed(this)).subscribe(op => {
 *       // create agent, load dataset, send message...
 *     });
 *   }
 */
@Injectable({ providedIn: "root" })
export class DataGuardAutoTriggerService {
  // Operator types that imply "the user just brought a tabular dataset onto
  // the canvas." Extend cautiously — every type here triggers DataGuard.
  private static readonly DATASET_OPERATOR_TYPES = new Set<string>([
    "CSVFileScan",
    "TableFileScan",
    "JSONFileScan",
    "ParallelCSVFileScan",
  ]);

  constructor(private readonly workflowActionService: WorkflowActionService) {}

  /**
   * Emits an OperatorPredicate every time a dataset-reading operator is
   * added to the workflow. Subscribers should react by auto-launching a
   * DataGuard agent and loading the referenced dataset.
   */
  public getDatasetAddedStream(): Observable<OperatorPredicate> {
    return this.workflowActionService
      .getTexeraGraph()
      .getOperatorAddStream()
      .pipe(
        filter((op: OperatorPredicate) =>
          DataGuardAutoTriggerService.DATASET_OPERATOR_TYPES.has(op.operatorType)
        ),
        map((op: OperatorPredicate) => op)
      );
  }

  /**
   * For tests / debugging: is a given operatorType one we'd auto-trigger on?
   */
  public isDatasetOperatorType(operatorType: string): boolean {
    return DataGuardAutoTriggerService.DATASET_OPERATOR_TYPES.has(operatorType);
  }
}
