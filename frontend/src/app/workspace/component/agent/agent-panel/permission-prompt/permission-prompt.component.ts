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

import { Component, Input } from "@angular/core";
import { NgIf } from "@angular/common";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { ReActStep } from "../../../../service/agent/agent-types";
import { AgentService } from "../../../../service/agent/agent.service";

/**
 * DataGuard permission prompt. Rendered inline in the agent chat panel for
 * any ReActStep whose `pendingApproval` field is set. The user's click sends
 * a {type:"decision", stepId, verdict, ...} message over the agent WS; the
 * server-side ReAct loop resumes once the awaiting tool promise resolves.
 */
@Component({
  selector: "texera-permission-prompt",
  standalone: true,
  imports: [NgIf, NzButtonComponent],
  templateUrl: "./permission-prompt.component.html",
  styleUrls: ["./permission-prompt.component.scss"],
})
export class PermissionPromptComponent {
  @Input() step!: ReActStep;
  @Input() agentId!: string;

  public submitted = false;

  constructor(private readonly agentService: AgentService) {}

  public onAllow(remember: boolean): void {
    if (this.submitted) return;
    this.submitted = true;
    this.agentService.sendDecision(this.agentId, this.step.id, "allow", { remember });
  }

  public onDeny(): void {
    if (this.submitted) return;
    this.submitted = true;
    this.agentService.sendDecision(this.agentId, this.step.id, "deny");
  }
}
