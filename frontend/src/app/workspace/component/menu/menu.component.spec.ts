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

import { DatePipe, Location } from "@angular/common";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { RouterTestingModule } from "@angular/router/testing";
import { NzModalService, NzModalModule, NzModalRef } from "ng-zorro-antd/modal";
import { BehaviorSubject, of, Subject, throwError } from "rxjs";

import { MenuComponent } from "./menu.component";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";
import type { ExecutionDurationUpdateEvent } from "../../types/workflow-websocket.interface";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { UserService } from "../../../common/service/user/user.service";
import { StubUserService } from "../../../common/service/user/stub-user.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { DEFAULT_WORKFLOW, WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { ValidationWorkflowService, ValidationOutput } from "../../service/validation/validation-workflow.service";
import { PanelService } from "../../service/panel/panel.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { ComputingUnitState } from "../../../common/type/computing-unit-connection.interface";
import { mockPoint, mockScanPredicate } from "../../service/workflow-graph/model/mock-workflow-data";
import { saveAs } from "file-saver";
import type { ModalOptions } from "ng-zorro-antd/modal";
import type { ComputingUnitSelectionComponent } from "../power-button/computing-unit-selection.component";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import type { WorkflowMetadata } from "../../../dashboard/type/workflow-metadata.interface";
import type { NzUploadFile } from "ng-zorro-antd/upload";
import { Router } from "@angular/router";
import { USER_WORKFLOW } from "../../../app-routing.constant";
import type { Mocked } from "vitest";

vi.mock("file-saver", () => ({ saveAs: vi.fn() }));

describe("MenuComponent", () => {
  let component: MenuComponent;
  let fixture: ComponentFixture<MenuComponent>;
  let workflowActionService: WorkflowActionService;
  let executeWorkflowService: ExecuteWorkflowService;
  let validationWorkflowService: ValidationWorkflowService;
  let panelService: PanelService;
  let workflowVersionService: WorkflowVersionService;
  let workflowPersistService: WorkflowPersistService;
  let modalService: NzModalService;
  let notificationService: NotificationService;
  let location: Location;
  let validationStream$: BehaviorSubject<ValidationOutput>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [MenuComponent, HttpClientTestingModule, RouterTestingModule.withRoutes([]), NzModalModule],
      providers: [
        DatePipe,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        {
          provide: ComputingUnitStatusService,
          useValue: {
            getSelectedComputingUnit: () => of(null),
            getStatus: () => of(ComputingUnitState.NoComputingUnit),
            // Read by ComputingUnitSelectionComponent.ngOnInit when the menu
            // template renders the <texera-computing-unit-selection> child.
            getAllComputingUnits: () => of([]),
          },
        },
        { provide: UserService, useClass: StubUserService },
        ...commonTestProviders,
      ],
    }).compileComponents();

    workflowActionService = TestBed.inject(WorkflowActionService);
    executeWorkflowService = TestBed.inject(ExecuteWorkflowService);
    validationWorkflowService = TestBed.inject(ValidationWorkflowService);
    panelService = TestBed.inject(PanelService);
    workflowVersionService = TestBed.inject(WorkflowVersionService);
    workflowPersistService = TestBed.inject(WorkflowPersistService);
    modalService = TestBed.inject(NzModalService);
    notificationService = TestBed.inject(NotificationService);
    location = TestBed.inject(Location);

    validationStream$ = new BehaviorSubject<ValidationOutput>({ errors: {}, workflowEmpty: false });
    vi.spyOn(validationWorkflowService, "getWorkflowValidationErrorStream").mockReturnValue(
      validationStream$.asObservable()
    );

    fixture = TestBed.createComponent(MenuComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    vi.mocked(saveAs).mockClear();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("getRunButtonBehavior", () => {
    it("returns 'Invalid Workflow' when the workflow is invalid", () => {
      component.isWorkflowValid = false;
      component.isWorkflowEmpty = false;

      const behavior = component.getRunButtonBehavior();

      expect(behavior.text).toBe("Invalid Workflow");
      expect(behavior.icon).toBe("warning");
      expect(behavior.disable).toBe(true);
    });

    it("returns 'Empty Workflow' when the workflow has no operators", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = true;

      const behavior = component.getRunButtonBehavior();

      expect(behavior.text).toBe("Empty Workflow");
      expect(behavior.icon).toBe("info-circle");
      expect(behavior.disable).toBe(true);
    });

    it("returns 'Connect' when no computing unit is attached", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.NoComputingUnit;

      const behavior = component.getRunButtonBehavior();

      expect(behavior.text).toBe("Connect");
      expect(behavior.icon).toBe("plus-circle");
      expect(behavior.disable).toBe(false);
    });

    it("returns 'Run' when connected and execution is uninitialized", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.Running;
      Object.defineProperty(component.workflowWebsocketService, "isConnected", { get: () => true, configurable: true });
      component.executionState = ExecutionState.Uninitialized;

      const behavior = component.getRunButtonBehavior();

      expect(behavior.text).toBe("Run");
      expect(behavior.icon).toBe("play-circle");
      expect(behavior.disable).toBe(false);
    });

    it("returns 'Pause' while a workflow is running", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.Running;
      Object.defineProperty(component.workflowWebsocketService, "isConnected", { get: () => true, configurable: true });
      component.executionState = ExecutionState.Running;

      const pauseSpy = vi.spyOn(executeWorkflowService, "pauseWorkflow").mockImplementation(() => {});
      const behavior = component.getRunButtonBehavior();
      behavior.onClick();

      expect(behavior.text).toBe("Pause");
      expect(behavior.disable).toBe(false);
      expect(pauseSpy).toHaveBeenCalled();
    });

    it("returns 'Resume' when execution is paused", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.Running;
      Object.defineProperty(component.workflowWebsocketService, "isConnected", { get: () => true, configurable: true });
      component.executionState = ExecutionState.Paused;

      const resumeSpy = vi.spyOn(executeWorkflowService, "resumeWorkflow").mockImplementation(() => {});
      const behavior = component.getRunButtonBehavior();
      behavior.onClick();

      expect(behavior.text).toBe("Resume");
      expect(resumeSpy).toHaveBeenCalled();
    });

    it("returns 'Connecting' when a unit exists but the websocket is not connected", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.Running;
      Object.defineProperty(component.workflowWebsocketService, "isConnected", {
        get: () => false,
        configurable: true,
      });

      const behavior = component.getRunButtonBehavior();

      expect(behavior.text).toBe("Connecting");
      expect(behavior.disable).toBe(true);
    });
  });

  it("applyRunButtonBehavior copies the behavior onto the bound fields", () => {
    const handler = () => {};
    component.applyRunButtonBehavior({
      text: "Custom",
      icon: "custom-icon",
      disable: true,
      onClick: handler,
    });

    expect(component.runButtonText).toBe("Custom");
    expect(component.runIcon).toBe("custom-icon");
    expect(component.runDisable).toBe(true);
    expect(component.onClickRunHandler).toBe(handler);
  });

  it("re-applies run button behavior when the validation stream reports an empty workflow", () => {
    validationStream$.next({ errors: {}, workflowEmpty: true });

    expect(component.isWorkflowEmpty).toBe(true);
    expect(component.runButtonText).toBe("Empty Workflow");
    expect(component.runDisable).toBe(true);
  });

  describe("hasOperators", () => {
    it("returns false on an empty graph", () => {
      expect(component.hasOperators()).toBe(false);
    });

    it("returns true once an operator is added", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      expect(component.hasOperators()).toBe(true);
    });
  });

  it("onClickAddCommentBox delegates to the workflow action service", () => {
    const addCommentBoxSpy = vi.spyOn(workflowActionService, "addCommentBox");

    component.onClickAddCommentBox();

    expect(addCommentBoxSpy).toHaveBeenCalledTimes(1);
  });

  it("onClickDeleteAllOperators removes every operator from the graph", () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    expect(workflowActionService.getTexeraGraph().getAllOperators().length).toBe(1);

    component.onClickDeleteAllOperators();

    expect(workflowActionService.getTexeraGraph().getAllOperators().length).toBe(0);
  });

  it("onClickAutoLayout is a no-op when there are no operators", () => {
    const autoLayoutSpy = vi.spyOn(workflowActionService, "autoLayoutWorkflow");

    component.onClickAutoLayout();

    expect(autoLayoutSpy).not.toHaveBeenCalled();
  });

  it("onClickAutoLayout invokes auto layout when operators are present", () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    const autoLayoutSpy = vi.spyOn(workflowActionService, "autoLayoutWorkflow").mockImplementation(() => {});

    component.onClickAutoLayout();

    expect(autoLayoutSpy).toHaveBeenCalledTimes(1);
  });

  it("handleKill delegates to executeWorkflowService.killWorkflow", () => {
    const killSpy = vi.spyOn(executeWorkflowService, "killWorkflow").mockImplementation(() => {});

    component.handleKill();

    expect(killSpy).toHaveBeenCalledTimes(1);
  });

  it("handleCheckpoint delegates to executeWorkflowService.takeGlobalCheckpoint", () => {
    const checkpointSpy = vi.spyOn(executeWorkflowService, "takeGlobalCheckpoint").mockImplementation(() => {});

    component.handleCheckpoint();

    expect(checkpointSpy).toHaveBeenCalledTimes(1);
  });

  it("onClickClosePanels and onClickResetPanels delegate to PanelService", () => {
    const closeSpy = vi.spyOn(panelService, "closePanels").mockImplementation(() => {});
    const resetSpy = vi.spyOn(panelService, "resetPanels").mockImplementation(() => {});

    component.onClickClosePanels();
    component.onClickResetPanels();

    expect(closeSpy).toHaveBeenCalledTimes(1);
    expect(resetSpy).toHaveBeenCalledTimes(1);
  });

  describe("runWorkflow", () => {
    beforeEach(() => {
      component.computingUnitSelectionComponent = {
        showAddComputeUnitModalVisible: vi.fn(),
      } as unknown as Mocked<ComputingUnitSelectionComponent>;
    });

    it("does nothing when the workflow is invalid", () => {
      component.isWorkflowValid = false;
      component.isWorkflowEmpty = false;
      const executeSpy = vi.spyOn(executeWorkflowService, "executeWorkflowWithEmailNotification");

      component.runWorkflow();

      expect(executeSpy).not.toHaveBeenCalled();
      expect(component.computingUnitSelectionComponent.showAddComputeUnitModalVisible).not.toHaveBeenCalled();
    });

    it("does nothing when the workflow is empty", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = true;
      const executeSpy = vi.spyOn(executeWorkflowService, "executeWorkflowWithEmailNotification");

      component.runWorkflow();

      expect(executeSpy).not.toHaveBeenCalled();
    });

    it("opens the add-computing-unit modal when no unit is connected", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.NoComputingUnit;
      component.currentWorkflowName = "wf";
      const executeSpy = vi.spyOn(executeWorkflowService, "executeWorkflowWithEmailNotification");

      component.runWorkflow();

      expect(component.computingUnitSelectionComponent.showAddComputeUnitModalVisible).toHaveBeenCalledWith(
        "wf's Computing Unit"
      );
      expect(component.computingUnitSelectionComponent.showAddComputeUnitModalVisible).toHaveBeenCalledTimes(1);
      expect(executeSpy).not.toHaveBeenCalled();
    });

    it("submits the execution when connected", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.Running;
      component.currentExecutionName = "exec-1";
      const executeSpy = vi
        .spyOn(executeWorkflowService, "executeWorkflowWithEmailNotification")
        .mockImplementation(() => {});

      component.runWorkflow();

      expect(executeSpy).toHaveBeenCalledWith("exec-1", expect.any(Boolean));
    });
  });

  it("onWorkflowNameChange forwards the new name to the workflow action service", () => {
    const setNameSpy = vi.spyOn(workflowActionService, "setWorkflowName");
    component.currentWorkflowName = "renamed";

    component.onWorkflowNameChange();

    expect(setNameSpy).toHaveBeenCalledWith("renamed");
  });

  describe("onClickExportWorkflow (save)", () => {
    it("serializes the workflow content as JSON and downloads it under the workflow name", () => {
      const fakeContent = {
        operators: [{ operatorID: "op1" }],
        links: [],
        commentBoxes: [],
        settings: {},
      } as unknown as WorkflowContent;
      vi.spyOn(workflowActionService, "getWorkflowContent").mockReturnValue(fakeContent);
      component.currentWorkflowName = "my-workflow";

      component.onClickExportWorkflow();

      expect(saveAs).toHaveBeenCalledTimes(1);
      const [blobArg, fileNameArg] = vi.mocked(saveAs).mock.calls[0] as [Blob, string];
      expect(fileNameArg).toBe("my-workflow.json");
      expect(blobArg).toBeInstanceOf(Blob);
      expect(blobArg.type).toBe("text/plain;charset=utf-8");
    });
  });

  // Regression coverage for the import-duplicate bug: importing a workflow file
  // used to fabricate metadata with `wid: undefined` and a file-derived name, so
  // the auto-persist created a brand-new duplicate workflow instead of saving
  // into the currently opened one (and stomped description / published state).
  // Import must replace only the content and keep every metadata field intact.
  describe("onClickImportWorkflow (import)", () => {
    const existingMetadata: WorkflowMetadata = {
      name: "existing workflow",
      description: "existing description",
      wid: 42,
      creationTime: 1000,
      lastModifiedTime: 2000,
      isPublished: 1,
      readonly: false,
    };

    const importedContent = {
      operators: [{ operatorID: "imported-op" }],
      operatorPositions: { "imported-op": { x: 1, y: 2 } },
      links: [],
      commentBoxes: [],
      settings: {},
    } as unknown as WorkflowContent;

    // onClickImportWorkflow reads the file through an async FileReader, so tests
    // hand it a real File and await the spy they expect to fire.
    function importFile(fileContent: string, fileName = "imported-file.json"): boolean {
      const file = new File([fileContent], fileName, { type: "application/json" }) as unknown as NzUploadFile;
      return component.onClickImportWorkflow(file);
    }

    it("preserves the current workflow metadata and only replaces the content", async () => {
      workflowActionService.setWorkflowMetadata(existingMetadata);
      const reloadSpy = vi.spyOn(workflowActionService, "reloadWorkflow").mockImplementation(() => {});

      importFile(JSON.stringify(importedContent));

      await vi.waitFor(() => expect(reloadSpy).toHaveBeenCalledTimes(1));
      const imported = reloadSpy.mock.calls[0][0] as Workflow;
      expect(imported.wid).toBe(42);
      expect(imported.name).toBe("existing workflow");
      expect(imported.description).toBe("existing description");
      expect(imported.isPublished).toBe(1);
      expect(imported.creationTime).toBe(1000);
      expect(imported.lastModifiedTime).toBe(2000);
      expect(imported.readonly).toBe(false);
      expect(imported.content).toEqual(importedContent);
    });

    it("reloads with the parsed file content and async rendering enabled", async () => {
      const reloadSpy = vi.spyOn(workflowActionService, "reloadWorkflow").mockImplementation(() => {});

      const returnValue = importFile(JSON.stringify(importedContent));

      await vi.waitFor(() => expect(reloadSpy).toHaveBeenCalledTimes(1));
      expect((reloadSpy.mock.calls[0][0] as Workflow).content).toEqual(importedContent);
      expect(reloadSpy.mock.calls[0][1]).toBe(true);
      // returning false stops nz-upload from also uploading the file
      expect(returnValue).toBe(false);
    });

    it("clears the undo and redo stacks after a successful import", async () => {
      vi.spyOn(workflowActionService, "reloadWorkflow").mockImplementation(() => {});
      const clearUndoSpy = vi.spyOn(component.undoRedoService, "clearUndoStack");
      const clearRedoSpy = vi.spyOn(component.undoRedoService, "clearRedoStack");

      importFile(JSON.stringify(importedContent));

      await vi.waitFor(() => expect(clearUndoSpy).toHaveBeenCalled());
      expect(clearRedoSpy).toHaveBeenCalled();
    });

    it("notifies an error and does not reload when the file is not valid JSON", async () => {
      const reloadSpy = vi.spyOn(workflowActionService, "reloadWorkflow").mockImplementation(() => {});
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});
      vi.spyOn(console, "error").mockImplementation(() => {});

      importFile("this is not json");

      await vi.waitFor(() => expect(errorSpy).toHaveBeenCalledTimes(1));
      expect(errorSpy.mock.calls[0][0]).toContain("importing the workflow");
      expect(reloadSpy).not.toHaveBeenCalled();
    });

    it("passes a new object to reloadWorkflow instead of the live metadata reference", async () => {
      // setWorkflowMetadata early-returns on reference equality, so handing the
      // live metadata object back to reloadWorkflow would silently swallow the
      // metadata-changed broadcast; the spread must guarantee a fresh object.
      workflowActionService.setWorkflowMetadata(existingMetadata);
      const reloadSpy = vi.spyOn(workflowActionService, "reloadWorkflow").mockImplementation(() => {});

      importFile(JSON.stringify(importedContent));

      await vi.waitFor(() => expect(reloadSpy).toHaveBeenCalledTimes(1));
      expect(reloadSpy.mock.calls[0][0]).not.toBe(workflowActionService.getWorkflowMetadata());
    });

    it("keeps the default metadata when importing into a brand-new unsaved workspace", async () => {
      // Resetting with undefined deterministically restores DEFAULT_WORKFLOW.
      workflowActionService.setWorkflowMetadata(undefined);
      const reloadSpy = vi.spyOn(workflowActionService, "reloadWorkflow").mockImplementation(() => {});

      importFile(JSON.stringify(importedContent), "some-pipeline.json");

      await vi.waitFor(() => expect(reloadSpy).toHaveBeenCalledTimes(1));
      const imported = reloadSpy.mock.calls[0][0] as Workflow;
      // Even in an unsaved workspace the file name must not become the workflow name.
      expect(imported.wid).toBe(DEFAULT_WORKFLOW.wid);
      expect(imported.name).toBe(DEFAULT_WORKFLOW.name);
    });

    it("notifies an error and leaves undo/redo untouched when reload fails on malformed content", async () => {
      // Valid JSON that is not a valid WorkflowContent has no schema validation;
      // reloadWorkflow throwing is the only signal, and the outer try/catch must
      // skip the stack clearing that sits after it inside the try block.
      vi.spyOn(workflowActionService, "reloadWorkflow").mockImplementation(() => {
        throw new Error("malformed workflow content");
      });
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});
      vi.spyOn(console, "error").mockImplementation(() => {});
      const clearUndoSpy = vi.spyOn(component.undoRedoService, "clearUndoStack");
      const clearRedoSpy = vi.spyOn(component.undoRedoService, "clearRedoStack");

      importFile("{}");

      await vi.waitFor(() => expect(errorSpy).toHaveBeenCalledTimes(1));
      expect(clearUndoSpy).not.toHaveBeenCalled();
      expect(clearRedoSpy).not.toHaveBeenCalled();
    });
  });

  describe("version history", () => {
    it("onClickGetAllVersions delegates to workflowVersionService.displayWorkflowVersions", () => {
      const displaySpy = vi.spyOn(workflowVersionService, "displayWorkflowVersions").mockImplementation(() => {});

      component.onClickGetAllVersions();

      expect(displaySpy).toHaveBeenCalledTimes(1);
    });

    it("closeParticularVersionDisplay delegates to workflowVersionService", () => {
      const closeSpy = vi.spyOn(workflowVersionService, "closeParticularVersionDisplay").mockImplementation(() => {});

      component.closeParticularVersionDisplay();

      expect(closeSpy).toHaveBeenCalledTimes(1);
    });

    it("revertToVersion reverts and then persists the workflow", () => {
      const revertSpy = vi.spyOn(workflowVersionService, "revertToVersion").mockImplementation(() => {});
      const persistSpy = vi
        .spyOn(workflowPersistService, "persistWorkflow")
        .mockReturnValue(of(workflowActionService.getWorkflow()));

      component.revertToVersion();

      expect(revertSpy).toHaveBeenCalledTimes(1);
      expect(persistSpy).toHaveBeenCalledTimes(1);
    });

    it("cloneVersion notifies success and closes the version panel when cloning succeeds", () => {
      vi.spyOn(workflowVersionService, "cloneWorkflowVersion").mockReturnValue(of(42));
      const successSpy = vi.spyOn(notificationService, "success").mockImplementation(() => {});
      const closeSpy = vi.spyOn(workflowVersionService, "closeParticularVersionDisplay").mockImplementation(() => {});

      component.cloneVersion();

      expect(successSpy).toHaveBeenCalledTimes(1);
      expect(successSpy.mock.calls[0][0]).toContain("42");
      expect(closeSpy).toHaveBeenCalledTimes(1);
    });

    it("cloneVersion shows an error notification and does not close the panel when cloning fails", () => {
      vi.spyOn(workflowVersionService, "cloneWorkflowVersion").mockReturnValue(throwError(() => new Error("boom")));
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});
      const successSpy = vi.spyOn(notificationService, "success").mockImplementation(() => {});
      const closeSpy = vi.spyOn(workflowVersionService, "closeParticularVersionDisplay").mockImplementation(() => {});

      component.cloneVersion();

      expect(errorSpy).toHaveBeenCalledTimes(1);
      expect(successSpy).not.toHaveBeenCalled();
      expect(closeSpy).not.toHaveBeenCalled();
    });
  });

  describe("onClickOpenShareAccess (share)", () => {
    it("looks up workflow owners and opens the share-access modal", async () => {
      vi.spyOn(workflowPersistService, "retrieveOwners").mockReturnValue(of(["alice@example.com"]));
      const fakeModalRef = { afterClose: of(undefined) } as unknown as NzModalRef;
      const createSpy = vi.spyOn(modalService, "create").mockReturnValue(fakeModalRef);
      component.workflowId = 7;
      component.writeAccess = true;

      await component.onClickOpenShareAccess();

      expect(createSpy).toHaveBeenCalledTimes(1);
      const config = createSpy.mock.calls[0][0] as ModalOptions;
      expect(config.nzTitle).toBe("Share this workflow with others");
      expect(config.nzData).toEqual(
        expect.objectContaining({
          writeAccess: true,
          type: "workflow",
          id: 7,
          allOwners: ["alice@example.com"],
          inWorkspace: true,
        })
      );
    });

    it("navigates to /user/workflow (no /dashboard prefix) when the modal reports the owner revoked their own access", async () => {
      vi.spyOn(workflowPersistService, "retrieveOwners").mockReturnValue(of([]));
      const fakeModalRef = { afterClose: of({ userRevokedOwnAccess: true }) } as unknown as NzModalRef;
      vi.spyOn(modalService, "create").mockReturnValue(fakeModalRef);
      const router = TestBed.inject(Router);
      const navigateSpy = vi.spyOn(router, "navigate").mockResolvedValue(true);

      await component.onClickOpenShareAccess();

      expect(navigateSpy).toHaveBeenCalledWith([USER_WORKFLOW]);
      expect(USER_WORKFLOW).toBe("/user/workflow");
    });

    it("does not navigate when the share-access modal closes without revoking own access", async () => {
      vi.spyOn(workflowPersistService, "retrieveOwners").mockReturnValue(of([]));
      const fakeModalRef = { afterClose: of(undefined) } as unknown as NzModalRef;
      vi.spyOn(modalService, "create").mockReturnValue(fakeModalRef);
      const router = TestBed.inject(Router);
      const navigateSpy = vi.spyOn(router, "navigate").mockResolvedValue(true);

      await component.onClickOpenShareAccess();

      expect(navigateSpy).not.toHaveBeenCalled();
    });
  });

  it("onClickCreateNewWorkflow resets the graph and navigates back to root", () => {
    const resetSpy = vi.spyOn(workflowActionService, "resetAsNewWorkflow").mockImplementation(() => {});
    const goSpy = vi.spyOn(location, "go").mockImplementation(() => {});

    component.onClickCreateNewWorkflow();

    expect(resetSpy).toHaveBeenCalledTimes(1);
    expect(goSpy).toHaveBeenCalledWith("/");
  });

  it("onClickRestoreZoomOffsetDefault delegates to the joint graph wrapper", () => {
    const restoreSpy = vi
      .spyOn(workflowActionService.getJointGraphWrapper(), "restoreDefaultZoomAndOffset")
      .mockImplementation(() => {});

    component.onClickRestoreZoomOffsetDefault();

    expect(restoreSpy).toHaveBeenCalledTimes(1);
  });

  it("onClickEditDescription opens the markdown description modal seeded with the current description", () => {
    vi.spyOn(workflowActionService, "getWorkflow").mockReturnValue({
      content: { operators: [], links: [], commentBoxes: [], settings: {} } as unknown as WorkflowContent,
      name: "wf",
      description: "hello world",
      wid: 1,
      creationTime: undefined,
      lastModifiedTime: undefined,
      readonly: false,
      isPublished: 0,
    });
    const fakeModalRef = {
      afterClose: of(undefined),
      getContentComponent: () => ({ descriptionChange: of() }),
      close: vi.fn(),
    } as unknown as NzModalRef;
    const createSpy = vi.spyOn(modalService, "create").mockReturnValue(fakeModalRef);

    component.onClickEditDescription();

    expect(createSpy).toHaveBeenCalledTimes(1);
    const config = createSpy.mock.calls[0][0] as ModalOptions;
    expect(config.nzTitle).toBe("Edit Workflow Description");
    expect(config.nzData).toEqual({ description: "hello world" });
  });

  it("onClickExportExecutionResult opens the result-exportation modal with the current workflow name", () => {
    const fakeModalRef = { afterClose: of(undefined) } as unknown as NzModalRef;
    const createSpy = vi.spyOn(modalService, "create").mockReturnValue(fakeModalRef);
    component.currentWorkflowName = "report-wf";

    component.onClickExportExecutionResult();

    expect(createSpy).toHaveBeenCalledTimes(1);
    const config = createSpy.mock.calls[0][0] as ModalOptions;
    expect(config.nzTitle).toBe("Export All Operators Result");
    expect(config.nzData).toEqual(expect.objectContaining({ workflowName: "report-wf", sourceTriggered: "menu" }));
  });

  describe("canvas display toggles", () => {
    // A fake JointJS element that records `attr(path, value)` calls and answers `get("type")`.
    function fakeElement(type: string) {
      return {
        type,
        attrs: {} as Record<string, unknown>,
        get(key: string) {
          return key === "type" ? this.type : undefined;
        },
        attr: vi.fn(function (this: { attrs: Record<string, unknown> }, path: string, value: unknown) {
          this.attrs[path] = value;
        }),
      };
    }

    // Stubs getJointGraphWrapper() with a paper element + model/graph backed by the given elements.
    function stubWrapper(elements: ReturnType<typeof fakeElement>[]) {
      const el = document.createElement("div");
      const wrapper = {
        mainPaper: { el, model: { getElements: () => elements } },
        jointGraph: { getElements: () => elements },
      };
      vi.spyOn(workflowActionService, "getJointGraphWrapper").mockReturnValue(wrapper as any);
      return el;
    }

    describe("toggleRegion", () => {
      it("publishes the displayed flag to the joint graph wrapper when enabled", () => {
        const setSpy = vi.spyOn(workflowActionService.getJointGraphWrapper(), "setRegionsDisplayed");

        component.showRegion = true;
        component.toggleRegion();

        expect(setSpy).toHaveBeenCalledWith(true);
      });

      it("publishes the displayed flag to the joint graph wrapper when disabled", () => {
        const setSpy = vi.spyOn(workflowActionService.getJointGraphWrapper(), "setRegionsDisplayed");

        component.showRegion = false;
        component.toggleRegion();

        expect(setSpy).toHaveBeenCalledWith(false);
      });
    });

    describe("toggleStatus", () => {
      it("removes hide-operator-status when enabled and repositions the status label", () => {
        const operator = fakeElement("operator");
        const el = stubWrapper([operator]);
        el.classList.add("hide-operator-status");

        component.showStatus = true;
        component.showNumWorkers = false;
        component.toggleStatus();

        expect(el.classList.contains("hide-operator-status")).toBe(false);
        expect(operator.attr).toHaveBeenCalledWith(".texera-operator-state/ref-x", -10);
        expect(operator.attr).toHaveBeenCalledWith(".texera-operator-state/ref-y", -35);
      });

      it("adds hide-operator-status when disabled", () => {
        const operator = fakeElement("operator");
        const el = stubWrapper([operator]);

        component.showStatus = false;
        component.toggleStatus();

        expect(el.classList.contains("hide-operator-status")).toBe(true);
      });

      it("offsets the status label higher when worker counts are shown", () => {
        const operator = fakeElement("operator");
        stubWrapper([operator]);

        component.showNumWorkers = true;
        component.toggleStatus();

        expect(operator.attr).toHaveBeenCalledWith(".texera-operator-state/ref-y", -55);
      });
    });
  });

  // Regression coverage for #5323: the elapsed-time timer was refactored from a
  // manually managed `durationUpdateSubscription` into a declarative `switchMap`
  // pipe terminated by `untilDestroyed`. These tests pin the resulting behavior
  // (base-duration updates, 1s cadence, restart-on-event, stop-when-idle) and,
  // crucially, that the timer is torn down with the component so it cannot keep
  // firing or leak after destroy.
  describe("execution duration timer", () => {
    let durationEvents$: Subject<{ type: "ExecutionDurationUpdateEvent" } & ExecutionDurationUpdateEvent>;
    let timerFixture: ComponentFixture<MenuComponent>;
    let timerComponent: MenuComponent;

    function emitDuration(duration: number, isRunning: boolean): void {
      durationEvents$.next({ type: "ExecutionDurationUpdateEvent", duration, isRunning });
    }

    beforeEach(() => {
      vi.useFakeTimers();
      durationEvents$ = new Subject();
      const websocket = TestBed.inject(WorkflowWebsocketService);
      const original = websocket.subscribeToEvent.bind(websocket);
      // Only intercept the duration event; defer every other event type to the
      // real implementation so unrelated subscriptions keep working.
      vi.spyOn(websocket, "subscribeToEvent").mockImplementation((type: any) =>
        type === "ExecutionDurationUpdateEvent" ? (durationEvents$.asObservable() as any) : original(type)
      );

      timerFixture = TestBed.createComponent(MenuComponent);
      timerComponent = timerFixture.componentInstance;
      timerFixture.detectChanges();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it("sets executionDuration to the event's base duration on each event", () => {
      emitDuration(5000, false);
      expect(timerComponent.executionDuration).toBe(5000);

      emitDuration(8000, false);
      expect(timerComponent.executionDuration).toBe(8000);
    });

    it("advances the duration by 1s every second while running", () => {
      emitDuration(0, true);
      expect(timerComponent.executionDuration).toBe(0);

      vi.advanceTimersByTime(1000);
      expect(timerComponent.executionDuration).toBe(1000);

      vi.advanceTimersByTime(2000);
      expect(timerComponent.executionDuration).toBe(3000);
    });

    it("does not start a timer when the execution is not running", () => {
      emitDuration(7000, false);

      vi.advanceTimersByTime(5000);

      expect(timerComponent.executionDuration).toBe(7000);
    });

    it("restarts the 1s timer on each new running event, cancelling the previous one", () => {
      emitDuration(0, true);
      vi.advanceTimersByTime(1000);
      expect(timerComponent.executionDuration).toBe(1000);

      // A new event resets the base duration and restarts the cadence; the
      // previous timer must be cancelled (switchMap) so it cannot double-count.
      emitDuration(10000, true);
      expect(timerComponent.executionDuration).toBe(10000);

      vi.advanceTimersByTime(500);
      expect(timerComponent.executionDuration).toBe(10000);

      vi.advanceTimersByTime(500);
      expect(timerComponent.executionDuration).toBe(11000);
    });

    it("stops the timer when a running execution transitions to not running", () => {
      emitDuration(0, true);
      vi.advanceTimersByTime(1000);
      expect(timerComponent.executionDuration).toBe(1000);

      emitDuration(2000, false);
      vi.advanceTimersByTime(5000);
      expect(timerComponent.executionDuration).toBe(2000);
    });

    it("tears down the timer on destroy so the duration stops advancing", () => {
      emitDuration(0, true);
      vi.advanceTimersByTime(1000);
      expect(timerComponent.executionDuration).toBe(1000);

      timerFixture.destroy();

      // The previously running timer must not keep firing after destroy...
      vi.advanceTimersByTime(5000);
      expect(timerComponent.executionDuration).toBe(1000);

      // ...nor should late events revive it (the source subscription is closed).
      emitDuration(9999, true);
      vi.advanceTimersByTime(5000);
      expect(timerComponent.executionDuration).toBe(1000);
    });
  });

  // Regression coverage for #5323: the computing-unit status subscription lost
  // its manual `computingUnitStatusSubscription` aggregator and its
  // `ngOnDestroy` unsubscribe, relying on `untilDestroyed` instead. These tests
  // pin both that status updates still propagate and that they stop on destroy.
  describe("computing unit status subscription", () => {
    let status$: Subject<ComputingUnitState>;
    let cuFixture: ComponentFixture<MenuComponent>;
    let cuComponent: MenuComponent;

    beforeEach(() => {
      status$ = new Subject<ComputingUnitState>();
      const cuService = TestBed.inject(ComputingUnitStatusService);
      vi.spyOn(cuService, "getStatus").mockReturnValue(status$.asObservable());

      cuFixture = TestBed.createComponent(MenuComponent);
      cuComponent = cuFixture.componentInstance;
      cuFixture.detectChanges();
    });

    it("updates computingUnitStatus and re-applies the run button behavior on each status emission", () => {
      const applySpy = vi.spyOn(cuComponent, "applyRunButtonBehavior");

      status$.next(ComputingUnitState.Running);

      expect(cuComponent.computingUnitStatus).toBe(ComputingUnitState.Running);
      expect(applySpy).toHaveBeenCalledTimes(1);
    });

    it("stops updating computingUnitStatus once the component is destroyed", () => {
      status$.next(ComputingUnitState.Running);
      expect(cuComponent.computingUnitStatus).toBe(ComputingUnitState.Running);

      cuFixture.destroy();

      status$.next(ComputingUnitState.NoComputingUnit);
      expect(cuComponent.computingUnitStatus).toBe(ComputingUnitState.Running);
    });
  });
});
