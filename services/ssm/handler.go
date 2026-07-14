package ssm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

var ErrUnknownOperation = errors.New("UnknownOperationException")

const errCodeDoesNotExist = "DoesNotExistException"

type regionContextKey struct{}

// Handler is the Echo HTTP service handler for SSM operations.
type Handler struct {
	Backend StorageBackend
	janitor *Janitor
	ops     map[string]ssmActionFn
}

// NewHandler creates a new SSM handler with the given storage backend.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.ssmDispatchTable()

	return h
}

// WithJanitor attaches a background janitor to the handler.
// The janitor periodically evicts expired commands. interval=0 uses the default.
func (h *Handler) WithJanitor(interval time.Duration, taskTimeout ...time.Duration) *Handler {
	if memBackend, ok := h.Backend.(*InMemoryBackend); ok {
		j := NewJanitor(memBackend, interval)
		if len(taskTimeout) > 0 {
			j.TaskTimeout = taskTimeout[0]
		}
		h.janitor = j
	}

	return h
}

// StartWorker starts the background janitor if it is configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "SSM"
}

// GetSupportedOperations returns the list of mocked SSM operations.
//
//nolint:funlen // exhaustive list of 152 operations — splitting would reduce readability
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AddTagsToResource",
		"AssociateOpsItemRelatedItem",
		"CancelCommand",
		"CancelMaintenanceWindowExecution",
		"CreateActivation",
		"CreateAssociation",
		"CreateAssociationBatch",
		"CreateCloudConnector",
		"CreateDocument",
		"CreateMaintenanceWindow",
		"CreateOpsItem",
		"CreateOpsMetadata",
		"CreatePatchBaseline",
		"CreateResourceDataSync",
		"DeleteActivation",
		"DeleteAssociation",
		"DeleteCloudConnector",
		"DeleteDocument",
		"DeleteInventory",
		"DeleteMaintenanceWindow",
		"DeleteOpsItem",
		"DeleteOpsMetadata",
		"DeleteParameter",
		"DeleteParameters",
		"DeletePatchBaseline",
		"DeleteResourceDataSync",
		"DeleteResourcePolicy",
		"DeregisterManagedInstance",
		"DeregisterPatchBaselineForPatchGroup",
		"DeregisterTargetFromMaintenanceWindow",
		"DeregisterTaskFromMaintenanceWindow",
		"DescribeActivations",
		"DescribeAssociation",
		"DescribeAssociationExecutionTargets",
		"DescribeAssociationExecutions",
		"DescribeAutomationExecutions",
		"DescribeAutomationStepExecutions",
		"DescribeAvailablePatches",
		"DescribeDocument",
		"DescribeDocumentPermission",
		"DescribeEffectiveInstanceAssociations",
		"DescribeEffectivePatchesForPatchBaseline",
		"DescribeInstanceAssociationsStatus",
		"DescribeInstanceInformation",
		"DescribeInstancePatchStates",
		"DescribeInstancePatchStatesForPatchGroup",
		"DescribeInstancePatches",
		"DescribeInstanceProperties",
		"DescribeInventoryDeletions",
		"DescribeMaintenanceWindowExecutionTaskInvocations",
		"DescribeMaintenanceWindowExecutionTasks",
		"DescribeMaintenanceWindowExecutions",
		"DescribeMaintenanceWindowSchedule",
		"DescribeMaintenanceWindowTargets",
		"DescribeMaintenanceWindowTasks",
		"DescribeMaintenanceWindows",
		"DescribeMaintenanceWindowsForTarget",
		"DescribeOpsItems",
		"DescribeParameters",
		"DescribePatchBaselines",
		"DescribePatchGroupState",
		"DescribePatchGroups",
		"DescribePatchProperties",
		"DescribeSessions",
		"DisassociateOpsItemRelatedItem",
		"GetAccessToken",
		"GetAutomationExecution",
		"GetCalendarState",
		"GetCloudConnector",
		"GetCommandInvocation",
		"GetConnectionStatus",
		"GetDefaultPatchBaseline",
		"GetDeployablePatchSnapshotForInstance",
		"GetDocument",
		"GetExecutionPreview",
		"GetInventory",
		"GetInventorySchema",
		"GetMaintenanceWindow",
		"GetMaintenanceWindowExecution",
		"GetMaintenanceWindowExecutionTask",
		"GetMaintenanceWindowExecutionTaskInvocation",
		"GetMaintenanceWindowTask",
		"GetOpsItem",
		"GetOpsMetadata",
		"GetOpsSummary",
		"GetParameter",
		"GetParameterHistory",
		"GetParameters",
		"GetParametersByPath",
		"GetPatchBaseline",
		"GetPatchBaselineForPatchGroup",
		"GetResourcePolicies",
		"GetServiceSetting",
		"LabelParameterVersion",
		"ListAssociationVersions",
		"ListAssociations",
		"ListCloudConnectors",
		"ListCommandInvocations",
		"ListCommands",
		"ListComplianceItems",
		"ListComplianceSummaries",
		"ListDocumentMetadataHistory",
		"ListDocumentVersions",
		"ListDocuments",
		"ListInventoryEntries",
		"ListNodes",
		"ListNodesSummary",
		"ListOpsItemEvents",
		"ListOpsItemRelatedItems",
		"ListOpsMetadata",
		"ListResourceComplianceSummaries",
		"ListResourceDataSync",
		"ListTagsForResource",
		"ModifyDocumentPermission",
		"PutComplianceItems",
		"PutInventory",
		"PutParameter",
		"PutResourcePolicy",
		"RegisterDefaultPatchBaseline",
		"RegisterPatchBaselineForPatchGroup",
		"RegisterTargetWithMaintenanceWindow",
		"RegisterTaskWithMaintenanceWindow",
		"RemoveTagsFromResource",
		"ResetServiceSetting",
		"ResumeSession",
		"SendAutomationSignal",
		"SendCommand",
		"StartAccessRequest",
		"StartAssociationsOnce",
		"StartAutomationExecution",
		"StartChangeRequestExecution",
		"StartExecutionPreview",
		"StartSession",
		"StopAutomationExecution",
		"TerminateSession",
		"UnlabelParameterVersion",
		"UpdateAssociation",
		"UpdateAssociationStatus",
		"UpdateCloudConnector",
		"UpdateDocument",
		"UpdateDocumentDefaultVersion",
		"UpdateDocumentMetadata",
		"UpdateMaintenanceWindow",
		"UpdateMaintenanceWindowTarget",
		"UpdateMaintenanceWindowTask",
		"UpdateManagedInstanceRole",
		"UpdateOpsItem",
		"UpdateOpsMetadata",
		"UpdatePatchBaseline",
		"UpdateResourceDataSync",
		"UpdateServiceSetting",
		"ValidateCloudConnector",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "ssm" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this SSM instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches incoming requests for SSM.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "AmazonSSM")
	}
}

// MatchPriority returns the routing priority for the SSM handler.
func (h *Handler) MatchPriority() int {
	return service.PriorityHeaderExact // Same header-based priority as DynamoDB
}

// ExtractOperation attempts to extract the specific SSM operation from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")
	const targetParts = 2
	if len(parts) == targetParts {
		return parts[1]
	}

	return "Unknown"
}

// ExtractResource attempts to extract the specific SSM resource from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	if name, exists := data["Name"]; exists {
		if nameStr, ok := name.(string); ok {
			return nameStr
		}
	}

	return ""
}

// Handler is the Echo HTTP handler for SSM operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		region := httputils.ExtractRegionFromRequest(c.Request(), config.DefaultRegion)
		ctx = context.WithValue(ctx, regionContextKey{}, region)

		return service.HandleTarget(
			c, logger.Load(ctx),
			"SSM", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(_ context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(ctx, action, body)
			},
			h.handleError,
		)
	}
}

type ssmActionFn func(context.Context, []byte) (any, error)

func (h *Handler) ssmDispatchTable() map[string]ssmActionFn {
	ops := h.ssmParameterOps()
	maps.Copy(ops, h.ssmTagOps())
	maps.Copy(ops, h.ssmDocumentOps())
	maps.Copy(ops, h.ssmCommandOps())
	maps.Copy(ops, h.ssmNewOps())
	maps.Copy(ops, h.ssmCloudConnectorOps())
	maps.Copy(ops, h.ssmStubOps())

	return ops
}

func (h *Handler) ssmParameterOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"PutParameter": func(ctx context.Context, b []byte) (any, error) {
			var input PutParameterInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PutParameter(ctx, &input)
		},
		"GetParameter": func(ctx context.Context, b []byte) (any, error) {
			var input GetParameterInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetParameter(ctx, &input)
		},
		"GetParameters": func(ctx context.Context, b []byte) (any, error) {
			var input GetParametersInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetParameters(ctx, &input)
		},
		"GetParameterHistory": func(ctx context.Context, b []byte) (any, error) {
			var input GetParameterHistoryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetParameterHistory(ctx, &input)
		},
		"DeleteParameter": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteParameterInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteParameter(ctx, &input)
		},
		"DeleteParameters": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteParametersInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteParameters(ctx, &input)
		},
		"GetParametersByPath": func(ctx context.Context, b []byte) (any, error) {
			var input GetParametersByPathInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetParametersByPath(ctx, &input)
		},
		"DescribeParameters": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeParametersInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeParameters(ctx, &input)
		},
	}
}

func (h *Handler) ssmTagOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"AddTagsToResource": func(ctx context.Context, b []byte) (any, error) {
			var input AddTagsToResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.AddTagsToResource(ctx, &input)
		},
		"RemoveTagsFromResource": func(ctx context.Context, b []byte) (any, error) {
			var input RemoveTagsFromResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.RemoveTagsFromResource(ctx, &input)
		},
		"ListTagsForResource": func(ctx context.Context, b []byte) (any, error) {
			var input ListTagsForResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListTagsForResource(ctx, &input)
		},
	}
}

func (h *Handler) ssmDocumentOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"CreateDocument": func(ctx context.Context, b []byte) (any, error) {
			var input CreateDocumentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateDocument(ctx, &input)
		},
		"GetDocument": func(ctx context.Context, b []byte) (any, error) {
			var input GetDocumentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetDocument(ctx, &input)
		},
		"DescribeDocument": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeDocumentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeDocument(ctx, &input)
		},
		"ListDocuments": func(ctx context.Context, b []byte) (any, error) {
			var input ListDocumentsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListDocuments(ctx, &input)
		},
		"UpdateDocument": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateDocumentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateDocument(ctx, &input)
		},
		"DeleteDocument": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteDocumentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteDocument(ctx, &input)
		},
		"DescribeDocumentPermission": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeDocumentPermissionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeDocumentPermission(ctx, &input)
		},
		"ModifyDocumentPermission": func(ctx context.Context, b []byte) (any, error) {
			var input ModifyDocumentPermissionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ModifyDocumentPermission(ctx, &input)
		},
		"ListDocumentVersions": func(ctx context.Context, b []byte) (any, error) {
			var input ListDocumentVersionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListDocumentVersions(ctx, &input)
		},
	}
}

func (h *Handler) ssmCommandOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"SendCommand": func(ctx context.Context, b []byte) (any, error) {
			var input SendCommandInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.SendCommand(ctx, &input)
		},
		"ListCommands": func(ctx context.Context, b []byte) (any, error) {
			var input ListCommandsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListCommands(ctx, &input)
		},
		"GetCommandInvocation": func(ctx context.Context, b []byte) (any, error) {
			var input GetCommandInvocationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetCommandInvocation(ctx, &input)
		},
		"ListCommandInvocations": func(ctx context.Context, b []byte) (any, error) {
			var input ListCommandInvocationsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListCommandInvocations(ctx, &input)
		},
	}
}

func (h *Handler) ssmCloudConnectorOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"CreateCloudConnector": func(ctx context.Context, b []byte) (any, error) {
			var input CreateCloudConnectorInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateCloudConnector(ctx, &input)
		},
		"DeleteCloudConnector": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteCloudConnectorInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteCloudConnector(ctx, &input)
		},
		"GetCloudConnector": func(ctx context.Context, b []byte) (any, error) {
			var input GetCloudConnectorInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetCloudConnector(ctx, &input)
		},
		"ListCloudConnectors": func(ctx context.Context, b []byte) (any, error) {
			var input ListCloudConnectorsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListCloudConnectors(ctx, &input)
		},
		"UpdateCloudConnector": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateCloudConnectorInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateCloudConnector(ctx, &input)
		},
		"ValidateCloudConnector": func(ctx context.Context, b []byte) (any, error) {
			var input ValidateCloudConnectorInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ValidateCloudConnector(ctx, &input)
		},
	}
}

// dispatch routes the operation to the appropriate handler.
func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}

	response, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(response)
}

// classifySSMError maps a backend error to an HTTP status code and error type string.
func classifySSMError(reqErr error) (string, int) {
	statusCode := http.StatusBadRequest

	switch {
	case errors.Is(reqErr, ErrParameterVersionNotFound):
		return "ParameterVersionNotFound", statusCode
	case errors.Is(reqErr, ErrParameterNotFound):
		return "ParameterNotFound", statusCode
	case errors.Is(reqErr, ErrParameterAlreadyExists):
		return "ParameterAlreadyExists", statusCode
	case errors.Is(reqErr, ErrDocumentAlreadyExists):
		return "DocumentAlreadyExists", statusCode
	case errors.Is(reqErr, ErrDocumentNotFound):
		return "InvalidDocument", statusCode
	case errors.Is(reqErr, ErrInvalidDocumentVersion):
		return "InvalidDocumentVersion", statusCode
	case errors.Is(reqErr, ErrCommandNotFound):
		return "InvalidCommandId", statusCode
	case errors.Is(reqErr, ErrValidationException):
		return "ValidationException", statusCode
	case errors.Is(reqErr, ErrHierarchyLevelLimitExceeded):
		return "HierarchyLevelLimitExceededException", statusCode
	case errors.Is(reqErr, ErrParameterMaxVersionLimitExceeded):
		return "ParameterMaxVersionLimitExceeded", statusCode
	}

	return classifySSMErrorExtended(reqErr)
}

func classifySSMErrorExtended(reqErr error) (string, int) {
	statusCode := http.StatusBadRequest

	switch {
	case errors.Is(reqErr, ErrCloudConnectorNotFound):
		return "ResourceNotFoundException", statusCode
	case errors.Is(reqErr, ErrActivationNotFound):
		return "ActivationNotFound", statusCode
	case errors.Is(reqErr, ErrAssociationNotFound):
		return "AssociationDoesNotExist", statusCode
	case errors.Is(reqErr, ErrMaintenanceWindowExecutionNotFound):
		return errCodeDoesNotExist, statusCode
	case errors.Is(reqErr, ErrMaintenanceWindowNotFound):
		return errCodeDoesNotExist, statusCode
	case errors.Is(reqErr, ErrOpsItemNotFound):
		return "OpsItemNotFoundException", statusCode
	case errors.Is(reqErr, ErrOpsMetadataNotFound):
		return "OpsMetadataNotFoundException", statusCode
	case errors.Is(reqErr, ErrOpsMetadataAlreadyExists):
		return "OpsMetadataAlreadyExistsException", statusCode
	case errors.Is(reqErr, ErrPatchBaselineNotFound):
		return errCodeDoesNotExist, statusCode
	case errors.Is(reqErr, ErrUnknownOperation):
		return "UnknownOperationException", statusCode
	default:
		return "InternalServerError", http.StatusInternalServerError
	}
}

// handleError writes a standardized error response back to the client.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	errorType, statusCode := classifySSMError(reqErr)

	if errorType == "InternalServerError" {
		log.ErrorContext(ctx, "SSM internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "SSM request error", "error", reqErr, "action", action)
	}

	errResp := service.JSONErrorResponse{
		Type:    errorType,
		Message: reqErr.Error(),
	}

	payload, _ := json.Marshal(errResp)

	return c.JSONBlob(statusCode, payload)
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}
}

func (h *Handler) ssmNewOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"CancelCommand": func(ctx context.Context, b []byte) (any, error) {
			var input CancelCommandInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CancelCommand(ctx, &input)
		},
		"CancelMaintenanceWindowExecution": func(ctx context.Context, b []byte) (any, error) {
			var input CancelMaintenanceWindowExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CancelMaintenanceWindowExecution(ctx, &input)
		},
		"CreateActivation": func(ctx context.Context, b []byte) (any, error) {
			var input CreateActivationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateActivation(ctx, &input)
		},
		"CreateAssociation": func(ctx context.Context, b []byte) (any, error) {
			var input CreateAssociationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateAssociation(ctx, &input)
		},
		"CreateAssociationBatch": func(ctx context.Context, b []byte) (any, error) {
			var input CreateAssociationBatchInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateAssociationBatch(ctx, &input)
		},
		"CreateMaintenanceWindow": func(ctx context.Context, b []byte) (any, error) {
			var input CreateMaintenanceWindowInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateMaintenanceWindow(ctx, &input)
		},
		"CreateOpsItem": func(ctx context.Context, b []byte) (any, error) {
			var input CreateOpsItemInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateOpsItem(ctx, &input)
		},
		"CreateOpsMetadata": func(ctx context.Context, b []byte) (any, error) {
			var input CreateOpsMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateOpsMetadata(ctx, &input)
		},
		"CreatePatchBaseline": func(ctx context.Context, b []byte) (any, error) {
			var input CreatePatchBaselineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreatePatchBaseline(ctx, &input)
		},
		"AssociateOpsItemRelatedItem": func(ctx context.Context, b []byte) (any, error) {
			var input AssociateOpsItemRelatedItemInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.AssociateOpsItemRelatedItem(ctx, &input)
		},
	}
}
