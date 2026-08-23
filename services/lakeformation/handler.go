package lakeformation

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	lakeformationService       = "lakeformation"
	lakeformationMatchPriority = 87
)

// isLakeFormationPath reports whether path is a supported LakeFormation operation path.
func isLakeFormationPath(path string) bool {
	switch path {
	case "/GetDataLakeSettings",
		"/PutDataLakeSettings",
		"/RegisterResource",
		"/UpdateResource",
		"/DeregisterResource",
		"/DescribeResource",
		"/ListResources",
		"/GrantPermissions",
		"/RevokePermissions",
		"/ListPermissions",
		"/CreateLFTag",
		"/DeleteLFTag",
		"/GetLFTag",
		"/UpdateLFTag",
		"/ListLFTags",
		"/BatchGrantPermissions",
		"/BatchRevokePermissions",
		"/AddLFTagsToResource",
		"/RemoveLFTagsFromResource",
		"/GetResourceLFTags",
		"/AssumeDecoratedRoleWithSAML",
		"/StartTransaction",
		"/CancelTransaction",
		"/CommitTransaction",
		"/DescribeTransaction",
		"/ListTransactions",
		"/CreateDataCellsFilter",
		"/ListDataCellsFilter",
		"/CreateLFTagExpression",
		"/ListLFTagExpressions",
		"/CreateLakeFormationIdentityCenterConfiguration",
		"/CreateLakeFormationOptIn",
		"/DeleteLakeFormationOptIn",
		"/ListLakeFormationOptIns",
		"/DeleteDataCellsFilter",
		"/DeleteLFTagExpression",
		"/GetDataLakePrincipal",
		"/DeleteLakeFormationIdentityCenterConfiguration",
		"/DeleteObjectsOnCancel",
		"/DescribeLakeFormationIdentityCenterConfiguration",
		"/ExtendTransaction",
		"/GetDataCellsFilter",
		"/GetEffectivePermissionsForPath",
		"/GetLFTagExpression",
		"/GetQueryState",
		"/GetQueryStatistics",
		"/GetTableObjects",
		"/GetTemporaryDataLocationCredentials",
		"/GetTemporaryGluePartitionCredentials",
		"/GetTemporaryGlueTableCredentials",
		"/GetWorkUnitResults",
		"/GetWorkUnits",
		"/ListTableStorageOptimizers",
		"/SearchDatabasesByLFTags",
		"/SearchTablesByLFTags",
		"/StartQueryPlanning",
		"/UpdateDataCellsFilter",
		"/UpdateLFTagExpression",
		"/UpdateLakeFormationIdentityCenterConfiguration",
		"/UpdateTableObjects",
		"/UpdateTableStorageOptimizer":
		return true
	}

	return false
}

// Handler is the HTTP handler for the Lake Formation REST API.
type Handler struct {
	Backend       StorageBackend
	ops           map[string]func(context.Context, *echo.Context, []byte) error
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new Lake Formation handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset resets the backend to a clean state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "LakeFormation" }

// GetSupportedOperations returns the list of supported Lake Formation operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AddLFTagsToResource",
		"AssumeDecoratedRoleWithSAML",
		"BatchGrantPermissions",
		"BatchRevokePermissions",
		"CancelTransaction",
		"CommitTransaction",
		"CreateDataCellsFilter",
		"CreateLFTag",
		"CreateLFTagExpression",
		"CreateLakeFormationIdentityCenterConfiguration",
		"CreateLakeFormationOptIn",
		"DeleteDataCellsFilter",
		"DeleteLFTag",
		"DeleteLFTagExpression",
		"DeleteLakeFormationIdentityCenterConfiguration",
		"DeleteLakeFormationOptIn",
		"DeleteObjectsOnCancel",
		"DeregisterResource",
		"DescribeLakeFormationIdentityCenterConfiguration",
		"DescribeResource",
		"DescribeTransaction",
		"ExtendTransaction",
		"GetDataCellsFilter",
		"GetDataLakePrincipal",
		"GetDataLakeSettings",
		"GetEffectivePermissionsForPath",
		"GetLFTag",
		"GetLFTagExpression",
		"GetQueryState",
		"GetQueryStatistics",
		"GetResourceLFTags",
		"GetTableObjects",
		"GetTemporaryDataLocationCredentials",
		"GetTemporaryGluePartitionCredentials",
		"GetTemporaryGlueTableCredentials",
		"GetWorkUnitResults",
		"GetWorkUnits",
		"GrantPermissions",
		"ListDataCellsFilter",
		"ListLFTagExpressions",
		"ListLFTags",
		"ListLakeFormationOptIns",
		"ListPermissions",
		"ListResources",
		"ListTableStorageOptimizers",
		"ListTransactions",
		"PutDataLakeSettings",
		"RegisterResource",
		"RemoveLFTagsFromResource",
		"RevokePermissions",
		"SearchDatabasesByLFTags",
		"SearchTablesByLFTags",
		"StartQueryPlanning",
		"StartTransaction",
		"UpdateDataCellsFilter",
		"UpdateLFTag",
		"UpdateLFTagExpression",
		"UpdateLakeFormationIdentityCenterConfiguration",
		"UpdateResource",
		"UpdateTableObjects",
		"UpdateTableStorageOptimizer",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return lakeformationService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches Lake Formation REST API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
		if !isLakeFormationPath(path) {
			return false
		}

		return httputils.ExtractServiceFromRequest(c.Request()) == lakeformationService
	}
}

// MatchPriority returns the routing priority for this service.
func (h *Handler) MatchPriority() int { return lakeformationMatchPriority }

// ExtractOperation extracts the operation name by stripping the leading slash.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().URL.Path, "/")
}

// ExtractResource returns an empty string (LakeFormation uses body-level resources).
func (h *Handler) ExtractResource(_ *echo.Context) string { return "" }

// Handler returns the Echo handler function for Lake Formation requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		if c.Request().Method != http.MethodPost {
			return h.writeError(c, http.StatusMethodNotAllowed, "InvalidInputException", "Method not allowed")
		}

		op := h.ExtractOperation(c)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "lakeformation: failed to read request body", "error", err)

			return h.writeError(
				c,
				http.StatusInternalServerError,
				"InternalServiceException",
				"failed to read request body",
			)
		}

		log.DebugContext(ctx, "lakeformation request", "op", op)

		return h.dispatch(ctx, c, op, body)
	}
}

func (h *Handler) buildOps() map[string]func(context.Context, *echo.Context, []byte) error {
	return map[string]func(context.Context, *echo.Context, []byte) error{
		"GetDataLakeSettings":    h.handleGetDataLakeSettings,
		"PutDataLakeSettings":    h.handlePutDataLakeSettings,
		"RegisterResource":       h.handleRegisterResource,
		"UpdateResource":         h.handleUpdateResource,
		"DeregisterResource":     h.handleDeregisterResource,
		"DescribeResource":       h.handleDescribeResource,
		"ListResources":          h.handleListResources,
		"GrantPermissions":       h.handleGrantPermissions,
		"RevokePermissions":      h.handleRevokePermissions,
		"ListPermissions":        h.handleListPermissions,
		"CreateLFTag":            h.handleCreateLFTag,
		"DeleteLFTag":            h.handleDeleteLFTag,
		"GetLFTag":               h.handleGetLFTag,
		"UpdateLFTag":            h.handleUpdateLFTag,
		"ListLFTags":             h.handleListLFTags,
		"BatchGrantPermissions":  h.handleBatchGrantPermissions,
		"BatchRevokePermissions": h.handleBatchRevokePermissions,

		"AddLFTagsToResource":      h.handleAddLFTagsToResource,
		"RemoveLFTagsFromResource": h.handleRemoveLFTagsFromResource,
		"GetResourceLFTags":        h.handleGetResourceLFTags,

		"AssumeDecoratedRoleWithSAML": h.handleAssumeDecoratedRoleWithSAML,

		"StartTransaction":    h.handleStartTransaction,
		"CancelTransaction":   h.handleCancelTransaction,
		"CommitTransaction":   h.handleCommitTransaction,
		"DescribeTransaction": h.handleDescribeTransaction,
		"ListTransactions":    h.handleListTransactions,

		"CreateDataCellsFilter": h.handleCreateDataCellsFilter,
		"ListDataCellsFilter":   h.handleListDataCellsFilter,

		"CreateLFTagExpression": h.handleCreateLFTagExpression,
		"ListLFTagExpressions":  h.handleListLFTagExpressions,

		"CreateLakeFormationIdentityCenterConfiguration": h.handleCreateLakeFormationIdentityCenterConfiguration,

		"CreateLakeFormationOptIn": h.handleCreateLakeFormationOptIn,
		"DeleteLakeFormationOptIn": h.handleDeleteLakeFormationOptIn,
		"ListLakeFormationOptIns":  h.handleListLakeFormationOptIns,

		"DeleteDataCellsFilter": h.handleDeleteDataCellsFilter,
		"DeleteLFTagExpression": h.handleDeleteLFTagExpression,

		"GetDataLakePrincipal": h.handleGetDataLakePrincipal,

		"DeleteLakeFormationIdentityCenterConfiguration":   h.handleDeleteLakeFormationIdentityCenterConfiguration,
		"DeleteObjectsOnCancel":                            h.handleDeleteObjectsOnCancel,
		"DescribeLakeFormationIdentityCenterConfiguration": h.handleDescribeLakeFormationIdentityCenterConfiguration,
		"ExtendTransaction":                                h.handleExtendTransaction,
		"GetDataCellsFilter":                               h.handleGetDataCellsFilter,
		"GetEffectivePermissionsForPath":                   h.handleGetEffectivePermissionsForPath,
		"GetLFTagExpression":                               h.handleGetLFTagExpression,
		"GetQueryState":                                    h.handleGetQueryState,
		"GetQueryStatistics":                               h.handleGetQueryStatistics,
		"GetTableObjects":                                  h.handleGetTableObjects,
		"GetTemporaryDataLocationCredentials":              h.handleGetTemporaryDataLocationCredentials,
		"GetTemporaryGluePartitionCredentials":             h.handleGetTemporaryGluePartitionCredentials,
		"GetTemporaryGlueTableCredentials":                 h.handleGetTemporaryGlueTableCredentials,
		"GetWorkUnitResults":                               h.handleGetWorkUnitResults,
		"GetWorkUnits":                                     h.handleGetWorkUnits,
		"ListTableStorageOptimizers":                       h.handleListTableStorageOptimizers,
		"SearchDatabasesByLFTags":                          h.handleSearchDatabasesByLFTags,
		"SearchTablesByLFTags":                             h.handleSearchTablesByLFTags,
		"StartQueryPlanning":                               h.handleStartQueryPlanning,
		"UpdateDataCellsFilter":                            h.handleUpdateDataCellsFilter,
		"UpdateLFTagExpression":                            h.handleUpdateLFTagExpression,
		"UpdateLakeFormationIdentityCenterConfiguration":   h.handleUpdateLakeFormationIdentityCenterConfiguration,
		"UpdateTableObjects":                               h.handleUpdateTableObjects,
		"UpdateTableStorageOptimizer":                      h.handleUpdateTableStorageOptimizer,
	}
}

func (h *Handler) dispatch(ctx context.Context, c *echo.Context, op string, body []byte) error {
	fn, ok := h.ops[op]
	if !ok {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "unknown operation: "+op)
	}

	return fn(ctx, c, body)
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrValidation):
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	case errors.Is(err, awserr.ErrNotFound):
		return h.writeError(c, http.StatusNotFound, "EntityNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return h.writeError(c, http.StatusConflict, "AlreadyExistsException", err.Error())
	case errors.Is(err, errTransactionCommitted):
		return h.writeError(c, http.StatusBadRequest, "TransactionCommittedException", err.Error())
	case errors.Is(err, awserr.ErrConflict):
		return h.writeError(c, http.StatusBadRequest, "TransactionCanceledException", err.Error())
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalServiceException", err.Error())
	}
}

func (h *Handler) writeError(c *echo.Context, status int, errType, msg string) error {
	return c.JSON(status, errorResponse{Type: errType, Message: msg})
}
