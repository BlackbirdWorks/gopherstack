package lakeformation

import (
	"context"
	"encoding/json"
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
			return c.String(http.StatusMethodNotAllowed, "Method not allowed")
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

func (h *Handler) handleGetDataLakeSettings(_ context.Context, c *echo.Context, body []byte) error {
	var in getDataLakeSettingsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	settings := h.Backend.GetDataLakeSettings()

	return c.JSON(http.StatusOK, getDataLakeSettingsOutput{DataLakeSettings: settings})
}

func (h *Handler) handlePutDataLakeSettings(_ context.Context, c *echo.Context, body []byte) error {
	var in putDataLakeSettingsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if in.DataLakeSettings == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "DataLakeSettings is required")
	}

	h.Backend.PutDataLakeSettings(in.DataLakeSettings)

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleRegisterResource(_ context.Context, c *echo.Context, body []byte) error {
	var in registerResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if strings.TrimSpace(in.ResourceArn) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "ResourceArn is required")
	}

	if in.UseServiceLinkedRole && in.RoleArn != "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException",
			"RoleArn must not be specified when UseServiceLinkedRole is true")
	}

	roleArn := in.RoleArn
	if in.UseServiceLinkedRole {
		roleArn = "arn:aws:iam::123456789012:role/aws-service-role/" +
			"lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess"
	}

	if err := h.Backend.RegisterResource(in.ResourceArn, roleArn); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, registerResourceOutput{})
}

func (h *Handler) handleDeregisterResource(_ context.Context, c *echo.Context, body []byte) error {
	var in deregisterResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if err := h.Backend.DeregisterResource(in.ResourceArn); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deregisterResourceOutput{})
}

func (h *Handler) handleDescribeResource(_ context.Context, c *echo.Context, body []byte) error {
	var in describeResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	info, err := h.Backend.DescribeResource(in.ResourceArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeResourceOutput{ResourceInfo: toResourceInfoWire(info)})
}

func (h *Handler) handleListResources(_ context.Context, c *echo.Context, body []byte) error {
	var in listResourcesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	resources, nextToken := h.Backend.ListResources(in.MaxResults, in.NextToken)

	return c.JSON(http.StatusOK, listResourcesOutput{
		ResourceInfoList: toResourceInfoWireList(resources),
		NextToken:        nextToken,
	})
}

func (h *Handler) handleGrantPermissions(_ context.Context, c *echo.Context, body []byte) error {
	var in grantPermissionsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	// Validate PermissionsWithGrantOption is a subset of Permissions.
	if len(in.PermissionsWithGrantOption) > 0 {
		permSet := make(map[string]bool, len(in.Permissions))
		for _, p := range in.Permissions {
			permSet[p] = true
		}
		for _, g := range in.PermissionsWithGrantOption {
			if !permSet[g] {
				return h.writeError(c, http.StatusBadRequest, "InvalidInputException",
					"PermissionsWithGrantOption must be a subset of Permissions")
			}
		}
	}

	entry := &PermissionEntry{
		Principal:                  in.Principal,
		Resource:                   in.Resource,
		Permissions:                in.Permissions,
		PermissionsWithGrantOption: in.PermissionsWithGrantOption,
	}

	if err := h.Backend.GrantPermissions(entry); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, grantPermissionsOutput{})
}

func (h *Handler) handleRevokePermissions(_ context.Context, c *echo.Context, body []byte) error {
	var in revokePermissionsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	entry := &PermissionEntry{
		Principal:                  in.Principal,
		Resource:                   in.Resource,
		Permissions:                in.Permissions,
		PermissionsWithGrantOption: in.PermissionsWithGrantOption,
	}

	if err := h.Backend.RevokePermissions(entry); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, revokePermissionsOutput{})
}

func (h *Handler) handleListPermissions(_ context.Context, c *echo.Context, body []byte) error {
	var in listPermissionsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	entries, nextToken := h.Backend.ListPermissions(
		in.ResourceArn,
		in.MaxResults,
		in.NextToken,
		in.Principal,
		in.ResourceType,
	)

	return c.JSON(http.StatusOK, listPermissionsOutput{
		PrincipalResourcePermissions: entries,
		NextToken:                    nextToken,
	})
}

func (h *Handler) handleCreateLFTag(_ context.Context, c *echo.Context, body []byte) error {
	var in createLFTagInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if err := h.Backend.CreateLFTag(in.CatalogID, in.TagKey, in.TagValues); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, createLFTagOutput{})
}

func (h *Handler) handleDeleteLFTag(_ context.Context, c *echo.Context, body []byte) error {
	var in deleteLFTagInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if err := h.Backend.DeleteLFTag(in.CatalogID, in.TagKey); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteLFTagOutput{})
}

func (h *Handler) handleGetLFTag(_ context.Context, c *echo.Context, body []byte) error {
	var in getLFTagInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	tag, err := h.Backend.GetLFTag(in.CatalogID, in.TagKey)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, getLFTagOutput{
		CatalogID: tag.CatalogID,
		TagKey:    tag.TagKey,
		TagValues: tag.TagValues,
	})
}

func (h *Handler) handleUpdateLFTag(_ context.Context, c *echo.Context, body []byte) error {
	var in updateLFTagInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if err := h.Backend.UpdateLFTag(in.CatalogID, in.TagKey, in.TagValuesToAdd, in.TagValuesToDelete); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateLFTagOutput{})
}

func (h *Handler) handleListLFTags(_ context.Context, c *echo.Context, body []byte) error {
	var in listLFTagsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	tags, nextToken := h.Backend.ListLFTags(in.CatalogID, in.MaxResults, in.NextToken)

	return c.JSON(http.StatusOK, listLFTagsOutput{
		LFTags:    tags,
		NextToken: nextToken,
	})
}

func (h *Handler) handleBatchGrantPermissions(_ context.Context, c *echo.Context, body []byte) error {
	var in batchGrantPermissionsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	failures := h.Backend.BatchGrantPermissions(in.Entries)

	result := batchGrantPermissionsOutput{Failures: make([]BatchFailureEntry, 0, len(failures))}

	for _, f := range failures {
		if f != nil {
			result.Failures = append(result.Failures, *f)
		}
	}

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleBatchRevokePermissions(_ context.Context, c *echo.Context, body []byte) error {
	var in batchRevokePermissionsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	failures := h.Backend.BatchRevokePermissions(in.Entries)

	result := batchRevokePermissionsOutput{Failures: make([]BatchFailureEntry, 0, len(failures))}

	for _, f := range failures {
		if f != nil {
			result.Failures = append(result.Failures, *f)
		}
	}

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleAddLFTagsToResource(_ context.Context, c *echo.Context, body []byte) error {
	var in addLFTagsToResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if in.Resource == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Resource is required")
	}

	if len(in.LFTags) == 0 {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "LFTags is required")
	}

	failures := h.Backend.AddLFTagsToResource(in.CatalogID, in.Resource, in.LFTags)

	out := addLFTagsToResourceOutput{Failures: make([]LFTagError, 0, len(failures))}
	out.Failures = append(out.Failures, failures...)

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handleAssumeDecoratedRoleWithSAML(_ context.Context, c *echo.Context, body []byte) error {
	var in assumeDecoratedRoleWithSAMLInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if strings.TrimSpace(in.PrincipalArn) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "PrincipalArn is required")
	}

	if strings.TrimSpace(in.RoleArn) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "RoleArn is required")
	}

	if strings.TrimSpace(in.SAMLAssertion) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "SAMLAssertion is required")
	}

	out := h.Backend.AssumeDecoratedRoleWithSAML(in.PrincipalArn, in.RoleArn, in.SAMLAssertion, in.DurationSeconds)

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handleCancelTransaction(_ context.Context, c *echo.Context, body []byte) error {
	var in cancelTransactionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if strings.TrimSpace(in.TransactionID) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TransactionId is required")
	}

	if err := h.Backend.CancelTransaction(in.TransactionID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, cancelTransactionOutput{})
}

func (h *Handler) handleCommitTransaction(_ context.Context, c *echo.Context, body []byte) error {
	var in commitTransactionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if strings.TrimSpace(in.TransactionID) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TransactionId is required")
	}

	status, err := h.Backend.CommitTransaction(in.TransactionID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, commitTransactionOutput{TransactionStatus: status})
}

func (h *Handler) handleCreateDataCellsFilter(_ context.Context, c *echo.Context, body []byte) error {
	var in createDataCellsFilterInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if in.TableData == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TableData is required")
	}

	if strings.TrimSpace(in.TableData.Name) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TableData.Name is required")
	}

	if err := h.Backend.CreateDataCellsFilter(in.TableData); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, createDataCellsFilterOutput{})
}

func (h *Handler) handleCreateLFTagExpression(_ context.Context, c *echo.Context, body []byte) error {
	var in createLFTagExpressionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if strings.TrimSpace(in.Name) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Name is required")
	}

	if len(in.Expression) == 0 {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Expression is required")
	}

	if err := h.Backend.CreateLFTagExpression(in.Name, in.Description, in.CatalogID, in.Expression); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, createLFTagExpressionOutput{})
}

func (h *Handler) handleCreateLakeFormationIdentityCenterConfiguration(
	_ context.Context,
	c *echo.Context,
	body []byte,
) error {
	var in createLakeFormationIdentityCenterConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	catalogID := in.CatalogID
	if catalogID == "" {
		catalogID = h.AccountID
	}

	appArn, err := h.Backend.CreateLakeFormationIdentityCenterConfiguration(
		catalogID,
		in.InstanceArn,
		in.ExternalFiltering,
		in.ShareRecipients,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, createLakeFormationIdentityCenterConfigurationOutput{ApplicationArn: appArn})
}

func (h *Handler) handleCreateLakeFormationOptIn(_ context.Context, c *echo.Context, body []byte) error {
	var in createLakeFormationOptInInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if in.Principal == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Principal is required")
	}

	if in.Resource == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Resource is required")
	}

	if err := h.Backend.CreateLakeFormationOptIn(in.Principal, in.Resource); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, createLakeFormationOptInOutput{})
}

func (h *Handler) handleDeleteDataCellsFilter(_ context.Context, c *echo.Context, body []byte) error {
	var in deleteDataCellsFilterInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if err := h.Backend.DeleteDataCellsFilter(in.TableCatalogID, in.DatabaseName, in.TableName, in.Name); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteDataCellsFilterOutput{})
}

func (h *Handler) handleDeleteLFTagExpression(_ context.Context, c *echo.Context, body []byte) error {
	var in deleteLFTagExpressionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if strings.TrimSpace(in.Name) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Name is required")
	}

	if err := h.Backend.DeleteLFTagExpression(in.Name, in.CatalogID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteLFTagExpressionOutput{})
}

func (h *Handler) handleUpdateResource(_ context.Context, c *echo.Context, body []byte) error {
	var in updateResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if err := h.Backend.UpdateResource(in.ResourceArn, in.RoleArn); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateResourceOutput{})
}

func (h *Handler) handleStartTransaction(_ context.Context, c *echo.Context, body []byte) error {
	var in startTransactionInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	id := h.Backend.StartTransaction(in.TransactionType)

	return c.JSON(http.StatusOK, startTransactionOutput{TransactionID: id})
}

func (h *Handler) handleDescribeTransaction(_ context.Context, c *echo.Context, body []byte) error {
	var in describeTransactionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if strings.TrimSpace(in.TransactionID) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TransactionId is required")
	}

	tx, err := h.Backend.DescribeTransaction(in.TransactionID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeTransactionOutput{TransactionDescription: toTransactionWire(tx)})
}

func (h *Handler) handleListTransactions(_ context.Context, c *echo.Context, body []byte) error {
	var in listTransactionsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	txns, nextToken := h.Backend.ListTransactions(in.StatusFilter, in.MaxResults, in.NextToken)

	return c.JSON(http.StatusOK, listTransactionsOutput{
		Transactions: toTransactionWireList(txns),
		NextToken:    nextToken,
	})
}

func (h *Handler) handleRemoveLFTagsFromResource(_ context.Context, c *echo.Context, body []byte) error {
	var in removeLFTagsFromResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if in.Resource == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Resource is required")
	}

	if len(in.LFTags) == 0 {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "LFTags is required")
	}

	failures := h.Backend.RemoveLFTagsFromResource(in.CatalogID, in.Resource, in.LFTags)

	out := removeLFTagsFromResourceOutput{Failures: make([]LFTagError, 0, len(failures))}
	out.Failures = append(out.Failures, failures...)

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handleGetResourceLFTags(_ context.Context, c *echo.Context, body []byte) error {
	var in getResourceLFTagsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if in.Resource == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Resource is required")
	}

	pairs, err := h.Backend.GetResourceLFTags(in.CatalogID, in.Resource)
	if err != nil {
		return h.handleError(c, err)
	}

	// Return tags in the appropriate output field based on resource type.
	out := getResourceLFTagsOutput{}
	switch {
	case in.Resource.Database != nil:
		out.LFTagOnDatabase = pairs
	case in.Resource.Table != nil:
		out.LFTagsOnTable = pairs
	default:
		out.LFTagsOnTable = pairs
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handleListDataCellsFilter(_ context.Context, c *echo.Context, body []byte) error {
	var in listDataCellsFilterInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	if in.Table == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Table is required")
	}
	if in.Table.DatabaseName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Table.DatabaseName is required")
	}

	tableCatalogID := in.Table.CatalogID
	databaseName := in.Table.DatabaseName
	tableName := in.Table.Name

	filters, nextToken := h.Backend.ListDataCellsFilter(
		tableCatalogID,
		databaseName,
		tableName,
		in.MaxResults,
		in.NextToken,
	)

	return c.JSON(http.StatusOK, listDataCellsFilterOutput{
		DataCellsFilters: filters,
		NextToken:        nextToken,
	})
}

func (h *Handler) handleListLFTagExpressions(_ context.Context, c *echo.Context, body []byte) error {
	var in listLFTagExpressionsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	exprs, nextToken := h.Backend.ListLFTagExpressions(in.CatalogID, in.MaxResults, in.NextToken)

	return c.JSON(http.StatusOK, listLFTagExpressionsOutput{
		LFTagExpressions: exprs,
		NextToken:        nextToken,
	})
}

func (h *Handler) handleDeleteLakeFormationOptIn(_ context.Context, c *echo.Context, body []byte) error {
	var in deleteLakeFormationOptInInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if in.Principal == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Principal is required")
	}

	if in.Resource == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Resource is required")
	}

	if err := h.Backend.DeleteLakeFormationOptIn(in.Principal, in.Resource); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteLakeFormationOptInOutput{})
}

func (h *Handler) handleListLakeFormationOptIns(_ context.Context, c *echo.Context, body []byte) error {
	var in listLakeFormationOptInsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	principalIdentifier := ""
	if in.Principal != nil {
		principalIdentifier = in.Principal.DataLakePrincipalIdentifier
	}

	optIns, nextToken := h.Backend.ListLakeFormationOptIns(
		principalIdentifier,
		in.Resource,
		in.MaxResults,
		in.NextToken,
	)

	return c.JSON(http.StatusOK, listLakeFormationOptInsOutput{
		LakeFormationOptInsInfoList: toLFOptInWireList(optIns),
		NextToken:                   nextToken,
	})
}

func (h *Handler) handleGetDataLakePrincipal(ctx context.Context, c *echo.Context, _ []byte) error {
	principal := h.Backend.GetDataLakePrincipal(ctx)

	return c.JSON(http.StatusOK, getDataLakePrincipalOutput{Identity: principal.DataLakePrincipalIdentifier})
}

func (h *Handler) handleDeleteLakeFormationIdentityCenterConfiguration(
	_ context.Context, c *echo.Context, body []byte,
) error {
	var in deleteLakeFormationIdentityCenterConfigurationInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	catalogID := in.CatalogID
	if catalogID == "" {
		catalogID = h.AccountID
	}
	if err := h.Backend.DeleteLakeFormationIdentityCenterConfiguration(catalogID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteLakeFormationIdentityCenterConfigurationOutput{})
}

func (h *Handler) handleDeleteObjectsOnCancel(_ context.Context, c *echo.Context, body []byte) error {
	var in deleteObjectsOnCancelInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	if err := h.Backend.DeleteObjectsOnCancel(in.TransactionID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteObjectsOnCancelOutput{})
}

func (h *Handler) handleDescribeLakeFormationIdentityCenterConfiguration(
	_ context.Context, c *echo.Context, body []byte,
) error {
	var in describeLakeFormationIdentityCenterConfigurationInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	catalogID := in.CatalogID
	if catalogID == "" {
		catalogID = h.AccountID
	}
	cfg, err := h.Backend.DescribeLakeFormationIdentityCenterConfiguration(catalogID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeLakeFormationIdentityCenterConfigurationOutput{
		CatalogID:         cfg.CatalogID,
		InstanceArn:       cfg.InstanceArn,
		ApplicationArn:    cfg.ApplicationArn,
		ApplicationStatus: cfg.ApplicationStatus,
		ExternalFiltering: cfg.ExternalFiltering,
		ShareRecipients:   cfg.ShareRecipients,
	})
}

func (h *Handler) handleExtendTransaction(_ context.Context, c *echo.Context, body []byte) error {
	var in extendTransactionInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	if err := h.Backend.ExtendTransaction(in.TransactionID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, extendTransactionOutput{})
}

func (h *Handler) handleGetDataCellsFilter(_ context.Context, c *echo.Context, body []byte) error {
	var in getDataCellsFilterInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	f, err := h.Backend.GetDataCellsFilter(in.TableCatalogID, in.DatabaseName, in.TableName, in.Name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, getDataCellsFilterOutput{DataCellsFilter: f})
}

func (h *Handler) handleGetEffectivePermissionsForPath(_ context.Context, c *echo.Context, body []byte) error {
	var in getEffectivePermissionsForPathInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	entries, nextToken := h.Backend.GetEffectivePermissionsForPath(in.ResourceArn, in.MaxResults, in.NextToken)

	return c.JSON(http.StatusOK, getEffectivePermissionsForPathOutput{
		PrincipalResourcePermissions: entries,
		NextToken:                    nextToken,
	})
}

func (h *Handler) handleGetLFTagExpression(_ context.Context, c *echo.Context, body []byte) error {
	var in getLFTagExpressionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	expr, err := h.Backend.GetLFTagExpression(in.Name, in.CatalogID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, getLFTagExpressionOutput{
		Name:        expr.Name,
		Description: expr.Description,
		CatalogID:   expr.CatalogID,
		Expression:  expr.Expression,
	})
}

func (h *Handler) handleGetQueryState(_ context.Context, c *echo.Context, body []byte) error {
	var in getQueryStateInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	state, err := h.Backend.GetQueryState(in.QueryID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, getQueryStateOutput{State: state})
}

func (h *Handler) handleGetQueryStatistics(_ context.Context, c *echo.Context, body []byte) error {
	var in getQueryStatisticsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	exec, plan, err := h.Backend.GetQueryStatistics(in.QueryID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, getQueryStatisticsOutput{ExecutionStatistics: exec, PlanningStatistics: plan})
}

func (h *Handler) handleGetTableObjects(_ context.Context, c *echo.Context, body []byte) error {
	var in getTableObjectsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	objects, nextToken := h.Backend.GetTableObjects(
		in.CatalogID, in.DatabaseName, in.TableName, in.TransactionID,
		in.MaxResults, in.NextToken,
	)

	return c.JSON(http.StatusOK, getTableObjectsOutput{Objects: objects, NextToken: nextToken})
}

func (h *Handler) handleGetTemporaryDataLocationCredentials(_ context.Context, c *echo.Context, body []byte) error {
	var in getTemporaryDataLocationCredentialsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	if strings.TrimSpace(in.ResourceArn) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "ResourceArn is required")
	}
	creds := h.Backend.GetTemporaryCredentials(in.DurationSeconds)

	return c.JSON(http.StatusOK, getTemporaryDataLocationCredentialsOutput{Credentials: creds})
}

func (h *Handler) handleGetTemporaryGluePartitionCredentials(_ context.Context, c *echo.Context, body []byte) error {
	var in getTemporaryGluePartitionCredentialsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	if strings.TrimSpace(in.TableArn) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TableArn is required")
	}
	creds := h.Backend.GetTemporaryCredentials(in.DurationSeconds)

	return c.JSON(http.StatusOK, getTemporaryGluePartitionCredentialsOutput{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		Expiration:      creds.Expiration,
	})
}

func (h *Handler) handleGetTemporaryGlueTableCredentials(_ context.Context, c *echo.Context, body []byte) error {
	var in getTemporaryGlueTableCredentialsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	if strings.TrimSpace(in.TableArn) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TableArn is required")
	}
	creds := h.Backend.GetTemporaryCredentials(in.DurationSeconds)

	return c.JSON(http.StatusOK, getTemporaryGlueTableCredentialsOutput{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		Expiration:      creds.Expiration,
	})
}

func (h *Handler) handleGetWorkUnitResults(_ context.Context, c *echo.Context, body []byte) error {
	var in getWorkUnitResultsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	result, err := h.Backend.GetWorkUnitResults(in.QueryID, in.WorkUnitToken)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.Blob(http.StatusOK, "application/json", []byte(result))
}

func (h *Handler) handleGetWorkUnits(_ context.Context, c *echo.Context, body []byte) error {
	var in getWorkUnitsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	ranges, nextToken, err := h.Backend.GetWorkUnits(in.QueryID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, getWorkUnitsOutput{QueryID: in.QueryID, WorkUnitRanges: ranges, NextToken: nextToken})
}

func (h *Handler) handleListTableStorageOptimizers(_ context.Context, c *echo.Context, body []byte) error {
	var in listTableStorageOptimizersInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	opts := h.Backend.ListTableStorageOptimizers(in.CatalogID, in.DatabaseName, in.TableName, in.StorageOptimizerType)

	return c.JSON(http.StatusOK, listTableStorageOptimizersOutput{StorageOptimizerList: opts})
}

func (h *Handler) handleSearchDatabasesByLFTags(_ context.Context, c *echo.Context, body []byte) error {
	var in searchDatabasesByLFTagsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	maxResults := 0
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}
	dbs, nextToken := h.Backend.SearchDatabasesByLFTags(in.Expression, in.CatalogID, maxResults, in.NextToken)

	return c.JSON(http.StatusOK, searchDatabasesByLFTagsOutput{DatabaseList: dbs, NextToken: nextToken})
}

func (h *Handler) handleSearchTablesByLFTags(_ context.Context, c *echo.Context, body []byte) error {
	var in searchTablesByLFTagsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	maxResults := 0
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}
	tables, nextToken := h.Backend.SearchTablesByLFTags(in.Expression, in.CatalogID, maxResults, in.NextToken)

	return c.JSON(http.StatusOK, searchTablesByLFTagsOutput{TableList: tables, NextToken: nextToken})
}

func (h *Handler) handleStartQueryPlanning(_ context.Context, c *echo.Context, body []byte) error {
	var in startQueryPlanningInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	if strings.TrimSpace(in.QueryString) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "QueryString is required")
	}
	if strings.TrimSpace(in.QueryPlanningContext.DatabaseName) == "" {
		return h.writeError(
			c, http.StatusBadRequest, "InvalidInputException", "QueryPlanningContext.DatabaseName is required",
		)
	}
	queryID := h.Backend.StartQueryPlanning(in.QueryString)

	return c.JSON(http.StatusOK, startQueryPlanningOutput{QueryID: queryID})
}

func (h *Handler) handleUpdateDataCellsFilter(_ context.Context, c *echo.Context, body []byte) error {
	var in updateDataCellsFilterInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	if in.TableData == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TableData is required")
	}
	if err := h.Backend.UpdateDataCellsFilter(in.TableData); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateDataCellsFilterOutput{})
}

func (h *Handler) handleUpdateLFTagExpression(_ context.Context, c *echo.Context, body []byte) error {
	var in updateLFTagExpressionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	if strings.TrimSpace(in.Name) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Name is required")
	}
	if err := h.Backend.UpdateLFTagExpression(in.Name, in.CatalogID, in.Description, in.Expression); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateLFTagExpressionOutput{})
}

func (h *Handler) handleUpdateLakeFormationIdentityCenterConfiguration(
	_ context.Context, c *echo.Context, body []byte,
) error {
	var in updateLakeFormationIdentityCenterConfigurationInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	catalogID := in.CatalogID
	if catalogID == "" {
		catalogID = h.AccountID
	}
	if err := h.Backend.UpdateLakeFormationIdentityCenterConfiguration(
		catalogID, in.ExternalFiltering, in.ApplicationStatus,
	); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateLakeFormationIdentityCenterConfigurationOutput{})
}

func (h *Handler) handleUpdateTableObjects(_ context.Context, c *echo.Context, body []byte) error {
	var in updateTableObjectsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	if err := h.Backend.UpdateTableObjects(
		in.CatalogID, in.DatabaseName, in.TableName, in.TransactionID, in.WriteOperations,
	); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateTableObjectsOutput{})
}

func (h *Handler) handleUpdateTableStorageOptimizer(_ context.Context, c *echo.Context, body []byte) error {
	var in updateTableStorageOptimizerInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	result := h.Backend.UpdateTableStorageOptimizer(
		in.CatalogID, in.DatabaseName, in.TableName, in.StorageOptimizerConfig,
	)

	return c.JSON(http.StatusOK, updateTableStorageOptimizerOutput{Result: result})
}
