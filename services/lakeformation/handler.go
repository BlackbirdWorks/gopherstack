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
		"/AssumeDecoratedRoleWithSAML",
		"/CancelTransaction",
		"/CommitTransaction",
		"/CreateDataCellsFilter",
		"/CreateLFTagExpression",
		"/CreateLakeFormationIdentityCenterConfiguration",
		"/CreateLakeFormationOptIn",
		"/DeleteDataCellsFilter",
		"/DeleteLFTagExpression":
		return true
	}

	return false
}

// Handler is the HTTP handler for the Lake Formation REST API.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new Lake Formation handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
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
		"DeregisterResource",
		"DescribeResource",
		"GetDataLakeSettings",
		"GetLFTag",
		"GrantPermissions",
		"ListLFTags",
		"ListPermissions",
		"ListResources",
		"PutDataLakeSettings",
		"RegisterResource",
		"RevokePermissions",
		"UpdateLFTag",
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

func (h *Handler) dispatch(ctx context.Context, c *echo.Context, op string, body []byte) error {
	type dispatchFn func(context.Context, *echo.Context, []byte) error

	table := map[string]dispatchFn{
		"GetDataLakeSettings":    h.handleGetDataLakeSettings,
		"PutDataLakeSettings":    h.handlePutDataLakeSettings,
		"RegisterResource":       h.handleRegisterResource,
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

		"AddLFTagsToResource":                            h.handleAddLFTagsToResource,
		"AssumeDecoratedRoleWithSAML":                    h.handleAssumeDecoratedRoleWithSAML,
		"CancelTransaction":                              h.handleCancelTransaction,
		"CommitTransaction":                              h.handleCommitTransaction,
		"CreateDataCellsFilter":                          h.handleCreateDataCellsFilter,
		"CreateLFTagExpression":                          h.handleCreateLFTagExpression,
		"CreateLakeFormationIdentityCenterConfiguration": h.handleCreateLakeFormationIdentityCenterConfiguration,
		"CreateLakeFormationOptIn":                       h.handleCreateLakeFormationOptIn,
		"DeleteDataCellsFilter":                          h.handleDeleteDataCellsFilter,
		"DeleteLFTagExpression":                          h.handleDeleteLFTagExpression,
	}

	fn, ok := table[op]
	if !ok {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "unknown operation: "+op)
	}

	return fn(ctx, c, body)
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return h.writeError(c, http.StatusNotFound, "EntityNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return h.writeError(c, http.StatusConflict, "AlreadyExistsException", err.Error())
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

	if err := h.Backend.RegisterResource(in.ResourceArn, in.RoleArn); err != nil {
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

	return c.JSON(http.StatusOK, describeResourceOutput{ResourceInfo: info})
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
		ResourceInfoList: resources,
		NextToken:        nextToken,
	})
}

func (h *Handler) handleGrantPermissions(_ context.Context, c *echo.Context, body []byte) error {
	var in grantPermissionsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
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

	entries, nextToken := h.Backend.ListPermissions(in.ResourceArn, in.MaxResults, in.NextToken)

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

	appArn := h.Backend.CreateLakeFormationIdentityCenterConfiguration(catalogID, in.InstanceArn)

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
