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
		"/BatchRevokePermissions":
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
		"GetDataLakeSettings",
		"PutDataLakeSettings",
		"RegisterResource",
		"DeregisterResource",
		"DescribeResource",
		"ListResources",
		"GrantPermissions",
		"RevokePermissions",
		"ListPermissions",
		"CreateLFTag",
		"DeleteLFTag",
		"GetLFTag",
		"UpdateLFTag",
		"ListLFTags",
		"BatchGrantPermissions",
		"BatchRevokePermissions",
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
