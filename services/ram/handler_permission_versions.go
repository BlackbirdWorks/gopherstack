package ram

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/labstack/echo/v5"
)

type createPermissionVersionRequest struct {
	PermissionArn  string `json:"permissionArn"`
	PolicyTemplate string `json:"policyTemplate"`
}

// createPermissionVersionResponse.Permission is a ResourceSharePermissionDetail, not a
// Summary: CreatePermissionVersionOutput.Permission is typed *types.ResourceSharePermissionDetail
// (api_op_CreatePermissionVersion.go:100), and its deserializer routes "permission" through
// awsRestjson1_deserializeDocumentResourceSharePermissionDetail (deserializers.go:916+),
// which carries the policy-document "permission" text field the Summary type lacks.
type createPermissionVersionResponse struct {
	Permission permissionDetailObject `json:"permission"`
}

func (h *Handler) handleCreatePermissionVersion(_ context.Context, body []byte) ([]byte, error) {
	var req createPermissionVersionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PermissionArn == "" {
		return nil, fmt.Errorf("%w: permissionArn is required", errInvalidRequest)
	}

	if req.PolicyTemplate == "" {
		return nil, fmt.Errorf("%w: policyTemplate is required", errInvalidRequest)
	}

	p, err := h.Backend.CreatePermissionVersion(req.PermissionArn, req.PolicyTemplate)
	if err != nil {
		return nil, err
	}

	pv := p.Versions[p.LatestVersion]

	return json.Marshal(
		createPermissionVersionResponse{Permission: toPermissionDetailObject(p, pv)},
	)
}

type deletePermissionVersionResponse struct {
	PermissionStatus string `json:"permissionStatus,omitempty"`
	ReturnValue      bool   `json:"returnValue"`
}

func (h *Handler) handleDeletePermissionVersion(
	_ context.Context,
	c *echo.Context,
) ([]byte, error) {
	permissionARN := c.Request().URL.Query().Get("permissionArn")
	if permissionARN == "" {
		return nil, fmt.Errorf("%w: permissionArn query parameter is required", errInvalidRequest)
	}

	versionStr := c.Request().URL.Query().Get("permissionVersion")
	if versionStr == "" {
		return nil, fmt.Errorf(
			"%w: permissionVersion query parameter is required",
			errInvalidRequest,
		)
	}

	v64, parseErr := strconv.ParseInt(versionStr, 10, 32)
	if parseErr != nil || v64 <= 0 {
		return nil, fmt.Errorf(
			"%w: permissionVersion must be a positive integer",
			errInvalidRequest,
		)
	}

	version := int32(v64)

	if delErr := h.Backend.DeletePermissionVersion(permissionARN, version); delErr != nil {
		return nil, delErr
	}

	return json.Marshal(
		deletePermissionVersionResponse{ReturnValue: true, PermissionStatus: "DELETING"},
	)
}

type listPermissionVersionsRequest struct {
	MaxResults    *int32 `json:"maxResults,omitempty"`
	PermissionArn string `json:"permissionArn"`
	NextToken     string `json:"nextToken"`
}

// listPermissionVersionsResponse.Permissions is a ResourceSharePermissionSummary list, not
// Detail: ListPermissionVersionsOutput.Permissions is typed []types.ResourceSharePermissionSummary
// (api_op_ListPermissionVersions.go:75), and its deserializer routes each element through
// awsRestjson1_deserializeDocumentResourceSharePermissionSummary (via
// deserializeDocumentResourceSharePermissionList, deserializers.go:3821+), which has no
// "permission" policy-document field -- unlike GetPermission/CreatePermissionVersion.
type listPermissionVersionsResponse struct {
	NextToken   string                    `json:"nextToken,omitempty"`
	Permissions []permissionSummaryObject `json:"permissions"`
}

// toPermissionVersionSummaryObject builds a permission summary for one specific version,
// as returned by ListPermissionVersions.
func toPermissionVersionSummaryObject(p *Permission, pv *PermissionVersion) permissionSummaryObject {
	obj := toPermissionSummaryObject(p)
	obj.Version = strconv.Itoa(int(pv.Version))
	obj.DefaultVersion = pv.Version == p.DefaultVersion

	return obj
}

func (h *Handler) handleListPermissionVersions(_ context.Context, body []byte) ([]byte, error) {
	var req listPermissionVersionsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PermissionArn == "" {
		return nil, fmt.Errorf("%w: permissionArn is required", errInvalidRequest)
	}

	versions, err := h.Backend.ListPermissionVersions(req.PermissionArn)
	if err != nil {
		return nil, err
	}

	p, _, pErr := h.Backend.GetPermission(req.PermissionArn, nil)
	if pErr != nil {
		return nil, pErr
	}

	objs := make([]permissionSummaryObject, 0, len(versions))

	for _, pv := range versions {
		objs = append(objs, toPermissionVersionSummaryObject(p, pv))
	}

	paged, nextToken, pErr2 := ramPaginate(objs, req.NextToken, req.MaxResults)
	if pErr2 != nil {
		return nil, pErr2
	}

	return json.Marshal(listPermissionVersionsResponse{NextToken: nextToken, Permissions: paged})
}

type setDefaultPermissionVersionRequest struct {
	PermissionArn     string `json:"permissionArn"`
	PermissionVersion int32  `json:"permissionVersion"`
}

type setDefaultPermissionVersionResponse struct {
	ReturnValue bool `json:"returnValue"`
}

func (h *Handler) handleSetDefaultPermissionVersion(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req setDefaultPermissionVersionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PermissionArn == "" {
		return nil, fmt.Errorf("%w: permissionArn is required", errInvalidRequest)
	}

	if req.PermissionVersion == 0 {
		return nil, fmt.Errorf("%w: permissionVersion is required", errInvalidRequest)
	}

	if _, err := h.Backend.SetDefaultPermissionVersion(req.PermissionArn, req.PermissionVersion); err != nil {
		return nil, err
	}

	return json.Marshal(setDefaultPermissionVersionResponse{ReturnValue: true})
}
