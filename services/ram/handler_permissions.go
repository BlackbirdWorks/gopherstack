package ram

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/labstack/echo/v5"
)

// permissionStatusAttachable is the steady-state PermissionStatus for every
// permission gopherstack models -- there is no async DELETING/UNATTACHABLE
// transition, so non-deleted permissions are always ATTACHABLE.
const permissionStatusAttachable = "ATTACHABLE"

// permissionSummaryObject is the JSON representation of a RAM permission summary.
// Matches types.ResourceSharePermissionSummary (aws-sdk-go-v2/service/ram@v1.39.4
// types/types.go:492-, checked 2026-08-13): no ResourceRegionScope member -- that
// field exists only on types.Resource and types.ServiceNameAndResourceType.
type permissionSummaryObject struct {
	Arn                   string      `json:"arn"`
	Name                  string      `json:"name"`
	ResourceType          string      `json:"resourceType"`
	PermissionType        string      `json:"permissionType"`
	FeatureSet            string      `json:"featureSet"`
	Version               string      `json:"version"`
	Status                string      `json:"status,omitempty"`
	Tags                  []tagObject `json:"tags,omitempty"`
	CreationTime          float64     `json:"creationTime"`
	LastUpdatedTime       float64     `json:"lastUpdatedTime"`
	DefaultVersion        bool        `json:"defaultVersion"`
	IsResourceTypeDefault bool        `json:"isResourceTypeDefault"`
}

// permissionDetailObject is the JSON representation of a RAM permission detail (GetPermission).
// Matches types.ResourceSharePermissionDetail (same SDK version, types/types.go:403-):
// no ResourceRegionScope member either.
type permissionDetailObject struct {
	Arn                   string      `json:"arn"`
	Name                  string      `json:"name"`
	ResourceType          string      `json:"resourceType"`
	PermissionType        string      `json:"permissionType"`
	FeatureSet            string      `json:"featureSet"`
	Version               string      `json:"version"`
	Status                string      `json:"status,omitempty"`
	Permission            string      `json:"permission,omitempty"`
	Tags                  []tagObject `json:"tags,omitempty"`
	CreationTime          float64     `json:"creationTime"`
	LastUpdatedTime       float64     `json:"lastUpdatedTime"`
	DefaultVersion        bool        `json:"defaultVersion"`
	IsResourceTypeDefault bool        `json:"isResourceTypeDefault"`
}

func toPermissionSummaryObject(p *Permission) permissionSummaryObject {
	permType := p.PermissionType
	if permType == "" {
		permType = permissionTypeCustomer
	}

	obj := permissionSummaryObject{
		Arn:                   p.ARN,
		Name:                  p.Name,
		ResourceType:          p.ResourceType,
		PermissionType:        permType,
		FeatureSet:            permStandard,
		Status:                permissionStatusAttachable,
		CreationTime:          epochSeconds(p.CreationTime),
		LastUpdatedTime:       epochSeconds(p.LastUpdatedTime),
		Version:               strconv.Itoa(int(p.DefaultVersion)),
		DefaultVersion:        true,
		IsResourceTypeDefault: p.IsResourceTypeDefault,
	}

	if len(p.Tags) > 0 {
		obj.Tags = toTagObjects(p.Tags)
	}

	return obj
}

// toResourceSharePermissionSummaryObject builds a permission summary reflecting
// the version actually associated with a specific resource share (which may not
// be the permission's current default version), per AWS's ResourceSharePermissionSummary.
func toResourceSharePermissionSummaryObject(d *ResourceSharePermissionDetail) permissionSummaryObject {
	obj := toPermissionSummaryObject(d.Permission)
	obj.Version = strconv.Itoa(int(d.Version))
	obj.DefaultVersion = d.Version == d.Permission.DefaultVersion

	return obj
}

func toPermissionDetailObject(p *Permission, pv *PermissionVersion) permissionDetailObject {
	permType := p.PermissionType
	if permType == "" {
		permType = permissionTypeCustomer
	}

	obj := permissionDetailObject{
		Arn:                   p.ARN,
		Name:                  p.Name,
		ResourceType:          p.ResourceType,
		PermissionType:        permType,
		FeatureSet:            permStandard,
		Status:                permissionStatusAttachable,
		CreationTime:          epochSeconds(p.CreationTime),
		LastUpdatedTime:       epochSeconds(p.LastUpdatedTime),
		Version:               strconv.Itoa(int(pv.Version)),
		DefaultVersion:        pv.Version == p.DefaultVersion,
		IsResourceTypeDefault: p.IsResourceTypeDefault,
		Permission:            pv.PolicyTemplate,
	}

	if len(p.Tags) > 0 {
		obj.Tags = toTagObjects(p.Tags)
	}

	return obj
}

type createPermissionRequest struct {
	Name           string      `json:"name"`
	ResourceType   string      `json:"resourceType"`
	PolicyTemplate string      `json:"policyTemplate"`
	Tags           []tagObject `json:"tags"`
}

type createPermissionResponse struct {
	Permission permissionSummaryObject `json:"permission"`
}

func (h *Handler) handleCreatePermission(_ context.Context, body []byte) ([]byte, error) {
	var req createPermissionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	if req.ResourceType == "" {
		return nil, fmt.Errorf("%w: resourceType is required", errInvalidRequest)
	}

	if req.PolicyTemplate == "" {
		return nil, fmt.Errorf("%w: policyTemplate is required", errInvalidRequest)
	}

	p, err := h.Backend.CreatePermission(
		req.Name,
		req.ResourceType,
		req.PolicyTemplate,
		fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(createPermissionResponse{Permission: toPermissionSummaryObject(p)})
}

type deletePermissionResponse struct {
	PermissionStatus string `json:"permissionStatus,omitempty"`
	ReturnValue      bool   `json:"returnValue"`
}

func (h *Handler) handleDeletePermission(_ context.Context, c *echo.Context) ([]byte, error) {
	permissionARN := c.Request().URL.Query().Get("permissionArn")
	if permissionARN == "" {
		return nil, fmt.Errorf("%w: permissionArn query parameter is required", errInvalidRequest)
	}

	if err := h.Backend.DeletePermission(permissionARN); err != nil {
		return nil, err
	}

	return json.Marshal(deletePermissionResponse{ReturnValue: true, PermissionStatus: "DELETING"})
}

type getPermissionRequest struct {
	PermissionVersion *int32 `json:"permissionVersion,omitempty"`
	PermissionArn     string `json:"permissionArn"`
}

type getPermissionResponse struct {
	Permission permissionDetailObject `json:"permission"`
}

func (h *Handler) handleGetPermission(_ context.Context, body []byte) ([]byte, error) {
	var req getPermissionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PermissionArn == "" {
		return nil, fmt.Errorf("%w: permissionArn is required", errInvalidRequest)
	}

	p, pv, err := h.Backend.GetPermission(req.PermissionArn, req.PermissionVersion)
	if err != nil {
		return nil, err
	}

	return json.Marshal(getPermissionResponse{Permission: toPermissionDetailObject(p, pv)})
}

type listPermissionsRequest struct {
	MaxResults     *int32 `json:"maxResults,omitempty"`
	PermissionType string `json:"permissionType"`
	ResourceType   string `json:"resourceType"`
	NextToken      string `json:"nextToken"`
}

type listPermissionsResponse struct {
	NextToken   string                    `json:"nextToken,omitempty"`
	Permissions []permissionSummaryObject `json:"permissions"`
}

func (h *Handler) handleListPermissions(_ context.Context, body []byte) ([]byte, error) {
	var req listPermissionsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	perms := h.Backend.ListPermissions(req.ResourceType)
	objs := make([]permissionSummaryObject, 0, len(perms))

	for _, p := range perms {
		if req.PermissionType != "" && req.PermissionType != permissionTypeFilterAll &&
			p.PermissionType != req.PermissionType {
			continue
		}

		objs = append(objs, toPermissionSummaryObject(p))
	}

	page, nextToken, err := ramPaginate(objs, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	return json.Marshal(listPermissionsResponse{NextToken: nextToken, Permissions: page})
}

type promotePermissionCreatedFromPolicyRequest struct {
	PermissionArn string `json:"permissionArn"`
	Name          string `json:"name"`
}

type promotePermissionCreatedFromPolicyResponse struct {
	Permission permissionSummaryObject `json:"permission"`
}

func (h *Handler) handlePromotePermissionCreatedFromPolicy(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req promotePermissionCreatedFromPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PermissionArn == "" {
		return nil, fmt.Errorf("%w: permissionArn is required", errInvalidRequest)
	}

	p, err := h.Backend.PromotePermissionCreatedFromPolicy(req.PermissionArn, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(
		promotePermissionCreatedFromPolicyResponse{Permission: toPermissionSummaryObject(p)},
	)
}
