package ram

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/labstack/echo/v5"
)

// resourceShareObject is the JSON representation of a ResourceShare.
type resourceShareObject struct {
	Name                    string      `json:"name"`
	ResourceShareArn        string      `json:"resourceShareArn"`
	OwningAccountID         string      `json:"owningAccountId"`
	Status                  string      `json:"status"`
	StatusMessage           string      `json:"statusMessage,omitempty"`
	FeatureSet              string      `json:"featureSet"`
	Tags                    []tagObject `json:"tags,omitempty"`
	CreationTime            float64     `json:"creationTime"`
	LastUpdatedTime         float64     `json:"lastUpdatedTime"`
	AllowExternalPrincipals bool        `json:"allowExternalPrincipals"`
}

func toResourceShareObject(rs *ResourceShare) resourceShareObject {
	obj := resourceShareObject{
		Name:                    rs.Name,
		ResourceShareArn:        rs.ARN,
		OwningAccountID:         rs.OwningAccountID,
		Status:                  rs.Status,
		StatusMessage:           rs.StatusMessage,
		FeatureSet:              permStandard,
		AllowExternalPrincipals: rs.AllowExternalPrincipals,
		CreationTime:            epochSeconds(rs.CreationTime),
		LastUpdatedTime:         epochSeconds(rs.LastUpdatedTime),
	}

	if len(rs.Tags) > 0 {
		obj.Tags = toTagObjects(rs.Tags)
	}

	return obj
}

type createResourceShareRequest struct {
	Name                    string      `json:"name"`
	Tags                    []tagObject `json:"tags"`
	Principals              []string    `json:"principals"`
	ResourceArns            []string    `json:"resourceArns"`
	PermissionArns          []string    `json:"permissionArns"`
	AllowExternalPrincipals bool        `json:"allowExternalPrincipals"`
}

type createResourceShareResponse struct {
	ResourceShare resourceShareObject `json:"resourceShare"`
}

func (h *Handler) handleCreateResourceShare(_ context.Context, body []byte) ([]byte, error) {
	var req createResourceShareRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	if len(req.Name) > maxShareNameLen || !ramShareNameRegex.MatchString(req.Name) {
		return nil, fmt.Errorf(
			"%w: name must be 1-256 characters matching ^[\\w\\-.]+$",
			ErrValidation,
		)
	}

	rs, err := h.Backend.CreateResourceShare(
		req.Name,
		req.AllowExternalPrincipals,
		fromTagObjects(req.Tags),
		req.Principals,
		req.ResourceArns,
	)
	if err != nil {
		return nil, err
	}

	if len(req.PermissionArns) > 0 {
		// Associate exactly the explicitly requested permissions.
		for _, permARN := range req.PermissionArns {
			if assocErr := h.Backend.AssociateResourceSharePermission(rs.ARN, permARN, false, nil); assocErr != nil {
				return nil, assocErr
			}
		}
	} else if len(req.ResourceArns) > 0 {
		// AWS: "If you don't specify [permissionArns], the resource share is
		// automatically associated with the default RAM-managed permission for
		// each resource type included in the resource share."
		if autoErr := h.Backend.AutoAssociateDefaultPermissions(rs.ARN); autoErr != nil {
			return nil, autoErr
		}
	}

	return json.Marshal(createResourceShareResponse{
		ResourceShare: toResourceShareObject(rs),
	})
}

type tagFilterRequest struct {
	TagKey    string   `json:"tagKey"`
	TagValues []string `json:"tagValues"`
}

type getResourceSharesRequest struct {
	MaxResults          *int32             `json:"maxResults,omitempty"`
	PermissionVersion   *int32             `json:"permissionVersion,omitempty"`
	ResourceOwner       string             `json:"resourceOwner"`
	Name                string             `json:"name"`
	NextToken           string             `json:"nextToken"`
	ResourceShareStatus string             `json:"resourceShareStatus"`
	PermissionArn       string             `json:"permissionArn"`
	ResourceShareArns   []string           `json:"resourceShareArns"`
	TagFilters          []tagFilterRequest `json:"tagFilters"`
}

type getResourceSharesResponse struct {
	NextToken      string                `json:"nextToken,omitempty"`
	ResourceShares []resourceShareObject `json:"resourceShares"`
}

func (h *Handler) handleGetResourceShares(_ context.Context, body []byte) ([]byte, error) {
	var req getResourceSharesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceOwner == "" {
		return nil, fmt.Errorf("%w: resourceOwner is required", errInvalidRequest)
	}

	var shares []resourceShareObject

	// If specific ARNs requested, look them up individually but still apply
	// the name/status/permission/tag filters -- AWS combines resourceShareArns
	// with the other filters rather than treating it as an exclusive lookup mode.
	if len(req.ResourceShareArns) > 0 {
		shares = h.getResourceSharesByARN(req)
	} else {
		shares = h.getResourceSharesByFilter(req)
	}

	page, nextToken, err := ramPaginate(shares, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	return json.Marshal(getResourceSharesResponse{NextToken: nextToken, ResourceShares: page})
}

// getResourceSharesByARN looks up each requested ARN individually, applying
// the name/status/permission/tag filters to the results.
func (h *Handler) getResourceSharesByARN(req getResourceSharesRequest) []resourceShareObject {
	shares := make([]resourceShareObject, 0, len(req.ResourceShareArns))

	for _, shareARN := range req.ResourceShareArns {
		rs, err := h.Backend.GetResourceShare(shareARN)
		if err != nil {
			continue
		}

		if !h.resourceShareMatchesFilters(rs, req) {
			continue
		}

		shares = append(shares, toResourceShareObject(rs))
	}

	return shares
}

// getResourceSharesByFilter lists shares by owner/status, applying the remaining filters.
func (h *Handler) getResourceSharesByFilter(req getResourceSharesRequest) []resourceShareObject {
	list := h.Backend.ListResourceShares(req.ResourceOwner, req.ResourceShareStatus)
	shares := make([]resourceShareObject, 0, len(list))

	for _, rs := range list {
		if req.Name != "" && rs.Name != req.Name {
			continue
		}

		if !h.matchesPermissionAndTagFilters(rs, req) {
			continue
		}

		shares = append(shares, toResourceShareObject(rs))
	}

	return shares
}

// resourceShareMatchesFilters applies the full name/status/permission/tag filter set to
// rs. Used by the resourceShareArns lookup path, which (unlike getResourceSharesByFilter)
// does not already apply ListResourceShares' owner/status filtering.
func (h *Handler) resourceShareMatchesFilters(rs *ResourceShare, req getResourceSharesRequest) bool {
	if req.Name != "" && rs.Name != req.Name {
		return false
	}

	if req.ResourceShareStatus != "" && rs.Status != req.ResourceShareStatus {
		return false
	}

	return h.matchesPermissionAndTagFilters(rs, req)
}

// matchesPermissionAndTagFilters applies just the permissionArn/permissionVersion and
// tagFilters filters, shared by both GetResourceShares lookup paths.
func (h *Handler) matchesPermissionAndTagFilters(rs *ResourceShare, req getResourceSharesRequest) bool {
	if req.PermissionArn != "" && !h.shareUsesPermission(rs.ARN, req.PermissionArn, req.PermissionVersion) {
		return false
	}

	if len(req.TagFilters) > 0 && !tagsMatchFilters(rs.Tags, req.TagFilters) {
		return false
	}

	return true
}

// shareUsesPermission reports whether shareARN has permARN associated, optionally
// pinned to a specific version.
func (h *Handler) shareUsesPermission(shareARN, permARN string, version *int32) bool {
	for _, d := range h.Backend.ListResourceSharePermissions(shareARN) {
		if d.Permission.ARN != permARN {
			continue
		}

		if version != nil && d.Version != *version {
			continue
		}

		return true
	}

	return false
}

// tagsMatchFilters reports whether tags satisfies every filter in filters (AND across
// filters). A filter matches when its key is present and, if TagValues is non-empty,
// the tag's value is one of them.
func tagsMatchFilters(tags map[string]string, filters []tagFilterRequest) bool {
	for _, f := range filters {
		v, ok := tags[f.TagKey]
		if !ok {
			return false
		}

		if len(f.TagValues) > 0 && !slices.Contains(f.TagValues, v) {
			return false
		}
	}

	return true
}

type updateResourceShareRequest struct {
	AllowExternalPrincipals *bool  `json:"allowExternalPrincipals"`
	ResourceShareArn        string `json:"resourceShareArn"`
	Name                    string `json:"name"`
}

type updateResourceShareResponse struct {
	ResourceShare resourceShareObject `json:"resourceShare"`
}

func (h *Handler) handleUpdateResourceShare(_ context.Context, body []byte) ([]byte, error) {
	var req updateResourceShareRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareArn == "" {
		return nil, fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	rs, err := h.Backend.UpdateResourceShare(
		req.ResourceShareArn,
		req.Name,
		req.AllowExternalPrincipals,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(updateResourceShareResponse{
		ResourceShare: toResourceShareObject(rs),
	})
}

type deleteResourceShareResponse struct {
	ReturnValue bool `json:"returnValue"`
}

func (h *Handler) handleDeleteResourceShare(_ context.Context, c *echo.Context) ([]byte, error) {
	shareARN := c.Request().URL.Query().Get("resourceShareArn")
	if shareARN == "" {
		return nil, fmt.Errorf(
			"%w: resourceShareArn query parameter is required",
			errInvalidRequest,
		)
	}

	if err := h.Backend.DeleteResourceShare(shareARN); err != nil {
		return nil, err
	}

	return json.Marshal(deleteResourceShareResponse{ReturnValue: true})
}

type enableSharingWithAwsOrganizationResponse struct {
	ReturnValue bool `json:"returnValue"`
}

func (h *Handler) handleEnableSharingWithAwsOrganization() ([]byte, error) {
	return json.Marshal(enableSharingWithAwsOrganizationResponse{ReturnValue: true})
}

type promoteResourceShareCreatedFromPolicyRequest struct {
	ResourceShareArn string `json:"resourceShareArn"`
}

type promoteResourceShareCreatedFromPolicyResponse struct {
	ReturnValue bool `json:"returnValue"`
}

func (h *Handler) handlePromoteResourceShareCreatedFromPolicy(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req promoteResourceShareCreatedFromPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareArn == "" {
		return nil, fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	if _, err := h.Backend.PromoteResourceShareCreatedFromPolicy(req.ResourceShareArn); err != nil {
		return nil, err
	}

	return json.Marshal(promoteResourceShareCreatedFromPolicyResponse{ReturnValue: true})
}
