package ram

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
)

type listResourceSharePermissionsRequest struct {
	MaxResults       *int32 `json:"maxResults,omitempty"`
	ResourceShareArn string `json:"resourceShareArn"`
	NextToken        string `json:"nextToken"`
}

type listResourceSharePermissionsResponse struct {
	NextToken   string                    `json:"nextToken,omitempty"`
	Permissions []permissionSummaryObject `json:"permissions"`
}

// handleListResourceSharePermissions returns the managed permissions associated with a resource share.
func (h *Handler) handleListResourceSharePermissions(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req listResourceSharePermissionsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareArn == "" {
		return nil, fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	perms := h.Backend.ListResourceSharePermissions(req.ResourceShareArn)
	objs := make([]permissionSummaryObject, 0, len(perms))

	for _, d := range perms {
		objs = append(objs, toResourceSharePermissionSummaryObject(d))
	}

	page, nextToken, err := ramPaginate(objs, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	return json.Marshal(
		listResourceSharePermissionsResponse{NextToken: nextToken, Permissions: page},
	)
}

type associateResourceSharePermissionRequest struct {
	PermissionVersion *int32 `json:"permissionVersion,omitempty"`
	ResourceShareArn  string `json:"resourceShareArn"`
	PermissionArn     string `json:"permissionArn"`
	Replace           bool   `json:"replace"`
}

type associateResourceSharePermissionResponse struct {
	ReturnValue bool `json:"returnValue"`
}

func (h *Handler) handleAssociateResourceSharePermission(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req associateResourceSharePermissionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareArn == "" {
		return nil, fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	if req.PermissionArn == "" {
		return nil, fmt.Errorf("%w: permissionArn is required", errInvalidRequest)
	}

	if err := h.Backend.AssociateResourceSharePermission(
		req.ResourceShareArn, req.PermissionArn, req.Replace, req.PermissionVersion,
	); err != nil {
		return nil, err
	}

	return json.Marshal(associateResourceSharePermissionResponse{ReturnValue: true})
}

type disassociateResourceSharePermissionRequest struct {
	ResourceShareArn string `json:"resourceShareArn"`
	PermissionArn    string `json:"permissionArn"`
}

type disassociateResourceSharePermissionResponse struct {
	ReturnValue bool `json:"returnValue"`
}

func (h *Handler) handleDisassociateResourceSharePermission(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req disassociateResourceSharePermissionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareArn == "" {
		return nil, fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	if req.PermissionArn == "" {
		return nil, fmt.Errorf("%w: permissionArn is required", errInvalidRequest)
	}

	if err := h.Backend.DisassociateResourceSharePermission(req.ResourceShareArn, req.PermissionArn); err != nil {
		return nil, err
	}

	return json.Marshal(disassociateResourceSharePermissionResponse{ReturnValue: true})
}

type replacePermissionAssociationsRequest struct {
	FromPermissionVersion *int32 `json:"fromPermissionVersion,omitempty"`
	FromPermissionArn     string `json:"fromPermissionArn"`
	ToPermissionArn       string `json:"toPermissionArn"`
}

// replacePermissionAssociationsWork is the JSON representation of a
// ReplacePermissionAssociationsWork item. fromPermissionVersion/toPermissionVersion are
// strings on the wire (matching the real SDK's ReplacePermissionAssociationsWork type,
// which models them as *string even though they are numeric version identifiers).
type replacePermissionAssociationsWork struct {
	ID                    string  `json:"id"`
	FromPermissionArn     string  `json:"fromPermissionArn"`
	FromPermissionVersion string  `json:"fromPermissionVersion"`
	ToPermissionArn       string  `json:"toPermissionArn"`
	ToPermissionVersion   string  `json:"toPermissionVersion"`
	Status                string  `json:"status"`
	StatusMessage         string  `json:"statusMessage,omitempty"`
	CreationTime          float64 `json:"creationTime"`
	LastUpdatedTime       float64 `json:"lastUpdatedTime"`
}

func toReplacePermissionAssociationsWorkObject(
	w *ReplacePermissionAssociationsWork,
) replacePermissionAssociationsWork {
	return replacePermissionAssociationsWork{
		ID:                    w.ID,
		FromPermissionArn:     w.FromPermissionARN,
		FromPermissionVersion: strconv.Itoa(int(w.FromPermissionVersion)),
		ToPermissionArn:       w.ToPermissionARN,
		ToPermissionVersion:   strconv.Itoa(int(w.ToPermissionVersion)),
		Status:                w.Status,
		StatusMessage:         w.StatusMessage,
		CreationTime:          epochSeconds(w.CreationTime),
		LastUpdatedTime:       epochSeconds(w.LastUpdatedTime),
	}
}

type replacePermissionAssociationsResponse struct {
	ReplacePermissionAssociationsWork replacePermissionAssociationsWork `json:"replacePermissionAssociationsWork"`
}

func (h *Handler) handleReplacePermissionAssociations(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req replacePermissionAssociationsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FromPermissionArn == "" {
		return nil, fmt.Errorf("%w: fromPermissionArn is required", errInvalidRequest)
	}

	if req.ToPermissionArn == "" {
		return nil, fmt.Errorf("%w: toPermissionArn is required", errInvalidRequest)
	}

	work, err := h.Backend.ReplacePermissionAssociations(
		req.FromPermissionArn,
		req.ToPermissionArn,
		req.FromPermissionVersion,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(replacePermissionAssociationsResponse{
		ReplacePermissionAssociationsWork: toReplacePermissionAssociationsWorkObject(work),
	})
}

// permissionAssociationObject matches types.AssociatedPermission
// (aws-sdk-go-v2/service/ram@v1.39.4 types/types.go:11-): the permission ARN wire key is
// "arn", not "permissionArn" -- and PermissionVersion is a *string on the wire
// (deserializers.go's awsRestjson1_deserializeDocumentAssociatedPermission type-asserts it
// to string and errors otherwise), not a number.
type permissionAssociationObject struct {
	Arn               string `json:"arn"`
	ResourceShareArn  string `json:"resourceShareArn"`
	PermissionVersion string `json:"permissionVersion"`
}

type listPermissionAssociationsRequest struct {
	PermissionVersion *int32 `json:"permissionVersion,omitempty"`
	MaxResults        *int32 `json:"maxResults,omitempty"`
	PermissionArn     string `json:"permissionArn"`
	NextToken         string `json:"nextToken"`
}

type listPermissionAssociationsResponse struct {
	NextToken   string                        `json:"nextToken,omitempty"`
	Permissions []permissionAssociationObject `json:"permissions"`
}

func (h *Handler) handleListPermissionAssociations(_ context.Context, body []byte) ([]byte, error) {
	var req listPermissionAssociationsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	assocs := h.Backend.ListPermissionAssociations(req.PermissionArn)
	objs := make([]permissionAssociationObject, 0, len(assocs))

	for _, a := range assocs {
		objs = append(objs, permissionAssociationObject{
			Arn:               a.PermissionARN,
			ResourceShareArn:  a.ShareARN,
			PermissionVersion: strconv.Itoa(int(a.Version)),
		})
	}

	page, nextToken, err := ramPaginate(objs, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	return json.Marshal(listPermissionAssociationsResponse{NextToken: nextToken, Permissions: page})
}

// listReplacePermissionAssociationsWorkResponse's list field is plural
// (replacePermissionAssociationsWorks) on the wire -- distinct from the singular
// replacePermissionAssociationsWork field on ReplacePermissionAssociations's own response.
type listReplacePermissionAssociationsWorkResponse struct {
	NextToken                          string                              `json:"nextToken,omitempty"`
	ReplacePermissionAssociationsWorks []replacePermissionAssociationsWork `json:"replacePermissionAssociationsWorks"`
}

type listReplacePermissionAssociationsWorkRequest struct {
	MaxResults *int32   `json:"maxResults,omitempty"`
	Status     string   `json:"status"`
	NextToken  string   `json:"nextToken"`
	WorkIDs    []string `json:"workIds"`
}

func (h *Handler) handleListReplacePermissionAssociationsWork(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req listReplacePermissionAssociationsWorkRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	works := h.Backend.ListReplacePermissionAssociationsWork(req.WorkIDs, req.Status)
	objs := make([]replacePermissionAssociationsWork, 0, len(works))

	for _, w := range works {
		objs = append(objs, toReplacePermissionAssociationsWorkObject(w))
	}

	page, nextToken, err := ramPaginate(objs, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	return json.Marshal(listReplacePermissionAssociationsWorkResponse{
		NextToken:                          nextToken,
		ReplacePermissionAssociationsWorks: page,
	})
}
