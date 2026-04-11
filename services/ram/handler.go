package ram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	ramService       = "ram"
	ramMatchPriority = 87
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the HTTP handler for the AWS RAM REST API.
type Handler struct {
	Backend   StorageBackend
	AccountID string
	Region    string
}

// NewHandler creates a new RAM handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.AccountID(),
		Region:    backend.Region(),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "RAM" }

// GetSupportedOperations returns the list of supported RAM operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AcceptResourceShareInvitation",
		"AssociateResourceSharePermission",
		"CreatePermission",
		"CreatePermissionVersion",
		"CreateResourceShare",
		"DeletePermission",
		"DeletePermissionVersion",
		"DeleteResourceShare",
		"DisassociateResourceSharePermission",
		"DisassociateResourceShare",
		"AssociateResourceShare",
		"GetPermission",
		"GetResourcePolicies",
		"GetResourceShareAssociations",
		"GetResourceShareInvitations",
		"GetResourceShares",
		"EnableSharingWithAwsOrganization",
		"ListResourceSharePermissions",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"UpdateResourceShare",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return ramService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches RAM API requests.
// All path-based matches are gated on the SigV4 service name to prevent
// routing conflicts with other services that share similar REST paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if httputils.ExtractServiceFromRequest(c.Request()) != ramService {
			return false
		}

		path := c.Request().URL.Path

		return isRAMCreateOrAcceptPath(path) || isRAMDeletePath(path) ||
			isRAMGetPath(path) || isRAMAssociationPath(path) || isRAMTagPath(path)
	}
}

// isRAMCreateOrAcceptPath returns true for create/accept paths.
func isRAMCreateOrAcceptPath(path string) bool {
	return strings.HasPrefix(path, "/acceptresourceshareinvitation") ||
		strings.HasPrefix(path, "/createpermission") ||
		strings.HasPrefix(path, "/createresourceshare") ||
		strings.HasPrefix(path, "/enablesharingwithawsorganization") ||
		strings.HasPrefix(path, "/updateresourceshare")
}

// isRAMDeletePath returns true for delete paths.
func isRAMDeletePath(path string) bool {
	return strings.HasPrefix(path, "/deletepermission") ||
		strings.HasPrefix(path, "/deleteresourceshare")
}

// isRAMGetPath returns true for read paths.
func isRAMGetPath(path string) bool {
	return strings.HasPrefix(path, "/getpermission") ||
		strings.HasPrefix(path, "/getresourcepolicies") ||
		strings.HasPrefix(path, "/getresourceshareassociations") ||
		strings.HasPrefix(path, "/getresourceshareinvitations") ||
		strings.HasPrefix(path, "/getresourceshares") ||
		strings.HasPrefix(path, "/listresourcesharepermissions") ||
		strings.HasPrefix(path, "/listtagsforresource")
}

// isRAMAssociationPath returns true for association/disassociation paths.
func isRAMAssociationPath(path string) bool {
	return strings.HasPrefix(path, "/associateresourcesharepermission") ||
		strings.HasPrefix(path, "/associateresourceshare") ||
		strings.HasPrefix(path, "/disassociateresourcesharepermission") ||
		strings.HasPrefix(path, "/disassociateresourceshare")
}

// isRAMTagPath returns true for tag paths.
func isRAMTagPath(path string) bool {
	return strings.HasPrefix(path, "/tagresource") ||
		strings.HasPrefix(path, "/untagresource")
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return ramMatchPriority }

// ExtractOperation extracts the operation name from the request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path

	if op := extractCommonOperation(path); op != "" {
		return op
	}

	return extractAssociationOperation(path)
}

// extractCommonOperation maps non-association RAM paths to their operation names.
func extractCommonOperation(path string) string {
	if op := extractCreateDeleteOp(path); op != "" {
		return op
	}

	return extractGetListOp(path)
}

// extractCreateDeleteOp maps create/delete/accept paths to operation names.
func extractCreateDeleteOp(path string) string {
	switch {
	case strings.HasPrefix(path, "/acceptresourceshareinvitation"):
		return "AcceptResourceShareInvitation"
	case strings.HasPrefix(path, "/createpermissionversion"):
		return "CreatePermissionVersion"
	case strings.HasPrefix(path, "/createpermission"):
		return "CreatePermission"
	case strings.HasPrefix(path, "/createresourceshare"):
		return "CreateResourceShare"
	case strings.HasPrefix(path, "/deletepermissionversion"):
		return "DeletePermissionVersion"
	case strings.HasPrefix(path, "/deletepermission"):
		return "DeletePermission"
	case strings.HasPrefix(path, "/deleteresourceshare"):
		return "DeleteResourceShare"
	case strings.HasPrefix(path, "/enablesharingwithawsorganization"):
		return "EnableSharingWithAwsOrganization"
	case strings.HasPrefix(path, "/updateresourceshare"):
		return "UpdateResourceShare"
	default:
		return ""
	}
}

// extractGetListOp maps get/list paths to operation names.
func extractGetListOp(path string) string {
	switch {
	case strings.HasPrefix(path, "/getpermission"):
		return "GetPermission"
	case strings.HasPrefix(path, "/getresourcepolicies"):
		return "GetResourcePolicies"
	case strings.HasPrefix(path, "/getresourceshareassociations"):
		return "GetResourceShareAssociations"
	case strings.HasPrefix(path, "/getresourceshareinvitations"):
		return "GetResourceShareInvitations"
	case strings.HasPrefix(path, "/getresourceshares"):
		return "GetResourceShares"
	case strings.HasPrefix(path, "/listresourcesharepermissions"):
		return "ListResourceSharePermissions"
	case strings.HasPrefix(path, "/listtagsforresource"):
		return "ListTagsForResource"
	case strings.HasPrefix(path, "/tagresource"):
		return "TagResource"
	case strings.HasPrefix(path, "/untagresource"):
		return "UntagResource"
	default:
		return ""
	}
}

// extractAssociationOperation maps associate/disassociate RAM paths to their operation names.
func extractAssociationOperation(path string) string {
	switch {
	case strings.HasPrefix(path, "/associateresourcesharepermission"):
		return "AssociateResourceSharePermission"
	case strings.HasPrefix(path, "/disassociateresourcesharepermission"):
		return "DisassociateResourceSharePermission"
	case strings.HasPrefix(path, "/disassociateresourceshare"):
		return "DisassociateResourceShare"
	case strings.HasPrefix(path, "/associateresourceshare"):
		return "AssociateResourceShare"
	default:
		return "Unknown"
	}
}

// ExtractResource extracts the resource share ARN from the request body or query.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return c.Request().URL.Query().Get("resourceShareArn")
}

// Handler returns the Echo handler function for RAM requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "ram: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := h.ExtractOperation(c)

		result, dispErr := h.dispatch(ctx, op, c, body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.JSON(http.StatusOK, map[string]any{})
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op string, c *echo.Context, body []byte) ([]byte, error) {
	if result, ok, err := h.dispatchMutateOps(ctx, op, c, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchReadOps(ctx, op, body); ok {
		return result, err
	}

	return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
}

func (h *Handler) dispatchMutateOps(
	ctx context.Context,
	op string,
	c *echo.Context,
	body []byte,
) ([]byte, bool, error) {
	if result, ok, err := h.dispatchCRUDOps(ctx, op, c, body); ok {
		return result, true, err
	}

	if result, ok, err := h.dispatchAssocOps(ctx, op, body); ok {
		return result, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchCRUDOps(
	ctx context.Context,
	op string,
	c *echo.Context,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case "AcceptResourceShareInvitation":
		r, err := h.handleAcceptResourceShareInvitation(ctx, body)

		return r, true, err
	case "CreatePermission":
		r, err := h.handleCreatePermission(ctx, body)

		return r, true, err
	case "CreatePermissionVersion":
		r, err := h.handleCreatePermissionVersion(ctx, body)

		return r, true, err
	case "CreateResourceShare":
		r, err := h.handleCreateResourceShare(ctx, body)

		return r, true, err
	case "DeletePermission":
		r, err := h.handleDeletePermission(ctx, c)

		return r, true, err
	case "DeletePermissionVersion":
		r, err := h.handleDeletePermissionVersion(ctx, c)

		return r, true, err
	case "DeleteResourceShare":
		r, err := h.handleDeleteResourceShare(ctx, c)

		return r, true, err
	case "UpdateResourceShare":
		r, err := h.handleUpdateResourceShare(ctx, body)

		return r, true, err
	case "EnableSharingWithAwsOrganization":
		r, err := h.handleEnableSharingWithAwsOrganization()

		return r, true, err
	case "TagResource":
		err := h.handleTagResource(ctx, body)

		return nil, true, err
	case "UntagResource":
		err := h.handleUntagResource(ctx, body)

		return nil, true, err
	default:
		return nil, false, nil
	}
}

func (h *Handler) dispatchAssocOps(ctx context.Context, op string, body []byte) ([]byte, bool, error) {
	switch op {
	case "AssociateResourceSharePermission":
		r, err := h.handleAssociateResourceSharePermission(ctx, body)

		return r, true, err
	case "AssociateResourceShare":
		r, err := h.handleAssociateResourceShare(ctx, body)

		return r, true, err
	case "DisassociateResourceSharePermission":
		r, err := h.handleDisassociateResourceSharePermission(ctx, body)

		return r, true, err
	case "DisassociateResourceShare":
		r, err := h.handleDisassociateResourceShare(ctx, body)

		return r, true, err
	default:
		return nil, false, nil
	}
}

func (h *Handler) dispatchReadOps(ctx context.Context, op string, body []byte) ([]byte, bool, error) {
	switch op {
	case "GetPermission":
		r, err := h.handleGetPermission(ctx, body)

		return r, true, err
	case "GetResourcePolicies":
		r, err := h.handleGetResourcePolicies(ctx, body)

		return r, true, err
	case "GetResourceShareAssociations":
		r, err := h.handleGetResourceShareAssociations(ctx, body)

		return r, true, err
	case "GetResourceShareInvitations":
		r, err := h.handleGetResourceShareInvitations(ctx, body)

		return r, true, err
	case "GetResourceShares":
		r, err := h.handleGetResourceShares(ctx, body)

		return r, true, err
	case "ListResourceSharePermissions":
		r, err := h.handleListResourceSharePermissions(ctx, body)

		return r, true, err
	case "ListTagsForResource":
		r, err := h.handleListTagsForResource(ctx, body)

		return r, true, err
	default:
		return nil, false, nil
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrPermissionNotFound),
		errors.Is(err, ErrPermissionVersionNotFound), errors.Is(err, ErrInvitationNotFound):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "UnknownResourceException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ResourceShareAlreadyExistsException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrInvitationAlreadyAccepted):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ResourceShareInvitationAlreadyAcceptedException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

// epochSeconds converts a [time.Time] to Unix epoch seconds as float64,
// as required by the AWS REST-JSON protocol for timestamp fields.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix())
}

// tagObject represents a RAM tag in the JSON API format.
type tagObject struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// toTagObjects converts a map of tags to a slice of tag objects.
func toTagObjects(tags map[string]string) []tagObject {
	result := make([]tagObject, 0, len(tags))

	for k, v := range tags {
		result = append(result, tagObject{Key: k, Value: v})
	}

	return result
}

// fromTagObjects converts a slice of tag objects to a map.
func fromTagObjects(tags []tagObject) map[string]string {
	result := make(map[string]string, len(tags))

	for _, t := range tags {
		result[t.Key] = t.Value
	}

	return result
}

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
		FeatureSet:              "STANDARD",
		AllowExternalPrincipals: rs.AllowExternalPrincipals,
		CreationTime:            epochSeconds(rs.CreationTime),
		LastUpdatedTime:         epochSeconds(rs.LastUpdatedTime),
	}

	if len(rs.Tags) > 0 {
		obj.Tags = toTagObjects(rs.Tags)
	}

	return obj
}

// associationObject is the JSON representation of a ResourceShareAssociation.
type associationObject struct {
	ResourceShareArn  string  `json:"resourceShareArn"`
	ResourceShareName string  `json:"resourceShareName"`
	AssociatedEntity  string  `json:"associatedEntity"`
	AssociationType   string  `json:"associationType"`
	Status            string  `json:"status"`
	CreationTime      float64 `json:"creationTime"`
	LastUpdatedTime   float64 `json:"lastUpdatedTime"`
	External          bool    `json:"external"`
}

func toAssociationObject(a *ResourceShareAssociation) associationObject {
	return associationObject{
		ResourceShareArn:  a.ResourceShareARN,
		ResourceShareName: a.ResourceShareName,
		AssociatedEntity:  a.AssociatedEntity,
		AssociationType:   a.AssociationType,
		Status:            a.Status,
		CreationTime:      epochSeconds(a.CreationTime),
		LastUpdatedTime:   epochSeconds(a.LastUpdatedTime),
		External:          a.External,
	}
}

type createResourceShareRequest struct {
	Name                    string      `json:"name"`
	Tags                    []tagObject `json:"tags"`
	Principals              []string    `json:"principals"`
	ResourceArns            []string    `json:"resourceArns"`
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

	return json.Marshal(createResourceShareResponse{
		ResourceShare: toResourceShareObject(rs),
	})
}

type getResourceSharesRequest struct {
	ResourceOwner     string   `json:"resourceOwner"`
	Name              string   `json:"name"`
	NextToken         string   `json:"nextToken"`
	ResourceShareArns []string `json:"resourceShareArns"`
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

	// If specific ARNs requested, look them up individually.
	if len(req.ResourceShareArns) > 0 {
		shares := make([]resourceShareObject, 0, len(req.ResourceShareArns))

		for _, shareARN := range req.ResourceShareArns {
			rs, err := h.Backend.GetResourceShare(shareARN)
			if err != nil {
				continue
			}

			shares = append(shares, toResourceShareObject(rs))
		}

		return json.Marshal(getResourceSharesResponse{ResourceShares: shares})
	}

	list := h.Backend.ListResourceShares(req.ResourceOwner)

	// Filter by name if provided.
	shares := make([]resourceShareObject, 0, len(list))

	for _, rs := range list {
		if req.Name != "" && rs.Name != req.Name {
			continue
		}

		shares = append(shares, toResourceShareObject(rs))
	}

	return json.Marshal(getResourceSharesResponse{ResourceShares: shares})
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

	rs, err := h.Backend.UpdateResourceShare(req.ResourceShareArn, req.Name, req.AllowExternalPrincipals)
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
		return nil, fmt.Errorf("%w: resourceShareArn query parameter is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteResourceShare(shareARN); err != nil {
		return nil, err
	}

	return json.Marshal(deleteResourceShareResponse{ReturnValue: true})
}

type associateResourceShareRequest struct {
	ResourceShareArn string   `json:"resourceShareArn"`
	Principals       []string `json:"principals"`
	ResourceArns     []string `json:"resourceArns"`
}

type associateResourceShareResponse struct {
	ResourceShareAssociations []associationObject `json:"resourceShareAssociations"`
}

func (h *Handler) handleAssociateResourceShare(_ context.Context, body []byte) ([]byte, error) {
	var req associateResourceShareRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareArn == "" {
		return nil, fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	associations, err := h.Backend.AssociateResourceShare(req.ResourceShareArn, req.Principals, req.ResourceArns)
	if err != nil {
		return nil, err
	}

	objs := make([]associationObject, 0, len(associations))

	for _, a := range associations {
		objs = append(objs, toAssociationObject(a))
	}

	return json.Marshal(associateResourceShareResponse{ResourceShareAssociations: objs})
}

type disassociateResourceShareRequest struct {
	ResourceShareArn string   `json:"resourceShareArn"`
	Principals       []string `json:"principals"`
	ResourceArns     []string `json:"resourceArns"`
}

type disassociateResourceShareResponse struct {
	ResourceShareAssociations []associationObject `json:"resourceShareAssociations"`
}

func (h *Handler) handleDisassociateResourceShare(_ context.Context, body []byte) ([]byte, error) {
	var req disassociateResourceShareRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareArn == "" {
		return nil, fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	associations, err := h.Backend.DisassociateResourceShare(req.ResourceShareArn, req.Principals, req.ResourceArns)
	if err != nil {
		return nil, err
	}

	objs := make([]associationObject, 0, len(associations))

	for _, a := range associations {
		objs = append(objs, toAssociationObject(a))
	}

	return json.Marshal(disassociateResourceShareResponse{ResourceShareAssociations: objs})
}

type getResourceShareAssociationsRequest struct {
	AssociationType   string   `json:"associationType"`
	Principal         string   `json:"principal"`
	ResourceArn       string   `json:"resourceArn"`
	NextToken         string   `json:"nextToken"`
	ResourceShareArns []string `json:"resourceShareArns"`
}

type getResourceShareAssociationsResponse struct {
	NextToken                 string              `json:"nextToken,omitempty"`
	ResourceShareAssociations []associationObject `json:"resourceShareAssociations"`
}

func (h *Handler) handleGetResourceShareAssociations(_ context.Context, body []byte) ([]byte, error) {
	var req getResourceShareAssociationsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	associations := h.Backend.GetResourceShareAssociations(req.AssociationType, req.ResourceShareArns)

	// Apply principal or resource ARN filter.
	filtered := make([]associationObject, 0, len(associations))

	for _, a := range associations {
		if req.Principal != "" && a.AssociatedEntity != req.Principal {
			continue
		}

		if req.ResourceArn != "" && a.AssociatedEntity != req.ResourceArn {
			continue
		}

		filtered = append(filtered, toAssociationObject(a))
	}

	return json.Marshal(getResourceShareAssociationsResponse{ResourceShareAssociations: filtered})
}

type tagResourceRequest struct {
	ResourceShareArn string      `json:"resourceShareArn"`
	ResourceArn      string      `json:"resourceArn"`
	Tags             []tagObject `json:"tags"`
}

func (h *Handler) handleTagResource(_ context.Context, body []byte) error {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	shareARN := req.ResourceShareArn
	if shareARN == "" {
		shareARN = req.ResourceArn
	}

	if shareARN == "" {
		return fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(shareARN, fromTagObjects(req.Tags)); err != nil {
		return err
	}

	return nil
}

type untagResourceRequest struct {
	ResourceShareArn string   `json:"resourceShareArn"`
	ResourceArn      string   `json:"resourceArn"`
	TagKeys          []string `json:"tagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, body []byte) error {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	shareARN := req.ResourceShareArn
	if shareARN == "" {
		shareARN = req.ResourceArn
	}

	if shareARN == "" {
		return fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(shareARN, req.TagKeys); err != nil {
		return err
	}

	return nil
}

type listTagsForResourceRequest struct {
	ResourceShareArn string `json:"resourceShareArn"`
	ResourceArn      string `json:"resourceArn"`
}

type listTagsForResourceResponse struct {
	Tags []tagObject `json:"tags"`
}

func (h *Handler) handleListTagsForResource(_ context.Context, body []byte) ([]byte, error) {
	var req listTagsForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	shareARN := req.ResourceShareArn
	if shareARN == "" {
		shareARN = req.ResourceArn
	}

	if shareARN == "" {
		return nil, fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(shareARN)
	if err != nil {
		return nil, err
	}

	return json.Marshal(listTagsForResourceResponse{Tags: toTagObjects(tags)})
}

type listResourceSharePermissionsRequest struct {
	ResourceShareArn string `json:"resourceShareArn"`
	NextToken        string `json:"nextToken"`
}

type listResourceSharePermissionsResponse struct {
	NextToken   string `json:"nextToken,omitempty"`
	Permissions []any  `json:"permissions"`
}

// handleListResourceSharePermissions returns an empty permissions list.
// The Terraform AWS provider calls this after creating a resource share to read
// its managed permissions. Returning an empty list is sufficient for the mock.
func (h *Handler) handleListResourceSharePermissions(_ context.Context, body []byte) ([]byte, error) {
	var req listResourceSharePermissionsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return json.Marshal(listResourceSharePermissionsResponse{Permissions: []any{}})
}

type enableSharingWithAwsOrganizationResponse struct {
	ReturnValue bool `json:"returnValue"`
}

func (h *Handler) handleEnableSharingWithAwsOrganization() ([]byte, error) {
	return json.Marshal(enableSharingWithAwsOrganizationResponse{ReturnValue: true})
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// permissionSummaryObject is the JSON representation of a RAM permission summary.
type permissionSummaryObject struct {
	Arn                   string      `json:"arn"`
	Name                  string      `json:"name"`
	ResourceType          string      `json:"resourceType"`
	PermissionType        string      `json:"permissionType"`
	FeatureSet            string      `json:"featureSet"`
	Version               string      `json:"version"`
	Tags                  []tagObject `json:"tags,omitempty"`
	CreationTime          float64     `json:"creationTime"`
	LastUpdatedTime       float64     `json:"lastUpdatedTime"`
	DefaultVersion        bool        `json:"defaultVersion"`
	IsResourceTypeDefault bool        `json:"isResourceTypeDefault"`
}

// permissionDetailObject is the JSON representation of a RAM permission detail (GetPermission).
type permissionDetailObject struct {
	Arn                   string      `json:"arn"`
	Name                  string      `json:"name"`
	ResourceType          string      `json:"resourceType"`
	PermissionType        string      `json:"permissionType"`
	FeatureSet            string      `json:"featureSet"`
	Version               string      `json:"version"`
	Permission            string      `json:"permission,omitempty"`
	Tags                  []tagObject `json:"tags,omitempty"`
	CreationTime          float64     `json:"creationTime"`
	LastUpdatedTime       float64     `json:"lastUpdatedTime"`
	DefaultVersion        bool        `json:"defaultVersion"`
	IsResourceTypeDefault bool        `json:"isResourceTypeDefault"`
}

func toPermissionSummaryObject(p *Permission) permissionSummaryObject {
	obj := permissionSummaryObject{
		Arn:             p.ARN,
		Name:            p.Name,
		ResourceType:    p.ResourceType,
		PermissionType:  permissionTypeCustomer,
		FeatureSet:      "STANDARD",
		CreationTime:    epochSeconds(p.CreationTime),
		LastUpdatedTime: epochSeconds(p.LastUpdatedTime),
		Version:         strconv.Itoa(int(p.DefaultVersion)),
		DefaultVersion:  true,
	}

	if len(p.Tags) > 0 {
		obj.Tags = toTagObjects(p.Tags)
	}

	return obj
}

func toPermissionDetailObject(p *Permission, pv *PermissionVersion) permissionDetailObject {
	obj := permissionDetailObject{
		Arn:             p.ARN,
		Name:            p.Name,
		ResourceType:    p.ResourceType,
		PermissionType:  permissionTypeCustomer,
		FeatureSet:      "STANDARD",
		CreationTime:    epochSeconds(p.CreationTime),
		LastUpdatedTime: epochSeconds(p.LastUpdatedTime),
		Version:         strconv.Itoa(int(pv.Version)),
		DefaultVersion:  pv.Version == p.DefaultVersion,
		Permission:      pv.PolicyTemplate,
	}

	if len(p.Tags) > 0 {
		obj.Tags = toTagObjects(p.Tags)
	}

	return obj
}

// invitationObject is the JSON representation of a ResourceShareInvitation.
type invitationObject struct {
	ResourceShareInvitationArn string  `json:"resourceShareInvitationArn"`
	ResourceShareArn           string  `json:"resourceShareArn"`
	ResourceShareName          string  `json:"resourceShareName"`
	SenderAccountID            string  `json:"senderAccountId"`
	ReceiverAccountID          string  `json:"receiverAccountId"`
	Status                     string  `json:"status"`
	InvitationTimestamp        float64 `json:"invitationTimestamp"`
}

func toInvitationObject(inv *ResourceShareInvitation) invitationObject {
	return invitationObject{
		ResourceShareInvitationArn: inv.InvitationARN,
		ResourceShareArn:           inv.ResourceShareARN,
		ResourceShareName:          inv.ResourceShareName,
		SenderAccountID:            inv.SenderAccountID,
		ReceiverAccountID:          inv.ReceiverAccountID,
		Status:                     inv.Status,
		InvitationTimestamp:        epochSeconds(inv.CreationTime),
	}
}

// --- AcceptResourceShareInvitation ---

type acceptResourceShareInvitationRequest struct {
	ResourceShareInvitationArn string `json:"resourceShareInvitationArn"`
}

type acceptResourceShareInvitationResponse struct {
	ResourceShareInvitation invitationObject `json:"resourceShareInvitation"`
}

func (h *Handler) handleAcceptResourceShareInvitation(_ context.Context, body []byte) ([]byte, error) {
	var req acceptResourceShareInvitationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareInvitationArn == "" {
		return nil, fmt.Errorf("%w: resourceShareInvitationArn is required", errInvalidRequest)
	}

	inv, err := h.Backend.AcceptResourceShareInvitation(req.ResourceShareInvitationArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(acceptResourceShareInvitationResponse{
		ResourceShareInvitation: toInvitationObject(inv),
	})
}

// --- CreatePermission ---

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

	p, err := h.Backend.CreatePermission(req.Name, req.ResourceType, req.PolicyTemplate, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(createPermissionResponse{Permission: toPermissionSummaryObject(p)})
}

// --- CreatePermissionVersion ---

type createPermissionVersionRequest struct {
	PermissionArn  string `json:"permissionArn"`
	PolicyTemplate string `json:"policyTemplate"`
}

type createPermissionVersionResponse struct {
	Permission permissionSummaryObject `json:"permission"`
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

	return json.Marshal(createPermissionVersionResponse{Permission: toPermissionSummaryObject(p)})
}

// --- DeletePermission ---

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

// --- DeletePermissionVersion ---

type deletePermissionVersionResponse struct {
	PermissionStatus string `json:"permissionStatus,omitempty"`
	ReturnValue      bool   `json:"returnValue"`
}

func (h *Handler) handleDeletePermissionVersion(_ context.Context, c *echo.Context) ([]byte, error) {
	permissionARN := c.Request().URL.Query().Get("permissionArn")
	if permissionARN == "" {
		return nil, fmt.Errorf("%w: permissionArn query parameter is required", errInvalidRequest)
	}

	versionStr := c.Request().URL.Query().Get("permissionVersion")
	if versionStr == "" {
		return nil, fmt.Errorf("%w: permissionVersion query parameter is required", errInvalidRequest)
	}

	var version int32
	if _, err := fmt.Sscanf(versionStr, "%d", &version); err != nil {
		return nil, fmt.Errorf("%w: permissionVersion must be an integer", errInvalidRequest)
	}

	if err := h.Backend.DeletePermissionVersion(permissionARN, version); err != nil {
		return nil, err
	}

	return json.Marshal(deletePermissionVersionResponse{ReturnValue: true, PermissionStatus: "UPDATING"})
}

// --- GetPermission ---

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

// --- AssociateResourceSharePermission ---

type associateResourceSharePermissionRequest struct {
	PermissionVersion *int32 `json:"permissionVersion,omitempty"`
	ResourceShareArn  string `json:"resourceShareArn"`
	PermissionArn     string `json:"permissionArn"`
	Replace           bool   `json:"replace"`
}

type associateResourceSharePermissionResponse struct {
	ReturnValue bool `json:"returnValue"`
}

func (h *Handler) handleAssociateResourceSharePermission(_ context.Context, body []byte) ([]byte, error) {
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

// --- DisassociateResourceSharePermission ---

type disassociateResourceSharePermissionRequest struct {
	ResourceShareArn string `json:"resourceShareArn"`
	PermissionArn    string `json:"permissionArn"`
}

type disassociateResourceSharePermissionResponse struct {
	ReturnValue bool `json:"returnValue"`
}

func (h *Handler) handleDisassociateResourceSharePermission(_ context.Context, body []byte) ([]byte, error) {
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

// --- GetResourceShareInvitations ---

type getResourceShareInvitationsRequest struct {
	NextToken                   string   `json:"nextToken"`
	ResourceShareInvitationArns []string `json:"resourceShareInvitationArns"`
	ResourceShareArns           []string `json:"resourceShareArns"`
}

type getResourceShareInvitationsResponse struct {
	NextToken                string             `json:"nextToken,omitempty"`
	ResourceShareInvitations []invitationObject `json:"resourceShareInvitations"`
}

func (h *Handler) handleGetResourceShareInvitations(_ context.Context, body []byte) ([]byte, error) {
	var req getResourceShareInvitationsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	invitations := h.Backend.GetResourceShareInvitations(req.ResourceShareInvitationArns, req.ResourceShareArns)
	objs := make([]invitationObject, 0, len(invitations))

	for _, inv := range invitations {
		objs = append(objs, toInvitationObject(inv))
	}

	return json.Marshal(getResourceShareInvitationsResponse{ResourceShareInvitations: objs})
}

// --- GetResourcePolicies ---

type getResourcePoliciesRequest struct {
	NextToken    string   `json:"nextToken"`
	ResourceArns []string `json:"resourceArns"`
}

type getResourcePoliciesResponse struct {
	NextToken string   `json:"nextToken,omitempty"`
	Policies  []string `json:"policies"`
}

func (h *Handler) handleGetResourcePolicies(_ context.Context, body []byte) ([]byte, error) {
	var req getResourcePoliciesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	policies := h.Backend.GetResourcePolicies(req.ResourceArns)

	return json.Marshal(getResourcePoliciesResponse{Policies: policies})
}
