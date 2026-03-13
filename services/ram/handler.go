package ram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new RAM handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "RAM" }

// GetSupportedOperations returns the list of supported RAM operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateResourceShare",
		"GetResourceShares",
		"UpdateResourceShare",
		"DeleteResourceShare",
		"AssociateResourceShare",
		"DisassociateResourceShare",
		"GetResourceShareAssociations",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"EnableSharingWithAwsOrganization",
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

		return strings.HasPrefix(path, "/createresourceshare") ||
			strings.HasPrefix(path, "/getresourceshares") ||
			strings.HasPrefix(path, "/updateresourceshare") ||
			strings.HasPrefix(path, "/deleteresourceshare") ||
			strings.HasPrefix(path, "/associateresourceshare") ||
			strings.HasPrefix(path, "/disassociateresourceshare") ||
			strings.HasPrefix(path, "/getresourceshareassociations") ||
			strings.HasPrefix(path, "/tagresource") ||
			strings.HasPrefix(path, "/untagresource") ||
			strings.HasPrefix(path, "/listtagsforresource") ||
			strings.HasPrefix(path, "/enablesharingwithawsorganization")
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return ramMatchPriority }

// ExtractOperation extracts the operation name from the request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path

	switch {
	case strings.HasPrefix(path, "/createresourceshare"):
		return "CreateResourceShare"
	case strings.HasPrefix(path, "/getresourceshares"):
		return "GetResourceShares"
	case strings.HasPrefix(path, "/updateresourceshare"):
		return "UpdateResourceShare"
	case strings.HasPrefix(path, "/deleteresourceshare"):
		return "DeleteResourceShare"
	case strings.HasPrefix(path, "/disassociateresourceshare") &&
		!strings.HasPrefix(path, "/disassociateresourcesharepermission"):
		return "DisassociateResourceShare"
	case strings.HasPrefix(path, "/associateresourceshare") &&
		!strings.HasPrefix(path, "/associateresourcesharepermission"):
		return "AssociateResourceShare"
	case strings.HasPrefix(path, "/getresourceshareassociations"):
		return "GetResourceShareAssociations"
	case strings.HasPrefix(path, "/tagresource"):
		return "TagResource"
	case strings.HasPrefix(path, "/untagresource"):
		return "UntagResource"
	case strings.HasPrefix(path, "/listtagsforresource"):
		return "ListTagsForResource"
	case strings.HasPrefix(path, "/enablesharingwithawsorganization"):
		return "EnableSharingWithAwsOrganization"
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
	switch op {
	case "CreateResourceShare":
		return h.handleCreateResourceShare(ctx, body)
	case "GetResourceShares":
		return h.handleGetResourceShares(ctx, body)
	case "UpdateResourceShare":
		return h.handleUpdateResourceShare(ctx, body)
	case "DeleteResourceShare":
		return h.handleDeleteResourceShare(ctx, c)
	case "AssociateResourceShare":
		return h.handleAssociateResourceShare(ctx, body)
	case "DisassociateResourceShare":
		return h.handleDisassociateResourceShare(ctx, body)
	case "GetResourceShareAssociations":
		return h.handleGetResourceShareAssociations(ctx, body)
	case "TagResource":
		return h.handleTagResource(ctx, body)
	case "UntagResource":
		return h.handleUntagResource(ctx, body)
	case "ListTagsForResource":
		return h.handleListTagsForResource(ctx, body)
	case "EnableSharingWithAwsOrganization":
		return h.handleEnableSharingWithAwsOrganization()
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
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

func (h *Handler) handleTagResource(_ context.Context, body []byte) ([]byte, error) {
	var req tagResourceRequest
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

	if err := h.Backend.TagResource(shareARN, fromTagObjects(req.Tags)); err != nil {
		return nil, err
	}

	return nil, nil
}

type untagResourceRequest struct {
	ResourceShareArn string   `json:"resourceShareArn"`
	ResourceArn      string   `json:"resourceArn"`
	TagKeys          []string `json:"tagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, body []byte) ([]byte, error) {
	var req untagResourceRequest
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

	if err := h.Backend.UntagResource(shareARN, req.TagKeys); err != nil {
		return nil, err
	}

	return nil, nil
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

type enableSharingWithAwsOrganizationResponse struct {
	ReturnValue bool `json:"returnValue"`
}

func (h *Handler) handleEnableSharingWithAwsOrganization() ([]byte, error) {
	return json.Marshal(enableSharingWithAwsOrganizationResponse{ReturnValue: true})
}
