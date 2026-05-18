package verifiedpermissions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	targetPrefix    = "VerifiedPermissions."
	keyTypeField    = "__type"
	keyMessageField = "message"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for Amazon Verified Permissions operations.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new Verified Permissions handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears all Verified Permissions state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "VerifiedPermissions" }

// GetSupportedOperations returns the list of supported Verified Permissions operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"BatchGetPolicy",
		"BatchIsAuthorized",
		"BatchIsAuthorizedWithToken",
		"CreateIdentitySource",
		"CreatePolicy",
		"CreatePolicyStore",
		"CreatePolicyTemplate",
		"DeleteIdentitySource",
		"DeletePolicy",
		"DeletePolicyStore",
		"DeletePolicyTemplate",
		"GetIdentitySource",
		"GetPolicy",
		"GetPolicyStore",
		"GetPolicyTemplate",
		"GetSchema",
		"IsAuthorized",
		"IsAuthorizedWithToken",
		"ListIdentitySources",
		"ListPolicies",
		"ListPolicyStores",
		"ListPolicyTemplates",
		"ListTagsForResource",
		"PutSchema",
		"TagResource",
		"UntagResource",
		"UpdateIdentitySource",
		"UpdatePolicy",
		"UpdatePolicyStore",
		"UpdatePolicyTemplate",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "verifiedpermissions" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches Verified Permissions API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Verified Permissions action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, targetPrefix)

	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the resource identifier from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		PolicyStoreID    string `json:"policyStoreId"`
		PolicyID         string `json:"policyId"`
		PolicyTemplateID string `json:"policyTemplateId"`
	}
	_ = json.Unmarshal(body, &req)

	if req.PolicyStoreID != "" && req.PolicyID != "" {
		return req.PolicyStoreID + "/" + req.PolicyID
	}

	if req.PolicyStoreID != "" && req.PolicyTemplateID != "" {
		return req.PolicyStoreID + "/" + req.PolicyTemplateID
	}

	return req.PolicyStoreID
}

// Handler returns the Echo handler function for Verified Permissions requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"VerifiedPermissions", "application/x-amz-json-1.0",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"BatchGetPolicy":             service.WrapOp(h.handleBatchGetPolicy),
		"BatchIsAuthorized":          service.WrapOp(h.handleBatchIsAuthorized),
		"BatchIsAuthorizedWithToken": service.WrapOp(h.handleBatchIsAuthorizedWithToken),
		"CreateIdentitySource":       service.WrapOp(h.handleCreateIdentitySource),
		"CreatePolicyStore":          service.WrapOp(h.handleCreatePolicyStore),
		"CreatePolicy":               service.WrapOp(h.handleCreatePolicy),
		"CreatePolicyTemplate":       service.WrapOp(h.handleCreatePolicyTemplate),
		"DeleteIdentitySource":       service.WrapOp(h.handleDeleteIdentitySource),
		"DeletePolicyStore":          service.WrapOp(h.handleDeletePolicyStore),
		"DeletePolicy":               service.WrapOp(h.handleDeletePolicy),
		"DeletePolicyTemplate":       service.WrapOp(h.handleDeletePolicyTemplate),
		"GetIdentitySource":          service.WrapOp(h.handleGetIdentitySource),
		"GetPolicyStore":             service.WrapOp(h.handleGetPolicyStore),
		"GetPolicy":                  service.WrapOp(h.handleGetPolicy),
		"GetPolicyTemplate":          service.WrapOp(h.handleGetPolicyTemplate),
		"GetSchema":                  service.WrapOp(h.handleGetSchema),
		"IsAuthorized":               service.WrapOp(h.handleIsAuthorized),
		"IsAuthorizedWithToken":      service.WrapOp(h.handleIsAuthorizedWithToken),
		"ListIdentitySources":        service.WrapOp(h.handleListIdentitySources),
		"ListPolicyStores":           service.WrapOp(h.handleListPolicyStores),
		"ListPolicies":               service.WrapOp(h.handleListPolicies),
		"ListPolicyTemplates":        service.WrapOp(h.handleListPolicyTemplates),
		"ListTagsForResource":        service.WrapOp(h.handleListTagsForResource),
		"PutSchema":                  service.WrapOp(h.handlePutSchema),
		"TagResource":                service.WrapOp(h.handleTagResource),
		"UntagResource":              service.WrapOp(h.handleUntagResource),
		"UpdateIdentitySource":       service.WrapOp(h.handleUpdateIdentitySource),
		"UpdatePolicyStore":          service.WrapOp(h.handleUpdatePolicyStore),
		"UpdatePolicy":               service.WrapOp(h.handleUpdatePolicy),
		"UpdatePolicyTemplate":       service.WrapOp(h.handleUpdatePolicyTemplate),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ResourceNotFoundException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrConflict):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ResourceConflictException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrInvalidParameter), errors.Is(err, errInvalidRequest),
		errors.Is(err, errUnknownAction), errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ValidationException",
			keyMessageField: err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			keyTypeField:    "InternalServerException",
			keyMessageField: err.Error(),
		})
	}
}

// --- Policy Store operations ---

type validationSettingsJSON struct {
	Mode string `json:"mode"`
}

type createPolicyStoreInput struct {
	Tags               map[string]string      `json:"tags"`
	Description        string                 `json:"description"`
	ValidationSettings validationSettingsJSON `json:"validationSettings"`
	DeletionProtection string                 `json:"deletionProtection,omitempty"`
}

type createPolicyStoreOutput struct {
	PolicyStoreID      string                 `json:"policyStoreId"`
	Arn                string                 `json:"arn"`
	CreatedDate        string                 `json:"createdDate"`
	LastUpdatedDate    string                 `json:"lastUpdatedDate"`
	ValidationSettings validationSettingsJSON `json:"validationSettings"`
}

func (h *Handler) handleCreatePolicyStore(
	_ context.Context,
	in *createPolicyStoreInput,
) (*createPolicyStoreOutput, error) {
	if in.ValidationSettings.Mode == "" {
		return nil, fmt.Errorf("%w: validationSettings.mode is required", errInvalidRequest)
	}

	if in.ValidationSettings.Mode != ValidationModeOff && in.ValidationSettings.Mode != ValidationModeStrict {
		return nil, fmt.Errorf(
			"%w: validationSettings.mode must be %q or %q",
			errInvalidRequest, ValidationModeOff, ValidationModeStrict,
		)
	}

	ps, err := h.Backend.CreatePolicyStore(
		in.Description, in.Tags,
		in.ValidationSettings.Mode, in.DeletionProtection,
	)
	if err != nil {
		return nil, err
	}

	return &createPolicyStoreOutput{
		PolicyStoreID:      ps.PolicyStoreID,
		Arn:                ps.Arn,
		CreatedDate:        ps.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate:    ps.LastUpdated.UTC().Format(timeFormat),
		ValidationSettings: validationSettingsJSON{Mode: ps.ValidationMode},
	}, nil
}

type policyStoreIDInput struct {
	PolicyStoreID string `json:"policyStoreId"`
}

type policyStoreView struct {
	PolicyStoreID      string                 `json:"policyStoreId"`
	Arn                string                 `json:"arn"`
	Description        string                 `json:"description"`
	CreatedDate        string                 `json:"createdDate"`
	LastUpdatedDate    string                 `json:"lastUpdatedDate"`
	ValidationSettings validationSettingsJSON `json:"validationSettings"`
	DeletionProtection string                 `json:"deletionProtection,omitempty"`
}

type getPolicyStoreOutput struct {
	PolicyStoreID      string                 `json:"policyStoreId"`
	Arn                string                 `json:"arn"`
	Description        string                 `json:"description"`
	CreatedDate        string                 `json:"createdDate"`
	LastUpdatedDate    string                 `json:"lastUpdatedDate"`
	ValidationSettings validationSettingsJSON `json:"validationSettings"`
	DeletionProtection string                 `json:"deletionProtection,omitempty"`
}

func (h *Handler) handleGetPolicyStore(_ context.Context, in *policyStoreIDInput) (*getPolicyStoreOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	ps, err := h.Backend.GetPolicyStore(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	return &getPolicyStoreOutput{
		PolicyStoreID:      ps.PolicyStoreID,
		Arn:                ps.Arn,
		Description:        ps.Description,
		CreatedDate:        ps.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate:    ps.LastUpdated.UTC().Format(timeFormat),
		ValidationSettings: validationSettingsJSON{Mode: ps.ValidationMode},
		DeletionProtection: ps.DeletionProtection,
	}, nil
}

type listPolicyStoresInput struct {
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int    `json:"maxResults,omitempty"`
}

type listPolicyStoresOutput struct {
	NextToken    string            `json:"nextToken,omitempty"`
	PolicyStores []policyStoreView `json:"policyStores"`
}

func (h *Handler) handleListPolicyStores(
	_ context.Context,
	in *listPolicyStoresInput,
) (*listPolicyStoresOutput, error) {
	stores, nextToken := h.Backend.ListPolicyStores(in.NextToken, in.MaxResults)
	items := make([]policyStoreView, 0, len(stores))

	for i := range stores {
		ps := &stores[i]
		items = append(items, policyStoreView{
			PolicyStoreID:      ps.PolicyStoreID,
			Arn:                ps.Arn,
			Description:        ps.Description,
			CreatedDate:        ps.CreatedDate.UTC().Format(timeFormat),
			LastUpdatedDate:    ps.LastUpdated.UTC().Format(timeFormat),
			ValidationSettings: validationSettingsJSON{Mode: ps.ValidationMode},
			DeletionProtection: ps.DeletionProtection,
		})
	}

	return &listPolicyStoresOutput{PolicyStores: items, NextToken: nextToken}, nil
}

type updatePolicyStoreInput struct {
	PolicyStoreID      string                  `json:"policyStoreId"`
	Description        string                  `json:"description"`
	ValidationSettings *validationSettingsJSON `json:"validationSettings,omitempty"`
	DeletionProtection string                  `json:"deletionProtection,omitempty"`
}

type updatePolicyStoreOutput struct {
	PolicyStoreID      string                 `json:"policyStoreId"`
	Arn                string                 `json:"arn"`
	LastUpdatedDate    string                 `json:"lastUpdatedDate"`
	ValidationSettings validationSettingsJSON `json:"validationSettings"`
}

func (h *Handler) handleUpdatePolicyStore(
	_ context.Context,
	in *updatePolicyStoreInput,
) (*updatePolicyStoreOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	var validationMode string

	if in.ValidationSettings != nil {
		validationMode = in.ValidationSettings.Mode
	}

	ps, err := h.Backend.UpdatePolicyStore(in.PolicyStoreID, in.Description, validationMode, in.DeletionProtection)
	if err != nil {
		return nil, err
	}

	return &updatePolicyStoreOutput{
		PolicyStoreID:      ps.PolicyStoreID,
		Arn:                ps.Arn,
		LastUpdatedDate:    ps.LastUpdated.UTC().Format(timeFormat),
		ValidationSettings: validationSettingsJSON{Mode: ps.ValidationMode},
	}, nil
}

func (h *Handler) handleDeletePolicyStore(_ context.Context, in *policyStoreIDInput) (*struct{}, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if err := h.Backend.DeletePolicyStore(in.PolicyStoreID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- Policy operations ---

type staticPolicyDefinition struct {
	Statement   string `json:"statement"`
	Description string `json:"description,omitempty"`
}

type templateLinkedPolicyDefinition struct {
	Principal        *entityIdentifier `json:"principal,omitempty"`
	Resource         *entityIdentifier `json:"resource,omitempty"`
	PolicyTemplateID string            `json:"policyTemplateId"`
}

type entityIdentifier struct {
	EntityType string `json:"entityType"`
	EntityID   string `json:"entityId"`
}

type policyDefinitionIn struct {
	Static         *staticPolicyDefinition         `json:"static,omitempty"`
	TemplateLinked *templateLinkedPolicyDefinition `json:"templateLinked,omitempty"`
}

type staticPolicyDefinitionOut struct {
	Statement   string `json:"statement"`
	Description string `json:"description,omitempty"`
}

type templateLinkedPolicyDefinitionOut struct {
	Principal        *entityIdentifier `json:"principal,omitempty"`
	Resource         *entityIdentifier `json:"resource,omitempty"`
	PolicyTemplateID string            `json:"policyTemplateId"`
}

type policyDefinitionOut struct {
	Static         *staticPolicyDefinitionOut         `json:"static,omitempty"`
	TemplateLinked *templateLinkedPolicyDefinitionOut `json:"templateLinked,omitempty"`
}

type createPolicyInput struct {
	Definition    policyDefinitionIn `json:"definition"`
	PolicyStoreID string             `json:"policyStoreId"`
}

type policyIDsOutput struct {
	PolicyStoreID   string `json:"policyStoreId"`
	PolicyID        string `json:"policyId"`
	PolicyType      string `json:"policyType"`
	CreatedDate     string `json:"createdDate"`
	LastUpdatedDate string `json:"lastUpdatedDate"`
}

//nolint:nestif // definition union type dispatch
func (h *Handler) handleCreatePolicy(_ context.Context, in *createPolicyInput) (*policyIDsOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if (in.Definition.Static == nil) == (in.Definition.TemplateLinked == nil) {
		return nil, fmt.Errorf("%w: definition must contain exactly one of static or templateLinked", errInvalidRequest)
	}

	var params CreatePolicyParams

	if in.Definition.Static != nil {
		if in.Definition.Static.Statement == "" {
			return nil, fmt.Errorf("%w: definition.static.statement is required", errInvalidRequest)
		}

		params.PolicyType = policyTypeStatic
		params.Statement = in.Definition.Static.Statement
		params.Description = in.Definition.Static.Description
	} else {
		tl := in.Definition.TemplateLinked
		if tl.PolicyTemplateID == "" {
			return nil, fmt.Errorf("%w: definition.templateLinked.policyTemplateId is required", errInvalidRequest)
		}

		params.PolicyType = policyTypeTemplateLinked
		params.PolicyTemplateID = tl.PolicyTemplateID

		if tl.Principal != nil {
			params.PrincipalEntityType = tl.Principal.EntityType
			params.PrincipalEntityID = tl.Principal.EntityID
		}

		if tl.Resource != nil {
			params.ResourceEntityType = tl.Resource.EntityType
			params.ResourceEntityID = tl.Resource.EntityID
		}
	}

	p, err := h.Backend.CreatePolicy(in.PolicyStoreID, params)
	if err != nil {
		return nil, err
	}

	return &policyIDsOutput{
		PolicyStoreID:   p.PolicyStoreID,
		PolicyID:        p.PolicyID,
		PolicyType:      p.PolicyType,
		CreatedDate:     p.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate: p.LastUpdated.UTC().Format(timeFormat),
	}, nil
}

type policyInput struct {
	PolicyStoreID string `json:"policyStoreId"`
	PolicyID      string `json:"policyId"`
}

type policyView struct {
	PolicyStoreID   string              `json:"policyStoreId"`
	PolicyID        string              `json:"policyId"`
	PolicyType      string              `json:"policyType"`
	Definition      policyDefinitionOut `json:"definition"`
	Principal       *entityIdentifier   `json:"principal,omitempty"`
	Resource        *entityIdentifier   `json:"resource,omitempty"`
	CreatedDate     string              `json:"createdDate"`
	LastUpdatedDate string              `json:"lastUpdatedDate"`
}

type getPolicyOutput struct {
	PolicyStoreID   string              `json:"policyStoreId"`
	PolicyID        string              `json:"policyId"`
	PolicyType      string              `json:"policyType"`
	Definition      policyDefinitionOut `json:"definition"`
	Principal       *entityIdentifier   `json:"principal,omitempty"`
	Resource        *entityIdentifier   `json:"resource,omitempty"`
	CreatedDate     string              `json:"createdDate"`
	LastUpdatedDate string              `json:"lastUpdatedDate"`
}

func policyToView(p *Policy) policyView {
	v := policyView{
		PolicyStoreID:   p.PolicyStoreID,
		PolicyID:        p.PolicyID,
		PolicyType:      p.PolicyType,
		CreatedDate:     p.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate: p.LastUpdated.UTC().Format(timeFormat),
	}

	switch p.PolicyType {
	case policyTypeStatic:
		v.Definition.Static = &staticPolicyDefinitionOut{
			Statement:   p.Statement,
			Description: p.Description,
		}
	case policyTypeTemplateLinked:
		v.Definition.TemplateLinked = &templateLinkedPolicyDefinitionOut{
			PolicyTemplateID: p.PolicyTemplateID,
		}

		if p.PrincipalEntityType != "" {
			v.Definition.TemplateLinked.Principal = &entityIdentifier{
				EntityType: p.PrincipalEntityType,
				EntityID:   p.PrincipalEntityID,
			}
			v.Principal = &entityIdentifier{
				EntityType: p.PrincipalEntityType,
				EntityID:   p.PrincipalEntityID,
			}
		}

		if p.ResourceEntityType != "" {
			v.Definition.TemplateLinked.Resource = &entityIdentifier{
				EntityType: p.ResourceEntityType,
				EntityID:   p.ResourceEntityID,
			}
			v.Resource = &entityIdentifier{
				EntityType: p.ResourceEntityType,
				EntityID:   p.ResourceEntityID,
			}
		}
	}

	return v
}

func (h *Handler) handleGetPolicy(_ context.Context, in *policyInput) (*getPolicyOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.PolicyID == "" {
		return nil, fmt.Errorf("%w: policyId is required", errInvalidRequest)
	}

	p, err := h.Backend.GetPolicy(in.PolicyStoreID, in.PolicyID)
	if err != nil {
		return nil, err
	}

	v := policyToView(p)

	return &getPolicyOutput{
		PolicyStoreID:   v.PolicyStoreID,
		PolicyID:        v.PolicyID,
		PolicyType:      v.PolicyType,
		Definition:      v.Definition,
		Principal:       v.Principal,
		Resource:        v.Resource,
		CreatedDate:     v.CreatedDate,
		LastUpdatedDate: v.LastUpdatedDate,
	}, nil
}

type listPoliciesFilterJSON struct {
	Principal                 *entityIdentifier `json:"principal,omitempty"`
	Resource                  *entityIdentifier `json:"resource,omitempty"`
	PolicyType                string            `json:"policyType,omitempty"`
	PolicyTemplateIDForFilter string            `json:"policyTemplateId,omitempty"`
}

type listPoliciesInput struct {
	PolicyStoreID string                  `json:"policyStoreId"`
	Filter        *listPoliciesFilterJSON `json:"filter,omitempty"`
	NextToken     string                  `json:"nextToken,omitempty"`
	MaxResults    int                     `json:"maxResults,omitempty"`
}

type listPoliciesOutput struct {
	NextToken string       `json:"nextToken,omitempty"`
	Policies  []policyView `json:"policies"`
}

func (h *Handler) handleListPolicies(_ context.Context, in *listPoliciesInput) (*listPoliciesOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	var filter ListPoliciesFilter

	if in.Filter != nil {
		filter.PolicyType = in.Filter.PolicyType
		filter.PolicyTemplateID = in.Filter.PolicyTemplateIDForFilter

		if in.Filter.Principal != nil {
			filter.PrincipalEntityType = in.Filter.Principal.EntityType
			filter.PrincipalEntityID = in.Filter.Principal.EntityID
		}

		if in.Filter.Resource != nil {
			filter.ResourceEntityType = in.Filter.Resource.EntityType
			filter.ResourceEntityID = in.Filter.Resource.EntityID
		}
	}

	policies, nextToken, err := h.Backend.ListPolicies(in.PolicyStoreID, filter, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	items := make([]policyView, 0, len(policies))

	for i := range policies {
		items = append(items, policyToView(&policies[i]))
	}

	return &listPoliciesOutput{Policies: items, NextToken: nextToken}, nil
}

type updatePolicyInput struct {
	Definition    policyDefinitionIn `json:"definition"`
	PolicyStoreID string             `json:"policyStoreId"`
	PolicyID      string             `json:"policyId"`
}

func (h *Handler) handleUpdatePolicy(_ context.Context, in *updatePolicyInput) (*policyIDsOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.PolicyID == "" {
		return nil, fmt.Errorf("%w: policyId is required", errInvalidRequest)
	}

	if (in.Definition.Static == nil) == (in.Definition.TemplateLinked == nil) {
		return nil, fmt.Errorf("%w: definition must contain exactly one of static or templateLinked", errInvalidRequest)
	}

	var params UpdatePolicyParams

	if in.Definition.Static != nil {
		if in.Definition.Static.Statement == "" {
			return nil, fmt.Errorf("%w: definition.static.statement is required", errInvalidRequest)
		}

		params.Statement = in.Definition.Static.Statement
		params.Description = in.Definition.Static.Description
	} else {
		tl := in.Definition.TemplateLinked
		if tl.Principal != nil {
			params.PrincipalEntityType = tl.Principal.EntityType
			params.PrincipalEntityID = tl.Principal.EntityID
		}

		if tl.Resource != nil {
			params.ResourceEntityType = tl.Resource.EntityType
			params.ResourceEntityID = tl.Resource.EntityID
		}
	}

	p, err := h.Backend.UpdatePolicy(in.PolicyStoreID, in.PolicyID, params)
	if err != nil {
		return nil, err
	}

	return &policyIDsOutput{
		PolicyStoreID:   p.PolicyStoreID,
		PolicyID:        p.PolicyID,
		PolicyType:      p.PolicyType,
		CreatedDate:     p.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate: p.LastUpdated.UTC().Format(timeFormat),
	}, nil
}

func (h *Handler) handleDeletePolicy(_ context.Context, in *policyInput) (*struct{}, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.PolicyID == "" {
		return nil, fmt.Errorf("%w: policyId is required", errInvalidRequest)
	}

	if err := h.Backend.DeletePolicy(in.PolicyStoreID, in.PolicyID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- Policy Template operations ---

type createPolicyTemplateInput struct {
	PolicyStoreID string `json:"policyStoreId"`
	Description   string `json:"description"`
	Statement     string `json:"statement"`
}

type policyTemplateIDsOutput struct {
	PolicyStoreID    string `json:"policyStoreId"`
	PolicyTemplateID string `json:"policyTemplateId"`
	CreatedDate      string `json:"createdDate"`
	LastUpdatedDate  string `json:"lastUpdatedDate"`
}

func (h *Handler) handleCreatePolicyTemplate(
	_ context.Context,
	in *createPolicyTemplateInput,
) (*policyTemplateIDsOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.Statement == "" {
		return nil, fmt.Errorf("%w: statement is required", errInvalidRequest)
	}

	pt, err := h.Backend.CreatePolicyTemplate(in.PolicyStoreID, in.Description, in.Statement)
	if err != nil {
		return nil, err
	}

	return &policyTemplateIDsOutput{
		PolicyStoreID:    pt.PolicyStoreID,
		PolicyTemplateID: pt.PolicyTemplateID,
		CreatedDate:      pt.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate:  pt.LastUpdated.UTC().Format(timeFormat),
	}, nil
}

type policyTemplateInput struct {
	PolicyStoreID    string `json:"policyStoreId"`
	PolicyTemplateID string `json:"policyTemplateId"`
}

type policyTemplateView struct {
	PolicyStoreID    string `json:"policyStoreId"`
	PolicyTemplateID string `json:"policyTemplateId"`
	Description      string `json:"description"`
	Statement        string `json:"statement"`
	CreatedDate      string `json:"createdDate"`
	LastUpdatedDate  string `json:"lastUpdatedDate"`
}

type getPolicyTemplateOutput struct {
	PolicyStoreID    string `json:"policyStoreId"`
	PolicyTemplateID string `json:"policyTemplateId"`
	Description      string `json:"description"`
	Statement        string `json:"statement"`
	CreatedDate      string `json:"createdDate"`
	LastUpdatedDate  string `json:"lastUpdatedDate"`
}

func (h *Handler) handleGetPolicyTemplate(
	_ context.Context,
	in *policyTemplateInput,
) (*getPolicyTemplateOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.PolicyTemplateID == "" {
		return nil, fmt.Errorf("%w: policyTemplateId is required", errInvalidRequest)
	}

	pt, err := h.Backend.GetPolicyTemplate(in.PolicyStoreID, in.PolicyTemplateID)
	if err != nil {
		return nil, err
	}

	return &getPolicyTemplateOutput{
		PolicyStoreID:    pt.PolicyStoreID,
		PolicyTemplateID: pt.PolicyTemplateID,
		Description:      pt.Description,
		Statement:        pt.Statement,
		CreatedDate:      pt.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate:  pt.LastUpdated.UTC().Format(timeFormat),
	}, nil
}

type listPolicyTemplatesInput struct {
	PolicyStoreID string `json:"policyStoreId"`
	NextToken     string `json:"nextToken,omitempty"`
	MaxResults    int    `json:"maxResults,omitempty"`
}

type listPolicyTemplatesOutput struct {
	NextToken       string               `json:"nextToken,omitempty"`
	PolicyTemplates []policyTemplateView `json:"policyTemplates"`
}

func (h *Handler) handleListPolicyTemplates(
	_ context.Context,
	in *listPolicyTemplatesInput,
) (*listPolicyTemplatesOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	templates, nextToken, err := h.Backend.ListPolicyTemplates(in.PolicyStoreID, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	items := make([]policyTemplateView, 0, len(templates))

	for i := range templates {
		pt := &templates[i]
		items = append(items, policyTemplateView{
			PolicyStoreID:    pt.PolicyStoreID,
			PolicyTemplateID: pt.PolicyTemplateID,
			Description:      pt.Description,
			Statement:        pt.Statement,
			CreatedDate:      pt.CreatedDate.UTC().Format(timeFormat),
			LastUpdatedDate:  pt.LastUpdated.UTC().Format(timeFormat),
		})
	}

	return &listPolicyTemplatesOutput{PolicyTemplates: items, NextToken: nextToken}, nil
}

type updatePolicyTemplateInput struct {
	PolicyStoreID    string `json:"policyStoreId"`
	PolicyTemplateID string `json:"policyTemplateId"`
	Description      string `json:"description"`
	Statement        string `json:"statement"`
}

func (h *Handler) handleUpdatePolicyTemplate(
	_ context.Context,
	in *updatePolicyTemplateInput,
) (*policyTemplateIDsOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.PolicyTemplateID == "" {
		return nil, fmt.Errorf("%w: policyTemplateId is required", errInvalidRequest)
	}

	if in.Statement == "" {
		return nil, fmt.Errorf("%w: statement is required", errInvalidRequest)
	}

	pt, err := h.Backend.UpdatePolicyTemplate(in.PolicyStoreID, in.PolicyTemplateID, in.Description, in.Statement)
	if err != nil {
		return nil, err
	}

	return &policyTemplateIDsOutput{
		PolicyStoreID:    pt.PolicyStoreID,
		PolicyTemplateID: pt.PolicyTemplateID,
		CreatedDate:      pt.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate:  pt.LastUpdated.UTC().Format(timeFormat),
	}, nil
}

func (h *Handler) handleDeletePolicyTemplate(_ context.Context, in *policyTemplateInput) (*struct{}, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.PolicyTemplateID == "" {
		return nil, fmt.Errorf("%w: policyTemplateId is required", errInvalidRequest)
	}

	if err := h.Backend.DeletePolicyTemplate(in.PolicyStoreID, in.PolicyTemplateID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type tagResourceInput struct {
	Tags        map[string]string `json:"tags"`
	ResourceArn string            `json:"resourceArn"`
}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*struct{}, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(in.ResourceArn, in.Tags); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*struct{}, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}

// --- BatchGetPolicy ---

type batchGetPolicyRequest struct {
	Requests []struct {
		PolicyStoreID string `json:"policyStoreId"`
		PolicyID      string `json:"policyId"`
	} `json:"requests"`
}

type batchGetPolicyHandlerOutput struct {
	Results []batchGetPolicyOutputItem `json:"results"`
	Errors  []batchGetPolicyErrorItem  `json:"errors"`
}

func (h *Handler) handleBatchGetPolicy(
	_ context.Context,
	in *batchGetPolicyRequest,
) (*batchGetPolicyHandlerOutput, error) {
	items := make([]BatchGetPolicyItem, 0, len(in.Requests))

	for _, r := range in.Requests {
		if r.PolicyStoreID == "" || r.PolicyID == "" {
			return nil, fmt.Errorf("%w: each request requires policyStoreId and policyId", errInvalidRequest)
		}

		items = append(items, BatchGetPolicyItem{
			PolicyStoreID: r.PolicyStoreID,
			PolicyID:      r.PolicyID,
		})
	}

	result := h.Backend.BatchGetPolicy(items)

	return &batchGetPolicyHandlerOutput{
		Results: result.Results,
		Errors:  result.Errors,
	}, nil
}

// --- BatchIsAuthorized ---

type entityIdentifierJSON struct {
	EntityType string `json:"entityType"`
	EntityID   string `json:"entityId"`
}

type actionIdentifierJSON struct {
	ActionType string `json:"actionType"`
	ActionID   string `json:"actionId"`
}

type entityJSON struct {
	Identifier *entityIdentifierJSON      `json:"identifier,omitempty"`
	Attributes map[string]json.RawMessage `json:"attrs,omitempty"`
	Parents    []entityIdentifierJSON     `json:"parents,omitempty"`
}

type batchIsAuthorizedRequestItem struct {
	Principal *entityIdentifierJSON `json:"principal,omitempty"`
	Action    *actionIdentifierJSON `json:"action,omitempty"`
	Resource  *entityIdentifierJSON `json:"resource,omitempty"`
}

type batchIsAuthorizedInput struct {
	PolicyStoreID string                         `json:"policyStoreId"`
	Entities      []entityJSON                   `json:"entities,omitempty"`
	Requests      []batchIsAuthorizedRequestItem `json:"requests"`
}

type batchIsAuthorizedDecision struct {
	Request             AuthorizationRequest `json:"request"`
	Decision            string               `json:"decision"`
	DeterminingPolicies []string             `json:"determiningPolicies"`
	Errors              []string             `json:"errors"`
}

type batchIsAuthorizedOutput struct {
	Results []batchIsAuthorizedDecision `json:"results"`
}

func toBatchDecisions(decisions []AuthDecision) []batchIsAuthorizedDecision {
	out := make([]batchIsAuthorizedDecision, 0, len(decisions))

	for _, d := range decisions {
		out = append(out, batchIsAuthorizedDecision(d))
	}

	return out
}

func (h *Handler) handleBatchIsAuthorized(
	_ context.Context,
	in *batchIsAuthorizedInput,
) (*batchIsAuthorizedOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if len(in.Requests) > maxBatchRequests {
		return nil, fmt.Errorf(
			"%w: batch size %d exceeds maximum of %d",
			errInvalidRequest, len(in.Requests), maxBatchRequests,
		)
	}

	requests := make([]AuthorizationRequest, 0, len(in.Requests))

	for _, r := range in.Requests {
		req := AuthorizationRequest{}

		if r.Principal != nil {
			req.PrincipalEntityType = r.Principal.EntityType
			req.PrincipalEntityID = r.Principal.EntityID
		}

		if r.Action != nil {
			req.ActionType = r.Action.ActionType
			req.ActionID = r.Action.ActionID
		}

		if r.Resource != nil {
			req.ResourceEntityType = r.Resource.EntityType
			req.ResourceEntityID = r.Resource.EntityID
		}

		requests = append(requests, req)
	}

	decisions, err := h.Backend.BatchIsAuthorized(in.PolicyStoreID, requests)
	if err != nil {
		return nil, err
	}

	return &batchIsAuthorizedOutput{Results: toBatchDecisions(decisions)}, nil
}

// --- BatchIsAuthorizedWithToken ---

type batchIsAuthorizedWithTokenRequestItem struct {
	Action   *actionIdentifierJSON `json:"action,omitempty"`
	Resource *entityIdentifierJSON `json:"resource,omitempty"`
}

type batchIsAuthorizedWithTokenInput struct {
	PolicyStoreID string                                  `json:"policyStoreId"`
	AccessToken   string                                  `json:"accessToken,omitempty"`
	IdentityToken string                                  `json:"identityToken,omitempty"`
	Entities      []entityJSON                            `json:"entities,omitempty"`
	Requests      []batchIsAuthorizedWithTokenRequestItem `json:"requests"`
}

func (h *Handler) handleBatchIsAuthorizedWithToken(
	_ context.Context,
	in *batchIsAuthorizedWithTokenInput,
) (*batchIsAuthorizedOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.AccessToken == "" && in.IdentityToken == "" {
		return nil, fmt.Errorf("%w: accessToken or identityToken is required", errInvalidRequest)
	}

	if len(in.Requests) > maxBatchRequests {
		return nil, fmt.Errorf(
			"%w: batch size %d exceeds maximum of %d",
			errInvalidRequest, len(in.Requests), maxBatchRequests,
		)
	}

	requests := make([]AuthorizationRequest, 0, len(in.Requests))

	for _, r := range in.Requests {
		req := AuthorizationRequest{}

		if r.Action != nil {
			req.ActionType = r.Action.ActionType
			req.ActionID = r.Action.ActionID
		}

		if r.Resource != nil {
			req.ResourceEntityType = r.Resource.EntityType
			req.ResourceEntityID = r.Resource.EntityID
		}

		requests = append(requests, req)
	}

	decisions, err := h.Backend.BatchIsAuthorizedWithToken(in.PolicyStoreID, requests)
	if err != nil {
		return nil, err
	}

	return &batchIsAuthorizedOutput{Results: toBatchDecisions(decisions)}, nil
}

// --- IdentitySource operations ---

type cognitoGroupConfigJSON struct {
	GroupEntityType string `json:"groupEntityType,omitempty"`
}

type cognitoUserPoolConfigJSON struct {
	GroupConfiguration *cognitoGroupConfigJSON `json:"groupConfiguration,omitempty"`
	UserPoolArn        string                  `json:"userPoolArn"`
	ClientIDs          []string                `json:"clientIds,omitempty"`
}

type oidcGroupConfigJSON struct {
	GroupClaim      string `json:"groupClaim,omitempty"`
	GroupEntityType string `json:"groupEntityType,omitempty"`
}

type oidcTokenSelectionJSON struct {
	IdentityTokenOnly *oidcTokenOnlyJSON `json:"identityTokenOnly,omitempty"`
	AccessTokenOnly   *oidcTokenOnlyJSON `json:"accessTokenOnly,omitempty"`
}

type oidcTokenOnlyJSON struct {
	PrincipalIDClaim string   `json:"principalIdClaim,omitempty"`
	Audiences        []string `json:"audiences,omitempty"`
}

type openIDConnectConfigJSON struct {
	GroupConfiguration *oidcGroupConfigJSON    `json:"groupConfiguration,omitempty"`
	TokenSelection     *oidcTokenSelectionJSON `json:"tokenSelection,omitempty"`
	Issuer             string                  `json:"issuer"`
	EntityIDPrefix     string                  `json:"entityIdPrefix,omitempty"`
}

type identitySourceConfigJSON struct {
	CognitoUserPool *cognitoUserPoolConfigJSON `json:"cognitoUserPoolConfiguration,omitempty"`
	OpenIDConnect   *openIDConnectConfigJSON   `json:"openIdConnectConfiguration,omitempty"`
}

type createIdentitySourceInput struct {
	PolicyStoreID       string                   `json:"policyStoreId"`
	PrincipalEntityType string                   `json:"principalEntityType"`
	Configuration       identitySourceConfigJSON `json:"configuration"`
	ClientToken         string                   `json:"clientToken,omitempty"`
}

type identitySourceOutput struct {
	IdentitySourceID    string                    `json:"identitySourceId"`
	PolicyStoreID       string                    `json:"policyStoreId"`
	PrincipalEntityType string                    `json:"principalEntityType"`
	Configuration       *identitySourceConfigJSON `json:"configuration,omitempty"`
	CreatedDate         string                    `json:"createdDate"`
	LastUpdatedDate     string                    `json:"lastUpdatedDate"`
}

func identitySourceToConfigJSON(is *IdentitySource) *identitySourceConfigJSON {
	if is.UserPoolArn != "" {
		cfg := &identitySourceConfigJSON{
			CognitoUserPool: &cognitoUserPoolConfigJSON{
				UserPoolArn: is.UserPoolArn,
				ClientIDs:   is.ClientIDs,
			},
		}

		if is.CognitoGroupConfig != nil {
			cfg.CognitoUserPool.GroupConfiguration = &cognitoGroupConfigJSON{
				GroupEntityType: is.CognitoGroupConfig.GroupEntityType,
			}
		}

		return cfg
	}

	if is.OpenIDIssuer != "" {
		cfg := &identitySourceConfigJSON{
			OpenIDConnect: &openIDConnectConfigJSON{
				Issuer:         is.OpenIDIssuer,
				EntityIDPrefix: is.EntityIDPrefix,
			},
		}

		if is.OIDCGroupConfig != nil {
			cfg.OpenIDConnect.GroupConfiguration = &oidcGroupConfigJSON{
				GroupClaim:      is.OIDCGroupConfig.GroupClaim,
				GroupEntityType: is.OIDCGroupConfig.GroupEntityType,
			}
		}

		if is.OIDCTokenSelection != nil {
			sel := is.OIDCTokenSelection
			tok := &oidcTokenSelectionJSON{}

			switch sel.TokenType {
			case "IDENTITY":
				tok.IdentityTokenOnly = &oidcTokenOnlyJSON{
					PrincipalIDClaim: sel.PrincipalIDClaim,
					Audiences:        sel.Audiences,
				}
			case "ACCESS":
				tok.AccessTokenOnly = &oidcTokenOnlyJSON{
					PrincipalIDClaim: sel.PrincipalIDClaim,
					Audiences:        sel.Audiences,
				}
			}

			cfg.OpenIDConnect.TokenSelection = tok
		}

		return cfg
	}

	return nil
}

func identitySourceToOutput(is *IdentitySource) *identitySourceOutput {
	return &identitySourceOutput{
		IdentitySourceID:    is.IdentitySourceID,
		PolicyStoreID:       is.PolicyStoreID,
		PrincipalEntityType: is.PrincipalEntityType,
		Configuration:       identitySourceToConfigJSON(is),
		CreatedDate:         is.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate:     is.LastUpdated.UTC().Format(timeFormat),
	}
}

//nolint:nestif // identity source config union type
func configJSONToBackend(cfg identitySourceConfigJSON) IdentitySourceConfig {
	var out IdentitySourceConfig

	if cfg.CognitoUserPool != nil {
		out.UserPoolArn = cfg.CognitoUserPool.UserPoolArn
		out.ClientIDs = cfg.CognitoUserPool.ClientIDs

		if cfg.CognitoUserPool.GroupConfiguration != nil {
			out.CognitoGroupEntityType = cfg.CognitoUserPool.GroupConfiguration.GroupEntityType
		}
	} else if cfg.OpenIDConnect != nil {
		out.Issuer = cfg.OpenIDConnect.Issuer
		out.EntityIDPrefix = cfg.OpenIDConnect.EntityIDPrefix

		if cfg.OpenIDConnect.GroupConfiguration != nil {
			out.OIDCGroupClaim = cfg.OpenIDConnect.GroupConfiguration.GroupClaim
			out.OIDCGroupEntityType = cfg.OpenIDConnect.GroupConfiguration.GroupEntityType
		}

		if cfg.OpenIDConnect.TokenSelection != nil {
			if cfg.OpenIDConnect.TokenSelection.IdentityTokenOnly != nil {
				tok := cfg.OpenIDConnect.TokenSelection.IdentityTokenOnly
				out.TokenType = "IDENTITY"
				out.PrincipalIDClaim = tok.PrincipalIDClaim
				out.Audiences = tok.Audiences
			} else if cfg.OpenIDConnect.TokenSelection.AccessTokenOnly != nil {
				tok := cfg.OpenIDConnect.TokenSelection.AccessTokenOnly
				out.TokenType = "ACCESS"
				out.PrincipalIDClaim = tok.PrincipalIDClaim
				out.Audiences = tok.Audiences
			}
		}
	}

	return out
}

func (h *Handler) handleCreateIdentitySource(
	_ context.Context,
	in *createIdentitySourceInput,
) (*identitySourceOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.Configuration.CognitoUserPool == nil && in.Configuration.OpenIDConnect == nil {
		return nil, fmt.Errorf(
			"%w: configuration must contain cognitoUserPoolConfiguration or openIdConnectConfiguration",
			errInvalidRequest,
		)
	}

	if in.Configuration.CognitoUserPool != nil && in.Configuration.CognitoUserPool.UserPoolArn == "" {
		return nil, fmt.Errorf("%w: cognitoUserPoolConfiguration.userPoolArn is required", errInvalidRequest)
	}

	if in.Configuration.OpenIDConnect != nil && in.Configuration.OpenIDConnect.Issuer == "" {
		return nil, fmt.Errorf("%w: openIdConnectConfiguration.issuer is required", errInvalidRequest)
	}

	cfg := configJSONToBackend(in.Configuration)

	is, err := h.Backend.CreateIdentitySource(in.PolicyStoreID, in.PrincipalEntityType, cfg)
	if err != nil {
		return nil, err
	}

	return identitySourceToOutput(is), nil
}

type identitySourceIDInput struct {
	PolicyStoreID    string `json:"policyStoreId"`
	IdentitySourceID string `json:"identitySourceId"`
}

func (h *Handler) handleGetIdentitySource(
	_ context.Context,
	in *identitySourceIDInput,
) (*identitySourceOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.IdentitySourceID == "" {
		return nil, fmt.Errorf("%w: identitySourceId is required", errInvalidRequest)
	}

	is, err := h.Backend.GetIdentitySource(in.PolicyStoreID, in.IdentitySourceID)
	if err != nil {
		return nil, err
	}

	return identitySourceToOutput(is), nil
}

func (h *Handler) handleDeleteIdentitySource(_ context.Context, in *identitySourceIDInput) (*struct{}, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.IdentitySourceID == "" {
		return nil, fmt.Errorf("%w: identitySourceId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteIdentitySource(in.PolicyStoreID, in.IdentitySourceID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type listIdentitySourcesInput struct {
	PolicyStoreID string `json:"policyStoreId"`
	NextToken     string `json:"nextToken,omitempty"`
	MaxResults    int    `json:"maxResults,omitempty"`
}

type listIdentitySourcesOutput struct {
	NextToken       string                 `json:"nextToken,omitempty"`
	IdentitySources []identitySourceOutput `json:"identitySources"`
}

func (h *Handler) handleListIdentitySources(
	_ context.Context,
	in *listIdentitySourcesInput,
) (*listIdentitySourcesOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	sources, nextToken, err := h.Backend.ListIdentitySources(in.PolicyStoreID, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	items := make([]identitySourceOutput, 0, len(sources))

	for i := range sources {
		items = append(items, *identitySourceToOutput(&sources[i]))
	}

	return &listIdentitySourcesOutput{IdentitySources: items, NextToken: nextToken}, nil
}

// --- Schema operations ---

type putSchemaDefinitionJSON struct {
	CedarJSON string `json:"cedarJson"`
}

type putSchemaInput struct {
	PolicyStoreID string                  `json:"policyStoreId"`
	Definition    putSchemaDefinitionJSON `json:"definition"`
}

type putSchemaOutput struct {
	PolicyStoreID   string   `json:"policyStoreId"`
	CreatedDate     string   `json:"createdDate"`
	LastUpdatedDate string   `json:"lastUpdatedDate"`
	Namespaces      []string `json:"namespaces"`
}

func (h *Handler) handlePutSchema(_ context.Context, in *putSchemaInput) (*putSchemaOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.Definition.CedarJSON == "" {
		return nil, fmt.Errorf("%w: definition.cedarJson is required", errInvalidRequest)
	}

	namespaces, err := h.Backend.PutSchema(in.PolicyStoreID, in.Definition.CedarJSON)
	if err != nil {
		return nil, err
	}

	// Read back timestamps directly from stored schema.
	s, err := h.Backend.GetSchema(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	if namespaces == nil {
		namespaces = []string{}
	}

	return &putSchemaOutput{
		PolicyStoreID:   in.PolicyStoreID,
		CreatedDate:     s.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate: s.LastUpdated.UTC().Format(timeFormat),
		Namespaces:      namespaces,
	}, nil
}

type getSchemaInput struct {
	PolicyStoreID string `json:"policyStoreId"`
}

type getSchemaOutput struct {
	PolicyStoreID   string   `json:"policyStoreId"`
	Schema          string   `json:"schema"`
	CreatedDate     string   `json:"createdDate"`
	LastUpdatedDate string   `json:"lastUpdatedDate"`
	Namespaces      []string `json:"namespaces,omitempty"`
}

func (h *Handler) handleGetSchema(_ context.Context, in *getSchemaInput) (*getSchemaOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	s, err := h.Backend.GetSchema(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	return &getSchemaOutput{
		PolicyStoreID:   in.PolicyStoreID,
		Schema:          s.Schema,
		Namespaces:      s.Namespaces,
		CreatedDate:     s.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate: s.LastUpdated.UTC().Format(timeFormat),
	}, nil
}

// --- IsAuthorized ---

type isAuthorizedInput struct {
	Principal     *entityIdentifierJSON `json:"principal,omitempty"`
	Action        *actionIdentifierJSON `json:"action,omitempty"`
	Resource      *entityIdentifierJSON `json:"resource,omitempty"`
	PolicyStoreID string                `json:"policyStoreId"`
}

type isAuthorizedOutput struct {
	Decision            string   `json:"decision"`
	DeterminingPolicies []string `json:"determiningPolicies"`
	Errors              []string `json:"errors"`
}

func (h *Handler) handleIsAuthorized(_ context.Context, in *isAuthorizedInput) (*isAuthorizedOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	req := AuthorizationRequest{}

	if in.Principal != nil {
		req.PrincipalEntityType = in.Principal.EntityType
		req.PrincipalEntityID = in.Principal.EntityID
	}

	if in.Action != nil {
		req.ActionType = in.Action.ActionType
		req.ActionID = in.Action.ActionID
	}

	if in.Resource != nil {
		req.ResourceEntityType = in.Resource.EntityType
		req.ResourceEntityID = in.Resource.EntityID
	}

	decision, err := h.Backend.IsAuthorized(in.PolicyStoreID, req)
	if err != nil {
		return nil, err
	}

	return &isAuthorizedOutput{
		Decision:            decision.Decision,
		DeterminingPolicies: decision.DeterminingPolicies,
		Errors:              decision.Errors,
	}, nil
}

// --- IsAuthorizedWithToken ---

type isAuthorizedWithTokenInput struct {
	Action        *actionIdentifierJSON `json:"action,omitempty"`
	Resource      *entityIdentifierJSON `json:"resource,omitempty"`
	PolicyStoreID string                `json:"policyStoreId"`
	AccessToken   string                `json:"accessToken,omitempty"`
	IdentityToken string                `json:"identityToken,omitempty"`
}

func (h *Handler) handleIsAuthorizedWithToken(
	_ context.Context,
	in *isAuthorizedWithTokenInput,
) (*isAuthorizedOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.AccessToken == "" && in.IdentityToken == "" {
		return nil, fmt.Errorf("%w: accessToken or identityToken is required", errInvalidRequest)
	}

	req := AuthorizationRequest{}

	if in.Action != nil {
		req.ActionType = in.Action.ActionType
		req.ActionID = in.Action.ActionID
	}

	if in.Resource != nil {
		req.ResourceEntityType = in.Resource.EntityType
		req.ResourceEntityID = in.Resource.EntityID
	}

	decision, err := h.Backend.IsAuthorizedWithToken(in.PolicyStoreID, req)
	if err != nil {
		return nil, err
	}

	return &isAuthorizedOutput{
		Decision:            decision.Decision,
		DeterminingPolicies: decision.DeterminingPolicies,
		Errors:              decision.Errors,
	}, nil
}

// --- UpdateIdentitySource ---

type updateIdentitySourceInput struct {
	UpdateConfiguration identitySourceConfigJSON `json:"updateConfiguration"`
	PolicyStoreID       string                   `json:"policyStoreId"`
	IdentitySourceID    string                   `json:"identitySourceId"`
	PrincipalEntityType string                   `json:"principalEntityType,omitempty"`
}

type updateIdentitySourceOutput struct {
	IdentitySourceID    string `json:"identitySourceId"`
	PolicyStoreID       string `json:"policyStoreId"`
	PrincipalEntityType string `json:"principalEntityType"`
	CreatedDate         string `json:"createdDate"`
	LastUpdatedDate     string `json:"lastUpdatedDate"`
}

func (h *Handler) handleUpdateIdentitySource(
	_ context.Context,
	in *updateIdentitySourceInput,
) (*updateIdentitySourceOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.IdentitySourceID == "" {
		return nil, fmt.Errorf("%w: identitySourceId is required", errInvalidRequest)
	}

	if in.UpdateConfiguration.CognitoUserPool == nil && in.UpdateConfiguration.OpenIDConnect == nil {
		return nil, fmt.Errorf(
			"%w: updateConfiguration must contain cognitoUserPoolConfiguration or openIdConnectConfiguration",
			errInvalidRequest,
		)
	}

	if in.UpdateConfiguration.CognitoUserPool != nil && in.UpdateConfiguration.CognitoUserPool.UserPoolArn == "" {
		return nil, fmt.Errorf(
			"%w: updateConfiguration.cognitoUserPoolConfiguration.userPoolArn is required",
			errInvalidRequest,
		)
	}

	if in.UpdateConfiguration.OpenIDConnect != nil && in.UpdateConfiguration.OpenIDConnect.Issuer == "" {
		return nil, fmt.Errorf(
			"%w: updateConfiguration.openIdConnectConfiguration.issuer is required",
			errInvalidRequest,
		)
	}

	cfg := configJSONToBackend(in.UpdateConfiguration)

	is, err := h.Backend.UpdateIdentitySource(
		in.PolicyStoreID, in.IdentitySourceID, in.PrincipalEntityType, cfg,
	)
	if err != nil {
		return nil, err
	}

	return &updateIdentitySourceOutput{
		IdentitySourceID:    is.IdentitySourceID,
		PolicyStoreID:       is.PolicyStoreID,
		PrincipalEntityType: is.PrincipalEntityType,
		CreatedDate:         is.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate:     is.LastUpdated.UTC().Format(timeFormat),
	}, nil
}
