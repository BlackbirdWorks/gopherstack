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

const targetPrefix = "VerifiedPermissions."

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for Amazon Verified Permissions operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Verified Permissions handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
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
		"ListIdentitySources",
		"ListPolicies",
		"ListPolicyStores",
		"ListPolicyTemplates",
		"ListTagsForResource",
		"PutSchema",
		"TagResource",
		"UntagResource",
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

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
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
		"ListIdentitySources":        service.WrapOp(h.handleListIdentitySources),
		"ListPolicyStores":           service.WrapOp(h.handleListPolicyStores),
		"ListPolicies":               service.WrapOp(h.handleListPolicies),
		"ListPolicyTemplates":        service.WrapOp(h.handleListPolicyTemplates),
		"ListTagsForResource":        service.WrapOp(h.handleListTagsForResource),
		"PutSchema":                  service.WrapOp(h.handlePutSchema),
		"TagResource":                service.WrapOp(h.handleTagResource),
		"UntagResource":              service.WrapOp(h.handleUntagResource),
		"UpdatePolicyStore":          service.WrapOp(h.handleUpdatePolicyStore),
		"UpdatePolicy":               service.WrapOp(h.handleUpdatePolicy),
		"UpdatePolicyTemplate":       service.WrapOp(h.handleUpdatePolicyTemplate),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
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
			"__type":  "ResourceNotFoundException",
			"message": err.Error(),
		})
	case errors.Is(err, awserr.ErrConflict):
		return c.JSON(http.StatusBadRequest, map[string]string{
			"__type":  "ResourceConflictException",
			"message": err.Error(),
		})
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			"__type":  "ValidationException",
			"message": err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"__type":  "InternalServerException",
			"message": err.Error(),
		})
	}
}

// --- Policy Store operations ---

type createPolicyStoreInput struct {
	Tags        map[string]string `json:"tags"`
	Description string            `json:"description"`
}

type createPolicyStoreOutput struct {
	PolicyStoreID   string `json:"policyStoreId"`
	Arn             string `json:"arn"`
	CreatedDate     string `json:"createdDate"`
	LastUpdatedDate string `json:"lastUpdatedDate"`
}

func (h *Handler) handleCreatePolicyStore(
	_ context.Context,
	in *createPolicyStoreInput,
) (*createPolicyStoreOutput, error) {
	ps, err := h.Backend.CreatePolicyStore(in.Description, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createPolicyStoreOutput{
		PolicyStoreID:   ps.PolicyStoreID,
		Arn:             ps.Arn,
		CreatedDate:     ps.CreatedDate.UTC().Format("2006-01-02T15:04:05.000Z"),
		LastUpdatedDate: ps.LastUpdated.UTC().Format("2006-01-02T15:04:05.000Z"),
	}, nil
}

type policyStoreIDInput struct {
	PolicyStoreID string `json:"policyStoreId"`
}

type policyStoreView struct {
	PolicyStoreID   string `json:"policyStoreId"`
	Arn             string `json:"arn"`
	Description     string `json:"description"`
	CreatedDate     string `json:"createdDate"`
	LastUpdatedDate string `json:"lastUpdatedDate"`
}

type getPolicyStoreOutput struct {
	PolicyStoreID   string `json:"policyStoreId"`
	Arn             string `json:"arn"`
	Description     string `json:"description"`
	CreatedDate     string `json:"createdDate"`
	LastUpdatedDate string `json:"lastUpdatedDate"`
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
		PolicyStoreID:   ps.PolicyStoreID,
		Arn:             ps.Arn,
		Description:     ps.Description,
		CreatedDate:     ps.CreatedDate.UTC().Format("2006-01-02T15:04:05.000Z"),
		LastUpdatedDate: ps.LastUpdated.UTC().Format("2006-01-02T15:04:05.000Z"),
	}, nil
}

type listPolicyStoresOutput struct {
	PolicyStores []policyStoreView `json:"policyStores"`
}

func (h *Handler) handleListPolicyStores(_ context.Context, _ *struct{}) (*listPolicyStoresOutput, error) {
	stores := h.Backend.ListPolicyStores()
	items := make([]policyStoreView, 0, len(stores))

	for i := range stores {
		ps := &stores[i]
		items = append(items, policyStoreView{
			PolicyStoreID:   ps.PolicyStoreID,
			Arn:             ps.Arn,
			Description:     ps.Description,
			CreatedDate:     ps.CreatedDate.UTC().Format("2006-01-02T15:04:05.000Z"),
			LastUpdatedDate: ps.LastUpdated.UTC().Format("2006-01-02T15:04:05.000Z"),
		})
	}

	return &listPolicyStoresOutput{PolicyStores: items}, nil
}

type updatePolicyStoreInput struct {
	PolicyStoreID string `json:"policyStoreId"`
	Description   string `json:"description"`
}

type updatePolicyStoreOutput struct {
	PolicyStoreID   string `json:"policyStoreId"`
	Arn             string `json:"arn"`
	LastUpdatedDate string `json:"lastUpdatedDate"`
}

func (h *Handler) handleUpdatePolicyStore(
	_ context.Context,
	in *updatePolicyStoreInput,
) (*updatePolicyStoreOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	ps, err := h.Backend.UpdatePolicyStore(in.PolicyStoreID, in.Description)
	if err != nil {
		return nil, err
	}

	return &updatePolicyStoreOutput{
		PolicyStoreID:   ps.PolicyStoreID,
		Arn:             ps.Arn,
		LastUpdatedDate: ps.LastUpdated.UTC().Format("2006-01-02T15:04:05.000Z"),
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
	Description string `json:"description"`
}

type templateLinkedPolicyDefinition struct {
	PolicyTemplateID string `json:"policyTemplateId"`
}

type policyDefinition struct {
	Static         *staticPolicyDefinition         `json:"static,omitempty"`
	TemplateLinked *templateLinkedPolicyDefinition `json:"templateLinked,omitempty"`
}

type createPolicyInput struct {
	Definition    policyDefinition `json:"definition"`
	PolicyStoreID string           `json:"policyStoreId"`
}

type policyIDsOutput struct {
	PolicyStoreID   string `json:"policyStoreId"`
	PolicyID        string `json:"policyId"`
	PolicyType      string `json:"policyType"`
	CreatedDate     string `json:"createdDate"`
	LastUpdatedDate string `json:"lastUpdatedDate"`
}

func (h *Handler) handleCreatePolicy(_ context.Context, in *createPolicyInput) (*policyIDsOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if (in.Definition.Static == nil) == (in.Definition.TemplateLinked == nil) {
		return nil, fmt.Errorf("%w: definition must contain exactly one of static or templateLinked", errInvalidRequest)
	}

	policyType := "STATIC"

	var statement string

	if in.Definition.Static != nil {
		if in.Definition.Static.Statement == "" {
			return nil, fmt.Errorf("%w: definition.static.statement is required", errInvalidRequest)
		}

		statement = in.Definition.Static.Statement
	} else {
		if in.Definition.TemplateLinked.PolicyTemplateID == "" {
			return nil, fmt.Errorf("%w: definition.templateLinked.policyTemplateId is required", errInvalidRequest)
		}

		policyType = "TEMPLATE_LINKED"
		statement = in.Definition.TemplateLinked.PolicyTemplateID
	}

	p, err := h.Backend.CreatePolicy(in.PolicyStoreID, policyType, statement)
	if err != nil {
		return nil, err
	}

	return &policyIDsOutput{
		PolicyStoreID:   p.PolicyStoreID,
		PolicyID:        p.PolicyID,
		PolicyType:      p.PolicyType,
		CreatedDate:     p.CreatedDate.UTC().Format("2006-01-02T15:04:05.000Z"),
		LastUpdatedDate: p.LastUpdated.UTC().Format("2006-01-02T15:04:05.000Z"),
	}, nil
}

type policyInput struct {
	PolicyStoreID string `json:"policyStoreId"`
	PolicyID      string `json:"policyId"`
}

type policyView struct {
	PolicyStoreID   string `json:"policyStoreId"`
	PolicyID        string `json:"policyId"`
	PolicyType      string `json:"policyType"`
	CreatedDate     string `json:"createdDate"`
	LastUpdatedDate string `json:"lastUpdatedDate"`
}

type getPolicyOutput struct {
	PolicyStoreID   string `json:"policyStoreId"`
	PolicyID        string `json:"policyId"`
	PolicyType      string `json:"policyType"`
	CreatedDate     string `json:"createdDate"`
	LastUpdatedDate string `json:"lastUpdatedDate"`
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

	return &getPolicyOutput{
		PolicyStoreID:   p.PolicyStoreID,
		PolicyID:        p.PolicyID,
		PolicyType:      p.PolicyType,
		CreatedDate:     p.CreatedDate.UTC().Format("2006-01-02T15:04:05.000Z"),
		LastUpdatedDate: p.LastUpdated.UTC().Format("2006-01-02T15:04:05.000Z"),
	}, nil
}

type listPoliciesInput struct {
	PolicyStoreID string `json:"policyStoreId"`
}

type listPoliciesOutput struct {
	Policies []policyView `json:"policies"`
}

func (h *Handler) handleListPolicies(_ context.Context, in *listPoliciesInput) (*listPoliciesOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	policies, err := h.Backend.ListPolicies(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	items := make([]policyView, 0, len(policies))

	for i := range policies {
		p := &policies[i]
		items = append(items, policyView{
			PolicyStoreID:   p.PolicyStoreID,
			PolicyID:        p.PolicyID,
			PolicyType:      p.PolicyType,
			CreatedDate:     p.CreatedDate.UTC().Format("2006-01-02T15:04:05.000Z"),
			LastUpdatedDate: p.LastUpdated.UTC().Format("2006-01-02T15:04:05.000Z"),
		})
	}

	return &listPoliciesOutput{Policies: items}, nil
}

type updatePolicyInput struct {
	Definition    policyDefinition `json:"definition"`
	PolicyStoreID string           `json:"policyStoreId"`
	PolicyID      string           `json:"policyId"`
}

func (h *Handler) handleUpdatePolicy(_ context.Context, in *updatePolicyInput) (*policyIDsOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.PolicyID == "" {
		return nil, fmt.Errorf("%w: policyId is required", errInvalidRequest)
	}

	if in.Definition.TemplateLinked != nil {
		return nil, fmt.Errorf("%w: updating TEMPLATE_LINKED policies is not supported", errInvalidRequest)
	}

	if in.Definition.Static == nil || in.Definition.Static.Statement == "" {
		return nil, fmt.Errorf("%w: definition.static.statement is required", errInvalidRequest)
	}

	p, err := h.Backend.UpdatePolicy(in.PolicyStoreID, in.PolicyID, in.Definition.Static.Statement)
	if err != nil {
		return nil, err
	}

	return &policyIDsOutput{
		PolicyStoreID:   p.PolicyStoreID,
		PolicyID:        p.PolicyID,
		PolicyType:      p.PolicyType,
		CreatedDate:     p.CreatedDate.UTC().Format("2006-01-02T15:04:05.000Z"),
		LastUpdatedDate: p.LastUpdated.UTC().Format("2006-01-02T15:04:05.000Z"),
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
		CreatedDate:      pt.CreatedDate.UTC().Format("2006-01-02T15:04:05.000Z"),
		LastUpdatedDate:  pt.LastUpdated.UTC().Format("2006-01-02T15:04:05.000Z"),
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
		CreatedDate:      pt.CreatedDate.UTC().Format("2006-01-02T15:04:05.000Z"),
		LastUpdatedDate:  pt.LastUpdated.UTC().Format("2006-01-02T15:04:05.000Z"),
	}, nil
}

type listPolicyTemplatesInput struct {
	PolicyStoreID string `json:"policyStoreId"`
}

type listPolicyTemplatesOutput struct {
	PolicyTemplates []policyTemplateView `json:"policyTemplates"`
}

func (h *Handler) handleListPolicyTemplates(
	_ context.Context,
	in *listPolicyTemplatesInput,
) (*listPolicyTemplatesOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	templates, err := h.Backend.ListPolicyTemplates(in.PolicyStoreID)
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
			CreatedDate:      pt.CreatedDate.UTC().Format("2006-01-02T15:04:05.000Z"),
			LastUpdatedDate:  pt.LastUpdated.UTC().Format("2006-01-02T15:04:05.000Z"),
		})
	}

	return &listPolicyTemplatesOutput{PolicyTemplates: items}, nil
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
		CreatedDate:      pt.CreatedDate.UTC().Format("2006-01-02T15:04:05.000Z"),
		LastUpdatedDate:  pt.LastUpdated.UTC().Format("2006-01-02T15:04:05.000Z"),
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

type batchIsAuthorizedRequestItem struct {
	Principal *entityIdentifierJSON `json:"principal,omitempty"`
	Action    *actionIdentifierJSON `json:"action,omitempty"`
	Resource  *entityIdentifierJSON `json:"resource,omitempty"`
}

type batchIsAuthorizedInput struct {
	PolicyStoreID string                         `json:"policyStoreId"`
	Requests      []batchIsAuthorizedRequestItem `json:"requests"`
}

type batchIsAuthorizedDecision struct {
	Decision string               `json:"decision"`
	Request  AuthorizationRequest `json:"request"`
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

type cognitoUserPoolConfigJSON struct {
	UserPoolArn string   `json:"userPoolArn"`
	ClientIDs   []string `json:"clientIds,omitempty"`
}

type openIDConnectConfigJSON struct {
	Issuer string `json:"issuer"`
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
	IdentitySourceID    string `json:"identitySourceId"`
	PolicyStoreID       string `json:"policyStoreId"`
	PrincipalEntityType string `json:"principalEntityType"`
	CreatedDate         string `json:"createdDate"`
	LastUpdatedDate     string `json:"lastUpdatedDate"`
}

func identitySourceToOutput(is *IdentitySource) *identitySourceOutput {
	return &identitySourceOutput{
		IdentitySourceID:    is.IdentitySourceID,
		PolicyStoreID:       is.PolicyStoreID,
		PrincipalEntityType: is.PrincipalEntityType,
		CreatedDate:         is.CreatedDate.UTC().Format("2006-01-02T15:04:05.000Z"),
		LastUpdatedDate:     is.LastUpdated.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
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

	var userPoolArn, openIDIssuer string

	var clientIDs []string

	if in.Configuration.CognitoUserPool != nil {
		if in.Configuration.CognitoUserPool.UserPoolArn == "" {
			return nil, fmt.Errorf("%w: cognitoUserPoolConfiguration.userPoolArn is required", errInvalidRequest)
		}

		userPoolArn = in.Configuration.CognitoUserPool.UserPoolArn
		clientIDs = in.Configuration.CognitoUserPool.ClientIDs
	} else {
		if in.Configuration.OpenIDConnect.Issuer == "" {
			return nil, fmt.Errorf("%w: openIdConnectConfiguration.issuer is required", errInvalidRequest)
		}

		openIDIssuer = in.Configuration.OpenIDConnect.Issuer
	}

	is, err := h.Backend.CreateIdentitySource(
		in.PolicyStoreID,
		userPoolArn,
		openIDIssuer,
		in.PrincipalEntityType,
		clientIDs,
	)
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
}

type listIdentitySourcesOutput struct {
	IdentitySources []identitySourceOutput `json:"identitySources"`
}

func (h *Handler) handleListIdentitySources(
	_ context.Context,
	in *listIdentitySourcesInput,
) (*listIdentitySourcesOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	sources, err := h.Backend.ListIdentitySources(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	items := make([]identitySourceOutput, 0, len(sources))

	for i := range sources {
		items = append(items, *identitySourceToOutput(&sources[i]))
	}

	return &listIdentitySourcesOutput{IdentitySources: items}, nil
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

	if err := h.Backend.PutSchema(in.PolicyStoreID, in.Definition.CedarJSON); err != nil {
		return nil, err
	}

	s, err := h.Backend.GetSchema(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	return &putSchemaOutput{
		PolicyStoreID:   in.PolicyStoreID,
		CreatedDate:     s.CreatedDate.UTC().Format("2006-01-02T15:04:05.000Z"),
		LastUpdatedDate: s.LastUpdated.UTC().Format("2006-01-02T15:04:05.000Z"),
		Namespaces:      []string{},
	}, nil
}

type getSchemaInput struct {
	PolicyStoreID string `json:"policyStoreId"`
}

type getSchemaOutput struct {
	PolicyStoreID   string `json:"policyStoreId"`
	Schema          string `json:"schema"`
	CreatedDate     string `json:"createdDate"`
	LastUpdatedDate string `json:"lastUpdatedDate"`
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
		CreatedDate:     s.CreatedDate.UTC().Format("2006-01-02T15:04:05.000Z"),
		LastUpdatedDate: s.LastUpdated.UTC().Format("2006-01-02T15:04:05.000Z"),
	}, nil
}
