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
		"CreatePolicyStore",
		"GetPolicyStore",
		"ListPolicyStores",
		"UpdatePolicyStore",
		"DeletePolicyStore",
		"CreatePolicy",
		"GetPolicy",
		"ListPolicies",
		"UpdatePolicy",
		"DeletePolicy",
		"CreatePolicyTemplate",
		"GetPolicyTemplate",
		"ListPolicyTemplates",
		"UpdatePolicyTemplate",
		"DeletePolicyTemplate",
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
		"CreatePolicyStore":    service.WrapOp(h.handleCreatePolicyStore),
		"GetPolicyStore":       service.WrapOp(h.handleGetPolicyStore),
		"ListPolicyStores":     service.WrapOp(h.handleListPolicyStores),
		"UpdatePolicyStore":    service.WrapOp(h.handleUpdatePolicyStore),
		"DeletePolicyStore":    service.WrapOp(h.handleDeletePolicyStore),
		"CreatePolicy":         service.WrapOp(h.handleCreatePolicy),
		"GetPolicy":            service.WrapOp(h.handleGetPolicy),
		"ListPolicies":         service.WrapOp(h.handleListPolicies),
		"UpdatePolicy":         service.WrapOp(h.handleUpdatePolicy),
		"DeletePolicy":         service.WrapOp(h.handleDeletePolicy),
		"CreatePolicyTemplate": service.WrapOp(h.handleCreatePolicyTemplate),
		"GetPolicyTemplate":    service.WrapOp(h.handleGetPolicyTemplate),
		"ListPolicyTemplates":  service.WrapOp(h.handleListPolicyTemplates),
		"UpdatePolicyTemplate": service.WrapOp(h.handleUpdatePolicyTemplate),
		"DeletePolicyTemplate": service.WrapOp(h.handleDeletePolicyTemplate),
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

	policyType := "STATIC"
	statement := ""

	if in.Definition.Static != nil {
		statement = in.Definition.Static.Statement
	} else if in.Definition.TemplateLinked != nil {
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

	statement := ""
	if in.Definition.Static != nil {
		statement = in.Definition.Static.Statement
	}

	p, err := h.Backend.UpdatePolicy(in.PolicyStoreID, in.PolicyID, statement)
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
