package bedrock

import (
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

func extractMarketplaceEndpointOperation(path, method string) (string, bool) {
	if path == marketplaceEndpointsPrefix {
		return extractMarketplaceEndpointRootOp(method)
	}

	if !strings.HasPrefix(path, marketplaceEndpointsPrefix+"/") {
		return "", false
	}

	return extractMarketplaceEndpointSubOp(path, method)
}

func extractMarketplaceEndpointRootOp(method string) (string, bool) {
	switch method {
	case http.MethodPost:
		return "CreateMarketplaceModelEndpoint", true
	case http.MethodGet:
		return "ListMarketplaceModelEndpoints", true
	default:
		return "", false
	}
}

// extractMarketplaceEndpointSubOp maps /marketplace-model/endpoints/{id}[/registration]
// sub-paths. AWS uses the SAME "/registration" suffix for both Register (POST) and
// Deregister (DELETE) — there is no separate "/deregistration" path — and
// UpdateMarketplaceModelEndpoint uses PATCH, not PUT.
func extractMarketplaceEndpointSubOp(path, method string) (string, bool) {
	isReg := strings.HasSuffix(path, "/registration")

	switch {
	case method == http.MethodPost && isReg:
		return "RegisterMarketplaceModelEndpoint", true
	case method == http.MethodDelete && isReg:
		return "DeregisterMarketplaceModelEndpoint", true
	case method == http.MethodGet && !isReg:
		return "GetMarketplaceModelEndpoint", true
	case method == http.MethodPatch && !isReg:
		return "UpdateMarketplaceModelEndpoint", true
	case method == http.MethodDelete && !isReg:
		return "DeleteMarketplaceModelEndpoint", true
	default:
		return "", false
	}
}

func (h *Handler) routeMarketplaceEndpoint(
	c *echo.Context,
	path, method string,
	body []byte,
) (bool, error) {
	if path == marketplaceEndpointsPrefix {
		return h.routeMarketplaceEndpointRoot(c, method, body)
	}

	if !strings.HasPrefix(path, marketplaceEndpointsPrefix+"/") {
		return false, nil
	}

	return h.routeMarketplaceEndpointSub(c, path, method, body)
}

func (h *Handler) routeMarketplaceEndpointRoot(
	c *echo.Context, method string, body []byte,
) (bool, error) {
	switch method {
	case http.MethodPost:
		return true, h.handleCreateMarketplaceModelEndpoint(c, body)
	case http.MethodGet:
		return true, h.handleListMarketplaceModelEndpoints(c)
	default:
		return false, nil
	}
}

// routeMarketplaceEndpointSub routes /marketplace-model/endpoints/{id}[/registration].
// AWS reuses the SAME "/registration" suffix for both Register (POST) and Deregister
// (DELETE) — there is no "/deregistration" path — and Update uses PATCH, not PUT.
func (h *Handler) routeMarketplaceEndpointSub(c *echo.Context, path, method string, body []byte) (bool, error) {
	rest := strings.TrimPrefix(path, marketplaceEndpointsPrefix+"/")
	isReg := strings.HasSuffix(path, "/registration")

	switch {
	case method == http.MethodPost && isReg:
		id := decodePath(strings.TrimSuffix(rest, "/registration"))

		return true, h.handleRegisterMarketplaceModelEndpoint(c, id)
	case method == http.MethodDelete && isReg:
		id := decodePath(strings.TrimSuffix(rest, "/registration"))

		return true, h.handleDeregisterMarketplaceModelEndpoint(c, id)
	case method == http.MethodGet && !isReg:
		return true, h.handleGetMarketplaceModelEndpoint(c, decodePath(rest))
	case method == http.MethodPatch && !isReg:
		return true, h.handleUpdateMarketplaceModelEndpoint(c, decodePath(rest), body)
	case method == http.MethodDelete && !isReg:
		return true, h.handleDeleteMarketplaceModelEndpoint(c, decodePath(rest))
	default:
		return false, nil
	}
}

// endpointConfigWire is the wire shape of the real EndpointConfig union, whose
// sole member today is "sageMaker" (types.EndpointConfigMemberSageMaker).
type endpointConfigWire struct {
	SageMaker *sageMakerEndpointConfigWire `json:"sageMaker,omitempty"`
}

type sageMakerEndpointConfigWire struct {
	ExecutionRole        string `json:"executionRole"`
	InstanceType         string `json:"instanceType"`
	KmsEncryptionKey     string `json:"kmsEncryptionKey,omitempty"`
	InitialInstanceCount int32  `json:"initialInstanceCount"`
}

func endpointConfigFromWire(w *endpointConfigWire) *SageMakerEndpointConfig {
	if w == nil || w.SageMaker == nil {
		return nil
	}

	return &SageMakerEndpointConfig{
		ExecutionRole:        w.SageMaker.ExecutionRole,
		InitialInstanceCount: w.SageMaker.InitialInstanceCount,
		InstanceType:         w.SageMaker.InstanceType,
		KmsEncryptionKey:     w.SageMaker.KmsEncryptionKey,
	}
}

func endpointConfigToWire(cfg *SageMakerEndpointConfig) *endpointConfigWire {
	if cfg == nil {
		return nil
	}

	return &endpointConfigWire{
		SageMaker: &sageMakerEndpointConfigWire{
			ExecutionRole:        cfg.ExecutionRole,
			InitialInstanceCount: cfg.InitialInstanceCount,
			InstanceType:         cfg.InstanceType,
			KmsEncryptionKey:     cfg.KmsEncryptionKey,
		},
	}
}

type createMarketplaceModelEndpointInput struct {
	EndpointConfig        *endpointConfigWire `json:"endpointConfig,omitempty"`
	EndpointName          string              `json:"endpointName"`
	ModelSourceIdentifier string              `json:"modelSourceIdentifier"`
	Tags                  []Tag               `json:"tags,omitempty"`
}

type updateMarketplaceModelEndpointInput struct {
	EndpointConfig *endpointConfigWire `json:"endpointConfig,omitempty"`
}

type createMarketplaceModelEndpointOutput struct {
	MarketplaceModelEndpoint marketplaceEndpointOutput `json:"marketplaceModelEndpoint"`
}

type marketplaceEndpointOutput struct {
	EndpointConfig        *endpointConfigWire `json:"endpointConfig,omitempty"`
	CreatedAt             string              `json:"createdAt"`
	UpdatedAt             string              `json:"updatedAt"`
	EndpointArn           string              `json:"endpointArn"`
	EndpointName          string              `json:"endpointName"`
	ModelSourceIdentifier string              `json:"modelSourceIdentifier"`
	Status                string              `json:"status"`
}

func marketplaceEndpointToOutput(ep *MarketplaceModelEndpoint) marketplaceEndpointOutput {
	return marketplaceEndpointOutput{
		EndpointArn:           ep.EndpointArn,
		EndpointName:          ep.EndpointName,
		ModelSourceIdentifier: ep.ModelSourceID,
		Status:                ep.Status,
		EndpointConfig:        endpointConfigToWire(ep.EndpointConfig),
		CreatedAt:             ep.CreatedAt.Format(time.RFC3339),
		UpdatedAt:             ep.UpdatedAt.Format(time.RFC3339),
	}
}

func (h *Handler) handleCreateMarketplaceModelEndpoint(c *echo.Context, body []byte) error {
	in, err := parseBody[createMarketplaceModelEndpointInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	ep, opErr := h.Backend.CreateMarketplaceModelEndpoint(
		in.EndpointName,
		in.ModelSourceIdentifier,
		endpointConfigFromWire(in.EndpointConfig),
		in.Tags,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, createMarketplaceModelEndpointOutput{
		MarketplaceModelEndpoint: marketplaceEndpointToOutput(ep),
	})
}

func (h *Handler) handleGetMarketplaceModelEndpoint(c *echo.Context, id string) error {
	ep, err := h.Backend.GetMarketplaceModelEndpoint(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, marketplaceEndpointToOutput(ep))
}

type listMarketplaceModelEndpointsOutput struct {
	NextToken                 string                      `json:"nextToken,omitempty"`
	MarketplaceModelEndpoints []marketplaceEndpointOutput `json:"marketplaceModelEndpoints"`
}

func (h *Handler) handleListMarketplaceModelEndpoints(c *echo.Context) error {
	nextToken := c.Request().URL.Query().Get("nextToken")
	endpoints, outToken := h.Backend.ListMarketplaceModelEndpoints(nextToken)
	summaries := make([]marketplaceEndpointOutput, 0, len(endpoints))

	for _, ep := range endpoints {
		summaries = append(summaries, marketplaceEndpointToOutput(ep))
	}

	return c.JSON(http.StatusOK, listMarketplaceModelEndpointsOutput{
		MarketplaceModelEndpoints: summaries,
		NextToken:                 outToken,
	})
}

func (h *Handler) handleDeleteMarketplaceModelEndpoint(c *echo.Context, id string) error {
	if err := h.Backend.DeleteMarketplaceModelEndpoint(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUpdateMarketplaceModelEndpoint(c *echo.Context, id string, body []byte) error {
	in, parseErr := parseBody[updateMarketplaceModelEndpointInput](body)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	ep, err := h.Backend.UpdateMarketplaceModelEndpoint(id, endpointConfigFromWire(in.EndpointConfig))
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, marketplaceEndpointToOutput(ep))
}

func (h *Handler) handleRegisterMarketplaceModelEndpoint(c *echo.Context, id string) error {
	if err := h.Backend.RegisterMarketplaceModelEndpoint(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeregisterMarketplaceModelEndpoint(c *echo.Context, id string) error {
	if err := h.Backend.DeregisterMarketplaceModelEndpoint(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
