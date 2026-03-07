package iot

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	iotMatchPriority = 90
	unknownOperation = "Unknown"
)

// Handler is the Echo HTTP handler for IoT control-plane operations.
type Handler struct {
	Backend StorageBackend
	broker  *Broker
}

// NewHandler creates a new IoT Handler.
func NewHandler(backend StorageBackend, broker *Broker) *Handler {
	return &Handler{Backend: backend, broker: broker}
}

// Broker returns the embedded MQTT broker (used for cross-service wiring).
func (h *Handler) Broker() *Broker { return h.broker }

// Name returns the service name.
func (h *Handler) Name() string { return "IoT" }

// GetSupportedOperations returns the list of supported IoT control-plane operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateThing",
		"DescribeThing",
		"DeleteThing",
		"CreateTopicRule",
		"GetTopicRule",
		"DeleteTopicRule",
		"AttachPrincipalPolicy",
		"CreatePolicy",
		"DescribeEndpoint",
	}
}

// RouteMatcher returns a function matching IoT control-plane requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, "/things/") ||
			strings.HasPrefix(path, "/rules/") ||
			strings.HasPrefix(path, "/target-policies/") ||
			strings.HasPrefix(path, "/policies/") ||
			path == "/endpoint"
	}
}

// MatchPriority returns the routing priority for the IoT handler.
func (h *Handler) MatchPriority() int { return iotMatchPriority }

// ExtractOperation extracts the IoT operation name from the request method + path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return resolveOperation(c.Request().URL.Path, c.Request().Method)
}

// maxPathSegments is used to split the path into at most 2 segments.
const maxPathSegments = 2

// ExtractResource extracts the resource name (thing/rule/policy) from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	for _, prefix := range []string{"/things/", "/rules/", "/policies/", "/target-policies/"} {
		if after, ok := strings.CutPrefix(path, prefix); ok {
			return strings.SplitN(after, "/", maxPathSegments)[0]
		}
	}

	return ""
}

// StartWorker starts the embedded MQTT broker as a background worker.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.broker == nil {
		return nil
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "starting IoT MQTT broker", "port", h.broker.port)

	go func() {
		if err := h.broker.Start(ctx); err != nil {
			log.ErrorContext(ctx, "IoT MQTT broker stopped", "error", err)
		}
	}()

	return nil
}

func resolveOperation(path, method string) string {
	switch {
	case strings.HasPrefix(path, "/things/"):
		return thingOperation(method)
	case strings.HasPrefix(path, "/rules/"):
		return ruleOperation(method)
	case strings.HasPrefix(path, "/target-policies/") && method == http.MethodPost:
		return "AttachPrincipalPolicy"
	case strings.HasPrefix(path, "/policies/") && method == http.MethodPost:
		return "CreatePolicy"
	case path == "/endpoint" && method == http.MethodGet:
		return "DescribeEndpoint"
	}

	return unknownOperation
}

func thingOperation(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateThing"
	case http.MethodGet:
		return "DescribeThing"
	case http.MethodDelete:
		return "DeleteThing"
	}

	return unknownOperation
}

func ruleOperation(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateTopicRule"
	case http.MethodGet:
		return "GetTopicRule"
	case http.MethodDelete:
		return "DeleteTopicRule"
	}

	return unknownOperation
}

// Handler returns the Echo handler function for IoT operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())
		op := resolveOperation(c.Request().URL.Path, c.Request().Method)

		log.Debug("iot request", "operation", op, "path", c.Request().URL.Path)

		switch op {
		case "CreateThing":
			return h.handleCreateThing(c)
		case "DescribeThing":
			return h.handleDescribeThing(c)
		case "DeleteThing":
			return h.handleDeleteThing(c)
		case "CreateTopicRule":
			return h.handleCreateTopicRule(c)
		case "GetTopicRule":
			return h.handleGetTopicRule(c)
		case "DeleteTopicRule":
			return h.handleDeleteTopicRule(c)
		case "AttachPrincipalPolicy":
			return h.handleAttachPrincipalPolicy(c)
		case "CreatePolicy":
			return h.handleCreatePolicy(c)
		case "DescribeEndpoint":
			return h.handleDescribeEndpoint(c)
		}

		return c.JSON(http.StatusBadRequest, map[string]string{"error": "unknown operation: " + op})
	}
}

func (h *Handler) handleCreateThing(c *echo.Context) error {
	thingName := strings.TrimPrefix(c.Request().URL.Path, "/things/")

	var body struct {
		AttributePayload *AttributePayload `json:"attributePayload"`
		ThingTypeName    string            `json:"thingTypeName"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	out, err := h.Backend.CreateThing(&CreateThingInput{
		ThingName:        thingName,
		ThingTypeName:    body.ThingTypeName,
		AttributePayload: body.AttributePayload,
	})
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"thingName": out.ThingName,
		"thingArn":  out.ThingARN,
		"thingId":   out.ThingID,
	})
}

func (h *Handler) handleDescribeThing(c *echo.Context) error {
	thingName := strings.TrimPrefix(c.Request().URL.Path, "/things/")

	t, err := h.Backend.DescribeThing(thingName)
	if err != nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"thingName":  t.ThingName,
		"thingArn":   t.ARN,
		"thingType":  t.ThingType,
		"attributes": t.Attributes,
		"version":    t.Version,
	})
}

func (h *Handler) handleDeleteThing(c *echo.Context) error {
	thingName := strings.TrimPrefix(c.Request().URL.Path, "/things/")

	if err := h.Backend.DeleteThing(thingName); err != nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateTopicRule(c *echo.Context) error {
	ruleName := strings.TrimPrefix(c.Request().URL.Path, "/rules/")

	var payload TopicRulePayload

	if err := json.NewDecoder(c.Request().Body).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	if err := h.Backend.CreateTopicRule(&CreateTopicRuleInput{
		RuleName:         ruleName,
		TopicRulePayload: &payload,
	}); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetTopicRule(c *echo.Context) error {
	ruleName := strings.TrimPrefix(c.Request().URL.Path, "/rules/")

	r, err := h.Backend.GetTopicRule(ruleName)
	if err != nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ruleName":     r.RuleName,
		"sql":          r.SQL,
		"description":  r.Description,
		"actions":      r.Actions,
		"ruleDisabled": !r.Enabled,
		"createdAt":    r.CreatedAt,
	})
}

func (h *Handler) handleDeleteTopicRule(c *echo.Context) error {
	ruleName := strings.TrimPrefix(c.Request().URL.Path, "/rules/")

	if err := h.Backend.DeleteTopicRule(ruleName); err != nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleAttachPrincipalPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/target-policies/")
	principal := c.Request().Header.Get("X-Amzn-Iot-Thingname")

	if err := h.Backend.AttachPrincipalPolicy(&AttachPrincipalPolicyInput{
		PolicyName: policyName,
		Principal:  principal,
	}); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleCreatePolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/policies/")

	var body struct {
		PolicyDocument string `json:"policyDocument"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	out, err := h.Backend.CreatePolicy(&CreatePolicyInput{
		PolicyName:     policyName,
		PolicyDocument: body.PolicyDocument,
	})
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"policyName":     out.PolicyName,
		"policyArn":      out.PolicyARN,
		"policyDocument": out.PolicyDocument,
	})
}

func (h *Handler) handleDescribeEndpoint(c *echo.Context) error {
	endpointType := c.QueryParam("endpointType")

	out, err := h.Backend.DescribeEndpoint(endpointType)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"endpointAddress": out.EndpointAddress,
	})
}
