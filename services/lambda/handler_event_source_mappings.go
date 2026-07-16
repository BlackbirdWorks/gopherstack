package lambda

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// handleESMRoute dispatches event-source-mapping REST API requests.
func (h *Handler) handleESMRoute(c *echo.Context, path, method string) error {
	rest := strings.TrimPrefix(path, esmPathPrefix)
	// Remove leading slash
	rest = strings.TrimPrefix(rest, "/")

	switch {
	case method == http.MethodPost && rest == "":
		return h.handleCreateESM(c)
	case method == http.MethodGet && rest == "":
		return h.handleListESMs(c)
	case method == http.MethodGet && rest != "":
		return h.handleGetESM(c, rest)
	case method == http.MethodPut && rest != "":
		return h.handleUpdateESM(c, rest)
	case method == http.MethodDelete && rest != "":
		return h.handleDeleteESM(c, rest)
	default:
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

type handleCreateESMInput struct {
	Enabled                             *bool                                `json:"Enabled"`
	FilterCriteria                      *FilterCriteria                      `json:"FilterCriteria"`
	DestinationConfig                   *ESMDestinationConfig                `json:"DestinationConfig"`
	AmazonManagedKafkaEventSourceConfig *AmazonManagedKafkaEventSourceConfig `json:"AmazonManagedKafkaEventSourceConfig"`
	SelfManagedKafkaEventSourceConfig   *SelfManagedKafkaEventSourceConfig   `json:"SelfManagedKafkaEventSourceConfig"`
	SelfManagedEventSource              *SelfManagedEventSource              `json:"SelfManagedEventSource"`
	DocumentDBEventSourceConfig         *DocumentDBEventSourceConfig         `json:"DocumentDBEventSourceConfig"`
	EventSourceARN                      string                               `json:"EventSourceArn"`
	FunctionName                        string                               `json:"FunctionName"`
	StartingPosition                    string                               `json:"StartingPosition"`
	SourceAccessConfigurations          []SourceAccessConfiguration          `json:"SourceAccessConfigurations"`
	Topics                              []string                             `json:"Topics"`
	Queues                              []string                             `json:"Queues"`
	FunctionResponseTypes               []string                             `json:"FunctionResponseTypes"`
	BatchSize                           int                                  `json:"BatchSize"`
	MaximumBatchingWindowInSeconds      int                                  `json:"MaximumBatchingWindowInSeconds"`
	TumblingWindowInSeconds             int                                  `json:"TumblingWindowInSeconds"`
	MaximumRecordAgeInSeconds           int                                  `json:"MaximumRecordAgeInSeconds"`
	MaximumRetryAttempts                int                                  `json:"MaximumRetryAttempts"`
	ParallelizationFactor               int                                  `json:"ParallelizationFactor"`
	BisectBatchOnFunctionError          bool                                 `json:"BisectBatchOnFunctionError"`
}

// handleCreateESM handles POST /2015-03-31/event-source-mappings/.
func (h *Handler) handleCreateESM(c *echo.Context) error {
	if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok {
		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
		}

		var req handleCreateESMInput

		if err = json.Unmarshal(body, &req); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}

		enabled := req.Enabled == nil || *req.Enabled // default enabled=true

		m, err := lambdaBk.CreateEventSourceMapping(&CreateEventSourceMappingInput{
			EventSourceARN:                      req.EventSourceARN,
			FunctionName:                        req.FunctionName,
			StartingPosition:                    req.StartingPosition,
			BatchSize:                           req.BatchSize,
			Enabled:                             enabled,
			FilterCriteria:                      req.FilterCriteria,
			DestinationConfig:                   req.DestinationConfig,
			AmazonManagedKafkaEventSourceConfig: req.AmazonManagedKafkaEventSourceConfig,
			SelfManagedKafkaEventSourceConfig:   req.SelfManagedKafkaEventSourceConfig,
			SelfManagedEventSource:              req.SelfManagedEventSource,
			DocumentDBEventSourceConfig:         req.DocumentDBEventSourceConfig,
			SourceAccessConfigurations:          req.SourceAccessConfigurations,
			Topics:                              req.Topics,
			Queues:                              req.Queues,
			FunctionResponseTypes:               req.FunctionResponseTypes,
			MaximumBatchingWindowInSeconds:      req.MaximumBatchingWindowInSeconds,
			TumblingWindowInSeconds:             req.TumblingWindowInSeconds,
			MaximumRecordAgeInSeconds:           req.MaximumRecordAgeInSeconds,
			MaximumRetryAttempts:                req.MaximumRetryAttempts,
			ParallelizationFactor:               req.ParallelizationFactor,
			BisectBatchOnFunctionError:          req.BisectBatchOnFunctionError,
		})
		if err != nil {
			return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
		}

		return c.JSON(http.StatusCreated, toJSONESMResponse(m))
	}

	return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
}

// handleListESMs handles GET /2015-03-31/event-source-mappings/.
func (h *Handler) handleListESMs(c *echo.Context) error {
	if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok {
		q := c.Request().URL.Query()
		functionName := q.Get("FunctionName")
		eventSourceARN := q.Get("EventSourceArn")
		marker, maxItems := parsePaginationParams(c.Request())
		p := lambdaBk.ListEventSourceMappings(functionName, eventSourceARN, marker, maxItems)
		resp := make([]jsonESMResponse, len(p.Data))
		for i, m := range p.Data {
			resp[i] = toJSONESMResponse(m)
		}

		return c.JSON(http.StatusOK, jsonListESMResponse{EventSourceMappings: resp, NextMarker: p.Next})
	}

	return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
}

// handleGetESM handles GET /2015-03-31/event-source-mappings/{UUID}.
func (h *Handler) handleGetESM(c *echo.Context, id string) error {
	if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok {
		m, err := lambdaBk.GetEventSourceMapping(id)
		if err != nil {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "event source mapping not found")
		}

		return c.JSON(http.StatusOK, toJSONESMResponse(m))
	}

	return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
}

// handleDeleteESM handles DELETE /2015-03-31/event-source-mappings/{UUID}.
func (h *Handler) handleDeleteESM(c *echo.Context, id string) error {
	if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok {
		m, err := lambdaBk.DeleteEventSourceMapping(id)
		if err != nil {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "event source mapping not found")
		}

		return c.JSON(http.StatusOK, toJSONESMResponse(m))
	}

	return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
}

// handleUpdateESMInput is the request body for UpdateEventSourceMapping.
type handleUpdateESMInput struct {
	Enabled                        *bool                       `json:"Enabled"`
	FilterCriteria                 *FilterCriteria             `json:"FilterCriteria"`
	DestinationConfig              *ESMDestinationConfig       `json:"DestinationConfig"`
	BisectBatchOnFunctionError     *bool                       `json:"BisectBatchOnFunctionError"`
	SourceAccessConfigurations     []SourceAccessConfiguration `json:"SourceAccessConfigurations"`
	Topics                         []string                    `json:"Topics"`
	Queues                         []string                    `json:"Queues"`
	FunctionResponseTypes          []string                    `json:"FunctionResponseTypes"`
	BatchSize                      int                         `json:"BatchSize"`
	MaximumBatchingWindowInSeconds int                         `json:"MaximumBatchingWindowInSeconds"`
	TumblingWindowInSeconds        int                         `json:"TumblingWindowInSeconds"`
	MaximumRecordAgeInSeconds      int                         `json:"MaximumRecordAgeInSeconds"`
	MaximumRetryAttempts           int                         `json:"MaximumRetryAttempts"`
	ParallelizationFactor          int                         `json:"ParallelizationFactor"`
}

// handleUpdateESM handles PUT /2015-03-31/event-source-mappings/{UUID}.
func (h *Handler) handleUpdateESM(c *echo.Context, id string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var req handleUpdateESMInput
	if err = json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	m, err := lambdaBk.UpdateEventSourceMapping(id, &UpdateEventSourceMappingInput{
		Enabled:                        req.Enabled,
		BatchSize:                      req.BatchSize,
		FilterCriteria:                 req.FilterCriteria,
		DestinationConfig:              req.DestinationConfig,
		SourceAccessConfigurations:     req.SourceAccessConfigurations,
		Topics:                         req.Topics,
		Queues:                         req.Queues,
		FunctionResponseTypes:          req.FunctionResponseTypes,
		MaximumBatchingWindowInSeconds: req.MaximumBatchingWindowInSeconds,
		TumblingWindowInSeconds:        req.TumblingWindowInSeconds,
		MaximumRecordAgeInSeconds:      req.MaximumRecordAgeInSeconds,
		MaximumRetryAttempts:           req.MaximumRetryAttempts,
		ParallelizationFactor:          req.ParallelizationFactor,
		BisectBatchOnFunctionError:     req.BisectBatchOnFunctionError,
	})
	if err != nil {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "event source mapping not found")
	}

	return c.JSON(http.StatusOK, toJSONESMResponse(m))
}
