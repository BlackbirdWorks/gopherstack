package awsconfig

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	awsConfigTargetPrefix  = "StarlingDoveService."
	awsConfigMatchPriority = 100
)

// Handler is the Echo HTTP handler for AWS Config operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
}

// NewHandler creates a new AWS Config handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

// Name returns the service name.
func (h *Handler) Name() string { return "AWSConfig" }

// GetSupportedOperations returns the list of supported AWS Config operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"PutConfigurationRecorder",
		"DescribeConfigurationRecorders",
		"StartConfigurationRecorder",
		"PutDeliveryChannel",
		"DescribeDeliveryChannels",
	}
}

// RouteMatcher returns a function that matches AWS Config requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), awsConfigTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return awsConfigMatchPriority }

// ExtractOperation extracts the AWS Config action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, awsConfigTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts a resource identifier from the request body based on the operation.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	switch h.ExtractOperation(c) {
	case "PutConfigurationRecorder":
		return extractConfigRecorderName(body)
	case "StartConfigurationRecorder":
		return extractTopLevelRecorderName(body)
	case "DescribeConfigurationRecorders":
		return extractFirstRecorderName(body)
	case "PutDeliveryChannel":
		return extractDeliveryChannelName(body)
	case "DescribeDeliveryChannels":
		return extractFirstDeliveryChannelName(body)
	default:
		return extractTopLevelRecorderName(body)
	}
}

func extractConfigRecorderName(body []byte) string {
	var req struct {
		ConfigurationRecorder struct {
			Name string `json:"name"`
		} `json:"ConfigurationRecorder"`
	}
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return req.ConfigurationRecorder.Name
}

func extractTopLevelRecorderName(body []byte) string {
	var req struct {
		ConfigurationRecorderName string `json:"ConfigurationRecorderName"`
	}
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return req.ConfigurationRecorderName
}

func extractFirstRecorderName(body []byte) string {
	var req struct {
		ConfigurationRecorderNames []string `json:"ConfigurationRecorderNames"`
	}
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}
	if len(req.ConfigurationRecorderNames) > 0 {
		return req.ConfigurationRecorderNames[0]
	}

	return ""
}

func extractDeliveryChannelName(body []byte) string {
	var req struct {
		DeliveryChannel struct {
			Name string `json:"name"`
		} `json:"DeliveryChannel"`
	}
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return req.DeliveryChannel.Name
}

func extractFirstDeliveryChannelName(body []byte) string {
	var req struct {
		DeliveryChannelNames []string `json:"DeliveryChannelNames"`
	}
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}
	if len(req.DeliveryChannelNames) > 0 {
		return req.DeliveryChannelNames[0]
	}

	return ""
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"message": "failed to read body"})
		}

		action := strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), awsConfigTargetPrefix)
		switch action {
		case "PutConfigurationRecorder":
			return h.handlePutConfigurationRecorder(c, body)
		case "DescribeConfigurationRecorders":
			return h.handleDescribeConfigurationRecorders(c)
		case "StartConfigurationRecorder":
			return h.handleStartConfigurationRecorder(c, body)
		case "PutDeliveryChannel":
			return h.handlePutDeliveryChannel(c, body)
		case "DescribeDeliveryChannels":
			return h.handleDescribeDeliveryChannels(c)
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{"message": "unknown action: " + action})
		}
	}
}

type putConfigurationRecorderRequest struct {
	ConfigurationRecorder struct {
		Name    string `json:"name"`
		RoleARN string `json:"roleARN"`
	} `json:"ConfigurationRecorder"`
}

func (h *Handler) handlePutConfigurationRecorder(c *echo.Context, body []byte) error {
	var req putConfigurationRecorderRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	if err := h.Backend.PutConfigurationRecorder(
		req.ConfigurationRecorder.Name,
		req.ConfigurationRecorder.RoleARN,
	); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (h *Handler) handleDescribeConfigurationRecorders(c *echo.Context) error {
	recorders := h.Backend.DescribeConfigurationRecorders()

	return c.JSON(http.StatusOK, map[string]any{
		"ConfigurationRecorders": recorders,
	})
}

func (h *Handler) handleStartConfigurationRecorder(c *echo.Context, body []byte) error {
	var req struct {
		ConfigurationRecorderName string `json:"ConfigurationRecorderName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	if err := h.Backend.StartConfigurationRecorder(req.ConfigurationRecorderName); err != nil {
		return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (h *Handler) handlePutDeliveryChannel(c *echo.Context, body []byte) error {
	var req struct {
		DeliveryChannel struct {
			Name         string `json:"name"`
			S3BucketName string `json:"s3BucketName"`
			SnsTopicARN  string `json:"snsTopicARN"`
		} `json:"DeliveryChannel"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	if err := h.Backend.PutDeliveryChannel(
		req.DeliveryChannel.Name,
		req.DeliveryChannel.S3BucketName,
		req.DeliveryChannel.SnsTopicARN,
	); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (h *Handler) handleDescribeDeliveryChannels(c *echo.Context) error {
	channels := h.Backend.DescribeDeliveryChannels()

	return c.JSON(http.StatusOK, map[string]any{
		"DeliveryChannels": channels,
	})
}
