package awsconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const awsConfigTargetPrefix = "StarlingDoveService."

var errUnknownAction = errors.New("unknown action")

type configurationRecorderNameInput struct {
	ConfigurationRecorderName string `json:"ConfigurationRecorderName"`
}

// Handler is the Echo HTTP handler for AWS Config operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new AWS Config handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "AWSConfig" }

// GetSupportedOperations returns the list of supported AWS Config operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"PutConfigurationRecorder",
		"DescribeConfigurationRecorders",
		"StartConfigurationRecorder",
		"DeleteConfigurationRecorder",
		"PutDeliveryChannel",
		"DescribeDeliveryChannels",
		"DeleteDeliveryChannel",
	}
}

// RouteMatcher returns a function that matches AWS Config requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), awsConfigTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

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

// configNameBody is a JSON wrapper carrying a single "name" field, used for both
// configuration recorder and delivery channel name extraction.
type configNameBody struct {
	Name string `json:"name"`
}

type extractConfigRecorderNameInput struct {
	ConfigurationRecorder configNameBody `json:"ConfigurationRecorder"`
}

func extractConfigRecorderName(body []byte) string {
	var req extractConfigRecorderNameInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return req.ConfigurationRecorder.Name
}

func extractTopLevelRecorderName(body []byte) string {
	var req configurationRecorderNameInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return req.ConfigurationRecorderName
}

type extractFirstRecorderNameInput struct {
	ConfigurationRecorderNames []string `json:"ConfigurationRecorderNames"`
}

func extractFirstRecorderName(body []byte) string {
	var req extractFirstRecorderNameInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}
	if len(req.ConfigurationRecorderNames) > 0 {
		return req.ConfigurationRecorderNames[0]
	}

	return ""
}

type extractDeliveryChannelNameInput struct {
	DeliveryChannel configNameBody `json:"DeliveryChannel"`
}

func extractDeliveryChannelName(body []byte) string {
	var req extractDeliveryChannelNameInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return req.DeliveryChannel.Name
}

type extractFirstDeliveryChannelNameInput struct {
	DeliveryChannelNames []string `json:"DeliveryChannelNames"`
}

func extractFirstDeliveryChannelName(body []byte) string {
	var req extractFirstDeliveryChannelNameInput
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
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"AWSConfig", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"PutConfigurationRecorder":       service.WrapOp(h.handlePutConfigurationRecorder),
		"DescribeConfigurationRecorders": service.WrapOp(h.handleDescribeConfigurationRecorders),
		"StartConfigurationRecorder":     service.WrapOp(h.handleStartConfigurationRecorder),
		"DeleteConfigurationRecorder":    service.WrapOp(h.handleDeleteConfigurationRecorder),
		"PutDeliveryChannel":             service.WrapOp(h.handlePutDeliveryChannel),
		"DescribeDeliveryChannels":       service.WrapOp(h.handleDescribeDeliveryChannels),
		"DeleteDeliveryChannel":          service.WrapOp(h.handleDeleteDeliveryChannel),
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
	code := http.StatusBadRequest

	if errors.Is(err, ErrNotFound) {
		code = http.StatusNotFound
	}

	return c.JSON(code, map[string]string{"message": err.Error()})
}

// configurationRecorderBody is the nested JSON body for a configuration recorder.
type configurationRecorderBody struct {
	Name    string `json:"name"`
	RoleARN string `json:"roleARN"`
}

type putConfigurationRecorderRequest struct {
	ConfigurationRecorder configurationRecorderBody `json:"ConfigurationRecorder"`
}

type putConfigurationRecorderOutput struct{}

func (h *Handler) handlePutConfigurationRecorder(
	_ context.Context,
	in *putConfigurationRecorderRequest,
) (*putConfigurationRecorderOutput, error) {
	if err := h.Backend.PutConfigurationRecorder(
		in.ConfigurationRecorder.Name,
		in.ConfigurationRecorder.RoleARN,
	); err != nil {
		return nil, err
	}

	return &putConfigurationRecorderOutput{}, nil
}

type describeConfigurationRecordersInput struct{}

type describeConfigurationRecordersOutput struct {
	ConfigurationRecorders []ConfigurationRecorder `json:"ConfigurationRecorders"`
}

func (h *Handler) handleDescribeConfigurationRecorders(
	_ context.Context,
	_ *describeConfigurationRecordersInput,
) (*describeConfigurationRecordersOutput, error) {
	recorders := h.Backend.DescribeConfigurationRecorders()

	return &describeConfigurationRecordersOutput{ConfigurationRecorders: recorders}, nil
}

type startConfigurationRecorderOutput struct{}

func (h *Handler) handleStartConfigurationRecorder(
	_ context.Context,
	in *configurationRecorderNameInput,
) (*startConfigurationRecorderOutput, error) {
	if err := h.Backend.StartConfigurationRecorder(in.ConfigurationRecorderName); err != nil {
		return nil, err
	}

	return &startConfigurationRecorderOutput{}, nil
}

// deliveryChannelBody is the nested JSON body for a delivery channel.
type deliveryChannelBody struct {
	Name         string `json:"name"`
	S3BucketName string `json:"s3BucketName"`
	SnsTopicARN  string `json:"snsTopicARN"`
}

type handlePutDeliveryChannelInput struct {
	DeliveryChannel deliveryChannelBody `json:"DeliveryChannel"`
}

type putDeliveryChannelOutput struct{}

func (h *Handler) handlePutDeliveryChannel(
	_ context.Context,
	in *handlePutDeliveryChannelInput,
) (*putDeliveryChannelOutput, error) {
	if err := h.Backend.PutDeliveryChannel(
		in.DeliveryChannel.Name,
		in.DeliveryChannel.S3BucketName,
		in.DeliveryChannel.SnsTopicARN,
	); err != nil {
		return nil, err
	}

	return &putDeliveryChannelOutput{}, nil
}

type describeDeliveryChannelsInput struct{}

type describeDeliveryChannelsOutput struct {
	DeliveryChannels []DeliveryChannel `json:"DeliveryChannels"`
}

func (h *Handler) handleDescribeDeliveryChannels(
	_ context.Context,
	_ *describeDeliveryChannelsInput,
) (*describeDeliveryChannelsOutput, error) {
	channels := h.Backend.DescribeDeliveryChannels()

	return &describeDeliveryChannelsOutput{DeliveryChannels: channels}, nil
}

type deleteDeliveryChannelInput struct {
	DeliveryChannelName string `json:"DeliveryChannelName"`
}

type deleteDeliveryChannelOutput struct{}

func (h *Handler) handleDeleteDeliveryChannel(
	_ context.Context,
	in *deleteDeliveryChannelInput,
) (*deleteDeliveryChannelOutput, error) {
	if err := h.Backend.DeleteDeliveryChannel(in.DeliveryChannelName); err != nil {
		return nil, err
	}

	return &deleteDeliveryChannelOutput{}, nil
}

type deleteConfigurationRecorderInput struct {
	ConfigurationRecorderName string `json:"ConfigurationRecorderName"`
}

type deleteConfigurationRecorderOutput struct{}

func (h *Handler) handleDeleteConfigurationRecorder(
	_ context.Context,
	in *deleteConfigurationRecorderInput,
) (*deleteConfigurationRecorderOutput, error) {
	if err := h.Backend.DeleteConfigurationRecorder(in.ConfigurationRecorderName); err != nil {
		return nil, err
	}

	return &deleteConfigurationRecorderOutput{}, nil
}
