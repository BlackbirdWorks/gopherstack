package firehose

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const firehoseTargetPrefix = "Firehose_20150804."

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for Kinesis Firehose operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Firehose handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Firehose" }

// StartWorker starts the background interval flusher.
// It implements service.BackgroundWorker.
func (h *Handler) StartWorker(ctx context.Context) error {
	h.Backend.RunFlusher(ctx)

	return nil
}

// Shutdown implements service.Shutdowner.
// It flushes any buffered records to their destinations before the process
// exits so that records received since the last interval flush are not lost.
// If ctx expires before FlushAll returns, Shutdown returns immediately.
func (h *Handler) Shutdown(ctx context.Context) {
	if h.Backend == nil {
		return
	}

	done := make(chan struct{})

	go func() {
		h.Backend.FlushAll(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

// Ensure Handler implements service.BackgroundWorker and service.Shutdowner at compile time.
var _ service.BackgroundWorker = (*Handler)(nil)
var _ service.Shutdowner = (*Handler)(nil)

// GetSupportedOperations returns the list of supported Firehose operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateDeliveryStream",
		"DeleteDeliveryStream",
		"DescribeDeliveryStream",
		"ListDeliveryStreams",
		"PutRecord",
		"PutRecordBatch",
		"ListTagsForDeliveryStream",
		"TagDeliveryStream",
		"UntagDeliveryStream",
		"UpdateDestination",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "firehose" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Firehose instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Firehose requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), firehoseTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Firehose action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, firehoseTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

type deliveryStreamNameInput struct {
	DeliveryStreamName string `json:"DeliveryStreamName"`
}

// ExtractResource extracts the delivery stream name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req deliveryStreamNameInput
	_ = json.Unmarshal(body, &req)

	return req.DeliveryStreamName
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Firehose", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateDeliveryStream":      service.WrapOp(h.handleCreateDeliveryStream),
		"DeleteDeliveryStream":      service.WrapOp(h.handleDeleteDeliveryStream),
		"DescribeDeliveryStream":    service.WrapOp(h.handleDescribeDeliveryStream),
		"ListDeliveryStreams":       service.WrapOp(h.handleListDeliveryStreams),
		"PutRecord":                 service.WrapOp(h.handlePutRecord),
		"PutRecordBatch":            service.WrapOp(h.handlePutRecordBatch),
		"ListTagsForDeliveryStream": service.WrapOp(h.handleListTagsForDeliveryStream),
		"TagDeliveryStream":         service.WrapOp(h.handleTagDeliveryStream),
		"UntagDeliveryStream":       service.WrapOp(h.handleUntagDeliveryStream),
		"UpdateDestination":         service.WrapOp(h.handleUpdateDestination),
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
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound,
			map[string]any{"__type": "ResourceNotFoundException", "message": err.Error()})
	case errors.Is(err, ErrAlreadyExists), errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.Is(err, awserr.ErrInvalidParameter),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

// s3DestinationInput holds the S3 destination configuration from the API request.
// It maps both S3DestinationConfiguration and ExtendedS3DestinationConfiguration fields.
type s3DestinationInput struct {
	BufferingHints          *BufferingHints          `json:"BufferingHints"`
	ProcessingConfiguration *ProcessingConfiguration `json:"ProcessingConfiguration"`
	BucketARN               string                   `json:"BucketARN"`
	RoleARN                 string                   `json:"RoleARN"`
	Prefix                  string                   `json:"Prefix"`
	ErrorOutputPrefix       string                   `json:"ErrorOutputPrefix"`
	CompressionFormat       string                   `json:"CompressionFormat"`
}

type createDeliveryStreamInput struct {
	S3DestinationConfiguration         *s3DestinationInput `json:"S3DestinationConfiguration"`
	ExtendedS3DestinationConfiguration *s3DestinationInput `json:"ExtendedS3DestinationConfiguration"`
	DeliveryStreamName                 string              `json:"DeliveryStreamName"`
}

type createDeliveryStreamOutput struct {
	DeliveryStreamARN string `json:"DeliveryStreamARN"`
}

func (h *Handler) handleCreateDeliveryStream(
	_ context.Context,
	in *createDeliveryStreamInput,
) (*createDeliveryStreamOutput, error) {
	var dest *S3DestinationDescription

	// ExtendedS3 takes precedence over plain S3.
	raw := in.ExtendedS3DestinationConfiguration
	if raw == nil {
		raw = in.S3DestinationConfiguration
	}

	if raw != nil {
		dest = &S3DestinationDescription{
			BucketARN:               raw.BucketARN,
			RoleARN:                 raw.RoleARN,
			Prefix:                  raw.Prefix,
			ErrorOutputPrefix:       raw.ErrorOutputPrefix,
			CompressionFormat:       raw.CompressionFormat,
			BufferingHints:          raw.BufferingHints,
			ProcessingConfiguration: raw.ProcessingConfiguration,
		}
	}

	s, err := h.Backend.CreateDeliveryStream(CreateDeliveryStreamInput{
		Name:          in.DeliveryStreamName,
		S3Destination: dest,
	})
	if err != nil {
		return nil, err
	}

	return &createDeliveryStreamOutput{DeliveryStreamARN: s.ARN}, nil
}

type deleteDeliveryStreamOutput struct{}

func (h *Handler) handleDeleteDeliveryStream(
	_ context.Context,
	in *deliveryStreamNameInput,
) (*deleteDeliveryStreamOutput, error) {
	if err := h.Backend.DeleteDeliveryStream(in.DeliveryStreamName); err != nil {
		return nil, err
	}

	return &deleteDeliveryStreamOutput{}, nil
}

type deliveryStreamDescriptionFields struct {
	DeliveryStreamName        string                     `json:"DeliveryStreamName"`
	DeliveryStreamARN         string                     `json:"DeliveryStreamARN"`
	DeliveryStreamStatus      string                     `json:"DeliveryStreamStatus"`
	S3DestinationDescriptions []S3DestinationDescription `json:"S3DestinationDescriptions,omitempty"`
}

type describeDeliveryStreamOutput struct {
	DeliveryStreamDescription deliveryStreamDescriptionFields `json:"DeliveryStreamDescription"`
}

func (h *Handler) handleDescribeDeliveryStream(
	_ context.Context,
	in *deliveryStreamNameInput,
) (*describeDeliveryStreamOutput, error) {
	s, err := h.Backend.DescribeDeliveryStream(in.DeliveryStreamName)
	if err != nil {
		return nil, err
	}

	desc := deliveryStreamDescriptionFields{
		DeliveryStreamName:   s.Name,
		DeliveryStreamARN:    s.ARN,
		DeliveryStreamStatus: s.Status,
	}

	if s.S3Destination != nil {
		desc.S3DestinationDescriptions = []S3DestinationDescription{*s.S3Destination}
	}

	return &describeDeliveryStreamOutput{DeliveryStreamDescription: desc}, nil
}

type listDeliveryStreamsInput struct{}

type listDeliveryStreamsOutput struct {
	DeliveryStreamNames    []string `json:"DeliveryStreamNames"`
	HasMoreDeliveryStreams bool     `json:"HasMoreDeliveryStreams"`
}

func (h *Handler) handleListDeliveryStreams(
	_ context.Context,
	_ *listDeliveryStreamsInput,
) (*listDeliveryStreamsOutput, error) {
	names := h.Backend.ListDeliveryStreams()

	return &listDeliveryStreamsOutput{
		DeliveryStreamNames:    names,
		HasMoreDeliveryStreams: false,
	}, nil
}

// firehoseRecord holds the base64-encoded data for a single Firehose record.
type firehoseRecord struct {
	Data string `json:"Data"`
}

type handlePutRecordInput struct {
	DeliveryStreamName string         `json:"DeliveryStreamName"`
	Record             firehoseRecord `json:"Record"`
}

type putRecordOutput struct {
	RecordID string `json:"RecordId"`
}

func (h *Handler) handlePutRecord(_ context.Context, in *handlePutRecordInput) (*putRecordOutput, error) {
	data, err := base64.StdEncoding.DecodeString(in.Record.Data)
	if err != nil {
		data = []byte(in.Record.Data)
	}

	if putErr := h.Backend.PutRecord(in.DeliveryStreamName, data); putErr != nil {
		return nil, putErr
	}

	return &putRecordOutput{RecordID: "stub-record-id"}, nil
}

type handlePutRecordBatchInput struct {
	DeliveryStreamName string           `json:"DeliveryStreamName"`
	Records            []firehoseRecord `json:"Records"`
}

type putRecordBatchOutput struct {
	RequestResponses []struct{} `json:"RequestResponses"`
	FailedPutCount   int        `json:"FailedPutCount"`
}

func (h *Handler) handlePutRecordBatch(
	_ context.Context,
	in *handlePutRecordBatchInput,
) (*putRecordBatchOutput, error) {
	records := make([][]byte, 0, len(in.Records))
	for _, r := range in.Records {
		data, err := base64.StdEncoding.DecodeString(r.Data)
		if err != nil {
			data = []byte(r.Data)
		}

		records = append(records, data)
	}

	failedCount, err := h.Backend.PutRecordBatch(in.DeliveryStreamName, records)
	if err != nil {
		return nil, err
	}

	return &putRecordBatchOutput{
		FailedPutCount:   failedCount,
		RequestResponses: []struct{}{},
	}, nil
}

type listTagsForDeliveryStreamOutput struct {
	Tags        []svcTags.KV `json:"Tags"`
	HasMoreTags bool         `json:"HasMoreTags"`
}

func (h *Handler) handleListTagsForDeliveryStream(
	_ context.Context,
	in *deliveryStreamNameInput,
) (*listTagsForDeliveryStreamOutput, error) {
	tags, err := h.Backend.ListTagsForDeliveryStream(in.DeliveryStreamName)
	if err != nil {
		return nil, err
	}

	tagList := make([]svcTags.KV, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, svcTags.KV{Key: k, Value: v})
	}

	return &listTagsForDeliveryStreamOutput{
		Tags:        tagList,
		HasMoreTags: false,
	}, nil
}

type tagDeliveryStreamInput struct {
	DeliveryStreamName string       `json:"DeliveryStreamName"`
	Tags               []svcTags.KV `json:"Tags"`
}

type tagDeliveryStreamOutput struct{}

func (h *Handler) handleTagDeliveryStream(
	_ context.Context,
	in *tagDeliveryStreamInput,
) (*tagDeliveryStreamOutput, error) {
	tagMap := make(map[string]string, len(in.Tags))
	for _, t := range in.Tags {
		tagMap[t.Key] = t.Value
	}

	if err := h.Backend.TagDeliveryStream(in.DeliveryStreamName, tagMap); err != nil {
		return nil, err
	}

	return &tagDeliveryStreamOutput{}, nil
}

type untagDeliveryStreamInput struct {
	DeliveryStreamName string   `json:"DeliveryStreamName"`
	TagKeys            []string `json:"TagKeys"`
}

type untagDeliveryStreamOutput struct{}

func (h *Handler) handleUntagDeliveryStream(
	_ context.Context,
	in *untagDeliveryStreamInput,
) (*untagDeliveryStreamOutput, error) {
	if err := h.Backend.UntagDeliveryStream(in.DeliveryStreamName, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagDeliveryStreamOutput{}, nil
}

type updateDestinationInput struct {
	S3DestinationUpdate            *s3DestinationInput `json:"S3DestinationUpdate"`
	ExtendedS3DestinationUpdate    *s3DestinationInput `json:"ExtendedS3DestinationUpdate"`
	DeliveryStreamName             string              `json:"DeliveryStreamName"`
	CurrentDeliveryStreamVersionID string              `json:"CurrentDeliveryStreamVersionId"`
	DestinationID                  string              `json:"DestinationId"`
}

type updateDestinationOutput struct{}

func (h *Handler) handleUpdateDestination(
	_ context.Context,
	in *updateDestinationInput,
) (*updateDestinationOutput, error) {
	raw := in.ExtendedS3DestinationUpdate
	if raw == nil {
		raw = in.S3DestinationUpdate
	}

	var dest *S3DestinationDescription
	if raw != nil {
		dest = &S3DestinationDescription{
			BucketARN:               raw.BucketARN,
			RoleARN:                 raw.RoleARN,
			Prefix:                  raw.Prefix,
			ErrorOutputPrefix:       raw.ErrorOutputPrefix,
			CompressionFormat:       raw.CompressionFormat,
			BufferingHints:          raw.BufferingHints,
			ProcessingConfiguration: raw.ProcessingConfiguration,
		}
	}

	if err := h.Backend.UpdateDestination(in.DeliveryStreamName, dest); err != nil {
		return nil, err
	}

	return &updateDestinationOutput{}, nil
}
