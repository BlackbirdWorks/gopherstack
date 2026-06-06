package firehose

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	firehoseTargetPrefix = "Firehose_20150804."
	errFieldMessage      = "message"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for Kinesis Firehose operations.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new Firehose handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears all state in the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
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
		"StartDeliveryStreamEncryption",
		"StopDeliveryStreamEncryption",
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

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateDeliveryStream":          service.WrapOp(h.handleCreateDeliveryStream),
		"DeleteDeliveryStream":          service.WrapOp(h.handleDeleteDeliveryStream),
		"DescribeDeliveryStream":        service.WrapOp(h.handleDescribeDeliveryStream),
		"ListDeliveryStreams":           service.WrapOp(h.handleListDeliveryStreams),
		"PutRecord":                     service.WrapOp(h.handlePutRecord),
		"PutRecordBatch":                service.WrapOp(h.handlePutRecordBatch),
		"ListTagsForDeliveryStream":     service.WrapOp(h.handleListTagsForDeliveryStream),
		"TagDeliveryStream":             service.WrapOp(h.handleTagDeliveryStream),
		"UntagDeliveryStream":           service.WrapOp(h.handleUntagDeliveryStream),
		"UpdateDestination":             service.WrapOp(h.handleUpdateDestination),
		"StartDeliveryStreamEncryption": service.WrapOp(h.handleStartDeliveryStreamEncryption),
		"StopDeliveryStreamEncryption":  service.WrapOp(h.handleStopDeliveryStreamEncryption),
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
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound,
			map[string]any{"__type": "ResourceNotFoundException", errFieldMessage: err.Error()})
	case errors.Is(err, ErrAlreadyExists), errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.Is(err, awserr.ErrInvalidParameter), errors.Is(err, ErrValidation),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{errFieldMessage: err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{errFieldMessage: err.Error()})
	}
}

const (
	maxTagCount     = 50
	maxTagKeyLen    = 128
	maxTagValueLen  = 256
	maxTagListLimit = 50
	maxListLimit    = 10000
)

// s3DestinationInput holds the S3 destination configuration from the API request.
// It maps both S3DestinationConfiguration and ExtendedS3DestinationConfiguration fields.
type s3DestinationInput struct {
	BufferingHints                   *BufferingHints                   `json:"BufferingHints"`
	ProcessingConfiguration          *ProcessingConfiguration          `json:"ProcessingConfiguration"`
	S3BackupConfiguration            *s3BackupInput                    `json:"S3BackupConfiguration"`
	EncryptionConfiguration          *S3EncryptionConfiguration        `json:"EncryptionConfiguration"`
	CloudWatchLoggingOptions         *CloudWatchLoggingOptions         `json:"CloudWatchLoggingOptions"`
	DynamicPartitioningConfiguration *DynamicPartitioningConfiguration `json:"DynamicPartitioningConfiguration"`
	BucketARN                        string                            `json:"BucketARN"`
	RoleARN                          string                            `json:"RoleARN"`
	Prefix                           string                            `json:"Prefix"`
	ErrorOutputPrefix                string                            `json:"ErrorOutputPrefix"`
	CompressionFormat                string                            `json:"CompressionFormat"`
	FileExtension                    string                            `json:"FileExtension"`
	CustomTimeZone                   string                            `json:"CustomTimeZone"`
	S3BackupMode                     string                            `json:"S3BackupMode"`
}

// s3BackupInput holds the S3 backup destination configuration.
type s3BackupInput struct {
	BufferingHints    *BufferingHints `json:"BufferingHints"`
	BucketARN         string          `json:"BucketARN"`
	RoleARN           string          `json:"RoleARN"`
	Prefix            string          `json:"Prefix"`
	CompressionFormat string          `json:"CompressionFormat"`
}

// httpEndpointDestinationInput holds HTTP endpoint destination configuration.
type httpEndpointDestinationInput struct {
	EndpointConfiguration    *httpEndpointConfigurationInput   `json:"EndpointConfiguration"`
	ProcessingConfiguration  *ProcessingConfiguration          `json:"ProcessingConfiguration"`
	S3BackupConfiguration    *s3BackupInput                    `json:"S3BackupConfiguration"`
	RequestConfiguration     *HTTPEndpointRequestConfiguration `json:"RequestConfiguration"`
	BufferingHints           *BufferingHints                   `json:"BufferingHints"`
	RetryOptions             *RetryOptions                     `json:"RetryOptions"`
	CloudWatchLoggingOptions *CloudWatchLoggingOptions         `json:"CloudWatchLoggingOptions"`
	S3BackupMode             string                            `json:"S3BackupMode"`
}

// httpEndpointConfigurationInput holds the HTTP endpoint URL and name.
type httpEndpointConfigurationInput struct {
	URL       string `json:"Url"`
	Name      string `json:"Name"`
	AccessKey string `json:"AccessKey"`
}

// kinesisStreamSourceConfigurationInput holds Kinesis stream source config.
type kinesisStreamSourceConfigurationInput struct {
	KinesisStreamARN string `json:"KinesisStreamARN"`
	RoleARN          string `json:"RoleARN"`
}

// mskSourceConfigurationInput holds MSK cluster source config.
type mskSourceConfigurationInput struct {
	AuthenticationConfiguration *MSKAuthenticationConfiguration `json:"AuthenticationConfiguration"`
	MSKClusterARN               string                          `json:"MSKClusterARN"`
	TopicName                   string                          `json:"TopicName"`
	ReadFromTimestamp           string                          `json:"ReadFromTimestamp"`
}

// redshiftDestinationInput holds the Redshift destination configuration.
type redshiftDestinationInput struct {
	ProcessingConfiguration *ProcessingConfiguration `json:"ProcessingConfiguration"`
	RetryOptions            *RetryOptions            `json:"RetryOptions"`
	S3BackupConfiguration   *s3BackupInput           `json:"S3BackupConfiguration"`
	ClusterJDBCURL          string                   `json:"ClusterJDBCURL"`
	RoleARN                 string                   `json:"RoleARN"`
	S3BackupMode            string                   `json:"S3BackupMode"`
	DataTableName           string                   `json:"DataTableName"`
	DataTableColumns        string                   `json:"DataTableColumns"`
	CopyOptions             string                   `json:"CopyOptions"`
	Username                string                   `json:"Username"`
}

// openSearchDestinationInput holds the OpenSearch destination configuration.
type openSearchDestinationInput struct {
	ProcessingConfiguration  *ProcessingConfiguration  `json:"ProcessingConfiguration"`
	BufferingHints           *BufferingHints           `json:"BufferingHints"`
	RetryOptions             *RetryOptions             `json:"RetryOptions"`
	CloudWatchLoggingOptions *CloudWatchLoggingOptions `json:"CloudWatchLoggingOptions"`
	S3BackupConfiguration    *s3BackupInput            `json:"S3BackupConfiguration"`
	DomainARN                string                    `json:"DomainARN"`
	ClusterEndpoint          string                    `json:"ClusterEndpoint"`
	IndexName                string                    `json:"IndexName"`
	TypeName                 string                    `json:"TypeName"`
	IndexRotationPeriod      string                    `json:"IndexRotationPeriod"`
	S3BackupMode             string                    `json:"S3BackupMode"`
	RoleARN                  string                    `json:"RoleARN"`
}

// splunkDestinationInput holds the Splunk HEC destination configuration.
type splunkDestinationInput struct {
	ProcessingConfiguration           *ProcessingConfiguration  `json:"ProcessingConfiguration"`
	RetryOptions                      *RetryOptions             `json:"RetryOptions"`
	CloudWatchLoggingOptions          *CloudWatchLoggingOptions `json:"CloudWatchLoggingOptions"`
	S3BackupConfiguration             *s3BackupInput            `json:"S3BackupConfiguration"`
	HECEndpoint                       string                    `json:"HECEndpoint"`
	HECEndpointType                   string                    `json:"HECEndpointType"`
	HECToken                          string                    `json:"HECToken"`
	S3BackupMode                      string                    `json:"S3BackupMode"`
	HECAcknowledgmentTimeoutInSeconds int                       `json:"HECAcknowledgmentTimeoutInSeconds"`
}

type createDeliveryStreamInput struct {
	S3DestinationConfiguration         *s3DestinationInput                    `json:"S3DestinationConfiguration"`
	ExtendedS3DestinationConfiguration *s3DestinationInput                    `json:"ExtendedS3DestinationConfiguration"`
	HTTPEndpointDestinationConfiguration *httpEndpointDestinationInput `json:"HTTPEndpointDestinationConfiguration"`
	KinesisStreamSourceConfiguration   *kinesisStreamSourceConfigurationInput  `json:"KinesisStreamSourceConfiguration"`
	MSKSourceConfiguration             *mskSourceConfigurationInput            `json:"MSKSourceConfiguration"`
	RedshiftDestinationConfiguration   *redshiftDestinationInput               `json:"RedshiftDestinationConfiguration"`
	AmazonOpenSearchServiceDestinationConfiguration *openSearchDestinationInput `json:"AmazonOpenSearchServiceDestinationConfiguration"` //nolint:lll // AWS field name
	SplunkDestinationConfiguration     *splunkDestinationInput                 `json:"SplunkDestinationConfiguration"`
	DeliveryStreamName                 string                                  `json:"DeliveryStreamName"`
	DeliveryStreamType                 string                                  `json:"DeliveryStreamType"`
	Tags                               []svcTags.KV                            `json:"Tags"`
}

type createDeliveryStreamOutput struct {
	DeliveryStreamARN string `json:"DeliveryStreamARN"`
}

// buildS3DestinationDescription converts an s3DestinationInput to the backend type.
func buildS3DestinationDescription(raw *s3DestinationInput) *S3DestinationDescription {
	if raw == nil {
		return nil
	}

	dest := &S3DestinationDescription{
		BucketARN:                        raw.BucketARN,
		RoleARN:                          raw.RoleARN,
		Prefix:                           raw.Prefix,
		ErrorOutputPrefix:                raw.ErrorOutputPrefix,
		CompressionFormat:                raw.CompressionFormat,
		FileExtension:                    raw.FileExtension,
		CustomTimeZone:                   raw.CustomTimeZone,
		BufferingHints:                   raw.BufferingHints,
		ProcessingConfiguration:          raw.ProcessingConfiguration,
		S3BackupMode:                     raw.S3BackupMode,
		EncryptionConfiguration:          raw.EncryptionConfiguration,
		CloudWatchLoggingOptions:         raw.CloudWatchLoggingOptions,
		DynamicPartitioningConfiguration: raw.DynamicPartitioningConfiguration,
	}

	dest.S3BackupDescription = buildS3BackupDescription(raw.S3BackupConfiguration)

	return dest
}

// buildHTTPEndpointDestination converts httpEndpointDestinationInput to the backend type.
func buildHTTPEndpointDestination(ep *httpEndpointDestinationInput) *HTTPEndpointDestinationDescription {
	if ep == nil {
		return nil
	}

	dest := &HTTPEndpointDestinationDescription{
		ProcessingConfiguration:  ep.ProcessingConfiguration,
		S3BackupMode:             ep.S3BackupMode,
		RequestConfiguration:     ep.RequestConfiguration,
		BufferingHints:           ep.BufferingHints,
		RetryOptions:             ep.RetryOptions,
		CloudWatchLoggingOptions: ep.CloudWatchLoggingOptions,
	}

	if ep.EndpointConfiguration != nil {
		dest.EndpointConfiguration = &HTTPEndpointConfiguration{
			URL:       ep.EndpointConfiguration.URL,
			Name:      ep.EndpointConfiguration.Name,
			AccessKey: ep.EndpointConfiguration.AccessKey,
		}
	}

	if ep.S3BackupConfiguration != nil {
		dest.S3BackupDescription = buildS3BackupDescription(ep.S3BackupConfiguration)
	}

	return dest
}

// buildRedshiftDestination converts redshiftDestinationInput to the backend type.
func buildRedshiftDestination(rs *redshiftDestinationInput) *RedshiftDestinationDescription {
	if rs == nil {
		return nil
	}

	dest := &RedshiftDestinationDescription{
		ClusterJDBCURL:          rs.ClusterJDBCURL,
		RoleARN:                 rs.RoleARN,
		S3BackupMode:            rs.S3BackupMode,
		ProcessingConfiguration: rs.ProcessingConfiguration,
		RetryOptions:            rs.RetryOptions,
		DataTableName:           rs.DataTableName,
		DataTableColumns:        rs.DataTableColumns,
		CopyOptions:             rs.CopyOptions,
		Username:                rs.Username,
	}

	if rs.S3BackupConfiguration != nil {
		dest.S3BackupDescription = buildS3BackupDescription(rs.S3BackupConfiguration)
	}

	return dest
}

// buildOpenSearchDestination converts openSearchDestinationInput to the backend type.
func buildOpenSearchDestination(os *openSearchDestinationInput) *OpenSearchDestinationDescription {
	if os == nil {
		return nil
	}

	dest := &OpenSearchDestinationDescription{
		DomainARN:                os.DomainARN,
		ClusterEndpoint:          os.ClusterEndpoint,
		IndexName:                os.IndexName,
		TypeName:                 os.TypeName,
		IndexRotationPeriod:      os.IndexRotationPeriod,
		S3BackupMode:             os.S3BackupMode,
		RoleARN:                  os.RoleARN,
		ProcessingConfiguration:  os.ProcessingConfiguration,
		BufferingHints:           os.BufferingHints,
		RetryOptions:             os.RetryOptions,
		CloudWatchLoggingOptions: os.CloudWatchLoggingOptions,
	}

	if os.S3BackupConfiguration != nil {
		dest.S3BackupDescription = buildS3BackupDescription(os.S3BackupConfiguration)
	}

	return dest
}

// buildSplunkDestination converts splunkDestinationInput to the backend type.
func buildSplunkDestination(sp *splunkDestinationInput) *SplunkDestinationDescription {
	if sp == nil {
		return nil
	}

	dest := &SplunkDestinationDescription{
		HECEndpoint:                       sp.HECEndpoint,
		HECEndpointType:                   sp.HECEndpointType,
		HECToken:                          sp.HECToken,
		S3BackupMode:                      sp.S3BackupMode,
		HECAcknowledgmentTimeoutInSeconds: sp.HECAcknowledgmentTimeoutInSeconds,
		ProcessingConfiguration:           sp.ProcessingConfiguration,
		RetryOptions:                      sp.RetryOptions,
		CloudWatchLoggingOptions:          sp.CloudWatchLoggingOptions,
	}

	if sp.S3BackupConfiguration != nil {
		dest.S3BackupDescription = buildS3BackupDescription(sp.S3BackupConfiguration)
	}

	return dest
}

// buildS3BackupDescription converts an s3BackupInput to the backend type.
func buildS3BackupDescription(b *s3BackupInput) *S3BackupDescription {
	if b == nil {
		return nil
	}

	return &S3BackupDescription{
		BucketARN:         b.BucketARN,
		RoleARN:           b.RoleARN,
		Prefix:            b.Prefix,
		CompressionFormat: b.CompressionFormat,
		BufferingHints:    b.BufferingHints,
	}
}

// buildSourceDescription converts source config inputs to the backend type.
func buildSourceDescription(
	ks *kinesisStreamSourceConfigurationInput,
	msk *mskSourceConfigurationInput,
) *SourceDescription {
	if ks != nil {
		return &SourceDescription{
			KinesisStreamSourceDescription: &KinesisStreamSourceDescription{
				KinesisStreamARN: ks.KinesisStreamARN,
				RoleARN:          ks.RoleARN,
			},
		}
	}

	if msk != nil {
		return &SourceDescription{
			MSKSourceDescription: &MSKSourceDescription{
				MSKClusterARN:               msk.MSKClusterARN,
				TopicName:                   msk.TopicName,
				ReadFromTimestamp:           msk.ReadFromTimestamp,
				AuthenticationConfiguration: msk.AuthenticationConfiguration,
			},
		}
	}

	return nil
}

func (h *Handler) handleCreateDeliveryStream(
	_ context.Context,
	in *createDeliveryStreamInput,
) (*createDeliveryStreamOutput, error) {
	if err := validateTags(in.Tags); err != nil {
		return nil, err
	}

	// ExtendedS3 takes precedence over plain S3.
	rawS3 := in.ExtendedS3DestinationConfiguration
	if rawS3 == nil {
		rawS3 = in.S3DestinationConfiguration
	}

	s, err := h.Backend.CreateDeliveryStream(CreateDeliveryStreamInput{
		Name:                    in.DeliveryStreamName,
		DeliveryStreamType:      in.DeliveryStreamType,
		S3Destination:           buildS3DestinationDescription(rawS3),
		HTTPEndpointDestination: buildHTTPEndpointDestination(in.HTTPEndpointDestinationConfiguration),
		RedshiftDestination:     buildRedshiftDestination(in.RedshiftDestinationConfiguration),
		OpenSearchDestination:   buildOpenSearchDestination(in.AmazonOpenSearchServiceDestinationConfiguration),
		SplunkDestination:       buildSplunkDestination(in.SplunkDestinationConfiguration),
		Source:                  buildSourceDescription(in.KinesisStreamSourceConfiguration, in.MSKSourceConfiguration),
	})
	if err != nil {
		return nil, err
	}

	if len(in.Tags) > 0 {
		tagMap := make(map[string]string, len(in.Tags))
		for _, t := range in.Tags {
			tagMap[t.Key] = t.Value
		}

		_ = h.Backend.TagDeliveryStream(in.DeliveryStreamName, tagMap)
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
	EncryptionConfiguration             *EncryptionConfig                    `json:"EncryptionConfiguration,omitempty"`
	Source                              *SourceDescription                   `json:"Source,omitempty"`
	CreateTimestamp                     *int64                               `json:"CreateTimestamp,omitempty"`
	LastUpdateTimestamp                 *int64                               `json:"LastUpdateTimestamp,omitempty"`
	DeliveryStreamName                  string                               `json:"DeliveryStreamName"`
	DeliveryStreamARN                   string                               `json:"DeliveryStreamARN"`
	DeliveryStreamStatus                string                               `json:"DeliveryStreamStatus"`
	DeliveryStreamType                  string                               `json:"DeliveryStreamType,omitempty"`
	VersionID                           string                               `json:"VersionId,omitempty"`
	S3DestinationDescriptions           []S3DestinationDescription           `json:"S3DestinationDescriptions,omitempty"`
	HTTPEndpointDestinationDescriptions []HTTPEndpointDestinationDescription `json:"HTTPEndpointDestinationDescriptions,omitempty"` //nolint:lll // AWS field name must match the API spec
	RedshiftDestinationDescriptions     []RedshiftDestinationDescription     `json:"RedshiftDestinationDescriptions,omitempty"`     //nolint:lll // AWS field name
	HasMoreDestinations                 bool                                 `json:"HasMoreDestinations"`
}

type describeDeliveryStreamInput struct {
	DeliveryStreamName          string `json:"DeliveryStreamName"`
	ExclusiveStartDestinationID string `json:"ExclusiveStartDestinationId"`
	Limit                       int    `json:"Limit"`
}

type describeDeliveryStreamOutput struct {
	DeliveryStreamDescription deliveryStreamDescriptionFields `json:"DeliveryStreamDescription"`
}

func (h *Handler) handleDescribeDeliveryStream(
	_ context.Context,
	in *describeDeliveryStreamInput,
) (*describeDeliveryStreamOutput, error) {
	s, err := h.Backend.DescribeDeliveryStream(in.DeliveryStreamName)
	if err != nil {
		return nil, err
	}

	var createTS, updateTS *int64
	if !s.CreateTimestamp.IsZero() {
		ts := s.CreateTimestamp.Unix()
		createTS = &ts
	}

	if !s.LastUpdateTimestamp.IsZero() {
		ts := s.LastUpdateTimestamp.Unix()
		updateTS = &ts
	}

	desc := deliveryStreamDescriptionFields{
		DeliveryStreamName:      s.Name,
		DeliveryStreamARN:       s.ARN,
		DeliveryStreamStatus:    s.Status,
		DeliveryStreamType:      s.DeliveryStreamType,
		VersionID:               s.VersionID,
		EncryptionConfiguration: s.Encryption,
		Source:                  s.Source,
		CreateTimestamp:         createTS,
		LastUpdateTimestamp:     updateTS,
		HasMoreDestinations:     false,
	}

	if s.S3Destination != nil {
		desc.S3DestinationDescriptions = []S3DestinationDescription{*s.S3Destination}
	}

	if s.HTTPEndpointDestination != nil {
		desc.HTTPEndpointDestinationDescriptions = []HTTPEndpointDestinationDescription{*s.HTTPEndpointDestination}
	}

	if s.RedshiftDestination != nil {
		desc.RedshiftDestinationDescriptions = []RedshiftDestinationDescription{*s.RedshiftDestination}
	}

	return &describeDeliveryStreamOutput{DeliveryStreamDescription: desc}, nil
}

type listDeliveryStreamsInput struct {
	ExclusiveStartDeliveryStreamName string `json:"ExclusiveStartDeliveryStreamName"`
	DeliveryStreamType               string `json:"DeliveryStreamType"`
	Limit                            int    `json:"Limit"`
}

type listDeliveryStreamsOutput struct {
	DeliveryStreamNames    []string `json:"DeliveryStreamNames"`
	HasMoreDeliveryStreams bool     `json:"HasMoreDeliveryStreams"`
}

func (h *Handler) handleListDeliveryStreams(
	_ context.Context,
	in *listDeliveryStreamsInput,
) (*listDeliveryStreamsOutput, error) {
	names := h.Backend.ListDeliveryStreams()

	// Apply ExclusiveStartDeliveryStreamName cursor.
	if in.ExclusiveStartDeliveryStreamName != "" {
		startIdx := -1
		for i, n := range names {
			if n == in.ExclusiveStartDeliveryStreamName {
				startIdx = i

				break
			}
		}
		if startIdx >= 0 {
			names = names[startIdx+1:]
		}
	}

	hasMore := false
	limit := in.Limit
	if limit <= 0 || limit > maxListLimit {
		limit = maxListLimit
	}

	if len(names) > limit {
		names = names[:limit]
		hasMore = true
	}

	return &listDeliveryStreamsOutput{
		DeliveryStreamNames:    names,
		HasMoreDeliveryStreams: hasMore,
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

	return &putRecordOutput{RecordID: newRecordID()}, nil
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

type listTagsForDeliveryStreamInput struct {
	DeliveryStreamName   string `json:"DeliveryStreamName"`
	ExclusiveStartTagKey string `json:"ExclusiveStartTagKey"`
	Limit                int    `json:"Limit"`
}

type listTagsForDeliveryStreamOutput struct {
	Tags        []svcTags.KV `json:"Tags"`
	HasMoreTags bool         `json:"HasMoreTags"`
}

func (h *Handler) handleListTagsForDeliveryStream(
	_ context.Context,
	in *listTagsForDeliveryStreamInput,
) (*listTagsForDeliveryStreamOutput, error) {
	tagMap, err := h.Backend.ListTagsForDeliveryStream(in.DeliveryStreamName)
	if err != nil {
		return nil, err
	}

	tagList := make([]svcTags.KV, 0, len(tagMap))
	for k, v := range tagMap {
		tagList = append(tagList, svcTags.KV{Key: k, Value: v})
	}

	sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

	// Apply ExclusiveStartTagKey cursor.
	if in.ExclusiveStartTagKey != "" {
		startIdx := -1
		for i, t := range tagList {
			if t.Key == in.ExclusiveStartTagKey {
				startIdx = i

				break
			}
		}
		if startIdx >= 0 {
			tagList = tagList[startIdx+1:]
		}
	}

	hasMore := false
	limit := in.Limit
	if limit <= 0 || limit > maxTagListLimit {
		limit = maxTagListLimit
	}

	if len(tagList) > limit {
		tagList = tagList[:limit]
		hasMore = true
	}

	return &listTagsForDeliveryStreamOutput{
		Tags:        tagList,
		HasMoreTags: hasMore,
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
	if err := validateTags(in.Tags); err != nil {
		return nil, err
	}

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

	dest := buildS3DestinationDescription(raw)

	if err := h.Backend.UpdateDestination(in.DeliveryStreamName, in.CurrentDeliveryStreamVersionID, dest); err != nil {
		return nil, err
	}

	return &updateDestinationOutput{}, nil
}

type startDeliveryStreamEncryptionInput struct {
	EncryptionConfig   *EncryptionConfigInput `json:"DeliveryStreamEncryptionConfigurationInput,omitempty"`
	DeliveryStreamName string                 `json:"DeliveryStreamName"`
}

type startDeliveryStreamEncryptionOutput struct{}

func (h *Handler) handleStartDeliveryStreamEncryption(
	ctx context.Context,
	in *startDeliveryStreamEncryptionInput,
) (*startDeliveryStreamEncryptionOutput, error) {
	cfgInput := in.EncryptionConfig
	if err := h.Backend.StartDeliveryStreamEncryption(ctx, in.DeliveryStreamName, cfgInput); err != nil {
		return nil, err
	}

	return &startDeliveryStreamEncryptionOutput{}, nil
}

type stopDeliveryStreamEncryptionOutput struct{}

func (h *Handler) handleStopDeliveryStreamEncryption(
	ctx context.Context,
	in *deliveryStreamNameInput,
) (*stopDeliveryStreamEncryptionOutput, error) {
	if err := h.Backend.StopDeliveryStreamEncryption(ctx, in.DeliveryStreamName); err != nil {
		return nil, err
	}

	return &stopDeliveryStreamEncryptionOutput{}, nil
}

// validateTags enforces AWS tag limits: max 50 tags, key ≤128 chars, value ≤256 chars.
func validateTags(tags []svcTags.KV) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: tag count %d exceeds maximum of %d", ErrValidation, len(tags), maxTagCount)
	}

	for _, t := range tags {
		if len(t.Key) == 0 || len(t.Key) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key length must be between 1 and %d characters", ErrValidation, maxTagKeyLen)
		}
		if len(t.Value) > maxTagValueLen {
			return fmt.Errorf("%w: tag value length must not exceed %d characters", ErrValidation, maxTagValueLen)
		}
	}

	return nil
}
