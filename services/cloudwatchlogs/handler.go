package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var errUnknownOperation = errors.New("UnknownOperationException")

type createLogGroupInput struct {
	Tags          map[string]string `json:"tags,omitempty"`
	LogGroupName  string            `json:"logGroupName"`
	KmsKeyID      string            `json:"kmsKeyId,omitempty"`
	LogGroupClass string            `json:"logGroupClass,omitempty"`
}

type deleteLogGroupInput struct {
	LogGroupName string `json:"logGroupName"`
}

type describeLogGroupsInput struct {
	LogGroupNamePrefix string `json:"logGroupNamePrefix"`
	NextToken          string `json:"nextToken"`
	Limit              int    `json:"limit"`
}

type createLogStreamInput struct {
	LogGroupName  string `json:"logGroupName"`
	LogStreamName string `json:"logStreamName"`
}

type describeLogStreamsInput struct {
	LogGroupName        string `json:"logGroupName"`
	LogStreamNamePrefix string `json:"logStreamNamePrefix"`
	NextToken           string `json:"nextToken"`
	OrderBy             string `json:"orderBy"`
	Limit               int    `json:"limit"`
	Descending          bool   `json:"descending"`
}

type deleteLogStreamInput struct {
	LogGroupName  string `json:"logGroupName"`
	LogStreamName string `json:"logStreamName"`
}

type putLogEventsInput struct {
	LogGroupName  string          `json:"logGroupName"`
	LogStreamName string          `json:"logStreamName"`
	SequenceToken string          `json:"sequenceToken,omitempty"`
	LogEvents     []InputLogEvent `json:"logEvents"`
}

type getLogEventsInput struct {
	StartTime     *int64 `json:"startTime"`
	EndTime       *int64 `json:"endTime"`
	LogGroupName  string `json:"logGroupName"`
	LogStreamName string `json:"logStreamName"`
	NextToken     string `json:"nextToken"`
	Limit         int    `json:"limit"`
	StartFromHead bool   `json:"startFromHead"`
}

type filterLogEventsInput struct {
	StartTime      *int64   `json:"startTime"`
	EndTime        *int64   `json:"endTime"`
	LogGroupName   string   `json:"logGroupName"`
	FilterPattern  string   `json:"filterPattern"`
	NextToken      string   `json:"nextToken"`
	LogStreamNames []string `json:"logStreamNames"`
	Limit          int      `json:"limit"`
}

type listTagsLogGroupInput struct {
	LogGroupName string `json:"logGroupName"`
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type tagLogGroupInput struct {
	Tags         *tags.Tags `json:"tags"`
	LogGroupName string     `json:"logGroupName"`
}

type untagLogGroupInput struct {
	LogGroupName string   `json:"logGroupName"`
	Tags         []string `json:"tags"`
}

type tagResourceInput struct {
	Tags        map[string]string `json:"tags"`
	ResourceArn string            `json:"resourceArn"`
}

type untagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type putRetentionPolicyInput struct {
	LogGroupName    string `json:"logGroupName"`
	RetentionInDays int32  `json:"retentionInDays"`
}

type deleteRetentionPolicyInput struct {
	LogGroupName string `json:"logGroupName"`
}

type putSubscriptionFilterInput struct {
	FilterPattern  string `json:"filterPattern"`
	FilterName     string `json:"filterName"`
	LogGroupName   string `json:"logGroupName"`
	DestinationArn string `json:"destinationArn"`
	RoleArn        string `json:"roleArn,omitempty"`
	Distribution   string `json:"distribution,omitempty"`
}

type describeSubscriptionFiltersInput struct {
	FilterNamePrefix string `json:"filterNamePrefix"`
	LogGroupName     string `json:"logGroupName"`
	NextToken        string `json:"nextToken"`
	Limit            int    `json:"limit"`
}

type deleteSubscriptionFilterInput struct {
	FilterName   string `json:"filterName"`
	LogGroupName string `json:"logGroupName"`
}

type startQueryInput struct {
	LogGroupName        string   `json:"logGroupName"`
	QueryString         string   `json:"queryString"`
	LogGroupNames       []string `json:"logGroupNames"`
	LogGroupIdentifiers []string `json:"logGroupIdentifiers"`
	StartTime           int64    `json:"startTime"`
	EndTime             int64    `json:"endTime"`
}

type startQueryOutput struct {
	QueryID string `json:"queryId"`
}

type getQueryResultsInput struct {
	QueryID string `json:"queryId"`
}

type getQueryResultsOutput struct {
	Status     QueryStatus     `json:"status"`
	Results    [][]ResultField `json:"results"`
	Statistics QueryStatistics `json:"statistics"`
}

type stopQueryInput struct {
	QueryID string `json:"queryId"`
}

type stopQueryOutput struct {
	Success bool `json:"success"`
}

type describeQueriesInput struct {
	LogGroupName string `json:"logGroupName"`
	Status       string `json:"status"`
	NextToken    string `json:"nextToken"`
	MaxResults   int    `json:"maxResults"`
}

type describeQueriesOutput struct {
	NextToken string      `json:"nextToken,omitempty"`
	Queries   []QueryInfo `json:"queries"`
}

type associateKmsKeyInput struct {
	LogGroupName       string `json:"logGroupName"`
	ResourceIdentifier string `json:"resourceIdentifier"`
	KmsKeyID           string `json:"kmsKeyId"`
}

type associateKmsKeyOutput struct{}

type associateSourceToS3TableIntegrationInput struct {
	DataSource     map[string]any `json:"dataSource"`
	IntegrationArn string         `json:"integrationArn"`
}

type associateSourceToS3TableIntegrationOutput struct {
	Identifier string `json:"identifier,omitempty"`
}

type cancelExportTaskInput struct {
	TaskID string `json:"taskId"`
}

type cancelExportTaskOutput struct{}

type cancelImportTaskInput struct {
	ImportID string `json:"importId"`
}

type cancelImportTaskOutput struct {
	CreationTime    *int64 `json:"creationTime,omitempty"`
	LastUpdatedTime *int64 `json:"lastUpdatedTime,omitempty"`
	ImportID        string `json:"importId,omitempty"`
	ImportStatus    string `json:"importStatus,omitempty"`
}

type createDeliveryInput struct {
	Tags                   map[string]string `json:"tags"`
	DeliverySourceName     string            `json:"deliverySourceName"`
	DeliveryDestinationArn string            `json:"deliveryDestinationArn"`
}

type createDeliveryOutput struct {
	Delivery *Delivery `json:"delivery,omitempty"`
}

type createExportTaskInput struct {
	TaskName            string `json:"taskName"`
	LogGroupName        string `json:"logGroupName"`
	LogStreamNamePrefix string `json:"logStreamNamePrefix"`
	Destination         string `json:"destination"`
	DestinationPrefix   string `json:"destinationPrefix"`
	From                int64  `json:"from"`
	To                  int64  `json:"to"`
}

type createExportTaskOutput struct {
	TaskID string `json:"taskId,omitempty"`
}

type createImportTaskInput struct {
	ImportRoleArn   string `json:"importRoleArn"`
	ImportSourceArn string `json:"importSourceArn"`
}

type createImportTaskOutput struct {
	CreationTime         *int64 `json:"creationTime,omitempty"`
	ImportID             string `json:"importId,omitempty"`
	ImportDestinationArn string `json:"importDestinationArn,omitempty"`
}

type createLogAnomalyDetectorInput struct {
	DetectorName          string   `json:"detectorName"`
	EvaluationFrequency   string   `json:"evaluationFrequency"`
	FilterPattern         string   `json:"filterPattern"`
	KmsKeyID              string   `json:"kmsKeyId"`
	LogGroupArnList       []string `json:"logGroupArnList"`
	AnomalyVisibilityTime int64    `json:"anomalyVisibilityTime"`
}

type createLogAnomalyDetectorOutput struct {
	AnomalyDetectorArn string `json:"anomalyDetectorArn,omitempty"`
}

type createScheduledQueryInput struct {
	Name               string `json:"name"`
	QueryString        string `json:"queryString"`
	ScheduleExpression string `json:"scheduleExpression"`
	ExecutionRoleArn   string `json:"executionRoleArn"`
	State              string `json:"state"`
}

type createScheduledQueryOutput struct {
	ScheduledQueryArn string `json:"scheduledQueryArn,omitempty"`
	State             string `json:"state,omitempty"`
}

type deleteAccountPolicyInput struct {
	PolicyName string `json:"policyName"`
	PolicyType string `json:"policyType"`
}

type deleteAccountPolicyOutput struct{}

// Handler is the Echo HTTP service handler for CloudWatch Logs operations.
type Handler struct {
	Backend StorageBackend
	ops     map[string]actionFn
	janitor *Janitor
	tags    map[string]*tags.Tags
	tagsMu  *lockmetrics.RWMutex
}

// NewHandler creates a new CloudWatch Logs handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{
		Backend: backend,
		tags:    make(map[string]*tags.Tags),
		tagsMu:  lockmetrics.New("cwl.tags"),
	}
	h.ops = h.buildOps()

	return h
}

// WithJanitor attaches a background janitor to the handler.
// The janitor periodically evicts log events that have aged past their log
// group's retention policy. interval=0 uses the default of one minute.
// The optional taskTimeout bounds each sweep; 0 means no per-task timeout.
func (h *Handler) WithJanitor(interval time.Duration, taskTimeout ...time.Duration) *Handler {
	if memBackend, ok := h.Backend.(*InMemoryBackend); ok {
		j := NewJanitor(memBackend, interval)
		if len(taskTimeout) > 0 {
			j.TaskTimeout = taskTimeout[0]
		}

		h.janitor = j
	}

	return h
}

// StartWorker starts the background janitor if it is configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = tags.New("cwl." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.RLock("removeTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t != nil {
		t.DeleteKeys(keys)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock("getTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t == nil {
		return map[string]string{}
	}

	return t.Clone()
}

// Name returns the service name.
func (h *Handler) Name() string { return "CloudWatchLogs" }

// GetSupportedOperations returns all mocked CloudWatch Logs operations.
//
//nolint:funlen // large service with many operations
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateLogGroup",
		"DeleteLogGroup",
		"DescribeLogGroups",
		"CreateLogStream",
		"DeleteLogStream",
		"DescribeLogStreams",
		"PutLogEvents",
		"GetLogEvents",
		"FilterLogEvents",
		"ListTagsLogGroup",
		"ListTagsForResource",
		"TagLogGroup",
		"UntagLogGroup",
		"TagResource",
		"UntagResource",
		"PutRetentionPolicy",
		"DeleteRetentionPolicy",
		"PutSubscriptionFilter",
		"DescribeSubscriptionFilters",
		"DeleteSubscriptionFilter",
		"StartQuery",
		"GetQueryResults",
		"StopQuery",
		"DescribeQueries",
		"AssociateKmsKey",
		"AssociateSourceToS3TableIntegration",
		"CancelExportTask",
		"CancelImportTask",
		"CreateDelivery",
		"CreateExportTask",
		"CreateImportTask",
		"CreateLogAnomalyDetector",
		"CreateScheduledQuery",
		"DeleteAccountPolicy",
		"DescribeExportTasks",
		"DescribeImportTasks",
		"DescribeDeliveries",
		"GetDelivery",
		"DeleteDelivery",
		"DeleteLogAnomalyDetector",
		"ListLogAnomalyDetectors",
		"UpdateLogAnomalyDetector",
		"DeleteScheduledQuery",
		"ListScheduledQueries",
		"UpdateScheduledQuery",
		"PutAccountPolicy",
		"DescribeAccountPolicies",
		"DisassociateKmsKey",
		"PutMetricFilter",
		"DescribeMetricFilters",
		"DeleteMetricFilter",
		"TestMetricFilter",
		"PutQueryDefinition",
		"DescribeQueryDefinitions",
		"DeleteQueryDefinition",
		"GetLogAnomalyDetector",
		"GetScheduledQuery",
		"GetLogGroupFields",
		"GetLogRecord",
		"ListAnomalies",
		"ListLogGroupsForQuery",
		"GetScheduledQueryHistory",
		"UpdateAnomaly",
		"ListLogGroups",
		// Completeness pass — previously notImplemented
		"DeleteDataProtectionPolicy",
		"DeleteDeliveryDestination",
		"DeleteDeliveryDestinationPolicy",
		"DeleteDeliverySource",
		"DeleteDestination",
		"DeleteIndexPolicy",
		"DeleteIntegration",
		"DeleteResourcePolicy",
		"DeleteTransformer",
		"DescribeConfigurationTemplates",
		"DescribeDeliveryDestinations",
		"DescribeDeliverySources",
		"DescribeDestinations",
		"DescribeFieldIndexes",
		"DescribeImportTaskBatches",
		"DescribeIndexPolicies",
		"DescribeResourcePolicies",
		"DisassociateSourceFromS3TableIntegration",
		"GetDataProtectionPolicy",
		"GetDeliveryDestination",
		"GetDeliveryDestinationPolicy",
		"GetDeliverySource",
		"GetIntegration",
		"GetLogFields",
		"GetLogObject",
		"GetTransformer",
		"ListAggregateLogGroupSummaries",
		"ListIntegrations",
		"ListSourcesForS3TableIntegration",
		"PutBearerTokenAuthentication",
		"PutDataProtectionPolicy",
		"PutDeliveryDestination",
		"PutDeliveryDestinationPolicy",
		"PutDeliverySource",
		"PutDestination",
		"PutDestinationPolicy",
		"PutIndexPolicy",
		"PutIntegration",
		"PutLogGroupDeletionProtection",
		"PutResourcePolicy",
		"PutTransformer",
		"StartLiveTail",
		"TestTransformer",
		"UpdateDeliveryConfiguration",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "logs" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CloudWatch Logs instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a matcher for CloudWatch Logs requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "Logs_20140328.")
	}
}

const cloudWatchLogsMatchPriority = 100

// MatchPriority returns the routing priority for the CloudWatch Logs handler.
func (h *Handler) MatchPriority() int { return cloudWatchLogsMatchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")
	const targetParts = 2
	if len(parts) == targetParts {
		return parts[1]
	}

	return "Unknown"
}

// ExtractResource extracts the resource name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	for _, key := range []string{"logGroupName", "logStreamName"} {
		if v, ok := data[key].(string); ok && v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for CloudWatch Logs requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		region := httputils.ExtractRegionFromRequest(c.Request(), config.DefaultRegion)
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)
		c.SetRequest(c.Request().WithContext(ctx))

		return service.HandleTarget(
			c, logger.Load(ctx),
			"CloudWatchLogs", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

type actionFn func(ctx context.Context, body []byte) (any, error)

type createLogGroupOutput struct{}

type deleteLogGroupOutput struct{}

type describeLogGroupsOutput struct {
	NextToken string     `json:"nextToken,omitempty"`
	LogGroups []LogGroup `json:"logGroups"`
}

type createLogStreamOutput struct{}

type deleteLogStreamOutput struct{}

type describeLogStreamsOutput struct {
	NextToken  string      `json:"nextToken,omitempty"`
	LogStreams []LogStream `json:"logStreams"`
}

type putLogEventsOutput struct {
	RejectedLogEventsInfo *RejectedLogEventsInfo `json:"rejectedLogEventsInfo,omitempty"`
	NextSequenceToken     string                 `json:"nextSequenceToken"`
}

type getLogEventsOutput struct {
	NextForwardToken  string           `json:"nextForwardToken"`
	NextBackwardToken string           `json:"nextBackwardToken"`
	Events            []OutputLogEvent `json:"events"`
}

type filterLogEventsOutput struct {
	NextToken string           `json:"nextToken,omitempty"`
	Events    []OutputLogEvent `json:"events"`
}

type listTagsLogGroupOutput struct {
	Tags map[string]string `json:"tags"`
}

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"tags"`
}

type tagLogGroupOutput struct{}

type untagLogGroupOutput struct{}

type tagResourceOutput struct{}

type untagResourceOutput struct{}

type putRetentionPolicyOutput struct{}

type deleteRetentionPolicyOutput struct{}

type putSubscriptionFilterOutput struct{}

type describeSubscriptionFiltersOutput struct {
	NextToken           string               `json:"nextToken,omitempty"`
	SubscriptionFilters []SubscriptionFilter `json:"subscriptionFilters"`
}

type deleteSubscriptionFilterOutput struct{}

// --- DescribeExportTasks ---.
type describeExportTasksInput struct {
	TaskID     string `json:"taskId"`
	StatusCode string `json:"statusCode"`
	NextToken  string `json:"nextToken"`
	Limit      int    `json:"limit"`
}

type describeExportTasksOutput struct {
	NextToken   string       `json:"nextToken,omitempty"`
	ExportTasks []ExportTask `json:"exportTasks"`
}

// --- DescribeImportTasks ---.
type describeImportTasksInput struct {
	TaskID    string `json:"taskId"`
	NextToken string `json:"nextToken"`
	Limit     int    `json:"limit"`
}

type describeImportTasksOutput struct {
	NextToken   string       `json:"nextToken,omitempty"`
	ImportTasks []ImportTask `json:"importTasks"`
}

// --- DescribeDeliveries ---.
type describeDeliveriesInput struct {
	NextToken string `json:"nextToken"`
	Limit     int    `json:"limit"`
}

type describeDeliveriesOutput struct {
	NextToken  string     `json:"nextToken,omitempty"`
	Deliveries []Delivery `json:"deliveries"`
}

// --- GetDelivery ---.
type getDeliveryInput struct {
	ID string `json:"id"`
}

type getDeliveryOutput struct {
	Delivery *Delivery `json:"delivery,omitempty"`
}

// --- DeleteDelivery ---.
type deleteDeliveryInput struct {
	ID string `json:"id"`
}

type deleteDeliveryOutput struct{}

// --- DeleteLogAnomalyDetector ---.
type deleteLogAnomalyDetectorInput struct {
	AnomalyDetectorArn string `json:"anomalyDetectorArn"`
}

type deleteLogAnomalyDetectorOutput struct{}

// --- ListLogAnomalyDetectors ---.
type listLogAnomalyDetectorsInput struct {
	NextToken             string   `json:"nextToken"`
	FilterLogGroupArnList []string `json:"filterLogGroupArnList"`
	Limit                 int      `json:"limit"`
}

type listLogAnomalyDetectorsOutput struct {
	NextToken        string               `json:"nextToken,omitempty"`
	AnomalyDetectors []LogAnomalyDetector `json:"anomalyDetectors"`
}

// --- UpdateLogAnomalyDetector ---.
type updateLogAnomalyDetectorInput struct {
	AnomalyDetectorArn    string `json:"anomalyDetectorArn"`
	EvaluationFrequency   string `json:"evaluationFrequency"`
	AnomalyVisibilityTime int64  `json:"anomalyVisibilityTime"`
}

type updateLogAnomalyDetectorOutput struct{}

// --- DeleteScheduledQuery ---.
type deleteScheduledQueryInput struct {
	ScheduledQueryArn string `json:"scheduledQueryArn"`
}

type deleteScheduledQueryOutput struct{}

// --- ListScheduledQueries ---.
type listScheduledQueriesInput struct {
	NextToken  string `json:"nextToken"`
	MaxResults int    `json:"maxResults"`
}

type listScheduledQueriesOutput struct {
	NextToken        string           `json:"nextToken,omitempty"`
	ScheduledQueries []ScheduledQuery `json:"scheduledQueries"`
}

// --- UpdateScheduledQuery ---.
type updateScheduledQueryInput struct {
	ScheduledQueryArn string `json:"scheduledQueryArn"`
	State             string `json:"state"`
}

type updateScheduledQueryOutput struct{}

// --- PutAccountPolicy ---.
type putAccountPolicyInput struct {
	PolicyDocument    string `json:"policyDocument"`
	PolicyName        string `json:"policyName"`
	PolicyType        string `json:"policyType"`
	Scope             string `json:"scope,omitempty"`
	SelectionCriteria string `json:"selectionCriteria,omitempty"`
}

type putAccountPolicyOutput struct {
	AccountPolicy *AccountPolicy `json:"accountPolicy,omitempty"`
}

// --- DescribeAccountPolicies ---.
type describeAccountPoliciesInput struct {
	PolicyName         string   `json:"policyName"`
	PolicyType         string   `json:"policyType"`
	NextToken          string   `json:"nextToken,omitempty"`
	AccountIdentifiers []string `json:"accountIdentifiers,omitempty"`
	MaxResults         int      `json:"maxResults,omitempty"`
}

type describeAccountPoliciesOutput struct {
	NextToken       string          `json:"nextToken,omitempty"`
	AccountPolicies []AccountPolicy `json:"accountPolicies"`
}

// --- DisassociateKmsKey ---.
type disassociateKmsKeyInput struct {
	LogGroupName       string `json:"logGroupName"`
	ResourceIdentifier string `json:"resourceIdentifier"`
}

type disassociateKmsKeyOutput struct{}

// --- PutMetricFilter ---.
type putMetricFilterInput struct {
	FilterPattern         string                 `json:"filterPattern"`
	FilterName            string                 `json:"filterName"`
	LogGroupName          string                 `json:"logGroupName"`
	MetricTransformations []MetricTransformation `json:"metricTransformations"`
}

type putMetricFilterOutput struct{}

// --- DescribeMetricFilters ---.
type describeMetricFiltersInput struct {
	FilterNamePrefix string `json:"filterNamePrefix"`
	LogGroupName     string `json:"logGroupName"`
	MetricName       string `json:"metricName"`
	MetricNamespace  string `json:"metricNamespace"`
	NextToken        string `json:"nextToken"`
	Limit            int    `json:"limit"`
}

type describeMetricFiltersOutput struct {
	NextToken     string         `json:"nextToken,omitempty"`
	MetricFilters []MetricFilter `json:"metricFilters"`
}

// --- DeleteMetricFilter ---.
type deleteMetricFilterInput struct {
	FilterName   string `json:"filterName"`
	LogGroupName string `json:"logGroupName"`
}

type deleteMetricFilterOutput struct{}

// --- TestMetricFilter ---.
type testMetricFilterInput struct {
	FilterPattern    string   `json:"filterPattern"`
	LogEventMessages []string `json:"logEventMessages"`
}

type testMetricFilterOutput struct {
	Matches []MetricFilterMatchRecord `json:"matches"`
}

// --- PutQueryDefinition ---.
type putQueryDefinitionInput struct {
	Name              string   `json:"name"`
	QueryDefinitionID string   `json:"queryDefinitionId"`
	QueryString       string   `json:"queryString"`
	LogGroupNames     []string `json:"logGroupNames"`
}

type putQueryDefinitionOutput struct {
	QueryDefinitionID string `json:"queryDefinitionId,omitempty"`
}

// --- DescribeQueryDefinitions ---.
type describeQueryDefinitionsInput struct {
	QueryDefinitionNamePrefix string `json:"queryDefinitionNamePrefix"`
	NextToken                 string `json:"nextToken"`
	MaxResults                int    `json:"maxResults"`
}

type describeQueryDefinitionsOutput struct {
	NextToken        string            `json:"nextToken,omitempty"`
	QueryDefinitions []QueryDefinition `json:"queryDefinitions"`
}

// --- DeleteQueryDefinition ---.
type deleteQueryDefinitionInput struct {
	QueryDefinitionID string `json:"queryDefinitionId"`
}

type deleteQueryDefinitionOutput struct {
	Success bool `json:"success"`
}

// --- GetLogAnomalyDetector ---.
type getLogAnomalyDetectorInput struct {
	AnomalyDetectorArn string `json:"anomalyDetectorArn"`
}

type getLogAnomalyDetectorOutput struct {
	AnomalyDetector *LogAnomalyDetector `json:"anomalyDetector,omitempty"`
}

// --- GetScheduledQuery ---.
type getScheduledQueryInput struct {
	ScheduledQueryArn string `json:"scheduledQueryArn"`
}

type getScheduledQueryOutput struct {
	ScheduledQuery *ScheduledQuery `json:"scheduledQuery,omitempty"`
}

// --- GetLogGroupFields ---.
type getLogGroupFieldsInput struct {
	LogGroupName string `json:"logGroupName"`
}

type getLogGroupFieldsOutput struct {
	LogGroupFields []LogGroupField `json:"logGroupFields"`
}

// --- GetLogRecord ---.
type getLogRecordInput struct {
	LogRecordPointer string `json:"logRecordPointer"`
}

type getLogRecordOutput struct {
	LogRecord map[string]string `json:"logRecord"`
}

// --- ListAnomalies ---.
type listAnomaliesInput struct {
	AnomalyDetectorArn string `json:"anomalyDetectorArn"`
	NextToken          string `json:"nextToken"`
	Limit              int    `json:"limit"`
}

type listAnomaliesOutput struct {
	NextToken string    `json:"nextToken,omitempty"`
	Anomalies []Anomaly `json:"anomalies"`
}

// --- ListLogGroupsForQuery ---.
type listLogGroupsForQueryInput struct {
	QueryID string `json:"queryId"`
}

type listLogGroupsForQueryOutput struct {
	LogGroupIdentifiers []string `json:"logGroupIdentifiers"`
}

// --- GetScheduledQueryHistory ---.
type getScheduledQueryHistoryInput struct {
	ScheduledQueryArn string `json:"scheduledQueryArn"`
	NextToken         string `json:"nextToken"`
	MaxResults        int    `json:"maxResults"`
}

type getScheduledQueryHistoryOutput struct {
	NextToken                  string                     `json:"nextToken,omitempty"`
	ScheduledQueryRunSummaries []ScheduledQueryRunSummary `json:"scheduledQueryRunSummaries"`
}

// --- UpdateAnomaly ---.
type updateAnomalyInput struct {
	AnomalyDetectorArn string `json:"anomalyDetectorArn"`
	AnomalyID          string `json:"anomalyId"`
	SuppressionType    string `json:"suppressionType"`
}

type updateAnomalyOutput struct{}

// --- ListLogGroups ---.
type listLogGroupsInput struct {
	LogGroupNamePrefix string `json:"logGroupNamePrefix"`
	NextToken          string `json:"nextToken"`
	Limit              int    `json:"limit"`
}

func (h *Handler) logGroupActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateLogGroup": func(ctx context.Context, b []byte) (any, error) {
			var input createLogGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if _, err := h.Backend.CreateLogGroup(ctx, input.LogGroupName, input.LogGroupClass, input.KmsKeyID); err != nil {
				return nil, err
			}
			if len(input.Tags) > 0 {
				h.setTags(input.LogGroupName, input.Tags)
			}

			return &createLogGroupOutput{}, nil
		},
		"DeleteLogGroup": func(ctx context.Context, b []byte) (any, error) {
			var input deleteLogGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteLogGroup(ctx, input.LogGroupName); err != nil {
				return nil, err
			}

			return &deleteLogGroupOutput{}, nil
		},
		"DescribeLogGroups": func(ctx context.Context, b []byte) (any, error) {
			var input describeLogGroupsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			groups, next, err := h.Backend.DescribeLogGroups(ctx, input.LogGroupNamePrefix, input.NextToken, input.Limit)
			if err != nil {
				return nil, err
			}

			return &describeLogGroupsOutput{LogGroups: groups, NextToken: next}, nil
		},
	}
}

func (h *Handler) logStreamActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateLogStream": func(ctx context.Context, b []byte) (any, error) {
			var input createLogStreamInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if _, err := h.Backend.CreateLogStream(ctx, input.LogGroupName, input.LogStreamName); err != nil {
				return nil, err
			}

			return &createLogStreamOutput{}, nil
		},
		"DeleteLogStream": func(ctx context.Context, b []byte) (any, error) {
			var input deleteLogStreamInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteLogStream(ctx, input.LogGroupName, input.LogStreamName); err != nil {
				return nil, err
			}

			return &deleteLogStreamOutput{}, nil
		},
		"DescribeLogStreams": func(ctx context.Context, b []byte) (any, error) {
			var input describeLogStreamsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			streams, next, err := h.Backend.DescribeLogStreams(ctx,
				input.LogGroupName,
				input.LogStreamNamePrefix,
				input.NextToken,
				input.OrderBy,
				input.Descending,
				input.Limit,
			)
			if err != nil {
				return nil, err
			}

			return &describeLogStreamsOutput{LogStreams: streams, NextToken: next}, nil
		},
	}
}

func (h *Handler) logEventActions() map[string]actionFn {
	return map[string]actionFn{
		"PutLogEvents": func(ctx context.Context, b []byte) (any, error) {
			var input putLogEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			result, err := h.Backend.PutLogEvents(ctx,
				input.LogGroupName,
				input.LogStreamName,
				input.SequenceToken,
				input.LogEvents,
			)
			if err != nil {
				return nil, err
			}

			return &putLogEventsOutput{
				NextSequenceToken:     result.NextSequenceToken,
				RejectedLogEventsInfo: result.RejectedLogEventsInfo,
			}, nil
		},
		"GetLogEvents": func(ctx context.Context, b []byte) (any, error) {
			var input getLogEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			evts, fwd, bwd, err := h.Backend.GetLogEvents(ctx,
				input.LogGroupName, input.LogStreamName, input.StartTime, input.EndTime,
				input.Limit, input.NextToken, input.StartFromHead)
			if err != nil {
				return nil, err
			}

			return &getLogEventsOutput{
				Events:            evts,
				NextForwardToken:  fwd,
				NextBackwardToken: bwd,
			}, nil
		},
		"FilterLogEvents": func(ctx context.Context, b []byte) (any, error) {
			var input filterLogEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			evts, next, err := h.Backend.FilterLogEvents(ctx,
				input.LogGroupName, input.LogStreamNames, input.FilterPattern,
				input.StartTime, input.EndTime, input.Limit, input.NextToken)
			if err != nil {
				return nil, err
			}

			return &filterLogEventsOutput{Events: evts, NextToken: next}, nil
		},
	}
}

func (h *Handler) logTagActions() map[string]actionFn {
	return map[string]actionFn{
		"ListTagsLogGroup": func(ctx context.Context, b []byte) (any, error) {
			var input listTagsLogGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &listTagsLogGroupOutput{Tags: h.getTags(input.LogGroupName)}, nil
		},
		"ListTagsForResource": func(ctx context.Context, b []byte) (any, error) {
			var input listTagsForResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &listTagsForResourceOutput{Tags: h.getTags(input.ResourceArn)}, nil
		},
		"TagLogGroup": func(ctx context.Context, b []byte) (any, error) {
			var input tagLogGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			var kv map[string]string
			if input.Tags != nil {
				kv = input.Tags.Clone()
			}
			h.setTags(input.LogGroupName, kv)

			return &tagLogGroupOutput{}, nil
		},
		"UntagLogGroup": func(ctx context.Context, b []byte) (any, error) {
			var input untagLogGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.removeTags(input.LogGroupName, input.Tags)

			return &untagLogGroupOutput{}, nil
		},
		"TagResource": func(ctx context.Context, b []byte) (any, error) {
			var input tagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.setTags(input.ResourceArn, maps.Clone(input.Tags))

			return &tagResourceOutput{}, nil
		},
		"UntagResource": func(ctx context.Context, b []byte) (any, error) {
			var input untagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.removeTags(input.ResourceArn, input.TagKeys)

			return &untagResourceOutput{}, nil
		},
	}
}

func (h *Handler) retentionActions() map[string]actionFn {
	return map[string]actionFn{
		"PutRetentionPolicy": func(ctx context.Context, b []byte) (any, error) {
			var input putRetentionPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			days := input.RetentionInDays
			if err := h.Backend.SetRetentionPolicy(ctx, input.LogGroupName, &days); err != nil {
				return nil, err
			}

			return &putRetentionPolicyOutput{}, nil
		},
		"DeleteRetentionPolicy": func(ctx context.Context, b []byte) (any, error) {
			var input deleteRetentionPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.SetRetentionPolicy(ctx, input.LogGroupName, nil); err != nil {
				return nil, err
			}

			return &deleteRetentionPolicyOutput{}, nil
		},
	}
}

func (h *Handler) subscriptionFilterActions() map[string]actionFn {
	return map[string]actionFn{
		"PutSubscriptionFilter": func(ctx context.Context, b []byte) (any, error) {
			var input putSubscriptionFilterInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.PutSubscriptionFilter(ctx,
				input.LogGroupName, input.FilterName, input.FilterPattern, input.DestinationArn,
				input.RoleArn, input.Distribution,
			); err != nil {
				return nil, err
			}

			return &putSubscriptionFilterOutput{}, nil
		},
		"DescribeSubscriptionFilters": func(ctx context.Context, b []byte) (any, error) {
			var input describeSubscriptionFiltersInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			filters, next, err := h.Backend.DescribeSubscriptionFilters(ctx,
				input.LogGroupName, input.FilterNamePrefix, input.NextToken, input.Limit,
			)
			if err != nil {
				return nil, err
			}

			return &describeSubscriptionFiltersOutput{SubscriptionFilters: filters, NextToken: next}, nil
		},
		"DeleteSubscriptionFilter": func(ctx context.Context, b []byte) (any, error) {
			var input deleteSubscriptionFilterInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteSubscriptionFilter(ctx, input.LogGroupName, input.FilterName); err != nil {
				return nil, err
			}

			return &deleteSubscriptionFilterOutput{}, nil
		},
	}
}

// normalizeLogGroupIdentifier converts a log group identifier to a log group name.
// Log group identifiers may be ARNs (arn:...:log-group:<name>); in that case the
// log group name is extracted. Non-ARN identifiers are returned unchanged.
func normalizeLogGroupIdentifier(id string) string {
	const logGroupToken = ":log-group:"
	if idx := strings.LastIndex(id, logGroupToken); idx >= 0 {
		return id[idx+len(logGroupToken):]
	}

	return id
}

func (h *Handler) handleStartQuery(ctx context.Context, b []byte) (any, error) {
	var input startQueryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	logGroups := input.LogGroupNames
	if len(logGroups) == 0 && input.LogGroupName != "" {
		logGroups = []string{input.LogGroupName}
	}
	for _, id := range input.LogGroupIdentifiers {
		logGroups = append(logGroups, normalizeLogGroupIdentifier(id))
	}

	queryID := uuid.New().String()
	if _, err := h.Backend.StartQuery(ctx,
		queryID,
		input.QueryString,
		logGroups,
		input.StartTime,
		input.EndTime,
	); err != nil {
		return nil, err
	}

	return &startQueryOutput{QueryID: queryID}, nil
}

func (h *Handler) handleGetQueryResults(ctx context.Context, b []byte) (any, error) {
	var input getQueryResultsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	results, stats, status, err := h.Backend.GetQueryResults(input.QueryID)
	if err != nil {
		return nil, err
	}

	return &getQueryResultsOutput{Results: results, Statistics: stats, Status: status}, nil
}

func (h *Handler) handleStopQuery(ctx context.Context, b []byte) (any, error) {
	var input stopQueryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.StopQuery(input.QueryID); err != nil {
		return nil, err
	}

	return &stopQueryOutput{Success: true}, nil
}

func (h *Handler) handleDescribeQueries(ctx context.Context, b []byte) (any, error) {
	var input describeQueriesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	queries, next, err := h.Backend.DescribeQueries(
		input.LogGroupName, input.Status, input.NextToken, input.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	return &describeQueriesOutput{Queries: queries, NextToken: next}, nil
}

func (h *Handler) insightsActions() map[string]actionFn {
	return map[string]actionFn{
		"StartQuery":      h.handleStartQuery,
		"GetQueryResults": h.handleGetQueryResults,
		"StopQuery":       h.handleStopQuery,
		"DescribeQueries": h.handleDescribeQueries,
	}
}

func (h *Handler) handleAssociateKmsKey(ctx context.Context, b []byte) (any, error) {
	var input associateKmsKeyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.AssociateKmsKey(input.LogGroupName, input.ResourceIdentifier, input.KmsKeyID); err != nil {
		return nil, err
	}

	return &associateKmsKeyOutput{}, nil
}

func (h *Handler) handleAssociateSourceToS3TableIntegration(ctx context.Context, b []byte) (any, error) {
	var input associateSourceToS3TableIntegrationInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	var dataSourceName, dataSourceType string
	if input.DataSource != nil {
		if v, ok := input.DataSource["name"].(string); ok {
			dataSourceName = v
		}
		if v, ok := input.DataSource["type"].(string); ok {
			dataSourceType = v
		}
	}

	id, err := h.Backend.AssociateSourceToS3TableIntegration(input.IntegrationArn, dataSourceName, dataSourceType)
	if err != nil {
		return nil, err
	}

	return &associateSourceToS3TableIntegrationOutput{Identifier: id}, nil
}

func (h *Handler) handleCancelExportTask(ctx context.Context, b []byte) (any, error) {
	var input cancelExportTaskInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.CancelExportTask(input.TaskID); err != nil {
		return nil, err
	}

	return &cancelExportTaskOutput{}, nil
}

func (h *Handler) handleCancelImportTask(ctx context.Context, b []byte) (any, error) {
	var input cancelImportTaskInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	task, err := h.Backend.CancelImportTask(input.ImportID)
	if err != nil {
		return nil, err
	}

	return &cancelImportTaskOutput{
		ImportID:        task.ImportID,
		ImportStatus:    task.Status,
		CreationTime:    &task.CreationTime,
		LastUpdatedTime: &task.LastUpdatedTime,
	}, nil
}

func (h *Handler) handleCreateDelivery(ctx context.Context, b []byte) (any, error) {
	var input createDeliveryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	delivery, err := h.Backend.CreateDelivery(input.DeliverySourceName, input.DeliveryDestinationArn, input.Tags)
	if err != nil {
		return nil, err
	}

	return &createDeliveryOutput{Delivery: delivery}, nil
}

func (h *Handler) handleCreateExportTask(ctx context.Context, b []byte) (any, error) {
	var input createExportTaskInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	taskID, err := h.Backend.CreateExportTask(
		input.TaskName, input.LogGroupName, input.LogStreamNamePrefix,
		input.Destination, input.DestinationPrefix, input.From, input.To,
	)
	if err != nil {
		return nil, err
	}

	return &createExportTaskOutput{TaskID: taskID}, nil
}

func (h *Handler) handleCreateImportTask(ctx context.Context, b []byte) (any, error) {
	var input createImportTaskInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	task, err := h.Backend.CreateImportTask(input.ImportRoleArn, input.ImportSourceArn)
	if err != nil {
		return nil, err
	}

	return &createImportTaskOutput{
		ImportID:             task.ImportID,
		ImportDestinationArn: task.ImportDestinationArn,
		CreationTime:         &task.CreationTime,
	}, nil
}

func (h *Handler) handleCreateLogAnomalyDetector(ctx context.Context, b []byte) (any, error) {
	var input createLogAnomalyDetectorInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	detectorArn, err := h.Backend.CreateLogAnomalyDetector(
		input.LogGroupArnList, input.DetectorName, input.EvaluationFrequency,
		input.FilterPattern, input.KmsKeyID, input.AnomalyVisibilityTime,
	)
	if err != nil {
		return nil, err
	}

	return &createLogAnomalyDetectorOutput{AnomalyDetectorArn: detectorArn}, nil
}

func (h *Handler) handleCreateScheduledQuery(ctx context.Context, b []byte) (any, error) {
	var input createScheduledQueryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	queryArn, err := h.Backend.CreateScheduledQuery(
		input.Name, input.QueryString, input.ScheduleExpression, input.ExecutionRoleArn, input.State,
	)
	if err != nil {
		return nil, err
	}

	// The backend defaults an empty state to statusEnabled; reflect the effective value in the response.
	effectiveState := input.State
	if effectiveState == "" {
		effectiveState = statusEnabled
	}

	return &createScheduledQueryOutput{ScheduledQueryArn: queryArn, State: effectiveState}, nil
}

func (h *Handler) handleDeleteAccountPolicy(ctx context.Context, b []byte) (any, error) {
	var input deleteAccountPolicyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteAccountPolicy(input.PolicyName, input.PolicyType); err != nil {
		return nil, err
	}

	return &deleteAccountPolicyOutput{}, nil
}

func (h *Handler) handleDescribeExportTasks(ctx context.Context, b []byte) (any, error) {
	var input describeExportTasksInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	tasks, next, err := h.Backend.DescribeExportTasks(input.TaskID, input.StatusCode, input.Limit, input.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeExportTasksOutput{ExportTasks: tasks, NextToken: next}, nil
}

func (h *Handler) handleDescribeImportTasks(ctx context.Context, b []byte) (any, error) {
	var input describeImportTasksInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	tasks, next, err := h.Backend.DescribeImportTasks(input.TaskID, input.Limit, input.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeImportTasksOutput{ImportTasks: tasks, NextToken: next}, nil
}

func (h *Handler) handleDescribeDeliveries(ctx context.Context, b []byte) (any, error) {
	var input describeDeliveriesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	deliveries, next, err := h.Backend.DescribeDeliveries(input.Limit, input.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeDeliveriesOutput{Deliveries: deliveries, NextToken: next}, nil
}

func (h *Handler) handleGetDelivery(ctx context.Context, b []byte) (any, error) {
	var input getDeliveryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	d, err := h.Backend.GetDelivery(input.ID)
	if err != nil {
		return nil, err
	}

	return &getDeliveryOutput{Delivery: d}, nil
}

func (h *Handler) handleDeleteDelivery(ctx context.Context, b []byte) (any, error) {
	var input deleteDeliveryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.DeleteDelivery(input.ID); err != nil {
		return nil, err
	}

	return &deleteDeliveryOutput{}, nil
}

func (h *Handler) handleDeleteLogAnomalyDetector(ctx context.Context, b []byte) (any, error) {
	var input deleteLogAnomalyDetectorInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.DeleteLogAnomalyDetector(input.AnomalyDetectorArn); err != nil {
		return nil, err
	}

	return &deleteLogAnomalyDetectorOutput{}, nil
}

func (h *Handler) handleListLogAnomalyDetectors(ctx context.Context, b []byte) (any, error) {
	var input listLogAnomalyDetectorsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	detectors, next, err := h.Backend.ListLogAnomalyDetectors(input.FilterLogGroupArnList, input.Limit, input.NextToken)
	if err != nil {
		return nil, err
	}

	return &listLogAnomalyDetectorsOutput{AnomalyDetectors: detectors, NextToken: next}, nil
}

func (h *Handler) handleUpdateLogAnomalyDetector(ctx context.Context, b []byte) (any, error) {
	var input updateLogAnomalyDetectorInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.UpdateLogAnomalyDetector(
		input.AnomalyDetectorArn,
		input.EvaluationFrequency,
		input.AnomalyVisibilityTime,
	); err != nil {
		return nil, err
	}

	return &updateLogAnomalyDetectorOutput{}, nil
}

func (h *Handler) handleDeleteScheduledQuery(ctx context.Context, b []byte) (any, error) {
	var input deleteScheduledQueryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.DeleteScheduledQuery(input.ScheduledQueryArn); err != nil {
		return nil, err
	}

	return &deleteScheduledQueryOutput{}, nil
}

func (h *Handler) handleListScheduledQueries(ctx context.Context, b []byte) (any, error) {
	var input listScheduledQueriesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	queries, next, err := h.Backend.ListScheduledQueries(input.MaxResults, input.NextToken)
	if err != nil {
		return nil, err
	}

	return &listScheduledQueriesOutput{ScheduledQueries: queries, NextToken: next}, nil
}

func (h *Handler) handleUpdateScheduledQuery(ctx context.Context, b []byte) (any, error) {
	var input updateScheduledQueryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.UpdateScheduledQuery(input.ScheduledQueryArn, input.State); err != nil {
		return nil, err
	}

	return &updateScheduledQueryOutput{}, nil
}

func (h *Handler) handlePutAccountPolicy(ctx context.Context, b []byte) (any, error) {
	var input putAccountPolicyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	policy, err := h.Backend.PutAccountPolicy(
		input.PolicyName,
		input.PolicyType,
		input.PolicyDocument,
		input.Scope,
		input.SelectionCriteria,
	)
	if err != nil {
		return nil, err
	}

	return &putAccountPolicyOutput{AccountPolicy: policy}, nil
}

func (h *Handler) handleDescribeAccountPolicies(ctx context.Context, b []byte) (any, error) {
	var input describeAccountPoliciesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	policies, nextToken, err := h.Backend.DescribeAccountPolicies(
		input.PolicyType,
		input.PolicyName,
		input.AccountIdentifiers,
		input.MaxResults,
		input.NextToken,
	)
	if err != nil {
		return nil, err
	}

	return &describeAccountPoliciesOutput{AccountPolicies: policies, NextToken: nextToken}, nil
}

func (h *Handler) handleDisassociateKmsKey(ctx context.Context, b []byte) (any, error) {
	var input disassociateKmsKeyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.DisassociateKmsKey(input.LogGroupName, input.ResourceIdentifier); err != nil {
		return nil, err
	}

	return &disassociateKmsKeyOutput{}, nil
}

func (h *Handler) handlePutMetricFilter(ctx context.Context, b []byte) (any, error) {
	var input putMetricFilterInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.PutMetricFilter(ctx,
		input.LogGroupName,
		input.FilterName,
		input.FilterPattern,
		input.MetricTransformations,
	); err != nil {
		return nil, err
	}

	return &putMetricFilterOutput{}, nil
}

func (h *Handler) handleDescribeMetricFilters(ctx context.Context, b []byte) (any, error) {
	var input describeMetricFiltersInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	filters, next, err := h.Backend.DescribeMetricFilters(ctx,
		input.LogGroupName,
		input.FilterNamePrefix,
		input.MetricName,
		input.MetricNamespace,
		input.NextToken,
		input.Limit,
	)
	if err != nil {
		return nil, err
	}

	return &describeMetricFiltersOutput{MetricFilters: filters, NextToken: next}, nil
}

func (h *Handler) handleDeleteMetricFilter(ctx context.Context, b []byte) (any, error) {
	var input deleteMetricFilterInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.DeleteMetricFilter(ctx, input.LogGroupName, input.FilterName); err != nil {
		return nil, err
	}

	return &deleteMetricFilterOutput{}, nil
}

func (h *Handler) handleTestMetricFilter(ctx context.Context, b []byte) (any, error) {
	var input testMetricFilterInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	matches, err := h.Backend.TestMetricFilter(input.FilterPattern, input.LogEventMessages)
	if err != nil {
		return nil, err
	}

	return &testMetricFilterOutput{Matches: matches}, nil
}

func (h *Handler) handlePutQueryDefinition(ctx context.Context, b []byte) (any, error) {
	var input putQueryDefinitionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	id, err := h.Backend.PutQueryDefinition(input.Name, input.QueryString, input.QueryDefinitionID, input.LogGroupNames)
	if err != nil {
		return nil, err
	}

	return &putQueryDefinitionOutput{QueryDefinitionID: id}, nil
}

func (h *Handler) handleDescribeQueryDefinitions(ctx context.Context, b []byte) (any, error) {
	var input describeQueryDefinitionsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	defs, next, err := h.Backend.DescribeQueryDefinitions(
		input.QueryDefinitionNamePrefix,
		input.MaxResults,
		input.NextToken,
	)
	if err != nil {
		return nil, err
	}

	return &describeQueryDefinitionsOutput{QueryDefinitions: defs, NextToken: next}, nil
}

func (h *Handler) handleDeleteQueryDefinition(ctx context.Context, b []byte) (any, error) {
	var input deleteQueryDefinitionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.DeleteQueryDefinition(input.QueryDefinitionID); err != nil {
		return nil, err
	}

	return &deleteQueryDefinitionOutput{Success: true}, nil
}

func (h *Handler) handleGetLogAnomalyDetector(ctx context.Context, b []byte) (any, error) {
	var input getLogAnomalyDetectorInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	d, err := h.Backend.GetLogAnomalyDetector(input.AnomalyDetectorArn)
	if err != nil {
		return nil, err
	}

	return &getLogAnomalyDetectorOutput{AnomalyDetector: d}, nil
}

func (h *Handler) handleGetScheduledQuery(ctx context.Context, b []byte) (any, error) {
	var input getScheduledQueryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	sq, err := h.Backend.GetScheduledQuery(input.ScheduledQueryArn)
	if err != nil {
		return nil, err
	}

	return &getScheduledQueryOutput{ScheduledQuery: sq}, nil
}

func (h *Handler) handleGetLogGroupFields(ctx context.Context, b []byte) (any, error) {
	var input getLogGroupFieldsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	fields, err := h.Backend.GetLogGroupFields(ctx, input.LogGroupName)
	if err != nil {
		return nil, err
	}

	return &getLogGroupFieldsOutput{LogGroupFields: fields}, nil
}

func (h *Handler) handleGetLogRecord(ctx context.Context, b []byte) (any, error) {
	var input getLogRecordInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	record, err := h.Backend.GetLogRecord(ctx, input.LogRecordPointer)
	if err != nil {
		return nil, err
	}

	return &getLogRecordOutput{LogRecord: record}, nil
}

func (h *Handler) handleListAnomalies(ctx context.Context, b []byte) (any, error) {
	var input listAnomaliesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	anomalies, next, err := h.Backend.ListAnomalies(input.AnomalyDetectorArn, input.Limit, input.NextToken)
	if err != nil {
		return nil, err
	}

	return &listAnomaliesOutput{Anomalies: anomalies, NextToken: next}, nil
}

func (h *Handler) handleListLogGroupsForQuery(ctx context.Context, b []byte) (any, error) {
	var input listLogGroupsForQueryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	groups, err := h.Backend.ListLogGroupsForQuery(input.QueryID)
	if err != nil {
		return nil, err
	}

	return &listLogGroupsForQueryOutput{LogGroupIdentifiers: groups}, nil
}

func (h *Handler) handleGetScheduledQueryHistory(ctx context.Context, b []byte) (any, error) {
	var input getScheduledQueryHistoryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	summaries, next, err := h.Backend.GetScheduledQueryHistory(
		input.ScheduledQueryArn,
		input.NextToken,
		input.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	return &getScheduledQueryHistoryOutput{ScheduledQueryRunSummaries: summaries, NextToken: next}, nil
}

func (h *Handler) handleUpdateAnomaly(ctx context.Context, b []byte) (any, error) {
	var input updateAnomalyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.UpdateAnomaly(input.AnomalyID, input.AnomalyDetectorArn, input.SuppressionType); err != nil {
		return nil, err
	}

	return &updateAnomalyOutput{}, nil
}

func (h *Handler) handleListLogGroups(ctx context.Context, b []byte) (any, error) {
	var input listLogGroupsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	groups, next, err := h.Backend.ListLogGroups(ctx, input.LogGroupNamePrefix, input.NextToken, input.Limit)
	if err != nil {
		return nil, err
	}

	return &describeLogGroupsOutput{LogGroups: groups, NextToken: next}, nil
}

func (h *Handler) newOperationsActions() map[string]actionFn {
	return map[string]actionFn{
		"AssociateKmsKey":                     h.handleAssociateKmsKey,
		"AssociateSourceToS3TableIntegration": h.handleAssociateSourceToS3TableIntegration,
		"CancelExportTask":                    h.handleCancelExportTask,
		"CancelImportTask":                    h.handleCancelImportTask,
		"CreateDelivery":                      h.handleCreateDelivery,
		"CreateExportTask":                    h.handleCreateExportTask,
		"CreateImportTask":                    h.handleCreateImportTask,
		"CreateLogAnomalyDetector":            h.handleCreateLogAnomalyDetector,
		"CreateScheduledQuery":                h.handleCreateScheduledQuery,
		"DeleteAccountPolicy":                 h.handleDeleteAccountPolicy,
		"DescribeExportTasks":                 h.handleDescribeExportTasks,
		"DescribeImportTasks":                 h.handleDescribeImportTasks,
		"DescribeDeliveries":                  h.handleDescribeDeliveries,
		"GetDelivery":                         h.handleGetDelivery,
		"DeleteDelivery":                      h.handleDeleteDelivery,
		"DeleteLogAnomalyDetector":            h.handleDeleteLogAnomalyDetector,
		"ListLogAnomalyDetectors":             h.handleListLogAnomalyDetectors,
		"UpdateLogAnomalyDetector":            h.handleUpdateLogAnomalyDetector,
		"DeleteScheduledQuery":                h.handleDeleteScheduledQuery,
		"ListScheduledQueries":                h.handleListScheduledQueries,
		"UpdateScheduledQuery":                h.handleUpdateScheduledQuery,
		"PutAccountPolicy":                    h.handlePutAccountPolicy,
		"DescribeAccountPolicies":             h.handleDescribeAccountPolicies,
		"DisassociateKmsKey":                  h.handleDisassociateKmsKey,
		"PutMetricFilter":                     h.handlePutMetricFilter,
		"DescribeMetricFilters":               h.handleDescribeMetricFilters,
		"DeleteMetricFilter":                  h.handleDeleteMetricFilter,
		"TestMetricFilter":                    h.handleTestMetricFilter,
		"PutQueryDefinition":                  h.handlePutQueryDefinition,
		"DescribeQueryDefinitions":            h.handleDescribeQueryDefinitions,
		"DeleteQueryDefinition":               h.handleDeleteQueryDefinition,
		"GetLogAnomalyDetector":               h.handleGetLogAnomalyDetector,
		"GetScheduledQuery":                   h.handleGetScheduledQuery,
		"GetLogGroupFields":                   h.handleGetLogGroupFields,
		"GetLogRecord":                        h.handleGetLogRecord,
		"ListAnomalies":                       h.handleListAnomalies,
		"ListLogGroupsForQuery":               h.handleListLogGroupsForQuery,
		"GetScheduledQueryHistory":            h.handleGetScheduledQueryHistory,
		"UpdateAnomaly":                       h.handleUpdateAnomaly,
		"ListLogGroups":                       h.handleListLogGroups,
	}
}

func (h *Handler) buildOps() map[string]actionFn {
	table := make(map[string]actionFn)
	maps.Copy(table, h.logGroupActions())
	maps.Copy(table, h.logStreamActions())
	maps.Copy(table, h.logEventActions())
	maps.Copy(table, h.logTagActions())
	maps.Copy(table, h.retentionActions())
	maps.Copy(table, h.subscriptionFilterActions())
	maps.Copy(table, h.insightsActions())
	maps.Copy(table, h.newOperationsActions())
	maps.Copy(table, h.completenessActions())

	return table
}

// dispatch routes the action to the correct handler function.
func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w:%s", errUnknownOperation, action)
	}

	response, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(response)
}

// handleError writes a standardized JSON error response.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	var errType string
	var statusCode int

	switch {
	case errors.Is(reqErr, ErrLogGroupNotFound), errors.Is(reqErr, ErrLogStreamNotFound),
		errors.Is(reqErr, ErrSubscriptionFilterNotFound), errors.Is(reqErr, ErrQueryNotFound),
		errors.Is(reqErr, ErrExportTaskNotFound), errors.Is(reqErr, ErrImportTaskNotFound),
		errors.Is(reqErr, ErrDeliveryNotFound), errors.Is(reqErr, ErrLogAnomalyDetectorNotFound),
		errors.Is(reqErr, ErrScheduledQueryNotFound), errors.Is(reqErr, ErrMetricFilterNotFound),
		errors.Is(reqErr, ErrQueryDefinitionNotFound),
		errors.Is(reqErr, ErrResourcePolicyNotFound), errors.Is(reqErr, ErrDeliveryDestinationNotFound),
		errors.Is(reqErr, ErrDeliverySourceNotFound), errors.Is(reqErr, ErrDestinationNotFound),
		errors.Is(reqErr, ErrIndexPolicyNotFound), errors.Is(reqErr, ErrTransformerNotFound),
		errors.Is(reqErr, ErrIntegrationNotFound):
		errType = "ResourceNotFoundException"
		statusCode = http.StatusNotFound
	case errors.Is(reqErr, ErrLogGroupAlreadyExists), errors.Is(reqErr, ErrLogStreamAlreadyExist):
		errType = "ResourceAlreadyExistsException"
		statusCode = http.StatusConflict
	case errors.Is(reqErr, ErrSubscriptionFilterLimitExceed):
		errType = "LimitExceededException"
		statusCode = http.StatusBadRequest
	case errors.Is(reqErr, ErrInvalidSequenceToken):
		errType = "InvalidSequenceTokenException"
		statusCode = http.StatusBadRequest
	case errors.Is(reqErr, ErrOperationAborted):
		errType = "OperationAbortedException"
		statusCode = http.StatusBadRequest
	case errors.Is(reqErr, ErrValidation):
		errType = "InvalidParameterException"
		statusCode = http.StatusBadRequest
	case errors.Is(reqErr, errUnknownOperation):
		errType = "UnknownOperationException"
		statusCode = http.StatusBadRequest
	default:
		errType = "InternalServerError"
		statusCode = http.StatusInternalServerError
	}

	if statusCode == http.StatusInternalServerError {
		log.ErrorContext(ctx, "CloudWatchLogs internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "CloudWatchLogs request error", "error", reqErr, "action", action)
	}

	errResp := service.JSONErrorResponse{
		Type:    errType,
		Message: reqErr.Error(),
	}

	payload, _ := json.Marshal(errResp)

	return c.JSONBlob(statusCode, payload)
}

// Reset clears all in-memory state from the backend and the handler-level tag
// store. It is used by the POST /_gopherstack/reset endpoint for CI pipelines
// and rapid local development.
func (h *Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}

	// Clear handler-level tag state so that tags don't bleed across test runs.
	h.tagsMu.Lock("Reset")
	defer h.tagsMu.Unlock()

	for _, t := range h.tags {
		t.Close()
	}

	h.tags = make(map[string]*tags.Tags)
}
