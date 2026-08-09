package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	opBatchAssociateScramSecret     = "BatchAssociateScramSecret"
	opBatchDisassociateScramSecret  = "BatchDisassociateScramSecret"
	opCreateChannel                 = "CreateChannel"
	opCreateCluster                 = "CreateCluster"
	opCreateClusterV2               = "CreateClusterV2"
	opCreateConfiguration           = "CreateConfiguration"
	opCreateReplicator              = "CreateReplicator"
	opCreateTopic                   = "CreateTopic"
	opCreateVpcConnection           = "CreateVpcConnection"
	opDeleteChannel                 = "DeleteChannel"
	opDeleteCluster                 = "DeleteCluster"
	opDeleteClusterPolicy           = "DeleteClusterPolicy"
	opDeleteConfiguration           = "DeleteConfiguration"
	opDeleteReplicator              = "DeleteReplicator"
	opDeleteTopic                   = "DeleteTopic"
	opDeleteVpcConnection           = "DeleteVpcConnection"
	opDescribeChannel               = "DescribeChannel"
	opDescribeCluster               = "DescribeCluster"
	opDescribeClusterOperation      = "DescribeClusterOperation"
	opDescribeClusterOperationV2    = "DescribeClusterOperationV2"
	opDescribeClusterV2             = "DescribeClusterV2"
	opDescribeConfiguration         = "DescribeConfiguration"
	opDescribeConfigurationRevision = "DescribeConfigurationRevision"
	opDescribeReplicator            = "DescribeReplicator"
	opDescribeTopic                 = "DescribeTopic"
	opDescribeTopicPartitions       = "DescribeTopicPartitions"
	opDescribeVpcConnection         = "DescribeVpcConnection"
	opGetBootstrapBrokers           = "GetBootstrapBrokers"
	opGetClusterPolicy              = "GetClusterPolicy"
	opGetCompatibleKafkaVersions    = "GetCompatibleKafkaVersions"
	opListChannels                  = "ListChannels"
	opListClientVpcConnections      = "ListClientVpcConnections"
	opListClusterOperations         = "ListClusterOperations"
	opListClusterOperationsV2       = "ListClusterOperationsV2"
	opListClusters                  = "ListClusters"
	opListClustersV2                = "ListClustersV2"
	opListConfigurationRevisions    = "ListConfigurationRevisions"
	opListConfigurations            = "ListConfigurations"
	opListKafkaVersions             = "ListKafkaVersions"
	opListNodes                     = "ListNodes"
	opListReplicators               = "ListReplicators"
	opListScramSecrets              = "ListScramSecrets"
	opListTagsForResource           = "ListTagsForResource"
	opListTopics                    = "ListTopics"
	opListVpcConnections            = "ListVpcConnections"
	opPutClusterPolicy              = "PutClusterPolicy"
	opRebootBroker                  = "RebootBroker"
	opRejectClientVpcConnection     = "RejectClientVpcConnection"
	opTagResource                   = "TagResource"
	opUntagResource                 = "UntagResource"
	opUpdateBrokerCount             = "UpdateBrokerCount"
	opUpdateBrokerStorage           = "UpdateBrokerStorage"
	opUpdateBrokerType              = "UpdateBrokerType"
	opUpdateChannel                 = "UpdateChannel"
	opUpdateClusterConfiguration    = "UpdateClusterConfiguration"
	opUpdateClusterKafkaVersion     = "UpdateClusterKafkaVersion"
	opUpdateConfiguration           = "UpdateConfiguration"
	opUpdateConnectivity            = "UpdateConnectivity"
	opUpdateMonitoring              = "UpdateMonitoring"
	opUpdateRebalancing             = "UpdateRebalancing"
	opUpdateReplicationInfo         = "UpdateReplicationInfo"
	opUpdateSecurity                = "UpdateSecurity"
	opUpdateStorage                 = "UpdateStorage"
	opUpdateTopic                   = "UpdateTopic"
)

// kafkaMatchPriority must be strictly higher than service.PriorityPathVersioned (85).
// AppSync also matches at PriorityPathVersioned via an ARN-agnostic "/v1/tags" path
// prefix (it tags GraphqlApi/Api resources at that same real AWS path) and is
// registered before Kafka in cli.go, so a tied priority lets AppSync's matcher win
// every "/v1/tags/{arn}" request -- including Kafka's own TagResource/
// UntagResource/ListTagsForResource, whose ARNs (arn:aws:kafka:...) AppSync then
// rejects with "invalid resourceArn". Kafka must be evaluated first.
const kafkaMatchPriority = service.PriorityPathVersioned + 1

const (
	clustersV1Prefix            = "/v1/clusters/"
	clustersV2Prefix            = "/api/v2/clusters/"
	configurationsPrefix        = "/v1/configurations/"
	tagsPrefix                  = "/v1/tags/"
	bootstrapBrokersSuffix      = "/bootstrap-brokers"
	scramSecretsSuffix          = "/scram-secrets"
	channelsSuffix              = "/channels"
	topicsSuffix                = "/topics"
	topicPartitionsSuffix       = "/partitions"
	policySuffix                = "/policy"
	operationsPrefix            = "/v1/operations/"
	operationsV2Prefix          = "/api/v2/operations/"
	replicatorsPrefix           = "/replication/v1/replicators/"
	replicatorsRoot             = "/replication/v1/replicators"
	replicationInfoSuffix       = "/replication-info"
	vpcConnectionPrefix         = "/v1/vpc-connection/"
	vpcConnectionRoot           = "/v1/vpc-connection"
	vpcConnectionsListPath      = "/v1/vpc-connections"
	kafkaVersionsRoot           = "/v1/kafka-versions"
	compatibleKafkaVersionsPath = "/v1/compatible-kafka-versions"
	nodesSuffix                 = "/nodes"
	rebootBrokerSuffix          = "/reboot-broker"
	clientVpcConnectionSuffix   = "/client-vpc-connections"
	clientVpcConnectionSingular = "/client-vpc-connection"
	revisionsSuffix             = "/revisions"
	brokerCountUpdateSuffix     = "/nodes/count"
	brokerStorageUpdateSuffix   = "/nodes/storage"
	brokerTypeUpdateSuffix      = "/nodes/type"
	clusterConfigUpdateSuffix   = "/configuration"
	clusterKafkaVersionSuffix   = "/version"
	connectivityUpdateSuffix    = "/connectivity"
	monitoringUpdateSuffix      = "/monitoring"
	rebalancingUpdateSuffix     = "/rebalancing"
	securityUpdateSuffix        = "/security"
	storageUpdateSuffix         = "/storage"

	arnMaxParts           = 6 // arn:partition:service:region:account:resource
	arnMinPartsForService = 3 // minimum ARN parts needed to read service field at index 2

	topicKeySeparator      = "|" // separator between clusterArn and topicName in resource field
	topicKeySeparatorParts = 2   // number of parts after splitting on topicKeySeparator
)

// Handler is the HTTP handler for the MSK REST API.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new Kafka handler backed by backend.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Kafka" }

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// GetSupportedOperations returns the list of supported MSK operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opBatchAssociateScramSecret,
		opBatchDisassociateScramSecret,
		opCreateChannel,
		opCreateCluster,
		opCreateClusterV2,
		opCreateConfiguration,
		opCreateReplicator,
		opCreateTopic,
		opCreateVpcConnection,
		opDeleteChannel,
		opDeleteCluster,
		opDeleteClusterPolicy,
		opDeleteConfiguration,
		opDeleteReplicator,
		opDeleteTopic,
		opDeleteVpcConnection,
		opDescribeChannel,
		opDescribeCluster,
		opDescribeClusterOperation,
		opDescribeClusterOperationV2,
		opDescribeClusterV2,
		opDescribeConfiguration,
		opDescribeConfigurationRevision,
		opDescribeReplicator,
		opDescribeTopic,
		opDescribeTopicPartitions,
		opDescribeVpcConnection,
		opGetBootstrapBrokers,
		opGetClusterPolicy,
		opGetCompatibleKafkaVersions,
		opListChannels,
		opListClientVpcConnections,
		opListClusterOperations,
		opListClusterOperationsV2,
		opListClusters,
		opListClustersV2,
		opListConfigurationRevisions,
		opListConfigurations,
		opListKafkaVersions,
		opListNodes,
		opListReplicators,
		opListScramSecrets,
		opListTagsForResource,
		opListTopics,
		opListVpcConnections,
		opPutClusterPolicy,
		opRebootBroker,
		opRejectClientVpcConnection,
		opTagResource,
		opUntagResource,
		opUpdateBrokerCount,
		opUpdateBrokerStorage,
		opUpdateBrokerType,
		opUpdateChannel,
		opUpdateClusterConfiguration,
		opUpdateClusterKafkaVersion,
		opUpdateConfiguration,
		opUpdateConnectivity,
		opUpdateMonitoring,
		opUpdateRebalancing,
		opUpdateReplicationInfo,
		opUpdateSecurity,
		opUpdateStorage,
		opUpdateTopic,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "kafka" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches MSK REST API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		p := c.Request().URL.Path

		// "/v1/configurations" is also MQ's real CreateConfiguration/
		// ListConfigurations wire path; scope by SigV4 so a correctly-signed MQ
		// request isn't swallowed here (see gopherstack-61i8).
		if strings.HasPrefix(p, "/v1/configurations") {
			return httputils.ScopedPrefixMatch(c.Request(), p, "/v1/configurations", "kafka")
		}

		return strings.HasPrefix(p, "/v1/clusters") ||
			strings.HasPrefix(p, "/api/v2/clusters") ||
			strings.HasPrefix(p, "/v1/operations/") ||
			strings.HasPrefix(p, "/api/v2/operations/") ||
			strings.HasPrefix(p, "/replication/v1/replicators") ||
			p == vpcConnectionRoot ||
			strings.HasPrefix(p, vpcConnectionPrefix) ||
			p == vpcConnectionsListPath ||
			p == kafkaVersionsRoot ||
			p == compatibleKafkaVersionsPath ||
			isKafkaTagsPath(p)
	}
}

// isKafkaTagsPath reports whether the path is a /v1/tags/{arn} path for a Kafka ARN.
// It avoids matching tag paths that belong to other services sharing the same prefix.
func isKafkaTagsPath(path string) bool {
	if !strings.HasPrefix(path, tagsPrefix) {
		return false
	}

	encodedARN := path[len(tagsPrefix):]
	if encodedARN == "" {
		return false
	}

	decodedARN, err := url.PathUnescape(encodedARN)
	if err != nil {
		return false
	}

	if !strings.HasPrefix(decodedARN, "arn:") {
		return false
	}

	// arn:partition:service:region:account:resource
	parts := strings.SplitN(decodedARN, ":", arnMaxParts)
	if len(parts) < arnMinPartsForService {
		return false
	}

	return parts[2] == "kafka"
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return kafkaMatchPriority }

// ExtractOperation extracts the MSK operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseKafkaPath(c.Request().Method, effectivePath(c.Request()))

	return op
}

// ExtractResource extracts a resource ARN from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := parseKafkaPath(c.Request().Method, effectivePath(c.Request()))

	return resource
}

// Handler returns the Echo handler function for MSK requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := h.contextWithRegion(c)
		log := logger.Load(ctx)

		method := c.Request().Method
		path := effectivePath(c.Request())

		op, resource := parseKafkaPath(method, path)
		if op == "" {
			return h.writeError(c, http.StatusNotFound, "NotFoundException", "not found")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "kafka: failed to read request body", "error", err)

			return h.writeError(
				c,
				http.StatusInternalServerError,
				"InternalFailure",
				"failed to read request body",
			)
		}

		log.DebugContext(ctx, "kafka request", "op", op, "resource", resource)

		return h.dispatch(ctx, c, op, resource, body)
	}
}

// contextWithRegion returns the request context with the resolved AWS region attached
// under regionContextKey so that backend operations are routed to the correct region.
// The SigV4 credential-scope region in the Authorization header (extracted by
// httputils.ExtractRegionFromRequest) takes precedence over the backend default.
func (h *Handler) contextWithRegion(c *echo.Context) context.Context {
	region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

	return context.WithValue(c.Request().Context(), regionContextKey{}, region)
}

// effectivePath returns the raw (percent-encoded) path if available, otherwise the decoded path.
func effectivePath(r *http.Request) string {
	if r.URL.RawPath != "" {
		return r.URL.RawPath
	}

	return r.URL.Path
}

type kafkaErrorResponse struct {
	Message   string `json:"message"`
	ErrorCode string `json:"errorCode"`
	Type      string `json:"__type"`
}

// dispatch routes a parsed operation to the appropriate handler.
func (h *Handler) dispatch(
	ctx context.Context,
	c *echo.Context,
	op, resource string,
	body []byte,
) error {
	if ok, err := h.dispatchCoreOps(ctx, c, op, resource, body); ok {
		return err
	}

	if ok, err := h.dispatchNewOps(ctx, c, op, resource, body); ok {
		return err
	}

	if ok, err := h.dispatchUpdateOps(ctx, c, op, resource, body); ok {
		return err
	}

	if ok, err := h.dispatchChannelOps(ctx, c, op, resource, body); ok {
		return err
	}

	return h.writeError(c, http.StatusNotFound, "NotFoundException", "unknown operation: "+op)
}

// dispatchChannelOps handles MSK Channel operations (Express cluster topic ->
// S3/Iceberg streaming), added in aws-sdk-go-v2/service/kafka v1.57. Returns
// (true, err) if the operation was handled, (false, nil) otherwise.
func (h *Handler) dispatchChannelOps(
	ctx context.Context,
	c *echo.Context,
	op, resource string,
	body []byte,
) (bool, error) {
	switch op {
	case opCreateChannel:
		return true, h.handleCreateChannel(ctx, c, resource, body)
	case opDeleteChannel:
		return true, h.handleDeleteChannel(ctx, c, resource)
	case opDescribeChannel:
		return true, h.handleDescribeChannel(ctx, c, resource)
	case opListChannels:
		return true, h.handleListChannels(ctx, c, resource)
	case opUpdateChannel:
		return true, h.handleUpdateChannel(ctx, c, resource, body)
	}

	return false, nil
}

// dispatchCoreOps handles cluster, configuration, and tag operations.
// Returns (true, err) if the operation was handled, (false, nil) otherwise.
func (h *Handler) dispatchCoreOps(ctx context.Context,
	c *echo.Context, op, resource string, body []byte) (bool, error) {
	if ok, err := h.dispatchClusterOps(ctx, c, op, resource, body); ok {
		return true, err
	}

	return h.dispatchConfigTagOps(ctx, c, op, resource, body)
}

// dispatchClusterOps handles cluster CRUD and bootstrap operations.
func (h *Handler) dispatchClusterOps(ctx context.Context,
	c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case opCreateCluster:
		return true, h.handleCreateCluster(ctx, c, body)
	case opCreateClusterV2:
		return true, h.handleCreateClusterV2(ctx, c, body)
	case opListClusters:
		return true, h.handleListClusters(ctx, c)
	case opListClustersV2:
		return true, h.handleListClustersV2(ctx, c)
	case opDescribeCluster:
		return true, h.handleDescribeCluster(ctx, c, resource)
	case opDescribeClusterV2:
		return true, h.handleDescribeClusterV2(ctx, c, resource)
	case opDeleteCluster:
		return true, h.handleDeleteCluster(ctx, c, resource)
	case opGetBootstrapBrokers:
		return true, h.handleGetBootstrapBrokers(ctx, c, resource)
	}

	return false, nil
}

// dispatchConfigTagOps handles configuration and tag operations.
func (h *Handler) dispatchConfigTagOps(ctx context.Context,
	c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case opCreateConfiguration:
		return true, h.handleCreateConfiguration(ctx, c, body)
	case opListConfigurations:
		return true, h.handleListConfigurations(ctx, c)
	case opDescribeConfiguration:
		return true, h.handleDescribeConfiguration(ctx, c, resource)
	case opDeleteConfiguration:
		return true, h.handleDeleteConfiguration(ctx, c, resource)
	case opListTagsForResource:
		return true, h.handleListTagsForResource(ctx, c, resource)
	case opTagResource:
		return true, h.handleTagResource(ctx, c, resource, body)
	case opUntagResource:
		return true, h.handleUntagResource(ctx, c, resource, c.Request().URL)
	}

	return false, nil
}

// dispatchNewOps handles SCRAM secrets, replicator, topic, VPC connection, and cluster policy operations.
// Returns (true, err) if the operation was handled, (false, nil) otherwise.
func (h *Handler) dispatchNewOps(ctx context.Context,
	c *echo.Context, op, resource string, body []byte) (bool, error) {
	if ok, err := h.dispatchScramAndReplicatorOps(ctx, c, op, resource, body); ok {
		return ok, err
	}

	if ok, err := h.dispatchTopicAndVpcOps(ctx, c, op, resource, body); ok {
		return ok, err
	}

	return h.dispatchPolicyAndMiscOps(ctx, c, op, resource, body)
}

// dispatchScramAndReplicatorOps handles SCRAM and replicator ops.
// Returns (true, err) if handled.
func (h *Handler) dispatchScramAndReplicatorOps(
	ctx context.Context,
	c *echo.Context,
	op, resource string,
	body []byte,
) (bool, error) {
	switch op {
	case opBatchAssociateScramSecret:
		return true, h.handleBatchAssociateScramSecret(ctx, c, resource, body)
	case opBatchDisassociateScramSecret:
		return true, h.handleBatchDisassociateScramSecret(ctx, c, resource, body)
	case opListScramSecrets:
		return true, h.handleListScramSecrets(ctx, c, resource)
	case opCreateReplicator:
		return true, h.handleCreateReplicator(ctx, c, body)
	case opDeleteReplicator:
		return true, h.handleDeleteReplicator(ctx, c, resource)
	case opDescribeReplicator:
		return true, h.handleDescribeReplicator(ctx, c, resource)
	case opListReplicators:
		return true, h.handleListReplicators(ctx, c)
	case opUpdateReplicationInfo:
		return true, h.handleUpdateReplicationInfo(ctx, c, resource, body)
	}

	return false, nil
}

// dispatchTopicAndVpcOps handles topic and VPC connection ops.
// Returns (true, err) if handled.
func (h *Handler) dispatchTopicAndVpcOps(
	ctx context.Context,
	c *echo.Context,
	op, resource string,
	body []byte,
) (bool, error) {
	switch op {
	case opCreateTopic:
		return true, h.handleCreateTopic(ctx, c, resource, body)
	case opDeleteTopic:
		return true, h.handleDeleteTopic(ctx, c, resource)
	case opDescribeTopic:
		return true, h.handleDescribeTopic(ctx, c, resource)
	case opDescribeTopicPartitions:
		return true, h.handleDescribeTopicPartitions(ctx, c, resource)
	case opListTopics:
		return true, h.handleListTopics(ctx, c, resource)
	case opUpdateTopic:
		return true, h.handleUpdateTopic(ctx, c, resource, body)
	case opCreateVpcConnection:
		return true, h.handleCreateVpcConnection(ctx, c, body)
	case opDeleteVpcConnection:
		return true, h.handleDeleteVpcConnection(ctx, c, resource)
	case opDescribeVpcConnection:
		return true, h.handleDescribeVpcConnection(ctx, c, resource)
	case opListVpcConnections:
		return true, h.handleListVpcConnections(ctx, c)
	case opListClientVpcConnections:
		return true, h.handleListClientVpcConnections(ctx, c, resource)
	case opRejectClientVpcConnection:
		return true, h.handleRejectClientVpcConnection(ctx, c, body)
	}

	return false, nil
}

// dispatchPolicyAndMiscOps handles cluster policy, operations, configuration revision,
// and node/version ops. Returns (true, err) if handled.
func (h *Handler) dispatchPolicyAndMiscOps(
	ctx context.Context,
	c *echo.Context,
	op, resource string,
	body []byte,
) (bool, error) {
	switch op {
	case opDeleteClusterPolicy:
		return true, h.handleDeleteClusterPolicy(ctx, c, resource)
	case opGetClusterPolicy:
		return true, h.handleGetClusterPolicy(ctx, c, resource)
	case opPutClusterPolicy:
		return true, h.handlePutClusterPolicy(ctx, c, resource, body)
	case opDescribeClusterOperation:
		return true, h.handleDescribeClusterOperation(ctx, c, resource)
	case opDescribeClusterOperationV2:
		return true, h.handleDescribeClusterOperationV2(ctx, c, resource)
	case opListClusterOperations:
		return true, h.handleListClusterOperations(ctx, c, resource)
	case opListClusterOperationsV2:
		return true, h.handleListClusterOperationsV2(ctx, c, resource)
	case opDescribeConfigurationRevision:
		return true, h.handleDescribeConfigurationRevision(ctx, c, resource)
	case opListConfigurationRevisions:
		return true, h.handleListConfigurationRevisions(ctx, c, resource)
	case opUpdateConfiguration:
		return true, h.handleUpdateConfiguration(ctx, c, resource, body)
	case opListKafkaVersions:
		return true, h.handleListKafkaVersions(ctx, c)
	case opGetCompatibleKafkaVersions:
		return true, h.handleGetCompatibleKafkaVersions(ctx, c)
	case opListNodes:
		return true, h.handleListNodes(ctx, c, resource)
	case opRebootBroker:
		return true, h.handleRebootBroker(ctx, c, resource, body)
	}

	return false, nil
}

// dispatchUpdateOps handles cluster and broker update operations.
// Returns (true, err) if the operation was handled, (false, nil) otherwise.
func (h *Handler) dispatchUpdateOps(
	ctx context.Context,
	c *echo.Context,
	op, resource string,
	body []byte,
) (bool, error) {
	switch op {
	case opUpdateBrokerCount:
		return true, h.handleUpdateBrokerCount(ctx, c, resource, body)
	case opUpdateBrokerStorage:
		return true, h.handleUpdateBrokerStorage(ctx, c, resource, body)
	case opUpdateBrokerType:
		return true, h.handleUpdateBrokerType(ctx, c, resource, body)
	case opUpdateClusterConfiguration:
		return true, h.handleUpdateClusterConfiguration(ctx, c, resource, body)
	case opUpdateClusterKafkaVersion:
		return true, h.handleUpdateClusterKafkaVersion(ctx, c, resource, body)
	case opUpdateConnectivity:
		return true, h.handleUpdateConnectivity(ctx, c, resource, body)
	case opUpdateMonitoring:
		return true, h.handleUpdateMonitoring(ctx, c, resource, body)
	case opUpdateRebalancing:
		return true, h.handleUpdateRebalancing(ctx, c, resource, body)
	case opUpdateSecurity:
		return true, h.handleUpdateSecurity(ctx, c, resource, body)
	case opUpdateStorage:
		return true, h.handleUpdateStorage(ctx, c, resource, body)
	}

	return false, nil
}

// decodeJSONBody unmarshals a (possibly empty) JSON request body. An empty body
// is treated as an empty object so optional update fields default to zero values.
func decodeJSONBody(body []byte, v any) error {
	if len(body) == 0 {
		return nil
	}

	return json.Unmarshal(body, v)
}

// ----------------------------------------
// Error helpers
// ----------------------------------------

const defaultPageSize = 100

// encodeKafkaPageToken encodes an integer offset as a base64url-encoded JSON token.
func encodeKafkaPageToken(offset int) string {
	data, _ := json.Marshal(struct {
		O int `json:"o"`
	}{O: offset})

	return base64.RawURLEncoding.EncodeToString(data)
}

// decodeKafkaPageToken decodes a base64url-encoded JSON token. Returns 0 on any failure.
func decodeKafkaPageToken(token string) int {
	if token == "" {
		return 0
	}

	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0
	}

	var t struct {
		O int `json:"o"`
	}

	err = json.Unmarshal(data, &t)
	if err != nil {
		return 0
	}

	return t.O
}

// kafkaPageSize parses the maxResults query param, falling back to defaultPageSize.
func kafkaPageSize(c *echo.Context) int {
	raw := c.Request().URL.Query().Get("maxResults")
	if raw == "" {
		return defaultPageSize
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return defaultPageSize
	}

	return n
}

func (h *Handler) writeError(c *echo.Context, status int, code, message string) error {
	return c.JSON(status, kafkaErrorResponse{
		Message:   message,
		ErrorCode: code,
		Type:      code,
	})
}

func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return h.writeError(c, http.StatusNotFound, "NotFoundException", err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return h.writeError(c, http.StatusConflict, "ConflictException", err.Error())
	case errors.Is(err, awserr.ErrInvalidParameter):
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", err.Error())
	}

	return h.writeError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
}
