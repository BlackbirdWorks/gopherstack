package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
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
	opCreateCluster                 = "CreateCluster"
	opCreateClusterV2               = "CreateClusterV2"
	opCreateConfiguration           = "CreateConfiguration"
	opCreateReplicator              = "CreateReplicator"
	opCreateTopic                   = "CreateTopic"
	opCreateVpcConnection           = "CreateVpcConnection"
	opDeleteCluster                 = "DeleteCluster"
	opDeleteClusterPolicy           = "DeleteClusterPolicy"
	opDeleteConfiguration           = "DeleteConfiguration"
	opDeleteReplicator              = "DeleteReplicator"
	opDeleteTopic                   = "DeleteTopic"
	opDeleteVpcConnection           = "DeleteVpcConnection"
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

const (
	clustersV1Prefix          = "/v1/clusters/"
	clustersV2Prefix          = "/api/v2/clusters/"
	configurationsPrefix      = "/v1/configurations/"
	tagsPrefix                = "/v1/tags/"
	bootstrapBrokersSuffix    = "/bootstrap-brokers"
	scramSecretsSuffix        = "/scram-secrets"
	topicsSuffix              = "/topics"
	topicPartitionsSuffix     = "/topic-partitions"
	policySuffix              = "/policy"
	operationsPrefix          = "/v1/operations/"
	operationsV2Prefix        = "/api/v2/operations/"
	replicatorsPrefix         = "/replication/v1/replicators/"
	replicatorsRoot           = "/replication/v1/replicators"
	vpcConnectionPrefix       = "/v1/vpc-connection/"
	vpcConnectionRoot         = "/v1/vpc-connection"
	kafkaVersionsRoot         = "/v1/kafka-versions"
	compatibleVersionsSuffix  = "/compatible-kafka-versions"
	nodesSuffix               = "/nodes"
	rebootBrokerSuffix        = "/reboot-broker"
	clientVpcConnectionSuffix = "/client-vpc-connections"
	rejectVpcConnectionSuffix = "/reject-client-vpc-connection"
	revisionsSuffix           = "/revisions"

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
		opCreateCluster,
		opCreateClusterV2,
		opCreateConfiguration,
		opCreateReplicator,
		opCreateTopic,
		opCreateVpcConnection,
		opDeleteCluster,
		opDeleteClusterPolicy,
		opDeleteConfiguration,
		opDeleteReplicator,
		opDeleteTopic,
		opDeleteVpcConnection,
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

		return strings.HasPrefix(p, "/v1/clusters") ||
			strings.HasPrefix(p, "/api/v2/clusters") ||
			strings.HasPrefix(p, "/v1/configurations") ||
			strings.HasPrefix(p, "/v1/operations/") ||
			strings.HasPrefix(p, "/api/v2/operations/") ||
			strings.HasPrefix(p, "/replication/v1/replicators") ||
			p == vpcConnectionRoot ||
			strings.HasPrefix(p, vpcConnectionPrefix) ||
			p == kafkaVersionsRoot ||
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
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

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

// parseKafkaPath parses an HTTP method + path into an operation name and resource ARN.
func parseKafkaPath(method, path string) (string, string) {
	if op, resource := parseClusterAndConfigPath(method, path); op != "" {
		return op, resource
	}

	return parseExtendedOpsPath(method, path)
}

// parseClusterAndConfigPath handles cluster (v1/v2), configuration, and tag paths.
func parseClusterAndConfigPath(method, path string) (string, string) {
	switch {
	case path == "/v1/clusters" || path == "/v1/clusters/":
		return parseClusterRootV1(method)
	case strings.HasPrefix(path, clustersV1Prefix):
		return parseClusterResourceV1(method, path[len(clustersV1Prefix):])
	case path == "/api/v2/clusters" || path == "/api/v2/clusters/":
		return parseClusterRootV2(method)
	case strings.HasPrefix(path, clustersV2Prefix):
		return parseClusterResourceV2(method, path[len(clustersV2Prefix):])
	case path == "/v1/configurations" || path == "/v1/configurations/":
		return parseConfigurationRoot(method)
	case strings.HasPrefix(path, configurationsPrefix):
		return parseConfigurationResource(method, path[len(configurationsPrefix):])
	case strings.HasPrefix(path, tagsPrefix):
		return parseTagsResource(method, path[len(tagsPrefix):])
	}

	return "", ""
}

// parseExtendedOpsPath handles operations, replicators, VPC connections, and misc paths.
func parseExtendedOpsPath(method, path string) (string, string) {
	switch {
	case strings.HasPrefix(path, operationsPrefix):
		return parseOperationResource(method, path[len(operationsPrefix):])
	case strings.HasPrefix(path, operationsV2Prefix):
		return parseOperationV2Resource(method, path[len(operationsV2Prefix):])
	case path == replicatorsRoot || path == replicatorsRoot+"/":
		return parseReplicatorsRoot(method)
	case strings.HasPrefix(path, replicatorsPrefix):
		return parseReplicatorResource(method, path[len(replicatorsPrefix):])
	case path == vpcConnectionRoot || path == vpcConnectionRoot+"/":
		return parseVpcConnectionRoot(method)
	case strings.HasPrefix(path, vpcConnectionPrefix):
		return parseVpcConnectionResource(method, path[len(vpcConnectionPrefix):])
	case path == kafkaVersionsRoot || path == kafkaVersionsRoot+"/":
		if method == http.MethodGet {
			return opListKafkaVersions, ""
		}
	}

	return "", ""
}

func parseClusterRootV1(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opListClusters, ""
	case http.MethodPost:
		return opCreateCluster, ""
	}

	return "", ""
}

// parseClusterResourceV1 routes V1 cluster resource paths.
func parseClusterResourceV1(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	if op, id := parseClusterResourceV1Topics(method, decoded); op != "" {
		return op, id
	}

	if op, id := parseClusterResourceV1Config(method, decoded); op != "" {
		return op, id
	}

	switch method {
	case http.MethodGet:
		return opDescribeCluster, decoded
	case http.MethodDelete:
		return opDeleteCluster, decoded
	}

	return "", ""
}

// parseClusterResourceV1Topics handles topic and scram secret sub-paths.
func parseClusterResourceV1Topics(method, decoded string) (string, string) {
	// /topic-partitions/{topicName}: must be checked before /topics suffix.
	if idx := strings.Index(decoded, topicPartitionsSuffix+"/"); idx != -1 {
		arnStr := decoded[:idx]
		topicName := decoded[idx+len(topicPartitionsSuffix)+1:]

		if method == http.MethodGet {
			return opDescribeTopicPartitions, arnStr + topicKeySeparator + topicName
		}

		return "", ""
	}

	// /topics/{topicName}: must be checked before /topics suffix.
	if idx := strings.Index(decoded, topicsSuffix+"/"); idx != -1 {
		arnStr := decoded[:idx]
		topicName := decoded[idx+len(topicsSuffix)+1:]

		switch method {
		case http.MethodDelete:
			return opDeleteTopic, arnStr + topicKeySeparator + topicName
		case http.MethodGet:
			return opDescribeTopic, arnStr + topicKeySeparator + topicName
		case http.MethodPut:
			return opUpdateTopic, arnStr + topicKeySeparator + topicName
		}

		return "", ""
	}

	// /topics (no trailing topic name): CreateTopic (POST) or ListTopics (GET).
	if strings.HasSuffix(decoded, topicsSuffix) {
		arnStr := decoded[:len(decoded)-len(topicsSuffix)]

		switch method {
		case http.MethodPost:
			return opCreateTopic, arnStr
		case http.MethodGet:
			return opListTopics, arnStr
		}

		return "", ""
	}

	// /scram-secrets: BatchAssociateScramSecret (POST), BatchDisassociateScramSecret (PATCH), ListScramSecrets (GET).
	if strings.HasSuffix(decoded, scramSecretsSuffix) {
		arnStr := decoded[:len(decoded)-len(scramSecretsSuffix)]

		switch method {
		case http.MethodPost:
			return opBatchAssociateScramSecret, arnStr
		case http.MethodPatch:
			return opBatchDisassociateScramSecret, arnStr
		case http.MethodGet:
			return opListScramSecrets, arnStr
		}

		return "", ""
	}

	return "", ""
}

// parseClusterResourceV1Config handles policy, broker, VPC, and operations sub-paths.
func parseClusterResourceV1Config(method, decoded string) (string, string) {
	// /policy: DeleteClusterPolicy (DELETE), GetClusterPolicy (GET), PutClusterPolicy (PUT).
	if strings.HasSuffix(decoded, policySuffix) {
		arnStr := decoded[:len(decoded)-len(policySuffix)]

		switch method {
		case http.MethodDelete:
			return opDeleteClusterPolicy, arnStr
		case http.MethodGet:
			return opGetClusterPolicy, arnStr
		case http.MethodPut:
			return opPutClusterPolicy, arnStr
		}

		return "", ""
	}

	// /bootstrap-brokers: GetBootstrapBrokers.
	if strings.HasSuffix(decoded, bootstrapBrokersSuffix) {
		arnStr := decoded[:len(decoded)-len(bootstrapBrokersSuffix)]

		if method == http.MethodGet {
			return opGetBootstrapBrokers, arnStr
		}

		return "", ""
	}

	if op, id := parseClusterResourceV1Misc(method, decoded); op != "" {
		return op, id
	}

	return "", ""
}

// parseClusterResourceV1Misc handles compatible versions, nodes, reboot, VPC, and operations paths.
func parseClusterResourceV1Misc(method, decoded string) (string, string) {
	// /compatible-kafka-versions: GetCompatibleKafkaVersions.
	if strings.HasSuffix(decoded, compatibleVersionsSuffix) {
		arnStr := decoded[:len(decoded)-len(compatibleVersionsSuffix)]

		if method == http.MethodGet {
			return opGetCompatibleKafkaVersions, arnStr
		}

		return "", ""
	}

	// /nodes: ListNodes.
	if strings.HasSuffix(decoded, nodesSuffix) {
		arnStr := decoded[:len(decoded)-len(nodesSuffix)]

		if method == http.MethodGet {
			return opListNodes, arnStr
		}

		return "", ""
	}

	// /reboot-broker: RebootBroker.
	if strings.HasSuffix(decoded, rebootBrokerSuffix) {
		arnStr := decoded[:len(decoded)-len(rebootBrokerSuffix)]

		if method == http.MethodPut {
			return opRebootBroker, arnStr
		}

		return "", ""
	}

	// /client-vpc-connections: ListClientVpcConnections.
	if strings.HasSuffix(decoded, clientVpcConnectionSuffix) {
		arnStr := decoded[:len(decoded)-len(clientVpcConnectionSuffix)]

		if method == http.MethodGet {
			return opListClientVpcConnections, arnStr
		}

		return "", ""
	}

	// /reject-client-vpc-connection/{vpcConnectionArn}: RejectClientVpcConnection.
	if idx := strings.Index(decoded, rejectVpcConnectionSuffix+"/"); idx != -1 {
		vpcConnectionArn := decoded[idx+len(rejectVpcConnectionSuffix)+1:]

		if method == http.MethodPut {
			return opRejectClientVpcConnection, vpcConnectionArn
		}

		return "", ""
	}

	// /operations: ListClusterOperations (GET).
	if strings.HasSuffix(decoded, "/operations") {
		arnStr := decoded[:len(decoded)-len("/operations")]

		if method == http.MethodGet {
			return opListClusterOperations, arnStr
		}

		return "", ""
	}

	return "", ""
}

func parseClusterRootV2(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opListClustersV2, ""
	case http.MethodPost:
		return opCreateClusterV2, ""
	}

	return "", ""
}

func parseClusterResourceV2(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	// /operations: ListClusterOperationsV2.
	if strings.HasSuffix(decoded, "/operations") {
		arnStr := decoded[:len(decoded)-len("/operations")]

		if method == http.MethodGet {
			return opListClusterOperationsV2, arnStr
		}

		return "", ""
	}

	// Update sub-operations via PUT with specific suffixes.
	if method == http.MethodPut {
		for suffix, op := range map[string]string{
			"/broker-count":   opUpdateBrokerCount,
			"/broker-storage": opUpdateBrokerStorage,
			"/broker-type":    opUpdateBrokerType,
			"/configuration":  opUpdateClusterConfiguration,
			"/kafka-version":  opUpdateClusterKafkaVersion,
			"/connectivity":   opUpdateConnectivity,
			"/monitoring":     opUpdateMonitoring,
			"/rebalancing":    opUpdateRebalancing,
			"/security":       opUpdateSecurity,
			"/storage":        opUpdateStorage,
		} {
			if strings.HasSuffix(decoded, suffix) {
				arnStr := decoded[:len(decoded)-len(suffix)]

				return op, arnStr
			}
		}
	}

	if method == http.MethodGet {
		return opDescribeClusterV2, decoded
	}

	return "", ""
}

func parseConfigurationRoot(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opListConfigurations, ""
	case http.MethodPost:
		return opCreateConfiguration, ""
	}

	return "", ""
}

func parseConfigurationResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	// /revisions/{revision}: DescribeConfigurationRevision (GET) or ListConfigurationRevisions (GET on root).
	if idx := strings.Index(decoded, revisionsSuffix+"/"); idx != -1 {
		configArn := decoded[:idx]
		revision := decoded[idx+len(revisionsSuffix)+1:]

		if method == http.MethodGet {
			return opDescribeConfigurationRevision, configArn + topicKeySeparator + revision
		}

		return "", ""
	}

	if strings.HasSuffix(decoded, revisionsSuffix) {
		configArn := decoded[:len(decoded)-len(revisionsSuffix)]

		if method == http.MethodGet {
			return opListConfigurationRevisions, configArn
		}

		return "", ""
	}

	switch method {
	case http.MethodGet:
		return opDescribeConfiguration, decoded
	case http.MethodDelete:
		return opDeleteConfiguration, decoded
	case http.MethodPut:
		return opUpdateConfiguration, decoded
	}

	return "", ""
}

func parseTagsResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	switch method {
	case http.MethodGet:
		return opListTagsForResource, decoded
	case http.MethodPost:
		return opTagResource, decoded
	case http.MethodDelete:
		return opUntagResource, decoded
	}

	return "", ""
}

func parseOperationResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	if method == http.MethodGet {
		return opDescribeClusterOperation, decoded
	}

	return "", ""
}

func parseOperationV2Resource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	if method == http.MethodGet {
		return opDescribeClusterOperationV2, decoded
	}

	return "", ""
}

func parseReplicatorsRoot(method string) (string, string) {
	switch method {
	case http.MethodPost:
		return opCreateReplicator, ""
	case http.MethodGet:
		return opListReplicators, ""
	}

	return "", ""
}

func parseReplicatorResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	switch method {
	case http.MethodDelete:
		return opDeleteReplicator, decoded
	case http.MethodGet:
		return opDescribeReplicator, decoded
	case http.MethodPut:
		return opUpdateReplicationInfo, decoded
	}

	return "", ""
}

func parseVpcConnectionRoot(method string) (string, string) {
	switch method {
	case http.MethodPost:
		return opCreateVpcConnection, ""
	case http.MethodGet:
		return opListVpcConnections, ""
	}

	return "", ""
}

func parseVpcConnectionResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	switch method {
	case http.MethodDelete:
		return opDeleteVpcConnection, decoded
	case http.MethodGet:
		return opDescribeVpcConnection, decoded
	}

	return "", ""
}

// dispatch routes a parsed operation to the appropriate handler.
func (h *Handler) dispatch(ctx context.Context, c *echo.Context, op, resource string, body []byte) error {
	if ok, err := h.dispatchCoreOps(ctx, c, op, resource, body); ok {
		return err
	}

	if ok, err := h.dispatchNewOps(ctx, c, op, resource, body); ok {
		return err
	}

	if ok, err := h.dispatchUpdateOps(ctx, c, op, resource, body); ok {
		return err
	}

	return h.writeError(c, http.StatusNotFound, "NotFoundException", "unknown operation: "+op)
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
		return true, h.handleRejectClientVpcConnection(ctx, c, resource)
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
		return true, h.handleGetCompatibleKafkaVersions(ctx, c, resource)
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

// ----------------------------------------
// Request / response types
// ----------------------------------------

type createClusterInput struct {
	Tags                 map[string]string     `json:"tags,omitempty"`
	ClientAuthentication *ClientAuthentication `json:"clientAuthentication,omitempty"`
	ClusterName          string                `json:"clusterName"`
	KafkaVersion         string                `json:"kafkaVersion"`
	BrokerNodeGroupInfo  BrokerNodeGroupInfo   `json:"brokerNodeGroupInfo"`
	NumberOfBrokerNodes  int32                 `json:"numberOfBrokerNodes"`
}

type createClusterOutput struct {
	ClusterArn  string `json:"clusterArn"`
	ClusterName string `json:"clusterName"`
	State       string `json:"state"`
}

// brokerSoftwareInfo represents the current broker software information.
type brokerSoftwareInfo struct {
	KafkaVersion string `json:"kafkaVersion"`
}

// clusterInfoV1 is the V1 cluster response shape (DescribeCluster / ListClusters).
type clusterInfoV1 struct {
	Tags                      map[string]string     `json:"tags,omitempty"`
	CurrentBrokerSoftwareInfo *brokerSoftwareInfo   `json:"currentBrokerSoftwareInfo,omitempty"`
	ClientAuthentication      *ClientAuthentication `json:"clientAuthentication,omitempty"`
	EncryptionInfo            *EncryptionInfo       `json:"encryptionInfo,omitempty"`
	OpenMonitoring            *OpenMonitoring       `json:"openMonitoring,omitempty"`
	LoggingInfo               *LoggingInfo          `json:"loggingInfo,omitempty"`
	StateInfo                 *StateInfo            `json:"stateInfo,omitempty"`
	ConfigurationInfo         *ConfigurationInfo    `json:"configurationInfo,omitempty"`
	ClusterArn                string                `json:"clusterArn"`
	ClusterName               string                `json:"clusterName"`
	KafkaVersion              string                `json:"kafkaVersion"`
	State                     string                `json:"state"`
	CurrentVersion            string                `json:"currentVersion"`
	ActiveOperationArn        string                `json:"activeOperationArn,omitempty"`
	EnhancedMonitoring        string                `json:"enhancedMonitoring,omitempty"`
	ZookeeperConnectString    string                `json:"zookeeperConnectString,omitempty"`
	BrokerNodeGroupInfo       BrokerNodeGroupInfo   `json:"brokerNodeGroupInfo"`
	NumberOfBrokerNodes       int32                 `json:"numberOfBrokerNodes"`
}

type describeClusterOutput struct {
	ClusterInfo *clusterInfoV1 `json:"clusterInfo"`
}

type listClustersOutput struct {
	ClusterInfoList []*clusterInfoV1 `json:"clusterInfoList"`
}

type provisionedClusterInfo struct {
	CurrentBrokerSoftwareInfo *brokerSoftwareInfo   `json:"currentBrokerSoftwareInfo,omitempty"`
	ClientAuthentication      *ClientAuthentication `json:"clientAuthentication,omitempty"`
	EncryptionInfo            *EncryptionInfo       `json:"encryptionInfo,omitempty"`
	OpenMonitoring            *OpenMonitoring       `json:"openMonitoring,omitempty"`
	LoggingInfo               *LoggingInfo          `json:"loggingInfo,omitempty"`
	ConfigurationInfo         *ConfigurationInfo    `json:"configurationInfo,omitempty"`
	KafkaVersion              string                `json:"kafkaVersion"`
	State                     string                `json:"state"`
	EnhancedMonitoring        string                `json:"enhancedMonitoring,omitempty"`
	StorageMode               string                `json:"storageMode,omitempty"`
	BrokerNodeGroupInfo       BrokerNodeGroupInfo   `json:"brokerNodeGroupInfo"`
	NumberOfBrokerNodes       int32                 `json:"numberOfBrokerNodes"`
}

type clusterInfoV2 struct {
	Tags           map[string]string       `json:"tags,omitempty"`
	Provisioned    *provisionedClusterInfo `json:"provisioned,omitempty"`
	Serverless     *ServerlessClusterInfo  `json:"serverless,omitempty"`
	ClusterArn     string                  `json:"clusterArn"`
	ClusterName    string                  `json:"clusterName"`
	ClusterType    string                  `json:"clusterType"`
	State          string                  `json:"state"`
	CurrentVersion string                  `json:"currentVersion,omitempty"`
}

type describeClusterV2Output struct {
	ClusterInfo *clusterInfoV2 `json:"clusterInfo"`
}

type listClustersV2Output struct {
	ClusterInfoList []*clusterInfoV2 `json:"clusterInfoList"`
}

type getBootstrapBrokersOutput struct {
	BootstrapBrokerString                     string `json:"bootstrapBrokerString,omitempty"`
	BootstrapBrokerStringTLS                  string `json:"bootstrapBrokerStringTls,omitempty"`
	BootstrapBrokerStringSaslScram            string `json:"bootstrapBrokerStringSaslScram,omitempty"`
	BootstrapBrokerStringSaslIam              string `json:"bootstrapBrokerStringSaslIam,omitempty"`
	BootstrapBrokerStringTLSPublic            string `json:"bootstrapBrokerStringTlsPublic,omitempty"`
	BootstrapBrokerStringSaslScramPublic      string `json:"bootstrapBrokerStringSaslScramPublic,omitempty"`
	BootstrapBrokerStringSaslIamPublic        string `json:"bootstrapBrokerStringSaslIamPublic,omitempty"`
	BootstrapBrokerStringVpcConnectivityTLS   string `json:"bootstrapBrokerStringVpcConnectivityTLS,omitempty"`
	BootstrapBrokerStringVpcConnectivityScram string `json:"bootstrapBrokerStringVpcConnectivitySaslScram,omitempty"`
	BootstrapBrokerStringVpcConnectivityIam   string `json:"bootstrapBrokerStringVpcConnectivitySaslIam,omitempty"`
}

type serverlessVpcConfigInput struct {
	SubnetIDs        []string `json:"subnetIds,omitempty"`
	SecurityGroupIDs []string `json:"securityGroupIds,omitempty"`
}

type serverlessAuthInput struct {
	Sasl *SaslSettings `json:"sasl,omitempty"`
}

type serverlessInput struct {
	ClientAuthentication *serverlessAuthInput       `json:"clientAuthentication,omitempty"`
	VpcConfigs           []serverlessVpcConfigInput `json:"vpcConfigs,omitempty"`
}

type createClusterV2Input struct {
	Tags        map[string]string `json:"tags,omitempty"`
	Provisioned *provisionedInput `json:"provisioned,omitempty"`
	Serverless  *serverlessInput  `json:"serverless,omitempty"`
	ClusterName string            `json:"clusterName"`
}

type provisionedInput struct {
	ClientAuthentication *ClientAuthentication `json:"clientAuthentication,omitempty"`
	KafkaVersion         string                `json:"kafkaVersion"`
	BrokerNodeGroupInfo  BrokerNodeGroupInfo   `json:"brokerNodeGroupInfo"`
	NumberOfBrokerNodes  int32                 `json:"numberOfBrokerNodes"`
}

type createClusterV2Output struct {
	ClusterArn  string `json:"clusterArn"`
	ClusterName string `json:"clusterName"`
	ClusterType string `json:"clusterType"`
}

type createConfigurationInput struct {
	Name             string   `json:"name"`
	Description      string   `json:"description,omitempty"`
	ServerProperties string   `json:"serverProperties"`
	KafkaVersions    []string `json:"kafkaVersions"`
}

type createConfigurationOutput struct {
	Arn            string                `json:"arn"`
	Name           string                `json:"name"`
	State          string                `json:"state"`
	LatestRevision configurationRevision `json:"latestRevision"`
}

type configurationRevision struct {
	Description string `json:"description,omitempty"`
	Revision    int64  `json:"revision"`
}

type describeConfigurationOutput struct {
	Arn            string                `json:"arn"`
	Name           string                `json:"name"`
	Description    string                `json:"description,omitempty"`
	State          string                `json:"state"`
	LatestRevision configurationRevision `json:"latestRevision"`
	KafkaVersions  []string              `json:"kafkaVersions,omitempty"`
}

type listConfigurationsOutput struct {
	Configurations []*Configuration `json:"configurations"`
}

type listTagsOutput struct {
	Tags map[string]string `json:"tags"`
}

type tagResourceInput struct {
	Tags map[string]string `json:"tags"`
}

type kafkaErrorResponse struct {
	Message   string `json:"message"`
	ErrorCode string `json:"errorCode"`
	Type      string `json:"__type"`
}

// ----------------------------------------
// Cluster handlers
// ----------------------------------------

func (h *Handler) handleCreateCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var in createClusterInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	cluster, err := h.Backend.CreateCluster(ctx,
		in.ClusterName,
		in.KafkaVersion,
		in.NumberOfBrokerNodes,
		in.BrokerNodeGroupInfo,
		in.ClientAuthentication,
		in.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createClusterOutput{
		ClusterArn:  cluster.ClusterArn,
		ClusterName: cluster.ClusterName,
		State:       cluster.State,
	})
}

func (h *Handler) handleCreateClusterV2(ctx context.Context, c *echo.Context, body []byte) error {
	var in createClusterV2Input
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if in.Provisioned != nil && in.Serverless != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"only one of provisioned or serverless may be specified",
		)
	}

	if in.Serverless != nil {
		srv := in.Serverless
		serverlessInfo := &ServerlessClusterInfo{}
		if srv.ClientAuthentication != nil {
			serverlessInfo.ClientAuthentication = &ServerlessClientAuthentication{
				Sasl: srv.ClientAuthentication.Sasl,
			}
		}
		for _, vc := range srv.VpcConfigs {
			serverlessInfo.VpcConfigs = append(serverlessInfo.VpcConfigs, ServerlessVpcConfig(vc))
		}

		cluster, err := h.Backend.CreateServerlessCluster(ctx, in.ClusterName, serverlessInfo, in.Tags)
		if err != nil {
			return h.writeBackendError(c, err)
		}

		return c.JSON(http.StatusOK, createClusterV2Output{
			ClusterArn:  cluster.ClusterArn,
			ClusterName: cluster.ClusterName,
			ClusterType: ClusterTypeServerless,
		})
	}

	var brokerInfo BrokerNodeGroupInfo

	var kafkaVersion string

	var numBrokers int32

	var clientAuth *ClientAuthentication

	if in.Provisioned != nil {
		brokerInfo = in.Provisioned.BrokerNodeGroupInfo
		kafkaVersion = in.Provisioned.KafkaVersion
		numBrokers = in.Provisioned.NumberOfBrokerNodes
		clientAuth = in.Provisioned.ClientAuthentication
	}

	cluster, err := h.Backend.CreateCluster(ctx,
		in.ClusterName,
		kafkaVersion,
		numBrokers,
		brokerInfo,
		clientAuth,
		in.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createClusterV2Output{
		ClusterArn:  cluster.ClusterArn,
		ClusterName: cluster.ClusterName,
		ClusterType: ClusterTypeProvisioned,
	})
}

func (h *Handler) handleListClusters(ctx context.Context, c *echo.Context) error {
	clusters := h.Backend.ListClusters(ctx)
	out := make([]*clusterInfoV1, 0, len(clusters))

	for _, cl := range clusters {
		out = append(out, toClusterInfoV1(cl))
	}

	return c.JSON(http.StatusOK, listClustersOutput{ClusterInfoList: out})
}

func (h *Handler) handleListClustersV2(ctx context.Context, c *echo.Context) error {
	clusters := h.Backend.ListClusters(ctx)
	out := make([]*clusterInfoV2, 0, len(clusters))

	for _, cl := range clusters {
		out = append(out, toClusterInfoV2(cl))
	}

	return c.JSON(http.StatusOK, listClustersV2Output{ClusterInfoList: out})
}

func (h *Handler) handleDescribeCluster(ctx context.Context, c *echo.Context, clusterArn string) error {
	cluster, err := h.Backend.DescribeCluster(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeClusterOutput{ClusterInfo: toClusterInfoV1(cluster)})
}

func (h *Handler) handleDescribeClusterV2(ctx context.Context, c *echo.Context, clusterArn string) error {
	cluster, err := h.Backend.DescribeCluster(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeClusterV2Output{ClusterInfo: toClusterInfoV2(cluster)})
}

func (h *Handler) handleDeleteCluster(ctx context.Context, c *echo.Context, clusterArn string) error {
	if err := h.Backend.DeleteCluster(ctx, clusterArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetBootstrapBrokers(ctx context.Context, c *echo.Context, clusterArn string) error {
	cluster, err := h.Backend.DescribeCluster(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, bootstrapBrokersFor(cluster))
}

// bootstrapBrokersFor builds the bootstrap broker response based on cluster auth settings.
func bootstrapBrokersFor(cl *Cluster) getBootstrapBrokersOutput {
	out := getBootstrapBrokersOutput{
		BootstrapBrokerString:    "localhost:9092",
		BootstrapBrokerStringTLS: "localhost:9094",
	}

	auth := cl.ClientAuthentication
	if auth == nil {
		return out
	}

	addSaslBrokers(auth.Sasl, &out)

	ci := cl.BrokerNodeGroupInfo.ConnectivityInfo
	if ci == nil {
		return out
	}

	addPublicBrokers(auth, ci.PublicAccess, &out)
	addVpcConnectivityBrokers(ci.VpcConnectivity, &out)

	return out
}

// addSaslBrokers populates SASL broker endpoints when SASL authentication is configured.
func addSaslBrokers(sasl *SaslSettings, out *getBootstrapBrokersOutput) {
	if sasl == nil {
		return
	}

	if sasl.Scram != nil && sasl.Scram.Enabled {
		out.BootstrapBrokerStringSaslScram = "localhost:9096"
	}

	if sasl.Iam != nil && sasl.Iam.Enabled {
		out.BootstrapBrokerStringSaslIam = "localhost:9098"
	}
}

// addPublicBrokers populates public access broker endpoints when public access is enabled.
func addPublicBrokers(auth *ClientAuthentication, pa *PublicAccess, out *getBootstrapBrokersOutput) {
	if pa == nil || pa.Type != "SERVICE_PROVIDED_EIPS" {
		return
	}

	out.BootstrapBrokerStringTLSPublic = "localhost:9194"

	if auth.Sasl == nil {
		return
	}

	if auth.Sasl.Scram != nil && auth.Sasl.Scram.Enabled {
		out.BootstrapBrokerStringSaslScramPublic = "localhost:9196"
	}

	if auth.Sasl.Iam != nil && auth.Sasl.Iam.Enabled {
		out.BootstrapBrokerStringSaslIamPublic = "localhost:9198"
	}
}

// addVpcConnectivityBrokers populates VPC connectivity broker endpoints.
func addVpcConnectivityBrokers(vc *VpcConnectivity, out *getBootstrapBrokersOutput) {
	if vc == nil || vc.ClientAuthentication == nil {
		return
	}

	vca := vc.ClientAuthentication

	if vca.TLS != nil && vca.TLS.Enabled {
		out.BootstrapBrokerStringVpcConnectivityTLS = "localhost:9294"
	}

	if vca.Sasl == nil {
		return
	}

	if vca.Sasl.Scram != nil && vca.Sasl.Scram.Enabled {
		out.BootstrapBrokerStringVpcConnectivityScram = "localhost:9296"
	}

	if vca.Sasl.Iam != nil && vca.Sasl.Iam.Enabled {
		out.BootstrapBrokerStringVpcConnectivityIam = "localhost:9298"
	}
}

// brokerSoftwareInfoFor returns a brokerSoftwareInfo for the given Kafka version,
// or nil if the version is empty.
func brokerSoftwareInfoFor(kafkaVersion string) *brokerSoftwareInfo {
	if kafkaVersion == "" {
		return nil
	}

	return &brokerSoftwareInfo{KafkaVersion: kafkaVersion}
}

// toClusterInfoV1 converts a Cluster to the V1 cluster info shape.
func toClusterInfoV1(cl *Cluster) *clusterInfoV1 {
	return &clusterInfoV1{
		ClusterArn:                cl.ClusterArn,
		ClusterName:               cl.ClusterName,
		KafkaVersion:              cl.KafkaVersion,
		State:                     cl.State,
		CurrentVersion:            cl.CurrentVersion,
		BrokerNodeGroupInfo:       cl.BrokerNodeGroupInfo,
		NumberOfBrokerNodes:       cl.NumberOfBrokerNodes,
		ClientAuthentication:      cl.ClientAuthentication,
		EncryptionInfo:            cl.EncryptionInfo,
		OpenMonitoring:            cl.OpenMonitoring,
		LoggingInfo:               cl.LoggingInfo,
		StateInfo:                 cl.StateInfo,
		ActiveOperationArn:        cl.ActiveOperationArn,
		EnhancedMonitoring:        cl.EnhancedMonitoring,
		ZookeeperConnectString:    zookeeperConnectStringFor(cl.ClusterArn),
		Tags:                      maps.Clone(cl.Tags),
		CurrentBrokerSoftwareInfo: brokerSoftwareInfoFor(cl.KafkaVersion),
		ConfigurationInfo:         cl.ConfigurationInfo,
	}
}

// zookeeperConnectStringFor synthesises a ZooKeeper connect string from the cluster ARN.
// This mirrors the legacy ZooKeeper endpoint format used by older MSK clusters.
func zookeeperConnectStringFor(clusterArn string) string {
	if clusterArn == "" {
		return ""
	}

	// Synthesise a deterministic-looking ZK endpoint from the ARN suffix.
	// Format: z-1.<cluster-id>.kafka.<region>.amazonaws.com:2181,...
	clusterID, region := parseClusterIDAndRegion(clusterArn)

	return fmt.Sprintf(
		"z-1.%s.kafka.%s.amazonaws.com:2181,"+
			"z-2.%s.kafka.%s.amazonaws.com:2181,"+
			"z-3.%s.kafka.%s.amazonaws.com:2181",
		clusterID, region,
		clusterID, region,
		clusterID, region,
	)
}

// parseClusterIDAndRegion extracts the cluster ID and region from an ARN.
func parseClusterIDAndRegion(clusterArn string) (string, string) {
	const (
		minARNSlashParts  = 2
		arnColonFields    = 6
		arnRegionPosition = 4
	)

	clusterID := "unknown"
	region := "us-east-1"

	parts := strings.Split(clusterArn, "/")
	if len(parts) >= minARNSlashParts {
		clusterID = parts[len(parts)-1]
	}

	arnParts := strings.SplitN(clusterArn, ":", arnColonFields)
	if len(arnParts) >= arnRegionPosition {
		region = arnParts[3]
	}

	return clusterID, region
}

// toClusterInfoV2 converts a Cluster to the V2 cluster info shape.
func toClusterInfoV2(cl *Cluster) *clusterInfoV2 {
	clusterType := cl.ClusterType
	if clusterType == "" {
		clusterType = ClusterTypeProvisioned
	}

	info := &clusterInfoV2{
		ClusterArn:     cl.ClusterArn,
		ClusterName:    cl.ClusterName,
		ClusterType:    clusterType,
		State:          cl.State,
		CurrentVersion: cl.CurrentVersion,
		Tags:           maps.Clone(cl.Tags),
	}

	if clusterType == ClusterTypeServerless {
		info.Serverless = cl.Serverless
	} else {
		info.Provisioned = &provisionedClusterInfo{
			BrokerNodeGroupInfo:       cl.BrokerNodeGroupInfo,
			KafkaVersion:              cl.KafkaVersion,
			NumberOfBrokerNodes:       cl.NumberOfBrokerNodes,
			State:                     cl.State,
			ClientAuthentication:      cl.ClientAuthentication,
			EncryptionInfo:            cl.EncryptionInfo,
			OpenMonitoring:            cl.OpenMonitoring,
			LoggingInfo:               cl.LoggingInfo,
			EnhancedMonitoring:        cl.EnhancedMonitoring,
			StorageMode:               cl.StorageMode,
			CurrentBrokerSoftwareInfo: brokerSoftwareInfoFor(cl.KafkaVersion),
			ConfigurationInfo:         cl.ConfigurationInfo,
		}
	}

	return info
}

// ----------------------------------------
// Configuration handlers
// ----------------------------------------

func (h *Handler) handleCreateConfiguration(ctx context.Context, c *echo.Context, body []byte) error {
	var in createConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	config, err := h.Backend.CreateConfiguration(ctx,
		in.Name,
		in.Description,
		in.KafkaVersions,
		in.ServerProperties,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createConfigurationOutput{
		Arn:   config.Arn,
		Name:  config.Name,
		State: ClusterStateActive,
		LatestRevision: configurationRevision{
			Revision:    1,
			Description: config.Description,
		},
	})
}

func (h *Handler) handleListConfigurations(ctx context.Context, c *echo.Context) error {
	configs := h.Backend.ListConfigurations(ctx)

	return c.JSON(http.StatusOK, listConfigurationsOutput{Configurations: configs})
}

func (h *Handler) handleDescribeConfiguration(ctx context.Context, c *echo.Context, configArn string) error {
	config, err := h.Backend.DescribeConfiguration(ctx, configArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeConfigurationOutput{
		Arn:           config.Arn,
		Name:          config.Name,
		Description:   config.Description,
		KafkaVersions: config.KafkaVersions,
		State:         ClusterStateActive,
		LatestRevision: configurationRevision{
			Revision:    1,
			Description: config.Description,
		},
	})
}

func (h *Handler) handleDeleteConfiguration(ctx context.Context, c *echo.Context, configArn string) error {
	if err := h.Backend.DeleteConfiguration(ctx, configArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ----------------------------------------
// Tag handlers
// ----------------------------------------

func (h *Handler) handleListTagsForResource(ctx context.Context, c *echo.Context, resourceArn string) error {
	tags, err := h.Backend.GetTags(ctx, resourceArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsOutput{Tags: tags})
}

func (h *Handler) handleTagResource(ctx context.Context, c *echo.Context, resourceArn string, body []byte) error {
	var in tagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if err := h.Backend.TagResource(ctx, resourceArn, in.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(ctx context.Context, c *echo.Context, resourceArn string, u *url.URL) error {
	tagKeys := u.Query()["tagKeys"]

	if err := h.Backend.UntagResource(ctx, resourceArn, tagKeys); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ----------------------------------------
// SCRAM secret request / response types
// ----------------------------------------

type scramSecretInput struct {
	SecretArnList []string `json:"secretArnList"`
}

type batchScramSecretOutput struct {
	UnprocessedScramSecrets []ScramSecretError `json:"unprocessedScramSecrets"`
}

// ----------------------------------------
// SCRAM secret handlers
// ----------------------------------------

func (h *Handler) handleBatchAssociateScramSecret(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in scramSecretInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	errs, err := h.Backend.BatchAssociateScramSecret(ctx, clusterArn, in.SecretArnList)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, batchScramSecretOutput{UnprocessedScramSecrets: errs})
}

func (h *Handler) handleBatchDisassociateScramSecret(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in scramSecretInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	errs, err := h.Backend.BatchDisassociateScramSecret(ctx, clusterArn, in.SecretArnList)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, batchScramSecretOutput{UnprocessedScramSecrets: errs})
}

// ----------------------------------------
// Replicator request / response types
// ----------------------------------------

type createReplicatorInput struct {
	Tags                    map[string]string `json:"tags,omitempty"`
	ReplicatorName          string            `json:"replicatorName"`
	Description             string            `json:"description,omitempty"`
	ServiceExecutionRoleArn string            `json:"serviceExecutionRoleArn"`
}

type createReplicatorOutput struct {
	ReplicatorArn   string `json:"replicatorArn"`
	ReplicatorName  string `json:"replicatorName"`
	ReplicatorState string `json:"replicatorState"`
}

// ----------------------------------------
// Replicator handlers
// ----------------------------------------

func (h *Handler) handleCreateReplicator(ctx context.Context, c *echo.Context, body []byte) error {
	var in createReplicatorInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	replicator, err := h.Backend.CreateReplicator(ctx,
		in.ReplicatorName,
		in.Description,
		in.ServiceExecutionRoleArn,
		in.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createReplicatorOutput{
		ReplicatorArn:   replicator.ReplicatorArn,
		ReplicatorName:  replicator.ReplicatorName,
		ReplicatorState: replicator.ReplicatorState,
	})
}

func (h *Handler) handleDeleteReplicator(ctx context.Context, c *echo.Context, replicatorArn string) error {
	if err := h.Backend.DeleteReplicator(ctx, replicatorArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ----------------------------------------
// Topic request / response types
// ----------------------------------------

type createTopicInput struct {
	ConfigEntries     map[string]string `json:"configEntries,omitempty"`
	TopicName         string            `json:"topicName"`
	ReplicationFactor int32             `json:"replicationFactor"`
	NumPartitions     int32             `json:"numPartitions"`
}

type createTopicOutput struct {
	ConfigEntries     map[string]string `json:"configEntries,omitempty"`
	TopicName         string            `json:"topicName"`
	ReplicationFactor int32             `json:"replicationFactor"`
	NumPartitions     int32             `json:"numPartitions"`
}

// ----------------------------------------
// Topic handlers
// ----------------------------------------

func (h *Handler) handleCreateTopic(ctx context.Context, c *echo.Context, clusterArn string, body []byte) error {
	var in createTopicInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	topic, err := h.Backend.CreateTopic(ctx,
		clusterArn,
		in.TopicName,
		in.ReplicationFactor,
		in.NumPartitions,
		in.ConfigEntries,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createTopicOutput{
		TopicName:         topic.TopicName,
		ReplicationFactor: topic.ReplicationFactor,
		NumPartitions:     topic.NumPartitions,
		ConfigEntries:     topic.ConfigEntries,
	})
}

func (h *Handler) handleDeleteTopic(ctx context.Context, c *echo.Context, resource string) error {
	parts := strings.SplitN(resource, topicKeySeparator, topicKeySeparatorParts)
	if len(parts) != topicKeySeparatorParts {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid resource: missing topic name",
		)
	}

	clusterArn, topicName := parts[0], parts[1]

	if err := h.Backend.DeleteTopic(ctx, clusterArn, topicName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ----------------------------------------
// VPC connection request / response types
// ----------------------------------------

type createVpcConnectionInput struct {
	Tags             map[string]string `json:"tags,omitempty"`
	TargetClusterArn string            `json:"targetClusterArn"`
	VpcID            string            `json:"vpcId"`
	Authentication   string            `json:"authentication,omitempty"`
}

type createVpcConnectionOutput struct {
	VpcConnectionArn string `json:"vpcConnectionArn"`
	TargetClusterArn string `json:"targetClusterArn"`
	VpcID            string `json:"vpcId"`
	State            string `json:"state"`
}

// ----------------------------------------
// VPC connection handlers
// ----------------------------------------

func (h *Handler) handleCreateVpcConnection(ctx context.Context, c *echo.Context, body []byte) error {
	var in createVpcConnectionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	conn, err := h.Backend.CreateVpcConnection(ctx,
		in.TargetClusterArn,
		in.VpcID,
		in.Authentication,
		in.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createVpcConnectionOutput{
		VpcConnectionArn: conn.VpcConnectionArn,
		TargetClusterArn: conn.TargetClusterArn,
		VpcID:            conn.VpcID,
		State:            conn.State,
	})
}

func (h *Handler) handleDeleteVpcConnection(ctx context.Context, c *echo.Context, vpcConnectionArn string) error {
	if err := h.Backend.DeleteVpcConnection(ctx, vpcConnectionArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ----------------------------------------
// Cluster policy handlers
// ----------------------------------------

func (h *Handler) handleDeleteClusterPolicy(ctx context.Context, c *echo.Context, clusterArn string) error {
	if err := h.Backend.DeleteClusterPolicy(ctx, clusterArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ----------------------------------------
// Cluster operation request / response types
// ----------------------------------------

type describeClusterOperationOutput struct {
	ClusterOperationInfo *ClusterOperation `json:"clusterOperationInfo"`
}

// ----------------------------------------
// Cluster operation handlers
// ----------------------------------------

func (h *Handler) handleDescribeClusterOperation(
	ctx context.Context,
	c *echo.Context,
	clusterOperationArn string,
) error {
	op, err := h.Backend.DescribeClusterOperation(ctx, clusterOperationArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeClusterOperationOutput{ClusterOperationInfo: op})
}

// ----------------------------------------
// SCRAM list handler
// ----------------------------------------

type listScramSecretsOutput struct {
	SecretArnList []string `json:"secretArnList"`
}

func (h *Handler) handleListScramSecrets(ctx context.Context, c *echo.Context, clusterArn string) error {
	secrets, err := h.Backend.ListScramSecrets(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listScramSecretsOutput{SecretArnList: secrets})
}

// ----------------------------------------
// Replicator describe/list/update handlers
// ----------------------------------------

type listReplicatorsOutput struct {
	Replicators []*Replicator `json:"replicators"`
}

type updateReplicationInfoInput struct {
	Description string `json:"description,omitempty"`
}

type updateReplicationInfoOutput struct {
	ReplicatorArn   string `json:"replicatorArn"`
	ReplicatorState string `json:"replicatorState"`
}

func (h *Handler) handleDescribeReplicator(ctx context.Context, c *echo.Context, replicatorArn string) error {
	r, err := h.Backend.DescribeReplicator(ctx, replicatorArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, r)
}

func (h *Handler) handleListReplicators(ctx context.Context, c *echo.Context) error {
	replicators := h.Backend.ListReplicators(ctx)

	return c.JSON(http.StatusOK, listReplicatorsOutput{Replicators: replicators})
}

func (h *Handler) handleUpdateReplicationInfo(
	ctx context.Context,
	c *echo.Context,
	replicatorArn string,
	body []byte,
) error {
	var in updateReplicationInfoInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	r, err := h.Backend.UpdateReplicationInfo(ctx, replicatorArn, in.Description)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateReplicationInfoOutput{
		ReplicatorArn:   r.ReplicatorArn,
		ReplicatorState: r.ReplicatorState,
	})
}

// ----------------------------------------
// Topic describe/list/update handlers
// ----------------------------------------

type listTopicsOutput struct {
	Topics []*Topic `json:"topics"`
}

type updateTopicInput struct {
	ConfigEntries map[string]string `json:"configEntries,omitempty"`
	NumPartitions int32             `json:"numPartitions"`
}

func (h *Handler) handleDescribeTopic(ctx context.Context, c *echo.Context, resource string) error {
	parts := strings.SplitN(resource, topicKeySeparator, topicKeySeparatorParts)
	if len(parts) != topicKeySeparatorParts {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid resource: missing topic name",
		)
	}

	topic, err := h.Backend.DescribeTopic(ctx, parts[0], parts[1])
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, topic)
}

func (h *Handler) handleDescribeTopicPartitions(ctx context.Context, c *echo.Context, resource string) error {
	parts := strings.SplitN(resource, topicKeySeparator, topicKeySeparatorParts)
	if len(parts) != topicKeySeparatorParts {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid resource: missing topic name",
		)
	}

	topic, err := h.Backend.DescribeTopicPartitions(ctx, parts[0], parts[1])
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, topic)
}

func (h *Handler) handleListTopics(ctx context.Context, c *echo.Context, clusterArn string) error {
	topics, err := h.Backend.ListTopics(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTopicsOutput{Topics: topics})
}

func (h *Handler) handleUpdateTopic(ctx context.Context, c *echo.Context, resource string, body []byte) error {
	parts := strings.SplitN(resource, topicKeySeparator, topicKeySeparatorParts)
	if len(parts) != topicKeySeparatorParts {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid resource: missing topic name",
		)
	}

	var in updateTopicInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	topic, err := h.Backend.UpdateTopic(ctx, parts[0], parts[1], in.NumPartitions, in.ConfigEntries)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, topic)
}

// ----------------------------------------
// VPC connection describe/list handlers
// ----------------------------------------

type listVpcConnectionsOutput struct {
	VpcConnections []*VpcConnection `json:"vpcConnections"`
}

func (h *Handler) handleDescribeVpcConnection(ctx context.Context, c *echo.Context, vpcConnectionArn string) error {
	v, err := h.Backend.DescribeVpcConnection(ctx, vpcConnectionArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, v)
}

func (h *Handler) handleListVpcConnections(ctx context.Context, c *echo.Context) error {
	conns := h.Backend.ListVpcConnections(ctx)

	return c.JSON(http.StatusOK, listVpcConnectionsOutput{VpcConnections: conns})
}

func (h *Handler) handleListClientVpcConnections(ctx context.Context, c *echo.Context, clusterArn string) error {
	conns, err := h.Backend.ListClientVpcConnections(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listVpcConnectionsOutput{VpcConnections: conns})
}

func (h *Handler) handleRejectClientVpcConnection(ctx context.Context, c *echo.Context, vpcConnectionArn string) error {
	if err := h.Backend.RejectClientVpcConnection(ctx, vpcConnectionArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ----------------------------------------
// Cluster policy get/put handlers
// ----------------------------------------

type getClusterPolicyOutput struct {
	Policy string `json:"policy"`
}

type putClusterPolicyInput struct {
	Policy string `json:"policy"`
}

func (h *Handler) handleGetClusterPolicy(ctx context.Context, c *echo.Context, clusterArn string) error {
	policy, err := h.Backend.GetClusterPolicy(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getClusterPolicyOutput{Policy: policy})
}

func (h *Handler) handlePutClusterPolicy(ctx context.Context, c *echo.Context, clusterArn string, body []byte) error {
	var in putClusterPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if err := h.Backend.PutClusterPolicy(ctx, clusterArn, in.Policy); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ----------------------------------------
// Cluster operation list/V2 handlers
// ----------------------------------------

type listClusterOperationsOutput struct {
	ClusterOperationInfoList []*ClusterOperation `json:"clusterOperationInfoList"`
}

type describeClusterOperationV2Output struct {
	ClusterOperationInfo *ClusterOperation `json:"clusterOperationInfo"`
}

type listClusterOperationsV2Output struct {
	ClusterOperationInfoList []*ClusterOperation `json:"clusterOperationInfoList"`
}

func (h *Handler) handleDescribeClusterOperationV2(
	ctx context.Context,
	c *echo.Context,
	clusterOperationArn string,
) error {
	op, err := h.Backend.DescribeClusterOperationV2(ctx, clusterOperationArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeClusterOperationV2Output{ClusterOperationInfo: op})
}

func (h *Handler) handleListClusterOperations(ctx context.Context, c *echo.Context, clusterArn string) error {
	ops, err := h.Backend.ListClusterOperations(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listClusterOperationsOutput{ClusterOperationInfoList: ops})
}

func (h *Handler) handleListClusterOperationsV2(ctx context.Context, c *echo.Context, clusterArn string) error {
	ops, err := h.Backend.ListClusterOperationsV2(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listClusterOperationsV2Output{ClusterOperationInfoList: ops})
}

// ----------------------------------------
// Configuration revision handlers
// ----------------------------------------

type listConfigurationRevisionsOutput struct {
	Revisions []*ConfigurationRevision `json:"revisions"`
}

type updateConfigurationInput struct {
	Description      string `json:"description,omitempty"`
	ServerProperties string `json:"serverProperties,omitempty"`
}

func (h *Handler) handleDescribeConfigurationRevision(ctx context.Context, c *echo.Context, resource string) error {
	parts := strings.SplitN(resource, topicKeySeparator, topicKeySeparatorParts)
	if len(parts) != topicKeySeparatorParts {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid resource")
	}

	configArn := parts[0]
	revisionStr := parts[1]

	var revision int64
	if _, err := fmt.Sscanf(revisionStr, "%d", &revision); err != nil {
		revision = 1
	}

	rev, err := h.Backend.DescribeConfigurationRevision(ctx, configArn, revision)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, rev)
}

func (h *Handler) handleListConfigurationRevisions(ctx context.Context, c *echo.Context, configArn string) error {
	revisions, err := h.Backend.ListConfigurationRevisions(ctx, configArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listConfigurationRevisionsOutput{Revisions: revisions})
}

func (h *Handler) handleUpdateConfiguration(ctx context.Context, c *echo.Context, configArn string, body []byte) error {
	var in updateConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	config, err := h.Backend.UpdateConfiguration(ctx, configArn, in.Description, in.ServerProperties)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createConfigurationOutput{
		Arn:   config.Arn,
		Name:  config.Name,
		State: ClusterStateActive,
		LatestRevision: configurationRevision{
			Revision:    1,
			Description: config.Description,
		},
	})
}

// ----------------------------------------
// Kafka versions / nodes / compatible handlers
// ----------------------------------------

type listKafkaVersionsOutput struct {
	KafkaVersions []*MSKVersion `json:"kafkaVersions"`
}

type compatibleKafkaVersionsOutput struct {
	CompatibleKafkaVersions []*MSKVersion `json:"compatibleKafkaVersions"`
}

type listNodesOutput struct {
	NodeInfoList []*BrokerNode `json:"nodeInfoList"`
}

func (h *Handler) handleListKafkaVersions(ctx context.Context, c *echo.Context) error {
	versions := h.Backend.ListKafkaVersions(ctx)

	return c.JSON(http.StatusOK, listKafkaVersionsOutput{KafkaVersions: versions})
}

func (h *Handler) handleGetCompatibleKafkaVersions(ctx context.Context, c *echo.Context, clusterArn string) error {
	versions, err := h.Backend.GetCompatibleKafkaVersions(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, compatibleKafkaVersionsOutput{CompatibleKafkaVersions: versions})
}

func (h *Handler) handleListNodes(ctx context.Context, c *echo.Context, clusterArn string) error {
	nodes, err := h.Backend.ListNodes(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listNodesOutput{NodeInfoList: nodes})
}

// ----------------------------------------
// Broker reboot handler
// ----------------------------------------

type rebootBrokerInput struct {
	BrokerIDs []string `json:"brokerIds"`
}

type clusterOperationOutput struct {
	ClusterOperationArn string `json:"clusterOperationArn"`
}

func (h *Handler) handleRebootBroker(ctx context.Context, c *echo.Context, clusterArn string, body []byte) error {
	var in rebootBrokerInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	op, err := h.Backend.RebootBroker(ctx, clusterArn, in.BrokerIDs)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

// ----------------------------------------
// Cluster update handlers
// ----------------------------------------

// requireCurrentVersion validates that the supplied currentVersion matches the
// cluster's recorded CurrentVersion, enforcing AWS MSK's optimistic-lock guard.
// It writes an error response and returns (false, err) when validation fails so
// callers can do: if ok, err := h.requireCurrentVersion(...); !ok { return err }.
func (h *Handler) requireCurrentVersion(
	ctx context.Context,
	c *echo.Context,
	clusterArn, version string,
) (bool, error) {
	if version == "" {
		return false, h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"currentVersion is required",
		)
	}

	cl, err := h.Backend.DescribeCluster(ctx, clusterArn)
	if err != nil {
		return false, h.writeBackendError(c, err)
	}

	if cl.CurrentVersion != version {
		return false, h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"The specified cluster version is not current. Current version: "+cl.CurrentVersion+".",
		)
	}

	return true, nil
}

type updateBrokerCountInput struct {
	CurrentVersion            string `json:"currentVersion"`
	TargetNumberOfBrokerNodes int32  `json:"targetNumberOfBrokerNodes"`
}

type updateBrokerStorageInput struct {
	CurrentVersion            string                `json:"currentVersion"`
	TargetBrokerEBSVolumeInfo []brokerEBSVolumeInfo `json:"targetBrokerEBSVolumeInfo"`
}

type brokerEBSVolumeInfo struct {
	KafkaBrokerNodeID string `json:"kafkaBrokerNodeId"`
	VolumeSizeGB      int32  `json:"volumeSizeGB"`
}

type updateBrokerTypeInput struct {
	CurrentVersion     string `json:"currentVersion"`
	TargetInstanceType string `json:"targetInstanceType"`
}

type updateClusterConfigurationInput struct {
	CurrentVersion    string            `json:"currentVersion"`
	ConfigurationInfo ConfigurationInfo `json:"configurationInfo"`
}

type updateClusterKafkaVersionInput struct {
	CurrentVersion     string `json:"currentVersion"`
	TargetKafkaVersion string `json:"targetKafkaVersion"`
}

func (h *Handler) handleUpdateBrokerCount(ctx context.Context, c *echo.Context, clusterArn string, body []byte) error {
	var in updateBrokerCountInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateBrokerCount(ctx, clusterArn, in.TargetNumberOfBrokerNodes)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

func (h *Handler) handleUpdateBrokerStorage(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in updateBrokerStorageInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	var volumeSize int32
	if len(in.TargetBrokerEBSVolumeInfo) > 0 {
		volumeSize = in.TargetBrokerEBSVolumeInfo[0].VolumeSizeGB
	}

	op, err := h.Backend.UpdateBrokerStorage(ctx, clusterArn, volumeSize)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

func (h *Handler) handleUpdateBrokerType(ctx context.Context, c *echo.Context, clusterArn string, body []byte) error {
	var in updateBrokerTypeInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateBrokerType(ctx, clusterArn, in.TargetInstanceType)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

func (h *Handler) handleUpdateClusterConfiguration(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in updateClusterConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateClusterConfiguration(ctx,
		clusterArn,
		in.ConfigurationInfo.Arn,
		in.ConfigurationInfo.Revision,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

func (h *Handler) handleUpdateClusterKafkaVersion(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in updateClusterKafkaVersionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateClusterKafkaVersion(ctx, clusterArn, in.TargetKafkaVersion)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

// updateConnectivityInput is the UpdateConnectivity request body.
type updateConnectivityInput struct {
	ConnectivityInfo *ConnectivityInfo `json:"connectivityInfo"`
	CurrentVersion   string            `json:"currentVersion"`
}

func (h *Handler) handleUpdateConnectivity(ctx context.Context, c *echo.Context, clusterArn string, body []byte) error {
	var in updateConnectivityInput
	if err := decodeJSONBody(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", err.Error())
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateConnectivity(ctx, clusterArn, UpdateConnectivitySettings{
		ConnectivityInfo: in.ConnectivityInfo,
	})
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

// updateMonitoringInput is the UpdateMonitoring request body.
type updateMonitoringInput struct {
	OpenMonitoring     *OpenMonitoring `json:"openMonitoring"`
	LoggingInfo        *LoggingInfo    `json:"loggingInfo"`
	EnhancedMonitoring string          `json:"enhancedMonitoring"`
	CurrentVersion     string          `json:"currentVersion"`
}

func (h *Handler) handleUpdateMonitoring(ctx context.Context, c *echo.Context, clusterArn string, body []byte) error {
	var in updateMonitoringInput
	if err := decodeJSONBody(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", err.Error())
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateMonitoring(ctx, clusterArn, UpdateMonitoringSettings{
		EnhancedMonitoring: in.EnhancedMonitoring,
		OpenMonitoring:     in.OpenMonitoring,
		LoggingInfo:        in.LoggingInfo,
	})
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

func (h *Handler) handleUpdateRebalancing(ctx context.Context, c *echo.Context, clusterArn string, _ []byte) error {
	op, err := h.Backend.UpdateRebalancing(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

// updateSecurityInput is the UpdateSecurity request body.
type updateSecurityInput struct {
	ClientAuthentication *ClientAuthentication `json:"clientAuthentication"`
	EncryptionInfo       *EncryptionInfo       `json:"encryptionInfo"`
	CurrentVersion       string                `json:"currentVersion"`
}

func (h *Handler) handleUpdateSecurity(ctx context.Context, c *echo.Context, clusterArn string, body []byte) error {
	var in updateSecurityInput
	if err := decodeJSONBody(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", err.Error())
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateSecurity(ctx, clusterArn, UpdateSecuritySettings{
		ClientAuthentication: in.ClientAuthentication,
		EncryptionInfo:       in.EncryptionInfo,
	})
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

// updateStorageInput is the UpdateStorage request body.
type updateStorageInput struct {
	ProvisionedThroughput *ProvisionedThroughput `json:"provisionedThroughput"`
	StorageMode           string                 `json:"storageMode"`
	CurrentVersion        string                 `json:"currentVersion"`
	VolumeSizeGB          int32                  `json:"volumeSizeGB"`
}

func (h *Handler) handleUpdateStorage(ctx context.Context, c *echo.Context, clusterArn string, body []byte) error {
	var in updateStorageInput
	if err := decodeJSONBody(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", err.Error())
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateStorage(ctx, clusterArn, UpdateStorageSettings{
		StorageMode:           in.StorageMode,
		VolumeSizeGB:          in.VolumeSizeGB,
		ProvisionedThroughput: in.ProvisionedThroughput,
	})
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
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
