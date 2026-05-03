package kafka

import (
	"encoding/json"
	"errors"
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
	opBatchAssociateScramSecret    = "BatchAssociateScramSecret"
	opBatchDisassociateScramSecret = "BatchDisassociateScramSecret"
	opCreateCluster                = "CreateCluster"
	opCreateClusterV2              = "CreateClusterV2"
	opCreateConfiguration          = "CreateConfiguration"
	opCreateReplicator             = "CreateReplicator"
	opCreateTopic                  = "CreateTopic"
	opCreateVpcConnection          = "CreateVpcConnection"
	opDeleteCluster                = "DeleteCluster"
	opDeleteClusterPolicy          = "DeleteClusterPolicy"
	opDeleteConfiguration          = "DeleteConfiguration"
	opDeleteReplicator             = "DeleteReplicator"
	opDeleteTopic                  = "DeleteTopic"
	opDeleteVpcConnection          = "DeleteVpcConnection"
	opDescribeCluster              = "DescribeCluster"
	opDescribeClusterOperation     = "DescribeClusterOperation"
	opDescribeClusterV2            = "DescribeClusterV2"
	opDescribeConfiguration        = "DescribeConfiguration"
	opGetBootstrapBrokers          = "GetBootstrapBrokers"
	opListClusters                 = "ListClusters"
	opListClustersV2               = "ListClustersV2"
	opListConfigurations           = "ListConfigurations"
	opListTagsForResource          = "ListTagsForResource"
	opTagResource                  = "TagResource"
	opUntagResource                = "UntagResource"
)

const (
	clustersV1Prefix       = "/v1/clusters/"
	clustersV2Prefix       = "/api/v2/clusters/"
	configurationsPrefix   = "/v1/configurations/"
	tagsPrefix             = "/v1/tags/"
	bootstrapBrokersSuffix = "/bootstrap-brokers"
	scramSecretsSuffix     = "/scram-secrets"
	topicsSuffix           = "/topics"
	policySuffix           = "/policy"
	operationsPrefix       = "/v1/operations/"
	replicatorsPrefix      = "/replication/v1/replicators/"
	replicatorsRoot        = "/replication/v1/replicators"
	vpcConnectionPrefix    = "/v1/vpc-connection/"
	vpcConnectionRoot      = "/v1/vpc-connection"

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
		opDescribeClusterV2,
		opDescribeConfiguration,
		opGetBootstrapBrokers,
		opListClusters,
		opListClustersV2,
		opListConfigurations,
		opListTagsForResource,
		opTagResource,
		opUntagResource,
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
			strings.HasPrefix(p, "/replication/v1/replicators") ||
			p == vpcConnectionRoot ||
			strings.HasPrefix(p, vpcConnectionPrefix) ||
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
		ctx := c.Request().Context()
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

		return h.dispatch(c, op, resource, body)
	}
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

// parseExtendedOpsPath handles operations, replicators, and VPC connection paths.
func parseExtendedOpsPath(method, path string) (string, string) {
	switch {
	case strings.HasPrefix(path, operationsPrefix):
		return parseOperationResource(method, path[len(operationsPrefix):])
	case path == replicatorsRoot || path == replicatorsRoot+"/":
		return parseReplicatorsRoot(method)
	case strings.HasPrefix(path, replicatorsPrefix):
		return parseReplicatorResource(method, path[len(replicatorsPrefix):])
	case path == vpcConnectionRoot || path == vpcConnectionRoot+"/":
		return parseVpcConnectionRoot(method)
	case strings.HasPrefix(path, vpcConnectionPrefix):
		return parseVpcConnectionResource(method, path[len(vpcConnectionPrefix):])
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

func parseClusterResourceV1(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	// /topics/{topicName}: must be checked before /topics suffix.
	if idx := strings.Index(decoded, topicsSuffix+"/"); idx != -1 {
		arnStr := decoded[:idx]
		topicName := decoded[idx+len(topicsSuffix)+1:]

		if method == http.MethodDelete {
			return opDeleteTopic, arnStr + topicKeySeparator + topicName
		}

		return "", ""
	}

	// /topics (no trailing topic name): CreateTopic.
	if strings.HasSuffix(decoded, topicsSuffix) {
		arnStr := decoded[:len(decoded)-len(topicsSuffix)]

		if method == http.MethodPost {
			return opCreateTopic, arnStr
		}

		return "", ""
	}

	// /scram-secrets: BatchAssociateScramSecret (POST) or BatchDisassociateScramSecret (PATCH).
	if strings.HasSuffix(decoded, scramSecretsSuffix) {
		arnStr := decoded[:len(decoded)-len(scramSecretsSuffix)]

		switch method {
		case http.MethodPost:
			return opBatchAssociateScramSecret, arnStr
		case http.MethodPatch:
			return opBatchDisassociateScramSecret, arnStr
		}

		return "", ""
	}

	// /policy: DeleteClusterPolicy.
	if strings.HasSuffix(decoded, policySuffix) {
		arnStr := decoded[:len(decoded)-len(policySuffix)]

		if method == http.MethodDelete {
			return opDeleteClusterPolicy, arnStr
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

	switch method {
	case http.MethodGet:
		return opDescribeCluster, decoded
	case http.MethodDelete:
		return opDeleteCluster, decoded
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

	switch method {
	case http.MethodGet:
		return opDescribeConfiguration, decoded
	case http.MethodDelete:
		return opDeleteConfiguration, decoded
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

func parseReplicatorsRoot(method string) (string, string) {
	if method == http.MethodPost {
		return opCreateReplicator, ""
	}

	return "", ""
}

func parseReplicatorResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	if method == http.MethodDelete {
		return opDeleteReplicator, decoded
	}

	return "", ""
}

func parseVpcConnectionRoot(method string) (string, string) {
	if method == http.MethodPost {
		return opCreateVpcConnection, ""
	}

	return "", ""
}

func parseVpcConnectionResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	if method == http.MethodDelete {
		return opDeleteVpcConnection, decoded
	}

	return "", ""
}

// dispatch routes a parsed operation to the appropriate handler.
func (h *Handler) dispatch(c *echo.Context, op, resource string, body []byte) error {
	if ok, err := h.dispatchCoreOps(c, op, resource, body); ok {
		return err
	}

	if ok, err := h.dispatchNewOps(c, op, resource, body); ok {
		return err
	}

	return h.writeError(c, http.StatusNotFound, "NotFoundException", "unknown operation: "+op)
}

// dispatchCoreOps handles cluster, configuration, and tag operations.
// Returns (true, err) if the operation was handled, (false, nil) otherwise.
//
//nolint:cyclop // dispatch switch complexity is inherent to the number of operations
func (h *Handler) dispatchCoreOps(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case opCreateCluster:
		return true, h.handleCreateCluster(c, body)
	case opCreateClusterV2:
		return true, h.handleCreateClusterV2(c, body)
	case opListClusters:
		return true, h.handleListClusters(c)
	case opListClustersV2:
		return true, h.handleListClustersV2(c)
	case opDescribeCluster:
		return true, h.handleDescribeCluster(c, resource)
	case opDescribeClusterV2:
		return true, h.handleDescribeClusterV2(c, resource)
	case opDeleteCluster:
		return true, h.handleDeleteCluster(c, resource)
	case opGetBootstrapBrokers:
		return true, h.handleGetBootstrapBrokers(c, resource)
	case opCreateConfiguration:
		return true, h.handleCreateConfiguration(c, body)
	case opListConfigurations:
		return true, h.handleListConfigurations(c)
	case opDescribeConfiguration:
		return true, h.handleDescribeConfiguration(c, resource)
	case opDeleteConfiguration:
		return true, h.handleDeleteConfiguration(c, resource)
	case opListTagsForResource:
		return true, h.handleListTagsForResource(c, resource)
	case opTagResource:
		return true, h.handleTagResource(c, resource, body)
	case opUntagResource:
		return true, h.handleUntagResource(c, resource, c.Request().URL)
	}

	return false, nil
}

// dispatchNewOps handles SCRAM secrets, replicator, topic, VPC connection, and cluster policy operations.
// Returns (true, err) if the operation was handled, (false, nil) otherwise.
func (h *Handler) dispatchNewOps(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case opBatchAssociateScramSecret:
		return true, h.handleBatchAssociateScramSecret(c, resource, body)
	case opBatchDisassociateScramSecret:
		return true, h.handleBatchDisassociateScramSecret(c, resource, body)
	case opCreateReplicator:
		return true, h.handleCreateReplicator(c, body)
	case opDeleteReplicator:
		return true, h.handleDeleteReplicator(c, resource)
	case opCreateTopic:
		return true, h.handleCreateTopic(c, resource, body)
	case opDeleteTopic:
		return true, h.handleDeleteTopic(c, resource)
	case opCreateVpcConnection:
		return true, h.handleCreateVpcConnection(c, body)
	case opDeleteVpcConnection:
		return true, h.handleDeleteVpcConnection(c, resource)
	case opDeleteClusterPolicy:
		return true, h.handleDeleteClusterPolicy(c, resource)
	case opDescribeClusterOperation:
		return true, h.handleDescribeClusterOperation(c, resource)
	}

	return false, nil
}

// ----------------------------------------
// Request / response types
// ----------------------------------------

type createClusterInput struct {
	Tags                map[string]string   `json:"tags,omitempty"`
	ClusterName         string              `json:"clusterName"`
	KafkaVersion        string              `json:"kafkaVersion"`
	BrokerNodeGroupInfo BrokerNodeGroupInfo `json:"brokerNodeGroupInfo"`
	NumberOfBrokerNodes int32               `json:"numberOfBrokerNodes"`
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
	Tags                      map[string]string   `json:"tags,omitempty"`
	CurrentBrokerSoftwareInfo *brokerSoftwareInfo `json:"currentBrokerSoftwareInfo,omitempty"`
	ClusterArn                string              `json:"clusterArn"`
	ClusterName               string              `json:"clusterName"`
	KafkaVersion              string              `json:"kafkaVersion"`
	State                     string              `json:"state"`
	CurrentVersion            string              `json:"currentVersion"`
	BrokerNodeGroupInfo       BrokerNodeGroupInfo `json:"brokerNodeGroupInfo"`
	NumberOfBrokerNodes       int32               `json:"numberOfBrokerNodes"`
}

type describeClusterOutput struct {
	ClusterInfo *clusterInfoV1 `json:"clusterInfo"`
}

type listClustersOutput struct {
	ClusterInfoList []*clusterInfoV1 `json:"clusterInfoList"`
}

type provisionedClusterInfo struct {
	CurrentBrokerSoftwareInfo *brokerSoftwareInfo `json:"currentBrokerSoftwareInfo,omitempty"`
	KafkaVersion              string              `json:"kafkaVersion"`
	State                     string              `json:"state"`
	BrokerNodeGroupInfo       BrokerNodeGroupInfo `json:"brokerNodeGroupInfo"`
	NumberOfBrokerNodes       int32               `json:"numberOfBrokerNodes"`
}

type clusterInfoV2 struct {
	Tags           map[string]string       `json:"tags,omitempty"`
	Provisioned    *provisionedClusterInfo `json:"provisioned,omitempty"`
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
	BootstrapBrokerString    string `json:"bootstrapBrokerString"`
	BootstrapBrokerStringTLS string `json:"bootstrapBrokerStringTls"`
}

type createClusterV2Input struct {
	Tags        map[string]string `json:"tags,omitempty"`
	Provisioned *provisionedInput `json:"provisioned,omitempty"`
	ClusterName string            `json:"clusterName"`
}

type provisionedInput struct {
	KafkaVersion        string              `json:"kafkaVersion"`
	BrokerNodeGroupInfo BrokerNodeGroupInfo `json:"brokerNodeGroupInfo"`
	NumberOfBrokerNodes int32               `json:"numberOfBrokerNodes"`
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
	Arn  string `json:"arn"`
	Name string `json:"name"`
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
}

// ----------------------------------------
// Cluster handlers
// ----------------------------------------

func (h *Handler) handleCreateCluster(c *echo.Context, body []byte) error {
	var in createClusterInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	cluster, err := h.Backend.CreateCluster(
		in.ClusterName,
		in.KafkaVersion,
		in.NumberOfBrokerNodes,
		in.BrokerNodeGroupInfo,
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

func (h *Handler) handleCreateClusterV2(c *echo.Context, body []byte) error {
	var in createClusterV2Input
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	var brokerInfo BrokerNodeGroupInfo

	var kafkaVersion string

	var numBrokers int32

	if in.Provisioned != nil {
		brokerInfo = in.Provisioned.BrokerNodeGroupInfo
		kafkaVersion = in.Provisioned.KafkaVersion
		numBrokers = in.Provisioned.NumberOfBrokerNodes
	}

	cluster, err := h.Backend.CreateCluster(in.ClusterName, kafkaVersion, numBrokers, brokerInfo, in.Tags)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createClusterV2Output{
		ClusterArn:  cluster.ClusterArn,
		ClusterName: cluster.ClusterName,
		ClusterType: "PROVISIONED",
	})
}

func (h *Handler) handleListClusters(c *echo.Context) error {
	clusters := h.Backend.ListClusters()
	out := make([]*clusterInfoV1, 0, len(clusters))

	for _, cl := range clusters {
		out = append(out, toClusterInfoV1(cl))
	}

	return c.JSON(http.StatusOK, listClustersOutput{ClusterInfoList: out})
}

func (h *Handler) handleListClustersV2(c *echo.Context) error {
	clusters := h.Backend.ListClusters()
	out := make([]*clusterInfoV2, 0, len(clusters))

	for _, cl := range clusters {
		out = append(out, toClusterInfoV2(cl))
	}

	return c.JSON(http.StatusOK, listClustersV2Output{ClusterInfoList: out})
}

func (h *Handler) handleDescribeCluster(c *echo.Context, clusterArn string) error {
	cluster, err := h.Backend.DescribeCluster(clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeClusterOutput{ClusterInfo: toClusterInfoV1(cluster)})
}

func (h *Handler) handleDescribeClusterV2(c *echo.Context, clusterArn string) error {
	cluster, err := h.Backend.DescribeCluster(clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeClusterV2Output{ClusterInfo: toClusterInfoV2(cluster)})
}

func (h *Handler) handleDeleteCluster(c *echo.Context, clusterArn string) error {
	if err := h.Backend.DeleteCluster(clusterArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetBootstrapBrokers(c *echo.Context, clusterArn string) error {
	if _, err := h.Backend.DescribeCluster(clusterArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getBootstrapBrokersOutput{
		BootstrapBrokerString:    "localhost:9092",
		BootstrapBrokerStringTLS: "localhost:9094",
	})
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
		Tags:                      maps.Clone(cl.Tags),
		CurrentBrokerSoftwareInfo: brokerSoftwareInfoFor(cl.KafkaVersion),
	}
}

// toClusterInfoV2 converts a Cluster to the V2 cluster info shape.
func toClusterInfoV2(cl *Cluster) *clusterInfoV2 {
	return &clusterInfoV2{
		ClusterArn:     cl.ClusterArn,
		ClusterName:    cl.ClusterName,
		ClusterType:    "PROVISIONED",
		State:          cl.State,
		CurrentVersion: cl.CurrentVersion,
		Tags:           maps.Clone(cl.Tags),
		Provisioned: &provisionedClusterInfo{
			BrokerNodeGroupInfo:       cl.BrokerNodeGroupInfo,
			KafkaVersion:              cl.KafkaVersion,
			NumberOfBrokerNodes:       cl.NumberOfBrokerNodes,
			State:                     cl.State,
			CurrentBrokerSoftwareInfo: brokerSoftwareInfoFor(cl.KafkaVersion),
		},
	}
}

// ----------------------------------------
// Configuration handlers
// ----------------------------------------

func (h *Handler) handleCreateConfiguration(c *echo.Context, body []byte) error {
	var in createConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	config, err := h.Backend.CreateConfiguration(in.Name, in.Description, in.KafkaVersions, in.ServerProperties)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createConfigurationOutput{
		Arn:  config.Arn,
		Name: config.Name,
	})
}

func (h *Handler) handleListConfigurations(c *echo.Context) error {
	configs := h.Backend.ListConfigurations()

	return c.JSON(http.StatusOK, listConfigurationsOutput{Configurations: configs})
}

func (h *Handler) handleDescribeConfiguration(c *echo.Context, configArn string) error {
	config, err := h.Backend.DescribeConfiguration(configArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, config)
}

func (h *Handler) handleDeleteConfiguration(c *echo.Context, configArn string) error {
	if err := h.Backend.DeleteConfiguration(configArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ----------------------------------------
// Tag handlers
// ----------------------------------------

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceArn string) error {
	tags, err := h.Backend.GetTags(resourceArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsOutput{Tags: tags})
}

func (h *Handler) handleTagResource(c *echo.Context, resourceArn string, body []byte) error {
	var in tagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if err := h.Backend.TagResource(resourceArn, in.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceArn string, u *url.URL) error {
	tagKeys := u.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceArn, tagKeys); err != nil {
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

func (h *Handler) handleBatchAssociateScramSecret(c *echo.Context, clusterArn string, body []byte) error {
	var in scramSecretInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid request body: "+err.Error())
	}

	errs, err := h.Backend.BatchAssociateScramSecret(clusterArn, in.SecretArnList)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, batchScramSecretOutput{UnprocessedScramSecrets: errs})
}

func (h *Handler) handleBatchDisassociateScramSecret(c *echo.Context, clusterArn string, body []byte) error {
	var in scramSecretInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid request body: "+err.Error())
	}

	errs, err := h.Backend.BatchDisassociateScramSecret(clusterArn, in.SecretArnList)
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

func (h *Handler) handleCreateReplicator(c *echo.Context, body []byte) error {
	var in createReplicatorInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid request body: "+err.Error())
	}

	replicator, err := h.Backend.CreateReplicator(
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

func (h *Handler) handleDeleteReplicator(c *echo.Context, replicatorArn string) error {
	if err := h.Backend.DeleteReplicator(replicatorArn); err != nil {
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

func (h *Handler) handleCreateTopic(c *echo.Context, clusterArn string, body []byte) error {
	var in createTopicInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid request body: "+err.Error())
	}

	topic, err := h.Backend.CreateTopic(
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

func (h *Handler) handleDeleteTopic(c *echo.Context, resource string) error {
	parts := strings.SplitN(resource, topicKeySeparator, topicKeySeparatorParts)
	if len(parts) != topicKeySeparatorParts {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid resource: missing topic name")
	}

	clusterArn, topicName := parts[0], parts[1]

	if err := h.Backend.DeleteTopic(clusterArn, topicName); err != nil {
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

func (h *Handler) handleCreateVpcConnection(c *echo.Context, body []byte) error {
	var in createVpcConnectionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid request body: "+err.Error())
	}

	conn, err := h.Backend.CreateVpcConnection(in.TargetClusterArn, in.VpcID, in.Authentication, in.Tags)
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

func (h *Handler) handleDeleteVpcConnection(c *echo.Context, vpcConnectionArn string) error {
	if err := h.Backend.DeleteVpcConnection(vpcConnectionArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ----------------------------------------
// Cluster policy handlers
// ----------------------------------------

func (h *Handler) handleDeleteClusterPolicy(c *echo.Context, clusterArn string) error {
	if err := h.Backend.DeleteClusterPolicy(clusterArn); err != nil {
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

func (h *Handler) handleDescribeClusterOperation(c *echo.Context, clusterOperationArn string) error {
	op, err := h.Backend.DescribeClusterOperation(clusterOperationArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeClusterOperationOutput{ClusterOperationInfo: op})
}

// ----------------------------------------
// Error helpers
// ----------------------------------------

func (h *Handler) writeError(c *echo.Context, status int, code, message string) error {
	return c.JSON(status, kafkaErrorResponse{
		Message:   message,
		ErrorCode: code,
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
