package kafka

import (
	"encoding/json"
	"errors"
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
	clustersV1Prefix       = "/v1/clusters/"
	clustersV2Prefix       = "/v2/clusters/"
	configurationsPrefix   = "/v1/configurations/"
	tagsPrefix             = "/v1/tags/"
	bootstrapBrokersSuffix = "/bootstrap-brokers"

	arnMaxParts           = 6 // arn:partition:service:region:account:resource
	arnMinPartsForService = 3 // minimum ARN parts needed to read service field at index 2
)

// Handler is the HTTP handler for the MSK REST API.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Kafka handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Kafka" }

// GetSupportedOperations returns the list of supported MSK operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCluster",
		"CreateClusterV2",
		"ListClusters",
		"ListClustersV2",
		"DescribeCluster",
		"DescribeClusterV2",
		"DeleteCluster",
		"GetBootstrapBrokers",
		"CreateConfiguration",
		"ListConfigurations",
		"DescribeConfiguration",
		"DeleteConfiguration",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
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
			strings.HasPrefix(p, "/v2/clusters") ||
			strings.HasPrefix(p, "/v1/configurations") ||
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
	switch {
	case path == "/v1/clusters" || path == "/v1/clusters/":
		return parseClusterRootV1(method)
	case strings.HasPrefix(path, clustersV1Prefix):
		return parseClusterResourceV1(method, path[len(clustersV1Prefix):])
	case path == "/v2/clusters" || path == "/v2/clusters/":
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

func parseClusterRootV1(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return "ListClusters", ""
	case http.MethodPost:
		return "CreateCluster", ""
	}

	return "", ""
}

func parseClusterResourceV1(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	if strings.HasSuffix(decoded, bootstrapBrokersSuffix) {
		arnStr := decoded[:len(decoded)-len(bootstrapBrokersSuffix)]

		if method == http.MethodGet {
			return "GetBootstrapBrokers", arnStr
		}

		return "", ""
	}

	switch method {
	case http.MethodGet:
		return "DescribeCluster", decoded
	case http.MethodDelete:
		return "DeleteCluster", decoded
	}

	return "", ""
}

func parseClusterRootV2(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return "ListClustersV2", ""
	case http.MethodPost:
		return "CreateClusterV2", ""
	}

	return "", ""
}

func parseClusterResourceV2(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	if method == http.MethodGet {
		return "DescribeClusterV2", decoded
	}

	return "", ""
}

func parseConfigurationRoot(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return "ListConfigurations", ""
	case http.MethodPost:
		return "CreateConfiguration", ""
	}

	return "", ""
}

func parseConfigurationResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	switch method {
	case http.MethodGet:
		return "DescribeConfiguration", decoded
	case http.MethodDelete:
		return "DeleteConfiguration", decoded
	}

	return "", ""
}

func parseTagsResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	switch method {
	case http.MethodGet:
		return "ListTagsForResource", decoded
	case http.MethodPost:
		return "TagResource", decoded
	case http.MethodDelete:
		return "UntagResource", decoded
	}

	return "", ""
}

// dispatch routes a parsed operation to the appropriate handler.
//
//nolint:cyclop // dispatch table has necessary branches for each operation
func (h *Handler) dispatch(c *echo.Context, op, resource string, body []byte) error {
	switch op {
	case "CreateCluster":
		return h.handleCreateCluster(c, body)
	case "CreateClusterV2":
		return h.handleCreateClusterV2(c, body)
	case "ListClusters":
		return h.handleListClusters(c)
	case "ListClustersV2":
		return h.handleListClustersV2(c)
	case "DescribeCluster":
		return h.handleDescribeCluster(c, resource)
	case "DescribeClusterV2":
		return h.handleDescribeClusterV2(c, resource)
	case "DeleteCluster":
		return h.handleDeleteCluster(c, resource)
	case "GetBootstrapBrokers":
		return h.handleGetBootstrapBrokers(c, resource)
	case "CreateConfiguration":
		return h.handleCreateConfiguration(c, body)
	case "ListConfigurations":
		return h.handleListConfigurations(c)
	case "DescribeConfiguration":
		return h.handleDescribeConfiguration(c, resource)
	case "DeleteConfiguration":
		return h.handleDeleteConfiguration(c, resource)
	case "ListTagsForResource":
		return h.handleListTagsForResource(c, resource)
	case "TagResource":
		return h.handleTagResource(c, resource, body)
	case "UntagResource":
		return h.handleUntagResource(c, resource, c.Request().URL)
	}

	return h.writeError(c, http.StatusNotFound, "NotFoundException", "unknown operation: "+op)
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

type describeClusterOutput struct {
	ClusterInfo *Cluster `json:"clusterInfo"`
}

type listClustersOutput struct {
	ClusterInfoList []*Cluster `json:"clusterInfoList"`
}

type provisionedClusterInfo struct {
	KafkaVersion        string              `json:"kafkaVersion"`
	State               string              `json:"state"`
	BrokerNodeGroupInfo BrokerNodeGroupInfo `json:"brokerNodeGroupInfo"`
	NumberOfBrokerNodes int32               `json:"numberOfBrokerNodes"`
}

type clusterInfoV2 struct {
	Tags        map[string]string       `json:"tags,omitempty"`
	Provisioned *provisionedClusterInfo `json:"provisioned,omitempty"`
	ClusterArn  string                  `json:"clusterArn"`
	ClusterName string                  `json:"clusterName"`
	ClusterType string                  `json:"clusterType"`
}

type describeClusterV2Output struct {
	ClusterInfo *clusterInfoV2 `json:"clusterInfo"`
}

type listClustersV2Output struct {
	Clusters []*clusterInfoV2 `json:"clusters"`
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

	return c.JSON(http.StatusOK, listClustersOutput{ClusterInfoList: clusters})
}

func (h *Handler) handleListClustersV2(c *echo.Context) error {
	clusters := h.Backend.ListClusters()
	out := make([]*clusterInfoV2, 0, len(clusters))

	for _, cl := range clusters {
		out = append(out, toClusterInfoV2(cl))
	}

	return c.JSON(http.StatusOK, listClustersV2Output{Clusters: out})
}

func (h *Handler) handleDescribeCluster(c *echo.Context, clusterArn string) error {
	cluster, err := h.Backend.DescribeCluster(clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeClusterOutput{ClusterInfo: cluster})
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

// toClusterInfoV2 converts a Cluster to the V2 cluster info shape.
func toClusterInfoV2(cl *Cluster) *clusterInfoV2 {
	return &clusterInfoV2{
		ClusterArn:  cl.ClusterArn,
		ClusterName: cl.ClusterName,
		ClusterType: "PROVISIONED",
		Tags:        cl.Tags,
		Provisioned: &provisionedClusterInfo{
			BrokerNodeGroupInfo: cl.BrokerNodeGroupInfo,
			KafkaVersion:        cl.KafkaVersion,
			NumberOfBrokerNodes: cl.NumberOfBrokerNodes,
			State:               cl.State,
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
	}

	return h.writeError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
}
