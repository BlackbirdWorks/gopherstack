package redshift

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	redshiftVersion = "2012-12-01"
	redshiftXMLNS   = "http://redshift.amazonaws.com/doc/2012-12-01/"
)

// Handler is the Echo HTTP handler for Redshift operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Redshift handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Redshift" }

// GetSupportedOperations returns supported Redshift operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCluster",
		"DeleteCluster",
		"DescribeClusters",
		"DescribeLoggingStatus",
		"DescribeTags",
		"CreateTags",
		"DeleteTags",
	}
}

// RouteMatcher returns a function that matches Redshift requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
			return false
		}
		if strings.HasPrefix(r.URL.Path, "/dashboard/") {
			return false
		}
		ct := r.Header.Get("Content-Type")
		if !strings.Contains(ct, "application/x-www-form-urlencoded") {
			return false
		}
		body, err := httputils.ReadBody(r)
		if err != nil {
			return false
		}
		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return false
		}

		return vals.Get("Version") == redshiftVersion
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityFormRedshift }

// ExtractOperation extracts the Redshift action from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return "Unknown"
	}
	action := r.Form.Get("Action")
	if action == "" {
		return "Unknown"
	}

	return action
}

// ExtractResource returns the cluster identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	return r.Form.Get("ClusterIdentifier")
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		if err := r.ParseForm(); err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		vals := r.Form
		action := vals.Get("Action")
		if action == "" {
			return h.writeError(c, http.StatusBadRequest, "MissingAction", "missing Action parameter")
		}

		return h.dispatch(c, action, vals)
	}
}

type redshiftActionFn func(vals url.Values) (any, error)

func (h *Handler) dispatchTable() map[string]redshiftActionFn {
	return map[string]redshiftActionFn{
		"CreateCluster":         h.handleCreateCluster,
		"DeleteCluster":         h.handleDeleteCluster,
		"DescribeClusters":      h.handleDescribeClusters,
		"DescribeLoggingStatus": func(_ url.Values) (any, error) { return h.loggingStatusResponse(), nil },
		"DescribeTags":          func(_ url.Values) (any, error) { return h.describeTagsResponse(), nil },
		"CreateTags":            h.handleCreateTags,
		"DeleteTags":            h.handleDeleteTags,
	}
}

// dispatch routes the Redshift action to the appropriate handler function.
func (h *Handler) dispatch(c *echo.Context, action string, vals url.Values) error {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return h.writeError(c, http.StatusBadRequest, "InvalidAction",
			fmt.Sprintf("%s is not a valid Redshift action", action))
	}

	resp, opErr := fn(vals)
	if opErr != nil {
		return h.handleOpError(c, action, opErr)
	}

	return h.writeXMLResponse(c, http.StatusOK, resp)
}

func (h *Handler) handleCreateCluster(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")
	nodeType := vals.Get("NodeType")
	dbName := vals.Get("DBName")
	masterUser := vals.Get("MasterUsername")

	cluster, err := h.Backend.CreateCluster(id, nodeType, dbName, masterUser)
	if err != nil {
		return nil, err
	}

	return &createClusterResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleDeleteCluster(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")
	cluster, err := h.Backend.DeleteCluster(id)
	if err != nil {
		return nil, err
	}

	return &deleteClusterResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleDescribeClusters(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")
	clusters, err := h.Backend.DescribeClusters(id)
	if err != nil {
		return nil, err
	}
	members := make([]xmlCluster, 0, len(clusters))
	for _, c := range clusters {
		cp := c
		members = append(members, toXMLCluster(&cp))
	}

	return &describeClustersResponse{
		Xmlns:    redshiftXMLNS,
		Clusters: xmlClusterList{Members: members},
	}, nil
}

func toXMLCluster(c *Cluster) xmlCluster {
	return xmlCluster{
		ClusterIdentifier:                c.ClusterIdentifier,
		NodeType:                         c.NodeType,
		Endpoint:                         c.Endpoint,
		ClusterStatus:                    c.Status,
		ClusterAvailabilityStatus:        "Available",
		AvailabilityZoneRelocationStatus: "disabled",
		MultiAZ:                          "Disabled",
		AquaConfiguration:                xmlAquaConfig{AquaConfigurationStatus: "disabled", AquaStatus: "disabled"},
		ClusterNodes: xmlClusterNodes{
			Members: []xmlClusterNode{{
				NodeRole:         "LEADER",
				PrivateIPAddress: "10.0.0.1",
				PublicIPAddress:  "0.0.0.0",
			}},
		},
		ClusterParameterGroups: xmlClusterParamGroups{
			Members: []xmlClusterParamGroup{{
				ParameterGroupName:   "default.redshift-1.0",
				ParameterApplyStatus: "in-sync",
			}},
		},
		DBName:         c.DBName,
		MasterUsername: c.MasterUsername,
	}
}

func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	statusCode := http.StatusBadRequest
	var code string
	switch {
	case errors.Is(opErr, ErrClusterNotFound):
		code = "ClusterNotFound"
	case errors.Is(opErr, ErrClusterAlreadyExists):
		code = "ClusterAlreadyExists"
	case errors.Is(opErr, ErrInvalidParameter):
		code = "RedshiftInvalidParameter"
	default:
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		logger.Load(c.Request().Context()).Error("Redshift internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, statusCode, code, opErr.Error())
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &redshiftErrorResponse{
		Xmlns: redshiftXMLNS,
		Error: redshiftError{Code: code, Message: message, Type: "Sender"},
	}
	xmlBytes, err := marshalXML(errResp)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(statusCode, "text/xml", xmlBytes)
}

func marshalXML(v any) ([]byte, error) {
	raw, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}

	return append([]byte(xml.Header), raw...), nil
}

// ---- XML response types ----

type redshiftError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

type redshiftErrorResponse struct {
	XMLName xml.Name      `xml:"ErrorResponse"`
	Xmlns   string        `xml:"xmlns,attr"`
	Error   redshiftError `xml:"Error"`
}

type xmlCluster struct {
	AquaConfiguration                xmlAquaConfig         `xml:"AquaConfiguration"`
	ClusterIdentifier                string                `xml:"ClusterIdentifier"`
	NodeType                         string                `xml:"NodeType"`
	Endpoint                         string                `xml:"Endpoint>Address"`
	ClusterStatus                    string                `xml:"ClusterStatus"`
	ClusterAvailabilityStatus        string                `xml:"ClusterAvailabilityStatus"`
	AvailabilityZoneRelocationStatus string                `xml:"AvailabilityZoneRelocationStatus"`
	MultiAZ                          string                `xml:"MultiAZ"`
	DBName                           string                `xml:"DBName"`
	MasterUsername                   string                `xml:"MasterUsername"`
	ClusterNodes                     xmlClusterNodes       `xml:"ClusterNodes"`
	ClusterParameterGroups           xmlClusterParamGroups `xml:"ClusterParameterGroups"`
}

type xmlAquaConfig struct {
	AquaConfigurationStatus string `xml:"AquaConfigurationStatus"`
	AquaStatus              string `xml:"AquaStatus"`
}

type xmlClusterNode struct {
	NodeRole         string `xml:"NodeRole"`
	PrivateIPAddress string `xml:"PrivateIPAddress"`
	PublicIPAddress  string `xml:"PublicIPAddress"`
}

type xmlClusterNodes struct {
	Members []xmlClusterNode `xml:"member"`
}

type xmlClusterParamGroup struct {
	ParameterGroupName   string `xml:"ParameterGroupName"`
	ParameterApplyStatus string `xml:"ParameterApplyStatus"`
}

type xmlClusterParamGroups struct {
	Members []xmlClusterParamGroup `xml:"ClusterParameterGroup"`
}

type createClusterResponse struct {
	XMLName xml.Name   `xml:"CreateClusterResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"CreateClusterResult>Cluster"`
}

type deleteClusterResponse struct {
	XMLName xml.Name   `xml:"DeleteClusterResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"DeleteClusterResult>Cluster"`
}

type xmlClusterList struct {
	Members []xmlCluster `xml:"Cluster"`
}

type describeClustersResponse struct {
	XMLName  xml.Name       `xml:"DescribeClustersResponse"`
	Xmlns    string         `xml:"xmlns,attr"`
	Clusters xmlClusterList `xml:"DescribeClustersResult>Clusters"`
}

func (h *Handler) writeXMLResponse(c *echo.Context, status int, v any) error {
	xmlBytes, err := marshalXML(v)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(status, "text/xml", xmlBytes)
}

func (h *Handler) loggingStatusResponse() any {
	type describeLoggingStatusResult struct {
		XMLName        xml.Name `xml:"DescribeLoggingStatusResult"`
		LoggingEnabled bool     `xml:"LoggingEnabled"`
	}
	type response struct {
		XMLName                     xml.Name                    `xml:"DescribeLoggingStatusResponse"`
		Xmlns                       string                      `xml:"xmlns,attr"`
		DescribeLoggingStatusResult describeLoggingStatusResult `xml:"DescribeLoggingStatusResult"`
	}

	return &response{
		Xmlns:                       redshiftXMLNS,
		DescribeLoggingStatusResult: describeLoggingStatusResult{LoggingEnabled: false},
	}
}

type redshiftTaggedResource struct {
	Tag          svcTags.KV `xml:"Tag"`
	ResourceName string     `xml:"ResourceName"`
	ResourceType string     `xml:"ResourceType"`
}

func (h *Handler) describeTagsResponse() any {
	allTags := h.Backend.DescribeTags()

	type describeTagsResult struct {
		XMLName         xml.Name                 `xml:"DescribeTagsResult"`
		Marker          string                   `xml:"Marker,omitempty"`
		TaggedResources []redshiftTaggedResource `xml:"TaggedResources>TaggedResource,omitempty"`
	}
	type response struct {
		XMLName            xml.Name           `xml:"DescribeTagsResponse"`
		Xmlns              string             `xml:"xmlns,attr"`
		DescribeTagsResult describeTagsResult `xml:"DescribeTagsResult"`
	}

	var resources []redshiftTaggedResource
	for clusterID, tags := range allTags {
		for k, v := range tags {
			resources = append(resources, redshiftTaggedResource{
				Tag:          svcTags.KV{Key: k, Value: v},
				ResourceName: clusterID,
				ResourceType: "cluster",
			})
		}
	}

	return &response{
		Xmlns: redshiftXMLNS,
		DescribeTagsResult: describeTagsResult{
			TaggedResources: resources,
		},
	}
}

func (h *Handler) handleCreateTags(vals url.Values) (any, error) {
	clusterID := vals.Get("ResourceName")
	tags := parseRedshiftTags(vals)

	if err := h.Backend.CreateTags(clusterID, tags); err != nil {
		return nil, err
	}

	type response struct {
		XMLName xml.Name `xml:"CreateTagsResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}

	return &response{Xmlns: redshiftXMLNS}, nil
}

func (h *Handler) handleDeleteTags(vals url.Values) (any, error) {
	clusterID := vals.Get("ResourceName")
	keys := parseRedshiftTagKeys(vals)

	if err := h.Backend.DeleteTags(clusterID, keys); err != nil {
		return nil, err
	}

	type response struct {
		XMLName xml.Name `xml:"DeleteTagsResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}

	return &response{Xmlns: redshiftXMLNS}, nil
}

// parseRedshiftTags extracts Tags.Tag.N.Key/Tags.Tag.N.Value from form values.
func parseRedshiftTags(vals url.Values) map[string]string {
	tags := make(map[string]string)

	for i := 1; ; i++ {
		prefix := fmt.Sprintf("Tags.Tag.%d.", i)
		key := vals.Get(prefix + "Key")

		if key == "" {
			return tags
		}

		tags[key] = vals.Get(prefix + "Value")
	}
}

// parseRedshiftTagKeys extracts TagKeys.TagKey.N from form values.
func parseRedshiftTagKeys(vals url.Values) []string {
	var keys []string

	for i := 1; ; i++ {
		key := vals.Get(fmt.Sprintf("TagKeys.TagKey.%d", i))
		if key == "" {
			return keys
		}

		keys = append(keys, key)
	}
}
