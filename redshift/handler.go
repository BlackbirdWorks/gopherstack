package redshift

import (
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	redshiftVersion       = "2012-12-01"
	redshiftXMLNS         = "http://redshift.amazonaws.com/doc/2012-12-01/"
	redshiftMatchPriority = 83
)

// Handler is the Echo HTTP handler for Redshift operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
}

// NewHandler creates a new Redshift handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
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
		body, err := httputil.ReadBody(r)
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
func (h *Handler) MatchPriority() int { return redshiftMatchPriority }

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

		var (
			resp  any
			opErr error
		)

		switch action {
		case "CreateCluster":
			resp, opErr = h.handleCreateCluster(vals)
		case "DeleteCluster":
			resp, opErr = h.handleDeleteCluster(vals)
		case "DescribeClusters":
			resp, opErr = h.handleDescribeClusters(vals)
		case "DescribeLoggingStatus":
			return h.writeXMLResponse(c, http.StatusOK, h.loggingStatusResponse())
		case "DescribeTags":
			return h.writeXMLResponse(c, http.StatusOK, h.describeTagsResponse())
		case "CreateTags", "DeleteTags":
			return h.writeXMLResponse(c, http.StatusOK, h.emptyTagsResponse(action))
		default:
			return h.writeError(c, http.StatusBadRequest, "InvalidAction",
				fmt.Sprintf("%s is not a valid Redshift action", action))
		}

		if opErr != nil {
			return h.handleOpError(c, action, opErr)
		}

		xmlBytes, err := marshalXML(resp)
		if err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "internal server error")
		}

		return c.Blob(http.StatusOK, "text/xml", xmlBytes)
	}
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
		ClusterIdentifier:         c.ClusterIdentifier,
		NodeType:                  c.NodeType,
		Endpoint:                  c.Endpoint,
		ClusterStatus:             c.Status,
		ClusterAvailabilityStatus: "Available",
		DBName:                    c.DBName,
		MasterUsername:            c.MasterUsername,
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
		code = "InvalidParameterValue"
	default:
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		h.Logger.Error("Redshift internal error", "error", opErr, "action", action)
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
	ClusterIdentifier         string `xml:"ClusterIdentifier"`
	NodeType                  string `xml:"NodeType"`
	Endpoint                  string `xml:"Endpoint>Address"`
	ClusterStatus             string `xml:"ClusterStatus"`
	ClusterAvailabilityStatus string `xml:"ClusterAvailabilityStatus"`
	DBName                    string `xml:"DBName"`
	MasterUsername            string `xml:"MasterUsername"`
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

func (h *Handler) describeTagsResponse() any {
	type describeTagsResult struct {
		XMLName xml.Name `xml:"DescribeTagsResult"`
		Marker  string   `xml:"Marker,omitempty"`
	}
	type response struct {
		XMLName            xml.Name           `xml:"DescribeTagsResponse"`
		Xmlns              string             `xml:"xmlns,attr"`
		DescribeTagsResult describeTagsResult `xml:"DescribeTagsResult"`
	}

	return &response{
		Xmlns:              redshiftXMLNS,
		DescribeTagsResult: describeTagsResult{},
	}
}

func (h *Handler) emptyTagsResponse(action string) any {
	type response struct {
		XMLName xml.Name
		Xmlns   string `xml:"xmlns,attr"`
	}

	return &response{
		XMLName: xml.Name{Local: action + "Response"},
		Xmlns:   redshiftXMLNS,
	}
}
