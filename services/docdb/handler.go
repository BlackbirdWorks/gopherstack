package docdb

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	docdbVersion = "2014-10-31"
	docdbXMLNS   = "http://rds.amazonaws.com/doc/2014-10-31/"
	stringTrue   = "true"
)

// Handler is the Echo HTTP handler for DocDB operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new DocDB handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears all backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns the service name.
func (h *Handler) Name() string { return "DocDB" }

// GetSupportedOperations returns supported DocDB operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateDBCluster",
		"DescribeDBClusters",
		"DeleteDBCluster",
		"ModifyDBCluster",
		"StopDBCluster",
		"StartDBCluster",
		"FailoverDBCluster",
		"CreateDBInstance",
		"DescribeDBInstances",
		"DeleteDBInstance",
		"ModifyDBInstance",
		"RebootDBInstance",
		"CreateDBSubnetGroup",
		"DescribeDBSubnetGroups",
		"DeleteDBSubnetGroup",
		"CreateDBClusterParameterGroup",
		"DescribeDBClusterParameterGroups",
		"DeleteDBClusterParameterGroup",
		"ModifyDBClusterParameterGroup",
		"CreateDBClusterSnapshot",
		"DescribeDBClusterSnapshots",
		"DeleteDBClusterSnapshot",
		"ListTagsForResource",
		"AddTagsToResource",
		"RemoveTagsFromResource",
		"DescribeDBEngineVersions",
		"DescribeOrderableDBInstanceOptions",
		"DescribeGlobalClusters",
		"AddSourceIdentifierToSubscription",
		"ApplyPendingMaintenanceAction",
		"CopyDBClusterParameterGroup",
		"CopyDBClusterSnapshot",
		"CreateEventSubscription",
		"CreateGlobalCluster",
		"DeleteEventSubscription",
		"DeleteGlobalCluster",
		"DescribeCertificates",
		"DescribeDBClusterParameters",
		"DescribeDBClusterSnapshotAttributes",
		"DescribeEngineDefaultClusterParameters",
		"DescribeEventCategories",
		"DescribeEventSubscriptions",
		"DescribeEvents",
		"DescribePendingMaintenanceActions",
		"FailoverGlobalCluster",
		"ModifyDBClusterSnapshotAttribute",
		"ModifyDBSubnetGroup",
		"ModifyEventSubscription",
		"ModifyGlobalCluster",
		"RemoveFromGlobalCluster",
		"RemoveSourceIdentifierFromSubscription",
		"ResetDBClusterParameterGroup",
		"RestoreDBClusterFromSnapshot",
		"RestoreDBClusterToPointInTime",
		"SwitchoverGlobalCluster",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "docdb" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this DocDB instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// regionFromRequest extracts the SigV4 region from an incoming request.
func (h *Handler) regionFromRequest(c *echo.Context) string {
	return httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
}

// RouteMatcher returns a function that matches DocDB requests.
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
		// Checks both User-Agent (native SDKs) and X-Amz-User-Agent (the AWS
		// SDK for JavaScript in a browser, which cannot set User-Agent
		// itself) -- see service.MatchesUserAgentMarker.
		if !service.MatchesUserAgentMarker(r.Header, "api/docdb") {
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

		return vals.Get("Version") == docdbVersion
	}
}

// MatchPriority returns the routing priority for DocDB (higher than RDS to intercept DocDB requests first).
func (h *Handler) MatchPriority() int { return service.PriorityFormDocDB }

// ExtractOperation extracts the DocDB action from the request.
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

// ExtractResource returns the DB cluster identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	return r.Form.Get("DBClusterIdentifier")
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
		ctx := context.WithValue(r.Context(), regionContextKey{}, h.regionFromRequest(c))
		resp, opErr := h.dispatch(ctx, action, vals)
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

func (h *Handler) dispatch(ctx context.Context, action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBCluster":
		return h.handleCreateDBCluster(ctx, vals)
	case "DescribeDBClusters":
		return h.handleDescribeDBClusters(ctx, vals)
	case "DeleteDBCluster":
		return h.handleDeleteDBCluster(ctx, vals)
	case "ModifyDBCluster":
		return h.handleModifyDBCluster(ctx, vals)
	case "StopDBCluster":
		return h.handleStopDBCluster(ctx, vals)
	case "StartDBCluster":
		return h.handleStartDBCluster(ctx, vals)
	case "FailoverDBCluster":
		return h.handleFailoverDBCluster(ctx, vals)
	case "CreateDBInstance":
		return h.handleCreateDBInstance(ctx, vals)
	case "DescribeDBInstances":
		return h.handleDescribeDBInstances(ctx, vals)
	case "DeleteDBInstance":
		return h.handleDeleteDBInstance(ctx, vals)
	case "ModifyDBInstance":
		return h.handleModifyDBInstance(ctx, vals)
	case "RebootDBInstance":
		return h.handleRebootDBInstance(ctx, vals)
	default:
		return h.dispatchExtended(ctx, action, vals)
	}
}

func (h *Handler) dispatchExtended(ctx context.Context, action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBSubnetGroup":
		return h.handleCreateDBSubnetGroup(ctx, vals)
	case "DescribeDBSubnetGroups":
		return h.handleDescribeDBSubnetGroups(ctx, vals)
	case "DeleteDBSubnetGroup":
		return h.handleDeleteDBSubnetGroup(ctx, vals)
	case "CreateDBClusterParameterGroup":
		return h.handleCreateDBClusterParameterGroup(ctx, vals)
	case "DescribeDBClusterParameterGroups":
		return h.handleDescribeDBClusterParameterGroups(ctx, vals)
	case "DeleteDBClusterParameterGroup":
		return h.handleDeleteDBClusterParameterGroup(ctx, vals)
	case "ModifyDBClusterParameterGroup":
		return h.handleModifyDBClusterParameterGroup(ctx, vals)
	default:
		return h.dispatchExtended2(ctx, action, vals)
	}
}

func (h *Handler) dispatchExtended2(ctx context.Context, action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBClusterSnapshot":
		return h.handleCreateDBClusterSnapshot(ctx, vals)
	case "DescribeDBClusterSnapshots":
		return h.handleDescribeDBClusterSnapshots(ctx, vals)
	case "DeleteDBClusterSnapshot":
		return h.handleDeleteDBClusterSnapshot(ctx, vals)
	case "ListTagsForResource":
		return h.handleListTagsForResource(ctx, vals)
	case "AddTagsToResource":
		return h.handleAddTagsToResource(ctx, vals)
	case "RemoveTagsFromResource":
		return h.handleRemoveTagsFromResource(ctx, vals)
	case "DescribeDBEngineVersions":
		return h.handleDescribeDBEngineVersions(ctx, vals)
	case "DescribeOrderableDBInstanceOptions":
		return h.handleDescribeOrderableDBInstanceOptions(vals)
	case "DescribeGlobalClusters":
		return h.handleDescribeGlobalClusters(ctx, vals)
	default:
		return h.dispatchExtended3(ctx, action, vals)
	}
}

func (h *Handler) dispatchExtended3(ctx context.Context, action string, vals url.Values) (any, error) {
	switch action {
	case "AddSourceIdentifierToSubscription":
		return h.handleAddSourceIdentifierToSubscription(ctx, vals)
	case "ApplyPendingMaintenanceAction":
		return h.handleApplyPendingMaintenanceAction(ctx, vals)
	case "CopyDBClusterParameterGroup":
		return h.handleCopyDBClusterParameterGroup(ctx, vals)
	case "CopyDBClusterSnapshot":
		return h.handleCopyDBClusterSnapshot(ctx, vals)
	case "CreateEventSubscription":
		return h.handleCreateEventSubscription(ctx, vals)
	case "CreateGlobalCluster":
		return h.handleCreateGlobalCluster(ctx, vals)
	case "DeleteEventSubscription":
		return h.handleDeleteEventSubscription(ctx, vals)
	case "DeleteGlobalCluster":
		return h.handleDeleteGlobalCluster(ctx, vals)
	case "DescribeCertificates":
		return h.handleDescribeCertificates(ctx, vals)
	case "DescribeDBClusterParameters":
		return h.handleDescribeDBClusterParameters(ctx, vals)
	default:
		return h.dispatchExtended4(ctx, action, vals)
	}
}

func (h *Handler) dispatchExtended4(ctx context.Context, action string, vals url.Values) (any, error) {
	switch action {
	case "DescribeDBClusterSnapshotAttributes":
		return h.handleDescribeDBClusterSnapshotAttributes(ctx, vals)
	case "DescribeEngineDefaultClusterParameters":
		return h.handleDescribeEngineDefaultClusterParameters(ctx, vals)
	case "DescribeEventCategories":
		return h.handleDescribeEventCategories(ctx, vals)
	case "DescribeEventSubscriptions":
		return h.handleDescribeEventSubscriptions(ctx, vals)
	case "DescribeEvents":
		return h.handleDescribeEvents(ctx, vals)
	case "DescribePendingMaintenanceActions":
		return h.handleDescribePendingMaintenanceActions(ctx, vals)
	case "FailoverGlobalCluster":
		return h.handleFailoverGlobalCluster(ctx, vals)
	case "ModifyDBClusterSnapshotAttribute":
		return h.handleModifyDBClusterSnapshotAttribute(ctx, vals)
	default:
		return h.dispatchExtended5(ctx, action, vals)
	}
}

func (h *Handler) dispatchExtended5(ctx context.Context, action string, vals url.Values) (any, error) {
	switch action {
	case "ModifyDBSubnetGroup":
		return h.handleModifyDBSubnetGroup(ctx, vals)
	case "ModifyEventSubscription":
		return h.handleModifyEventSubscription(ctx, vals)
	case "ModifyGlobalCluster":
		return h.handleModifyGlobalCluster(ctx, vals)
	case "RemoveFromGlobalCluster":
		return h.handleRemoveFromGlobalCluster(ctx, vals)
	case "RemoveSourceIdentifierFromSubscription":
		return h.handleRemoveSourceIdentifierFromSubscription(ctx, vals)
	case "ResetDBClusterParameterGroup":
		return h.handleResetDBClusterParameterGroup(ctx, vals)
	case "RestoreDBClusterFromSnapshot":
		return h.handleRestoreDBClusterFromSnapshot(ctx, vals)
	case "RestoreDBClusterToPointInTime":
		return h.handleRestoreDBClusterToPointInTime(ctx, vals)
	case "SwitchoverGlobalCluster":
		return h.handleSwitchoverGlobalCluster(ctx, vals)
	default:
		return nil, fmt.Errorf("%w: %s is not a valid DocDB action", ErrUnknownAction, action)
	}
}

func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	statusCode := http.StatusBadRequest
	code := docdbErrorCode(opErr)
	if code == "" {
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		logger.Load(c.Request().Context()).Error("DocDB internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, statusCode, code, opErr.Error())
}

func docdbErrorCode(opErr error) string {
	sentinels := []error{
		ErrClusterNotFound, ErrClusterAlreadyExists,
		ErrInstanceNotFound, ErrInstanceAlreadyExists,
		ErrSubnetGroupNotFound, ErrSubnetGroupAlreadyExists, ErrSubnetGroupInUse,
		ErrClusterParameterGroupNotFound, ErrClusterParameterGroupAlreadyExists, ErrParameterGroupInUse,
		ErrClusterSnapshotNotFound, ErrClusterSnapshotAlreadyExists,
		ErrEventSubscriptionNotFound, ErrEventSubscriptionAlreadyExists,
		ErrGlobalClusterNotFound, ErrGlobalClusterAlreadyExists,
		ErrInvalidParameter, ErrInvalidClusterState, ErrInvalidGlobalClusterState, ErrUnknownAction,
	}
	for _, s := range sentinels {
		if errors.Is(opErr, s) {
			return s.Error()
		}
	}

	return ""
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &docdbErrorResponse{
		Xmlns: docdbXMLNS,
		Error: docdbError{Code: code, Message: message, Type: "Sender"},
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

// parseBoolParam parses an optional boolean query parameter.
// Returns nil if the key is absent, otherwise returns a pointer to the parsed value.
func parseBoolParam(vals url.Values, key string) *bool {
	s := vals.Get(key)
	if s == "" {
		return nil
	}
	v := s == stringTrue

	return &v
}

type docdbError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

type docdbErrorResponse struct {
	XMLName xml.Name   `xml:"ErrorResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Error   docdbError `xml:"Error"`
}

func tagsToMap(tags []Tag) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// parseTags parses Tags.Tag.N.Key/Value form values and returns a map.
func parseTags(vals url.Values) map[string]string {
	return tagsToMap(parseTagEntries(vals))
}

const defaultDocDBMaxRecords = 100

// applyDocDBMarker applies Marker/MaxRecords-based pagination to a slice.
func applyDocDBMarker[T any](items []T, marker, maxRecordsStr string) ([]T, string) {
	start := 0
	if marker != "" {
		idx, err := strconv.Atoi(marker)
		if err == nil && idx > 0 {
			start = idx
		}
	}

	if start >= len(items) {
		return []T{}, ""
	}

	items = items[start:]

	limit := defaultDocDBMaxRecords
	if maxRecordsStr != "" {
		if n, err := strconv.Atoi(maxRecordsStr); err == nil && n > 0 {
			limit = n
		}
	}

	if len(items) <= limit {
		return items, ""
	}

	return items[:limit], strconv.Itoa(start + limit)
}
