package quicksight

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	quicksightServiceName   = "QuickSight"
	quicksightSigningName   = "quicksight"
	quicksightPathPrefix    = "/accounts/"
	quicksightTagPrefix     = "/resources/"
	quicksightMatchPriority = service.PriorityPathVersioned + 1

	opUnknown = "Unknown"

	// path segment indices
	segAccountID   = 1
	segResource    = 2
	segResID       = 3
	segSubRes      = 4
	segSubResID    = 5
	segSubSubRes   = 6
	segSubSubResID = 7

	// JSON response keys
	keyRequestID = "RequestId"
	keyStatus    = "Status"
	keyNextToken = "NextToken"

	// request ID placeholder
	reqIDPlaceholder = "request-id"

	// path segment names
	pathSegAccounts      = "accounts"
	pathSegNamespaces    = "namespaces"
	pathSegGroups        = "groups"
	pathSegMembers       = "members"
	pathSegUsers         = "users"
	pathSegDataSources   = "data-sources"
	pathSegDataSets      = "data-sets"
	pathSegIngestions    = "ingestions"
	pathSegDashboards    = "dashboards"
	pathSegAnalyses      = "analyses"
	pathSegVersions      = "versions"
	pathSegSearch        = "search"
	pathSegRestore       = "restore"
	pathSegResources     = "resources"
	pathSegTagsSuffix    = "tags"
	pathSegGroupsSearch  = "groups-search"
	pathSegUserPrincipals = "user-principals"

	// time format
	timeFormat = "2006-01-02T15:04:05Z"
)

// Handler is the Echo HTTP handler for QuickSight operations.
type Handler struct {
	Backend   StorageBackend
	accountID string
	region    string
}

// NewHandler creates a new QuickSight handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{
		Backend:   b,
		accountID: b.AccountID(),
		region:    b.Region(),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return quicksightServiceName }

// Reset clears the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// MatchPriority returns routing priority (above SecurityHub which also uses /accounts/).
func (h *Handler) MatchPriority() int { return quicksightMatchPriority }

// RouteMatcher returns a function that matches QuickSight requests.
// QuickSight shares /accounts/ prefix with SecurityHub; we disambiguate via
// the Authorization header signing service name ("quicksight").
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
		if strings.HasPrefix(path, quicksightPathPrefix) || strings.HasPrefix(path, quicksightTagPrefix) {
			return isQuickSightRequest(c)
		}

		return false
	}
}

func isQuickSightRequest(c *echo.Context) bool {
	auth := c.Request().Header.Get("Authorization")

	return strings.Contains(auth, "/"+quicksightSigningName+"/")
}

// ExtractOperation extracts the QuickSight operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := classifyRequest(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the primary resource ID from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := classifyRequest(c.Request().Method, c.Request().URL.Path)

	return resource
}

// GetSupportedOperations returns the list of implemented QuickSight operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateNamespace",
		"DescribeNamespace",
		"DeleteNamespace",
		"ListNamespaces",
		"CreateGroup",
		"DescribeGroup",
		"UpdateGroup",
		"DeleteGroup",
		"ListGroups",
		"SearchGroups",
		"CreateGroupMembership",
		"DescribeGroupMembership",
		"DeleteGroupMembership",
		"ListGroupMemberships",
		"RegisterUser",
		"DescribeUser",
		"UpdateUser",
		"DeleteUser",
		"DeleteUserByPrincipalId",
		"ListUsers",
		"ListUserGroups",
		"CreateDataSource",
		"DescribeDataSource",
		"UpdateDataSource",
		"DeleteDataSource",
		"ListDataSources",
		"CreateDataSet",
		"DescribeDataSet",
		"UpdateDataSet",
		"DeleteDataSet",
		"ListDataSets",
		"CreateIngestion",
		"DescribeIngestion",
		"CancelIngestion",
		"ListIngestions",
		"CreateDashboard",
		"DescribeDashboard",
		"UpdateDashboard",
		"DeleteDashboard",
		"ListDashboards",
		"ListDashboardVersions",
		"CreateAnalysis",
		"DescribeAnalysis",
		"UpdateAnalysis",
		"DeleteAnalysis",
		"ListAnalyses",
		"RestoreAnalysis",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.dispatch(c)
	}
}

// Register registers routes on the Echo server.
func (h *Handler) Register(_ context.Context, _ *echo.Echo) error { return nil }

// ChaosServiceName returns the lowercase AWS service name.
func (h *Handler) ChaosServiceName() string { return quicksightSigningName }

// ChaosOperations returns operations eligible for fault injection.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler serves.
func (h *Handler) ChaosRegions() []string { return []string{h.region} }

func (h *Handler) dispatch(c *echo.Context) error {
	op, _ := classifyRequest(c.Request().Method, c.Request().URL.Path)

	var err error
	switch op {
	// Namespaces
	case "CreateNamespace":
		err = h.handleCreateNamespace(c)
	case "DescribeNamespace":
		err = h.handleDescribeNamespace(c)
	case "DeleteNamespace":
		err = h.handleDeleteNamespace(c)
	case "ListNamespaces":
		err = h.handleListNamespaces(c)

	// Groups
	case "CreateGroup":
		err = h.handleCreateGroup(c)
	case "DescribeGroup":
		err = h.handleDescribeGroup(c)
	case "UpdateGroup":
		err = h.handleUpdateGroup(c)
	case "DeleteGroup":
		err = h.handleDeleteGroup(c)
	case "ListGroups":
		err = h.handleListGroups(c)
	case "SearchGroups":
		err = h.handleSearchGroups(c)

	// Group Memberships
	case "CreateGroupMembership":
		err = h.handleCreateGroupMembership(c)
	case "DescribeGroupMembership":
		err = h.handleDescribeGroupMembership(c)
	case "DeleteGroupMembership":
		err = h.handleDeleteGroupMembership(c)
	case "ListGroupMemberships":
		err = h.handleListGroupMemberships(c)

	// Users
	case "RegisterUser":
		err = h.handleRegisterUser(c)
	case "DescribeUser":
		err = h.handleDescribeUser(c)
	case "UpdateUser":
		err = h.handleUpdateUser(c)
	case "DeleteUser":
		err = h.handleDeleteUser(c)
	case "DeleteUserByPrincipalId":
		err = h.handleDeleteUserByPrincipalID(c)
	case "ListUsers":
		err = h.handleListUsers(c)
	case "ListUserGroups":
		err = h.handleListUserGroups(c)

	// DataSources
	case "CreateDataSource":
		err = h.handleCreateDataSource(c)
	case "DescribeDataSource":
		err = h.handleDescribeDataSource(c)
	case "UpdateDataSource":
		err = h.handleUpdateDataSource(c)
	case "DeleteDataSource":
		err = h.handleDeleteDataSource(c)
	case "ListDataSources":
		err = h.handleListDataSources(c)

	// DataSets
	case "CreateDataSet":
		err = h.handleCreateDataSet(c)
	case "DescribeDataSet":
		err = h.handleDescribeDataSet(c)
	case "UpdateDataSet":
		err = h.handleUpdateDataSet(c)
	case "DeleteDataSet":
		err = h.handleDeleteDataSet(c)
	case "ListDataSets":
		err = h.handleListDataSets(c)

	// Ingestions
	case "CreateIngestion":
		err = h.handleCreateIngestion(c)
	case "DescribeIngestion":
		err = h.handleDescribeIngestion(c)
	case "CancelIngestion":
		err = h.handleCancelIngestion(c)
	case "ListIngestions":
		err = h.handleListIngestions(c)

	// Dashboards
	case "CreateDashboard":
		err = h.handleCreateDashboard(c)
	case "DescribeDashboard":
		err = h.handleDescribeDashboard(c)
	case "UpdateDashboard":
		err = h.handleUpdateDashboard(c)
	case "DeleteDashboard":
		err = h.handleDeleteDashboard(c)
	case "ListDashboards":
		err = h.handleListDashboards(c)
	case "ListDashboardVersions":
		err = h.handleListDashboardVersions(c)

	// Analyses
	case "CreateAnalysis":
		err = h.handleCreateAnalysis(c)
	case "DescribeAnalysis":
		err = h.handleDescribeAnalysis(c)
	case "UpdateAnalysis":
		err = h.handleUpdateAnalysis(c)
	case "DeleteAnalysis":
		err = h.handleDeleteAnalysis(c)
	case "ListAnalyses":
		err = h.handleListAnalyses(c)
	case "RestoreAnalysis":
		err = h.handleRestoreAnalysis(c)

	// Tags
	case "TagResource":
		err = h.handleTagResource(c)
	case "UntagResource":
		err = h.handleUntagResource(c)
	case "ListTagsForResource":
		err = h.handleListTagsForResource(c)

	default:
		return writeError(
			c,
			http.StatusNotImplemented,
			"UnsupportedOperationException",
			fmt.Sprintf("operation %q not implemented", op),
		)
	}

	return err
}

// ---- path classification ----

// pathSegs splits a URL path and returns non-empty segments.
func pathSegs(path string) []string {
	var segs []string
	for s := range strings.SplitSeq(path, "/") {
		if s != "" {
			segs = append(segs, s)
		}
	}

	return segs
}

// seg returns segment at index i (URL-decoded), or "" if out of range.
func seg(segs []string, i int) string {
	if i >= len(segs) {
		return ""
	}
	v, err := url.PathUnescape(segs[i])
	if err != nil {
		return segs[i]
	}

	return v
}

//nolint:cyclop // path router requires many branches
func classifyRequest(method, path string) (string, string) {
	segs := pathSegs(path)
	n := len(segs)

	// /resources/{arn}/tags — tag operations
	if n >= 3 && segs[0] == pathSegResources && segs[n-1] == pathSegTagsSuffix {
		arn := strings.Join(segs[1:n-1], "/")
		switch method {
		case http.MethodPost:
			return "TagResource", arn
		case http.MethodGet:
			return "ListTagsForResource", arn
		case http.MethodDelete:
			return "UntagResource", arn
		}

		return opUnknown, ""
	}

	// All remaining paths start with /accounts/{accountId}
	if n < 2 || segs[0] != pathSegAccounts {
		return opUnknown, ""
	}

	// segs[0]=accounts, segs[1]=accountId, segs[2]=resource-type, ...
	if n == 2 {
		// POST /accounts/{accountId} → CreateNamespace
		if method == http.MethodPost {
			return "CreateNamespace", seg(segs, segAccountID)
		}

		return opUnknown, ""
	}

	resourceType := seg(segs, segResource)

	switch resourceType {
	case pathSegNamespaces:
		return classifyNamespacePaths(method, segs, n)
	case pathSegDataSources:
		return classifyDataSourcePaths(method, segs, n)
	case pathSegDataSets:
		return classifyDataSetPaths(method, segs, n)
	case pathSegDashboards:
		return classifyDashboardPaths(method, segs, n)
	case pathSegAnalyses:
		return classifyAnalysisPaths(method, segs, n)
	case pathSegSearch:
		return classifySearchPaths(method, segs, n)
	case pathSegRestore:
		return classifyRestorePaths(method, segs, n)
	}

	return opUnknown, ""
}

func classifyNamespacePaths(method string, segs []string, n int) (string, string) {
	switch n {
	case 3:
		// /accounts/{id}/namespaces
		if method == http.MethodGet {
			return "ListNamespaces", seg(segs, segAccountID)
		}
	case 4:
		// /accounts/{id}/namespaces/{ns}
		ns := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return "DescribeNamespace", ns
		case http.MethodDelete:
			return "DeleteNamespace", ns
		}
	case 5:
		// /accounts/{id}/namespaces/{ns}/groups or users
		ns := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		switch sub {
		case pathSegGroups:
			switch method {
			case http.MethodPost:
				return "CreateGroup", ns
			case http.MethodGet:
				return "ListGroups", ns
			}
		case pathSegUsers:
			switch method {
			case http.MethodPost:
				return "RegisterUser", ns
			case http.MethodGet:
				return "ListUsers", ns
			}
		case pathSegGroupsSearch:
			if method == http.MethodPost {
				return "SearchGroups", ns
			}
		}
	case 6:
		// /accounts/{id}/namespaces/{ns}/groups/{gn}
		// or /accounts/{id}/namespaces/{ns}/users/{un}
		// or /accounts/{id}/namespaces/{ns}/user-principals/{pid}
		ns := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		id := seg(segs, segSubResID)
		switch sub {
		case pathSegGroups:
			switch method {
			case http.MethodGet:
				return "DescribeGroup", id
			case http.MethodPut:
				return "UpdateGroup", id
			case http.MethodDelete:
				return "DeleteGroup", id
			}
		case pathSegUsers:
			switch method {
			case http.MethodGet:
				return "DescribeUser", id
			case http.MethodPut:
				return "UpdateUser", id
			case http.MethodDelete:
				return "DeleteUser", id
			}
		case pathSegUserPrincipals:
			if method == http.MethodDelete {
				return "DeleteUserByPrincipalId", ns
			}
		}
	case 7:
		// /accounts/{id}/namespaces/{ns}/groups/{gn}/members
		// /accounts/{id}/namespaces/{ns}/users/{un}/groups
		sub := seg(segs, segSubRes)
		id := seg(segs, segSubResID)
		tail := seg(segs, segSubSubRes)
		switch {
		case sub == pathSegGroups && tail == pathSegMembers:
			if method == http.MethodGet {
				return "ListGroupMemberships", id
			}
		case sub == pathSegUsers && tail == pathSegGroups:
			if method == http.MethodGet {
				return "ListUserGroups", id
			}
		}
	case 8:
		// /accounts/{id}/namespaces/{ns}/groups/{gn}/members/{mn}
		sub := seg(segs, segSubRes)
		tail := seg(segs, segSubSubRes)
		if sub == pathSegGroups && tail == pathSegMembers {
			switch method {
			case http.MethodPut:
				return "CreateGroupMembership", seg(segs, segSubResID)
			case http.MethodGet:
				return "DescribeGroupMembership", seg(segs, segSubResID)
			case http.MethodDelete:
				return "DeleteGroupMembership", seg(segs, segSubResID)
			}
		}
	}

	return opUnknown, ""
}

func classifyDataSourcePaths(method string, segs []string, n int) (string, string) {
	switch n {
	case 3:
		switch method {
		case http.MethodPost:
			return "CreateDataSource", seg(segs, segAccountID)
		case http.MethodGet:
			return "ListDataSources", seg(segs, segAccountID)
		}
	case 4:
		id := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return "DescribeDataSource", id
		case http.MethodPut:
			return "UpdateDataSource", id
		case http.MethodDelete:
			return "DeleteDataSource", id
		}
	}

	return opUnknown, ""
}

func classifyIngestionPaths(method string, segs []string, n int) (string, string) {
	switch n {
	case 5:
		if method == http.MethodGet {
			return "ListIngestions", seg(segs, segResID)
		}
	case 6:
		ingID := seg(segs, segSubResID)
		switch method {
		case http.MethodPut:
			return "CreateIngestion", ingID
		case http.MethodGet:
			return "DescribeIngestion", ingID
		case http.MethodDelete:
			return "CancelIngestion", ingID
		}
	}

	return opUnknown, ""
}

func classifyDataSetPaths(method string, segs []string, n int) (string, string) {
	switch n {
	case 3:
		switch method {
		case http.MethodPost:
			return "CreateDataSet", seg(segs, segAccountID)
		case http.MethodGet:
			return "ListDataSets", seg(segs, segAccountID)
		}
	case 4:
		id := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return "DescribeDataSet", id
		case http.MethodPut:
			return "UpdateDataSet", id
		case http.MethodDelete:
			return "DeleteDataSet", id
		}
	case 5, 6:
		if seg(segs, segSubRes) == pathSegIngestions {
			return classifyIngestionPaths(method, segs, n)
		}
	}

	return opUnknown, ""
}

func classifyDashboardPaths(method string, segs []string, n int) (string, string) {
	switch n {
	case 3:
		if method == http.MethodGet {
			return "ListDashboards", seg(segs, segAccountID)
		}
	case 4:
		id := seg(segs, segResID)
		switch method {
		case http.MethodPost:
			return "CreateDashboard", id
		case http.MethodGet:
			return "DescribeDashboard", id
		case http.MethodPut:
			return "UpdateDashboard", id
		case http.MethodDelete:
			return "DeleteDashboard", id
		}
	case 5:
		if seg(segs, segSubRes) == pathSegVersions && method == http.MethodGet {
			return "ListDashboardVersions", seg(segs, segResID)
		}
	}

	return opUnknown, ""
}

func classifyAnalysisPaths(method string, segs []string, n int) (string, string) {
	switch n {
	case 3:
		if method == http.MethodGet {
			return "ListAnalyses", seg(segs, segAccountID)
		}
	case 4:
		id := seg(segs, segResID)
		switch method {
		case http.MethodPost:
			return "CreateAnalysis", id
		case http.MethodGet:
			return "DescribeAnalysis", id
		case http.MethodPut:
			return "UpdateAnalysis", id
		case http.MethodDelete:
			return "DeleteAnalysis", id
		}
	}

	return opUnknown, ""
}

func classifySearchPaths(method string, segs []string, n int) (string, string) {
	if method != http.MethodPost || n < 4 {
		return opUnknown, ""
	}

	switch seg(segs, segResID) {
	case pathSegAnalyses:
		return "SearchAnalyses", ""
	case pathSegDashboards:
		return "SearchDashboards", ""
	case pathSegDataSets:
		return "SearchDataSets", ""
	case pathSegDataSources:
		return "SearchDataSources", ""
	case "folders":
		return "SearchFolders", ""
	}

	return opUnknown, ""
}

func classifyRestorePaths(method string, segs []string, n int) (string, string) {
	// POST /accounts/{id}/restore/analyses/{analysisId}
	if method == http.MethodPost && n == 5 && seg(segs, segResID) == pathSegAnalyses {
		return "RestoreAnalysis", seg(segs, segSubRes)
	}

	return opUnknown, ""
}

// ---- request helpers ----

func readBody(c *echo.Context) (map[string]any, error) {
	var body map[string]any
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil {
		return nil, err
	}

	return body, nil
}

func strField(body map[string]any, key string) string {
	v, _ := body[key].(string)

	return v
}

func intField(body map[string]any, key string) int32 {
	switch v := body[key].(type) {
	case float64:
		return int32(v) //nolint:gosec // JSON numbers are bounded by the API
	case int:
		return int32(v) //nolint:gosec // JSON numbers are bounded by the API
	}

	return 0
}

func tagsFromBody(body map[string]any) map[string]string {
	raw, _ := body["Tags"].([]any)
	if len(raw) == 0 {
		return nil
	}

	tags := make(map[string]string, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			k, _ := m["Key"].(string)
			v, _ := m["Value"].(string)
			if k != "" {
				tags[k] = v
			}
		}
	}

	return tags
}

func queryParam(c *echo.Context, key string) string {
	return c.Request().URL.Query().Get(key)
}

func maxResultsParam(c *echo.Context) int32 {
	s := queryParam(c, "max-results")
	if s == "" {
		s = queryParam(c, "maxResults")
	}
	if s == "" {
		return 0
	}
	n, _ := strconv.ParseInt(s, 10, 32)

	return int32(n) //nolint:gosec // bitSize 32 ensures safe conversion
}

func nextTokenParam(c *echo.Context) string {
	t := queryParam(c, "next-token")
	if t == "" {
		t = queryParam(c, "nextToken")
	}

	return t
}

func writeJSON(c *echo.Context, code int, body any) error {
	c.Response().Header().Set("Content-Type", "application/json")
	c.Response().WriteHeader(code)

	return json.NewEncoder(c.Response()).Encode(body)
}

func writeError(c *echo.Context, code int, errCode, msg string) error {
	return writeJSON(c, code, map[string]any{
		"Code":    errCode,
		"Message": msg,
	})
}

func httpErr(c *echo.Context, err error) error {
	log := logger.Load(c.Request().Context())

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return writeError(c, http.StatusConflict, "ConflictException", err.Error())
	case errors.Is(err, awserr.ErrInvalidParameter):
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	log.Error("quicksight: unexpected error", "error", err)

	return writeError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
}

func pathSegsFromCtx(c *echo.Context) []string {
	return pathSegs(c.Request().URL.Path)
}

// ---- Namespace handlers ----

func (h *Handler) handleCreateNamespace(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	namespace := strField(body, "Namespace")
	capacityRegion := strField(body, "CapacityRegion")

	ns, err := h.Backend.CreateNamespace(accountID, namespace, capacityRegion)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Arn":            ns.Arn,
		"CapacityRegion": ns.CapacityRegion,
		"CreationStatus": ns.CreationStatus,
		"IdentityStore":  ns.IdentityStore,
		"Name":           ns.Name,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	})
}

func (h *Handler) handleDescribeNamespace(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	ns, err := h.Backend.DescribeNamespace(accountID, namespace)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Namespace": map[string]any{
			"Arn":            ns.Arn,
			"CapacityRegion": ns.CapacityRegion,
			"CreationStatus": ns.CreationStatus,
			"IdentityStore":  ns.IdentityStore,
			"Name":           ns.Name,
		},
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteNamespace(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	if err := h.Backend.DeleteNamespace(accountID, namespace); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListNamespaces(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	namespaces, next, err := h.Backend.ListNamespaces(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(namespaces))
	for _, ns := range namespaces {
		items = append(items, map[string]any{
			"Arn":            ns.Arn,
			"CapacityRegion": ns.CapacityRegion,
			"CreationStatus": ns.CreationStatus,
			"IdentityStore":  ns.IdentityStore,
			"Name":           ns.Name,
		})
	}

	resp := map[string]any{
		"Namespaces": items,
		keyRequestID:  reqIDPlaceholder,
		keyStatus:     http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// ---- Group handlers ----

func (h *Handler) handleCreateGroup(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	groupName := strField(body, "GroupName")
	description := strField(body, "Description")

	g, err := h.Backend.CreateGroup(accountID, namespace, groupName, description)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Group":     groupToMap(g),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeGroup(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	groupName := seg(segs, segSubResID)

	g, err := h.Backend.DescribeGroup(accountID, namespace, groupName)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Group":     groupToMap(g),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateGroup(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	groupName := seg(segs, segSubResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	description := strField(body, "Description")

	g, err := h.Backend.UpdateGroup(accountID, namespace, groupName, description)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Group":     groupToMap(g),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteGroup(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	groupName := seg(segs, segSubResID)

	if err := h.Backend.DeleteGroup(accountID, namespace, groupName); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListGroups(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	groups, next, err := h.Backend.ListGroups(accountID, namespace, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		items = append(items, groupToMap(g))
	}

	resp := map[string]any{
		"GroupList": items,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleSearchGroups(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	body, _ := readBody(c)
	query := strField(body, "Query")
	maxResults := int32(0)
	if body != nil {
		maxResults = intField(body, "MaxResults")
	}
	nextToken := ""
	if body != nil {
		nextToken = strField(body, "NextToken")
	}

	groups, next, err := h.Backend.SearchGroups(accountID, namespace, query, maxResults, nextToken)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		items = append(items, groupToMap(g))
	}

	resp := map[string]any{
		"GroupList": items,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func groupToMap(g *Group) map[string]any {
	return map[string]any{
		"Arn":         g.Arn,
		"Description": g.Description,
		"GroupName":   g.GroupName,
		"Namespace":   g.Namespace,
		"PrincipalId": g.PrincipalID,
	}
}

// ---- Group Membership handlers ----

func (h *Handler) handleCreateGroupMembership(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	groupName := seg(segs, segSubResID)
	memberName := seg(segs, segSubSubResID)

	m, err := h.Backend.CreateGroupMembership(accountID, namespace, groupName, memberName)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"GroupMember": map[string]any{
			"Arn":        m.Arn,
			"MemberName": m.MemberName,
		},
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeGroupMembership(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	groupName := seg(segs, segSubResID)
	memberName := seg(segs, segSubSubResID)

	m, err := h.Backend.DescribeGroupMembership(accountID, namespace, groupName, memberName)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"GroupMember": map[string]any{
			"Arn":        m.Arn,
			"MemberName": m.MemberName,
		},
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteGroupMembership(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	groupName := seg(segs, segSubResID)
	memberName := seg(segs, segSubSubResID)

	if err := h.Backend.DeleteGroupMembership(accountID, namespace, groupName, memberName); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListGroupMemberships(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	groupName := seg(segs, segSubResID)

	members, next, err := h.Backend.ListGroupMemberships(
		accountID,
		namespace,
		groupName,
		maxResultsParam(c),
		nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(members))
	for _, m := range members {
		items = append(items, map[string]any{
			"Arn":        m.Arn,
			"MemberName": m.MemberName,
		})
	}

	resp := map[string]any{
		"GroupMemberList": items,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// ---- User handlers ----

func (h *Handler) handleRegisterUser(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	u, err := h.Backend.RegisterUser(
		accountID, namespace,
		strField(body, "UserName"),
		strField(body, "Email"),
		strField(body, "UserRole"),
		strField(body, "IdentityType"),
		strField(body, "SessionName"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"User":      userToMap(u),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeUser(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	userName := seg(segs, segSubResID)

	u, err := h.Backend.DescribeUser(accountID, namespace, userName)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"User":      userToMap(u),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateUser(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	userName := seg(segs, segSubResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	u, err := h.Backend.UpdateUser(accountID, namespace, userName, strField(body, "Email"), strField(body, "Role"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"User":      userToMap(u),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteUser(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	userName := seg(segs, segSubResID)

	if err := h.Backend.DeleteUser(accountID, namespace, userName); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteUserByPrincipalID(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	principalID := seg(segs, segSubResID)

	if err := h.Backend.DeleteUserByPrincipalID(accountID, namespace, principalID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListUsers(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	users, next, err := h.Backend.ListUsers(accountID, namespace, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(users))
	for _, u := range users {
		items = append(items, userToMap(u))
	}

	resp := map[string]any{
		"UserList":  items,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleListUserGroups(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	userName := seg(segs, segSubResID)

	groups, next, err := h.Backend.ListUserGroups(accountID, namespace, userName, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		items = append(items, groupToMap(g))
	}

	resp := map[string]any{
		"GroupList": items,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func userToMap(u *User) map[string]any {
	return map[string]any{
		"Active":       u.Active,
		"Arn":          u.Arn,
		"Email":        u.Email,
		"IdentityType": u.IdentityType,
		"Namespace":    u.Namespace,
		"PrincipalId":  u.PrincipalID,
		"Role":         u.Role,
		"UserName":     u.UserName,
	}
}

// ---- DataSource handlers ----

func (h *Handler) handleCreateDataSource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	ds, err := h.Backend.CreateDataSource(
		accountID,
		strField(body, "DataSourceId"),
		strField(body, "Name"),
		strField(body, "Type"),
		tagsFromBody(body),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusCreated, map[string]any{
		"Arn":            ds.Arn,
		"CreationStatus": ds.Status,
		"DataSourceId":   ds.DataSourceID,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusCreated,
	})
}

func (h *Handler) handleDescribeDataSource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSourceID := seg(segs, segResID)

	ds, err := h.Backend.DescribeDataSource(accountID, dataSourceID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"DataSource": dataSourceToMap(ds),
		keyRequestID:  reqIDPlaceholder,
		keyStatus:     http.StatusOK,
	})
}

func (h *Handler) handleUpdateDataSource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSourceID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	ds, err := h.Backend.UpdateDataSource(accountID, dataSourceID, strField(body, "Name"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Arn":          ds.Arn,
		"DataSourceId": ds.DataSourceID,
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
		"UpdateStatus": ds.Status,
	})
}

func (h *Handler) handleDeleteDataSource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSourceID := seg(segs, segResID)

	if err := h.Backend.DeleteDataSource(accountID, dataSourceID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListDataSources(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	sources, next, err := h.Backend.ListDataSources(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(sources))
	for _, ds := range sources {
		items = append(items, dataSourceToMap(ds))
	}

	resp := map[string]any{
		"DataSources": items,
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func dataSourceToMap(ds *DataSource) map[string]any {
	return map[string]any{
		"Arn":             ds.Arn,
		"CreatedTime":     ds.CreatedTime.Format(timeFormat),
		"DataSourceId":    ds.DataSourceID,
		"LastUpdatedTime": ds.LastUpdatedTime.Format(timeFormat),
		"Name":            ds.Name,
		keyStatus:          ds.Status,
		"Type":            ds.Type,
	}
}

// ---- DataSet handlers ----

func (h *Handler) handleCreateDataSet(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	ds, err := h.Backend.CreateDataSet(
		accountID,
		strField(body, "DataSetId"),
		strField(body, "Name"),
		strField(body, "ImportMode"),
		tagsFromBody(body),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusCreated, map[string]any{
		"Arn":          ds.Arn,
		"DataSetId":    ds.DataSetID,
		"IngestionArn": fmt.Sprintf("%s/ingestion/auto", ds.Arn),
		"IngestionId":  "auto",
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusCreated,
	})
}

func (h *Handler) handleDescribeDataSet(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)

	ds, err := h.Backend.DescribeDataSet(accountID, dataSetID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"DataSet":   dataSetToMap(ds),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateDataSet(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	ds, err := h.Backend.UpdateDataSet(accountID, dataSetID, strField(body, "Name"), strField(body, "ImportMode"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Arn":       ds.Arn,
		"DataSetId": ds.DataSetID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteDataSet(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)

	if err := h.Backend.DeleteDataSet(accountID, dataSetID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListDataSets(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	datasets, next, err := h.Backend.ListDataSets(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(datasets))
	for _, ds := range datasets {
		items = append(items, dataSetToMap(ds))
	}

	resp := map[string]any{
		"DataSetSummaries": items,
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func dataSetToMap(ds *DataSet) map[string]any {
	return map[string]any{
		"Arn":             ds.Arn,
		"CreatedTime":     ds.CreatedTime.Format(timeFormat),
		"DataSetId":       ds.DataSetID,
		"ImportMode":      ds.ImportMode,
		"LastUpdatedTime": ds.LastUpdatedTime.Format(timeFormat),
		"Name":            ds.Name,
	}
}

// ---- Ingestion handlers ----

func (h *Handler) handleCreateIngestion(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)
	ingestionID := seg(segs, segSubResID)

	ing, err := h.Backend.CreateIngestion(accountID, dataSetID, ingestionID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusCreated, map[string]any{
		"Arn":             ing.Arn,
		"IngestionId":     ing.IngestionID,
		"IngestionStatus": ing.IngestionStatus,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusCreated,
	})
}

func (h *Handler) handleDescribeIngestion(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)
	ingestionID := seg(segs, segSubResID)

	ing, err := h.Backend.DescribeIngestion(accountID, dataSetID, ingestionID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Ingestion": map[string]any{
			"Arn":             ing.Arn,
			"CreatedTime":     ing.CreatedTime.Format(timeFormat),
			"IngestionId":     ing.IngestionID,
			"IngestionStatus": ing.IngestionStatus,
		},
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleCancelIngestion(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)
	ingestionID := seg(segs, segSubResID)

	if err := h.Backend.CancelIngestion(accountID, dataSetID, ingestionID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListIngestions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)

	ingestions, next, err := h.Backend.ListIngestions(accountID, dataSetID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(ingestions))
	for _, ing := range ingestions {
		items = append(items, map[string]any{
			"Arn":             ing.Arn,
			"CreatedTime":     ing.CreatedTime.Format(timeFormat),
			"IngestionId":     ing.IngestionID,
			"IngestionStatus": ing.IngestionStatus,
		})
	}

	resp := map[string]any{
		"Ingestions": items,
		keyRequestID:  reqIDPlaceholder,
		keyStatus:     http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// ---- Dashboard handlers ----

func (h *Handler) handleCreateDashboard(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	name := strField(body, "Name")
	if name == "" {
		name = dashboardID
	}

	d, err := h.Backend.CreateDashboard(accountID, dashboardID, name, tagsFromBody(body))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Arn":            d.Arn,
		"CreationStatus": d.Status,
		"DashboardId":    d.DashboardID,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
		"VersionArn":     fmt.Sprintf("%s/version/1", d.Arn),
	})
}

func (h *Handler) handleDescribeDashboard(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)

	d, err := h.Backend.DescribeDashboard(accountID, dashboardID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Dashboard": dashboardToMap(d),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateDashboard(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	d, err := h.Backend.UpdateDashboard(accountID, dashboardID, strField(body, "Name"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Arn":         d.Arn,
		"DashboardId": d.DashboardID,
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
		"VersionArn":  fmt.Sprintf("%s/version/%d", d.Arn, d.VersionNumber),
	})
}

func (h *Handler) handleDeleteDashboard(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)

	if err := h.Backend.DeleteDashboard(accountID, dashboardID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListDashboards(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	dashboards, next, err := h.Backend.ListDashboards(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(dashboards))
	for _, d := range dashboards {
		items = append(items, dashboardToMap(d))
	}

	resp := map[string]any{
		"DashboardSummaryList": items,
		keyRequestID:            reqIDPlaceholder,
		keyStatus:               http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleListDashboardVersions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)

	versions, next, err := h.Backend.ListDashboardVersions(
		accountID,
		dashboardID,
		maxResultsParam(c),
		nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		items = append(items, map[string]any{
			"Arn":           v.Arn,
			"CreatedTime":   v.CreatedTime.Format(timeFormat),
			keyStatus:        v.Status,
			"VersionNumber": v.VersionNumber,
		})
	}

	resp := map[string]any{
		"DashboardVersionSummaryList": items,
		keyRequestID:                   reqIDPlaceholder,
		keyStatus:                      http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func dashboardToMap(d *Dashboard) map[string]any {
	return map[string]any{
		"Arn":                    d.Arn,
		"CreatedTime":            d.CreatedTime.Format(timeFormat),
		"DashboardId":            d.DashboardID,
		"LastUpdatedTime":        d.LastUpdatedTime.Format(timeFormat),
		"Name":                   d.Name,
		"PublishedVersionNumber": d.VersionNumber,
	}
}

// ---- Analysis handlers ----

func (h *Handler) handleCreateAnalysis(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	analysisID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	name := strField(body, "Name")
	if name == "" {
		name = analysisID
	}

	a, err := h.Backend.CreateAnalysis(accountID, analysisID, name, tagsFromBody(body))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AnalysisId":     a.AnalysisID,
		"Arn":            a.Arn,
		"CreationStatus": a.Status,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	})
}

func (h *Handler) handleDescribeAnalysis(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	analysisID := seg(segs, segResID)

	a, err := h.Backend.DescribeAnalysis(accountID, analysisID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Analysis":  analysisToMap(a),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateAnalysis(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	analysisID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	a, err := h.Backend.UpdateAnalysis(accountID, analysisID, strField(body, "Name"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AnalysisId":   a.AnalysisID,
		"Arn":          a.Arn,
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
		"UpdateStatus": a.Status,
	})
}

func (h *Handler) handleDeleteAnalysis(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	analysisID := seg(segs, segResID)

	force := c.Request().URL.Query().Get("forceDeleteWithoutRecovery") == "true"

	if err := h.Backend.DeleteAnalysis(accountID, analysisID, force); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AnalysisId": analysisID,
		keyRequestID:  reqIDPlaceholder,
		keyStatus:     http.StatusOK,
	})
}

func (h *Handler) handleListAnalyses(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	analyses, next, err := h.Backend.ListAnalyses(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(analyses))
	for _, a := range analyses {
		items = append(items, analysisToMap(a))
	}

	resp := map[string]any{
		"AnalysisSummaryList": items,
		keyRequestID:           reqIDPlaceholder,
		keyStatus:              http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleRestoreAnalysis(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	// path: /accounts/{id}/restore/analyses/{analysisId}
	analysisID := seg(segs, segSubRes)

	a, err := h.Backend.RestoreAnalysis(accountID, analysisID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AnalysisId": a.AnalysisID,
		"Arn":        a.Arn,
		keyRequestID:  reqIDPlaceholder,
		keyStatus:     http.StatusOK,
	})
}

func analysisToMap(a *Analysis) map[string]any {
	return map[string]any{
		"AnalysisId":      a.AnalysisID,
		"Arn":             a.Arn,
		"CreatedTime":     a.CreatedTime.Format(timeFormat),
		"LastUpdatedTime": a.LastUpdatedTime.Format(timeFormat),
		"Name":            a.Name,
		keyStatus:          a.Status,
	}
}

// ---- Tag handlers ----

func (h *Handler) handleTagResource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	// /resources/{arnParts...}/tags
	arn := strings.Join(segs[1:len(segs)-1], "/")

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	tags := tagsFromBody(body)
	if err := h.Backend.TagResource(arn, tags); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	arn := strings.Join(segs[1:len(segs)-1], "/")

	keys := c.Request().URL.Query()["keys"]

	if err := h.Backend.UntagResource(arn, keys); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	arn := strings.Join(segs[1:len(segs)-1], "/")

	tags, err := h.Backend.ListTagsForResource(arn)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		items = append(items, map[string]any{"Key": k, "Value": v})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
		"Tags":      items,
	})
}
