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

	// operation name constants.
	opCreateNamespace         = "CreateNamespace"
	opDescribeNamespace       = "DescribeNamespace"
	opDeleteNamespace         = "DeleteNamespace"
	opListNamespaces          = "ListNamespaces"
	opCreateGroup             = "CreateGroup"
	opDescribeGroup           = "DescribeGroup"
	opUpdateGroup             = "UpdateGroup"
	opDeleteGroup             = "DeleteGroup"
	opListGroups              = "ListGroups"
	opSearchGroups            = "SearchGroups"
	opCreateGroupMembership   = "CreateGroupMembership"
	opDescribeGroupMembership = "DescribeGroupMembership"
	opDeleteGroupMembership   = "DeleteGroupMembership"
	opListGroupMemberships    = "ListGroupMemberships"
	opRegisterUser            = "RegisterUser"
	opDescribeUser            = "DescribeUser"
	opUpdateUser              = "UpdateUser"
	opDeleteUser              = "DeleteUser"
	opDeleteUserByPrincipalID = "DeleteUserByPrincipalId"
	opListUsers               = "ListUsers"
	opListUserGroups          = "ListUserGroups"
	opCreateDataSource        = "CreateDataSource"
	opDescribeDataSource      = "DescribeDataSource"
	opUpdateDataSource        = "UpdateDataSource"
	opDeleteDataSource        = "DeleteDataSource"
	opListDataSources         = "ListDataSources"
	opCreateDataSet           = "CreateDataSet"
	opDescribeDataSet         = "DescribeDataSet"
	opUpdateDataSet           = "UpdateDataSet"
	opDeleteDataSet           = "DeleteDataSet"
	opListDataSets            = "ListDataSets"
	opCreateIngestion         = "CreateIngestion"
	opDescribeIngestion       = "DescribeIngestion"
	opCancelIngestion         = "CancelIngestion"
	opListIngestions          = "ListIngestions"
	opCreateDashboard         = "CreateDashboard"
	opDescribeDashboard       = "DescribeDashboard"
	opUpdateDashboard         = "UpdateDashboard"
	opDeleteDashboard         = "DeleteDashboard"
	opListDashboards          = "ListDashboards"
	opListDashboardVersions   = "ListDashboardVersions"
	opCreateAnalysis          = "CreateAnalysis"
	opDescribeAnalysis        = "DescribeAnalysis"
	opUpdateAnalysis          = "UpdateAnalysis"
	opDeleteAnalysis          = "DeleteAnalysis"
	opListAnalyses            = "ListAnalyses"
	opRestoreAnalysis         = "RestoreAnalysis"
	opTagResource             = "TagResource"
	opUntagResource           = "UntagResource"
	opListTagsForResource     = "ListTagsForResource"

	// path segment indices.
	segAccountID   = 1
	segResource    = 2
	segResID       = 3
	segSubRes      = 4
	segSubResID    = 5
	segSubSubRes   = 6
	segSubSubResID = 7

	// segment count constants.
	nSegsAccountRoot  = 2
	nSegsAccountRes   = 3
	nSegsAccountResID = 4
	nSegsSubRes       = 5
	nSegsSubResID     = 6
	nSegsSubSubRes    = 7
	nSegsSubSubResID  = 8

	// JSON response keys.
	keyRequestID            = "RequestId"
	keyStatus               = "Status"
	keyNextToken            = "NextToken"
	keyGroup                = "Group"
	keyGroupList            = "GroupList"
	keyUser                 = "User"
	keyUserList             = "UserList"
	keyDataSource           = "DataSource"
	keyDataSources          = "DataSources"
	keyDataSourceID         = "DataSourceId"
	keyDataSet              = "DataSet"
	keyDataSetSummaries     = "DataSetSummaries"
	keyDataSetID            = "DataSetId"
	keyIngestion            = "Ingestion"
	keyIngestions           = "Ingestions"
	keyIngestionID          = "IngestionId"
	keyIngestionStatus      = "IngestionStatus"
	keyDashboard            = "Dashboard"
	keyDashboardSummaryList = "DashboardSummaryList"
	keyDashboardID          = "DashboardId"
	keyAnalysis             = "Analysis"
	keyAnalysisSummaryList  = "AnalysisSummaryList"
	keyAnalysisID           = "AnalysisId"
	keyCreatedTime          = "CreatedTime"
	keyLastUpdatedTime      = "LastUpdatedTime"
	keyArn                  = "Arn"
	keyName                 = "Name"
	keyCapacityRegion       = "CapacityRegion"
	keyCreationStatus       = "CreationStatus"
	keyIdentityStore        = "IdentityStore"
	keyNamespace            = "Namespace"
	keyMemberName           = "MemberName"

	// request ID placeholder.
	reqIDPlaceholder = "request-id"

	// path segment names.
	pathSegAccounts       = "accounts"
	pathSegNamespaces     = "namespaces"
	pathSegGroups         = "groups"
	pathSegMembers        = "members"
	pathSegUsers          = "users"
	pathSegDataSources    = "data-sources"
	pathSegDataSets       = "data-sets"
	pathSegIngestions     = "ingestions"
	pathSegDashboards     = "dashboards"
	pathSegAnalyses       = "analyses"
	pathSegVersions       = "versions"
	pathSegSearch         = "search"
	pathSegRestore        = "restore"
	pathSegResources      = "resources"
	pathSegTagsSuffix     = "tags"
	pathSegGroupsSearch   = "groups-search"
	pathSegUserPrincipals = "user-principals"

	// time format.
	timeFormat = "2006-01-02T15:04:05Z"

	// error codes.
	errInvalidParam = "InvalidParameterValueException"
	errInvalidBody  = "invalid request body"
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
		opCreateNamespace,
		opDescribeNamespace,
		opDeleteNamespace,
		opListNamespaces,
		opCreateGroup,
		opDescribeGroup,
		opUpdateGroup,
		opDeleteGroup,
		opListGroups,
		opSearchGroups,
		opCreateGroupMembership,
		opDescribeGroupMembership,
		opDeleteGroupMembership,
		opListGroupMemberships,
		opRegisterUser,
		opDescribeUser,
		opUpdateUser,
		opDeleteUser,
		opDeleteUserByPrincipalID,
		opListUsers,
		opListUserGroups,
		opCreateDataSource,
		opDescribeDataSource,
		opUpdateDataSource,
		opDeleteDataSource,
		opListDataSources,
		opCreateDataSet,
		opDescribeDataSet,
		opUpdateDataSet,
		opDeleteDataSet,
		opListDataSets,
		opCreateIngestion,
		opDescribeIngestion,
		opCancelIngestion,
		opListIngestions,
		opCreateDashboard,
		opDescribeDashboard,
		opUpdateDashboard,
		opDeleteDashboard,
		opListDashboards,
		opListDashboardVersions,
		opCreateAnalysis,
		opDescribeAnalysis,
		opUpdateAnalysis,
		opDeleteAnalysis,
		opListAnalyses,
		opRestoreAnalysis,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
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

func isNamespaceOp(op string) bool {
	switch op {
	case opCreateNamespace, opDescribeNamespace, opDeleteNamespace, opListNamespaces:
		return true
	}

	return false
}

func isGroupOp(op string) bool {
	switch op {
	case opCreateGroup, opDescribeGroup, opUpdateGroup, opDeleteGroup, opListGroups, opSearchGroups,
		opCreateGroupMembership, opDescribeGroupMembership, opDeleteGroupMembership, opListGroupMemberships:
		return true
	}

	return false
}

func isUserOp(op string) bool {
	switch op {
	case opRegisterUser, opDescribeUser, opUpdateUser, opDeleteUser,
		opDeleteUserByPrincipalID, opListUsers, opListUserGroups:
		return true
	}

	return false
}

func isDataSourceOp(op string) bool {
	switch op {
	case opCreateDataSource, opDescribeDataSource, opUpdateDataSource, opDeleteDataSource, opListDataSources:
		return true
	}

	return false
}

func isDataSetOp(op string) bool {
	switch op {
	case opCreateDataSet, opDescribeDataSet, opUpdateDataSet, opDeleteDataSet, opListDataSets,
		opCreateIngestion, opDescribeIngestion, opCancelIngestion, opListIngestions:
		return true
	}

	return false
}

func isDashboardOp(op string) bool {
	switch op {
	case opCreateDashboard, opDescribeDashboard, opUpdateDashboard, opDeleteDashboard,
		opListDashboards, opListDashboardVersions:
		return true
	}

	return false
}

func isAnalysisOp(op string) bool {
	switch op {
	case opCreateAnalysis, opDescribeAnalysis, opUpdateAnalysis, opDeleteAnalysis,
		opListAnalyses, opRestoreAnalysis:
		return true
	}

	return false
}

func isTagOp(op string) bool {
	switch op {
	case opTagResource, opUntagResource, opListTagsForResource:
		return true
	}

	return false
}

func (h *Handler) dispatch(c *echo.Context) error {
	op, _ := classifyRequest(c.Request().Method, c.Request().URL.Path)
	switch {
	case isNamespaceOp(op):
		return h.dispatchNamespace(c, op)
	case isGroupOp(op):
		return h.dispatchGroup(c, op)
	case isUserOp(op):
		return h.dispatchUser(c, op)
	case isDataSourceOp(op):
		return h.dispatchDataSource(c, op)
	case isDataSetOp(op):
		return h.dispatchDataSet(c, op)
	case isDashboardOp(op):
		return h.dispatchDashboard(c, op)
	case isAnalysisOp(op):
		return h.dispatchAnalysis(c, op)
	case isTagOp(op):
		return h.dispatchTag(c, op)
	default:
		return writeError(
			c,
			http.StatusNotImplemented,
			"UnsupportedOperationException",
			fmt.Sprintf("operation %q not implemented", op),
		)
	}
}

func (h *Handler) dispatchNamespace(c *echo.Context, op string) error {
	switch op {
	case opCreateNamespace:
		return h.handleCreateNamespace(c)
	case opDescribeNamespace:
		return h.handleDescribeNamespace(c)
	case opDeleteNamespace:
		return h.handleDeleteNamespace(c)
	case opListNamespaces:
		return h.handleListNamespaces(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

func (h *Handler) dispatchGroup(c *echo.Context, op string) error {
	switch op {
	case opCreateGroup:
		return h.handleCreateGroup(c)
	case opDescribeGroup:
		return h.handleDescribeGroup(c)
	case opUpdateGroup:
		return h.handleUpdateGroup(c)
	case opDeleteGroup:
		return h.handleDeleteGroup(c)
	case opListGroups:
		return h.handleListGroups(c)
	case opSearchGroups:
		return h.handleSearchGroups(c)
	case opCreateGroupMembership:
		return h.handleCreateGroupMembership(c)
	case opDescribeGroupMembership:
		return h.handleDescribeGroupMembership(c)
	case opDeleteGroupMembership:
		return h.handleDeleteGroupMembership(c)
	case opListGroupMemberships:
		return h.handleListGroupMemberships(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

func (h *Handler) dispatchUser(c *echo.Context, op string) error {
	switch op {
	case opRegisterUser:
		return h.handleRegisterUser(c)
	case opDescribeUser:
		return h.handleDescribeUser(c)
	case opUpdateUser:
		return h.handleUpdateUser(c)
	case opDeleteUser:
		return h.handleDeleteUser(c)
	case opDeleteUserByPrincipalID:
		return h.handleDeleteUserByPrincipalID(c)
	case opListUsers:
		return h.handleListUsers(c)
	case opListUserGroups:
		return h.handleListUserGroups(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

func (h *Handler) dispatchDataSource(c *echo.Context, op string) error {
	switch op {
	case opCreateDataSource:
		return h.handleCreateDataSource(c)
	case opDescribeDataSource:
		return h.handleDescribeDataSource(c)
	case opUpdateDataSource:
		return h.handleUpdateDataSource(c)
	case opDeleteDataSource:
		return h.handleDeleteDataSource(c)
	case opListDataSources:
		return h.handleListDataSources(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

func (h *Handler) dispatchDataSet(c *echo.Context, op string) error {
	switch op {
	case opCreateDataSet:
		return h.handleCreateDataSet(c)
	case opDescribeDataSet:
		return h.handleDescribeDataSet(c)
	case opUpdateDataSet:
		return h.handleUpdateDataSet(c)
	case opDeleteDataSet:
		return h.handleDeleteDataSet(c)
	case opListDataSets:
		return h.handleListDataSets(c)
	case opCreateIngestion:
		return h.handleCreateIngestion(c)
	case opDescribeIngestion:
		return h.handleDescribeIngestion(c)
	case opCancelIngestion:
		return h.handleCancelIngestion(c)
	case opListIngestions:
		return h.handleListIngestions(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

func (h *Handler) dispatchDashboard(c *echo.Context, op string) error {
	switch op {
	case opCreateDashboard:
		return h.handleCreateDashboard(c)
	case opDescribeDashboard:
		return h.handleDescribeDashboard(c)
	case opUpdateDashboard:
		return h.handleUpdateDashboard(c)
	case opDeleteDashboard:
		return h.handleDeleteDashboard(c)
	case opListDashboards:
		return h.handleListDashboards(c)
	case opListDashboardVersions:
		return h.handleListDashboardVersions(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

func (h *Handler) dispatchAnalysis(c *echo.Context, op string) error {
	switch op {
	case opCreateAnalysis:
		return h.handleCreateAnalysis(c)
	case opDescribeAnalysis:
		return h.handleDescribeAnalysis(c)
	case opUpdateAnalysis:
		return h.handleUpdateAnalysis(c)
	case opDeleteAnalysis:
		return h.handleDeleteAnalysis(c)
	case opListAnalyses:
		return h.handleListAnalyses(c)
	case opRestoreAnalysis:
		return h.handleRestoreAnalysis(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

func (h *Handler) dispatchTag(c *echo.Context, op string) error {
	switch op {
	case opTagResource:
		return h.handleTagResource(c)
	case opUntagResource:
		return h.handleUntagResource(c)
	case opListTagsForResource:
		return h.handleListTagsForResource(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
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
	if n >= nSegsAccountRes && segs[0] == pathSegResources && segs[n-1] == pathSegTagsSuffix {
		arn := strings.Join(segs[1:n-1], "/")
		switch method {
		case http.MethodPost:
			return opTagResource, arn
		case http.MethodGet:
			return opListTagsForResource, arn
		case http.MethodDelete:
			return opUntagResource, arn
		}

		return opUnknown, ""
	}

	// All remaining paths start with /accounts/{accountId}
	if n < nSegsAccountRoot || segs[0] != pathSegAccounts {
		return opUnknown, ""
	}

	// segs[0]=accounts, segs[1]=accountId, segs[2]=resource-type, ...
	if n == nSegsAccountRoot {
		// POST /accounts/{accountId} → CreateNamespace
		if method == http.MethodPost {
			return opCreateNamespace, seg(segs, segAccountID)
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
	case nSegsAccountRes:
		return classifyNsRoot(method)
	case nSegsAccountResID:
		return classifyNsWithID(method, segs)
	case nSegsSubRes:
		return classifyNsWithSubRes(method, segs)
	case nSegsSubResID:
		return classifyNsWithSubResID(method, segs)
	case nSegsSubSubRes:
		return classifyNsWithSubSubRes(method, segs)
	case nSegsSubSubResID:
		return classifyNsWithSubSubResID(method, segs)
	}

	return opUnknown, ""
}

func classifyNsRoot(method string) (string, string) {
	if method == http.MethodGet {
		return opListNamespaces, ""
	}

	return opUnknown, ""
}

func classifyNsWithID(method string, segs []string) (string, string) {
	ns := seg(segs, segResID)
	switch method {
	case http.MethodGet:
		return opDescribeNamespace, ns
	case http.MethodDelete:
		return opDeleteNamespace, ns
	}

	return opUnknown, ""
}

func classifyNsWithSubRes(method string, segs []string) (string, string) {
	ns := seg(segs, segResID)
	sub := seg(segs, segSubRes)
	switch sub {
	case pathSegGroups:
		switch method {
		case http.MethodPost:
			return opCreateGroup, ns
		case http.MethodGet:
			return opListGroups, ns
		}
	case pathSegUsers:
		switch method {
		case http.MethodPost:
			return opRegisterUser, ns
		case http.MethodGet:
			return opListUsers, ns
		}
	case pathSegGroupsSearch:
		if method == http.MethodPost {
			return opSearchGroups, ns
		}
	}

	return opUnknown, ""
}

func classifyNsWithSubResID(method string, segs []string) (string, string) {
	sub := seg(segs, segSubRes)
	id := seg(segs, segSubResID)
	switch sub {
	case pathSegGroups:
		switch method {
		case http.MethodGet:
			return opDescribeGroup, id
		case http.MethodPut:
			return opUpdateGroup, id
		case http.MethodDelete:
			return opDeleteGroup, id
		}
	case pathSegUsers:
		switch method {
		case http.MethodGet:
			return opDescribeUser, id
		case http.MethodPut:
			return opUpdateUser, id
		case http.MethodDelete:
			return opDeleteUser, id
		}
	case pathSegUserPrincipals:
		if method == http.MethodDelete {
			return opDeleteUserByPrincipalID, seg(segs, segResID)
		}
	}

	return opUnknown, ""
}

func classifyNsWithSubSubRes(method string, segs []string) (string, string) {
	sub := seg(segs, segSubRes)
	id := seg(segs, segSubResID)
	tail := seg(segs, segSubSubRes)
	switch {
	case sub == pathSegGroups && tail == pathSegMembers:
		if method == http.MethodGet {
			return opListGroupMemberships, id
		}
	case sub == pathSegUsers && tail == pathSegGroups:
		if method == http.MethodGet {
			return opListUserGroups, id
		}
	}

	return opUnknown, ""
}

func classifyNsWithSubSubResID(method string, segs []string) (string, string) {
	sub := seg(segs, segSubRes)
	tail := seg(segs, segSubSubRes)
	if sub == pathSegGroups && tail == pathSegMembers {
		switch method {
		case http.MethodPut:
			return opCreateGroupMembership, seg(segs, segSubResID)
		case http.MethodGet:
			return opDescribeGroupMembership, seg(segs, segSubResID)
		case http.MethodDelete:
			return opDeleteGroupMembership, seg(segs, segSubResID)
		}
	}

	return opUnknown, ""
}

func classifyDataSourcePaths(method string, segs []string, n int) (string, string) {
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opCreateDataSource, seg(segs, segAccountID)
		case http.MethodGet:
			return opListDataSources, seg(segs, segAccountID)
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return opDescribeDataSource, id
		case http.MethodPut:
			return opUpdateDataSource, id
		case http.MethodDelete:
			return opDeleteDataSource, id
		}
	}

	return opUnknown, ""
}

func classifyIngestionPaths(method string, segs []string, n int) (string, string) {
	switch n {
	case nSegsSubRes:
		if method == http.MethodGet {
			return opListIngestions, seg(segs, segResID)
		}
	case nSegsSubResID:
		ingID := seg(segs, segSubResID)
		switch method {
		case http.MethodPut:
			return opCreateIngestion, ingID
		case http.MethodGet:
			return opDescribeIngestion, ingID
		case http.MethodDelete:
			return opCancelIngestion, ingID
		}
	}

	return opUnknown, ""
}

func classifyDataSetPaths(method string, segs []string, n int) (string, string) {
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opCreateDataSet, seg(segs, segAccountID)
		case http.MethodGet:
			return opListDataSets, seg(segs, segAccountID)
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return opDescribeDataSet, id
		case http.MethodPut:
			return opUpdateDataSet, id
		case http.MethodDelete:
			return opDeleteDataSet, id
		}
	case nSegsSubRes, nSegsSubResID:
		if seg(segs, segSubRes) == pathSegIngestions {
			return classifyIngestionPaths(method, segs, n)
		}
	}

	return opUnknown, ""
}

func classifyDashboardPaths(method string, segs []string, n int) (string, string) {
	switch n {
	case nSegsAccountRes:
		if method == http.MethodGet {
			return opListDashboards, seg(segs, segAccountID)
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodPost:
			return opCreateDashboard, id
		case http.MethodGet:
			return opDescribeDashboard, id
		case http.MethodPut:
			return opUpdateDashboard, id
		case http.MethodDelete:
			return opDeleteDashboard, id
		}
	case nSegsSubRes:
		if seg(segs, segSubRes) == pathSegVersions && method == http.MethodGet {
			return opListDashboardVersions, seg(segs, segResID)
		}
	}

	return opUnknown, ""
}

func classifyAnalysisPaths(method string, segs []string, n int) (string, string) {
	switch n {
	case nSegsAccountRes:
		if method == http.MethodGet {
			return opListAnalyses, seg(segs, segAccountID)
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodPost:
			return opCreateAnalysis, id
		case http.MethodGet:
			return opDescribeAnalysis, id
		case http.MethodPut:
			return opUpdateAnalysis, id
		case http.MethodDelete:
			return opDeleteAnalysis, id
		}
	}

	return opUnknown, ""
}

func classifySearchPaths(method string, segs []string, n int) (string, string) {
	if method != http.MethodPost || n < nSegsAccountResID {
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
	if method == http.MethodPost && n == nSegsSubRes && seg(segs, segResID) == pathSegAnalyses {
		return opRestoreAnalysis, seg(segs, segSubRes)
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
		return int32(v)
	case int:
		return int32(v) //nolint:gosec // int from JSON always fits int32
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

	return int32(n)
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
		return writeError(c, http.StatusBadRequest, errInvalidParam, err.Error())
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
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	namespace := strField(body, "Namespace")
	capacityRegion := strField(body, "CapacityRegion")

	ns, err := h.Backend.CreateNamespace(accountID, namespace, capacityRegion)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:            ns.Arn,
		keyCapacityRegion: ns.CapacityRegion,
		keyCreationStatus: ns.CreationStatus,
		keyIdentityStore:  ns.IdentityStore,
		keyName:           ns.Name,
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
		keyNamespace: map[string]any{
			keyArn:            ns.Arn,
			keyCapacityRegion: ns.CapacityRegion,
			keyCreationStatus: ns.CreationStatus,
			keyIdentityStore:  ns.IdentityStore,
			keyName:           ns.Name,
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
			keyArn:            ns.Arn,
			keyCapacityRegion: ns.CapacityRegion,
			keyCreationStatus: ns.CreationStatus,
			keyIdentityStore:  ns.IdentityStore,
			keyName:           ns.Name,
		})
	}

	resp := map[string]any{
		"Namespaces": items,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
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
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	groupName := strField(body, "GroupName")
	description := strField(body, "Description")

	g, err := h.Backend.CreateGroup(accountID, namespace, groupName, description)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyGroup:     groupToMap(g),
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
		keyGroup:     groupToMap(g),
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
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	description := strField(body, "Description")

	g, err := h.Backend.UpdateGroup(accountID, namespace, groupName, description)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyGroup:     groupToMap(g),
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
		keyGroupList: items,
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
		keyGroupList: items,
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
		keyArn:        g.Arn,
		"Description": g.Description,
		"GroupName":   g.GroupName,
		keyNamespace:  g.Namespace,
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
			keyArn:        m.Arn,
			keyMemberName: m.MemberName,
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
			keyArn:        m.Arn,
			keyMemberName: m.MemberName,
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
			keyArn:        m.Arn,
			keyMemberName: m.MemberName,
		})
	}

	resp := map[string]any{
		"GroupMemberList": items,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
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
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
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
		keyUser:      userToMap(u),
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
		keyUser:      userToMap(u),
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
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	u, err := h.Backend.UpdateUser(accountID, namespace, userName, strField(body, "Email"), strField(body, "Role"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyUser:      userToMap(u),
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
		keyUserList:  items,
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
		keyGroupList: items,
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
		keyArn:         u.Arn,
		"Email":        u.Email,
		"IdentityType": u.IdentityType,
		keyNamespace:   u.Namespace,
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
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
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
		keyArn:            ds.Arn,
		keyCreationStatus: ds.Status,
		keyDataSourceID:   ds.DataSourceID,
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
		keyDataSource: dataSourceToMap(ds),
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
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	ds, err := h.Backend.UpdateDataSource(accountID, dataSourceID, strField(body, "Name"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:          ds.Arn,
		keyDataSourceID: ds.DataSourceID,
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
		"UpdateStatus":  ds.Status,
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
		keyDataSources: items,
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
		keyArn:             ds.Arn,
		keyCreatedTime:     ds.CreatedTime.Format(timeFormat),
		keyDataSourceID:    ds.DataSourceID,
		keyLastUpdatedTime: ds.LastUpdatedTime.Format(timeFormat),
		keyName:            ds.Name,
		keyStatus:          ds.Status,
		"Type":             ds.Type,
	}
}

// ---- DataSet handlers ----

func (h *Handler) handleCreateDataSet(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
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
		keyArn:         ds.Arn,
		keyDataSetID:   ds.DataSetID,
		"IngestionArn": fmt.Sprintf("%s/ingestion/auto", ds.Arn),
		"IngestionId":  "auto",
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusCreated,
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
		keyDataSet:   dataSetToMap(ds),
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
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	ds, err := h.Backend.UpdateDataSet(accountID, dataSetID, strField(body, "Name"), strField(body, "ImportMode"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:       ds.Arn,
		keyDataSetID: ds.DataSetID,
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
		keyDataSetSummaries: items,
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
		keyArn:             ds.Arn,
		keyCreatedTime:     ds.CreatedTime.Format(timeFormat),
		keyDataSetID:       ds.DataSetID,
		"ImportMode":       ds.ImportMode,
		keyLastUpdatedTime: ds.LastUpdatedTime.Format(timeFormat),
		keyName:            ds.Name,
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
		keyArn:             ing.Arn,
		keyIngestionID:     ing.IngestionID,
		keyIngestionStatus: ing.IngestionStatus,
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
		keyIngestion: map[string]any{
			keyArn:             ing.Arn,
			keyCreatedTime:     ing.CreatedTime.Format(timeFormat),
			keyIngestionID:     ing.IngestionID,
			keyIngestionStatus: ing.IngestionStatus,
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
			keyArn:             ing.Arn,
			keyCreatedTime:     ing.CreatedTime.Format(timeFormat),
			keyIngestionID:     ing.IngestionID,
			keyIngestionStatus: ing.IngestionStatus,
		})
	}

	resp := map[string]any{
		keyIngestions: items,
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
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
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
		keyArn:            d.Arn,
		keyCreationStatus: d.Status,
		keyDashboardID:    d.DashboardID,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
		"VersionArn":      fmt.Sprintf("%s/version/1", d.Arn),
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
		keyDashboard: dashboardToMap(d),
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
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	d, err := h.Backend.UpdateDashboard(accountID, dashboardID, strField(body, "Name"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:         d.Arn,
		keyDashboardID: d.DashboardID,
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
		"VersionArn":   fmt.Sprintf("%s/version/%d", d.Arn, d.VersionNumber),
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
		keyDashboardSummaryList: items,
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
			keyArn:          v.Arn,
			keyCreatedTime:  v.CreatedTime.Format(timeFormat),
			keyStatus:       v.Status,
			"VersionNumber": v.VersionNumber,
		})
	}

	resp := map[string]any{
		"DashboardVersionSummaryList": items,
		keyRequestID:                  reqIDPlaceholder,
		keyStatus:                     http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func dashboardToMap(d *Dashboard) map[string]any {
	return map[string]any{
		keyArn:                   d.Arn,
		keyCreatedTime:           d.CreatedTime.Format(timeFormat),
		keyDashboardID:           d.DashboardID,
		keyLastUpdatedTime:       d.LastUpdatedTime.Format(timeFormat),
		keyName:                  d.Name,
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
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
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
		keyAnalysisID:     a.AnalysisID,
		keyArn:            a.Arn,
		keyCreationStatus: a.Status,
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
		keyAnalysis:  analysisToMap(a),
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
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	a, err := h.Backend.UpdateAnalysis(accountID, analysisID, strField(body, "Name"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAnalysisID:  a.AnalysisID,
		keyArn:         a.Arn,
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
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
		keyAnalysisID: analysisID,
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
		keyAnalysisSummaryList: items,
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
		keyAnalysisID: a.AnalysisID,
		keyArn:        a.Arn,
		keyRequestID:  reqIDPlaceholder,
		keyStatus:     http.StatusOK,
	})
}

func analysisToMap(a *Analysis) map[string]any {
	return map[string]any{
		keyAnalysisID:      a.AnalysisID,
		keyArn:             a.Arn,
		keyCreatedTime:     a.CreatedTime.Format(timeFormat),
		keyLastUpdatedTime: a.LastUpdatedTime.Format(timeFormat),
		keyName:            a.Name,
		keyStatus:          a.Status,
	}
}

// ---- Tag handlers ----

func (h *Handler) handleTagResource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	// /resources/{arnParts...}/tags
	arn := strings.Join(segs[1:len(segs)-1], "/")

	body, bodyErr := readBody(c)
	if bodyErr != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
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
		"Tags":       items,
	})
}
