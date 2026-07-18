package quicksight

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/labstack/echo/v5"
)

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
func classifyRequest(method, path string) (string, string) { //nolint:gocognit,gocyclo,funlen // existing issue.
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

	// /account/{accountId} (singular) — AccountSubscription ops
	if n >= nSegsAccountRoot && segs[0] == pathSegAccountSingular {
		return classifyAccountSubscriptionPaths(method, segs, n)
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
	case pathSegNamespaceSingular:
		// DELETE /accounts/{id}/namespace/{ns}/iam-policy-assignments/{name}
		return classifyNamespaceSingularPaths(method, segs, n)
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
	case pathSegFolders:
		return classifyFolderPaths(method, segs, n)
	case pathSegTemplates:
		return classifyTemplatePaths(method, segs, n)
	case pathSegThemes:
		return classifyThemePaths(method, segs, n)
	case pathSegTopics:
		return classifyTopicPaths(method, segs, n)
	case pathSegVPCConnections:
		return classifyVPCConnectionPaths(method, segs, n)
	case pathSegActionConnectors:
		return classifyActionConnectorPaths(method, segs, n)
	case pathSegBrands:
		return classifyBrandPaths(method, segs, n)
	case pathSegBrandAssignments:
		return classifyBrandAssignmentPaths(method, segs, n)
	case pathSegCustomPermissions:
		return classifyCustomPermissionsPaths(method, segs, n)
	case pathSegOAuthApps:
		return classifyOAuthAppPaths(method, segs, n)
	case pathSegIdentityPropagation:
		return classifyIdentityPropagationPaths(method, segs, n)
	case pathSegAssetBundleExport:
		return classifyAssetBundleExportPaths(method, segs, n)
	case pathSegAssetBundleImport:
		return classifyAssetBundleImportPaths(method, segs, n)
	case pathSegAutomationGroups:
		return classifyAutomationPaths(method, segs, n)
	case pathSegFlows:
		return classifyFlowPaths(method, segs, n)
	case pathSegResource2:
		return classifyResourceFoldersPaths(method, segs, n)
	case pathSegCustomizations:
		return classifyCustomizationPaths(method, segs, n)
	case pathSegCustomPermission:
		return classifyAccountCustomPermissionPaths(method, segs, n)
	case pathSegSettings:
		return classifyAccountSettingsPaths(method, segs, n)
	case pathSegDashboardsQACfg:
		return classifyDashboardsQAPaths(method, segs, n)
	case pathSegDefaultQBiz:
		return classifyDefaultQBizPaths(method, segs, n)
	case pathSegIPRestriction:
		return classifyIPRestrictionPaths(method, segs, n)
	case pathSegKeyRegistration:
		return classifyKeyRegistrationPaths(method, segs, n)
	case pathSegPublicSharing:
		if method == http.MethodPut {
			return opUpdatePublicSharingSettings, seg(segs, segAccountID)
		}
	case pathSegQPersonalization:
		return classifyQPersonalizationPaths(method, segs, n)
	case pathSegQSearchConfig:
		return classifyQSearchConfigPaths(method, segs, n)
	case pathSegSPICECapacity:
		if method == http.MethodPost {
			return opUpdateSPICECapacity, seg(segs, segAccountID)
		}
	case pathSegEmbedUrl:
		return classifyEmbedURLPaths(method, segs, n)
	case pathSegSessionEmbedUrl:
		if method == http.MethodGet {
			return opGetSessionEmbedUrl, seg(segs, segAccountID)
		}
	case pathSegIdentityContext:
		if method == http.MethodPost {
			return opGetIdentityContext, seg(segs, segAccountID)
		}
	case pathSegQA:
		// POST /accounts/{id}/qa/predict (n == nSegsAccountResID: accounts,
		// {id}, qa, predict). The previous "n > nSegsAccountResID" guard made
		// this real 4-segment path unreachable.
		if n >= nSegsAccountResID && seg(segs, segResID) == pathSegPredict && method == http.MethodPost {
			return opPredictQAResults, seg(segs, segAccountID)
		}
	case pathSegAppTokenGrant:
		if method == http.MethodPut {
			return opUpdateAppTokenGrant, seg(segs, segAccountID)
		}
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

func classifyNsWithSubRes(method string, segs []string) (string, string) { //nolint:cyclop // existing issue.
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
	case pathSegIAMPolicyAssignments:
		switch method {
		case http.MethodPost:
			return opCreateIAMPolicyAssignment, ns
		case http.MethodGet:
			return opListIAMPolicyAssignments, ns
		}
	case pathSegSelfUpgradeCfg:
		switch method {
		case http.MethodGet:
			return opDescribeSelfUpgradeConfig, ns
		case http.MethodPut:
			return opUpdateSelfUpgradeConfig, ns
		}
	case pathSegSelfUpgradeReqs:
		if method == http.MethodGet {
			return opListSelfUpgrades, ns
		}
	case pathSegUpdateSelfUpgrade:
		if method == http.MethodPost {
			return opUpdateSelfUpgrade, ns
		}
	}

	return opUnknown, ""
}

func classifyNsWithSubResID(method string, segs []string) (string, string) { //nolint:cyclop // existing issue.
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
	case pathSegIAMPolicyAssignments:
		// namespaces/{ns}/iam-policy-assignments/{name}
		switch method {
		case http.MethodGet:
			return opDescribeIAMPolicyAssignment, id
		case http.MethodPut:
			return opUpdateIAMPolicyAssignment, id
		}
	case pathSegV2:
		// namespaces/{ns}/v2/iam-policy-assignments
		if id == pathSegIAMPolicyAssignments && method == http.MethodGet {
			return opListIAMPolicyAssignments, seg(segs, segResID)
		}
	}

	return opUnknown, ""
}

func classifyNsWithSubSubRes(method string, segs []string) (string, string) { //nolint:cyclop // existing issue.
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
	case sub == pathSegUsers && tail == pathSegIAMPolicyAssignments:
		if method == http.MethodGet {
			return opListIAMPolicyAssignmentsForUser, id
		}
	case sub == pathSegUsers && tail == pathSegCustomPermission:
		switch method {
		case http.MethodPut:
			return opUpdateUserCustomPermission, id
		case http.MethodDelete:
			return opDeleteUserCustomPermission, id
		}
	case sub == pathSegRoles && tail == pathSegCustomPermission:
		switch method {
		case http.MethodGet:
			return opGetRoleCustomPermission, id
		case http.MethodPut:
			return opUpdateRoleCustomPermission, id
		case http.MethodDelete:
			return opDeleteRoleCustomPermission, id
		}
	case sub == pathSegRoles && tail == pathSegMembers:
		if method == http.MethodGet {
			return opListRoleMemberships, id
		}
	}

	return opUnknown, ""
}

func classifyNsWithSubSubResID(method string, segs []string) (string, string) {
	sub := seg(segs, segSubRes)
	tail := seg(segs, segSubSubRes)
	switch {
	case sub == pathSegGroups && tail == pathSegMembers:
		switch method {
		case http.MethodPut:
			return opCreateGroupMembership, seg(segs, segSubResID)
		case http.MethodGet:
			return opDescribeGroupMembership, seg(segs, segSubResID)
		case http.MethodDelete:
			return opDeleteGroupMembership, seg(segs, segSubResID)
		}
	case sub == pathSegRoles && tail == pathSegMembers:
		switch method {
		case http.MethodPost:
			return opCreateRoleMembership, seg(segs, segSubResID)
		case http.MethodDelete:
			return opDeleteRoleMembership, seg(segs, segSubResID)
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
	case nSegsSubRes:
		id := seg(segs, segResID)
		if seg(segs, segSubRes) == pathSegPermissions {
			switch method {
			case http.MethodGet:
				return opDescribeDataSourcePerms, id
			case http.MethodPost:
				return opUpdateDataSourcePerms, id
			}
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

func classifyDataSetPaths( //nolint:gocognit,cyclop,funlen // existing issue.
	method string,
	segs []string,
	n int,
) (string, string) {
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
	case nSegsSubRes:
		sub := seg(segs, segSubRes)
		id := seg(segs, segResID)
		switch sub {
		case pathSegIngestions:
			return classifyIngestionPaths(method, segs, n)
		case pathSegRefreshSchedules:
			switch method {
			case http.MethodPost:
				return opCreateRefreshSchedule, id
			case http.MethodGet:
				return opListRefreshSchedules, id
			case http.MethodPut:
				return opUpdateRefreshSchedule, id
			}
		case pathSegRefreshProperties:
			switch method {
			case http.MethodPut:
				return opPutDataSetRefreshProperties, id
			case http.MethodGet:
				return opDescribeDataSetRefreshProps, id
			case http.MethodDelete:
				return opDeleteDataSetRefreshProps, id
			}
		case pathSegPermissions:
			switch method {
			case http.MethodGet:
				return opDescribeDataSetPerms, id
			case http.MethodPost:
				return opUpdateDataSetPerms, id
			}
		}
	case nSegsSubResID:
		sub := seg(segs, segSubRes)
		id := seg(segs, segResID)
		switch sub {
		case pathSegIngestions:
			return classifyIngestionPaths(method, segs, n)
		case pathSegRefreshSchedules:
			switch method {
			case http.MethodGet:
				return opDescribeRefreshSchedule, id
			case http.MethodDelete:
				return opDeleteRefreshSchedule, id
			}
		}
	}

	return opUnknown, ""
}

func classifyDashboardPaths( //nolint:gocognit,gocyclo,cyclop,funlen // existing issue.
	method string,
	segs []string,
	n int,
) (string, string) {
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
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		switch sub {
		case pathSegVersions:
			if method == http.MethodGet {
				return opListDashboardVersions, id
			}
		case pathSegDefinition:
			if method == http.MethodGet {
				return opDescribeDashboardDefinition, id
			}
		case pathSegPermissions:
			switch method {
			case http.MethodGet:
				return opDescribeDashboardPerms, id
			case http.MethodPut:
				return opUpdateDashboardPerms, id
			}
		case pathSegSnapshotJobs:
			switch method { //nolint:gocritic // existing issue.
			case http.MethodPost:
				return opStartDashboardSnapshotJob, id
			}
		case pathSegLinkedEntities:
			if method == http.MethodPut {
				return opUpdateDashboardLinks, id
			}
		case pathSegEmbedUrl:
			if method == http.MethodGet {
				return opGetDashboardEmbedUrl, id
			}
		}
	case nSegsSubResID:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		subID := seg(segs, segSubResID)
		switch sub {
		case pathSegVersions:
			// PUT /accounts/{id}/dashboards/{dashId}/versions/{versionNumber}
			if method == http.MethodPut {
				return opUpdateDashboardPublishedVersion, id
			}
		case pathSegSnapshotJobs:
			// GET /accounts/{id}/dashboards/{dashId}/snapshot-jobs/{jobId}
			if method == http.MethodGet {
				return opDescribeDashboardSnapshotJob, subID
			}
		case pathSegSchedules:
			// POST /accounts/{id}/dashboards/{dashId}/schedules/{scheduleId}
			if method == http.MethodPost {
				return opStartDashboardSnapshotJobSchedule, id
			}
		}
	case nSegsSubSubRes:
		// /accounts/{id}/dashboards/{dashId}/snapshot-jobs/{jobId}/result
		if seg(segs, segSubRes) == pathSegSnapshotJobs && seg(segs, segSubSubRes) == pathSegResult &&
			method == http.MethodGet {
			return opDescribeDashboardSnapshotJobResult, seg(segs, segSubResID)
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
	case nSegsSubRes:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		switch sub {
		case pathSegDefinition:
			if method == http.MethodGet {
				return opDescribeAnalysisDefinition, id
			}
		case pathSegPermissions:
			switch method {
			case http.MethodGet:
				return opDescribeAnalysisPerms, id
			case http.MethodPut:
				return opUpdateAnalysisPerms, id
			}
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
		return opSearchAnalyses, ""
	case pathSegDashboards:
		return opSearchDashboards, ""
	case pathSegDataSets:
		return opSearchDataSets, ""
	case pathSegDataSources:
		return opSearchDataSources, ""
	case pathSegFolders:
		return opSearchFolders, ""
	case pathSegActionConnectors:
		return opSearchActionConnectors, ""
	case pathSegTopics:
		return opSearchTopics, ""
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
