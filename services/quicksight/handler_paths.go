package quicksight

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

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

// resourceTypeClassifier classifies a path already known to start with
// /accounts/{accountId}/{resourceType}/... given the full segment list and
// its length.
type resourceTypeClassifier func(method string, segs []string, n int) (string, string)

// resourceTypeDispatchTable lazily builds the resourceType -> classifier
// lookup used by classifyRequest exactly once, keeping classifyRequest
// itself a small, flat sequence of special-case prefix checks followed by a
// single map lookup. Mirrors the onceOpTable pattern in
// services/apigatewayv2/handler.go. The handful of one-line cases from the
// original flat switch (pathSegPublicSharing, pathSegSPICECapacity, etc.)
// are wrapped as small closures so every entry shares the same signature.
//
//nolint:gochecknoglobals // read-only package-level lookup table, built once via sync.OnceValue
var resourceTypeDispatchTable = sync.OnceValue(func() map[string]resourceTypeClassifier {
	return map[string]resourceTypeClassifier{
		pathSegNamespaces: classifyNamespacePaths,
		// DELETE /accounts/{id}/namespace/{ns}/iam-policy-assignments/{name}
		pathSegNamespaceSingular:   classifyNamespaceSingularPaths,
		pathSegDataSources:         classifyDataSourcePaths,
		pathSegDataSets:            classifyDataSetPaths,
		pathSegDashboards:          classifyDashboardPaths,
		pathSegAnalyses:            classifyAnalysisPaths,
		pathSegSearch:              classifySearchPaths,
		pathSegRestore:             classifyRestorePaths,
		pathSegFolders:             classifyFolderPaths,
		pathSegTemplates:           classifyTemplatePaths,
		pathSegThemes:              classifyThemePaths,
		pathSegTopics:              classifyTopicPaths,
		pathSegVPCConnections:      classifyVPCConnectionPaths,
		pathSegActionConnectors:    classifyActionConnectorPaths,
		pathSegBrands:              classifyBrandPaths,
		pathSegBrandAssignments:    classifyBrandAssignmentPaths,
		pathSegCustomPermissions:   classifyCustomPermissionsPaths,
		pathSegOAuthApps:           classifyOAuthAppPaths,
		pathSegIdentityPropagation: classifyIdentityPropagationPaths,
		pathSegAssetBundleExport:   classifyAssetBundleExportPaths,
		pathSegAssetBundleImport:   classifyAssetBundleImportPaths,
		pathSegAutomationGroups:    classifyAutomationPaths,
		pathSegFlows:               classifyFlowPaths,
		pathSegResource2:           classifyResourceFoldersPaths,
		pathSegCustomizations:      classifyCustomizationPaths,
		pathSegCustomPermission:    classifyAccountCustomPermissionPaths,
		pathSegSettings:            classifyAccountSettingsPaths,
		pathSegDashboardsQACfg:     classifyDashboardsQAPaths,
		pathSegDefaultQBiz:         classifyDefaultQBizPaths,
		pathSegIPRestriction:       classifyIPRestrictionPaths,
		pathSegKeyRegistration:     classifyKeyRegistrationPaths,
		pathSegQPersonalization:    classifyQPersonalizationPaths,
		pathSegQSearchConfig:       classifyQSearchConfigPaths,
		pathSegEmbedUrl:            classifyEmbedURLPaths,
		pathSegPublicSharing:       classifySingleOpPut(opUpdatePublicSharingSettings),
		pathSegSPICECapacity:       classifySingleOpPost(opUpdateSPICECapacity),
		pathSegSessionEmbedUrl:     classifySingleOpGet(opGetSessionEmbedUrl),
		pathSegIdentityContext:     classifySingleOpPost(opGetIdentityContext),
		pathSegAppTokenGrant:       classifySingleOpPut(opUpdateAppTokenGrant),
		pathSegQA:                  classifyQAPaths,
	}
})

// classifySingleOpGet/Post/Put build a resourceTypeClassifier for a
// resource type that only supports one method, on the account itself
// (segAccountID), with no further path structure to inspect.
func classifySingleOpGet(op string) resourceTypeClassifier {
	return func(method string, segs []string, _ int) (string, string) {
		if method == http.MethodGet {
			return op, seg(segs, segAccountID)
		}

		return opUnknown, ""
	}
}

func classifySingleOpPost(op string) resourceTypeClassifier {
	return func(method string, segs []string, _ int) (string, string) {
		if method == http.MethodPost {
			return op, seg(segs, segAccountID)
		}

		return opUnknown, ""
	}
}

func classifySingleOpPut(op string) resourceTypeClassifier {
	return func(method string, segs []string, _ int) (string, string) {
		if method == http.MethodPut {
			return op, seg(segs, segAccountID)
		}

		return opUnknown, ""
	}
}

// classifyQAPaths classifies POST /accounts/{id}/qa/predict (n ==
// nSegsAccountResID: accounts, {id}, qa, predict).
func classifyQAPaths(method string, segs []string, n int) (string, string) {
	if n >= nSegsAccountResID && seg(segs, segResID) == pathSegPredict && method == http.MethodPost {
		return opPredictQAResults, seg(segs, segAccountID)
	}

	return opUnknown, ""
}

func classifyRequest(method, path string) (string, string) {
	segs := pathSegs(path)
	n := len(segs)

	// /resources/{arn}/tags — tag operations
	if n >= nSegsAccountRes && segs[0] == pathSegResources && segs[n-1] == pathSegTagsSuffix {
		return classifyTagResourcePaths(method, segs, n)
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

	classify, ok := resourceTypeDispatchTable()[seg(segs, segResource)]
	if !ok {
		return opUnknown, ""
	}

	return classify(method, segs, n)
}

// classifyTagResourcePaths classifies /resources/{arn}/tags.
func classifyTagResourcePaths(method string, segs []string, n int) (string, string) {
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

// classifyNsWithSubRes classifies a 4-segment /accounts/{id}/namespaces/{ns}/{sub}
// path. Every matched case returns ns itself as the resource id, so each
// per-sub helper only needs to decide the op name from method.
func classifyNsWithSubRes(method string, segs []string) (string, string) {
	ns := seg(segs, segResID)
	sub := seg(segs, segSubRes)

	var op string
	switch sub {
	case pathSegGroups:
		op = classifyNsGroupsCollection(method)
	case pathSegUsers:
		op = classifyNsUsersCollection(method)
	case pathSegGroupsSearch:
		op = classifyNsGroupsSearch(method)
	case pathSegIAMPolicyAssignments:
		op = classifyNsIAMPolicyCollection(method)
	case pathSegSelfUpgradeCfg:
		op = classifyNsSelfUpgradeConfig(method)
	case pathSegSelfUpgradeReqs:
		op = classifyNsSelfUpgradeRequests(method)
	case pathSegUpdateSelfUpgrade:
		op = classifyNsUpdateSelfUpgrade(method)
	default:
		op = opUnknown
	}

	if op == opUnknown {
		return opUnknown, ""
	}

	return op, ns
}

func classifyNsGroupsCollection(method string) string {
	switch method {
	case http.MethodPost:
		return opCreateGroup
	case http.MethodGet:
		return opListGroups
	default:
		return opUnknown
	}
}

func classifyNsUsersCollection(method string) string {
	switch method {
	case http.MethodPost:
		return opRegisterUser
	case http.MethodGet:
		return opListUsers
	default:
		return opUnknown
	}
}

func classifyNsGroupsSearch(method string) string {
	if method == http.MethodPost {
		return opSearchGroups
	}

	return opUnknown
}

func classifyNsIAMPolicyCollection(method string) string {
	switch method {
	case http.MethodPost:
		return opCreateIAMPolicyAssignment
	case http.MethodGet:
		return opListIAMPolicyAssignments
	default:
		return opUnknown
	}
}

func classifyNsSelfUpgradeConfig(method string) string {
	switch method {
	case http.MethodGet:
		return opDescribeSelfUpgradeConfig
	case http.MethodPut:
		return opUpdateSelfUpgradeConfig
	default:
		return opUnknown
	}
}

func classifyNsSelfUpgradeRequests(method string) string {
	if method == http.MethodGet {
		return opListSelfUpgrades
	}

	return opUnknown
}

func classifyNsUpdateSelfUpgrade(method string) string {
	if method == http.MethodPost {
		return opUpdateSelfUpgrade
	}

	return opUnknown
}

// classifyNsWithSubResID classifies a 5-segment
// /accounts/{id}/namespaces/{ns}/{sub}/{subResID} path. Most cases return
// segSubResID as the resource id; pathSegUserPrincipals and pathSegV2 are the
// two exceptions (they identify the resource by segResID instead), so those
// stay inlined here rather than going through a per-sub helper.
func classifyNsWithSubResID(method string, segs []string) (string, string) {
	sub := seg(segs, segSubRes)
	id := seg(segs, segSubResID)

	switch sub {
	case pathSegGroups:
		if op := classifyNsGroupByID(method); op != opUnknown {
			return op, id
		}
	case pathSegUsers:
		if op := classifyNsUserByID(method); op != opUnknown {
			return op, id
		}
	case pathSegUserPrincipals:
		if method == http.MethodDelete {
			return opDeleteUserByPrincipalID, seg(segs, segResID)
		}
	case pathSegIAMPolicyAssignments:
		// namespaces/{ns}/iam-policy-assignments/{name}
		if op := classifyNsIAMPolicyByID(method); op != opUnknown {
			return op, id
		}
	case pathSegV2:
		// namespaces/{ns}/v2/iam-policy-assignments
		if id == pathSegIAMPolicyAssignments && method == http.MethodGet {
			return opListIAMPolicyAssignments, seg(segs, segResID)
		}
	}

	return opUnknown, ""
}

func classifyNsGroupByID(method string) string {
	switch method {
	case http.MethodGet:
		return opDescribeGroup
	case http.MethodPut:
		return opUpdateGroup
	case http.MethodDelete:
		return opDeleteGroup
	default:
		return opUnknown
	}
}

func classifyNsUserByID(method string) string {
	switch method {
	case http.MethodGet:
		return opDescribeUser
	case http.MethodPut:
		return opUpdateUser
	case http.MethodDelete:
		return opDeleteUser
	default:
		return opUnknown
	}
}

func classifyNsIAMPolicyByID(method string) string {
	switch method {
	case http.MethodGet:
		return opDescribeIAMPolicyAssignment
	case http.MethodPut:
		return opUpdateIAMPolicyAssignment
	default:
		return opUnknown
	}
}

// nsSubSubResKey identifies a (sub, tail) pair for a 6-segment
// /accounts/{id}/namespaces/{ns}/{sub}/{subResID}/{tail} path.
type nsSubSubResKey struct {
	sub  string
	tail string
}

// nsSubSubResTable lazily builds the (sub, tail) -> per-method-op lookup for
// classifyNsWithSubSubRes exactly once. Every case in the original flat
// switch returned segSubResID as the resource id, so the table only needs to
// resolve the op name; the id is applied uniformly by the caller. Mirrors the
// onceOpTable pattern in services/apigatewayv2/handler.go.
//
//nolint:gochecknoglobals // read-only package-level lookup table, built once via sync.OnceValue
var nsSubSubResTable = sync.OnceValue(func() map[nsSubSubResKey]func(string) string {
	return map[nsSubSubResKey]func(string) string{
		{sub: pathSegGroups, tail: pathSegMembers}: func(method string) string {
			if method == http.MethodGet {
				return opListGroupMemberships
			}

			return opUnknown
		},
		{sub: pathSegUsers, tail: pathSegGroups}: func(method string) string {
			if method == http.MethodGet {
				return opListUserGroups
			}

			return opUnknown
		},
		{sub: pathSegUsers, tail: pathSegIAMPolicyAssignments}: func(method string) string {
			if method == http.MethodGet {
				return opListIAMPolicyAssignmentsForUser
			}

			return opUnknown
		},
		{sub: pathSegUsers, tail: pathSegCustomPermission}: func(method string) string {
			switch method {
			case http.MethodPut:
				return opUpdateUserCustomPermission
			case http.MethodDelete:
				return opDeleteUserCustomPermission
			default:
				return opUnknown
			}
		},
		{sub: pathSegRoles, tail: pathSegCustomPermission}: func(method string) string {
			switch method {
			case http.MethodGet:
				return opGetRoleCustomPermission
			case http.MethodPut:
				return opUpdateRoleCustomPermission
			case http.MethodDelete:
				return opDeleteRoleCustomPermission
			default:
				return opUnknown
			}
		},
		{sub: pathSegRoles, tail: pathSegMembers}: func(method string) string {
			if method == http.MethodGet {
				return opListRoleMemberships
			}

			return opUnknown
		},
	}
})

func classifyNsWithSubSubRes(method string, segs []string) (string, string) {
	sub := seg(segs, segSubRes)
	id := seg(segs, segSubResID)
	tail := seg(segs, segSubSubRes)

	resolve, ok := nsSubSubResTable()[nsSubSubResKey{sub: sub, tail: tail}]
	if !ok {
		return opUnknown, ""
	}

	op := resolve(method)
	if op == opUnknown {
		return opUnknown, ""
	}

	return op, id
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

func classifyDataSetPaths(method string, segs []string, n int) (string, string) {
	switch n {
	case nSegsAccountRes:
		return classifyDataSetRoot(method, segs)
	case nSegsAccountResID:
		return classifyDataSetByID(method, segs)
	case nSegsSubRes:
		return classifyDataSetSubRes(method, segs, n)
	case nSegsSubResID:
		return classifyDataSetSubResID(method, segs, n)
	}

	return opUnknown, ""
}

func classifyDataSetRoot(method string, segs []string) (string, string) {
	switch method {
	case http.MethodPost:
		return opCreateDataSet, seg(segs, segAccountID)
	case http.MethodGet:
		return opListDataSets, seg(segs, segAccountID)
	}

	return opUnknown, ""
}

func classifyDataSetByID(method string, segs []string) (string, string) {
	id := seg(segs, segResID)
	switch method {
	case http.MethodGet:
		return opDescribeDataSet, id
	case http.MethodPut:
		return opUpdateDataSet, id
	case http.MethodDelete:
		return opDeleteDataSet, id
	}

	return opUnknown, ""
}

func classifyDataSetSubRes(method string, segs []string, n int) (string, string) {
	sub := seg(segs, segSubRes)
	id := seg(segs, segResID)

	switch sub {
	case pathSegIngestions:
		return classifyIngestionPaths(method, segs, n)
	case pathSegRefreshSchedules:
		return classifyDataSetRefreshSchedulesCollection(method, id)
	case pathSegRefreshProperties:
		return classifyDataSetRefreshProperties(method, id)
	case pathSegPermissions:
		return classifyDataSetPermissions(method, id)
	}

	return opUnknown, ""
}

func classifyDataSetRefreshSchedulesCollection(method, id string) (string, string) {
	switch method {
	case http.MethodPost:
		return opCreateRefreshSchedule, id
	case http.MethodGet:
		return opListRefreshSchedules, id
	case http.MethodPut:
		return opUpdateRefreshSchedule, id
	}

	return opUnknown, ""
}

func classifyDataSetRefreshProperties(method, id string) (string, string) {
	switch method {
	case http.MethodPut:
		return opPutDataSetRefreshProperties, id
	case http.MethodGet:
		return opDescribeDataSetRefreshProps, id
	case http.MethodDelete:
		return opDeleteDataSetRefreshProps, id
	}

	return opUnknown, ""
}

func classifyDataSetPermissions(method, id string) (string, string) {
	switch method {
	case http.MethodGet:
		return opDescribeDataSetPerms, id
	case http.MethodPost:
		return opUpdateDataSetPerms, id
	}

	return opUnknown, ""
}

func classifyDataSetSubResID(method string, segs []string, n int) (string, string) {
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

	return opUnknown, ""
}

func classifyDashboardPaths(method string, segs []string, n int) (string, string) {
	switch n {
	case nSegsAccountRes:
		if method == http.MethodGet {
			return opListDashboards, seg(segs, segAccountID)
		}
	case nSegsAccountResID:
		return classifyDashboardByID(method, segs)
	case nSegsSubRes:
		return classifyDashboardSubRes(method, segs)
	case nSegsSubResID:
		return classifyDashboardSubResID(method, segs)
	case nSegsSubSubRes:
		return classifyDashboardSubSubRes(method, segs)
	}

	return opUnknown, ""
}

func classifyDashboardByID(method string, segs []string) (string, string) {
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

	return opUnknown, ""
}

func classifyDashboardSubRes(method string, segs []string) (string, string) {
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
		if method == http.MethodPost {
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

	return opUnknown, ""
}

func classifyDashboardSubResID(method string, segs []string) (string, string) {
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

	return opUnknown, ""
}

// classifyDashboardSubSubRes classifies
// /accounts/{id}/dashboards/{dashId}/snapshot-jobs/{jobId}/result.
func classifyDashboardSubSubRes(method string, segs []string) (string, string) {
	if seg(segs, segSubRes) == pathSegSnapshotJobs && seg(segs, segSubSubRes) == pathSegResult &&
		method == http.MethodGet {
		return opDescribeDashboardSnapshotJobResult, seg(segs, segSubResID)
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
	case errors.Is(err, awserr.ErrConflict):
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
