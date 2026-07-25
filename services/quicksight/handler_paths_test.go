package quicksight_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
)

// extractOp builds a signed request for method+path and returns the
// (operation, resource) pair the Handler's route classifier extracts for it.
// Exercises the same classifyRequest path handler_dispatch.go's dispatch
// loop uses, without going through a full doRequest round-trip.
func extractOp(t *testing.T, method, path string) (string, string) {
	t.Helper()

	h := newTestHandler(t)
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/quicksight/aws4_request")
	e := echo.New()
	c := e.NewContext(req, httptest.NewRecorder())

	return h.ExtractOperation(c), h.ExtractResource(c)
}

// TestQuickSight_RouteMap locks classifyRequest's op/resource extraction
// across every resource family after its decomposition from a handful of
// giant flat switches (classifyRequest itself, classifyDataSetPaths,
// classifyDashboardPaths, classifyFolderPaths, classifyTemplatePaths,
// classifyThemePaths, classifyTopicPaths, and the namespace sub-resource
// classifiers) into per-family/per-segment-depth helper functions plus two
// sync.OnceValue lookup tables (resourceTypeDispatchTable,
// nsSubSubResTable). One representative path per case is enough to catch a
// transcription error in the refactor (wrong op constant, wrong id source,
// case dropped) without re-deriving the whole path grammar.
func TestQuickSight_RouteMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantOp     string
		wantResIDs []string // any one of these is acceptable
	}{
		{"CreateNamespace", http.MethodPost, "/accounts/1", "CreateNamespace", []string{"1"}},
		{"ListNamespaces", http.MethodGet, "/accounts/1/namespaces", "ListNamespaces", []string{""}},
		{"DescribeNamespace", http.MethodGet, "/accounts/1/namespaces/ns1", "DescribeNamespace", []string{"ns1"}},
		{"CreateGroup", http.MethodPost, "/accounts/1/namespaces/ns1/groups", "CreateGroup", []string{"ns1"}},
		{"DescribeGroup", http.MethodGet, "/accounts/1/namespaces/ns1/groups/g1", "DescribeGroup", []string{"g1"}},
		{
			"ListGroupMemberships", http.MethodGet, "/accounts/1/namespaces/ns1/groups/g1/members",
			"ListGroupMemberships", []string{"g1"},
		},
		{
			"DeleteUserByPrincipalId", http.MethodDelete, "/accounts/1/namespaces/ns1/user-principals/p1",
			"DeleteUserByPrincipalId", []string{"ns1"},
		},
		{
			"ListIAMPolicyAssignments(v2)", http.MethodGet,
			"/accounts/1/namespaces/ns1/v2/iam-policy-assignments", "ListIAMPolicyAssignments", []string{"ns1"},
		},
		{
			"UpdateSelfUpgradeConfig", http.MethodPut, "/accounts/1/namespaces/ns1/self-upgrade-configuration",
			"UpdateSelfUpgradeConfiguration", []string{"ns1"},
		},
		{"CreateDataSet", http.MethodPost, "/accounts/1/data-sets", "CreateDataSet", []string{"1"}},
		{"UpdateDataSet", http.MethodPut, "/accounts/1/data-sets/ds1", "UpdateDataSet", []string{"ds1"}},
		{
			"CreateRefreshSchedule", http.MethodPost, "/accounts/1/data-sets/ds1/refresh-schedules",
			"CreateRefreshSchedule", []string{"ds1"},
		},
		{
			"DescribeRefreshSchedule", http.MethodGet, "/accounts/1/data-sets/ds1/refresh-schedules/sched1",
			"DescribeRefreshSchedule", []string{"ds1"},
		},
		{
			"CreateIngestion", http.MethodPut, "/accounts/1/data-sets/ds1/ingestions/ing1",
			"CreateIngestion", []string{"ds1", "ing1"},
		},
		{"CreateDashboard", http.MethodPost, "/accounts/1/dashboards/d1", "CreateDashboard", []string{"d1"}},
		{
			"ListDashboardVersions", http.MethodGet, "/accounts/1/dashboards/d1/versions",
			"ListDashboardVersions", []string{"d1"},
		},
		{
			"UpdateDashboardPublishedVersion", http.MethodPut, "/accounts/1/dashboards/d1/versions/2",
			"UpdateDashboardPublishedVersion", []string{"d1"},
		},
		{
			"StartDashboardSnapshotJob", http.MethodPost, "/accounts/1/dashboards/d1/snapshot-jobs",
			"StartDashboardSnapshotJob", []string{"d1"},
		},
		{
			"DescribeDashboardSnapshotJob", http.MethodGet, "/accounts/1/dashboards/d1/snapshot-jobs/job1",
			"DescribeDashboardSnapshotJob", []string{"job1"},
		},
		{
			"DescribeDashboardSnapshotJobResult", http.MethodGet,
			"/accounts/1/dashboards/d1/snapshot-jobs/job1/result",
			"DescribeDashboardSnapshotJobResult", []string{"job1"},
		},
		{"CreateAnalysis", http.MethodPost, "/accounts/1/analyses/a1", "CreateAnalysis", []string{"a1"}},
		{"CreateFolder", http.MethodPost, "/accounts/1/folders/f1", "CreateFolder", []string{"f1"}},
		{
			"CreateFolderMembership", http.MethodPut, "/accounts/1/folders/f1/members/dataset/ds1",
			"CreateFolderMembership", []string{"f1"},
		},
		{"CreateTemplate", http.MethodPost, "/accounts/1/templates/t1", "CreateTemplate", []string{"t1"}},
		{
			"CreateTemplateAlias", http.MethodPost, "/accounts/1/templates/t1/aliases/latest",
			"CreateTemplateAlias", []string{"latest"},
		},
		{
			// DeleteTemplateAlias is the one alias op that identifies the
			// resource by the template id, not the alias name -- see
			// classifyTemplateAlias's doc comment.
			"DeleteTemplateAlias", http.MethodDelete, "/accounts/1/templates/t1/aliases/latest",
			"DeleteTemplateAlias", []string{"t1"},
		},
		{"CreateTheme", http.MethodPost, "/accounts/1/themes/th1", "CreateTheme", []string{"th1"}},
		{
			"CreateThemeAlias", http.MethodPost, "/accounts/1/themes/th1/aliases/latest",
			"CreateThemeAlias", []string{"latest"},
		},
		{
			// Mirrors DeleteTemplateAlias -- see classifyThemeAlias's doc comment.
			"DeleteThemeAlias", http.MethodDelete, "/accounts/1/themes/th1/aliases/latest",
			"DeleteThemeAlias", []string{"th1"},
		},
		{"CreateTopic", http.MethodPost, "/accounts/1/topics", "CreateTopic", []string{"1"}},
		{
			"CreateTopicRefreshSchedule", http.MethodPost, "/accounts/1/topics/tp1/schedules",
			"CreateTopicRefreshSchedule", []string{"tp1"},
		},
		{
			"DescribeTopicRefreshSchedule", http.MethodGet, "/accounts/1/topics/tp1/schedules/sched1",
			"DescribeTopicRefreshSchedule", []string{"tp1"},
		},
		{
			"DescribeTopicRefresh", http.MethodGet, "/accounts/1/topics/tp1/refresh/job1",
			"DescribeTopicRefresh", []string{"tp1"},
		},
		{
			"BatchCreateTopicReviewedAnswer", http.MethodPost, "/accounts/1/topics/tp1/batch-create-reviewed-answers",
			"BatchCreateTopicReviewedAnswer", []string{"tp1"},
		},
		{"CreateVPCConnection", http.MethodPost, "/accounts/1/vpc-connections", "CreateVPCConnection", []string{"1"}},
		{
			"TagResource", http.MethodPost, "/resources/arn:aws:quicksight:us-east-1:1:dashboard/d1/tags",
			"TagResource", []string{"arn:aws:quicksight:us-east-1:1:dashboard/d1"},
		},
		{"DescribeAccountSubscription", http.MethodGet, "/account/1", "DescribeAccountSubscription", []string{"1"}},
		{
			"UpdatePublicSharingSettings", http.MethodPut, "/accounts/1/public-sharing-settings",
			"UpdatePublicSharingSettings", []string{"1"},
		},
		{
			"UpdateSPICECapacity", http.MethodPost, "/accounts/1/spice-capacity-configuration",
			"UpdateSPICECapacityConfiguration", []string{"1"},
		},
		{"GetSessionEmbedUrl", http.MethodGet, "/accounts/1/session-embed-url", "GetSessionEmbedUrl", []string{"1"}},
		{"GetIdentityContext", http.MethodPost, "/accounts/1/identity-context", "GetIdentityContext", []string{"1"}},
		{
			"UpdateApplicationWithTokenExchangeGrant", http.MethodPut,
			"/accounts/1/application-with-token-exchange-grant",
			"UpdateApplicationWithTokenExchangeGrant", []string{"1"},
		},
		{"PredictQAResults", http.MethodPost, "/accounts/1/qa/predict", "PredictQAResults", []string{"1"}},
		{"Unknown method", http.MethodPatch, "/accounts/1/data-sets", "Unknown", []string{""}},
		{"Unknown path", http.MethodGet, "/nonsense", "Unknown", []string{""}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			op, resource := extractOp(t, tc.method, tc.path)
			assert.Equal(t, tc.wantOp, op, "op for %s %s", tc.method, tc.path)
			assert.Contains(t, tc.wantResIDs, resource, "resource for %s %s", tc.method, tc.path)
		})
	}
}
