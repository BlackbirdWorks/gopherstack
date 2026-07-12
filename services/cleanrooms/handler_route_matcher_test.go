package cleanrooms_test

// This file exercises Handler.RouteMatcher and Handler.ExtractOperation
// directly -- the two entry points a real echo router uses to decide (a)
// whether a request belongs to this service at all, and (b) which operation
// it maps to.
//
// Every other test in this package drives requests through newTestServer,
// which registers h.Handler() on "/*" directly and so never calls
// RouteMatcher at all. That means a routing bug -- an op registered for the
// wrong HTTP method, or a path prefix RouteMatcher doesn't recognize -- can
// hide behind 100% green unit tests while being completely unreachable by a
// real aws-sdk-go-v2 client going through echo's router alongside every
// other service's RouteMatcher/MatchPriority. This exact bug class (a whole
// op family silently unroutable) previously hit services/backup, eks,
// s3control, and guardduty.
//
// The (method, path) pairs below are transcribed from
// aws-sdk-go-v2/service/cleanrooms@.../serializers.go's httpBindings for
// every operation (restjson1 protocol), not from this package's own
// classifyPath -- so a divergence between the two is a real bug, not a
// tautology.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

// testAnalysisTemplateArn is a representative GetCollaborationAnalysisTemplate
// path parameter: an ARN with two "/" characters embedded in its resource
// part (arn:...:membership/{id}/analysistemplate/{id}).
const testAnalysisTemplateArn = "arn:aws:cleanrooms:us-east-1:1234:membership/m1/analysistemplate/t1"

// routeMatcherContext builds a bare *echo.Context for method+path, with no
// body, matching the minimal input RouteMatcher/ExtractOperation need.
func routeMatcherContext(t *testing.T, method, path string) *echo.Context {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, http.NoBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	return c
}

// TestRouteMatcher_RecognizesServicePaths checks that RouteMatcher accepts
// every top-level path prefix a real Clean Rooms client can hit and rejects
// paths belonging to other services.
func TestRouteMatcher_RecognizesServicePaths(t *testing.T) {
	t.Parallel()

	h := cleanrooms.NewHandler(cleanrooms.NewInMemoryBackend("123456789012", "us-east-1"))

	tests := []struct {
		method string
		path   string
		name   string
		want   bool
	}{
		{name: "collaborations root", method: http.MethodPost, path: "/collaborations", want: true},
		{
			name:   "collaborations nested",
			method: http.MethodGet,
			path:   "/collaborations/c1/members",
			want:   true,
		},
		{
			name:   "configuredTables root",
			method: http.MethodPost,
			path:   "/configuredTables",
			want:   true,
		},
		{
			name:   "configuredTables nested",
			method: http.MethodGet,
			path:   "/configuredTables/t1/analysisRule",
			want:   true,
		},
		{name: "memberships root", method: http.MethodPost, path: "/memberships", want: true},
		{
			name:   "memberships nested",
			method: http.MethodGet,
			path:   "/memberships/m1/protectedQueries/q1",
			want:   true,
		},
		{
			name:   "tags for a resource",
			method: http.MethodGet,
			path:   "/tags/arn:aws:cleanrooms:us-east-1:123456789012:collaboration/c1",
			want:   true,
		},
		{
			name:   "unrelated service path",
			method: http.MethodGet,
			path:   "/2015-03-31/functions",
			want:   false,
		},
		{name: "unrelated root", method: http.MethodPost, path: "/buckets", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := routeMatcherContext(t, tt.method, tt.path)
			assert.Equal(t, tt.want, h.RouteMatcher()(c), "path %s %s", tt.method, tt.path)
		})
	}
}

// TestRouteMatcher_MethodSensitivity walks (method, path) -> expected
// operation for every operation family, the way a real echo router +
// ExtractOperation would see an incoming aws-sdk-go-v2 request. Each path is
// transcribed verbatim from the SDK's restjson1 httpBindings requestURI.
func TestRouteMatcher_MethodSensitivity(t *testing.T) {
	t.Parallel()

	h := cleanrooms.NewHandler(cleanrooms.NewInMemoryBackend("123456789012", "us-east-1"))

	tests := []struct {
		method string
		path   string
		wantOp string
		name   string
	}{
		// ---- Collaboration ----
		{
			name:   "CreateCollaboration",
			method: http.MethodPost,
			path:   "/collaborations",
			wantOp: "CreateCollaboration",
		},
		{
			name:   "ListCollaborations",
			method: http.MethodGet,
			path:   "/collaborations",
			wantOp: "ListCollaborations",
		},
		{
			name:   "GetCollaboration",
			method: http.MethodGet,
			path:   "/collaborations/c1",
			wantOp: "GetCollaboration",
		},
		{
			name:   "DeleteCollaboration",
			method: http.MethodDelete,
			path:   "/collaborations/c1",
			wantOp: "DeleteCollaboration",
		},
		{
			name:   "UpdateCollaboration",
			method: http.MethodPatch,
			path:   "/collaborations/c1",
			wantOp: "UpdateCollaboration",
		},
		{
			name:   "ListMembers",
			method: http.MethodGet,
			path:   "/collaborations/c1/members",
			wantOp: "ListMembers",
		},
		{
			name:   "DeleteMember",
			method: http.MethodDelete,
			path:   "/collaborations/c1/member/222222222222",
			wantOp: "DeleteMember",
		},

		// ---- Collaboration sub-resources (read-only views + batch) ----
		{
			name:   "ListCollaborationAnalysisTemplates",
			method: http.MethodGet,
			path:   "/collaborations/c1/analysistemplates",
			wantOp: "ListCollaborationAnalysisTemplates",
		},
		{
			name:   "GetCollaborationAnalysisTemplate",
			method: http.MethodGet,
			path:   "/collaborations/c1/analysistemplates/" + testAnalysisTemplateArn,
			wantOp: "GetCollaborationAnalysisTemplate",
		},
		{
			name:   "BatchGetCollaborationAnalysisTemplate",
			method: http.MethodPost,
			path:   "/collaborations/c1/batch-analysistemplates",
			wantOp: "BatchGetCollaborationAnalysisTemplate",
		},
		{
			name:   "BatchGetSchema",
			method: http.MethodPost,
			path:   "/collaborations/c1/batch-schema",
			wantOp: "BatchGetSchema",
		},
		{
			name:   "BatchGetSchemaAnalysisRule",
			method: http.MethodPost,
			path:   "/collaborations/c1/batch-schema-analysis-rule",
			wantOp: "BatchGetSchemaAnalysisRule",
		},
		{
			name:   "CreateCollaborationChangeRequest",
			method: http.MethodPost,
			path:   "/collaborations/c1/changeRequests",
			wantOp: "CreateCollaborationChangeRequest",
		},
		{
			name:   "ListCollaborationChangeRequests",
			method: http.MethodGet,
			path:   "/collaborations/c1/changeRequests",
			wantOp: "ListCollaborationChangeRequests",
		},
		{
			name:   "GetCollaborationChangeRequest",
			method: http.MethodGet,
			path:   "/collaborations/c1/changeRequests/cr1",
			wantOp: "GetCollaborationChangeRequest",
		},
		{
			name:   "UpdateCollaborationChangeRequest",
			method: http.MethodPatch,
			path:   "/collaborations/c1/changeRequests/cr1",
			wantOp: "UpdateCollaborationChangeRequest",
		},
		{
			name:   "ListCollaborationConfiguredAudienceModelAssociations",
			method: http.MethodGet,
			path:   "/collaborations/c1/configuredaudiencemodelassociations",
			wantOp: "ListCollaborationConfiguredAudienceModelAssociations",
		},
		{
			name:   "GetCollaborationConfiguredAudienceModelAssociation",
			method: http.MethodGet,
			path:   "/collaborations/c1/configuredaudiencemodelassociations/a1",
			wantOp: "GetCollaborationConfiguredAudienceModelAssociation",
		},
		{
			name:   "ListCollaborationIdNamespaceAssociations",
			method: http.MethodGet,
			path:   "/collaborations/c1/idnamespaceassociations",
			wantOp: "ListCollaborationIdNamespaceAssociations",
		},
		{
			name:   "GetCollaborationIdNamespaceAssociation",
			method: http.MethodGet,
			path:   "/collaborations/c1/idnamespaceassociations/a1",
			wantOp: "GetCollaborationIdNamespaceAssociation",
		},
		{
			name:   "ListCollaborationPrivacyBudgets",
			method: http.MethodGet,
			path:   "/collaborations/c1/privacybudgets",
			wantOp: "ListCollaborationPrivacyBudgets",
		},
		{
			name:   "ListCollaborationPrivacyBudgetTemplates",
			method: http.MethodGet,
			path:   "/collaborations/c1/privacybudgettemplates",
			wantOp: "ListCollaborationPrivacyBudgetTemplates",
		},
		{
			name:   "GetCollaborationPrivacyBudgetTemplate",
			method: http.MethodGet,
			path:   "/collaborations/c1/privacybudgettemplates/pt1",
			wantOp: "GetCollaborationPrivacyBudgetTemplate",
		},
		{
			name:   "ListSchemas",
			method: http.MethodGet,
			path:   "/collaborations/c1/schemas",
			wantOp: "ListSchemas",
		},
		{
			name:   "GetSchema",
			method: http.MethodGet,
			path:   "/collaborations/c1/schemas/s1",
			wantOp: "GetSchema",
		},
		{
			name:   "GetSchemaAnalysisRule",
			method: http.MethodGet,
			path:   "/collaborations/c1/schemas/s1/analysisRule/AGGREGATION",
			wantOp: "GetSchemaAnalysisRule",
		},

		// ---- ConfiguredTable ----
		{
			name:   "CreateConfiguredTable",
			method: http.MethodPost,
			path:   "/configuredTables",
			wantOp: "CreateConfiguredTable",
		},
		{
			name:   "ListConfiguredTables",
			method: http.MethodGet,
			path:   "/configuredTables",
			wantOp: "ListConfiguredTables",
		},
		{
			name:   "GetConfiguredTable",
			method: http.MethodGet,
			path:   "/configuredTables/t1",
			wantOp: "GetConfiguredTable",
		},
		{
			name:   "DeleteConfiguredTable",
			method: http.MethodDelete,
			path:   "/configuredTables/t1",
			wantOp: "DeleteConfiguredTable",
		},
		{
			name:   "UpdateConfiguredTable",
			method: http.MethodPatch,
			path:   "/configuredTables/t1",
			wantOp: "UpdateConfiguredTable",
		},
		{
			name:   "CreateConfiguredTableAnalysisRule",
			method: http.MethodPost,
			path:   "/configuredTables/t1/analysisRule",
			wantOp: "CreateConfiguredTableAnalysisRule",
		},
		{
			name:   "GetConfiguredTableAnalysisRule",
			method: http.MethodGet,
			path:   "/configuredTables/t1/analysisRule/AGGREGATION",
			wantOp: "GetConfiguredTableAnalysisRule",
		},
		{
			name:   "DeleteConfiguredTableAnalysisRule",
			method: http.MethodDelete,
			path:   "/configuredTables/t1/analysisRule/AGGREGATION",
			wantOp: "DeleteConfiguredTableAnalysisRule",
		},
		{
			name:   "UpdateConfiguredTableAnalysisRule",
			method: http.MethodPatch,
			path:   "/configuredTables/t1/analysisRule/AGGREGATION",
			wantOp: "UpdateConfiguredTableAnalysisRule",
		},

		// ---- Membership ----
		{
			name:   "CreateMembership",
			method: http.MethodPost,
			path:   "/memberships",
			wantOp: "CreateMembership",
		},
		{
			name:   "ListMemberships",
			method: http.MethodGet,
			path:   "/memberships",
			wantOp: "ListMemberships",
		},
		{
			name:   "GetMembership",
			method: http.MethodGet,
			path:   "/memberships/m1",
			wantOp: "GetMembership",
		},
		{
			name:   "DeleteMembership",
			method: http.MethodDelete,
			path:   "/memberships/m1",
			wantOp: "DeleteMembership",
		},
		{
			name:   "UpdateMembership",
			method: http.MethodPatch,
			path:   "/memberships/m1",
			wantOp: "UpdateMembership",
		},

		// ---- Membership: AnalysisTemplate ----
		{
			name:   "CreateAnalysisTemplate",
			method: http.MethodPost,
			path:   "/memberships/m1/analysistemplates",
			wantOp: "CreateAnalysisTemplate",
		},
		{
			name:   "ListAnalysisTemplates",
			method: http.MethodGet,
			path:   "/memberships/m1/analysistemplates",
			wantOp: "ListAnalysisTemplates",
		},
		{
			name:   "GetAnalysisTemplate",
			method: http.MethodGet,
			path:   "/memberships/m1/analysistemplates/t1",
			wantOp: "GetAnalysisTemplate",
		},
		{
			name:   "DeleteAnalysisTemplate",
			method: http.MethodDelete,
			path:   "/memberships/m1/analysistemplates/t1",
			wantOp: "DeleteAnalysisTemplate",
		},
		{
			name:   "UpdateAnalysisTemplate",
			method: http.MethodPatch,
			path:   "/memberships/m1/analysistemplates/t1",
			wantOp: "UpdateAnalysisTemplate",
		},

		// ---- Membership: ConfiguredAudienceModelAssociation ----
		{
			name:   "CreateConfiguredAudienceModelAssociation",
			method: http.MethodPost,
			path:   "/memberships/m1/configuredaudiencemodelassociations",
			wantOp: "CreateConfiguredAudienceModelAssociation",
		},
		{
			name:   "ListConfiguredAudienceModelAssociations",
			method: http.MethodGet,
			path:   "/memberships/m1/configuredaudiencemodelassociations",
			wantOp: "ListConfiguredAudienceModelAssociations",
		},
		{
			name:   "GetConfiguredAudienceModelAssociation",
			method: http.MethodGet,
			path:   "/memberships/m1/configuredaudiencemodelassociations/a1",
			wantOp: "GetConfiguredAudienceModelAssociation",
		},
		{
			name:   "DeleteConfiguredAudienceModelAssociation",
			method: http.MethodDelete,
			path:   "/memberships/m1/configuredaudiencemodelassociations/a1",
			wantOp: "DeleteConfiguredAudienceModelAssociation",
		},
		{
			name:   "UpdateConfiguredAudienceModelAssociation",
			method: http.MethodPatch,
			path:   "/memberships/m1/configuredaudiencemodelassociations/a1",
			wantOp: "UpdateConfiguredAudienceModelAssociation",
		},

		// ---- Membership: ConfiguredTableAssociation (+ nested analysis rule) ----
		{
			name:   "CreateConfiguredTableAssociation",
			method: http.MethodPost,
			path:   "/memberships/m1/configuredTableAssociations",
			wantOp: "CreateConfiguredTableAssociation",
		},
		{
			name:   "ListConfiguredTableAssociations",
			method: http.MethodGet,
			path:   "/memberships/m1/configuredTableAssociations",
			wantOp: "ListConfiguredTableAssociations",
		},
		{
			name:   "GetConfiguredTableAssociation",
			method: http.MethodGet,
			path:   "/memberships/m1/configuredTableAssociations/a1",
			wantOp: "GetConfiguredTableAssociation",
		},
		{
			name:   "DeleteConfiguredTableAssociation",
			method: http.MethodDelete,
			path:   "/memberships/m1/configuredTableAssociations/a1",
			wantOp: "DeleteConfiguredTableAssociation",
		},
		{
			name:   "UpdateConfiguredTableAssociation",
			method: http.MethodPatch,
			path:   "/memberships/m1/configuredTableAssociations/a1",
			wantOp: "UpdateConfiguredTableAssociation",
		},
		{
			name:   "CreateConfiguredTableAssociationAnalysisRule",
			method: http.MethodPost,
			path:   "/memberships/m1/configuredTableAssociations/a1/analysisRule",
			wantOp: "CreateConfiguredTableAssociationAnalysisRule",
		},
		{
			name:   "GetConfiguredTableAssociationAnalysisRule",
			method: http.MethodGet,
			path:   "/memberships/m1/configuredTableAssociations/a1/analysisRule/AGGREGATION",
			wantOp: "GetConfiguredTableAssociationAnalysisRule",
		},
		{
			name:   "DeleteConfiguredTableAssociationAnalysisRule",
			method: http.MethodDelete,
			path:   "/memberships/m1/configuredTableAssociations/a1/analysisRule/AGGREGATION",
			wantOp: "DeleteConfiguredTableAssociationAnalysisRule",
		},
		{
			name:   "UpdateConfiguredTableAssociationAnalysisRule",
			method: http.MethodPatch,
			path:   "/memberships/m1/configuredTableAssociations/a1/analysisRule/AGGREGATION",
			wantOp: "UpdateConfiguredTableAssociationAnalysisRule",
		},

		// ---- Membership: IdMappingTable ----
		{
			name:   "CreateIdMappingTable",
			method: http.MethodPost,
			path:   "/memberships/m1/idmappingtables",
			wantOp: "CreateIdMappingTable",
		},
		{
			name:   "ListIdMappingTables",
			method: http.MethodGet,
			path:   "/memberships/m1/idmappingtables",
			wantOp: "ListIdMappingTables",
		},
		{
			name:   "GetIdMappingTable",
			method: http.MethodGet,
			path:   "/memberships/m1/idmappingtables/t1",
			wantOp: "GetIdMappingTable",
		},
		{
			name:   "DeleteIdMappingTable",
			method: http.MethodDelete,
			path:   "/memberships/m1/idmappingtables/t1",
			wantOp: "DeleteIdMappingTable",
		},
		{
			name:   "UpdateIdMappingTable",
			method: http.MethodPatch,
			path:   "/memberships/m1/idmappingtables/t1",
			wantOp: "UpdateIdMappingTable",
		},
		{
			name:   "PopulateIdMappingTable",
			method: http.MethodPost,
			path:   "/memberships/m1/idmappingtables/t1/populate",
			wantOp: "PopulateIdMappingTable",
		},

		// ---- Membership: IdNamespaceAssociation ----
		{
			name:   "CreateIdNamespaceAssociation",
			method: http.MethodPost,
			path:   "/memberships/m1/idnamespaceassociations",
			wantOp: "CreateIdNamespaceAssociation",
		},
		{
			name:   "ListIdNamespaceAssociations",
			method: http.MethodGet,
			path:   "/memberships/m1/idnamespaceassociations",
			wantOp: "ListIdNamespaceAssociations",
		},
		{
			name:   "GetIdNamespaceAssociation",
			method: http.MethodGet,
			path:   "/memberships/m1/idnamespaceassociations/a1",
			wantOp: "GetIdNamespaceAssociation",
		},
		{
			name:   "DeleteIdNamespaceAssociation",
			method: http.MethodDelete,
			path:   "/memberships/m1/idnamespaceassociations/a1",
			wantOp: "DeleteIdNamespaceAssociation",
		},
		{
			name:   "UpdateIdNamespaceAssociation",
			method: http.MethodPatch,
			path:   "/memberships/m1/idnamespaceassociations/a1",
			wantOp: "UpdateIdNamespaceAssociation",
		},

		// ---- Membership: privacy ----
		{
			name:   "PreviewPrivacyImpact",
			method: http.MethodPost,
			path:   "/memberships/m1/previewprivacyimpact",
			wantOp: "PreviewPrivacyImpact",
		},
		{
			name:   "ListPrivacyBudgets",
			method: http.MethodGet,
			path:   "/memberships/m1/privacybudgets",
			wantOp: "ListPrivacyBudgets",
		},
		{
			name:   "CreatePrivacyBudgetTemplate",
			method: http.MethodPost,
			path:   "/memberships/m1/privacybudgettemplates",
			wantOp: "CreatePrivacyBudgetTemplate",
		},
		{
			name:   "ListPrivacyBudgetTemplates",
			method: http.MethodGet,
			path:   "/memberships/m1/privacybudgettemplates",
			wantOp: "ListPrivacyBudgetTemplates",
		},
		{
			name:   "GetPrivacyBudgetTemplate",
			method: http.MethodGet,
			path:   "/memberships/m1/privacybudgettemplates/pt1",
			wantOp: "GetPrivacyBudgetTemplate",
		},
		{
			name:   "DeletePrivacyBudgetTemplate",
			method: http.MethodDelete,
			path:   "/memberships/m1/privacybudgettemplates/pt1",
			wantOp: "DeletePrivacyBudgetTemplate",
		},
		{
			name:   "UpdatePrivacyBudgetTemplate",
			method: http.MethodPatch,
			path:   "/memberships/m1/privacybudgettemplates/pt1",
			wantOp: "UpdatePrivacyBudgetTemplate",
		},

		// ---- Membership: ProtectedJob / ProtectedQuery ----
		{
			name:   "StartProtectedJob",
			method: http.MethodPost,
			path:   "/memberships/m1/protectedJobs",
			wantOp: "StartProtectedJob",
		},
		{
			name:   "ListProtectedJobs",
			method: http.MethodGet,
			path:   "/memberships/m1/protectedJobs",
			wantOp: "ListProtectedJobs",
		},
		{
			name:   "GetProtectedJob",
			method: http.MethodGet,
			path:   "/memberships/m1/protectedJobs/j1",
			wantOp: "GetProtectedJob",
		},
		{
			name:   "UpdateProtectedJob",
			method: http.MethodPatch,
			path:   "/memberships/m1/protectedJobs/j1",
			wantOp: "UpdateProtectedJob",
		},
		{
			name:   "StartProtectedQuery",
			method: http.MethodPost,
			path:   "/memberships/m1/protectedQueries",
			wantOp: "StartProtectedQuery",
		},
		{
			name:   "ListProtectedQueries",
			method: http.MethodGet,
			path:   "/memberships/m1/protectedQueries",
			wantOp: "ListProtectedQueries",
		},
		{
			name:   "GetProtectedQuery",
			method: http.MethodGet,
			path:   "/memberships/m1/protectedQueries/q1",
			wantOp: "GetProtectedQuery",
		},
		{
			name:   "UpdateProtectedQuery",
			method: http.MethodPatch,
			path:   "/memberships/m1/protectedQueries/q1",
			wantOp: "UpdateProtectedQuery",
		},

		// ---- Tags ----
		{
			name:   "ListTagsForResource",
			method: http.MethodGet,
			path:   "/tags/arn:aws:cleanrooms:us-east-1:123456789012:collaboration/c1",
			wantOp: "ListTagsForResource",
		},
		{
			name:   "TagResource",
			method: http.MethodPost,
			path:   "/tags/arn:aws:cleanrooms:us-east-1:123456789012:collaboration/c1",
			wantOp: "TagResource",
		},
		{
			name:   "UntagResource",
			method: http.MethodDelete,
			path:   "/tags/arn:aws:cleanrooms:us-east-1:123456789012:collaboration/c1",
			wantOp: "UntagResource",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := routeMatcherContext(t, tt.method, tt.path)
			require.True(
				t,
				h.RouteMatcher()(c),
				"RouteMatcher should accept %s %s",
				tt.method,
				tt.path,
			)
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c), "%s %s", tt.method, tt.path)
		})
	}
}

// TestRouteMatcher_WrongMethodRejected is a regression guard for the
// route-matcher bug class: a handful of representative (method, path)
// combinations that a real client never sends must not resolve to any known
// operation, even though RouteMatcher itself is path-based and still accepts
// the request (echo then hits ExtractOperation, which must reject it).
func TestRouteMatcher_WrongMethodRejected(t *testing.T) {
	t.Parallel()

	h := cleanrooms.NewHandler(cleanrooms.NewInMemoryBackend("123456789012", "us-east-1"))

	tests := []struct {
		method string
		path   string
		name   string
	}{
		{
			name:   "POST to a single collaboration (only PATCH updates)",
			method: http.MethodPost,
			path:   "/collaborations/c1",
		},
		{
			name:   "PUT to memberships root (no PUT op exists)",
			method: http.MethodPut,
			path:   "/memberships",
		},
		{
			name:   "POST to collaboration analysistemplates (create is membership-scoped, not collaboration-scoped)",
			method: http.MethodPost,
			path:   "/collaborations/c1/analysistemplates",
		},
		{
			name:   "DELETE a protected query (no delete op exists, only PATCH to cancel)",
			method: http.MethodDelete,
			path:   "/memberships/m1/protectedQueries/q1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := routeMatcherContext(t, tt.method, tt.path)
			assert.Empty(
				t,
				h.ExtractOperation(c),
				"%s %s should not resolve to a known op",
				tt.method,
				tt.path,
			)
		})
	}
}
