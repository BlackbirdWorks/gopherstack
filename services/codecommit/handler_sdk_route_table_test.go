package codecommit_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real CodeCommit
// operation, extracted from codecommit@v1.36.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("CodeCommit_20150413.<Op>")
// and always POSTs to "/" -- CodeCommit is JSON-RPC 1.1
// (services/_PROTOCOLS.md), so unlike a REST-family service there is no path
// template to get wrong: dispatch is entirely by this one header. The target
// prefix ("CodeCommit_20150413", not guessed) is read directly from
// serializers.go. ExtractOperation and Handler() (via buildOps()'s map,
// dispatched through h.dispatch) both derive the action the same way, so the
// class of bug this table catches is a dispatch-table key that doesn't
// exactly match the real op name (typo, wrong case), not a route-template
// mismatch.
//
// This table covers all 79 real CodeCommit ops (codecommit@v1.36.4) --
// confirmed by diffing both GetSupportedOperations() and the actual
// buildOps() map's key set against this exact list: zero mismatches in
// either direction, no dead or excluded keys. GetSupportedOperations() here
// is a hand-maintained literal slice, not built by ranging over the dispatch
// map, so the two diffs are genuinely independent checks.
//
// This service's handlers were edited repeatedly and recently for wire-shape
// bugs (the Comment family's undecodable bodies, CreateCommit's invented
// filePath key) -- but that work was about response shapes, not dispatch
// keys, so it is not evidence the routing was reviewed alongside; this table
// checks routing on its own terms.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("CodeCommit_20150413.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{
			"AssociateApprovalRuleTemplateWithRepository",
			"CodeCommit_20150413.AssociateApprovalRuleTemplateWithRepository",
		},
		{
			"BatchAssociateApprovalRuleTemplateWithRepositories",
			"CodeCommit_20150413.BatchAssociateApprovalRuleTemplateWithRepositories",
		},
		{"BatchDescribeMergeConflicts", "CodeCommit_20150413.BatchDescribeMergeConflicts"},
		{
			"BatchDisassociateApprovalRuleTemplateFromRepositories",
			"CodeCommit_20150413.BatchDisassociateApprovalRuleTemplateFromRepositories",
		},
		{"BatchGetCommits", "CodeCommit_20150413.BatchGetCommits"},
		{"BatchGetRepositories", "CodeCommit_20150413.BatchGetRepositories"},
		{"CreateApprovalRuleTemplate", "CodeCommit_20150413.CreateApprovalRuleTemplate"},
		{"CreateBranch", "CodeCommit_20150413.CreateBranch"},
		{"CreateCommit", "CodeCommit_20150413.CreateCommit"},
		{"CreatePullRequest", "CodeCommit_20150413.CreatePullRequest"},
		{"CreatePullRequestApprovalRule", "CodeCommit_20150413.CreatePullRequestApprovalRule"},
		{"CreateRepository", "CodeCommit_20150413.CreateRepository"},
		{"CreateUnreferencedMergeCommit", "CodeCommit_20150413.CreateUnreferencedMergeCommit"},
		{"DeleteApprovalRuleTemplate", "CodeCommit_20150413.DeleteApprovalRuleTemplate"},
		{"DeleteBranch", "CodeCommit_20150413.DeleteBranch"},
		{"DeleteCommentContent", "CodeCommit_20150413.DeleteCommentContent"},
		{"DeleteFile", "CodeCommit_20150413.DeleteFile"},
		{"DeletePullRequestApprovalRule", "CodeCommit_20150413.DeletePullRequestApprovalRule"},
		{"DeleteRepository", "CodeCommit_20150413.DeleteRepository"},
		{"DescribeMergeConflicts", "CodeCommit_20150413.DescribeMergeConflicts"},
		{"DescribePullRequestEvents", "CodeCommit_20150413.DescribePullRequestEvents"},
		{
			"DisassociateApprovalRuleTemplateFromRepository",
			"CodeCommit_20150413.DisassociateApprovalRuleTemplateFromRepository",
		},
		{"EvaluatePullRequestApprovalRules", "CodeCommit_20150413.EvaluatePullRequestApprovalRules"},
		{"GetApprovalRuleTemplate", "CodeCommit_20150413.GetApprovalRuleTemplate"},
		{"GetBlob", "CodeCommit_20150413.GetBlob"},
		{"GetBranch", "CodeCommit_20150413.GetBranch"},
		{"GetComment", "CodeCommit_20150413.GetComment"},
		{"GetCommentReactions", "CodeCommit_20150413.GetCommentReactions"},
		{"GetCommentsForComparedCommit", "CodeCommit_20150413.GetCommentsForComparedCommit"},
		{"GetCommentsForPullRequest", "CodeCommit_20150413.GetCommentsForPullRequest"},
		{"GetCommit", "CodeCommit_20150413.GetCommit"},
		{"GetDifferences", "CodeCommit_20150413.GetDifferences"},
		{"GetFile", "CodeCommit_20150413.GetFile"},
		{"GetFolder", "CodeCommit_20150413.GetFolder"},
		{"GetMergeCommit", "CodeCommit_20150413.GetMergeCommit"},
		{"GetMergeConflicts", "CodeCommit_20150413.GetMergeConflicts"},
		{"GetMergeOptions", "CodeCommit_20150413.GetMergeOptions"},
		{"GetPullRequest", "CodeCommit_20150413.GetPullRequest"},
		{"GetPullRequestApprovalStates", "CodeCommit_20150413.GetPullRequestApprovalStates"},
		{"GetPullRequestOverrideState", "CodeCommit_20150413.GetPullRequestOverrideState"},
		{"GetRepository", "CodeCommit_20150413.GetRepository"},
		{"GetRepositoryTriggers", "CodeCommit_20150413.GetRepositoryTriggers"},
		{"ListApprovalRuleTemplates", "CodeCommit_20150413.ListApprovalRuleTemplates"},
		{
			"ListAssociatedApprovalRuleTemplatesForRepository",
			"CodeCommit_20150413.ListAssociatedApprovalRuleTemplatesForRepository",
		},
		{"ListBranches", "CodeCommit_20150413.ListBranches"},
		{"ListFileCommitHistory", "CodeCommit_20150413.ListFileCommitHistory"},
		{"ListPullRequests", "CodeCommit_20150413.ListPullRequests"},
		{"ListRepositories", "CodeCommit_20150413.ListRepositories"},
		{"ListRepositoriesForApprovalRuleTemplate", "CodeCommit_20150413.ListRepositoriesForApprovalRuleTemplate"},
		{"ListTagsForResource", "CodeCommit_20150413.ListTagsForResource"},
		{"MergeBranchesByFastForward", "CodeCommit_20150413.MergeBranchesByFastForward"},
		{"MergeBranchesBySquash", "CodeCommit_20150413.MergeBranchesBySquash"},
		{"MergeBranchesByThreeWay", "CodeCommit_20150413.MergeBranchesByThreeWay"},
		{"MergePullRequestByFastForward", "CodeCommit_20150413.MergePullRequestByFastForward"},
		{"MergePullRequestBySquash", "CodeCommit_20150413.MergePullRequestBySquash"},
		{"MergePullRequestByThreeWay", "CodeCommit_20150413.MergePullRequestByThreeWay"},
		{"OverridePullRequestApprovalRules", "CodeCommit_20150413.OverridePullRequestApprovalRules"},
		{"PostCommentForComparedCommit", "CodeCommit_20150413.PostCommentForComparedCommit"},
		{"PostCommentForPullRequest", "CodeCommit_20150413.PostCommentForPullRequest"},
		{"PostCommentReply", "CodeCommit_20150413.PostCommentReply"},
		{"PutCommentReaction", "CodeCommit_20150413.PutCommentReaction"},
		{"PutFile", "CodeCommit_20150413.PutFile"},
		{"PutRepositoryTriggers", "CodeCommit_20150413.PutRepositoryTriggers"},
		{"TagResource", "CodeCommit_20150413.TagResource"},
		{"TestRepositoryTriggers", "CodeCommit_20150413.TestRepositoryTriggers"},
		{"UntagResource", "CodeCommit_20150413.UntagResource"},
		{"UpdateApprovalRuleTemplateContent", "CodeCommit_20150413.UpdateApprovalRuleTemplateContent"},
		{"UpdateApprovalRuleTemplateDescription", "CodeCommit_20150413.UpdateApprovalRuleTemplateDescription"},
		{"UpdateApprovalRuleTemplateName", "CodeCommit_20150413.UpdateApprovalRuleTemplateName"},
		{"UpdateComment", "CodeCommit_20150413.UpdateComment"},
		{"UpdateDefaultBranch", "CodeCommit_20150413.UpdateDefaultBranch"},
		{"UpdatePullRequestApprovalRuleContent", "CodeCommit_20150413.UpdatePullRequestApprovalRuleContent"},
		{"UpdatePullRequestApprovalState", "CodeCommit_20150413.UpdatePullRequestApprovalState"},
		{"UpdatePullRequestDescription", "CodeCommit_20150413.UpdatePullRequestDescription"},
		{"UpdatePullRequestStatus", "CodeCommit_20150413.UpdatePullRequestStatus"},
		{"UpdatePullRequestTitle", "CodeCommit_20150413.UpdatePullRequestTitle"},
		{"UpdateRepositoryDescription", "CodeCommit_20150413.UpdateRepositoryDescription"},
		{"UpdateRepositoryEncryptionKey", "CodeCommit_20150413.UpdateRepositoryEncryptionKey"},
		{"UpdateRepositoryName", "CodeCommit_20150413.UpdateRepositoryName"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real CodeCommit operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to the dispatch-miss sentinel (errUnknownAction,
// handler.go's dispatch() single production call site) that a
// dispatch-table key mismatch would produce.
//
// errUnknownAction is not even listed in errCodeLookup (handler.go): a miss
// falls all the way through the errors.Is loop to handleError's own
// initialized defaults (400, "ValidationException") -- the exact same wire
// type errInvalidRequest is explicitly mapped to, and the same type/status
// every other genuinely unmatched error also renders as. That is a sharper
// version of the workmail/transfer trap: the dispatch-miss sentinel doesn't
// just share a type with another sentinel, it IS the loop's default
// fallthrough. This test instead asserts on the dispatch-miss message text,
// which is unique: dispatch's fmt.Errorf("%w: %s", errUnknownAction, action)
// always renders as `unknown action: <op>`, a substring none of this
// package's other error messages produce.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			b := codecommit.NewInMemoryBackend("000000000000", "us-east-1")
			h := codecommit.NewHandler(b)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action: "+tc.op,
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
