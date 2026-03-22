package codecommit_test

import (
	"testing"

	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// codecommit client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := codecommit.NewInMemoryBackend("000000000000", "us-east-1")
	h := codecommit.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &codecommitsdk.Client{}, h.GetSupportedOperations(), []string{
		"AssociateApprovalRuleTemplateWithRepository",
		"BatchAssociateApprovalRuleTemplateWithRepositories",
		"BatchDescribeMergeConflicts",
		"BatchDisassociateApprovalRuleTemplateFromRepositories",
		"BatchGetCommits",
		"BatchGetRepositories",
		"CreateApprovalRuleTemplate",
		"CreateBranch",
		"CreateCommit",
		"CreatePullRequest",
		"CreatePullRequestApprovalRule",
		"CreateUnreferencedMergeCommit",
		"DeleteApprovalRuleTemplate",
		"DeleteBranch",
		"DeleteCommentContent",
		"DeleteFile",
		"DeletePullRequestApprovalRule",
		"DescribeMergeConflicts",
		"DescribePullRequestEvents",
		"DisassociateApprovalRuleTemplateFromRepository",
		"EvaluatePullRequestApprovalRules",
		"GetApprovalRuleTemplate",
		"GetBlob",
		"GetBranch",
		"GetComment",
		"GetCommentReactions",
		"GetCommentsForComparedCommit",
		"GetCommentsForPullRequest",
		"GetCommit",
		"GetDifferences",
		"GetFile",
		"GetFolder",
		"GetMergeCommit",
		"GetMergeConflicts",
		"GetMergeOptions",
		"GetPullRequest",
		"GetPullRequestApprovalStates",
		"GetPullRequestOverrideState",
		"GetRepositoryTriggers",
		"ListApprovalRuleTemplates",
		"ListAssociatedApprovalRuleTemplatesForRepository",
		"ListBranches",
		"ListFileCommitHistory",
		"ListPullRequests",
		"ListRepositoriesForApprovalRuleTemplate",
		"MergeBranchesByFastForward",
		"MergeBranchesBySquash",
		"MergeBranchesByThreeWay",
		"MergePullRequestByFastForward",
		"MergePullRequestBySquash",
		"MergePullRequestByThreeWay",
		"OverridePullRequestApprovalRules",
		"PostCommentForComparedCommit",
		"PostCommentForPullRequest",
		"PostCommentReply",
		"PutCommentReaction",
		"PutFile",
		"PutRepositoryTriggers",
		"TestRepositoryTriggers",
		"UpdateApprovalRuleTemplateContent",
		"UpdateApprovalRuleTemplateDescription",
		"UpdateApprovalRuleTemplateName",
		"UpdateComment",
		"UpdateDefaultBranch",
		"UpdatePullRequestApprovalRuleContent",
		"UpdatePullRequestApprovalState",
		"UpdatePullRequestDescription",
		"UpdatePullRequestStatus",
		"UpdatePullRequestTitle",
		"UpdateRepositoryDescription",
		"UpdateRepositoryEncryptionKey",
		"UpdateRepositoryName",
	})
}
