package codecommit_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/aws/aws-sdk-go-v2/service/codecommit/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

// These tests prove that two ops the real AWS API documents as idempotent
// deletes no longer surface a not-found error through the real SDK client
// (gopherstack-wlo1). Before the fix, both used a generic not-found sentinel
// mapped to a code absent from that op's own deserializeOpError<Op> switch
// (aws-sdk-go-v2/service/codecommit@v1.36.4 deserializers.go) -- DeleteRepository
// and DeletePullRequestApprovalRule have no *DoesNotExistException case at
// all, unlike every other repository/approval-rule op.

func TestDeleteRepository_UnknownNameIsIdempotentSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := codecommit.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeCommitClient(t, codecommit.NewHandler(backend))

	out, err := client.DeleteRepository(t.Context(), &codecommitsdk.DeleteRepositoryInput{
		RepositoryName: aws.String("does-not-exist"),
	})
	require.NoError(
		t,
		err,
		"DeleteRepository on an unknown name must succeed (its own switch has no RepositoryDoesNotExistException case)",
	)
	require.Nil(t, out.RepositoryId)
}

func TestDeletePullRequestApprovalRule_UnknownRuleIsIdempotentSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := codecommit.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeCommitClient(t, codecommit.NewHandler(backend))

	_, err := client.CreateRepository(t.Context(), &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String("pr-repo"),
	})
	require.NoError(t, err)

	_, err = client.CreateCommit(t.Context(), &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("pr-repo"),
		BranchName:     aws.String("main"),
		AuthorName:     aws.String("test"),
		Email:          aws.String("test@example.com"),
		CommitMessage:  aws.String("initial"),
	})
	require.NoError(t, err)

	prOut, err := client.CreatePullRequest(t.Context(), &codecommitsdk.CreatePullRequestInput{
		Title: aws.String("Test PR"),
		Targets: []types.Target{
			{RepositoryName: aws.String("pr-repo"), SourceReference: aws.String("refs/heads/main")},
		},
	})
	require.NoError(t, err)
	prID := prOut.PullRequest.PullRequestId

	// The rule named "no-such-rule" was never created on this PR.
	out, err := client.DeletePullRequestApprovalRule(
		t.Context(),
		&codecommitsdk.DeletePullRequestApprovalRuleInput{
			PullRequestId:    prID,
			ApprovalRuleName: aws.String("no-such-rule"),
		},
	)
	require.NoError(
		t,
		err,
		"DeletePullRequestApprovalRule on an unknown rule must succeed (its own doc: 200 OK without content)",
	)
	require.Nil(t, out.ApprovalRuleId)
}

func TestDeleteApprovalRuleTemplate_UnknownNameIsIdempotentSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := codecommit.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeCommitClient(t, codecommit.NewHandler(backend))

	out, err := client.DeleteApprovalRuleTemplate(
		t.Context(),
		&codecommitsdk.DeleteApprovalRuleTemplateInput{
			ApprovalRuleTemplateName: aws.String("no-such-template"),
		},
	)
	require.NoError(
		t,
		err,
		"DeleteApprovalRuleTemplate on an unknown name must succeed (its own doc: 200 OK without content)",
	)
	require.Nil(t, out.ApprovalRuleTemplateId)
}

func TestDeleteBranch_UnknownNameIsIdempotentSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := codecommit.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeCommitClient(t, codecommit.NewHandler(backend))

	_, err := client.CreateRepository(t.Context(), &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String("branch-repo"),
	})
	require.NoError(t, err)

	out, err := client.DeleteBranch(t.Context(), &codecommitsdk.DeleteBranchInput{
		RepositoryName: aws.String("branch-repo"),
		BranchName:     aws.String("no-such-branch"),
	})
	require.NoError(
		t,
		err,
		"DeleteBranch on an unknown name must succeed (its own switch has no BranchDoesNotExistException case)",
	)
	require.Nil(t, out.DeletedBranch)
}
