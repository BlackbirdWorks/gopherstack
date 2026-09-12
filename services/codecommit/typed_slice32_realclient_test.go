package codecommit_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/aws/aws-sdk-go-v2/service/codecommit/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

func newSlice32CodeCommitClient(t *testing.T) *codecommitsdk.Client {
	t.Helper()

	backend := codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)

	return newTestCodeCommitClient(t, codecommit.NewHandler(backend))
}

// TestSlice32CC_RepositoryLifecycle drives BatchGetRepositories,
// UpdateRepositoryName, UpdateRepositoryDescription, TagResource,
// UntagResource and ListTagsForResource.
func TestSlice32CC_RepositoryLifecycle(t *testing.T) {
	t.Parallel()

	client := newSlice32CodeCommitClient(t)
	ctx := t.Context()

	created, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String("repo-a"),
	})
	require.NoError(t, err)
	repoARN := aws.ToString(created.RepositoryMetadata.Arn)

	_, err = client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo-b")})
	require.NoError(t, err)

	batch, err := client.BatchGetRepositories(ctx, &codecommitsdk.BatchGetRepositoriesInput{
		RepositoryNames: []string{"repo-a", "repo-b", "no-such-repo"},
	})
	require.NoError(t, err)
	require.Len(t, batch.Repositories, 2)
	require.Len(t, batch.RepositoriesNotFound, 1)
	assert.Equal(t, "no-such-repo", batch.RepositoriesNotFound[0])

	_, err = client.UpdateRepositoryDescription(ctx, &codecommitsdk.UpdateRepositoryDescriptionInput{
		RepositoryName:        aws.String("repo-a"),
		RepositoryDescription: aws.String("a real repo"),
	})
	require.NoError(t, err)

	_, err = client.UpdateRepositoryName(ctx, &codecommitsdk.UpdateRepositoryNameInput{
		OldName: aws.String("repo-a"),
		NewName: aws.String("repo-a-renamed"),
	})
	require.NoError(t, err)

	desc, err := client.GetRepository(ctx, &codecommitsdk.GetRepositoryInput{
		RepositoryName: aws.String("repo-a-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "a real repo", aws.ToString(desc.RepositoryMetadata.RepositoryDescription))

	_, err = client.TagResource(ctx, &codecommitsdk.TagResourceInput{
		ResourceArn: aws.String(repoARN),
		Tags:        map[string]string{"team": "vcs"},
	})
	require.NoError(t, err)

	tags, err := client.ListTagsForResource(ctx, &codecommitsdk.ListTagsForResourceInput{
		ResourceArn: aws.String(repoARN),
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "vcs"}, tags.Tags)

	_, err = client.UntagResource(ctx, &codecommitsdk.UntagResourceInput{
		ResourceArn: aws.String(repoARN),
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	tags, err = client.ListTagsForResource(ctx, &codecommitsdk.ListTagsForResourceInput{
		ResourceArn: aws.String(repoARN),
	})
	require.NoError(t, err)
	assert.Empty(t, tags.Tags)
}

// TestSlice32CC_BranchesAndCommits drives ListBranches, UpdateDefaultBranch
// and BatchGetCommits.
func TestSlice32CC_BranchesAndCommits(t *testing.T) {
	t.Parallel()

	client := newSlice32CodeCommitClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)

	c1, err := client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("main"),
		PutFiles:       []types.PutFileEntry{{FilePath: aws.String("a.txt"), FileContent: []byte("a")}},
	})
	require.NoError(t, err)

	_, err = client.CreateBranch(ctx, &codecommitsdk.CreateBranchInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("dev"),
		CommitId:       c1.CommitId,
	})
	require.NoError(t, err)

	branches, err := client.ListBranches(ctx, &codecommitsdk.ListBranchesInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"main", "dev"}, branches.Branches)

	_, err = client.UpdateDefaultBranch(ctx, &codecommitsdk.UpdateDefaultBranchInput{
		RepositoryName:    aws.String("repo"),
		DefaultBranchName: aws.String("dev"),
	})
	require.NoError(t, err)

	desc, err := client.GetRepository(ctx, &codecommitsdk.GetRepositoryInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)
	assert.Equal(t, "dev", aws.ToString(desc.RepositoryMetadata.DefaultBranch))

	batch, err := client.BatchGetCommits(ctx, &codecommitsdk.BatchGetCommitsInput{
		RepositoryName: aws.String("repo"),
		CommitIds:      []string{aws.ToString(c1.CommitId), "no-such-commit"},
	})
	require.NoError(t, err)
	require.Len(t, batch.Commits, 1)
	assert.Equal(t, aws.ToString(c1.CommitId), aws.ToString(batch.Commits[0].CommitId))
	require.Len(t, batch.Errors, 1)
	assert.Equal(t, "no-such-commit", aws.ToString(batch.Errors[0].CommitId))
}

// TestSlice32CC_FilesAndBlob drives GetFile, GetFolder and GetBlob.
func TestSlice32CC_FilesAndBlob(t *testing.T) {
	t.Parallel()

	client := newSlice32CodeCommitClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)

	_, err = client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("main"),
		PutFiles: []types.PutFileEntry{
			{FilePath: aws.String("dir/file.txt"), FileContent: []byte("hello world")},
		},
	})
	require.NoError(t, err)

	file, err := client.GetFile(ctx, &codecommitsdk.GetFileInput{
		RepositoryName: aws.String("repo"),
		FilePath:       aws.String("dir/file.txt"),
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("hello world"), file.FileContent)
	require.NotEmpty(t, aws.ToString(file.BlobId))

	folder, err := client.GetFolder(ctx, &codecommitsdk.GetFolderInput{
		RepositoryName: aws.String("repo"),
		FolderPath:     aws.String("dir"),
	})
	require.NoError(t, err)
	require.Len(t, folder.Files, 1)
	assert.Equal(t, "dir/file.txt", aws.ToString(folder.Files[0].AbsolutePath))

	blob, err := client.GetBlob(ctx, &codecommitsdk.GetBlobInput{
		RepositoryName: aws.String("repo"),
		BlobId:         file.BlobId,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("hello world"), blob.Content)
}

// TestSlice32CC_MergeBranches drives GetMergeOptions, GetMergeCommit and
// MergeBranchesByFastForward.
func TestSlice32CC_MergeBranches(t *testing.T) {
	t.Parallel()

	client := newSlice32CodeCommitClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)

	base, err := client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("main"),
		PutFiles:       []types.PutFileEntry{{FilePath: aws.String("a.txt"), FileContent: []byte("a")}},
	})
	require.NoError(t, err)

	_, err = client.CreateBranch(ctx, &codecommitsdk.CreateBranchInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("feature"),
		CommitId:       base.CommitId,
	})
	require.NoError(t, err)

	feature, err := client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("feature"),
		ParentCommitId: base.CommitId,
		PutFiles:       []types.PutFileEntry{{FilePath: aws.String("b.txt"), FileContent: []byte("b")}},
	})
	require.NoError(t, err)

	options, err := client.GetMergeOptions(ctx, &codecommitsdk.GetMergeOptionsInput{
		RepositoryName:             aws.String("repo"),
		SourceCommitSpecifier:      aws.String("feature"),
		DestinationCommitSpecifier: aws.String("main"),
	})
	require.NoError(t, err)
	assert.Contains(t, options.MergeOptions, types.MergeOptionTypeEnumFastForwardMerge)

	merged, err := client.MergeBranchesByFastForward(ctx, &codecommitsdk.MergeBranchesByFastForwardInput{
		RepositoryName:             aws.String("repo"),
		SourceCommitSpecifier:      aws.String("feature"),
		DestinationCommitSpecifier: aws.String("main"),
		TargetBranch:               aws.String("main"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(feature.CommitId), aws.ToString(merged.CommitId))

	mergeCommit, err := client.GetMergeCommit(ctx, &codecommitsdk.GetMergeCommitInput{
		RepositoryName:             aws.String("repo"),
		SourceCommitSpecifier:      aws.String("feature"),
		DestinationCommitSpecifier: aws.String("main"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(feature.CommitId), aws.ToString(mergeCommit.SourceCommitId))
}

// TestSlice32CC_PullRequestLifecycle drives GetPullRequest,
// UpdatePullRequestTitle/Description, GetPullRequestApprovalStates,
// UpdatePullRequestApprovalState, GetPullRequestOverrideState,
// OverridePullRequestApprovalRules, ListPullRequests, and
// MergePullRequestBy{Squash,ThreeWay,FastForward}.
func TestSlice32CC_PullRequestLifecycle(t *testing.T) {
	t.Parallel()

	client := newSlice32CodeCommitClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)

	_, err = client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("main"),
		PutFiles:       []types.PutFileEntry{{FilePath: aws.String("a.txt"), FileContent: []byte("a")}},
	})
	require.NoError(t, err)

	created, err := client.CreatePullRequest(ctx, &codecommitsdk.CreatePullRequestInput{
		Title: aws.String("my change"),
		Targets: []types.Target{
			{RepositoryName: aws.String("repo"), SourceReference: aws.String("main")},
		},
	})
	require.NoError(t, err)
	prID := created.PullRequest.PullRequestId

	got, err := client.GetPullRequest(ctx, &codecommitsdk.GetPullRequestInput{PullRequestId: prID})
	require.NoError(t, err)
	assert.Equal(t, "my change", aws.ToString(got.PullRequest.Title))

	updatedTitle, err := client.UpdatePullRequestTitle(ctx, &codecommitsdk.UpdatePullRequestTitleInput{
		PullRequestId: prID,
		Title:         aws.String("renamed change"),
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed change", aws.ToString(updatedTitle.PullRequest.Title))

	updatedDesc, err := client.UpdatePullRequestDescription(ctx, &codecommitsdk.UpdatePullRequestDescriptionInput{
		PullRequestId: prID,
		Description:   aws.String("a great change"),
	})
	require.NoError(t, err)
	assert.Equal(t, "a great change", aws.ToString(updatedDesc.PullRequest.Description))

	_, err = client.UpdatePullRequestApprovalState(ctx, &codecommitsdk.UpdatePullRequestApprovalStateInput{
		PullRequestId: prID,
		RevisionId:    created.PullRequest.RevisionId,
		ApprovalState: types.ApprovalStateApprove,
	})
	require.NoError(t, err)

	states, err := client.GetPullRequestApprovalStates(ctx, &codecommitsdk.GetPullRequestApprovalStatesInput{
		PullRequestId: prID,
		RevisionId:    created.PullRequest.RevisionId,
	})
	require.NoError(t, err)
	require.Len(t, states.Approvals, 1)
	assert.Equal(t, types.ApprovalStateApprove, states.Approvals[0].ApprovalState)

	overrideState, err := client.GetPullRequestOverrideState(ctx, &codecommitsdk.GetPullRequestOverrideStateInput{
		PullRequestId: prID,
		RevisionId:    created.PullRequest.RevisionId,
	})
	require.NoError(t, err)
	assert.False(t, overrideState.Overridden)

	_, err = client.OverridePullRequestApprovalRules(ctx, &codecommitsdk.OverridePullRequestApprovalRulesInput{
		PullRequestId:  prID,
		RevisionId:     created.PullRequest.RevisionId,
		OverrideStatus: types.OverrideStatusOverride,
	})
	require.NoError(t, err)

	overrideState, err = client.GetPullRequestOverrideState(ctx, &codecommitsdk.GetPullRequestOverrideStateInput{
		PullRequestId: prID,
		RevisionId:    created.PullRequest.RevisionId,
	})
	require.NoError(t, err)
	assert.True(t, overrideState.Overridden)

	listed, err := client.ListPullRequests(ctx, &codecommitsdk.ListPullRequestsInput{
		RepositoryName: aws.String("repo"),
	})
	require.NoError(t, err)
	require.Len(t, listed.PullRequestIds, 1)
	assert.Equal(t, aws.ToString(prID), listed.PullRequestIds[0])

	merged, err := client.MergePullRequestBySquash(ctx, &codecommitsdk.MergePullRequestBySquashInput{
		PullRequestId:  prID,
		RepositoryName: aws.String("repo"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.PullRequestStatusEnumClosed, merged.PullRequest.PullRequestStatus)
}

// TestSlice32CC_MergePullRequestByThreeWayAndFastForward drives
// MergePullRequestByThreeWay and MergePullRequestByFastForward against fresh
// pull requests (each PR can only be merged once).
func TestSlice32CC_MergePullRequestByThreeWayAndFastForward(t *testing.T) {
	t.Parallel()

	client := newSlice32CodeCommitClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)

	_, err = client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("main"),
		PutFiles:       []types.PutFileEntry{{FilePath: aws.String("a.txt"), FileContent: []byte("a")}},
	})
	require.NoError(t, err)

	target := types.Target{RepositoryName: aws.String("repo"), SourceReference: aws.String("main")}

	pr1, err := client.CreatePullRequest(ctx, &codecommitsdk.CreatePullRequestInput{
		Title:   aws.String("pr1"),
		Targets: []types.Target{target},
	})
	require.NoError(t, err)

	merged1, err := client.MergePullRequestByThreeWay(ctx, &codecommitsdk.MergePullRequestByThreeWayInput{
		PullRequestId:  pr1.PullRequest.PullRequestId,
		RepositoryName: aws.String("repo"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.PullRequestStatusEnumClosed, merged1.PullRequest.PullRequestStatus)

	pr2, err := client.CreatePullRequest(ctx, &codecommitsdk.CreatePullRequestInput{
		Title:   aws.String("pr2"),
		Targets: []types.Target{target},
	})
	require.NoError(t, err)

	merged2, err := client.MergePullRequestByFastForward(ctx, &codecommitsdk.MergePullRequestByFastForwardInput{
		PullRequestId:  pr2.PullRequest.PullRequestId,
		RepositoryName: aws.String("repo"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.PullRequestStatusEnumClosed, merged2.PullRequest.PullRequestStatus)
}

// TestSlice32CC_ApprovalRuleTemplateLifecycle drives GetApprovalRuleTemplate,
// ListApprovalRuleTemplates, Update{Content,Description,Name},
// Associate/DisassociateApprovalRuleTemplateWithRepository,
// ListAssociatedApprovalRuleTemplatesForRepository and
// ListRepositoriesForApprovalRuleTemplate.
func TestSlice32CC_ApprovalRuleTemplateLifecycle(t *testing.T) {
	t.Parallel()

	client := newSlice32CodeCommitClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)

	created, err := client.CreateApprovalRuleTemplate(ctx, &codecommitsdk.CreateApprovalRuleTemplateInput{
		ApprovalRuleTemplateName:    aws.String("art"),
		ApprovalRuleTemplateContent: aws.String(`{"Version":"2018-11-08","Statements":[]}`),
	})
	require.NoError(t, err)

	got, err := client.GetApprovalRuleTemplate(ctx, &codecommitsdk.GetApprovalRuleTemplateInput{
		ApprovalRuleTemplateName: aws.String("art"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.ApprovalRuleTemplate.ApprovalRuleTemplateId),
		aws.ToString(got.ApprovalRuleTemplate.ApprovalRuleTemplateId))

	listed, err := client.ListApprovalRuleTemplates(ctx, &codecommitsdk.ListApprovalRuleTemplatesInput{})
	require.NoError(t, err)
	assert.Contains(t, listed.ApprovalRuleTemplateNames, "art")

	updatedContent, err := client.UpdateApprovalRuleTemplateContent(
		ctx, &codecommitsdk.UpdateApprovalRuleTemplateContentInput{
			ApprovalRuleTemplateName:  aws.String("art"),
			NewRuleContent:            aws.String(`{"Version":"2018-11-08","Statements":[{"Type":"Approvers"}]}`),
			ExistingRuleContentSha256: created.ApprovalRuleTemplate.RuleContentSha256,
		},
	)
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(updatedContent.ApprovalRuleTemplate.ApprovalRuleTemplateContent), "Approvers")

	updatedDesc, err := client.UpdateApprovalRuleTemplateDescription(
		ctx, &codecommitsdk.UpdateApprovalRuleTemplateDescriptionInput{
			ApprovalRuleTemplateName:        aws.String("art"),
			ApprovalRuleTemplateDescription: aws.String("a template"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "a template", aws.ToString(updatedDesc.ApprovalRuleTemplate.ApprovalRuleTemplateDescription))

	updatedName, err := client.UpdateApprovalRuleTemplateName(ctx, &codecommitsdk.UpdateApprovalRuleTemplateNameInput{
		OldApprovalRuleTemplateName: aws.String("art"),
		NewApprovalRuleTemplateName: aws.String("art-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "art-renamed", aws.ToString(updatedName.ApprovalRuleTemplate.ApprovalRuleTemplateName))

	_, err = client.AssociateApprovalRuleTemplateWithRepository(
		ctx, &codecommitsdk.AssociateApprovalRuleTemplateWithRepositoryInput{
			ApprovalRuleTemplateName: aws.String("art-renamed"),
			RepositoryName:           aws.String("repo"),
		},
	)
	require.NoError(t, err)

	forRepo, err := client.ListAssociatedApprovalRuleTemplatesForRepository(
		ctx, &codecommitsdk.ListAssociatedApprovalRuleTemplatesForRepositoryInput{
			RepositoryName: aws.String("repo"),
		},
	)
	require.NoError(t, err)
	assert.Contains(t, forRepo.ApprovalRuleTemplateNames, "art-renamed")

	forTemplate, err := client.ListRepositoriesForApprovalRuleTemplate(
		ctx, &codecommitsdk.ListRepositoriesForApprovalRuleTemplateInput{
			ApprovalRuleTemplateName: aws.String("art-renamed"),
		},
	)
	require.NoError(t, err)
	assert.Contains(t, forTemplate.RepositoryNames, "repo")

	_, err = client.DisassociateApprovalRuleTemplateFromRepository(
		ctx, &codecommitsdk.DisassociateApprovalRuleTemplateFromRepositoryInput{
			ApprovalRuleTemplateName: aws.String("art-renamed"),
			RepositoryName:           aws.String("repo"),
		},
	)
	require.NoError(t, err)

	forRepo, err = client.ListAssociatedApprovalRuleTemplatesForRepository(
		ctx, &codecommitsdk.ListAssociatedApprovalRuleTemplatesForRepositoryInput{
			RepositoryName: aws.String("repo"),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, forRepo.ApprovalRuleTemplateNames)
}

// TestSlice32CC_BatchApprovalRuleTemplateAssociation drives
// BatchAssociateApprovalRuleTemplateWithRepositories and
// BatchDisassociateApprovalRuleTemplateFromRepositories.
func TestSlice32CC_BatchApprovalRuleTemplateAssociation(t *testing.T) {
	t.Parallel()

	client := newSlice32CodeCommitClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo-1")})
	require.NoError(t, err)
	_, err = client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo-2")})
	require.NoError(t, err)

	_, err = client.CreateApprovalRuleTemplate(ctx, &codecommitsdk.CreateApprovalRuleTemplateInput{
		ApprovalRuleTemplateName:    aws.String("batch-art"),
		ApprovalRuleTemplateContent: aws.String(`{"Version":"2018-11-08","Statements":[]}`),
	})
	require.NoError(t, err)

	assocOut, err := client.BatchAssociateApprovalRuleTemplateWithRepositories(
		ctx, &codecommitsdk.BatchAssociateApprovalRuleTemplateWithRepositoriesInput{
			ApprovalRuleTemplateName: aws.String("batch-art"),
			RepositoryNames:          []string{"repo-1", "repo-2", "no-such-repo"},
		},
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"repo-1", "repo-2"}, assocOut.AssociatedRepositoryNames)
	require.Len(t, assocOut.Errors, 1)
	assert.Equal(t, "no-such-repo", aws.ToString(assocOut.Errors[0].RepositoryName))

	disassocOut, err := client.BatchDisassociateApprovalRuleTemplateFromRepositories(
		ctx, &codecommitsdk.BatchDisassociateApprovalRuleTemplateFromRepositoriesInput{
			ApprovalRuleTemplateName: aws.String("batch-art"),
			RepositoryNames:          []string{"repo-1", "repo-2"},
		},
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"repo-1", "repo-2"}, disassocOut.DisassociatedRepositoryNames)
}

// TestSlice32CC_RepositoryTriggers drives PutRepositoryTriggers,
// GetRepositoryTriggers and TestRepositoryTriggers.
func TestSlice32CC_RepositoryTriggers(t *testing.T) {
	t.Parallel()

	client := newSlice32CodeCommitClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)

	trigger := types.RepositoryTrigger{
		Name:           aws.String("on-push"),
		DestinationArn: aws.String("arn:aws:sns:us-east-1:000000000000:topic"),
		Events:         []types.RepositoryTriggerEventEnum{types.RepositoryTriggerEventEnumAll},
		Branches:       []string{"main"},
	}

	_, err = client.PutRepositoryTriggers(ctx, &codecommitsdk.PutRepositoryTriggersInput{
		RepositoryName: aws.String("repo"),
		Triggers:       []types.RepositoryTrigger{trigger},
	})
	require.NoError(t, err)

	got, err := client.GetRepositoryTriggers(ctx, &codecommitsdk.GetRepositoryTriggersInput{
		RepositoryName: aws.String("repo"),
	})
	require.NoError(t, err)
	require.Len(t, got.Triggers, 1)
	assert.Equal(t, "on-push", aws.ToString(got.Triggers[0].Name))

	tested, err := client.TestRepositoryTriggers(ctx, &codecommitsdk.TestRepositoryTriggersInput{
		RepositoryName: aws.String("repo"),
		Triggers:       []types.RepositoryTrigger{trigger},
	})
	require.NoError(t, err)
	assert.Contains(t, tested.SuccessfulExecutions, "on-push")
}

// TestSlice32CC_CommentReactions drives PutCommentReaction and
// GetCommentReactions, and asserts the real nested ReactionForComment shape
// -- this test caught and locked a real bug: handleGetCommentReactions
// previously emitted a flat {emoji,userArn} object per reaction, but the
// real wire shape (codecommit@v1.36.4 types.ReactionForComment) nests a
// ReactionValueFormats object under "reaction" and the reacting users' ARNs
// under "reactionUsers" -- a typed client decoding the old shape always saw
// a nil Reaction and empty ReactionUsers regardless of what was stored.
func TestSlice32CC_CommentReactions(t *testing.T) {
	t.Parallel()

	client := newSlice32CodeCommitClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)

	comment, err := client.PostCommentForComparedCommit(ctx, &codecommitsdk.PostCommentForComparedCommitInput{
		RepositoryName: aws.String("repo"),
		AfterCommitId:  aws.String("abc123"),
		Content:        aws.String("nice work"),
	})
	require.NoError(t, err)
	commentID := comment.Comment.CommentId

	_, err = client.PutCommentReaction(ctx, &codecommitsdk.PutCommentReactionInput{
		CommentId:     commentID,
		ReactionValue: aws.String("THUMBSUP"),
	})
	require.NoError(t, err)

	got, err := client.GetCommentReactions(ctx, &codecommitsdk.GetCommentReactionsInput{CommentId: commentID})
	require.NoError(t, err)
	require.Len(t, got.ReactionsForComment, 1)

	entry := got.ReactionsForComment[0]
	require.NotNil(t, entry.Reaction, "Reaction must decode as a real nested object, not be nil")
	assert.Equal(t, "THUMBSUP", aws.ToString(entry.Reaction.Emoji))
}

// TestSlice32CC_ListPullRequestsFiltering drives ListPullRequests'
// pullRequestStatus filter.
func TestSlice32CC_ListPullRequestsFiltering(t *testing.T) {
	t.Parallel()

	client := newSlice32CodeCommitClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)

	_, err = client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("main"),
		PutFiles:       []types.PutFileEntry{{FilePath: aws.String("a.txt"), FileContent: []byte("a")}},
	})
	require.NoError(t, err)

	_, err = client.CreatePullRequest(ctx, &codecommitsdk.CreatePullRequestInput{
		Title: aws.String("open-pr"),
		Targets: []types.Target{
			{RepositoryName: aws.String("repo"), SourceReference: aws.String("main")},
		},
	})
	require.NoError(t, err)

	openOnly, err := client.ListPullRequests(ctx, &codecommitsdk.ListPullRequestsInput{
		RepositoryName:    aws.String("repo"),
		PullRequestStatus: types.PullRequestStatusEnumOpen,
	})
	require.NoError(t, err)
	assert.Len(t, openOnly.PullRequestIds, 1)

	closedOnly, err := client.ListPullRequests(ctx, &codecommitsdk.ListPullRequestsInput{
		RepositoryName:    aws.String("repo"),
		PullRequestStatus: types.PullRequestStatusEnumClosed,
	})
	require.NoError(t, err)
	assert.Empty(t, closedOnly.PullRequestIds)
}
