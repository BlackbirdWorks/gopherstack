package codecommit_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/aws/aws-sdk-go-v2/service/codecommit/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

const wireTestRegion = "us-east-1"

// newTestCodeCommitClient stands up the real aws-sdk-go-v2 codecommit client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestCodeCommitClient(t *testing.T, h *codecommit.Handler) *codecommitsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(wireTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return codecommitsdk.NewFromConfig(cfg, func(o *codecommitsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestUpdateRepositoryEncryptionKey_SurvivesWireConversion drives
// UpdateRepositoryEncryptionKey through the real SDK client and asserts the
// response echoes RepositoryId, KmsKeyId and OriginalKmsKeyId. The real
// UpdateRepositoryEncryptionKeyOutput carries all three
// (api_op_UpdateRepositoryEncryptionKey.go:49); the handler previously
// returned an empty envelope, so a client reading any of these three fields
// always saw a zero value even though the operation succeeded.
func TestUpdateRepositoryEncryptionKey_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	client := newTestCodeCommitClient(
		t, codecommit.NewHandler(codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)),
	)

	created, err := client.CreateRepository(t.Context(), &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String("enc-key-repo"),
	})
	require.NoError(t, err)
	repositoryID := aws.ToString(created.RepositoryMetadata.RepositoryId)

	const firstKey = "arn:aws:kms:us-east-1:123456789012:key/first"

	first, err := client.UpdateRepositoryEncryptionKey(
		t.Context(), &codecommitsdk.UpdateRepositoryEncryptionKeyInput{
			RepositoryName: aws.String("enc-key-repo"),
			KmsKeyId:       aws.String(firstKey),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, repositoryID, aws.ToString(first.RepositoryId))
	assert.Equal(t, firstKey, aws.ToString(first.KmsKeyId))
	assert.Empty(t, aws.ToString(first.OriginalKmsKeyId), "no key was set before this call")

	const secondKey = "arn:aws:kms:us-east-1:123456789012:key/second"

	second, err := client.UpdateRepositoryEncryptionKey(
		t.Context(), &codecommitsdk.UpdateRepositoryEncryptionKeyInput{
			RepositoryName: aws.String("enc-key-repo"),
			KmsKeyId:       aws.String(secondKey),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, repositoryID, aws.ToString(second.RepositoryId))
	assert.Equal(t, secondKey, aws.ToString(second.KmsKeyId))
	assert.Equal(t, firstKey, aws.ToString(second.OriginalKmsKeyId))
}

// TestCreateCommit_FilesAddedAndDeleted_AbsolutePath_SurvivesWireConversion
// drives CreateCommit through the real SDK client and asserts FilesAdded and
// FilesDeleted report AbsolutePath. The real FileMetadata (used for all
// three of CreateCommitOutput's FilesAdded/FilesDeleted/FilesUpdated) has
// exactly three wire keys -- absolutePath, blobId, fileMode
// (deserializers.go's awsAwsjson11_deserializeDocumentFileMetadata); there
// is no "filePath". The handler emitted "filePath" for FilesDeleted, so a
// client reading FilesDeleted[i].AbsolutePath always saw "".
func TestCreateCommit_FilesAddedAndDeleted_AbsolutePath_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	client := newTestCodeCommitClient(
		t, codecommit.NewHandler(codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)),
	)

	_, err := client.CreateRepository(t.Context(), &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String("cc-abspath-repo"),
	})
	require.NoError(t, err)

	added, err := client.CreateCommit(t.Context(), &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("cc-abspath-repo"),
		BranchName:     aws.String("main"),
		PutFiles: []types.PutFileEntry{
			{FilePath: aws.String("keep.txt"), FileContent: []byte("hello")},
		},
	})
	require.NoError(t, err)
	require.Len(t, added.FilesAdded, 1)
	assert.Equal(t, "keep.txt", aws.ToString(added.FilesAdded[0].AbsolutePath))
	assert.NotEmpty(t, aws.ToString(added.FilesAdded[0].BlobId))

	deleted, err := client.CreateCommit(t.Context(), &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("cc-abspath-repo"),
		BranchName:     aws.String("main"),
		ParentCommitId: added.CommitId,
		DeleteFiles: []types.DeleteFileEntry{
			{FilePath: aws.String("keep.txt")},
		},
	})
	require.NoError(t, err)
	require.Len(t, deleted.FilesDeleted, 1)
	assert.Equal(t, "keep.txt", aws.ToString(deleted.FilesDeleted[0].AbsolutePath))
	assert.NotEmpty(t, aws.ToString(deleted.FilesDeleted[0].BlobId))
}

// TestDeleteApprovalRuleTemplate_SurvivesWireConversion drives
// DeleteApprovalRuleTemplate through the real SDK client and asserts the
// response echoes ApprovalRuleTemplateId. The real
// DeleteApprovalRuleTemplateOutput carries it as a required field
// (api_op_DeleteApprovalRuleTemplate.go:38); the handler previously
// returned an empty envelope, so a client reading it always saw "".
func TestDeleteApprovalRuleTemplate_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	client := newTestCodeCommitClient(
		t, codecommit.NewHandler(codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)),
	)

	created, err := client.CreateApprovalRuleTemplate(t.Context(), &codecommitsdk.CreateApprovalRuleTemplateInput{
		ApprovalRuleTemplateName:    aws.String("art-1"),
		ApprovalRuleTemplateContent: aws.String(`{"Version":"2018-11-08","Statements":[]}`),
	})
	require.NoError(t, err)
	wantID := aws.ToString(created.ApprovalRuleTemplate.ApprovalRuleTemplateId)
	require.NotEmpty(t, wantID)

	deleted, err := client.DeleteApprovalRuleTemplate(
		t.Context(), &codecommitsdk.DeleteApprovalRuleTemplateInput{ApprovalRuleTemplateName: aws.String("art-1")},
	)
	require.NoError(t, err)
	assert.Equal(t, wantID, aws.ToString(deleted.ApprovalRuleTemplateId))
}

// TestPullRequestApprovalRule_UpdateAndDelete_SurviveWireConversion drives
// UpdatePullRequestApprovalRuleContent and DeletePullRequestApprovalRule
// through the real SDK client. Both real outputs carry required data --
// the full updated ApprovalRule, and the deleted rule's ApprovalRuleId,
// respectively (api_op_UpdatePullRequestApprovalRuleContent.go:82,
// api_op_DeletePullRequestApprovalRule.go:48) -- that the handler
// previously dropped by returning an empty envelope.
func TestPullRequestApprovalRule_UpdateAndDelete_SurviveWireConversion(t *testing.T) {
	t.Parallel()

	client := newTestCodeCommitClient(
		t, codecommit.NewHandler(codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)),
	)

	_, err := client.CreateRepository(t.Context(), &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String("pr-rule-repo"),
	})
	require.NoError(t, err)

	_, err = client.CreateCommit(t.Context(), &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("pr-rule-repo"),
		BranchName:     aws.String("main"),
		PutFiles: []types.PutFileEntry{
			{FilePath: aws.String("a.txt"), FileContent: []byte("a")},
		},
	})
	require.NoError(t, err)

	mainBranch, err := client.GetBranch(t.Context(), &codecommitsdk.GetBranchInput{
		RepositoryName: aws.String("pr-rule-repo"),
		BranchName:     aws.String("main"),
	})
	require.NoError(t, err)

	_, err = client.CreateBranch(t.Context(), &codecommitsdk.CreateBranchInput{
		RepositoryName: aws.String("pr-rule-repo"),
		BranchName:     aws.String("feature"),
		CommitId:       mainBranch.Branch.CommitId,
	})
	require.NoError(t, err)

	_, err = client.CreateCommit(t.Context(), &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("pr-rule-repo"),
		BranchName:     aws.String("feature"),
		ParentCommitId: mainBranch.Branch.CommitId,
		PutFiles: []types.PutFileEntry{
			{FilePath: aws.String("b.txt"), FileContent: []byte("b")},
		},
	})
	require.NoError(t, err)

	pr, err := client.CreatePullRequest(t.Context(), &codecommitsdk.CreatePullRequestInput{
		Title: aws.String("test PR"),
		Targets: []types.Target{
			{
				RepositoryName:       aws.String("pr-rule-repo"),
				SourceReference:      aws.String("feature"),
				DestinationReference: aws.String("main"),
			},
		},
	})
	require.NoError(t, err)
	prID := pr.PullRequest.PullRequestId

	const ruleContent = `{"Version":"2018-11-08","DestinationReferences":["refs/heads/main"],"Statements":[]}`

	rule, err := client.CreatePullRequestApprovalRule(
		t.Context(), &codecommitsdk.CreatePullRequestApprovalRuleInput{
			PullRequestId:       prID,
			ApprovalRuleName:    aws.String("rule-1"),
			ApprovalRuleContent: aws.String(ruleContent),
		},
	)
	require.NoError(t, err)
	ruleID := aws.ToString(rule.ApprovalRule.ApprovalRuleId)
	require.NotEmpty(t, ruleID)

	const updatedRuleContent = `{"Version":"2018-11-08","DestinationReferences":["refs/heads/main"],"Statements":[{}]}`

	updated, err := client.UpdatePullRequestApprovalRuleContent(
		t.Context(), &codecommitsdk.UpdatePullRequestApprovalRuleContentInput{
			PullRequestId:    prID,
			ApprovalRuleName: aws.String("rule-1"),
			NewRuleContent:   aws.String(updatedRuleContent),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, updated.ApprovalRule)
	assert.Equal(t, ruleID, aws.ToString(updated.ApprovalRule.ApprovalRuleId))
	assert.Equal(t, "rule-1", aws.ToString(updated.ApprovalRule.ApprovalRuleName))

	deleted, err := client.DeletePullRequestApprovalRule(
		t.Context(), &codecommitsdk.DeletePullRequestApprovalRuleInput{
			PullRequestId:    prID,
			ApprovalRuleName: aws.String("rule-1"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, ruleID, aws.ToString(deleted.ApprovalRuleId))
}
