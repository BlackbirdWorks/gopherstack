package codecommit_test

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/aws/aws-sdk-go-v2/service/codecommit/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_MergePullRequestCreatesMergeCommit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		merge       func(*codecommitsdk.Client, string) (*types.PullRequest, error)
		wantOption  types.MergeOptionTypeEnum
		wantParents int
		newCommit   bool
	}{
		{
			name: "squash",
			merge: func(c *codecommitsdk.Client, id string) (*types.PullRequest, error) {
				out, err := c.MergePullRequestBySquash(t.Context(), &codecommitsdk.MergePullRequestBySquashInput{
					PullRequestId: aws.String(id), RepositoryName: aws.String("repo"),
					AuthorName: aws.String("Ann"), Email: aws.String("ann@example.com"),
					CommitMessage: aws.String("squashed"),
				})
				if err != nil {
					return nil, err
				}

				return out.PullRequest, nil
			},
			wantOption:  types.MergeOptionTypeEnumSquashMerge,
			wantParents: 1,
			newCommit:   true,
		},
		{
			name: "three_way",
			merge: func(c *codecommitsdk.Client, id string) (*types.PullRequest, error) {
				out, err := c.MergePullRequestByThreeWay(t.Context(), &codecommitsdk.MergePullRequestByThreeWayInput{
					PullRequestId: aws.String(id), RepositoryName: aws.String("repo"),
					AuthorName: aws.String("Ann"), Email: aws.String("ann@example.com"),
					CommitMessage: aws.String("squashed"),
				})
				if err != nil {
					return nil, err
				}

				return out.PullRequest, nil
			},
			wantOption:  types.MergeOptionTypeEnumThreeWayMerge,
			wantParents: 2,
			newCommit:   true,
		},
		{
			name: "fast_forward",
			merge: func(c *codecommitsdk.Client, id string) (*types.PullRequest, error) {
				out, err := c.MergePullRequestByFastForward(
					t.Context(),
					&codecommitsdk.MergePullRequestByFastForwardInput{
						PullRequestId: aws.String(id), RepositoryName: aws.String("repo"),
					},
				)
				if err != nil {
					return nil, err
				}

				return out.PullRequest, nil
			},
			wantOption: types.MergeOptionTypeEnumFastForwardMerge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateRepository(
				ctx,
				&codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo")},
			)
			require.NoError(t, err)
			base, err := client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
				RepositoryName: aws.String("repo"), BranchName: aws.String("main"),
				PutFiles: []types.PutFileEntry{{FilePath: aws.String("a.txt"), FileContent: []byte("a")}},
			})
			require.NoError(t, err)
			_, err = client.CreateBranch(ctx, &codecommitsdk.CreateBranchInput{
				RepositoryName: aws.String("repo"), BranchName: aws.String("feature"), CommitId: base.CommitId,
			})
			require.NoError(t, err)
			feature, err := client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
				RepositoryName: aws.String("repo"), BranchName: aws.String("feature"),
				ParentCommitId: base.CommitId,
				PutFiles:       []types.PutFileEntry{{FilePath: aws.String("b.txt"), FileContent: []byte("b")}},
			})
			require.NoError(t, err)

			created, err := client.CreatePullRequest(ctx, &codecommitsdk.CreatePullRequestInput{
				Title: aws.String("t"),
				Targets: []types.Target{{
					RepositoryName: aws.String("repo"), SourceReference: aws.String("feature"),
					DestinationReference: aws.String("main"),
				}},
			})
			require.NoError(t, err)
			require.NotNil(t, created.PullRequest.PullRequestTargets[0].MergeMetadata)
			assert.False(t, created.PullRequest.PullRequestTargets[0].MergeMetadata.IsMerged)
			target := created.PullRequest.PullRequestTargets[0]
			assert.Equal(t, aws.ToString(feature.CommitId), aws.ToString(target.SourceCommit))
			assert.Equal(t, aws.ToString(base.CommitId), aws.ToString(target.DestinationCommit))
			assert.Equal(t, aws.ToString(base.CommitId), aws.ToString(target.MergeBase))

			merged, err := tt.merge(client, aws.ToString(created.PullRequest.PullRequestId))
			require.NoError(t, err)

			meta := merged.PullRequestTargets[0].MergeMetadata
			require.NotNil(t, meta)
			assert.True(t, meta.IsMerged)
			assert.Equal(t, tt.wantOption, meta.MergeOption)

			main, err := client.GetBranch(ctx, &codecommitsdk.GetBranchInput{
				RepositoryName: aws.String("repo"), BranchName: aws.String("main"),
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(meta.MergeCommitId), aws.ToString(main.Branch.CommitId))

			if !tt.newCommit {
				assert.Equal(t, aws.ToString(feature.CommitId), aws.ToString(meta.MergeCommitId))

				return
			}

			assert.NotEqual(t, aws.ToString(feature.CommitId), aws.ToString(meta.MergeCommitId))
			got, err := client.GetCommit(ctx, &codecommitsdk.GetCommitInput{
				RepositoryName: aws.String("repo"), CommitId: meta.MergeCommitId,
			})
			require.NoError(t, err)
			assert.Len(t, got.Commit.Parents, tt.wantParents)
			assert.Equal(t, "squashed", aws.ToString(got.Commit.Message))
			assert.Equal(t, "Ann", aws.ToString(got.Commit.Author.Name))
		})
	}
}

func TestRealClient_UpdateApprovalRuleContentChecksExistingSha(t *testing.T) {
	t.Parallel()

	oldContent := approvalRuleContent(1)
	newContent := approvalRuleContent(2)

	tests := []struct {
		setup  func(t *testing.T, c *codecommitsdk.Client) (sha string, update func(existing string) (string, error))
		name   string
		stale  bool
		wantOK bool
	}{
		{
			name: "template_stale_sha_rejected", stale: true,
			setup: templateShaSetup(oldContent, newContent),
		},
		{
			name: "template_matching_sha_applied", wantOK: true,
			setup: templateShaSetup(oldContent, newContent),
		},
		{
			name: "pull_request_rule_stale_sha_rejected", stale: true,
			setup: prRuleShaSetup(oldContent, newContent),
		},
		{
			name: "pull_request_rule_matching_sha_applied", wantOK: true,
			setup: prRuleShaSetup(oldContent, newContent),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			sha, update := tt.setup(t, client)
			require.NotEmpty(t, sha)

			existing := sha
			if tt.stale {
				existing = "0000"
			}

			gotSha, err := update(existing)
			if tt.wantOK {
				require.NoError(t, err)
				assert.NotEmpty(t, gotSha)
				assert.NotEqual(t, sha, gotSha, "content change must change the sha")

				_, err = update(sha)
				require.Error(t, err, "the old sha is now stale")

				return
			}

			var want *types.InvalidRuleContentSha256Exception
			require.ErrorAs(t, err, &want)
		})
	}
}

func templateShaSetup(
	oldContent, newContent string,
) func(*testing.T, *codecommitsdk.Client) (string, func(string) (string, error)) {
	return func(t *testing.T, c *codecommitsdk.Client) (string, func(string) (string, error)) {
		t.Helper()

		created, err := c.CreateApprovalRuleTemplate(t.Context(), &codecommitsdk.CreateApprovalRuleTemplateInput{
			ApprovalRuleTemplateName: aws.String("tmpl"), ApprovalRuleTemplateContent: aws.String(oldContent),
		})
		require.NoError(t, err)

		return aws.ToString(created.ApprovalRuleTemplate.RuleContentSha256), func(existing string) (string, error) {
			out, updErr := c.UpdateApprovalRuleTemplateContent(
				t.Context(),
				&codecommitsdk.UpdateApprovalRuleTemplateContentInput{
					ApprovalRuleTemplateName:  aws.String("tmpl"),
					NewRuleContent:            aws.String(newContent),
					ExistingRuleContentSha256: aws.String(existing),
				},
			)
			if updErr != nil {
				return "", updErr
			}

			return aws.ToString(out.ApprovalRuleTemplate.RuleContentSha256), nil
		}
	}
}

func prRuleShaSetup(
	oldContent, newContent string,
) func(*testing.T, *codecommitsdk.Client) (string, func(string) (string, error)) {
	return func(t *testing.T, c *codecommitsdk.Client) (string, func(string) (string, error)) {
		t.Helper()

		ctx := t.Context()
		_, err := c.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo")})
		require.NoError(t, err)
		_, err = c.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
			RepositoryName: aws.String("repo"), BranchName: aws.String("main"),
			PutFiles: []types.PutFileEntry{{FilePath: aws.String("a.txt"), FileContent: []byte("a")}},
		})
		require.NoError(t, err)
		seedFeatureBranch(t, c, "repo")
		pr, err := c.CreatePullRequest(ctx, &codecommitsdk.CreatePullRequestInput{
			Title:   aws.String("t"),
			Targets: []types.Target{{RepositoryName: aws.String("repo"), SourceReference: aws.String("feature")}},
		})
		require.NoError(t, err)
		rule, err := c.CreatePullRequestApprovalRule(ctx, &codecommitsdk.CreatePullRequestApprovalRuleInput{
			PullRequestId: pr.PullRequest.PullRequestId, ApprovalRuleName: aws.String("rule"),
			ApprovalRuleContent: aws.String(oldContent),
		})
		require.NoError(t, err)

		return aws.ToString(rule.ApprovalRule.RuleContentSha256), func(existing string) (string, error) {
			out, updErr := c.UpdatePullRequestApprovalRuleContent(
				ctx,
				&codecommitsdk.UpdatePullRequestApprovalRuleContentInput{
					PullRequestId:             pr.PullRequest.PullRequestId,
					ApprovalRuleName:          aws.String("rule"),
					NewRuleContent:            aws.String(newContent),
					ExistingRuleContentSha256: aws.String(existing),
				},
			)
			if updErr != nil {
				return "", updErr
			}

			return aws.ToString(out.ApprovalRule.RuleContentSha256), nil
		}
	}
}

func TestRealClient_MergeBaseCommitID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		base func(c *codecommitsdk.Client, src, dst string) (string, error)
		name string
	}{
		{name: "get_merge_options", base: func(c *codecommitsdk.Client, src, dst string) (string, error) {
			out, err := c.GetMergeOptions(t.Context(), &codecommitsdk.GetMergeOptionsInput{
				RepositoryName: aws.String("repo"), SourceCommitSpecifier: aws.String(src),
				DestinationCommitSpecifier: aws.String(dst),
			})
			if err != nil {
				return "", err
			}

			return aws.ToString(out.BaseCommitId), nil
		}},
		{name: "get_merge_commit", base: func(c *codecommitsdk.Client, src, dst string) (string, error) {
			out, err := c.GetMergeCommit(t.Context(), &codecommitsdk.GetMergeCommitInput{
				RepositoryName: aws.String("repo"), SourceCommitSpecifier: aws.String(src),
				DestinationCommitSpecifier: aws.String(dst),
			})
			if err != nil {
				return "", err
			}

			return aws.ToString(out.BaseCommitId), nil
		}},
		{name: "get_merge_conflicts", base: func(c *codecommitsdk.Client, src, dst string) (string, error) {
			out, err := c.GetMergeConflicts(t.Context(), &codecommitsdk.GetMergeConflictsInput{
				RepositoryName: aws.String("repo"), SourceCommitSpecifier: aws.String(src),
				DestinationCommitSpecifier: aws.String(dst), MergeOption: types.MergeOptionTypeEnumThreeWayMerge,
			})
			if err != nil {
				return "", err
			}

			return aws.ToString(out.BaseCommitId), nil
		}},
		{name: "batch_describe_merge_conflicts", base: func(c *codecommitsdk.Client, src, dst string) (string, error) {
			out, err := c.BatchDescribeMergeConflicts(t.Context(), &codecommitsdk.BatchDescribeMergeConflictsInput{
				RepositoryName: aws.String("repo"), SourceCommitSpecifier: aws.String(src),
				DestinationCommitSpecifier: aws.String(dst), MergeOption: types.MergeOptionTypeEnumThreeWayMerge,
			})
			if err != nil {
				return "", err
			}

			return aws.ToString(out.BaseCommitId), nil
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateRepository(
				ctx,
				&codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo")},
			)
			require.NoError(t, err)
			base, err := client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
				RepositoryName: aws.String("repo"), BranchName: aws.String("main"),
				PutFiles: []types.PutFileEntry{{FilePath: aws.String("a.txt"), FileContent: []byte("a")}},
			})
			require.NoError(t, err)
			_, err = client.CreateBranch(ctx, &codecommitsdk.CreateBranchInput{
				RepositoryName: aws.String("repo"), BranchName: aws.String("feature"), CommitId: base.CommitId,
			})
			require.NoError(t, err)
			for _, branch := range []string{"feature", "main"} {
				_, err = client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
					RepositoryName: aws.String("repo"), BranchName: aws.String(branch),
					ParentCommitId: base.CommitId,
					PutFiles: []types.PutFileEntry{
						{FilePath: aws.String(branch + ".txt"), FileContent: []byte(branch)},
					},
				})
				require.NoError(t, err)
			}

			got, err := tt.base(client, "feature", "main")
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(base.CommitId), got, "base is the fork point, not either tip")

			_, err = tt.base(client, "no-such-branch", "main")
			var notFound *types.CommitDoesNotExistException
			require.ErrorAs(t, err, &notFound)
		})
	}
}

func approvalRuleContent(approvals int) string {
	return `{"Version":"2018-11-08","Statements":[{"Type":"Approvers","NumberOfApprovalsNeeded":` +
		strconv.Itoa(approvals) +
		`,"ApprovalPoolMembers":["arn:aws:sts::123456789012:assumed-role/Dev/*"]}]}`
}
