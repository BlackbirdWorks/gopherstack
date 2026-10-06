package codecommit_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

const pagingRuleContent = `{"Version":"2018-11-08","Statements":[{"Type":"Approvers",` +
	`"NumberOfApprovalsNeeded":1,"ApprovalPoolMembers":["CodeCommitApprovers:000000000000:Mary"]}]}`

type ccPager func(
	ctx context.Context, c *codecommitsdk.Client, size int32, token *string,
) (int, *string, error)

// TestListOps_PageAndRejectBadTokens covers maxResults/nextToken (codecommit@v1.36.4 api_op_List*.go).
func TestListOps_PageAndRejectBadTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		list ccPager
		name string
	}{
		{name: "approval_rule_templates", list: func(
			ctx context.Context, c *codecommitsdk.Client, size int32, tok *string,
		) (int, *string, error) {
			out, err := c.ListApprovalRuleTemplates(ctx, &codecommitsdk.ListApprovalRuleTemplatesInput{
				MaxResults: aws.Int32(size), NextToken: tok,
			})
			if err != nil {
				return 0, nil, err
			}

			return len(out.ApprovalRuleTemplateNames), out.NextToken, nil
		},
		},
		{name: "associated_templates", list: func(
			ctx context.Context, c *codecommitsdk.Client, size int32, tok *string,
		) (int, *string, error) {
			out, err := c.ListAssociatedApprovalRuleTemplatesForRepository(ctx,
				&codecommitsdk.ListAssociatedApprovalRuleTemplatesForRepositoryInput{
					RepositoryName: aws.String("repo-a"), MaxResults: aws.Int32(size), NextToken: tok,
				})
			if err != nil {
				return 0, nil, err
			}

			return len(out.ApprovalRuleTemplateNames), out.NextToken, nil
		},
		},
		{name: "repositories_for_template", list: func(
			ctx context.Context, c *codecommitsdk.Client, size int32, tok *string,
		) (int, *string, error) {
			out, err := c.ListRepositoriesForApprovalRuleTemplate(ctx,
				&codecommitsdk.ListRepositoriesForApprovalRuleTemplateInput{
					ApprovalRuleTemplateName: aws.String("tmpl-a"), MaxResults: aws.Int32(size), NextToken: tok,
				})
			if err != nil {
				return 0, nil, err
			}

			return len(out.RepositoryNames), out.NextToken, nil
		},
		},
		{name: "repositories", list: func(
			ctx context.Context, c *codecommitsdk.Client, _ int32, tok *string,
		) (int, *string, error) {
			out, err := c.ListRepositories(ctx, &codecommitsdk.ListRepositoriesInput{NextToken: tok})
			if err != nil {
				return 0, nil, err
			}

			return len(out.Repositories), out.NextToken, nil
		},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCodeCommitClient(
				t,
				codecommit.NewHandler(codecommit.NewInMemoryBackend("000000000000", "us-east-1")),
			)
			seedPagingFixture(t, client)

			seen := 0

			var token *string

			for range 4 {
				n, next, err := tt.list(t.Context(), client, 2, token)
				require.NoError(t, err)

				seen += n
				if token = next; token == nil {
					break
				}
			}

			assert.Equal(t, 3, seen)
			assert.Nil(t, token)

			_, _, err := tt.list(t.Context(), client, 2, aws.String("not-a-number"))

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "InvalidContinuationTokenException", apiErr.ErrorCode())
		})
	}
}

func seedPagingFixture(t *testing.T, c *codecommitsdk.Client) {
	t.Helper()

	for _, n := range []string{"repo-a", "repo-b", "repo-c"} {
		_, err := c.CreateRepository(t.Context(), &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String(n)})
		require.NoError(t, err)
	}

	for _, n := range []string{"tmpl-a", "tmpl-b", "tmpl-c"} {
		_, err := c.CreateApprovalRuleTemplate(t.Context(), &codecommitsdk.CreateApprovalRuleTemplateInput{
			ApprovalRuleTemplateName:    aws.String(n),
			ApprovalRuleTemplateContent: aws.String(pagingRuleContent),
		})
		require.NoError(t, err)

		for _, r := range []string{"repo-a", "repo-b", "repo-c"} {
			if n == "tmpl-a" || r == "repo-a" {
				_, err = c.AssociateApprovalRuleTemplateWithRepository(t.Context(),
					&codecommitsdk.AssociateApprovalRuleTemplateWithRepositoryInput{
						ApprovalRuleTemplateName: aws.String(n), RepositoryName: aws.String(r),
					})
				require.NoError(t, err)
			}
		}
	}
}
