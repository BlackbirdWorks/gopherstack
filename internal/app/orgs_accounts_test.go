package app

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/organizations"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrganizations_CreatedAccountIsUsable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		roleName string
		wantRole string
		govCloud bool
	}{
		{name: "default role", wantRole: "OrganizationAccountAccessRole"},
		{name: "custom role", roleName: "CrossAcct", wantRole: "CrossAcct"},
		{name: "govcloud default role", govCloud: true, wantRole: "OrganizationAccountAccessRole"},
		{name: "govcloud custom role", govCloud: true, roleName: "GovAdmin", wantRole: "GovAdmin"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			stack := newMultiAccountStack(t)
			cfg := stack.config(t, defaultAccount, "", nil)
			org := organizations.NewFromConfig(cfg)

			_, err := org.CreateOrganization(ctx, &organizations.CreateOrganizationInput{})
			require.NoError(t, err)

			var statusID string

			if tt.govCloud {
				out, cerr := org.CreateGovCloudAccount(ctx, &organizations.CreateGovCloudAccountInput{
					AccountName: aws.String("gov"), Email: aws.String("gov@example.com"),
					RoleName: aws.String(tt.roleName),
				})
				require.NoError(t, cerr)

				statusID = aws.ToString(out.CreateAccountStatus.Id)
			} else {
				out, cerr := org.CreateAccount(ctx, &organizations.CreateAccountInput{
					AccountName: aws.String("dev"), Email: aws.String("dev@example.com"),
					RoleName: aws.String(tt.roleName),
				})
				require.NoError(t, cerr)

				statusID = aws.ToString(out.CreateAccountStatus.Id)
			}

			desc, err := org.DescribeCreateAccountStatus(ctx, &organizations.DescribeCreateAccountStatusInput{
				CreateAccountRequestId: aws.String(statusID),
			})
			require.NoError(t, err)
			require.Equal(t, "SUCCEEDED", string(desc.CreateAccountStatus.State))

			newAcct := aws.ToString(desc.CreateAccountStatus.AccountId)
			require.NotEmpty(t, newAcct)

			roleARN := "arn:aws:iam::" + newAcct + ":role/" + tt.wantRole

			assumed, err := sts.NewFromConfig(cfg).AssumeRole(ctx, &sts.AssumeRoleInput{
				RoleArn: aws.String(roleARN), RoleSessionName: aws.String("sess"),
			})
			require.NoError(t, err)

			creds := assumed.Credentials
			sessCfg := stack.config(t, aws.ToString(creds.AccessKeyId), aws.ToString(creds.SessionToken), nil)

			id, err := sts.NewFromConfig(sessCfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
			require.NoError(t, err)
			assert.Equal(t, newAcct, aws.ToString(id.Account))

			q, err := sqs.NewFromConfig(sessCfg).CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName: aws.String("in-new-account"),
			})
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(q.QueueUrl), "/"+newAcct+"/")

			iamClient := iam.NewFromConfig(stack.config(t, newAcct, "", nil))

			role, err := iamClient.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(tt.wantRole)})
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(role.Role.AssumeRolePolicyDocument), "root")

			attached, err := iamClient.ListAttachedRolePolicies(ctx, &iam.ListAttachedRolePoliciesInput{
				RoleName: aws.String(tt.wantRole),
			})
			require.NoError(t, err)
			require.Len(t, attached.AttachedPolicies, 1)
			assert.Equal(t, "AdministratorAccess", aws.ToString(attached.AttachedPolicies[0].PolicyName))
		})
	}
}
