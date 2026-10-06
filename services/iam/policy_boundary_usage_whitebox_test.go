package iam

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPolicy_PermissionsBoundaryUsageCount(t *testing.T) {
	t.Parallel()

	const doc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`

	tests := []struct {
		name  string
		users int
		roles int
	}{
		{name: "unused"},
		{name: "users_and_roles", users: 2, roles: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newSigningCertTestClient(t, NewHandler(NewInMemoryBackend()))

			pol, err := client.CreatePolicy(ctx, &iamsdk.CreatePolicyInput{
				PolicyName: aws.String("bnd"), PolicyDocument: aws.String(doc),
			})
			require.NoError(t, err)

			arn := pol.Policy.Arn

			for i := range tt.users {
				name := aws.String("u" + string(rune('a'+i)))
				_, err = client.CreateUser(ctx, &iamsdk.CreateUserInput{UserName: name})
				require.NoError(t, err)
				_, err = client.PutUserPermissionsBoundary(ctx, &iamsdk.PutUserPermissionsBoundaryInput{
					UserName: name, PermissionsBoundary: arn,
				})
				require.NoError(t, err)
			}

			for range tt.roles {
				_, err = client.CreateRole(ctx, &iamsdk.CreateRoleInput{
					RoleName:                 aws.String("r1"),
					AssumeRolePolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
					PermissionsBoundary:      arn,
				})
				require.NoError(t, err)
			}

			want := int32(tt.users + tt.roles)

			got, err := client.GetPolicy(ctx, &iamsdk.GetPolicyInput{PolicyArn: arn})
			require.NoError(t, err)
			assert.Equal(t, want, aws.ToInt32(got.Policy.PermissionsBoundaryUsageCount))

			list, err := client.ListPolicies(ctx, &iamsdk.ListPoliciesInput{})
			require.NoError(t, err)
			require.Len(t, list.Policies, 1)
			assert.Equal(t, want, aws.ToInt32(list.Policies[0].PermissionsBoundaryUsageCount))

			details, err := client.GetAccountAuthorizationDetails(ctx, &iamsdk.GetAccountAuthorizationDetailsInput{})
			require.NoError(t, err)
			require.Len(t, details.Policies, 1)
			assert.Equal(t, want, aws.ToInt32(details.Policies[0].PermissionsBoundaryUsageCount))
		})
	}
}
