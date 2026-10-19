package iam_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestCreateRole_PersistsOptionalFields_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		desc string
		msd  int32
	}{
		{name: "description_and_msd", desc: "replicated role", msd: 7200},
		{name: "description_only", desc: "only a description"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestIAMClient(t, iam.NewHandler(iam.NewInMemoryBackend()))
			in := &iamsdk.CreateRoleInput{
				RoleName:                 aws.String("r1"),
				AssumeRolePolicyDocument: aws.String("{}"),
				Description:              aws.String(tt.desc),
				Tags:                     []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
			}
			if tt.msd != 0 {
				in.MaxSessionDuration = aws.Int32(tt.msd)
			}

			created, err := client.CreateRole(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, tt.desc, aws.ToString(created.Role.Description))

			got, err := client.GetRole(t.Context(), &iamsdk.GetRoleInput{RoleName: aws.String("r1")})
			require.NoError(t, err)
			assert.Equal(t, tt.desc, aws.ToString(got.Role.Description))
			require.Len(t, got.Role.Tags, 1)

			list, err := client.ListRoles(t.Context(), &iamsdk.ListRolesInput{})
			require.NoError(t, err)
			require.Len(t, list.Roles, 1)
			assert.Equal(t, tt.desc, aws.ToString(list.Roles[0].Description))

			if tt.msd != 0 {
				assert.Equal(t, tt.msd, aws.ToInt32(got.Role.MaxSessionDuration))
			}
		})
	}
}

func TestCreatePolicy_PersistsDescription_SDK(t *testing.T) {
	t.Parallel()

	client := newTestIAMClient(t, iam.NewHandler(iam.NewInMemoryBackend()))
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`

	created, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String("p1"),
		PolicyDocument: aws.String(doc),
		Description:    aws.String("policy desc"),
	})
	require.NoError(t, err)
	assert.Equal(t, "policy desc", aws.ToString(created.Policy.Description))

	got, err := client.GetPolicy(t.Context(), &iamsdk.GetPolicyInput{PolicyArn: created.Policy.Arn})
	require.NoError(t, err)
	assert.Equal(t, "policy desc", aws.ToString(got.Policy.Description))

	list, err := client.ListPolicies(t.Context(), &iamsdk.ListPoliciesInput{Scope: types.PolicyScopeTypeLocal})
	require.NoError(t, err)
	require.Len(t, list.Policies, 1)
	assert.Equal(t, "policy desc", aws.ToString(list.Policies[0].Description))
}
