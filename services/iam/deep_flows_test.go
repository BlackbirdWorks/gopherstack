package iam_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const deepTrust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

const deepPolicyDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`

func TestDeepFlows_BoundaryMustExist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		boundary string
		wantCode string
	}{
		{name: "missing_customer", boundary: "arn:aws:iam::000000000000:policy/nope", wantCode: "NoSuchEntity"},
		{name: "aws_managed", boundary: "arn:aws:iam::aws:policy/ReadOnlyAccess"},
	}

	for _, tt := range tests {
		t.Run("create_role_"+tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			client := newTestIAMClient(t, h)

			_, err := client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
				RoleName:                 aws.String("r"),
				AssumeRolePolicyDocument: aws.String(deepTrust),
				PermissionsBoundary:      aws.String(tt.boundary),
			})
			if tt.wantCode != "" {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
		})

		t.Run("put_user_"+tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			client := newTestIAMClient(t, h)

			_, err := client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("u")})
			require.NoError(t, err)

			_, err = client.PutUserPermissionsBoundary(t.Context(), &iamsdk.PutUserPermissionsBoundaryInput{
				UserName: aws.String("u"), PermissionsBoundary: aws.String(tt.boundary),
			})
			if tt.wantCode != "" {
				var nse *types.NoSuchEntityException
				require.ErrorAs(t, err, &nse)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDeepFlows_DeletePolicyWithVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		extraVersion bool
		wantConflict bool
	}{
		{name: "single_version", extraVersion: false},
		{name: "extra_version", extraVersion: true, wantConflict: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			client := newTestIAMClient(t, h)

			pol, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
				PolicyName: aws.String("p"), PolicyDocument: aws.String(deepPolicyDoc),
			})
			require.NoError(t, err)

			if tt.extraVersion {
				_, err = client.CreatePolicyVersion(t.Context(), &iamsdk.CreatePolicyVersionInput{
					PolicyArn: pol.Policy.Arn, PolicyDocument: aws.String(deepPolicyDoc),
				})
				require.NoError(t, err)
			}

			_, err = client.DeletePolicy(t.Context(), &iamsdk.DeletePolicyInput{PolicyArn: pol.Policy.Arn})
			if tt.wantConflict {
				var dce *types.DeleteConflictException
				require.ErrorAs(t, err, &dce)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDeepFlows_DuplicateTagKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tags    []types.Tag
		wantErr bool
	}{
		{name: "unique", tags: []types.Tag{{Key: aws.String("a"), Value: aws.String("1")}}},
		{
			name: "exact_dup",
			tags: []types.Tag{
				{Key: aws.String("a"), Value: aws.String("1")}, {Key: aws.String("a"), Value: aws.String("2")},
			},
			wantErr: true,
		},
		{
			name: "case_dup",
			tags: []types.Tag{
				{Key: aws.String("a"), Value: aws.String("1")}, {Key: aws.String("A"), Value: aws.String("2")},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			client := newTestIAMClient(t, h)

			_, err := client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
				RoleName:                 aws.String("r"),
				AssumeRolePolicyDocument: aws.String(deepTrust),
				Tags:                     tt.tags,
			})
			if tt.wantErr {
				var ii *types.InvalidInputException
				require.ErrorAs(t, err, &ii)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDeepFlows_ServiceLinkedRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		service  string
		suffix   string
		wantName string
	}{
		{name: "elb", service: "elasticloadbalancing.amazonaws.com", wantName: "AWSServiceRoleForElasticLoadBalancing"},
		{name: "ecs", service: "ecs.amazonaws.com", wantName: "AWSServiceRoleForECS"},
		{
			name: "suffix", service: "autoscaling.amazonaws.com", suffix: "foo",
			wantName: "AWSServiceRoleForAutoScaling_foo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			client := newTestIAMClient(t, h)

			in := &iamsdk.CreateServiceLinkedRoleInput{
				AWSServiceName: aws.String(tt.service), Description: aws.String("desc"),
			}
			if tt.suffix != "" {
				in.CustomSuffix = aws.String(tt.suffix)
			}

			out, err := client.CreateServiceLinkedRole(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, aws.ToString(out.Role.RoleName))
			assert.Equal(t, "desc", aws.ToString(out.Role.Description))
			assert.EqualValues(t, 3600, aws.ToInt32(out.Role.MaxSessionDuration))

			del, err := client.DeleteServiceLinkedRole(t.Context(), &iamsdk.DeleteServiceLinkedRoleInput{
				RoleName: out.Role.RoleName,
			})
			require.NoError(t, err)

			st, err := client.GetServiceLinkedRoleDeletionStatus(t.Context(),
				&iamsdk.GetServiceLinkedRoleDeletionStatusInput{DeletionTaskId: del.DeletionTaskId})
			require.NoError(t, err)
			assert.Equal(t, types.DeletionTaskStatusTypeSucceeded, st.Status)

			_, err = client.DeleteServiceLinkedRole(t.Context(), &iamsdk.DeleteServiceLinkedRoleInput{
				RoleName: out.Role.RoleName,
			})

			var nse *types.NoSuchEntityException
			require.ErrorAs(t, err, &nse)

			_, err = client.GetServiceLinkedRoleDeletionStatus(t.Context(),
				&iamsdk.GetServiceLinkedRoleDeletionStatusInput{DeletionTaskId: aws.String("bogus")})
			require.ErrorAs(t, err, &nse)
		})
	}
}
