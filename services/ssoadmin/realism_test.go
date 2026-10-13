package ssoadmin_test

import (
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssoadminsdk "github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	instanceARNPattern      = regexp.MustCompile(`^arn:aws:sso:::instance/(sso)?ins-[a-zA-Z0-9-.]{16}$`)
	permissionSetARNPattern = regexp.MustCompile(
		`^arn:aws:sso:::permissionSet/(sso)?ins-[a-zA-Z0-9-.]{16}/ps-[a-zA-Z0-9-./]{16}$`)
)

func TestGeneratedARNsMatchSDKPatterns(t *testing.T) {
	t.Parallel()

	client := newTestSSOAdminClient(t, newTestHandler())

	list, err := client.ListInstances(t.Context(), &ssoadminsdk.ListInstancesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, list.Instances)

	for _, inst := range list.Instances {
		assert.Regexp(t, instanceARNPattern, aws.ToString(inst.InstanceArn))
	}

	ps, err := client.CreatePermissionSet(t.Context(), &ssoadminsdk.CreatePermissionSetInput{
		InstanceArn: list.Instances[0].InstanceArn,
		Name:        aws.String("ps-format"),
	})
	require.NoError(t, err)
	assert.Regexp(t, permissionSetARNPattern, aws.ToString(ps.PermissionSet.PermissionSetArn))
}

func TestListPagingValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		token   *string
		max     *int32
		name    string
		wantErr bool
	}{
		{name: "bad_token", token: aws.String("not base64!!"), wantErr: true},
		{name: "max_zero", max: aws.Int32(0), wantErr: true},
		{name: "max_over", max: aws.Int32(101), wantErr: true},
		{name: "max_ok", max: aws.Int32(100)},
		{name: "none"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSSOAdminClient(t, newTestHandler())
			list, err := client.ListInstances(t.Context(), &ssoadminsdk.ListInstancesInput{})
			require.NoError(t, err)

			_, err = client.ListPermissionSets(t.Context(), &ssoadminsdk.ListPermissionSetsInput{
				InstanceArn: list.Instances[0].InstanceArn,
				NextToken:   tt.token,
				MaxResults:  tt.max,
			})
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "ValidationException", apiErr.ErrorCode())
		})
	}
}

func TestCreatePermissionSetUnknownInstanceMessage(t *testing.T) {
	t.Parallel()

	client := newTestSSOAdminClient(t, newTestHandler())
	_, err := client.CreatePermissionSet(t.Context(), &ssoadminsdk.CreatePermissionSetInput{
		InstanceArn: aws.String("arn:aws:sso:::instance/ssoins-0123456789abcdef"),
		Name:        aws.String("x"),
	})

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
	assert.Contains(t, apiErr.ErrorMessage(), "instance")
}
