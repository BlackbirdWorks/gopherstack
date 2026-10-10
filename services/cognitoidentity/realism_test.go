package cognitoidentity_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidentitysdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateIdentityPoolNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		poolName string
		devName  string
		wantErr  bool
	}{
		{name: "valid_with_space", poolName: "my pool-1.x"},
		{name: "bang_rejected", poolName: "bad name!", wantErr: true},
		{name: "too_long", poolName: strings.Repeat("x", 129), wantErr: true},
		{name: "max_len", poolName: strings.Repeat("x", 128)},
		{name: "bad_developer_provider", poolName: "p", devName: "bad name", wantErr: true},
		{name: "good_developer_provider", poolName: "p", devName: "login.my-app_1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			in := &cognitoidentitysdk.CreateIdentityPoolInput{
				IdentityPoolName:               aws.String(tt.poolName),
				AllowUnauthenticatedIdentities: true,
			}
			if tt.devName != "" {
				in.DeveloperProviderName = aws.String(tt.devName)
			}

			_, err := client.CreateIdentityPool(t.Context(), in)
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "InvalidParameterException", apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception:")
		})
	}
}

func TestListIdentityPoolsPaging(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	for _, n := range []string{"a", "b", "c"} {
		_, err := client.CreateIdentityPool(t.Context(), &cognitoidentitysdk.CreateIdentityPoolInput{
			IdentityPoolName: aws.String(n), AllowUnauthenticatedIdentities: true,
		})
		require.NoError(t, err)
	}

	first, err := client.ListIdentityPools(t.Context(), &cognitoidentitysdk.ListIdentityPoolsInput{
		MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, first.IdentityPools, 2)
	require.NotEmpty(t, aws.ToString(first.NextToken))
	assert.NotEqual(t, "b", aws.ToString(first.NextToken), "token must be opaque")

	second, err := client.ListIdentityPools(t.Context(), &cognitoidentitysdk.ListIdentityPoolsInput{
		MaxResults: aws.Int32(2), NextToken: first.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, second.IdentityPools, 1)
	assert.Equal(t, "c", aws.ToString(second.IdentityPools[0].IdentityPoolName))

	for _, in := range []*cognitoidentitysdk.ListIdentityPoolsInput{
		{MaxResults: aws.Int32(10), NextToken: aws.String("not base64 !")},
		{MaxResults: aws.Int32(61)},
	} {
		_, err = client.ListIdentityPools(t.Context(), in)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidParameterException", apiErr.ErrorCode())
	}
}

func TestNotFoundMessageDropsCodePrefix(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	_, err := client.DescribeIdentityPool(t.Context(), &cognitoidentitysdk.DescribeIdentityPoolInput{
		IdentityPoolId: aws.String("us-east-1:11111111-1111-1111-1111-111111111111"),
	})

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
	assert.NotContains(t, apiErr.ErrorMessage(), "Exception:")
}
