package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserPool_IssuerAndKeyConfigurationRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestCognitoIDPClient(t, newTestHandler(t))
	ctx := t.Context()

	created, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
		PoolName:            aws.String("issuer"),
		IssuerConfiguration: &types.IssuerConfigurationType{Type: types.IssuerTypeUpdated},
		KeyConfiguration: &types.KeyConfigurationType{
			KeyType:   types.EncryptionKeyTypeCustomerManagedKey,
			KmsKeyArn: aws.String("arn:aws:kms:us-east-1:000000000000:key/abc"),
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeUserPool(
		ctx,
		&cognitoidpsdk.DescribeUserPoolInput{UserPoolId: created.UserPool.Id},
	)
	require.NoError(t, err)

	assert.Equal(t, types.IssuerTypeUpdated, described.UserPool.IssuerConfiguration.Type)
	assert.Equal(t, types.EncryptionKeyTypeCustomerManagedKey, described.UserPool.KeyConfiguration.KeyType)
	assert.Equal(
		t,
		"arn:aws:kms:us-east-1:000000000000:key/abc",
		aws.ToString(described.UserPool.KeyConfiguration.KmsKeyArn),
	)

	_, err = client.UpdateUserPool(ctx, &cognitoidpsdk.UpdateUserPoolInput{
		UserPoolId:          created.UserPool.Id,
		IssuerConfiguration: &types.IssuerConfigurationType{Type: types.IssuerTypeOriginal},
	})
	require.NoError(t, err)

	described, err = client.DescribeUserPool(ctx, &cognitoidpsdk.DescribeUserPoolInput{UserPoolId: created.UserPool.Id})
	require.NoError(t, err)
	assert.Equal(t, types.IssuerTypeOriginal, described.UserPool.IssuerConfiguration.Type)
}
