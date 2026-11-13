package bedrockagentcorecontrol_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCredentialProviderQuota(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	for i := range 50 {
		_, err := f.client.CreateApiKeyCredentialProvider(ctx, &sdk.CreateApiKeyCredentialProviderInput{
			Name: aws.String(fmt.Sprintf("key-%d", i)), ApiKey: aws.String("sk"),
		})
		require.NoError(t, err)
	}

	_, err := f.client.CreateApiKeyCredentialProvider(ctx, &sdk.CreateApiKeyCredentialProviderInput{
		Name: aws.String("one-too-many"), ApiKey: aws.String("sk"),
	})
	assert.Equal(t, "ServiceQuotaExceededException", apiCode(err))

	_, err = f.client.DeleteApiKeyCredentialProvider(ctx, &sdk.DeleteApiKeyCredentialProviderInput{
		Name: aws.String("key-0"),
	})
	require.NoError(t, err)

	_, err = f.client.CreateApiKeyCredentialProvider(ctx, &sdk.CreateApiKeyCredentialProviderInput{
		Name: aws.String("one-too-many"), ApiKey: aws.String("sk"),
	})
	require.NoError(t, err)
}
