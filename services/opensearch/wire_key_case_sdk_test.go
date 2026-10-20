package opensearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_EncryptionAtRestKmsKeyIdWireKeyCase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		domain string
		key    string
	}{
		{name: "key_a", domain: "kms-case-a", key: "arn:aws:kms:us-east-1:000000000000:key/aaaa"},
		{name: "key_b", domain: "kms-case-b", key: "arn:aws:kms:us-east-1:000000000000:key/bbbb"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealClient(t)

			_, err := client.CreateDomain(t.Context(), &opensearchsdk.CreateDomainInput{
				DomainName: aws.String(tt.domain),
				EncryptionAtRestOptions: &types.EncryptionAtRestOptions{
					Enabled:  aws.Bool(true),
					KmsKeyId: aws.String(tt.key),
				},
			})
			require.NoError(t, err)

			got, err := client.DescribeDomain(t.Context(), &opensearchsdk.DescribeDomainInput{
				DomainName: aws.String(tt.domain),
			})
			require.NoError(t, err)
			require.NotNil(t, got.DomainStatus.EncryptionAtRestOptions)
			assert.Equal(t, tt.key, aws.ToString(got.DomainStatus.EncryptionAtRestOptions.KmsKeyId))
		})
	}
}
