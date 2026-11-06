package kms_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestIsAWSManagedKey(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	ctx := t.Context()

	_, err := b.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: "alias/aws/s3"})
	require.NoError(t, err)

	customer, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
	require.NoError(t, err)

	tests := []struct {
		name  string
		keyID string
		want  bool
	}{
		{name: "aws_managed_alias", keyID: "alias/aws/s3", want: true},
		{name: "customer_key", keyID: customer.KeyMetadata.KeyID, want: false},
		{name: "unknown_key", keyID: "no-such-key", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, b.IsAWSManagedKey(ctx, tt.keyID))
		})
	}
}
