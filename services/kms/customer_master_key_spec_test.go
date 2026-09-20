package kms_test

import (
	"testing"

	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateKey_CustomerMasterKeySpec covers gopherstack-101r: CreateKey
// only read the modern KeySpec field, but aws_kms_key's
// customer_master_key_spec argument sends the deprecated
// CustomerMasterKeySpec wire field instead (same enum, kms@v1.59.0
// api_op_CreateKey.go), so KeySpec fell back to SYMMETRIC_DEFAULT.
func TestCreateKey_CustomerMasterKeySpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		keySpec               types.KeySpec
		customerMasterKeySpec types.CustomerMasterKeySpec
		keyUsage              types.KeyUsageType
		wantKeySpec           types.KeySpec
	}{
		{
			name:                  "customer master key spec falls back when key spec is empty",
			customerMasterKeySpec: types.CustomerMasterKeySpecEccNistP256,
			keyUsage:              types.KeyUsageTypeSignVerify,
			wantKeySpec:           types.KeySpecEccNistP256,
		},
		{
			name:                  "key spec wins when both are set",
			keySpec:               types.KeySpecRsa2048,
			customerMasterKeySpec: types.CustomerMasterKeySpecEccNistP256,
			keyUsage:              types.KeyUsageTypeSignVerify,
			wantKeySpec:           types.KeySpecRsa2048,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKMSClient(t, newTestKMSHandler())
			ctx := t.Context()

			created, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{
				KeySpec:               tc.keySpec,
				CustomerMasterKeySpec: tc.customerMasterKeySpec,
				KeyUsage:              tc.keyUsage,
			})
			require.NoError(t, err)

			desc, err := client.DescribeKey(ctx, &kmssdk.DescribeKeyInput{
				KeyId: created.KeyMetadata.KeyId,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantKeySpec, desc.KeyMetadata.KeySpec)
		})
	}
}
