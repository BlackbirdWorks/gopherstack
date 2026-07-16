package iot_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncryptionConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update     map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "customer_managed_requires_kms_key",
			update: map[string]any{
				"encryptionType": "CUSTOMER_MANAGED_KMS_KEY",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "customer_managed_succeeds",
			update: map[string]any{
				"encryptionType":   "CUSTOMER_MANAGED_KMS_KEY",
				"kmsKeyArn":        "arn:aws:kms:us-east-1:123456789012:key/abc",
				"kmsAccessRoleArn": "arn:aws:iam::123456789012:role/kms-role",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "invalid_encryption_type",
			update: map[string]any{
				"encryptionType": "KMS_BASED",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()

			// Default (never configured) should describe as AWS-owned.
			rec := doRefRequest(t, h, http.MethodGet, "/encryption-configuration", nil, nil)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "AWS_OWNED_KMS_KEY")

			rec = doRefRequest(t, h, http.MethodPatch, "/encryption-configuration", tt.update, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				rec = doRefRequest(t, h, http.MethodGet, "/encryption-configuration", nil, nil)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "CUSTOMER_MANAGED_KMS_KEY")
			}
		})
	}
}
