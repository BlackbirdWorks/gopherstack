package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateInstance_EncryptionConfiguration(t *testing.T) {
	t.Parallel()

	const key = "arn:aws:kms:us-east-1:123456789012:key/abc"

	cmk := map[string]any{"KeyType": "CUSTOMER_MANAGED_KEY", "KmsKeyArn": key}
	owned := map[string]any{"KeyType": "AWS_OWNED_KMS_KEY"}
	ownedWithArn := map[string]any{"KeyType": "AWS_OWNED_KMS_KEY", "KmsKeyArn": key}

	tests := []struct {
		body        map[string]any
		name        string
		wantKeyType string
		wantKeyArn  string
		wantStatus  int
	}{
		{
			name:        "customer managed key applied",
			body:        map[string]any{"EncryptionConfiguration": cmk},
			wantStatus:  http.StatusOK,
			wantKeyType: "CUSTOMER_MANAGED_KEY",
			wantKeyArn:  key,
		},
		{
			name:        "owned key applied",
			body:        map[string]any{"EncryptionConfiguration": owned},
			wantStatus:  http.StatusOK,
			wantKeyType: "AWS_OWNED_KMS_KEY",
		},
		{
			name:        "customer managed without arn rejected",
			body:        map[string]any{"EncryptionConfiguration": map[string]any{"KeyType": "CUSTOMER_MANAGED_KEY"}},
			wantStatus:  http.StatusBadRequest,
			wantKeyType: "AWS_OWNED_KMS_KEY",
		},
		{
			name:        "owned key with arn rejected",
			body:        map[string]any{"EncryptionConfiguration": ownedWithArn},
			wantStatus:  http.StatusBadRequest,
			wantKeyType: "AWS_OWNED_KMS_KEY",
		},
		{
			name: "combined with permission sets rejected",
			body: map[string]any{
				"PermissionSetsEnabled":   true,
				"EncryptionConfiguration": cmk,
			},
			wantStatus:  http.StatusBadRequest,
			wantKeyType: "AWS_OWNED_KMS_KEY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			inst := createInstance(t, h, "enc")
			tt.body["InstanceArn"] = inst

			rec := doRequest(t, h, "UpdateInstance", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			d := parseResponse(t, doRequest(t, h, "DescribeInstance", map[string]any{"InstanceArn": inst}))
			details, _ := d["EncryptionConfigurationDetails"].(map[string]any)
			assert.Equal(t, tt.wantKeyType, details["KeyType"])
			assert.Equal(t, "ENABLED", details["EncryptionStatus"])

			if tt.wantKeyArn != "" {
				assert.Equal(t, tt.wantKeyArn, details["KmsKeyArn"])
			} else {
				assert.NotContains(t, details, "KmsKeyArn")
			}
		})
	}
}
