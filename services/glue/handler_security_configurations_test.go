package glue_test

import (
	"testing"
)

// TestNewOps_SecurityConfiguration tests SecurityConfiguration CRUD.
func TestNewOps_SecurityConfiguration(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// CreateSecurityConfiguration
	out := dispatchNewOp(t, h, "CreateSecurityConfiguration", map[string]any{
		"Name": "my-sec-config",
		"EncryptionConfiguration": map[string]any{
			"S3Encryption": []any{
				map[string]any{"S3EncryptionMode": "SSE-S3"},
			},
		},
	})
	if out["Name"] != "my-sec-config" {
		t.Errorf("Name mismatch: %v", out["Name"])
	}

	// GetSecurityConfiguration
	out2 := dispatchNewOp(t, h, "GetSecurityConfiguration", map[string]any{"Name": "my-sec-config"})
	sc := out2["SecurityConfiguration"].(map[string]any)
	if sc["Name"] != "my-sec-config" {
		t.Errorf("SecurityConfiguration Name mismatch: %v", sc)
	}

	// GetSecurityConfigurations
	out3 := dispatchNewOp(t, h, "GetSecurityConfigurations", map[string]any{})
	configs, _ := out3["SecurityConfigurations"].([]any)
	if len(configs) != 1 {
		t.Errorf("expected 1 security config, got %d", len(configs))
	}

	// DeleteSecurityConfiguration
	dispatchNewOp(t, h, "DeleteSecurityConfiguration", map[string]any{"Name": "my-sec-config"})

	// Verify deletion
	dispatchNewOpExpectError(
		t,
		h,
		"GetSecurityConfiguration",
		map[string]any{"Name": "my-sec-config"},
	)
}
