package route53resolver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func TestResolverDnssecConfig_StatusValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     string
		validation string
		wantStatus string
		wantCode   int
	}{
		{
			name:       "get_default_disabled",
			action:     "get",
			wantCode:   http.StatusOK,
			wantStatus: "DISABLED",
		},
		{
			name:       "enable_transitions_to_enabling",
			action:     "enable",
			validation: "ENABLE",
			wantCode:   http.StatusOK,
			wantStatus: "ENABLING",
		},
		{
			name:       "disable_transitions_to_disabling",
			action:     "disable",
			validation: "DISABLE",
			wantCode:   http.StatusOK,
			wantStatus: "DISABLING",
		},
		{
			name:       "invalid_validation_rejected",
			action:     "invalid",
			validation: "UNKNOWN",
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vpcID := "vpc-dnssec-test"

			var rec *httptest.ResponseRecorder
			switch tt.action {
			case "get":
				rec = doRequest(
					t,
					h,
					"GetResolverDnssecConfig",
					map[string]any{"ResourceId": vpcID},
				)
			case "enable", "disable", "invalid":
				rec = doRequest(t, h, "UpdateResolverDnssecConfig", map[string]any{
					"ResourceId": vpcID,
					"Validation": tt.validation,
				})
			}

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				cfg := resp["ResolverDNSSECConfig"].(map[string]any)
				assert.Equal(t, tt.wantStatus, cfg["Validation"])
			}
		})
	}
}

// --- Issue 18: FirewallConfig no ARN ---

func TestFirewallConfig_NoArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetFirewallConfig", map[string]any{"ResourceId": "vpc-fwc-test"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cfg := resp["FirewallConfig"].(map[string]any)

	// AWS does not return Arn for FirewallConfig.
	_, hasArn := cfg["Arn"]
	assert.False(t, hasArn, "FirewallConfig should not have an Arn field")
	assert.NotEmpty(t, cfg["Id"])
	assert.NotEmpty(t, cfg["OwnerId"])
	assert.NotEmpty(t, cfg["ResourceId"])
}

// --- Issue 19: FirewallDomainList ManagedOwnerName ---

func TestBackend_DnssecDefaultDisabled(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	cfg := b.GetResolverDnssecConfig(context.Background(), "vpc-dnssec-new")

	assert.Equal(t, "DISABLED", cfg.ValidationStatus)
}

// --- Backend direct: DNSSEC ENABLING/DISABLING transitions ---

func TestBackend_DnssecTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		validation string
		wantStatus string
		wantErr    bool
	}{
		{name: "enable_to_enabling", validation: "ENABLE", wantStatus: "ENABLING"},
		{name: "disable_to_disabling", validation: "DISABLE", wantStatus: "DISABLING"},
		{name: "invalid_rejected", validation: "UNKNOWN", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			cfg, err := b.UpdateResolverDnssecConfig(context.Background(), "vpc-test", tt.validation)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, cfg.ValidationStatus)
			}
		})
	}
}

// --- Backend direct: FirewallConfig no ARN ---

func TestBackend_FirewallConfigNoArn(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	cfg := b.GetFirewallConfig(context.Background(), "vpc-fwc")

	assert.NotEmpty(t, cfg.ID)
	assert.NotEmpty(t, cfg.OwnerID)
	assert.Equal(t, "vpc-fwc", cfg.ResourceID)
	assert.Equal(t, "DISABLED", cfg.FirewallFailOpen)
}

// --- Backend direct: AssociateFirewallRuleGroup MutationProtection defaults DISABLED ---

func TestUpdateFirewallConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		wantFailOpen string
		wantCode     int
	}{
		{
			name:         "enable_success",
			body:         map[string]any{"ResourceId": "vpc-001", "FirewallFailOpen": "ENABLED"},
			wantCode:     http.StatusOK,
			wantFailOpen: "ENABLED",
		},
		{
			name:         "disable_success",
			body:         map[string]any{"ResourceId": "vpc-002", "FirewallFailOpen": "DISABLED"},
			wantCode:     http.StatusOK,
			wantFailOpen: "DISABLED",
		},
		{
			name:     "missing_resource_id",
			body:     map[string]any{"FirewallFailOpen": "ENABLED"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_fail_open_value",
			body:     map[string]any{"ResourceId": "vpc-003", "FirewallFailOpen": "INVALID"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "UpdateFirewallConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := decodeJSON(t, rec)
				cfg, ok := resp["FirewallConfig"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantFailOpen, cfg["FirewallFailOpen"])
				assert.NotEmpty(t, cfg["Id"])
			}
		})
	}
}

// --- ListFirewallConfigs ---

func TestListFirewallConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupVPCs []string
		wantCount int
	}{
		{
			name:      "empty_list",
			setupVPCs: nil,
			wantCount: 0,
		},
		{
			name:      "multiple_configs",
			setupVPCs: []string{"vpc-a", "vpc-b"},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, vpcID := range tt.setupVPCs {
				rec := doRequest(t, h, "GetFirewallConfig", map[string]any{"ResourceId": vpcID})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "ListFirewallConfigs", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := decodeJSON(t, rec)
			items, _ := resp["FirewallConfigs"].([]any)
			assert.Len(t, items, tt.wantCount)
		})
	}
}

// --- GetResolverConfig ---

func TestGetResolverConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name:     "creates_on_demand",
			body:     map[string]any{"ResourceId": "vpc-rc-1"},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing_resource_id",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetResolverConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := decodeJSON(t, rec)
				cfg, ok := resp["ResolverConfig"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, cfg["Id"])
				assert.Equal(t, "vpc-rc-1", cfg["ResourceId"])
			}
		})
	}
}

// --- UpdateResolverConfig ---

func TestUpdateResolverConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            any
		name            string
		wantAutoReverse string
		wantCode        int
	}{
		{
			name:            "enable_success",
			body:            map[string]any{"ResourceId": "vpc-urc-1", "AutodefinedReverse": "ENABLE"},
			wantCode:        http.StatusOK,
			wantAutoReverse: "ENABLED",
		},
		{
			name:            "disable_success",
			body:            map[string]any{"ResourceId": "vpc-urc-2", "AutodefinedReverse": "DISABLE"},
			wantCode:        http.StatusOK,
			wantAutoReverse: "DISABLED",
		},
		{
			name:     "missing_resource_id",
			body:     map[string]any{"AutodefinedReverse": "ENABLE"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_value",
			body:     map[string]any{"ResourceId": "vpc-urc-3", "AutodefinedReverse": "INVALID"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "UpdateResolverConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := decodeJSON(t, rec)
				cfg, ok := resp["ResolverConfig"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantAutoReverse, cfg["AutodefinedReverse"])
			}
		})
	}
}

// --- ListResolverConfigs ---

func TestListResolverConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupVPCs []string
		wantCount int
	}{
		{
			name:      "empty",
			setupVPCs: nil,
			wantCount: 0,
		},
		{
			name:      "two_configs",
			setupVPCs: []string{"vpc-lrc-1", "vpc-lrc-2"},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, vpcID := range tt.setupVPCs {
				rec := doRequest(t, h, "GetResolverConfig", map[string]any{"ResourceId": vpcID})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "ListResolverConfigs", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := decodeJSON(t, rec)
			items, _ := resp["ResolverConfigs"].([]any)
			assert.Len(t, items, tt.wantCount)
		})
	}
}

// --- ListResolverDnssecConfigs ---

func TestListResolverDnssecConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupVPCs []string
		wantCount int
	}{
		{
			name:      "empty",
			setupVPCs: nil,
			wantCount: 0,
		},
		{
			name:      "two_configs",
			setupVPCs: []string{"vpc-dnssec-1", "vpc-dnssec-2"},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, vpcID := range tt.setupVPCs {
				rec := doRequest(t, h, "GetResolverDnssecConfig", map[string]any{"ResourceId": vpcID})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "ListResolverDnssecConfigs", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := decodeJSON(t, rec)
			items, _ := resp["ResolverDnssecConfigs"].([]any)
			assert.Len(t, items, tt.wantCount)
		})
	}
}

// --- Outpost Resolver CRUD ---

func TestResolverConfigToOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		vpc      string
		wantCode int
	}{
		{
			name:     "get_creates_and_returns_config",
			vpc:      "vpc-rco-1",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetResolverConfig", map[string]any{"ResourceId": tt.vpc})
			require.Equal(t, tt.wantCode, rec.Code)
			resp := decodeJSON(t, rec)
			cfg, ok := resp["ResolverConfig"].(map[string]any)
			require.True(t, ok)
			assert.NotEmpty(t, cfg["Arn"])
			assert.Equal(t, tt.vpc, cfg["ResourceId"])
		})
	}
}

// --- UpdateResolverEndpoint ---

// TestParity_ListFirewallConfigs_Pagination verifies NextToken/MaxResults on
// ListFirewallConfigs. Configs are auto-created on GetFirewallConfig access.
func TestListFirewallConfigs_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := doRequest(t, h, "GetFirewallConfig", map[string]any{
			"ResourceId": fmt.Sprintf("vpc-fwcfg-%d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       3,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListFirewallConfigs", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			cfgs, _ := out["FirewallConfigs"].([]any)
			assert.Len(t, cfgs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListResolverConfigs_Pagination verifies NextToken/MaxResults on
// ListResolverConfigs. Configs are auto-created on GetResolverConfig access.
func TestListResolverConfigs_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := doRequest(t, h, "GetResolverConfig", map[string]any{
			"ResourceId": fmt.Sprintf("vpc-rescfg-%d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       3,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListResolverConfigs", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			cfgs, _ := out["ResolverConfigs"].([]any)
			assert.Len(t, cfgs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListResolverDnssecConfigs_Pagination verifies NextToken/MaxResults on
// ListResolverDnssecConfigs. Configs are auto-created on GetResolverDnssecConfig access.
func TestListResolverDnssecConfigs_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := doRequest(t, h, "GetResolverDnssecConfig", map[string]any{
			"ResourceId": fmt.Sprintf("vpc-dnssec-pag-%d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       3,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListResolverDnssecConfigs", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			cfgs, _ := out["ResolverDnssecConfigs"].([]any)
			assert.Len(t, cfgs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ResolverDnssecConfig_ValidationField verifies the DNSSEC config response
// uses the "Validation" field name (not "ValidationStatus") matching the real AWS API.
func TestResolverDnssecConfig_ValidationField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "GetResolverDnssecConfig", map[string]any{
		"ResourceId": "vpc-dnssec-field",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	cfg, ok := out["ResolverDNSSECConfig"].(map[string]any)
	require.True(t, ok, "ResolverDNSSECConfig must be present")

	_, hasValidation := cfg["Validation"]
	_, hasValidationStatus := cfg["ValidationStatus"]
	assert.True(t, hasValidation, "response must have Validation field (not ValidationStatus)")
	assert.False(t, hasValidationStatus, "response must not have ValidationStatus field")
}
