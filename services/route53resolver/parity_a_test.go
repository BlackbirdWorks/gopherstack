package route53resolver_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_AssociateResolverQueryLogConfig_RequiresFields verifies that
// AssociateResolverQueryLogConfig rejects requests missing required fields.
// Real AWS returns 400 InvalidRequest for missing ResolverQueryLogConfigId or
// ResourceId; the emulator had the validation but it lacked handler-level tests.
func TestParity_AssociateResolverQueryLogConfig_RequiresFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing_config_id_rejected",
			body:     map[string]any{"ResourceId": "vpc-12345"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_config_id_rejected",
			body:     map[string]any{"ResolverQueryLogConfigId": "", "ResourceId": "vpc-12345"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_resource_id_rejected",
			body:     map[string]any{"ResolverQueryLogConfigId": "rqlc-abc"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_resource_id_rejected",
			body:     map[string]any{"ResolverQueryLogConfigId": "rqlc-abc", "ResourceId": ""},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "AssociateResolverQueryLogConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"AssociateResolverQueryLogConfig status for case %q", tt.name)
		})
	}
}

// TestParity_AssociateFirewallRuleGroup_RequiresFields verifies that
// AssociateFirewallRuleGroup rejects requests missing FirewallRuleGroupId or
// VpcId. Real AWS returns 400 for both; the emulator had the validation but
// lacked handler-level tests.
func TestParity_AssociateFirewallRuleGroup_RequiresFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing_group_id_rejected",
			body:     map[string]any{"VpcId": "vpc-12345", "Priority": 100},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_vpc_id_rejected",
			body:     map[string]any{"FirewallRuleGroupId": "rslvr-frg-abc", "Priority": 100},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "AssociateFirewallRuleGroup", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"AssociateFirewallRuleGroup status for case %q", tt.name)
		})
	}
}

// TestParity_CreateOutpostResolver_RequiresFields verifies that
// CreateOutpostResolver rejects requests missing Name, OutpostArn, or
// PreferredInstanceType. Real AWS returns 400; the emulator had the validation
// but lacked handler-level tests.
func TestParity_CreateOutpostResolver_RequiresFields(t *testing.T) {
	t.Parallel()

	const validOutpostArn = "arn:aws:outposts:us-east-1:000000000000:outpost/op-abc"
	const validInstanceType = "m5.xlarge"

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_name_rejected",
			body: map[string]any{
				"OutpostArn":            validOutpostArn,
				"PreferredInstanceType": validInstanceType,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_outpost_arn_rejected",
			body: map[string]any{
				"Name":                  "my-outpost-resolver",
				"PreferredInstanceType": validInstanceType,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_preferred_instance_type_rejected",
			body: map[string]any{
				"Name":       "my-outpost-resolver",
				"OutpostArn": validOutpostArn,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "valid_request_accepted",
			body: map[string]any{
				"Name":                  "my-outpost-resolver",
				"OutpostArn":            validOutpostArn,
				"PreferredInstanceType": validInstanceType,
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateOutpostResolver", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"CreateOutpostResolver status for case %q", tt.name)

			if tt.wantCode == http.StatusOK {
				require.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}
