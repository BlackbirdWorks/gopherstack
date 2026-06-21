package wafv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_DescribeManagedRuleGroupRules verifies that DescribeManagedRuleGroup
// returns populated Rules and AvailableLabels for known AWS managed rule groups.
// The previous implementation always returned empty slices for both fields,
// breaking callers that rely on rule names for override/exclusion configuration.
func TestParity_DescribeManagedRuleGroupRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		vendorName    string
		groupName     string
		wantMinRules  int
		wantMinLabels int
	}{
		{
			name:          "crs_has_rules_and_labels",
			vendorName:    "AWS",
			groupName:     "AWSManagedRulesCommonRuleSet",
			wantMinRules:  5,
			wantMinLabels: 5,
		},
		{
			name:          "sqli_has_rules_and_labels",
			vendorName:    "AWS",
			groupName:     "AWSManagedRulesSQLiRuleSet",
			wantMinRules:  4,
			wantMinLabels: 4,
		},
		{
			name:          "known_bad_inputs_has_rules",
			vendorName:    "AWS",
			groupName:     "AWSManagedRulesKnownBadInputsRuleSet",
			wantMinRules:  3,
			wantMinLabels: 3,
		},
		{
			name:          "ip_reputation_has_rules",
			vendorName:    "AWS",
			groupName:     "AWSManagedRulesAmazonIpReputationList",
			wantMinRules:  3,
			wantMinLabels: 3,
		},
		{
			name:          "bot_control_has_rules",
			vendorName:    "AWS",
			groupName:     "AWSManagedRulesBotControlRuleSet",
			wantMinRules:  5,
			wantMinLabels: 5,
		},
		{
			name:         "group_without_rules_returns_empty_not_null",
			vendorName:   "AWS",
			groupName:    "AWSManagedRulesAdminProtectionRuleSet",
			wantMinRules: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "DescribeManagedRuleGroup", map[string]any{
				"Scope":      "REGIONAL",
				"VendorName": tt.vendorName,
				"Name":       tt.groupName,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Rules []struct {
					Action map[string]any `json:"Action"`
					Name   string         `json:"Name"`
				} `json:"Rules"`
				AvailableLabels []struct {
					Name string `json:"Name"`
				} `json:"AvailableLabels"`
				Capacity float64 `json:"Capacity"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			// Rules and AvailableLabels must never be null (always an array).
			assert.NotNil(t, out.Rules, "Rules must not be null")
			assert.NotNil(t, out.AvailableLabels, "AvailableLabels must not be null")

			assert.GreaterOrEqual(t, len(out.Rules), tt.wantMinRules,
				"Rules count for %s", tt.groupName)

			if tt.wantMinLabels > 0 {
				assert.GreaterOrEqual(t, len(out.AvailableLabels), tt.wantMinLabels,
					"AvailableLabels count for %s", tt.groupName)
			}

			// Each rule must have a non-empty Name and a non-empty Action.
			for _, r := range out.Rules {
				assert.NotEmpty(t, r.Name, "rule Name must not be empty")
				assert.NotEmpty(t, r.Action, "rule Action must not be empty")
			}

			// Each available label must have a non-empty Name.
			for _, lbl := range out.AvailableLabels {
				assert.NotEmpty(t, lbl.Name, "label Name must not be empty")
			}

			assert.Positive(t, out.Capacity, "Capacity must be positive")
		})
	}
}

// TestParity_DescribeManagedRuleGroupUnknown verifies that unknown vendor/name
// combinations return a 400 error (not an empty success response).
func TestParity_DescribeManagedRuleGroupUnknown(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(t, h, "DescribeManagedRuleGroup", map[string]any{
		"Scope":      "REGIONAL",
		"VendorName": "AWS",
		"Name":       "AWSManagedRulesDoesNotExist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
