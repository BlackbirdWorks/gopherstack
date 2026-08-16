package apigatewayv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// TestVpcLinkSecurityGroupIDsNullBug verifies that VpcLink responses always
// include "securityGroupIds" as [] when no security groups are provided.
// AWS always returns securityGroupIds:[] even when the field is empty.
func TestVpcLinkSecurityGroupIDsNullBug(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		wantSecGroups []string
	}{
		{
			name:          "no_security_groups_returns_empty_array",
			body:          map[string]any{"name": "my-link", "subnetIds": []string{"subnet-abc"}},
			wantSecGroups: []string{},
		},
		{
			name: "with_security_groups_returns_them",
			body: map[string]any{
				"name":             "my-link-2",
				"subnetIds":        []string{"subnet-abc"},
				"securityGroupIds": []string{"sg-111", "sg-222"},
			},
			wantSecGroups: []string{"sg-111", "sg-222"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rr := doRequest(t, h, http.MethodPost, "/v2/vpclinks", tt.body)
			require.Equal(t, http.StatusCreated, rr.Code)

			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))

			_, ok := raw["securityGroupIds"]
			assert.True(t, ok, "securityGroupIds key must always be present in VpcLink response")

			var link apigatewayv2.VpcLink
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &link))
			assert.Equal(t, tt.wantSecGroups, link.SecurityGroupIDs)
		})
	}
}

// TestVpcLinkSubnetIDsNullBug verifies that SubnetIDs is always present in
// VpcLink responses (never absent via omitempty).
func TestVpcLinkSubnetIDsNullBug(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rr := doRequest(t, h, http.MethodPost, "/v2/vpclinks", map[string]any{
		"name":      "test-link",
		"subnetIds": []string{"subnet-aaa", "subnet-bbb"},
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))

	_, ok := raw["subnetIds"]
	assert.True(t, ok, "subnetIds key must always be present in VpcLink response")
}

// TestVpcLinkGetReturnsArrayFields verifies that GetVpcLink also returns
// securityGroupIds and subnetIds as arrays.
func TestVpcLinkGetReturnsArrayFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createRR := doRequest(t, h, http.MethodPost, "/v2/vpclinks", map[string]any{
		"name":      "get-test-link",
		"subnetIds": []string{"subnet-xyz"},
	})
	require.Equal(t, http.StatusCreated, createRR.Code)

	var created apigatewayv2.VpcLink
	require.NoError(t, json.Unmarshal(createRR.Body.Bytes(), &created))

	getRR := doRequest(t, h, http.MethodGet, "/v2/vpclinks/"+created.VpcLinkID, nil)
	require.Equal(t, http.StatusOK, getRR.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(getRR.Body.Bytes(), &raw))

	_, hasSec := raw["securityGroupIds"]
	assert.True(t, hasSec, "securityGroupIds key must be present in GetVpcLink response")

	_, hasSub := raw["subnetIds"]
	assert.True(t, hasSub, "subnetIds key must be present in GetVpcLink response")
}
