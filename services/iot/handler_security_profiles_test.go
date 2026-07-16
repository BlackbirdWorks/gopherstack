package iot_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatch3_SecurityProfileTargets tests DetachSecurityProfile,
// ListTargetsForSecurityProfile, ListSecurityProfilesForTarget.
func TestSecurityProfileTargets(t *testing.T) {
	t.Parallel()
	h, b := newHandlerForBatch3Test(t)

	profileName := "test-profile"
	targetARN := "arn:aws:iot:us-east-1:000000000000:thinggroup/my-group"

	// Attach via backend directly (AttachSecurityProfile already implemented)
	if err := b.AttachSecurityProfile(&iot.AttachSecurityProfileInput{
		SecurityProfileName:      profileName,
		SecurityProfileTargetArn: targetARN,
	}); err != nil {
		t.Fatal(err)
	}

	// List targets for profile
	out := iotOK(t, h, http.MethodGet, "/security-profiles/"+profileName+"/targets", nil)
	targets, _ := out["securityProfileTargets"].([]any)
	if len(targets) != 1 {
		t.Errorf("expected 1 target, got %d", len(targets))
	}

	// List profiles for target
	out2 := iotOK(t, h, http.MethodGet, "/security-profiles-for-target?securityProfileTargetArn="+targetARN, nil)
	mappings, _ := out2["securityProfileTargetMappings"].([]any)
	if len(mappings) != 1 {
		t.Errorf("expected 1 mapping, got %d", len(mappings))
	}

	// Detach
	iotOK(
		t,
		h,
		http.MethodDelete,
		"/security-profiles/"+profileName+"/targets?securityProfileTargetArn="+targetARN,
		nil,
	)

	// Verify detached
	out3 := iotOK(t, h, http.MethodGet, "/security-profiles/"+profileName+"/targets", nil)
	targets2, _ := out3["securityProfileTargets"].([]any)
	if len(targets2) != 0 {
		t.Errorf("expected 0 targets after detach, got %d", len(targets2))
	}
}

// TestNewOps_SecurityProfile tests SecurityProfile CRUD.
func TestSecurityProfile(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateSecurityProfile
	out := iotOK(t, h, http.MethodPost, "/security-profiles/my-profile", map[string]any{
		"securityProfileDescription": "test profile",
	})
	if out["securityProfileName"] != "my-profile" {
		t.Errorf("name mismatch: %v", out)
	}

	// DescribeSecurityProfile
	out2 := iotOK(t, h, http.MethodGet, "/security-profiles/my-profile", nil)
	if out2["securityProfileName"] != "my-profile" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// ListSecurityProfiles
	out3 := iotOK(t, h, http.MethodGet, "/security-profiles", nil)
	profiles, _ := out3["securityProfileIdentifiers"].([]any)
	if len(profiles) != 1 {
		t.Errorf("expected 1 profile, got %d", len(profiles))
	}

	// UpdateSecurityProfile
	out4 := iotOK(t, h, http.MethodPatch, "/security-profiles/my-profile", map[string]any{
		"securityProfileDescription": "updated",
	})
	if out4["version"] == nil {
		t.Error("expected version in update response")
	}

	// DeleteSecurityProfile
	iotOK(t, h, http.MethodDelete, "/security-profiles/my-profile", nil)

	iotExpectError(t, h, "/security-profiles/my-profile")
}

func TestValidateSecurityProfileBehaviors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		behaviors  []map[string]any
		wantValid  bool
		wantStatus int
	}{
		{
			name: "valid_behavior",
			behaviors: []map[string]any{
				{
					"name":   "excessive-connects",
					"metric": "aws:num-connections",
					"criteria": map[string]any{
						"comparisonOperator": "greater-than",
						"durationSeconds":    300,
					},
				},
			},
			wantValid:  true,
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_name",
			behaviors: []map[string]any{
				{
					"criteria": map[string]any{"comparisonOperator": "greater-than"},
				},
			},
			wantValid:  false,
			wantStatus: http.StatusOK,
		},
		{
			name: "invalid_comparison_operator",
			behaviors: []map[string]any{
				{
					"name":     "bad-behavior",
					"criteria": map[string]any{"comparisonOperator": "not-a-real-operator"},
				},
			},
			wantValid:  false,
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_criteria",
			behaviors: []map[string]any{
				{"name": "no-criteria"},
			},
			wantValid:  false,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()

			rec := doRefRequest(t, h, http.MethodPost, "/security-profile-behaviors/validate", map[string]any{
				"behaviors": tt.behaviors,
			}, nil)

			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantValid {
				assert.Contains(t, rec.Body.String(), `"valid":true`)
			} else {
				assert.Contains(t, rec.Body.String(), `"valid":false`)
			}
		})
	}
}
