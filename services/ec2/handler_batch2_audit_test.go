package ec2_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2 "github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestBatch2Audit_LockSnapshot_LockStateMatchesMode verifies that LockSnapshot returns a
// LockState equal to the requested mode ("compliance" or "governance"), matching AWS EC2
// behaviour. Previously the backend hardcoded "locked" for all modes.
func TestBatch2Audit_LockSnapshot_LockStateMatchesMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantLockState string
		lockMode      string
		durationDays  int
	}{
		{
			name:          "compliance_mode_returns_compliance_state",
			lockMode:      "compliance",
			durationDays:  90,
			wantLockState: "compliance",
		},
		{
			name:          "governance_mode_returns_governance_state",
			lockMode:      "governance",
			durationDays:  30,
			wantLockState: "governance",
		},
		{
			name:          "compliance_indefinite_returns_compliance_state",
			lockMode:      "compliance",
			durationDays:  0,
			wantLockState: "compliance",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			vol, err := b.CreateVolume("us-east-1a", "gp2", 10)
			require.NoError(t, err)
			snap, err := b.CreateSnapshot(vol.ID, "audit-lock")
			require.NoError(t, err)

			lock, err := b.LockSnapshot(snap.SnapshotID, tt.lockMode, tt.durationDays)
			require.NoError(t, err)
			assert.Equal(t, tt.wantLockState, lock.LockState,
				"LockState must match the requested lock mode")
			assert.Equal(t, snap.SnapshotID, lock.SnapshotID)

			// DescribeLockedSnapshots must also return the correct state.
			locks := b.DescribeLockedSnapshots([]string{snap.SnapshotID})
			require.Len(t, locks, 1)
			assert.Equal(t, tt.wantLockState, locks[0].LockState)
		})
	}
}

// TestBatch2Audit_GetImageBlockPublicAccessState_DefaultUnblocked verifies that the
// account-level image block public access state defaults to "unblocked", matching AWS EC2
// behaviour. Previously the backend incorrectly defaulted to "block-new-sharing".
func TestBatch2Audit_GetImageBlockPublicAccessState_DefaultUnblocked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setState  string // empty = don't call Enable
		wantState string
	}{
		{
			name:      "fresh_backend_defaults_to_unblocked",
			wantState: "unblocked",
		},
		{
			name:      "after_enable_block_new_sharing",
			setState:  "block-new-sharing",
			wantState: "block-new-sharing",
		},
		{
			name:      "after_disable_returns_unblocked",
			setState:  "disable",
			wantState: "unblocked",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			switch tt.setState {
			case "disable":
				_ = b.EnableImageBlockPublicAccess("block-new-sharing")
				b.DisableImageBlockPublicAccess()
			case "":
				// no-op — test default
			default:
				require.NoError(t, b.EnableImageBlockPublicAccess(tt.setState))
			}

			got := b.GetImageBlockPublicAccessState()
			assert.Equal(t, tt.wantState, got)
		})
	}
}

// TestBatch2Audit_ModifyEbsDefaultKmsKeyId_ResponseXMLName verifies that
// ModifyEbsDefaultKmsKeyId returns a response with root element
// "ModifyEbsDefaultKmsKeyIdResponse", not "GetEbsDefaultKmsKeyIdResponse".
// The wrong element name causes AWS SDK deserialisation to fail.
func TestBatch2Audit_ModifyEbsDefaultKmsKeyId_ResponseXMLName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		kmsKeyID string
		wantXML  string
		wantErr  bool
	}{
		{
			name:     "valid_key_returns_modify_response_element",
			kmsKeyID: "alias/my-custom-key",
			wantXML:  "ModifyEbsDefaultKmsKeyIdResponse",
		},
		{
			name:     "another_key_alias_returns_modify_response_element",
			kmsKeyID: "arn:aws:kms:us-east-1:000000000000:key/abc-123",
			wantXML:  "ModifyEbsDefaultKmsKeyIdResponse",
		},
		{
			name:    "empty_key_id_returns_error",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp, err := ec2.ExportDispatch(h, url.Values{
				"Action":   {"ModifyEbsDefaultKmsKeyId"},
				"KmsKeyId": {tt.kmsKeyID},
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(resp, "<"+tt.wantXML+">"),
				"response XML root must be %s, got prefix: %.60s", tt.wantXML, resp)
		})
	}
}

// TestBatch2Audit_GetDefaultCreditSpecification_EchoesInstanceFamily verifies that
// GetDefaultCreditSpecification and ModifyDefaultCreditSpecification echo the requested
// InstanceFamily back in the response, matching AWS EC2 behaviour.
// Previously both handlers hardcoded "t3" regardless of the request parameter.
func TestBatch2Audit_GetDefaultCreditSpecification_EchoesInstanceFamily(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     string
		family     string
		cpuCredits string
		wantFamily string
	}{
		{
			name:       "get_t2_family_echoed",
			action:     "GetDefaultCreditSpecification",
			family:     "t2",
			wantFamily: "t2",
		},
		{
			name:       "get_t3a_family_echoed",
			action:     "GetDefaultCreditSpecification",
			family:     "t3a",
			wantFamily: "t3a",
		},
		{
			name:       "modify_t4g_family_echoed",
			action:     "ModifyDefaultCreditSpecification",
			family:     "t4g",
			cpuCredits: "unlimited",
			wantFamily: "t4g",
		},
		{
			name:       "modify_t2_family_echoed",
			action:     "ModifyDefaultCreditSpecification",
			family:     "t2",
			cpuCredits: "standard",
			wantFamily: "t2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := url.Values{
				"Action":         {tt.action},
				"InstanceFamily": {tt.family},
			}
			if tt.cpuCredits != "" {
				vals.Set("CpuCredits", tt.cpuCredits)
			}

			resp, err := ec2.ExportDispatch(h, vals)
			require.NoError(t, err)
			assert.Contains(t, resp, "<instanceFamily>"+tt.wantFamily+"</instanceFamily>",
				"response must echo the requested InstanceFamily")
		})
	}
}
