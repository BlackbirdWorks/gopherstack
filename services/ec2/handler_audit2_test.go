package ec2_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2 "github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestBatch2Audit_EnableEbsEncryptionByDefault_ResponseField verifies that
// EnableEbsEncryptionByDefault returns an <ebsEncryptionByDefault>true</ebsEncryptionByDefault>
// field, not <return>true</return>. Previously the handler used a stubResponse which
// produced the wrong field name, causing AWS SDK deserialisation to fail.
func TestBatch2Audit_EnableEbsEncryptionByDefault_ResponseField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		action      string
		wantField   string
		wantMissing string
	}{
		{
			name:        "enable_returns_ebsEncryptionByDefault_true",
			action:      "EnableEbsEncryptionByDefault",
			wantField:   "<ebsEncryptionByDefault>true</ebsEncryptionByDefault>",
			wantMissing: "<return>",
		},
		{
			name:        "disable_returns_ebsEncryptionByDefault_false",
			action:      "DisableEbsEncryptionByDefault",
			wantField:   "<ebsEncryptionByDefault>false</ebsEncryptionByDefault>",
			wantMissing: "<return>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp, err := ec2.ExportDispatch(h, url.Values{
				"Action": {tt.action},
			})
			require.NoError(t, err)
			assert.Contains(t, resp, tt.wantField,
				"response must contain %s", tt.wantField)
			assert.False(t, strings.Contains(resp, tt.wantMissing),
				"response must not contain %s", tt.wantMissing)
		})
	}
}

// TestBatch2Audit_EnableEbsEncryptionByDefault_RootElement verifies that
// Enable/DisableEbsEncryptionByDefault return the correct XML root element names.
func TestBatch2Audit_EnableEbsEncryptionByDefault_RootElement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		action  string
		wantXML string
	}{
		{
			name:    "enable_root_element",
			action:  "EnableEbsEncryptionByDefault",
			wantXML: "EnableEbsEncryptionByDefaultResponse",
		},
		{
			name:    "disable_root_element",
			action:  "DisableEbsEncryptionByDefault",
			wantXML: "DisableEbsEncryptionByDefaultResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp, err := ec2.ExportDispatch(h, url.Values{
				"Action": {tt.action},
			})
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(resp, "<"+tt.wantXML+">"),
				"response XML root must be %s, got prefix: %.80s", tt.wantXML, resp)
		})
	}
}

// TestBatch2Audit_ImageBlockPublicAccess_ResponseState verifies that
// EnableImageBlockPublicAccess returns the new state in an <imageBlockPublicAccessState>
// element rather than <return>true</return>, matching AWS EC2 behaviour.
// Similarly DisableImageBlockPublicAccess must return <state>unblocked</state>.
func TestBatch2Audit_ImageBlockPublicAccess_ResponseState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		action      string
		stateParam  string
		wantState   string
		wantMissing string
	}{
		{
			name:        "enable_block_new_sharing_returns_state",
			action:      "EnableImageBlockPublicAccess",
			stateParam:  "block-new-sharing",
			wantState:   "block-new-sharing",
			wantMissing: "<return>",
		},
		{
			name:        "enable_default_state_returns_block_new_sharing",
			action:      "EnableImageBlockPublicAccess",
			stateParam:  "",
			wantState:   "block-new-sharing",
			wantMissing: "<return>",
		},
		{
			name:        "disable_returns_unblocked",
			action:      "DisableImageBlockPublicAccess",
			stateParam:  "",
			wantState:   "unblocked",
			wantMissing: "<return>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := url.Values{"Action": {tt.action}}
			if tt.stateParam != "" {
				vals.Set("ImageBlockPublicAccessState", tt.stateParam)
			}

			resp, err := ec2.ExportDispatch(h, vals)
			require.NoError(t, err)
			assert.Contains(t, resp, "<state>"+tt.wantState+"</state>",
				"response must contain <state>%s</state>", tt.wantState)
			assert.False(t, strings.Contains(resp, tt.wantMissing),
				"response must not contain %s", tt.wantMissing)
		})
	}
}

// TestBatch2Audit_ImageBlockPublicAccess_RootElement verifies that
// Enable/DisableImageBlockPublicAccess return the correct XML root element names.
func TestBatch2Audit_ImageBlockPublicAccess_RootElement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		action  string
		wantXML string
	}{
		{
			name:    "enable_root_element",
			action:  "EnableImageBlockPublicAccess",
			wantXML: "EnableImageBlockPublicAccessResponse",
		},
		{
			name:    "disable_root_element",
			action:  "DisableImageBlockPublicAccess",
			wantXML: "DisableImageBlockPublicAccessResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := url.Values{"Action": {tt.action}}
			if tt.action == "EnableImageBlockPublicAccess" {
				vals.Set("ImageBlockPublicAccessState", "block-new-sharing")
			}

			resp, err := ec2.ExportDispatch(h, vals)
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(resp, "<"+tt.wantXML+">"),
				"response XML root must be %s, got prefix: %.80s", tt.wantXML, resp)
		})
	}
}
