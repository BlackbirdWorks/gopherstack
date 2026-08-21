package accessanalyzer_test

import (
	"testing"

	aasdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// TestValidatePolicy_LocationSpan_RealSDKClient proves the required
// types.Location.Span member (accessanalyzer@v1.51.4 types/types.go:1509-1521,
// "This member is required.", reachable only through
// ValidatePolicyOutput.Findings[].Locations -- invisible to a flat per-op
// scan of ValidatePolicyOutput itself, which carries no required member of
// its own besides the top-level Findings array) survives a real SDK client
// round trip.
//
// Before the fix, rootLoc/fieldLoc/stmtLoc/stmtFieldLoc (policy_analysis.go)
// built every Location with only "path" -- "span" was never emitted, so a
// real client's Location.Span decoded to a nil *types.Span for every finding
// ValidatePolicy has ever returned. gopherstack-r80d batch 18.
func TestValidatePolicy_LocationSpan_RealSDKClient(t *testing.T) {
	t.Parallel()

	backend := accessanalyzer.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestAccessAnalyzerClient(t, accessanalyzer.NewHandler(backend))
	ctx := t.Context()

	tests := []struct {
		name           string
		policyDocument string
		policyType     aatypes.PolicyType
	}{
		{
			name:           "invalid_json_root_span",
			policyDocument: "not-json",
			policyType:     aatypes.PolicyTypeIdentityPolicy,
		},
		{
			name:           "missing_version_root_span",
			policyDocument: `{"Statement":[]}`,
			policyType:     aatypes.PolicyTypeIdentityPolicy,
		},
		{
			name:           "invalid_version_field_span",
			policyDocument: `{"Version":"1999-01-01","Statement":[]}`,
			policyType:     aatypes.PolicyTypeIdentityPolicy,
		},
		{
			name: "invalid_effect_field_span_in_second_statement",
			policyDocument: `{"Version":"2012-10-17","Statement":[` +
				`{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"},` +
				`{"Effect":"Permit","Action":"s3:PutObject","Resource":"*"}]}`,
			policyType: aatypes.PolicyTypeIdentityPolicy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.ValidatePolicy(ctx, &aasdk.ValidatePolicyInput{
				PolicyDocument: &tt.policyDocument,
				PolicyType:     tt.policyType,
			})
			require.NoError(t, err)
			require.NotEmpty(t, out.Findings)

			for _, f := range out.Findings {
				for _, loc := range f.Locations {
					require.NotNil(t, loc.Span, "Location.Span is required but was dropped")
					require.NotNil(t, loc.Span.Start, "Span.Start is required")
					require.NotNil(t, loc.Span.End, "Span.End is required")
					require.NotNil(t, loc.Span.Start.Line, "Position.Line is required")
					require.NotNil(t, loc.Span.Start.Column, "Position.Column is required")
					require.NotNil(t, loc.Span.Start.Offset, "Position.Offset is required")
				}
			}
		})
	}
}

// TestValidatePolicy_LocationSpan_PointsAtRealText locks in that the span is
// not just present but accurate: it must bound the exact substring of the
// original policy document the finding is about, not an arbitrary
// placeholder range.
func TestValidatePolicy_LocationSpan_PointsAtRealText(t *testing.T) {
	t.Parallel()

	backend := accessanalyzer.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestAccessAnalyzerClient(t, accessanalyzer.NewHandler(backend))
	ctx := t.Context()

	doc := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Permit","Action":"s3:Get*","Resource":"*"}]}`

	out, err := client.ValidatePolicy(ctx, &aasdk.ValidatePolicyInput{
		PolicyDocument: &doc,
		PolicyType:     aatypes.PolicyTypeIdentityPolicy,
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Findings)

	var found bool

	for _, f := range out.Findings {
		if f.IssueCode == nil || *f.IssueCode != "INVALID_EFFECT" {
			continue
		}

		require.Len(t, f.Locations, 1)

		span := f.Locations[0].Span
		require.NotNil(t, span)

		start := int(*span.Start.Offset)
		end := int(*span.End.Offset)
		require.Equal(t, `"Permit"`, doc[start:end], "span must bound the real Effect value bytes")

		found = true
	}

	require.True(t, found, "expected an INVALID_EFFECT finding")
}
