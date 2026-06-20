package redshift_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_CreateCluster_IdentifierValidation verifies that CreateCluster
// enforces the AWS ClusterIdentifier naming rules:
//   - must begin with a lowercase letter
//   - only lowercase letters, digits, and hyphens
//   - must not end with a hyphen
//   - must not contain consecutive hyphens
//   - 1–63 characters
//
// Real AWS returns InvalidParameterCombination / ClusterIdentifierConstraint for
// violations; the emulator previously accepted any non-empty string.
func TestParity_CreateCluster_IdentifierValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		id       string
		wantCode int
	}{
		{
			name:     "starts_with_digit_rejected",
			id:       "1cluster",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "starts_with_hyphen_rejected",
			id:       "-cluster",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ends_with_hyphen_rejected",
			id:       "cluster-",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "consecutive_hyphens_rejected",
			id:       "my--cluster",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "uppercase_letter_rejected",
			id:       "MyCluster",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "valid_simple_name_accepted",
			id:       "mycluster",
			wantCode: http.StatusOK,
		},
		{
			name:     "valid_with_hyphens_accepted",
			id:       "my-cluster-1",
			wantCode: http.StatusOK,
		},
		{
			name:     "valid_min_length_accepted",
			id:       "a",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			body := "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=" + tt.id

			rec := postRedshiftForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"CreateCluster ClusterIdentifier=%q", tt.id)

			if tt.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue",
					"expected InvalidParameterValue error for ClusterIdentifier=%q", tt.id)
			}
		})
	}
}

// TestParity_CreateCluster_IdentifierMaxLength verifies that a 63-character
// identifier is accepted and a 64-character one is rejected.
func TestParity_CreateCluster_IdentifierMaxLength(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()

	// 63 chars: 'a' + 62 'b's = valid max
	validID := "a" + strings.Repeat("b", 62)

	rec := postRedshiftForm(t, h,
		"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier="+validID)
	require.Equal(t, http.StatusOK, rec.Code, "63-char identifier should be accepted")

	// 64 chars: 'a' + 63 'b's = too long
	tooLongID := "a" + strings.Repeat("b", 63)
	rec2 := postRedshiftForm(t, h,
		"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier="+tooLongID)
	assert.Equal(t, http.StatusBadRequest, rec2.Code, "64-char identifier should be rejected")
}
