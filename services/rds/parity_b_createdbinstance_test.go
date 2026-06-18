package rds_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParityB_CreateDBInstance_IdentifierValidation verifies that
// CreateDBInstance enforces AWS identifier constraints:
// must start with a letter, contain only alphanumeric/hyphens, 1–63 chars.
func TestParityB_CreateDBInstance_IdentifierValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		id         string
		wantStatus int
	}{
		{
			name:       "valid_simple",
			id:         "my-db-instance",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_single_letter",
			id:         "a",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_63_chars",
			id:         "a123456789012345678901234567890123456789012345678901234567890ab",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_starts_with_digit",
			id:         "1mydb",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_starts_with_hyphen",
			id:         "-mydb",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_underscore",
			id:         "my_db",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_space",
			id:         "my db",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_64_chars",
			id:         "a1234567890123456789012345678901234567890123456789012345678901234",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyRDSHandler()
			rec := doAccuracyRDS(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {tt.id},
				"Engine":               {"postgres"},
				"MasterUsername":       {"admin"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "id=%q", tt.id)
		})
	}
}
