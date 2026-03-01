package sts_test

import (
	"strings"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetSessionToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantErrContains string
		wantDuration    time.Duration
		tolerance       time.Duration
		duration        int32
		wantErr         bool
	}{
		{
			name:         "DefaultDuration",
			duration:     0,
			wantErr:      false,
			wantDuration: 12 * time.Hour,
			tolerance:    time.Hour,
		},
		{
			name:         "CustomDuration",
			duration:     3600,
			wantErr:      false,
			wantDuration: time.Hour,
			tolerance:    time.Minute,
		},
		{
			name:            "InvalidDuration",
			duration:        100,
			wantErr:         true,
			wantErrContains: "DurationSeconds",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backend := sts.NewInMemoryBackend()

			input := &sts.GetSessionTokenInput{}
			if tt.duration != 0 {
				input.DurationSeconds = tt.duration
			}

			resp, err := backend.GetSessionToken(input)

			if tt.wantErr {
				require.Error(t, err, "expected error")
				assert.Contains(t, err.Error(), tt.wantErrContains)

				return
			}

			require.NoError(t, err)

			creds := resp.GetSessionTokenResult.Credentials
			assert.True(
				t,
				strings.HasPrefix(creds.AccessKeyID, "ASIA"),
				"expected ASIA prefix, got %q",
				creds.AccessKeyID,
			)
			assert.NotEmpty(t, creds.SecretAccessKey, "expected non-empty SecretAccessKey")
			assert.NotEmpty(t, creds.SessionToken, "expected non-empty SessionToken")

			exp, err := time.Parse(time.RFC3339, creds.Expiration)
			require.NoError(t, err, "parse expiration")

			diff := time.Until(exp)
			assert.InDelta(t, tt.wantDuration, diff, float64(tt.tolerance), "expected ~%v expiration", tt.wantDuration)
		})
	}
}
