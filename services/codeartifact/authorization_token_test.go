package codeartifact_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAuthorizationToken_Duration_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		duration *int64
		name     string
		wantTTL  time.Duration
		wantErr  bool
	}{
		{name: "default_is_12h", wantTTL: 12 * time.Hour},
		{name: "explicit_15m", duration: aws.Int64(900), wantTTL: 15 * time.Minute},
		{name: "explicit_1h", duration: aws.Int64(3600), wantTTL: time.Hour},
		{name: "zero_uses_default", duration: aws.Int64(0), wantTTL: 12 * time.Hour},
		{name: "too_short", duration: aws.Int64(899), wantErr: true},
		{name: "too_long", duration: aws.Int64(43201), wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupDomain(t, h, "tok-domain")
			client := newTestCodeArtifactClient(t, h)

			get := func() (*casdk.GetAuthorizationTokenOutput, error) {
				return client.GetAuthorizationToken(t.Context(), &casdk.GetAuthorizationTokenInput{
					Domain: aws.String("tok-domain"), DurationSeconds: tc.duration,
				})
			}

			before := time.Now()
			out, err := get()
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationException")

				return
			}
			require.NoError(t, err)
			assert.WithinDuration(t, before.Add(tc.wantTTL), *out.Expiration, 5*time.Second)

			again, err := get()
			require.NoError(t, err)
			assert.NotEqual(t, aws.ToString(out.AuthorizationToken), aws.ToString(again.AuthorizationToken))
		})
	}
}
