package kms_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestCreateGrant_QuotaUsesKeyRegion(t *testing.T) {
	t.Parallel()

	const keyRegion = "eu-west-1"

	tests := []struct {
		wantSecond error
		name       string
		ctxRegion  string
	}{
		{name: "request in other region", ctxRegion: "us-east-1", wantSecond: kms.ErrLimitExceeded},
		{name: "request in key region", ctxRegion: keyRegion, wantSecond: kms.ErrLimitExceeded},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			h := kms.NewHandler(b)
			h.SetQuotaLimiter(func(_, region, _, _ string) (float64, bool) {
				if region == keyRegion {
					return 1, true
				}

				return 100, true
			})

			keyCtx := kms.WithRegion(t.Context(), keyRegion)
			out, err := b.CreateKey(keyCtx, &kms.CreateKeyInput{})
			require.NoError(t, err)

			in := func(name string) *kms.CreateGrantInput {
				return &kms.CreateGrantInput{
					KeyID:            out.KeyMetadata.Arn,
					GranteePrincipal: "arn:aws:iam::123456789012:role/g",
					Operations:       []string{"Encrypt"},
					Name:             name,
				}
			}

			reqCtx := kms.WithRegion(t.Context(), tc.ctxRegion)
			_, err = b.CreateGrant(reqCtx, in("one"))
			require.NoError(t, err)

			_, err = b.CreateGrant(reqCtx, in("two"))
			assert.ErrorIs(t, err, tc.wantSecond)
		})
	}
}
