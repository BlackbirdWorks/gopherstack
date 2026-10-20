package iam_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func trustPolicyFor(principal string) string {
	return fmt.Sprintf(
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":%q},`+
			`"Action":"sts:AssumeRole"}]}`, principal)
}

func TestCreateRole_TrustPolicyAccountPrincipal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		principal string
	}{
		{name: "unknown_account_root", principal: "arn:aws:iam::123456789012:root"},
		{name: "unknown_account_role", principal: "arn:aws:iam::210987654321:role/x"},
		{name: "short_account", principal: "arn:aws:iam::1234:root", wantErr: iam.ErrMalformedPolicyDocument},
		{
			name:      "non_numeric_account",
			principal: "arn:aws:iam::abcdefghijkl:root",
			wantErr:   iam.ErrMalformedPolicyDocument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			type result struct{ err error }
			done := make(chan result, 1)

			go func() {
				_, err := b.CreateRole("r", "/", trustPolicyFor(tt.principal), "")
				done <- result{err}
			}()

			select {
			case res := <-done:
				if tt.wantErr != nil {
					require.ErrorIs(t, res.err, tt.wantErr)
				} else {
					require.NoError(t, res.err)
				}
			case <-time.After(5 * time.Second):
				assert.Fail(t, "CreateRole deadlocked")
			case <-t.Context().Done():
			}
		})
	}
}
