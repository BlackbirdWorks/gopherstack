package iam_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestRoleTrustPolicyProvider(t *testing.T) {
	t.Parallel()

	const trust = `{"Version":"2012-10-17","Statement":` +
		`[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole"}]}`

	b := iam.NewInMemoryBackend()
	role, err := b.CreateRole("r", "/", trust, "")
	require.NoError(t, err)

	p := iam.RoleTrustPolicyProvider{Roles: b}

	tests := []struct {
		name, arn string
		wantInDoc string
		wantEmpty bool
	}{
		{name: "role_scoped", arn: role.Arn, wantInDoc: `"Resource":"*"`},
		{name: "unknown_role", arn: "arn:aws:iam::000000000000:role/none", wantEmpty: true},
		{name: "not_a_role", arn: "arn:aws:sqs:us-east-1:000000000000:q", wantEmpty: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			doc, getErr := p.GetResourcePolicy(t.Context(), tt.arn)
			require.NoError(t, getErr)

			if tt.wantEmpty {
				assert.Empty(t, doc)

				return
			}

			assert.Contains(t, doc, tt.wantInDoc)
		})
	}
}
