package sts_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

func TestAssumeRoleForService(t *testing.T) {
	t.Parallel()

	const roleArn = "arn:aws:iam::000000000000:role/svc-role"

	tests := []struct {
		meta      *sts.RoleMeta
		name      string
		principal string
		wantErr   bool
	}{
		{
			name:      "trusted_service",
			principal: "states.amazonaws.com",
			meta: &sts.RoleMeta{TrustPolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"Service":"states.amazonaws.com"},"Action":"sts:AssumeRole"}]}`},
		},
		{
			name:      "other_service_denied",
			principal: "states.amazonaws.com",
			meta: &sts.RoleMeta{TrustPolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`},
			wantErr: true,
		},
		{
			name:      "account_principal_not_service",
			principal: "states.amazonaws.com",
			meta: &sts.RoleMeta{TrustPolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"AWS":"000000000000"},"Action":"sts:AssumeRole"}]}`},
			wantErr: true,
		},
		{name: "missing_role", principal: "states.amazonaws.com", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			b.SetRoleLookup(&stubRoleLookup{meta: tt.meta})

			out, err := b.AssumeRoleForService(tt.principal, roleArn, "states-execution")
			if tt.wantErr {
				require.ErrorIs(t, err, sts.ErrAccessDenied)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, out.AssumeRoleResult.Credentials.AccessKeyID)
			assert.NotEmpty(t, out.AssumeRoleResult.Credentials.SessionToken)
		})
	}
}
