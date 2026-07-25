package verifiedpermissions_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_UpdateIdentitySource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		policyStoreID    string
		identitySourceID string
		userPoolArn      string
		openIDIssuer     string
		principalType    string
		wantErr          bool
	}{
		{
			name:          "update cognito user pool",
			userPoolArn:   "arn:aws:cognito-idp:us-east-1:123456789012:userpool/newpool",
			principalType: "NewUser",
			wantErr:       false,
		},
		{
			name:         "update openid connect",
			openIDIssuer: "https://new.issuer.example.com",
			wantErr:      false,
		},
		{
			name:             "not found identity source",
			identitySourceID: "missing-is",
			wantErr:          true,
		},
		{
			name:          "not found policy store",
			policyStoreID: "missing-ps",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			ps := seedPolicyStore(t, b, "is update store")

			is, err := b.CreateIdentitySource(
				ps.PolicyStoreID,
				"User",
				verifiedpermissions.IdentitySourceConfig{
					UserPoolArn: "arn:aws:cognito-idp:us-east-1:123456789012:userpool/original",
				}, "",
			)
			require.NoError(t, err)

			psID := ps.PolicyStoreID
			if tt.policyStoreID != "" {
				psID = tt.policyStoreID
			}

			isID := is.IdentitySourceID
			if tt.identitySourceID != "" {
				isID = tt.identitySourceID
			}

			updated, err := b.UpdateIdentitySource(
				psID,
				isID,
				tt.principalType,
				verifiedpermissions.IdentitySourceConfig{
					UserPoolArn: tt.userPoolArn,
					Issuer:      tt.openIDIssuer,
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tt.userPoolArn != "" {
				assert.Equal(t, tt.userPoolArn, updated.UserPoolArn)
			}

			if tt.openIDIssuer != "" {
				assert.Equal(t, tt.openIDIssuer, updated.OpenIDIssuer)
			}

			if tt.principalType != "" {
				assert.Equal(t, tt.principalType, updated.PrincipalEntityType)
			}
		})
	}
}
