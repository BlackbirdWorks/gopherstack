package verifiedpermissions_test

import (
	"testing"

	avpsdk "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	avptypes "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
)

func TestPolicyStore_EncryptionState(t *testing.T) {
	t.Parallel()

	kmsSettings := func(key string) avptypes.EncryptionSettings {
		return &avptypes.EncryptionSettingsMemberKmsEncryptionSettings{
			Value: avptypes.KmsEncryptionSettings{Key: &key, EncryptionContext: map[string]string{"a": "b"}},
		}
	}

	tests := []struct {
		settings avptypes.EncryptionSettings
		name     string
		wantKey  string
	}{
		{name: "unset", settings: nil},
		{name: "default", settings: &avptypes.EncryptionSettingsMemberDefault{}},
		{
			name:     "key id",
			settings: kmsSettings("1234abcd"),
			wantKey:  "arn:aws:kms:" + rtTestRegion + ":" + rtTestAccountID + ":key/1234abcd",
		},
		{
			name:     "alias",
			settings: kmsSettings("alias/vp"),
			wantKey:  "arn:aws:kms:" + rtTestRegion + ":" + rtTestAccountID + ":alias/vp",
		},
		{
			name:     "arn",
			settings: kmsSettings("arn:aws:kms:eu-west-1:111122223333:key/k1"),
			wantKey:  "arn:aws:kms:eu-west-1:111122223333:key/k1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			created, err := client.CreatePolicyStore(t.Context(), &avpsdk.CreatePolicyStoreInput{
				ValidationSettings: &avptypes.ValidationSettings{Mode: avptypes.ValidationModeOff},
				EncryptionSettings: tt.settings,
			})
			require.NoError(t, err)

			got, err := client.GetPolicyStore(
				t.Context(),
				&avpsdk.GetPolicyStoreInput{PolicyStoreId: created.PolicyStoreId},
			)
			require.NoError(t, err)

			if tt.wantKey == "" {
				assert.IsType(t, &avptypes.EncryptionStateMemberDefault{}, got.EncryptionState)

				return
			}

			state, ok := got.EncryptionState.(*avptypes.EncryptionStateMemberKmsEncryptionState)
			require.True(t, ok)
			assert.Equal(t, tt.wantKey, *state.Value.Key)
			assert.Equal(t, map[string]string{"a": "b"}, state.Value.EncryptionContext)
		})
	}
}

func TestPolicyStore_EncryptionPersisted(t *testing.T) {
	t.Parallel()

	b := verifiedpermissions.NewInMemoryBackend("000000000000", "us-east-1")
	ps, err := b.CreatePolicyStoreEncrypted("d", nil, "OFF", "", "", &verifiedpermissions.PolicyStoreEncryption{
		Key: "k1", Context: map[string]string{"x": "y"},
	})
	require.NoError(t, err)

	fresh := verifiedpermissions.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), b.Snapshot(t.Context())))

	got, err := fresh.GetPolicyStore(ps.PolicyStoreID)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/k1", got.KMSKeyArn)
	assert.Equal(t, map[string]string{"x": "y"}, got.KMSEncryptionCtx)
}
