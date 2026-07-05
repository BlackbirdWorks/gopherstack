package secretsmanager_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sm "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// Test_UpdateSecretVersionStage_RemoveFromVersionIDRequiredWhenLabelElsewhere verifies the
// real API contract on UpdateSecretVersionStageInput.RemoveFromVersionId: "If the staging
// label is already attached to a different version of the secret, then you must also
// specify the RemoveFromVersionId parameter... If the label is attached and you either do
// not specify this parameter, or the version ID does not match, then the operation
// fails." This mock previously stripped the label from wherever it happened to be,
// regardless of RemoveFromVersionId — a real client relying on that guard rail (e.g. to
// avoid racing another rotation step) would never see the failure it depends on.
func Test_UpdateSecretVersionStage_RemoveFromVersionIDRequiredWhenLabelElsewhere(t *testing.T) {
	t.Parallel()

	const label = "CUSTOM-LABEL"

	setup := func(t *testing.T) (*sm.InMemoryBackend, string, string) {
		t.Helper()

		b := sm.NewInMemoryBackend()
		out, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
			Name: "usvs-move-guard", SecretString: "v1",
		})
		require.NoError(t, err)
		holderVersion := out.VersionID

		put, err := b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
			SecretID: "usvs-move-guard", SecretString: "v2", VersionStages: []string{"AWSPENDING"},
		})
		require.NoError(t, err)
		otherVersion := put.VersionID

		// Attach the custom label to holderVersion only.
		_, err = b.UpdateSecretVersionStage(context.Background(), &sm.UpdateSecretVersionStageInput{
			SecretID: "usvs-move-guard", VersionStage: label, MoveToVersionID: holderVersion,
		})
		require.NoError(t, err)

		return b, holderVersion, otherVersion
	}

	cases := []struct {
		buildInput func(holderVersion, otherVersion string) *sm.UpdateSecretVersionStageInput
		name       string
		wantErr    bool
	}{
		{
			name: "no_remove_from_version_id_rejected",
			buildInput: func(_, otherVersion string) *sm.UpdateSecretVersionStageInput {
				return &sm.UpdateSecretVersionStageInput{
					SecretID: "usvs-move-guard", VersionStage: label, MoveToVersionID: otherVersion,
				}
			},
			wantErr: true,
		},
		{
			name: "mismatched_remove_from_version_id_rejected",
			buildInput: func(_, otherVersion string) *sm.UpdateSecretVersionStageInput {
				return &sm.UpdateSecretVersionStageInput{
					SecretID: "usvs-move-guard", VersionStage: label, MoveToVersionID: otherVersion,
					RemoveFromVersionID: otherVersion, // wrong: label isn't on otherVersion
				}
			},
			wantErr: true,
		},
		{
			name: "correct_remove_from_version_id_accepted",
			buildInput: func(holderVersion, otherVersion string) *sm.UpdateSecretVersionStageInput {
				return &sm.UpdateSecretVersionStageInput{
					SecretID: "usvs-move-guard", VersionStage: label, MoveToVersionID: otherVersion,
					RemoveFromVersionID: holderVersion,
				}
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, holderVersion, otherVersion := setup(t)

			_, err := b.UpdateSecretVersionStage(context.Background(), tc.buildInput(holderVersion, otherVersion))

			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, sm.ErrInvalidParameter)

				return
			}

			require.NoError(t, err)

			desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "usvs-move-guard"})
			require.NoError(t, err)
			assert.Contains(t, desc.VersionIDsToStages[otherVersion], label)
			assert.NotContains(t, desc.VersionIDsToStages[holderVersion], label)
		})
	}
}
