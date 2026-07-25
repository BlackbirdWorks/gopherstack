package backup_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestCreateRestoreTestingSelection_Validation(t *testing.T) {
	t.Parallel()

	t.Run("missing IamRoleArn is a validation error", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b.CreateRestoreTestingPlan("plan-a", "", 0)
		require.NoError(t, err)

		_, err = b.CreateRestoreTestingSelection("plan-a", "sel-a", backup.RestoreTestingSelectionInput{
			ProtectedResourceType: "EC2",
		})
		require.ErrorIs(t, err, backup.ErrValidation)
	})

	t.Run("missing ProtectedResourceType is a validation error", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b.CreateRestoreTestingPlan("plan-b", "", 0)
		require.NoError(t, err)

		_, err = b.CreateRestoreTestingSelection("plan-b", "sel-b", backup.RestoreTestingSelectionInput{
			IAMRoleArn: "arn:aws:iam::000000000000:role/r",
		})
		require.ErrorIs(t, err, backup.ErrValidation)
	})

	t.Run("full shape round-trips through Get", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b.CreateRestoreTestingPlan("plan-c", "", 0)
		require.NoError(t, err)

		sel, err := b.CreateRestoreTestingSelection("plan-c", "sel-c", backup.RestoreTestingSelectionInput{
			ProtectedResourceType: "EC2",
			IAMRoleArn:            "arn:aws:iam::000000000000:role/r",
			ProtectedResourceArns: []string{"*"},
			ProtectedResourceConditions: &backup.ProtectedResourceConditions{
				StringEquals: []backup.KeyValue{{Key: "Environment", Value: "prod"}},
			},
			RestoreMetadataOverrides: map[string]string{"newVolumeName": "restored"},
			ValidationWindowHours:    24,
		})
		require.NoError(t, err)

		got, err := b.GetRestoreTestingSelection("plan-c", "sel-c")
		require.NoError(t, err)
		assert.Equal(t, sel.IAMRoleArn, got.IAMRoleArn)
		assert.Equal(t, []string{"*"}, got.ProtectedResourceArns)
		require.NotNil(t, got.ProtectedResourceConditions)
		require.Len(t, got.ProtectedResourceConditions.StringEquals, 1)
		assert.Equal(t, "Environment", got.ProtectedResourceConditions.StringEquals[0].Key)
		assert.Equal(t, "restored", got.RestoreMetadataOverrides["newVolumeName"])
		assert.Equal(t, int64(24), got.ValidationWindowHours)
	})
}

func TestUpdateRestoreTestingSelection_FullReplace(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateRestoreTestingPlan("plan-u", "", 0)
	require.NoError(t, err)
	_, err = b.CreateRestoreTestingSelection("plan-u", "sel-u", backup.RestoreTestingSelectionInput{
		ProtectedResourceType: "EC2",
		IAMRoleArn:            "arn:aws:iam::000000000000:role/original",
	})
	require.NoError(t, err)

	updated, err := b.UpdateRestoreTestingSelection("plan-u", "sel-u", backup.RestoreTestingSelectionInput{
		IAMRoleArn:            "arn:aws:iam::000000000000:role/updated",
		ValidationWindowHours: 48,
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/updated", updated.IAMRoleArn)
	assert.Equal(t, int64(48), updated.ValidationWindowHours)
	// ProtectedResourceType is immutable on Update per the real API.
	assert.Equal(t, "EC2", updated.ProtectedResourceType)
}
