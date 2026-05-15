package secretsmanager_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

func TestRotateSecretRulesAndScheduler(t *testing.T) {
	t.Parallel()

	afterSevenDays := int64(7)
	rotateImmediatelyFalse := false

	tests := []struct {
		rotateInput        secretsmanager.RotateSecretInput
		name               string
		waitForAutoRotate  time.Duration
		wantAutoRotation   bool
		wantImmediateEmpty bool
	}{
		{
			name: "rotate_immediately_false_sets_rules_only",
			rotateInput: secretsmanager.RotateSecretInput{
				SecretID: "sched-secret",
				RotationRules: &secretsmanager.RotationRulesType{
					AutomaticallyAfterDays: &afterSevenDays,
				},
				RotateImmediately: &rotateImmediatelyFalse,
			},
			wantImmediateEmpty: true,
		},
		{
			name: "rate_expression_triggers_background_rotation",
			rotateInput: secretsmanager.RotateSecretInput{
				SecretID: "sched-secret",
				RotationRules: &secretsmanager.RotationRulesType{
					ScheduleExpression: "rate(1 second)",
				},
			},
			waitForAutoRotate: time.Second * 4,
			wantAutoRotation:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := secretsmanager.NewInMemoryBackend()
			_, err := backend.CreateSecret(&secretsmanager.CreateSecretInput{
				Name:         "sched-secret",
				SecretString: "initial",
			})
			require.NoError(t, err)

			before, err := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{SecretID: "sched-secret"})
			require.NoError(t, err)

			out, err := backend.RotateSecret(&tt.rotateInput)
			require.NoError(t, err)

			desc, err := backend.DescribeSecret(&secretsmanager.DescribeSecretInput{SecretID: "sched-secret"})
			require.NoError(t, err)
			require.NotNil(t, desc.RotationRules)

			if tt.wantImmediateEmpty {
				assert.Empty(t, out.VersionID)
				current, currentErr := backend.GetSecretValue(
					&secretsmanager.GetSecretValueInput{SecretID: "sched-secret"},
				)
				require.NoError(t, currentErr)
				assert.Equal(t, before.VersionID, current.VersionID)

				return
			}

			require.NotEmpty(t, out.VersionID)

			if !tt.wantAutoRotation {
				return
			}

			deadline := time.Now().Add(tt.waitForAutoRotate)
			rotated := false
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			for time.Now().Before(deadline) {
				current, currentErr := backend.GetSecretValue(
					&secretsmanager.GetSecretValueInput{SecretID: "sched-secret"},
				)
				require.NoError(t, currentErr)
				if current.VersionID != out.VersionID {
					rotated = true

					break
				}

				<-ticker.C
			}

			assert.True(t, rotated)
		})
	}
}

func TestReplicationStatusSync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		initialSecretString string
		name                string
		wantStatus          string
	}{
		{
			name:                "replication_fails_without_current_version",
			initialSecretString: "",
			wantStatus:          "Failed",
		},
		{
			name:                "replication_syncs_current_version",
			initialSecretString: "v1",
			wantStatus:          "InSync",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := secretsmanager.NewInMemoryBackend()
			_, err := backend.CreateSecret(&secretsmanager.CreateSecretInput{
				Name:         "replication-secret",
				SecretString: tt.initialSecretString,
			})
			require.NoError(t, err)

			_, err = backend.ReplicateSecretToRegions(&secretsmanager.ReplicateSecretToRegionsInput{
				SecretID:          "replication-secret",
				AddReplicaRegions: []secretsmanager.ReplicaRegion{{Region: "us-west-2"}},
			})
			require.NoError(t, err)

			desc, err := backend.DescribeSecret(&secretsmanager.DescribeSecretInput{
				SecretID: "replication-secret",
			})
			require.NoError(t, err)
			require.Len(t, desc.ReplicationStatus, 1)
			assert.Equal(t, tt.wantStatus, desc.ReplicationStatus[0].Status)

			if tt.wantStatus != "InSync" {
				assert.Contains(t, desc.ReplicationStatus[0].StatusMessage, "no current secret version")

				return
			}

			initialCurrent, currentErr := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{
				SecretID: "replication-secret",
			})
			require.NoError(t, currentErr)
			assert.Contains(t, desc.ReplicationStatus[0].StatusMessage, initialCurrent.VersionID)

			_, err = backend.PutSecretValue(&secretsmanager.PutSecretValueInput{
				SecretID:     "replication-secret",
				SecretString: "v2",
			})
			require.NoError(t, err)

			nextCurrent, nextErr := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{
				SecretID: "replication-secret",
			})
			require.NoError(t, nextErr)
			assert.NotEqual(t, initialCurrent.VersionID, nextCurrent.VersionID)

			descAfterPut, describeErr := backend.DescribeSecret(&secretsmanager.DescribeSecretInput{
				SecretID: "replication-secret",
			})
			require.NoError(t, describeErr)
			require.Len(t, descAfterPut.ReplicationStatus, 1)
			assert.Equal(t, "InSync", descAfterPut.ReplicationStatus[0].Status)
			assert.Contains(t, descAfterPut.ReplicationStatus[0].StatusMessage, nextCurrent.VersionID)
		})
	}
}
