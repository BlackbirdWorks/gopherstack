package secretsmanager_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sm "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ptr64 is a helper to take the address of an int64 literal.
func ptr64(v int64) *int64 {
	p := new(int64)
	*p = v

	return p
}

// ---------------------------------------------------------------------------
// Janitor — recovery window honoured on purge (bug fix: was hardcoded to 30d)
// ---------------------------------------------------------------------------

// TestAuditSM_Janitor_RecoveryWindowRespected verifies that secrets deleted with a
// custom RecoveryWindowInDays are purged by the janitor at the correct time.
// It uses SetNowForTest to pin the backend clock so DeletedDate / ScheduledDeletionDate
// are set relative to a known point; the janitor's sweep uses real time.Now() which
// is always in the future of any pinned past time.
func TestAuditSM_Janitor_RecoveryWindowRespected(t *testing.T) {
	t.Parallel()

	// Anchor: 8 days ago. ScheduledDeletionDate = anchor + 7 days = 1 day ago → purge.
	pastTime := time.Now().Add(-8 * 24 * time.Hour)

	t.Run("7day_window_expired_after_8_days_purged", func(t *testing.T) {
		t.Parallel()

		b := sm.NewInMemoryBackend()
		b.SetNowForTest(func() time.Time { return pastTime })

		_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
			Name:         "window-7d",
			SecretString: "secret",
		})
		require.NoError(t, err)

		_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{
			SecretID:             "window-7d",
			RecoveryWindowInDays: ptr64(7),
		})
		require.NoError(t, err)

		// ScheduledDeletionDate = pastTime + 7 days = 1 day ago — janitor must purge.
		j := sm.NewJanitor(b, 0)
		j.SweepOnce(context.Background())
		assert.Equal(t, 0, sm.SecretCount(b),
			"secret with 7-day window deleted 8 days ago must be purged")
	})

	t.Run("30day_window_not_expired_after_8_days_preserved", func(t *testing.T) {
		t.Parallel()

		b := sm.NewInMemoryBackend()
		b.SetNowForTest(func() time.Time { return pastTime })

		_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
			Name:         "window-30d",
			SecretString: "secret",
		})
		require.NoError(t, err)

		_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{
			SecretID:             "window-30d",
			RecoveryWindowInDays: ptr64(30),
		})
		require.NoError(t, err)

		// ScheduledDeletionDate = pastTime + 30 days = 22 days from now — must NOT purge.
		j := sm.NewJanitor(b, 0)
		j.SweepOnce(context.Background())
		assert.Equal(t, 1, sm.SecretCount(b),
			"secret with 30-day window deleted 8 days ago must NOT be purged")
	})
}

func TestAuditSM_Janitor_ForceDeletePurgesImmediately(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "force-delete-test",
		SecretString: "value",
	})
	require.NoError(t, err)

	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{
		SecretID:                   "force-delete-test",
		ForceDeleteWithoutRecovery: true,
	})
	require.NoError(t, err)
	// ForceDelete removes immediately — no janitor sweep needed.
	assert.Equal(t, 0, sm.SecretCount(b), "force-deleted secret must be removed immediately")
}

func TestAuditSM_Janitor_ActiveSecretNotPurged(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "active-secret",
		SecretString: "value",
	})
	require.NoError(t, err)

	j := sm.NewJanitor(b, 0)
	j.SweepOnce(context.Background())
	assert.Equal(t, 1, sm.SecretCount(b), "active secret must not be purged by janitor")
}

func TestAuditSM_Janitor_DeletionDateReturnedMatchesRecoveryWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		recoveryDays int64
	}{
		{"7_days", 7},
		{"14_days", 14},
		{"30_days", 30},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			epoch := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			b := sm.NewInMemoryBackend()
			b.SetNowForTest(func() time.Time { return epoch })

			_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
				Name:         "del-date-" + tc.name,
				SecretString: "v",
			})
			require.NoError(t, err)

			out, err := b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{
				SecretID:             "del-date-" + tc.name,
				RecoveryWindowInDays: ptr64(tc.recoveryDays),
			})
			require.NoError(t, err)

			expected := epoch.Add(time.Duration(tc.recoveryDays) * 24 * time.Hour)
			assert.InDelta(t, float64(expected.Unix()), out.DeletionDate, 1.0,
				"DeletionDate must be epoch + %d days", tc.recoveryDays)
		})
	}
}

// ---------------------------------------------------------------------------
// Version staging state machine: full AWSPENDING → AWSCURRENT → AWSPREVIOUS cycle
// ---------------------------------------------------------------------------

func TestAuditSM_VersionStaging_FullCycle(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	// v1 → AWSCURRENT
	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "stage-cycle",
		SecretString: "v1",
	})
	require.NoError(t, err)

	v1, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "stage-cycle"})
	require.NoError(t, err)
	v1ID := v1.VersionID

	// v2 → AWSCURRENT; v1 → AWSPREVIOUS
	pv2, err := b.PutSecretValue(ctx, &sm.PutSecretValueInput{
		SecretID:     "stage-cycle",
		SecretString: "v2",
	})
	require.NoError(t, err)
	v2ID := pv2.VersionID

	cur, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "stage-cycle"})
	require.NoError(t, err)
	assert.Equal(t, "v2", cur.SecretString, "AWSCURRENT must be v2")
	assert.Equal(t, v2ID, cur.VersionID)

	prev, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{
		SecretID:     "stage-cycle",
		VersionStage: "AWSPREVIOUS",
	})
	require.NoError(t, err)
	assert.Equal(t, "v1", prev.SecretString, "AWSPREVIOUS must be v1")
	assert.Equal(t, v1ID, prev.VersionID)

	// v3 → AWSCURRENT; v2 → AWSPREVIOUS; v1 → deprecated (no staging label)
	pv3, err := b.PutSecretValue(ctx, &sm.PutSecretValueInput{
		SecretID:     "stage-cycle",
		SecretString: "v3",
	})
	require.NoError(t, err)
	v3ID := pv3.VersionID

	cur3, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "stage-cycle"})
	require.NoError(t, err)
	assert.Equal(t, "v3", cur3.SecretString)
	assert.Equal(t, v3ID, cur3.VersionID)

	prev3, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{
		SecretID:     "stage-cycle",
		VersionStage: "AWSPREVIOUS",
	})
	require.NoError(t, err)
	assert.Equal(t, "v2", prev3.SecretString, "AWSPREVIOUS must now be v2")
	assert.Equal(t, v2ID, prev3.VersionID)

	// v1 must no longer carry AWSPREVIOUS — verify via ListSecretVersionIds.
	versions, err := b.ListSecretVersionIDs(ctx, &sm.ListSecretVersionIDsInput{
		SecretID:          "stage-cycle",
		IncludeDeprecated: true,
	})
	require.NoError(t, err)

	var v1Labels []string
	for _, v := range versions.Versions {
		if v.VersionID == v1ID {
			v1Labels = v.StagingLabels
		}
	}
	assert.NotContains(t, v1Labels, "AWSCURRENT")
	assert.NotContains(
		t,
		v1Labels,
		"AWSPREVIOUS",
		"v1 must not have AWSPREVIOUS after v3 is current",
	)
}

func TestAuditSM_VersionStaging_AWPENDINGDoesNotMoveCurrent(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "pending-test",
		SecretString: "v1",
	})
	require.NoError(t, err)

	// Assign AWSPENDING to a new version without displacing AWSCURRENT.
	p, err := b.PutSecretValue(ctx, &sm.PutSecretValueInput{
		SecretID:      "pending-test",
		SecretString:  "v2-pending",
		VersionStages: []string{"AWSPENDING"},
	})
	require.NoError(t, err)
	pendingID := p.VersionID

	// AWSCURRENT must still return v1.
	cur, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "pending-test"})
	require.NoError(t, err)
	assert.Equal(
		t,
		"v1",
		cur.SecretString,
		"AWSCURRENT must not change when only AWSPENDING assigned",
	)

	// AWSPENDING must return the new version.
	pend, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{
		SecretID:     "pending-test",
		VersionStage: "AWSPENDING",
	})
	require.NoError(t, err)
	assert.Equal(t, pendingID, pend.VersionID)
	assert.Equal(t, "v2-pending", pend.SecretString)
}

// ---------------------------------------------------------------------------
// Binary secrets round-trip
// ---------------------------------------------------------------------------

func TestAuditSM_SecretBinary_CreateAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		binary []byte
	}{
		{name: "arbitrary_bytes", binary: []byte{0x00, 0xFF, 0xAB, 0xCD, 0x42}},
		{name: "utf8_bytes", binary: []byte("binary secret value")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			ctx := context.Background()

			_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
				Name:         "bin-" + tc.name,
				SecretBinary: tc.binary,
			})
			require.NoError(t, err)

			out, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "bin-" + tc.name})
			require.NoError(t, err)
			assert.Equal(t, tc.binary, out.SecretBinary)
			assert.Empty(t, out.SecretString, "SecretString must be empty when binary was stored")
		})
	}
}

func TestAuditSM_SecretBinary_PutAndGet(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "bin-put",
		SecretString: "initial-string",
	})
	require.NoError(t, err)

	payload := []byte{0x01, 0x02, 0x03}
	_, err = b.PutSecretValue(ctx, &sm.PutSecretValueInput{
		SecretID:     "bin-put",
		SecretBinary: payload,
	})
	require.NoError(t, err)

	out, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "bin-put"})
	require.NoError(t, err)
	assert.Equal(t, payload, out.SecretBinary)
	assert.Empty(t, out.SecretString)
}

func TestAuditSM_SecretBinary_StringAndBinaryMutuallyExclusive(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "mutual-excl",
		SecretString: "str",
		SecretBinary: []byte("bin"),
	})
	require.Error(t, err, "specifying both SecretString and SecretBinary must fail")
}

// ---------------------------------------------------------------------------
// RestoreSecret state machine
// ---------------------------------------------------------------------------

func TestAuditSM_RestoreSecret_RestoredSecretIsReadable(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "restore-test",
		SecretString: "important-value",
	})
	require.NoError(t, err)

	_, err = b.DeleteSecret(ctx, &sm.DeleteSecretInput{
		SecretID:             "restore-test",
		RecoveryWindowInDays: ptr64(30),
	})
	require.NoError(t, err)

	// Secret in deleted state — GetSecretValue must fail.
	_, err = b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "restore-test"})
	require.Error(t, err)

	// Restore the secret.
	_, err = b.RestoreSecret(ctx, &sm.RestoreSecretInput{SecretID: "restore-test"})
	require.NoError(t, err)

	// Secret must now be readable again.
	out, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "restore-test"})
	require.NoError(t, err)
	assert.Equal(t, "important-value", out.SecretString)
}

func TestAuditSM_RestoreSecret_ClearsDeletedDate(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "restore-clear-test",
		SecretString: "v",
	})
	require.NoError(t, err)

	_, err = b.DeleteSecret(ctx, &sm.DeleteSecretInput{
		SecretID:             "restore-clear-test",
		RecoveryWindowInDays: ptr64(7),
	})
	require.NoError(t, err)

	_, err = b.RestoreSecret(ctx, &sm.RestoreSecretInput{SecretID: "restore-clear-test"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(ctx, &sm.DescribeSecretInput{SecretID: "restore-clear-test"})
	require.NoError(t, err)
	assert.Nil(t, desc.DeletedDate, "DeletedDate must be nil after RestoreSecret")
}

// ---------------------------------------------------------------------------
// UpdateSecretVersionStage edge cases
// ---------------------------------------------------------------------------

func TestAuditSM_UpdateSecretVersionStage_CannotRemoveAWSCURRENTWithoutMove(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "stage-remove-test",
		SecretString: "v1",
	})
	require.NoError(t, err)

	cur, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "stage-remove-test"})
	require.NoError(t, err)

	_, err = b.UpdateSecretVersionStage(ctx, &sm.UpdateSecretVersionStageInput{
		SecretID:            "stage-remove-test",
		VersionStage:        "AWSCURRENT",
		RemoveFromVersionID: cur.VersionID,
	})
	require.Error(t, err, "removing AWSCURRENT without MoveToVersionID must fail")
}

func TestAuditSM_UpdateSecretVersionStage_MoveAWSCURRENT(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "stage-move-test",
		SecretString: "v1",
	})
	require.NoError(t, err)

	// Add v2 with only AWSPENDING — does not change AWSCURRENT.
	pv2, err := b.PutSecretValue(ctx, &sm.PutSecretValueInput{
		SecretID:      "stage-move-test",
		SecretString:  "v2",
		VersionStages: []string{"AWSPENDING"},
	})
	require.NoError(t, err)
	v2ID := pv2.VersionID

	v1, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "stage-move-test"})
	require.NoError(t, err)
	v1ID := v1.VersionID

	// Move AWSCURRENT from v1 → v2.
	_, err = b.UpdateSecretVersionStage(ctx, &sm.UpdateSecretVersionStageInput{
		SecretID:            "stage-move-test",
		VersionStage:        "AWSCURRENT",
		MoveToVersionID:     v2ID,
		RemoveFromVersionID: v1ID,
	})
	require.NoError(t, err)

	cur, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "stage-move-test"})
	require.NoError(t, err)
	assert.Equal(t, v2ID, cur.VersionID, "AWSCURRENT must point to v2 after move")
	assert.Equal(t, "v2", cur.SecretString)
}

// ---------------------------------------------------------------------------
// GetRandomPassword — table-driven
// ---------------------------------------------------------------------------

func TestAuditSM_GetRandomPassword_Constraints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkFn func(t *testing.T, pw string)
		name    string
		input   sm.GetRandomPasswordInput
		wantErr bool
	}{
		{
			name:  "default_length_32",
			input: sm.GetRandomPasswordInput{},
			checkFn: func(t *testing.T, pw string) {
				t.Helper()
				assert.Len(t, pw, 32)
			},
		},
		{
			name:  "custom_length_16",
			input: sm.GetRandomPasswordInput{PasswordLength: ptr64(16)},
			checkFn: func(t *testing.T, pw string) {
				t.Helper()
				assert.Len(t, pw, 16)
			},
		},
		{
			name:    "length_zero_rejected",
			input:   sm.GetRandomPasswordInput{PasswordLength: ptr64(0)},
			wantErr: true,
		},
		{
			name:  "exclude_numbers",
			input: sm.GetRandomPasswordInput{PasswordLength: ptr64(100), ExcludeNumbers: true},
			checkFn: func(t *testing.T, pw string) {
				t.Helper()
				for _, c := range pw {
					assert.False(t, c >= '0' && c <= '9', "should not contain digits, got: %s", pw)
				}
			},
		},
		{
			name:  "exclude_uppercase",
			input: sm.GetRandomPasswordInput{PasswordLength: ptr64(100), ExcludeUppercase: true},
			checkFn: func(t *testing.T, pw string) {
				t.Helper()
				for _, c := range pw {
					assert.False(
						t,
						c >= 'A' && c <= 'Z',
						"should not contain uppercase, got: %s",
						pw,
					)
				}
			},
		},
		{
			name:  "exclude_lowercase",
			input: sm.GetRandomPasswordInput{PasswordLength: ptr64(100), ExcludeLowercase: true},
			checkFn: func(t *testing.T, pw string) {
				t.Helper()
				for _, c := range pw {
					assert.False(
						t,
						c >= 'a' && c <= 'z',
						"should not contain lowercase, got: %s",
						pw,
					)
				}
			},
		},
		{
			name: "exclude_specific_chars",
			input: sm.GetRandomPasswordInput{
				PasswordLength:    ptr64(100),
				ExcludeCharacters: "ABCabc123",
			},
			checkFn: func(t *testing.T, pw string) {
				t.Helper()
				for _, c := range "ABCabc123" {
					assert.NotContains(t, pw, string(c), "excluded char %q should not appear", c)
				}
			},
		},
		{
			name: "include_space_with_require_each_type",
			input: sm.GetRandomPasswordInput{
				PasswordLength:          ptr64(200),
				IncludeSpace:            true,
				RequireEachIncludedType: true,
			},
			checkFn: func(t *testing.T, pw string) {
				t.Helper()
				assert.Contains(
					t,
					pw,
					" ",
					"with IncludeSpace+RequireEachIncludedType, space must appear",
				)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			out, err := b.GetRandomPassword(&tc.input)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			if tc.checkFn != nil {
				tc.checkFn(t, out.RandomPassword)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ListSecrets — filter coverage
// ---------------------------------------------------------------------------

func TestAuditSM_ListSecrets_Filters(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	secrets := []sm.CreateSecretInput{
		{Name: "alpha-one", SecretString: "v", Description: "First alpha secret",
			Tags: []sm.Tag{{Key: "env", Value: "prod"}}},
		{Name: "alpha-two", SecretString: "v", Description: "Second alpha secret",
			Tags: []sm.Tag{{Key: "env", Value: "staging"}}},
		{Name: "beta-one", SecretString: "v", Description: "Beta secret",
			Tags: []sm.Tag{{Key: "team", Value: "platform"}}},
	}
	for i := range secrets {
		_, err := b.CreateSecret(ctx, &secrets[i])
		require.NoError(t, err)
	}

	tests := []struct {
		name      string
		filters   []sm.SecretFilter
		wantNames []string
	}{
		{
			name:      "filter_by_name_prefix",
			filters:   []sm.SecretFilter{{Key: "name", Values: []string{"alpha"}}},
			wantNames: []string{"alpha-one", "alpha-two"},
		},
		{
			name:      "filter_by_description",
			filters:   []sm.SecretFilter{{Key: "description", Values: []string{"Beta"}}},
			wantNames: []string{"beta-one"},
		},
		{
			name:      "filter_by_tag_key",
			filters:   []sm.SecretFilter{{Key: "tag-key", Values: []string{"env"}}},
			wantNames: []string{"alpha-one", "alpha-two"},
		},
		{
			name:      "filter_by_tag_value",
			filters:   []sm.SecretFilter{{Key: "tag-value", Values: []string{"prod"}}},
			wantNames: []string{"alpha-one"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.ListSecrets(ctx, &sm.ListSecretsInput{Filters: tc.filters})
			require.NoError(t, err)

			var gotNames []string
			for _, s := range out.SecretList {
				gotNames = append(gotNames, s.Name)
			}

			for _, wantName := range tc.wantNames {
				assert.Contains(t, gotNames, wantName)
			}
			assert.Len(t, gotNames, len(tc.wantNames),
				"filter %v returned unexpected secrets: %v", tc.filters, gotNames)
		})
	}
}

// ---------------------------------------------------------------------------
// ListSecretVersionIds
// ---------------------------------------------------------------------------

func TestAuditSM_ListSecretVersionIDs_IncludeDeprecated(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "ver-list-test",
		SecretString: "v1",
	})
	require.NoError(t, err)

	for i := range 4 {
		_, err = b.PutSecretValue(ctx, &sm.PutSecretValueInput{
			SecretID:     "ver-list-test",
			SecretString: "v" + string(rune('2'+i)),
		})
		require.NoError(t, err)
	}

	// Without IncludeDeprecated: only versions with staging labels returned.
	withoutDep, err := b.ListSecretVersionIDs(ctx, &sm.ListSecretVersionIDsInput{
		SecretID: "ver-list-test",
	})
	require.NoError(t, err)

	// With IncludeDeprecated: all versions returned.
	withDep, err := b.ListSecretVersionIDs(ctx, &sm.ListSecretVersionIDsInput{
		SecretID:          "ver-list-test",
		IncludeDeprecated: true,
	})
	require.NoError(t, err)

	assert.GreaterOrEqual(t, len(withDep.Versions), len(withoutDep.Versions),
		"IncludeDeprecated must return at least as many versions")
}

// ---------------------------------------------------------------------------
// DescribeSecret round-trip
// ---------------------------------------------------------------------------

func TestAuditSM_DescribeSecret_AllFields(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
		Name:         "describe-test",
		SecretString: "v",
		Description:  "test description",
		KmsKeyID:     "arn:aws:kms:us-east-1:123456789012:key/abc",
		Tags:         []sm.Tag{{Key: "app", Value: "myapp"}},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(ctx, &sm.DescribeSecretInput{SecretID: "describe-test"})
	require.NoError(t, err)

	assert.Equal(t, "describe-test", desc.Name)
	assert.Equal(t, "test description", desc.Description)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/abc", desc.KmsKeyID)
	assert.NotEmpty(t, desc.ARN)
	assert.NotNil(t, desc.CreatedDate)
	assert.Nil(t, desc.DeletedDate)
	require.NotNil(t, desc.Tags)
	tagMap := desc.Tags.Clone()
	assert.Equal(t, "myapp", tagMap["app"])
}

// ---------------------------------------------------------------------------
// UpdateSecret
// ---------------------------------------------------------------------------

func TestAuditSM_UpdateSecret_ValueAndMeta(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkFn     func(t *testing.T, desc *sm.DescribeSecretOutput, val *sm.GetSecretValueOutput)
		name        string
		updateInput sm.UpdateSecretInput
	}{
		{
			name: "update_description_only",
			updateInput: sm.UpdateSecretInput{
				SecretID:    "update-test",
				Description: "updated description",
			},
			checkFn: func(t *testing.T, desc *sm.DescribeSecretOutput, val *sm.GetSecretValueOutput) {
				t.Helper()
				assert.Equal(t, "updated description", desc.Description)
				assert.Equal(
					t,
					"original",
					val.SecretString,
					"value must not change on meta-only update",
				)
			},
		},
		{
			name: "update_value",
			updateInput: sm.UpdateSecretInput{
				SecretID:     "update-test",
				SecretString: "updated-value",
			},
			checkFn: func(t *testing.T, _ *sm.DescribeSecretOutput, val *sm.GetSecretValueOutput) {
				t.Helper()
				assert.Equal(t, "updated-value", val.SecretString)
			},
		},
		{
			name: "update_kms_key",
			updateInput: sm.UpdateSecretInput{
				SecretID: "update-test",
				KmsKeyID: "new-key-id",
			},
			checkFn: func(t *testing.T, desc *sm.DescribeSecretOutput, _ *sm.GetSecretValueOutput) {
				t.Helper()
				assert.Equal(t, "new-key-id", desc.KmsKeyID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			ctx := context.Background()

			_, err := b.CreateSecret(ctx, &sm.CreateSecretInput{
				Name:         "update-test",
				SecretString: "original",
				Description:  "original description",
			})
			require.NoError(t, err)

			_, err = b.UpdateSecret(ctx, &tc.updateInput)
			require.NoError(t, err)

			desc, err := b.DescribeSecret(ctx, &sm.DescribeSecretInput{SecretID: "update-test"})
			require.NoError(t, err)

			val, err := b.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretID: "update-test"})
			require.NoError(t, err)

			tc.checkFn(t, desc, val)
		})
	}
}
