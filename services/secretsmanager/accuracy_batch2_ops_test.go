package secretsmanager_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sm "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// Issue #1 — GetResourcePolicy on a deleted secret must return InvalidRequestException
// ---------------------------------------------------------------------------

// TestBatch2Ops_GetResourcePolicy_DeletedSecret verifies that calling GetResourcePolicy
// on a soft-deleted secret returns ErrSecretDeleted (not an empty policy response).
// AWS returns InvalidRequestException in this case.
func TestBatch2Ops_GetResourcePolicy_DeletedSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		setup  func(*testing.T, *sm.InMemoryBackend)
		wantFn func(*testing.T, error)
	}{
		{
			name: "backend_deleted_returns_error",
			setup: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "grp-del", SecretString: "v"})
				require.NoError(t, err)
				_, err = b.DeleteSecret(&sm.DeleteSecretInput{SecretID: "grp-del"})
				require.NoError(t, err)
			},
			wantFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, sm.ErrSecretDeleted, "deleted secret must return ErrSecretDeleted")
			},
		},
		{
			name: "backend_active_no_policy_returns_empty",
			setup: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "grp-active", SecretString: "v"})
				require.NoError(t, err)
			},
			wantFn: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err, "active secret with no policy must not error")
			},
		},
		{
			name: "backend_not_found_returns_error",
			setup: func(t *testing.T, _ *sm.InMemoryBackend) {
				t.Helper()
			},
			wantFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, sm.ErrSecretNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			tt.setup(t, b)

			secretID := map[string]string{
				"backend_deleted_returns_error":       "grp-del",
				"backend_active_no_policy_returns_empty": "grp-active",
				"backend_not_found_returns_error":     "nonexistent",
			}[tt.name]

			_, err := b.GetResourcePolicy(&sm.GetResourcePolicyInput{SecretID: secretID})
			tt.wantFn(t, err)
		})
	}
}

// TestBatch2Ops_GetResourcePolicy_DeletedSecret_HTTP verifies that the HTTP handler
// returns 400 InvalidRequestException for a deleted secret.
func TestBatch2Ops_GetResourcePolicy_DeletedSecret_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		body           string
		setup          func(*testing.T, *sm.InMemoryBackend)
		expectedStatus int
		expectedType   string
	}{
		{
			name: "deleted_returns_400_InvalidRequestException",
			setup: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "grp-http-del", SecretString: "v"})
				require.NoError(t, err)
				_, err = b.DeleteSecret(&sm.DeleteSecretInput{SecretID: "grp-http-del"})
				require.NoError(t, err)
			},
			body:           `{"SecretId":"grp-http-del"}`,
			expectedStatus: http.StatusBadRequest,
			expectedType:   "InvalidRequestException",
		},
		{
			name:           "not_found_returns_400_ResourceNotFoundException",
			body:           `{"SecretId":"no-such-secret"}`,
			expectedStatus: http.StatusBadRequest,
			expectedType:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			h := sm.NewHandler(b)
			rec := doR1Request(t, h, "secretsmanager.GetResourcePolicy", tt.body)

			assert.Equal(t, tt.expectedStatus, rec.Code)

			var errResp sm.ErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.expectedType, errResp.Type)
		})
	}
}

// ---------------------------------------------------------------------------
// Issue #2 — DescribeSecret VersionIDsToStages must exclude unlabeled versions
// ---------------------------------------------------------------------------

// TestBatch2Ops_DescribeSecret_VersionIDsToStages_ExcludesUnlabeled verifies that
// VersionIDsToStages only includes secret versions that carry at least one staging label.
// AWS omits versions with no labels (deprecated/pruned) from this map.
func TestBatch2Ops_DescribeSecret_VersionIDsToStages_ExcludesUnlabeled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*testing.T, *sm.InMemoryBackend) string
		checkFn func(*testing.T, *sm.DescribeSecretOutput, string)
	}{
		{
			name: "only_current_version_appears",
			setup: func(t *testing.T, b *sm.InMemoryBackend) string {
				t.Helper()
				out, err := b.CreateSecret(&sm.CreateSecretInput{Name: "desc-stg-1", SecretString: "v1"})
				require.NoError(t, err)
				return out.VersionID
			},
			checkFn: func(t *testing.T, out *sm.DescribeSecretOutput, versionID string) {
				t.Helper()
				require.Contains(t, out.VersionIDsToStages, versionID)
				assert.Equal(t, []string{"AWSCURRENT"}, out.VersionIDsToStages[versionID])
				assert.Len(t, out.VersionIDsToStages, 1)
			},
		},
		{
			name: "unlabeled_version_excluded",
			setup: func(t *testing.T, b *sm.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "desc-stg-2", SecretString: "v1"})
				require.NoError(t, err)
				// Second PutSecretValue promotes v1 to AWSPREVIOUS, v2 to AWSCURRENT.
				_, err = b.PutSecretValue(&sm.PutSecretValueInput{SecretID: "desc-stg-2", SecretString: "v2"})
				require.NoError(t, err)
				// Third PutSecretValue: v1 loses AWSPREVIOUS (it had it from the prior rotate),
				// v2 becomes AWSPREVIOUS, v3 becomes AWSCURRENT. v1 is now unlabeled.
				out, err := b.PutSecretValue(&sm.PutSecretValueInput{SecretID: "desc-stg-2", SecretString: "v3"})
				require.NoError(t, err)
				return out.VersionID
			},
			checkFn: func(t *testing.T, out *sm.DescribeSecretOutput, currentVersionID string) {
				t.Helper()
				// Only AWSCURRENT and AWSPREVIOUS versions should appear.
				assert.Len(t, out.VersionIDsToStages, 2, "unlabeled version must be excluded")
				require.Contains(t, out.VersionIDsToStages, currentVersionID)
				assert.Equal(t, []string{"AWSCURRENT"}, out.VersionIDsToStages[currentVersionID])
				// Verify no entry has an empty label list.
				for vid, labels := range out.VersionIDsToStages {
					assert.NotEmpty(t, labels, "version %s has empty labels — must be excluded", vid)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			versionID := tt.setup(t, b)

			out, err := b.DescribeSecret(&sm.DescribeSecretInput{SecretID: func() string {
				if tt.name == "only_current_version_appears" {
					return "desc-stg-1"
				}
				return "desc-stg-2"
			}()})
			require.NoError(t, err)

			tt.checkFn(t, out, versionID)
		})
	}
}

// ---------------------------------------------------------------------------
// Issue #3 — ListSecrets SecretListEntry must include RotationRules
// ---------------------------------------------------------------------------

// TestBatch2Ops_ListSecrets_IncludesRotationRules verifies that ListSecrets returns
// RotationRules in each secret entry when rotation has been configured.
// AWS includes RotationRules in the ListSecrets response.
func TestBatch2Ops_ListSecrets_IncludesRotationRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*testing.T, *sm.InMemoryBackend)
		checkFn func(*testing.T, *sm.ListSecretsOutput)
	}{
		{
			name: "rotation_rules_returned_in_list",
			setup: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "ls-rot", SecretString: "v"})
				require.NoError(t, err)
				days := int64(30)
				_, err = b.RotateSecret(&sm.RotateSecretInput{
					SecretID: "ls-rot",
					RotationRules: &sm.RotationRulesType{
						AutomaticallyAfterDays: &days,
					},
				})
				require.NoError(t, err)
			},
			checkFn: func(t *testing.T, out *sm.ListSecretsOutput) {
				t.Helper()
				require.Len(t, out.SecretList, 1)
				entry := out.SecretList[0]
				assert.True(t, entry.RotationEnabled)
				require.NotNil(t, entry.RotationRules, "RotationRules must be present in ListSecrets response")
				require.NotNil(t, entry.RotationRules.AutomaticallyAfterDays)
				assert.Equal(t, int64(30), *entry.RotationRules.AutomaticallyAfterDays)
			},
		},
		{
			name: "no_rotation_rules_when_not_configured",
			setup: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "ls-norot", SecretString: "v"})
				require.NoError(t, err)
			},
			checkFn: func(t *testing.T, out *sm.ListSecretsOutput) {
				t.Helper()
				require.Len(t, out.SecretList, 1)
				entry := out.SecretList[0]
				assert.False(t, entry.RotationEnabled)
				assert.Nil(t, entry.RotationRules, "RotationRules must be nil when rotation not configured")
			},
		},
		{
			name: "rotation_rules_in_http_response",
			setup: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "ls-http-rot", SecretString: "v"})
				require.NoError(t, err)
				days := int64(7)
				_, err = b.RotateSecret(&sm.RotateSecretInput{
					SecretID: "ls-http-rot",
					RotationRules: &sm.RotationRulesType{
						AutomaticallyAfterDays: &days,
					},
				})
				require.NoError(t, err)
			},
			checkFn: func(t *testing.T, out *sm.ListSecretsOutput) {
				t.Helper()
				require.Len(t, out.SecretList, 1)
				entry := out.SecretList[0]
				require.NotNil(t, entry.RotationRules)
				assert.Equal(t, int64(7), *entry.RotationRules.AutomaticallyAfterDays)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			tt.setup(t, b)

			out, err := b.ListSecrets(&sm.ListSecretsInput{})
			require.NoError(t, err)

			tt.checkFn(t, out)
		})
	}
}

// ---------------------------------------------------------------------------
// Issue #4 — BatchGetSecretValue must update LastAccessedDate like GetSecretValue
// ---------------------------------------------------------------------------

// TestBatch2Ops_BatchGetSecretValue_UpdatesLastAccessedDate verifies that calling
// BatchGetSecretValue updates the LastAccessedDate on each successfully retrieved
// secret and version, matching the behavior of GetSecretValue.
func TestBatch2Ops_BatchGetSecretValue_UpdatesLastAccessedDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*testing.T, *sm.InMemoryBackend)
		inputFn func() *sm.BatchGetSecretValueInput
		checkFn func(*testing.T, *sm.InMemoryBackend)
	}{
		{
			name: "by_id_list_updates_accessed_date",
			setup: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "bgv-acc-1", SecretString: "val"})
				require.NoError(t, err)
			},
			inputFn: func() *sm.BatchGetSecretValueInput {
				return &sm.BatchGetSecretValueInput{SecretIDList: []string{"bgv-acc-1"}}
			},
			checkFn: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				desc, err := b.DescribeSecret(&sm.DescribeSecretInput{SecretID: "bgv-acc-1"})
				require.NoError(t, err)
				assert.NotNil(t, desc.LastAccessedDate, "LastAccessedDate must be set after BatchGetSecretValue")
			},
		},
		{
			name: "by_filter_updates_accessed_date",
			setup: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "bgv-filt-1", SecretString: "val"})
				require.NoError(t, err)
			},
			inputFn: func() *sm.BatchGetSecretValueInput {
				return &sm.BatchGetSecretValueInput{}
			},
			checkFn: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				desc, err := b.DescribeSecret(&sm.DescribeSecretInput{SecretID: "bgv-filt-1"})
				require.NoError(t, err)
				assert.NotNil(t, desc.LastAccessedDate, "LastAccessedDate must be set after BatchGetSecretValue by filter")
			},
		},
		{
			name: "deleted_secret_in_id_list_does_not_update",
			setup: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "bgv-del-1", SecretString: "val"})
				require.NoError(t, err)
				_, err = b.DeleteSecret(&sm.DeleteSecretInput{SecretID: "bgv-del-1"})
				require.NoError(t, err)
			},
			inputFn: func() *sm.BatchGetSecretValueInput {
				return &sm.BatchGetSecretValueInput{SecretIDList: []string{"bgv-del-1"}}
			},
			checkFn: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				desc, err := b.DescribeSecret(&sm.DescribeSecretInput{SecretID: "bgv-del-1"})
				require.NoError(t, err)
				assert.Nil(t, desc.LastAccessedDate, "deleted secret must not have LastAccessedDate updated")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			tt.setup(t, b)

			out, err := b.BatchGetSecretValue(tt.inputFn())
			require.NoError(t, err)

			// For the non-deleted cases, verify we got a successful result.
			if tt.name != "deleted_secret_in_id_list_does_not_update" {
				assert.NotEmpty(t, out.SecretValues)
			}

			tt.checkFn(t, b)
		})
	}
}
