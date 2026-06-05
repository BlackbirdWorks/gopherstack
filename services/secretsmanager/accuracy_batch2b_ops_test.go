package secretsmanager_test

// accuracy_batch2b_ops_test.go — second batch-2 AWS-accuracy tests (go-8p57).
//
// Covers three accuracy gaps:
//
//  1. CreateSecret with the name of a soft-deleted secret must return
//     InvalidRequestException, not ResourceExistsException.
//
//  2. BatchGetSecretValue must reject SecretIdList with more than 20 entries
//     with InvalidParameterException.
//
//  3. UpdateSecretVersionStage must reject attempts to remove the AWSCURRENT
//     staging label without moving it to another version.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sm "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// Issue #1 — CreateSecret on a soft-deleted name must return InvalidRequestException
// ---------------------------------------------------------------------------

// TestBatch2B_CreateSecret_DeletedNameCollision verifies that CreateSecret returns
// ErrSecretDeleted (InvalidRequestException) when a secret with the same name is
// pending deletion. AWS distinguishes the two cases: active-name → ResourceExistsException,
// deleted-name → InvalidRequestException.
func TestBatch2B_CreateSecret_DeletedNameCollision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*testing.T, *sm.InMemoryBackend)
		wantFn  func(*testing.T, error)
		name    string
		newName string
	}{
		{
			name:    "deleted_name_returns_invalid_request",
			newName: "cs-del-collision",
			setup: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "cs-del-collision", SecretString: "v"})
				require.NoError(t, err)
				_, err = b.DeleteSecret(&sm.DeleteSecretInput{SecretID: "cs-del-collision"})
				require.NoError(t, err)
			},
			wantFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, sm.ErrSecretDeleted,
					"soft-deleted name collision must return ErrSecretDeleted (InvalidRequestException)")
				require.NotErrorIs(t, err, sm.ErrSecretAlreadyExists,
					"must NOT return ResourceExistsException for deleted-name collision")
			},
		},
		{
			name:    "active_name_still_returns_resource_exists",
			newName: "cs-active-collision",
			setup: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "cs-active-collision", SecretString: "v"})
				require.NoError(t, err)
			},
			wantFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, sm.ErrSecretAlreadyExists,
					"active-name collision must still return ErrSecretAlreadyExists")
			},
		},
		{
			name:    "force_deleted_name_allows_recreation",
			newName: "cs-force-del",
			setup: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "cs-force-del", SecretString: "v"})
				require.NoError(t, err)
				_, err = b.DeleteSecret(&sm.DeleteSecretInput{
					SecretID:                   "cs-force-del",
					ForceDeleteWithoutRecovery: true,
				})
				require.NoError(t, err)
			},
			wantFn: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err, "force-deleted secret name must be available for reuse")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			tt.setup(t, b)

			_, err := b.CreateSecret(&sm.CreateSecretInput{Name: tt.newName, SecretString: "new"})
			tt.wantFn(t, err)
		})
	}
}

// TestBatch2B_CreateSecret_DeletedNameCollision_HTTP verifies that the HTTP handler
// returns 400 InvalidRequestException (not ResourceExistsException) for a deleted-name collision.
func TestBatch2B_CreateSecret_DeletedNameCollision_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *sm.InMemoryBackend)
		name         string
		body         string
		wantErrType  string
		wantStatus   int
	}{
		{
			name: "deleted_name_returns_400_InvalidRequestException",
			setup: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "cs-http-del", SecretString: "v"})
				require.NoError(t, err)
				_, err = b.DeleteSecret(&sm.DeleteSecretInput{SecretID: "cs-http-del"})
				require.NoError(t, err)
			},
			body:        `{"Name":"cs-http-del","SecretString":"new"}`,
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidRequestException",
		},
		{
			name: "active_name_returns_400_ResourceExistsException",
			setup: func(t *testing.T, b *sm.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "cs-http-active", SecretString: "v"})
				require.NoError(t, err)
			},
			body:        `{"Name":"cs-http-active","SecretString":"new"}`,
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ResourceExistsException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			tt.setup(t, b)

			h := sm.NewHandler(b)
			rec := doR1Request(t, h, "secretsmanager.CreateSecret", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			var errResp sm.ErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantErrType, errResp.Type)
		})
	}
}

// ---------------------------------------------------------------------------
// Issue #2 — BatchGetSecretValue SecretIdList max-length validation (20 entries)
// ---------------------------------------------------------------------------

// TestBatch2B_BatchGetSecretValue_SecretIDListTooLong verifies that BatchGetSecretValue
// rejects a SecretIdList with more than 20 entries with InvalidParameterException.
// AWS enforces this limit: "SecretIdList must not contain more than 20 entries."
func TestBatch2B_BatchGetSecretValue_SecretIDListTooLong(t *testing.T) {
	t.Parallel()

	makeIDList := func(n int) []string {
		ids := make([]string, n)
		for i := range ids {
			ids[i] = fmt.Sprintf("secret-%d", i+1)
		}

		return ids
	}

	tests := []struct {
		name    string
		ids     []string
		wantErr bool
	}{
		{
			name:    "exactly_20_ids_allowed",
			ids:     makeIDList(20),
			wantErr: false,
		},
		{
			name:    "21_ids_rejected",
			ids:     makeIDList(21),
			wantErr: true,
		},
		{
			name:    "far_above_limit_rejected",
			ids:     makeIDList(100),
			wantErr: true,
		},
		{
			name:    "empty_list_allowed",
			ids:     []string{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			_, err := b.BatchGetSecretValue(&sm.BatchGetSecretValueInput{SecretIDList: tt.ids})

			if tt.wantErr {
				require.ErrorIs(t, err, sm.ErrInvalidParameter,
					"SecretIdList > 20 must return ErrInvalidParameter")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestBatch2B_BatchGetSecretValue_SecretIDListTooLong_HTTP verifies that the HTTP handler
// returns 400 InvalidParameterException when SecretIdList exceeds 20 entries.
func TestBatch2B_BatchGetSecretValue_SecretIDListTooLong_HTTP(t *testing.T) {
	t.Parallel()

	makeJSONIDList := func(n int) string {
		ids := make([]string, n)
		for i := range ids {
			ids[i] = fmt.Sprintf(`"secret-%d"`, i+1)
		}

		listJSON := "["
		for i, id := range ids {
			if i > 0 {
				listJSON += ","
			}

			listJSON += id
		}

		return listJSON + "]"
	}

	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantType   string
	}{
		{
			name:       "21_ids_returns_400_InvalidParameterException",
			body:       `{"SecretIdList":` + makeJSONIDList(21) + `}`,
			wantStatus: http.StatusBadRequest,
			wantType:   "InvalidParameterException",
		},
		{
			name:       "20_ids_returns_200",
			body:       `{"SecretIdList":` + makeJSONIDList(20) + `}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			h := sm.NewHandler(b)

			rec := doR1Request(t, h, "secretsmanager.BatchGetSecretValue", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				var errResp sm.ErrorResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantType, errResp.Type)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Issue #3 — UpdateSecretVersionStage must reject removing AWSCURRENT
// ---------------------------------------------------------------------------

// TestBatch2B_UpdateSecretVersionStage_CannotRemoveAWSCURRENT verifies that
// UpdateSecretVersionStage returns ErrInvalidParameter when the caller attempts to
// remove the AWSCURRENT staging label without providing a MoveToVersionId.
// AWS requires AWSCURRENT to always be assigned to exactly one version; removing it
// without a target is rejected with InvalidParameterException.
func TestBatch2B_UpdateSecretVersionStage_CannotRemoveAWSCURRENT(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*testing.T, *sm.InMemoryBackend) string
		wantFn  func(*testing.T, error)
		name    string
	}{
		{
			name: "remove_awscurrent_without_move_rejected",
			setup: func(t *testing.T, b *sm.InMemoryBackend) string {
				t.Helper()
				out, err := b.CreateSecret(&sm.CreateSecretInput{Name: "usvs-cur-1", SecretString: "v"})
				require.NoError(t, err)

				return out.VersionID
			},
			wantFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, sm.ErrInvalidParameter,
					"removing AWSCURRENT without MoveToVersionId must return ErrInvalidParameter")
			},
		},
		{
			name: "remove_non_current_label_allowed",
			setup: func(t *testing.T, b *sm.InMemoryBackend) string {
				t.Helper()
				out, err := b.CreateSecret(&sm.CreateSecretInput{Name: "usvs-noncur-1", SecretString: "v"})
				require.NoError(t, err)
				// Add a custom label to the version.
				_, err = b.UpdateSecretVersionStage(&sm.UpdateSecretVersionStageInput{
					SecretID:        "usvs-noncur-1",
					VersionStage:    "CUSTOM-LABEL",
					MoveToVersionID: out.VersionID,
				})
				require.NoError(t, err)

				return out.VersionID
			},
			wantFn: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err, "removing a non-AWSCURRENT label should succeed")
			},
		},
		{
			name: "move_awscurrent_to_new_version_allowed",
			setup: func(t *testing.T, b *sm.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "usvs-move-1", SecretString: "v1"})
				require.NoError(t, err)
				out2, err := b.PutSecretValue(&sm.PutSecretValueInput{SecretID: "usvs-move-1", SecretString: "v2"})
				require.NoError(t, err)

				return out2.VersionID
			},
			wantFn: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err, "moving AWSCURRENT to another version should succeed")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			versionID := tt.setup(t, b)

			var input *sm.UpdateSecretVersionStageInput

			switch tt.name {
			case "remove_awscurrent_without_move_rejected":
				input = &sm.UpdateSecretVersionStageInput{
					SecretID:            "usvs-cur-1",
					VersionStage:        "AWSCURRENT",
					RemoveFromVersionID: versionID,
				}
			case "remove_non_current_label_allowed":
				input = &sm.UpdateSecretVersionStageInput{
					SecretID:            "usvs-noncur-1",
					VersionStage:        "CUSTOM-LABEL",
					RemoveFromVersionID: versionID,
				}
			case "move_awscurrent_to_new_version_allowed":
				input = &sm.UpdateSecretVersionStageInput{
					SecretID:        "usvs-move-1",
					VersionStage:    "AWSCURRENT",
					MoveToVersionID: versionID,
				}
			}

			_, err := b.UpdateSecretVersionStage(input)
			tt.wantFn(t, err)
		})
	}
}

// TestBatch2B_UpdateSecretVersionStage_CannotRemoveAWSCURRENT_HTTP verifies that the
// HTTP handler returns 400 InvalidParameterException for the remove-AWSCURRENT case.
func TestBatch2B_UpdateSecretVersionStage_CannotRemoveAWSCURRENT_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *sm.InMemoryBackend) string
		name        string
		wantErrType string
		wantStatus  int
	}{
		{
			name: "remove_awscurrent_returns_400",
			setup: func(t *testing.T, b *sm.InMemoryBackend) string {
				t.Helper()
				out, err := b.CreateSecret(&sm.CreateSecretInput{Name: "usvs-http-cur", SecretString: "v"})
				require.NoError(t, err)

				return out.VersionID
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			versionID := tt.setup(t, b)

			h := sm.NewHandler(b)
			body := fmt.Sprintf(
				`{"SecretId":"usvs-http-cur","VersionStage":"AWSCURRENT","RemoveFromVersionId":%q}`,
				versionID,
			)
			rec := doR1Request(t, h, "secretsmanager.UpdateSecretVersionStage", body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			var errResp sm.ErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantErrType, errResp.Type)
		})
	}
}
