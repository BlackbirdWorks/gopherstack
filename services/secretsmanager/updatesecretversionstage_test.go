package secretsmanager_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// RemoveFromVersionId contract
// ---------------------------------------------------------------------------

// TestUpdateSecretVersionStage_RemoveFromVersionIDRequiredWhenLabelElsewhere verifies the
// real API contract on UpdateSecretVersionStageInput.RemoveFromVersionId: "If the staging
// label is already attached to a different version of the secret, then you must also
// specify the RemoveFromVersionId parameter... If the label is attached and you either do
// not specify this parameter, or the version ID does not match, then the operation
// fails." This mock previously stripped the label from wherever it happened to be,
// regardless of RemoveFromVersionId — a real client relying on that guard rail (e.g. to
// avoid racing another rotation step) would never see the failure it depends on.
func TestUpdateSecretVersionStage_RemoveFromVersionIDRequiredWhenLabelElsewhere(t *testing.T) {
	t.Parallel()

	const label = "CUSTOM-LABEL"

	setup := func(t *testing.T) (*secretsmanager.InMemoryBackend, string, string) {
		t.Helper()

		b := secretsmanager.NewInMemoryBackend()
		out, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
			Name: "usvs-move-guard", SecretString: "v1",
		})
		require.NoError(t, err)
		holderVersion := out.VersionID

		put, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
			SecretID: "usvs-move-guard", SecretString: "v2", VersionStages: []string{"AWSPENDING"},
		})
		require.NoError(t, err)
		otherVersion := put.VersionID

		// Attach the custom label to holderVersion only.
		_, err = b.UpdateSecretVersionStage(context.Background(), &secretsmanager.UpdateSecretVersionStageInput{
			SecretID: "usvs-move-guard", VersionStage: label, MoveToVersionID: holderVersion,
		})
		require.NoError(t, err)

		return b, holderVersion, otherVersion
	}

	cases := []struct {
		buildInput func(holderVersion, otherVersion string) *secretsmanager.UpdateSecretVersionStageInput
		name       string
		wantErr    bool
	}{
		{
			name: "no_remove_from_version_id_rejected",
			buildInput: func(_, otherVersion string) *secretsmanager.UpdateSecretVersionStageInput {
				return &secretsmanager.UpdateSecretVersionStageInput{
					SecretID: "usvs-move-guard", VersionStage: label, MoveToVersionID: otherVersion,
				}
			},
			wantErr: true,
		},
		{
			name: "mismatched_remove_from_version_id_rejected",
			buildInput: func(_, otherVersion string) *secretsmanager.UpdateSecretVersionStageInput {
				return &secretsmanager.UpdateSecretVersionStageInput{
					SecretID: "usvs-move-guard", VersionStage: label, MoveToVersionID: otherVersion,
					RemoveFromVersionID: otherVersion, // wrong: label isn't on otherVersion
				}
			},
			wantErr: true,
		},
		{
			name: "correct_remove_from_version_id_accepted",
			buildInput: func(holderVersion, otherVersion string) *secretsmanager.UpdateSecretVersionStageInput {
				return &secretsmanager.UpdateSecretVersionStageInput{
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
				assert.ErrorIs(t, err, secretsmanager.ErrInvalidParameter)

				return
			}

			require.NoError(t, err)

			desc, err := b.DescribeSecret(
				context.Background(),
				&secretsmanager.DescribeSecretInput{SecretID: "usvs-move-guard"},
			)
			require.NoError(t, err)
			assert.Contains(t, desc.VersionIDsToStages[otherVersion], label)
			assert.NotContains(t, desc.VersionIDsToStages[holderVersion], label)
		})
	}
}

// ---------------------------------------------------------------------------
// UpdateSecretVersionStage comprehensive
// ---------------------------------------------------------------------------

func TestUpdateSecretVersionStage_MoveCustomLabel(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "usvs-move",
		SecretString:       "v1",
		ClientRequestToken: "ver-1",
	})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:           "usvs-move",
		SecretString:       "v2",
		ClientRequestToken: "ver-2",
	})
	require.NoError(t, err)

	// Move AWSPREVIOUS from ver-1 to ver-2
	_, err = b.UpdateSecretVersionStage(context.Background(), &secretsmanager.UpdateSecretVersionStageInput{
		SecretID:            "usvs-move",
		VersionStage:        "AWSPREVIOUS",
		MoveToVersionID:     "ver-2",
		RemoveFromVersionID: "ver-1",
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "usvs-move"})
	require.NoError(t, err)
	assert.Contains(t, desc.VersionIDsToStages["ver-2"], "AWSPREVIOUS")
}

func TestUpdateSecretVersionStage_RemoveLabel(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "usvs-rm",
		SecretString:       "v1",
		ClientRequestToken: "ver-1",
	})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:           "usvs-rm",
		SecretString:       "v2",
		ClientRequestToken: "ver-2",
	})
	require.NoError(t, err)

	// Remove AWSPREVIOUS from ver-1
	_, err = b.UpdateSecretVersionStage(context.Background(), &secretsmanager.UpdateSecretVersionStageInput{
		SecretID:            "usvs-rm",
		VersionStage:        "AWSPREVIOUS",
		RemoveFromVersionID: "ver-1",
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "usvs-rm"})
	require.NoError(t, err)
	for _, l := range desc.VersionIDsToStages["ver-1"] {
		assert.NotEqual(t, "AWSPREVIOUS", l, "AWSPREVIOUS must be removed from ver-1")
	}
}

func TestUpdateSecretVersionStage_TargetNotFound(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "usvs-miss", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.UpdateSecretVersionStage(context.Background(), &secretsmanager.UpdateSecretVersionStageInput{
		SecretID:        "usvs-miss",
		VersionStage:    "AWSPENDING",
		MoveToVersionID: "nonexistent",
	})
	require.ErrorIs(t, err, secretsmanager.ErrVersionNotFound)
}

func TestUpdateSecretVersionStage_SecretNotFound(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.UpdateSecretVersionStage(context.Background(), &secretsmanager.UpdateSecretVersionStageInput{
		SecretID:        "missing",
		VersionStage:    "AWSPENDING",
		MoveToVersionID: "ver-1",
	})
	require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
}

// ---------------------------------------------------------------------------
// AWSCURRENT edge cases
// ---------------------------------------------------------------------------

func TestUpdateSecretVersionStage_CannotRemoveAWSCURRENTWithoutMove(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         "stage-remove-test",
		SecretString: "v1",
	})
	require.NoError(t, err)

	cur, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretID: "stage-remove-test"})
	require.NoError(t, err)

	_, err = b.UpdateSecretVersionStage(ctx, &secretsmanager.UpdateSecretVersionStageInput{
		SecretID:            "stage-remove-test",
		VersionStage:        "AWSCURRENT",
		RemoveFromVersionID: cur.VersionID,
	})
	require.Error(t, err, "removing AWSCURRENT without MoveToVersionID must fail")
}

func TestUpdateSecretVersionStage_MoveAWSCURRENT(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         "stage-move-test",
		SecretString: "v1",
	})
	require.NoError(t, err)

	// Add v2 with only AWSPENDING — does not change AWSCURRENT.
	pv2, err := b.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretID:      "stage-move-test",
		SecretString:  "v2",
		VersionStages: []string{"AWSPENDING"},
	})
	require.NoError(t, err)
	v2ID := pv2.VersionID

	v1, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretID: "stage-move-test"})
	require.NoError(t, err)
	v1ID := v1.VersionID

	// Move AWSCURRENT from v1 → v2.
	_, err = b.UpdateSecretVersionStage(ctx, &secretsmanager.UpdateSecretVersionStageInput{
		SecretID:            "stage-move-test",
		VersionStage:        "AWSCURRENT",
		MoveToVersionID:     v2ID,
		RemoveFromVersionID: v1ID,
	})
	require.NoError(t, err)

	cur, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretID: "stage-move-test"})
	require.NoError(t, err)
	assert.Equal(t, v2ID, cur.VersionID, "AWSCURRENT must point to v2 after move")
	assert.Equal(t, "v2", cur.SecretString)
}

// TestUpdateSecretVersionStage_MoveAWSCURRENTToOlderVersion verifies moving AWSCURRENT
// back to an older, previously-current version updates CurrentVersionID. This is the
// inverse direction of TestUpdateSecretVersionStage_MoveAWSCURRENT above (which moves
// forward to a pending version).
func TestUpdateSecretVersionStage_MoveAWSCURRENTToOlderVersion(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "vs-curr", SecretString: "v1"},
	)
	require.NoError(t, err)

	put, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:     "vs-curr",
		SecretString: "v2",
	})
	require.NoError(t, err)
	v2 := put.VersionID

	// Move AWSCURRENT back to the original v1.
	desc1, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "vs-curr"})
	require.NoError(t, err)
	var v1 string
	for id, labels := range desc1.VersionIDsToStages {
		for _, l := range labels {
			if l == secretsmanager.StagingLabelPrevious {
				v1 = id
			}
		}
	}
	require.NotEmpty(t, v1)

	_, err = b.UpdateSecretVersionStage(context.Background(), &secretsmanager.UpdateSecretVersionStageInput{
		SecretID:            "vs-curr",
		VersionStage:        secretsmanager.StagingLabelCurrent,
		MoveToVersionID:     v1,
		RemoveFromVersionID: v2,
	})
	require.NoError(t, err)

	// v1 should now be AWSCURRENT.
	got, err := b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
		SecretID:     "vs-curr",
		VersionStage: secretsmanager.StagingLabelCurrent,
	})
	require.NoError(t, err)
	assert.Equal(t, v1, got.VersionID)
}

// ---------------------------------------------------------------------------
// AWSCURRENT removal rejection
//
// NOTE: this covers similar ground to TestUpdateSecretVersionStage_CannotRemoveAWSCURRENTWithoutMove
// above, but is table-driven with additional angles (removing a non-current label,
// moving AWSCURRENT to a new version) and a companion HTTP test, so both are kept.
// ---------------------------------------------------------------------------

// TestUpdateSecretVersionStage_CannotRemoveAWSCURRENT verifies that
// UpdateSecretVersionStage returns ErrInvalidParameter when the caller attempts to
// remove the AWSCURRENT staging label without providing a MoveToVersionId.
// AWS requires AWSCURRENT to always be assigned to exactly one version; removing it
// without a target is rejected with InvalidParameterException.
func TestUpdateSecretVersionStage_CannotRemoveAWSCURRENT(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(*testing.T, *secretsmanager.InMemoryBackend) string
		wantFn func(*testing.T, error)
		name   string
	}{
		{
			name: "remove_awscurrent_without_move_rejected",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) string {
				t.Helper()
				out, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "usvs-cur-1", SecretString: "v"},
				)
				require.NoError(t, err)

				return out.VersionID
			},
			wantFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, secretsmanager.ErrInvalidParameter,
					"removing AWSCURRENT without MoveToVersionId must return ErrInvalidParameter")
			},
		},
		{
			name: "remove_non_current_label_allowed",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) string {
				t.Helper()
				out, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "usvs-noncur-1", SecretString: "v"},
				)
				require.NoError(t, err)
				// Add a custom label to the version.
				_, err = b.UpdateSecretVersionStage(context.Background(), &secretsmanager.UpdateSecretVersionStageInput{
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
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "usvs-move-1", SecretString: "v1"},
				)
				require.NoError(t, err)
				out2, err := b.PutSecretValue(
					context.Background(),
					&secretsmanager.PutSecretValueInput{SecretID: "usvs-move-1", SecretString: "v2"},
				)
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

			b := secretsmanager.NewInMemoryBackend()
			versionID := tt.setup(t, b)

			var input *secretsmanager.UpdateSecretVersionStageInput

			switch tt.name {
			case "remove_awscurrent_without_move_rejected":
				input = &secretsmanager.UpdateSecretVersionStageInput{
					SecretID:            "usvs-cur-1",
					VersionStage:        "AWSCURRENT",
					RemoveFromVersionID: versionID,
				}
			case "remove_non_current_label_allowed":
				input = &secretsmanager.UpdateSecretVersionStageInput{
					SecretID:            "usvs-noncur-1",
					VersionStage:        "CUSTOM-LABEL",
					RemoveFromVersionID: versionID,
				}
			case "move_awscurrent_to_new_version_allowed":
				input = &secretsmanager.UpdateSecretVersionStageInput{
					SecretID:        "usvs-move-1",
					VersionStage:    "AWSCURRENT",
					MoveToVersionID: versionID,
				}
			}

			_, err := b.UpdateSecretVersionStage(context.Background(), input)
			tt.wantFn(t, err)
		})
	}
}

// TestUpdateSecretVersionStage_CannotRemoveAWSCURRENT_HTTP verifies that the
// HTTP handler returns 400 InvalidParameterException for the remove-AWSCURRENT case.
func TestUpdateSecretVersionStage_CannotRemoveAWSCURRENT_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *secretsmanager.InMemoryBackend) string
		name        string
		wantErrType string
		wantStatus  int
	}{
		{
			name: "remove_awscurrent_returns_400",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) string {
				t.Helper()
				out, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "usvs-http-cur", SecretString: "v"},
				)
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

			b := secretsmanager.NewInMemoryBackend()
			versionID := tt.setup(t, b)

			h := secretsmanager.NewHandler(b)
			body := fmt.Sprintf(
				`{"SecretId":"usvs-http-cur","VersionStage":"AWSCURRENT","RemoveFromVersionId":%q}`,
				versionID,
			)
			rec := doR1Request(t, h, "secretsmanager.UpdateSecretVersionStage", body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			var errResp secretsmanager.ErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantErrType, errResp.Type)
		})
	}
}

// ---------------------------------------------------------------------------
// HTTP dispatch table
// ---------------------------------------------------------------------------

// TestUpdateSecretVersionStage_HTTP verifies UpdateSecretVersionStage via HTTP dispatch.
func TestUpdateSecretVersionStage_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*testing.T, *secretsmanager.InMemoryBackend) string
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		bodyFn         func(versionID string) string
		name           string
		expectedStatus int
	}{
		{
			name: "move_label_to_new_version",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "stage-secret", SecretString: "v1"},
				)
				require.NoError(t, err)
				out, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
					SecretID:     "stage-secret",
					SecretString: "v2",
				})
				require.NoError(t, err)

				return out.VersionID
			},
			bodyFn: func(versionID string) string {
				return `{"SecretId":"stage-secret","VersionStage":"AWSCUSTOM","MoveToVersionId":"` + versionID + `"}`
			},
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.UpdateSecretVersionStageOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "stage-secret", out.Name)
				assert.NotEmpty(t, out.ARN)
			},
		},
		{
			name: "remove_label_from_version",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) string {
				t.Helper()
				out, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "stage-remove", SecretString: "v1"},
				)
				require.NoError(t, err)
				// Add a custom label first so we can remove it.
				// AWSCURRENT cannot be removed without MoveToVersionId (AWS constraint).
				_, err = b.UpdateSecretVersionStage(context.Background(), &secretsmanager.UpdateSecretVersionStageInput{
					SecretID:        "stage-remove",
					VersionStage:    "AWSCUSTOM",
					MoveToVersionID: out.VersionID,
				})
				require.NoError(t, err)

				return out.VersionID
			},
			bodyFn: func(versionID string) string {
				return `{"SecretId":"stage-remove","VersionStage":"AWSCUSTOM","RemoveFromVersionId":"` + versionID + `"}`
			},
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.UpdateSecretVersionStageOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "stage-remove", out.Name)
			},
		},
		{
			name: "not_found_secret",
			setup: func(t *testing.T, _ *secretsmanager.InMemoryBackend) string {
				t.Helper()

				return "irrelevant"
			},
			bodyFn: func(_ string) string {
				return `{"SecretId":"nonexistent","VersionStage":"AWSCUSTOM"}`
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "bad_json",
			setup: func(t *testing.T, _ *secretsmanager.InMemoryBackend) string {
				t.Helper()

				return ""
			},
			bodyFn: func(_ string) string {
				return `{bad}`
			},
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := secretsmanager.NewInMemoryBackend()
			var versionID string

			if tt.setup != nil {
				versionID = tt.setup(t, backend)
			}

			h := secretsmanager.NewHandler(backend)
			rec := doSMRequest(t, h, "secretsmanager.UpdateSecretVersionStage", tt.bodyFn(versionID))

			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Backend edge cases (ported from the original TestNewOpsBackend)
// ---------------------------------------------------------------------------

// TestUpdateSecretVersionStage_BackendEdgeCases verifies version-not-found errors for
// both RemoveFromVersionId and MoveToVersionId.
func TestUpdateSecretVersionStage_BackendEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("version_not_found_on_remove", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "stage-ver-nf", SecretString: "v"},
		)
		require.NoError(t, err)

		_, err = b.UpdateSecretVersionStage(context.Background(), &secretsmanager.UpdateSecretVersionStageInput{
			SecretID:            "stage-ver-nf",
			VersionStage:        "AWSCUSTOM",
			RemoveFromVersionID: "no-such-version",
		})
		require.ErrorIs(t, err, secretsmanager.ErrVersionNotFound)
	})

	t.Run("version_not_found_on_move", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "stage-mov-nf", SecretString: "v"},
		)
		require.NoError(t, err)

		_, err = b.UpdateSecretVersionStage(context.Background(), &secretsmanager.UpdateSecretVersionStageInput{
			SecretID:        "stage-mov-nf",
			VersionStage:    "AWSCUSTOM",
			MoveToVersionID: "no-such-version",
		})
		require.ErrorIs(t, err, secretsmanager.ErrVersionNotFound)
	})
}
