package secretsmanager_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// PutSecretValue comprehensive
// ---------------------------------------------------------------------------

func TestPutSecretValue_EmptyValueRejected(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "empty-put", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID: "empty-put",
	})
	require.ErrorIs(t, err, secretsmanager.ErrInvalidParameter,
		"PutSecretValue with no value must return InvalidParameterException")
}

func TestPutSecretValue_EmptyValueHTTP(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	doR1Request(t, h, "secretsmanager.CreateSecret", `{"Name":"ev-http","SecretString":"v"}`)

	rec := doR1Request(t, h, "secretsmanager.PutSecretValue", `{"SecretId":"ev-http"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestPutSecretValue_AWSCURRENT_Promoted(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "promote-test",
		SecretString:       "first",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	out, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:           "promote-test",
		SecretString:       "second",
		ClientRequestToken: "v2",
	})
	require.NoError(t, err)
	assert.Contains(t, out.VersionStages, secretsmanager.StagingLabelCurrent)

	// v2 should be AWSCURRENT
	val, err := b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{SecretID: "promote-test"})
	require.NoError(t, err)
	assert.Equal(t, "second", val.SecretString)
	assert.Equal(t, "v2", val.VersionID)
}

func TestPutSecretValue_Idempotent(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "idem-put", SecretString: "v"},
	)
	require.NoError(t, err)

	out1, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:           "idem-put",
		SecretString:       "new-val",
		ClientRequestToken: "tok-xyz",
	})
	require.NoError(t, err)

	out2, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:           "idem-put",
		SecretString:       "new-val",
		ClientRequestToken: "tok-xyz",
	})
	require.NoError(t, err)
	assert.Equal(t, out1.VersionID, out2.VersionID, "idempotent: same token+value must return same version")
}

func TestPutSecretValue_WithAWSPENDING(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "pending-put", SecretString: "v1"},
	)
	require.NoError(t, err)

	out, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:      "pending-put",
		SecretString:  "v2",
		VersionStages: []string{"AWSPENDING"},
	})
	require.NoError(t, err)
	// Real AWS: when caller specifies only AWSPENDING, AWSCURRENT is NOT added.
	assert.NotContains(t, out.VersionStages, secretsmanager.StagingLabelCurrent)
	assert.Contains(t, out.VersionStages, "AWSPENDING")
}

func TestPutSecretValue_SecretNotFound(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:     "missing",
		SecretString: "v",
	})
	require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
}

func TestPutSecretValue_DeletedSecret(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "del-put", SecretString: "v"},
	)
	require.NoError(t, err)
	_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "del-put"})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:     "del-put",
		SecretString: "v2",
	})
	require.ErrorIs(t, err, secretsmanager.ErrSecretDeleted)
}

func TestPutSecretValue_SizeLimit(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "size-put", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:     "size-put",
		SecretString: strings.Repeat("x", 65537),
	})
	require.ErrorIs(t, err, secretsmanager.ErrSecretValueTooLarge)
}

// ---------------------------------------------------------------------------
// Version staging state machine
// ---------------------------------------------------------------------------

func TestPutSecretValue_VersionStagingFullCycle(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	ctx := context.Background()

	// v1 → AWSCURRENT
	_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         "stage-cycle",
		SecretString: "v1",
	})
	require.NoError(t, err)

	v1, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretID: "stage-cycle"})
	require.NoError(t, err)
	v1ID := v1.VersionID

	// v2 → AWSCURRENT; v1 → AWSPREVIOUS
	pv2, err := b.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretID:     "stage-cycle",
		SecretString: "v2",
	})
	require.NoError(t, err)
	v2ID := pv2.VersionID

	cur, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretID: "stage-cycle"})
	require.NoError(t, err)
	assert.Equal(t, "v2", cur.SecretString, "AWSCURRENT must be v2")
	assert.Equal(t, v2ID, cur.VersionID)

	prev, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretID:     "stage-cycle",
		VersionStage: "AWSPREVIOUS",
	})
	require.NoError(t, err)
	assert.Equal(t, "v1", prev.SecretString, "AWSPREVIOUS must be v1")
	assert.Equal(t, v1ID, prev.VersionID)

	// v3 → AWSCURRENT; v2 → AWSPREVIOUS; v1 → deprecated (no staging label)
	pv3, err := b.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretID:     "stage-cycle",
		SecretString: "v3",
	})
	require.NoError(t, err)
	v3ID := pv3.VersionID

	cur3, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretID: "stage-cycle"})
	require.NoError(t, err)
	assert.Equal(t, "v3", cur3.SecretString)
	assert.Equal(t, v3ID, cur3.VersionID)

	prev3, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretID:     "stage-cycle",
		VersionStage: "AWSPREVIOUS",
	})
	require.NoError(t, err)
	assert.Equal(t, "v2", prev3.SecretString, "AWSPREVIOUS must now be v2")
	assert.Equal(t, v2ID, prev3.VersionID)

	// v1 must no longer carry AWSPREVIOUS — verify via ListSecretVersionIds.
	versions, err := b.ListSecretVersionIDs(ctx, &secretsmanager.ListSecretVersionIDsInput{
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

func TestPutSecretValue_AWSPENDINGDoesNotMoveCurrent(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         "pending-test",
		SecretString: "v1",
	})
	require.NoError(t, err)

	// Assign AWSPENDING to a new version without displacing AWSCURRENT.
	p, err := b.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretID:      "pending-test",
		SecretString:  "v2-pending",
		VersionStages: []string{"AWSPENDING"},
	})
	require.NoError(t, err)
	pendingID := p.VersionID

	// AWSCURRENT must still return v1.
	cur, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretID: "pending-test"})
	require.NoError(t, err)
	assert.Equal(
		t,
		"v1",
		cur.SecretString,
		"AWSCURRENT must not change when only AWSPENDING assigned",
	)

	// AWSPENDING must return the new version.
	pend, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretID:     "pending-test",
		VersionStage: "AWSPENDING",
	})
	require.NoError(t, err)
	assert.Equal(t, pendingID, pend.VersionID)
	assert.Equal(t, "v2-pending", pend.SecretString)
}

// TestPutSecretValue_BinaryValue verifies that a binary value can be written over an
// existing string-valued secret and round-trips correctly.
func TestPutSecretValue_BinaryValue(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         "bin-put",
		SecretString: "initial-string",
	})
	require.NoError(t, err)

	payload := []byte{0x01, 0x02, 0x03}
	_, err = b.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretID:     "bin-put",
		SecretBinary: payload,
	})
	require.NoError(t, err)

	out, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretID: "bin-put"})
	require.NoError(t, err)
	assert.Equal(t, payload, out.SecretBinary)
	assert.Empty(t, out.SecretString)
}

// ---------------------------------------------------------------------------
// PutSecretValue — empty VersionStages always applies AWSCURRENT (issue #4)
// ---------------------------------------------------------------------------

// TestPutSecretValue_NilVersionStagesAppliesAWSCURRENT verifies that omitting VersionStages
// in PutSecretValue always attaches AWSCURRENT to the new version and promotes the old
// AWSCURRENT to AWSPREVIOUS.
func TestPutSecretValue_NilVersionStagesAppliesAWSCURRENT(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "empty-stages",
		SecretString: "v1",
	})
	require.NoError(t, err)

	// Capture v1's version ID before the second put.
	desc1, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "empty-stages"})
	require.NoError(t, err)
	var v1ID string
	for id, labels := range desc1.VersionIDsToStages {
		if contains(labels, secretsmanager.StagingLabelCurrent) {
			v1ID = id
		}
	}
	require.NotEmpty(t, v1ID)

	// Put second value with no explicit VersionStages (nil slice).
	put2, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:     "empty-stages",
		SecretString: "v2",
	})
	require.NoError(t, err)
	assert.Contains(t, put2.VersionStages, secretsmanager.StagingLabelCurrent,
		"new version must carry AWSCURRENT when VersionStages is empty")

	// v1 must now carry AWSPREVIOUS.
	desc2, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "empty-stages"})
	require.NoError(t, err)
	assert.Contains(t, desc2.VersionIDsToStages[v1ID], secretsmanager.StagingLabelPrevious,
		"old AWSCURRENT version must be promoted to AWSPREVIOUS")
}

// ---------------------------------------------------------------------------
// Real-AWS parity cases
// ---------------------------------------------------------------------------

// TestPutSecretValue_PendingOnlyDoesNotForceAWSCURRENT verifies that when
// VersionStages contains only AWSPENDING (as during Lambda rotation's createSecret step),
// the new version does NOT get AWSCURRENT. Real AWS honors the caller's label list exactly.
func TestPutSecretValue_PendingOnlyDoesNotForceAWSCURRENT(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         "rotation-secret",
		SecretString: "initial-value",
	})
	require.NoError(t, err)

	// Lambda rotation step: createSecret — puts a new version with only AWSPENDING.
	out, err := b.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretID:      "rotation-secret",
		SecretString:  "new-value",
		VersionStages: []string{"AWSPENDING"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"AWSPENDING"}, out.VersionStages,
		"real AWS: AWSPENDING-only PutSecretValue must not add AWSCURRENT")

	// Original AWSCURRENT version must still be intact.
	current, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretID: "rotation-secret"})
	require.NoError(t, err)
	assert.Equal(t, "initial-value", current.SecretString,
		"AWSCURRENT version must not be disturbed by AWSPENDING-only PutSecretValue")
}

// TestPutSecretValue_BothSecretStringAndBinaryRejected verifies that providing
// both SecretString and SecretBinary returns InvalidParameterException (400).
// Real AWS rejects this combination.
func TestPutSecretValue_BothSecretStringAndBinaryRejected(t *testing.T) {
	t.Parallel()

	h := newSMHandler()

	// Create a secret first.
	rec := doSMRequest(t, h, "secretsmanager.CreateSecret",
		`{"Name":"both-vals-secret","SecretString":"v1"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSMRequest(t, h, "secretsmanager.PutSecretValue",
		`{"SecretId":"both-vals-secret","SecretString":"str","SecretBinary":"YmluYXJ5"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"providing both SecretString and SecretBinary must return 400; body: %s", rec.Body.String())
}

// TestPutSecretValue_RequiresSecretId verifies that PutSecretValue rejects
// requests with a missing or empty SecretId. Real AWS returns InvalidParameterException
// (400); the emulator previously returned ResourceNotFoundException.
func TestPutSecretValue_RequiresSecretId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     string
		name     string
		wantCode int
	}{
		{
			name:     "absent_secret_id_rejected",
			body:     `{"SecretString":"val"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_secret_id_rejected",
			body:     `{"SecretId":"","SecretString":"val"}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newSMHandler()
			rec := doSMRequest(t, h, "secretsmanager.PutSecretValue", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"PutSecretValue status for case %q", tt.name)
		})
	}
}

// TestPutSecretValue_Idempotency verifies that re-sending the same
// ClientRequestToken and identical content returns the existing version, via HTTP.
func TestPutSecretValue_Idempotency(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret", `{"Name":"idempotent","SecretString":"initial"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	const token = "550e8400-e29b-41d4-a716-446655440000"
	const putBody = `{"SecretId":"idempotent","SecretString":"new-value","ClientRequestToken":"` + token + `"}`

	// First put: creates a new version.
	rec = doR1Request(t, h, "secretsmanager.PutSecretValue", putBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var out1 secretsmanager.PutSecretValueOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out1))
	assert.Equal(t, token, out1.VersionID)

	// Second put with same token + content: should return the existing version.
	rec = doR1Request(t, h, "secretsmanager.PutSecretValue", putBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var out2 secretsmanager.PutSecretValueOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out2))
	assert.Equal(t, token, out2.VersionID)
	assert.Equal(t, out1.VersionID, out2.VersionID)
}

// ---------------------------------------------------------------------------
// Backend scenarios
// ---------------------------------------------------------------------------

// TestPutSecretValue_BackendScenarios verifies adding new versions directly via the backend.
func TestPutSecretValue_BackendScenarios(t *testing.T) {
	t.Parallel()

	t.Run("NewVersion", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
			Name:         "versioned-secret",
			SecretString: "v1",
		})

		out, err := backend.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
			SecretID:     "versioned-secret",
			SecretString: "v2",
		})

		require.NoError(t, err)
		assert.NotEmpty(t, out.VersionID)
		assert.Contains(t, out.VersionStages, secretsmanager.StagingLabelCurrent)

		// New current value
		curr, _ := backend.GetSecretValue(
			context.Background(),
			&secretsmanager.GetSecretValueInput{SecretID: "versioned-secret"},
		)
		assert.Equal(t, "v2", curr.SecretString)

		// Previous value accessible via AWSPREVIOUS
		prev, prevErr := backend.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
			SecretID:     "versioned-secret",
			VersionStage: secretsmanager.StagingLabelPrevious,
		})
		require.NoError(t, prevErr)
		assert.Equal(t, "v1", prev.SecretString)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()

		_, err := backend.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
			SecretID:     "missing",
			SecretString: "value",
		})
		require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
	})
}

// TestPutSecretValue_VersionStages verifies custom VersionStages are attached to new versions.
func TestPutSecretValue_VersionStages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantStages    []string
		name          string
		versionStages []string
	}{
		{
			name:          "default_awscurrent",
			versionStages: nil,
			wantStages:    []string{"AWSCURRENT"},
		},
		{
			// Real AWS: caller specifies only AWSPENDING — AWSCURRENT is NOT forced.
			name:          "awspending_added",
			versionStages: []string{"AWSPENDING"},
			wantStages:    []string{"AWSPENDING"},
		},
		{
			name:          "duplicate_awscurrent_deduped",
			versionStages: []string{"AWSCURRENT", "AWSPENDING"},
			wantStages:    []string{"AWSCURRENT", "AWSPENDING"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()

			_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
				Name:         "vs-test",
				SecretString: "v1",
			})
			require.NoError(t, err)

			out, err := b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
				SecretID:      "vs-test",
				SecretString:  "v2",
				VersionStages: tt.versionStages,
			})
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.wantStages, out.VersionStages)
		})
	}
}
