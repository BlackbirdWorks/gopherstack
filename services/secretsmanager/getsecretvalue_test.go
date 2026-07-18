package secretsmanager_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// GetSecretValue comprehensive
// ---------------------------------------------------------------------------

func TestGetSecretValue_NotFound(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{SecretID: "missing"})
	require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
}

func TestGetSecretValue_NotFoundHTTP(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.GetSecretValue",
		`{"SecretId":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp secretsmanager.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp.Type)
}

func TestGetSecretValue_DeletedReturnsInvalidRequest(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "to-delete", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "to-delete"})
	require.NoError(t, err)

	_, err = b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{SecretID: "to-delete"})
	require.ErrorIs(t, err, secretsmanager.ErrSecretDeleted)
}

func TestGetSecretValue_DeletedHTTPStatus(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	doR1Request(t, h, "secretsmanager.CreateSecret", `{"Name":"del-http","SecretString":"v"}`)
	doR1Request(t, h, "secretsmanager.DeleteSecret", `{"SecretId":"del-http"}`)

	rec := doR1Request(t, h, "secretsmanager.GetSecretValue", `{"SecretId":"del-http"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp secretsmanager.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidRequestException", errResp.Type)
}

func TestGetSecretValue_AWSCURRENTDefault(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "curr-default",
		SecretString:       "hello",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	out, err := b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{SecretID: "curr-default"})
	require.NoError(t, err)
	assert.Equal(t, "hello", out.SecretString)
	assert.Contains(t, out.VersionStages, secretsmanager.StagingLabelCurrent)
}

func TestGetSecretValue_AWSPREVIOUSAfterPut(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "prev-test",
		SecretString:       "v1",
		ClientRequestToken: "ver-1",
	})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:           "prev-test",
		SecretString:       "v2",
		ClientRequestToken: "ver-2",
	})
	require.NoError(t, err)

	// v1 should now be AWSPREVIOUS
	out, err := b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
		SecretID:     "prev-test",
		VersionStage: secretsmanager.StagingLabelPrevious,
	})
	require.NoError(t, err)
	assert.Equal(t, "v1", out.SecretString)
	assert.Equal(t, "ver-1", out.VersionID)
}

func TestGetSecretValue_VersionIDNotFound(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "ver-missing", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
		SecretID:  "ver-missing",
		VersionID: "nonexistent-id",
	})
	require.ErrorIs(t, err, secretsmanager.ErrVersionNotFound)
}

func TestGetSecretValue_SetsLastAccessedDate(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "access-date", SecretString: "v"},
	)
	require.NoError(t, err)

	desc1, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "access-date"})
	require.NoError(t, err)
	assert.Nil(t, desc1.LastAccessedDate, "LastAccessedDate nil before first access")

	_, err = b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{SecretID: "access-date"})
	require.NoError(t, err)

	desc2, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "access-date"})
	require.NoError(t, err)
	assert.NotNil(t, desc2.LastAccessedDate, "LastAccessedDate set after access")
}

func TestGetSecretValue_ARNLookup(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	out, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "arn-lookup", SecretString: "secret"},
	)
	require.NoError(t, err)

	val, err := b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{SecretID: out.ARN})
	require.NoError(t, err)
	assert.Equal(t, "secret", val.SecretString)
}

// ---------------------------------------------------------------------------
// Issue #2 — GetSecretValue VersionId/VersionStage mismatch
// ---------------------------------------------------------------------------

// TestGetSecretValue_VersionMismatchReturnsResourceNotFound verifies that supplying both
// VersionId and a VersionStage that the version does not carry returns
// ResourceNotFoundException (HTTP 400 with the right error type), not
// InvalidParameterException.
func TestGetSecretValue_VersionMismatchReturnsResourceNotFound(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "mismatch-error-type",
		SecretString:       "value",
		ClientRequestToken: "ver-001",
	})
	require.NoError(t, err)

	// ver-001 carries AWSCURRENT — requesting AWSPREVIOUS must return ErrVersionNotFound.
	_, err = b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
		SecretID:     "mismatch-error-type",
		VersionID:    "ver-001",
		VersionStage: secretsmanager.StagingLabelPrevious,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, secretsmanager.ErrVersionNotFound,
		"mismatch must return ErrVersionNotFound (ResourceNotFoundException), not ErrInvalidParameter")
}

// TestGetSecretValue_VersionMismatchHTTPErrorType verifies the HTTP response body carries
// ResourceNotFoundException rather than InvalidParameterException.
func TestGetSecretValue_VersionMismatchHTTPErrorType(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"mismatch-http","SecretString":"v","ClientRequestToken":"v1"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	body, _ := json.Marshal(map[string]any{
		"SecretId":     "mismatch-http",
		"VersionId":    "v1",
		"VersionStage": secretsmanager.StagingLabelPrevious,
	})
	rec = doR1Request(t, h, "secretsmanager.GetSecretValue", string(body))
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp secretsmanager.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp.Type,
		"HTTP error body must carry ResourceNotFoundException")
}

// TestGetSecretValue_VersionIDAndStageBothSuppliedAndMatch verifies that when both
// VersionId and VersionStage match the same version the call succeeds.
func TestGetSecretValue_VersionIDAndStageBothSuppliedAndMatch(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "match-both",
		SecretString:       "value",
		ClientRequestToken: "ver-aaa",
	})
	require.NoError(t, err)

	out, err := b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
		SecretID:     "match-both",
		VersionID:    "ver-aaa",
		VersionStage: secretsmanager.StagingLabelCurrent,
	})
	require.NoError(t, err)
	assert.Equal(t, "ver-aaa", out.VersionID)
}

// TestGetSecretValue_ByVersionIDOnlySucceeds verifies that supplying only VersionId (no
// stage) works correctly.
func TestGetSecretValue_ByVersionIDOnlySucceeds(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "ver-only",
		SecretString:       "secret",
		ClientRequestToken: "tok-123",
	})
	require.NoError(t, err)

	out, err := b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
		SecretID:  "ver-only",
		VersionID: "tok-123",
	})
	require.NoError(t, err)
	assert.Equal(t, "tok-123", out.VersionID)
	assert.Equal(t, "secret", out.SecretString)
}

// TestGetSecretValue_ByVersionStageOnlySucceeds verifies that supplying only VersionStage
// works.
func TestGetSecretValue_ByVersionStageOnlySucceeds(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "stage-only",
		SecretString: "value",
	})
	require.NoError(t, err)

	out, err := b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
		SecretID:     "stage-only",
		VersionStage: secretsmanager.StagingLabelCurrent,
	})
	require.NoError(t, err)
	assert.Contains(t, out.VersionStages, secretsmanager.StagingLabelCurrent)
}

// TestGetSecretValue_VersionMismatchAfterRotation verifies the mismatch check works after
// rotation (when AWSPREVIOUS exists on a different version).
func TestGetSecretValue_VersionMismatchAfterRotation(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "rot-mismatch",
		SecretString:       "v1",
		ClientRequestToken: "v1-id",
	})
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{SecretID: "rot-mismatch"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "rot-mismatch"})
	require.NoError(t, err)

	// Find the AWSCURRENT version and try to get it with AWSPREVIOUS — must fail.
	var currentID string
	for id, labels := range desc.VersionIDsToStages {
		for _, l := range labels {
			if l == secretsmanager.StagingLabelCurrent {
				currentID = id
			}
		}
	}
	require.NotEmpty(t, currentID)

	_, err = b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
		SecretID:     "rot-mismatch",
		VersionID:    currentID,
		VersionStage: secretsmanager.StagingLabelPrevious,
	})
	require.ErrorIs(t, err, secretsmanager.ErrVersionNotFound)
}

// ---------------------------------------------------------------------------
// LastAccessedDate
// ---------------------------------------------------------------------------

// TestGetSecretValue_LastAccessedDateTracking verifies GetSecretValue updates LastAccessedDate,
// as observed via a subsequent DescribeSecret over HTTP.
func TestGetSecretValue_LastAccessedDateTracking(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	// Create secret.
	rec := doR1Request(t, h, "secretsmanager.CreateSecret", `{"Name":"access-tracked","SecretString":"data"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeSecret before access should have no LastAccessedDate.
	rec = doR1Request(t, h, "secretsmanager.DescribeSecret", `{"SecretId":"access-tracked"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var desc1 secretsmanager.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc1))
	assert.Nil(t, desc1.LastAccessedDate)

	// GetSecretValue should update LastAccessedDate.
	rec = doR1Request(t, h, "secretsmanager.GetSecretValue", `{"SecretId":"access-tracked"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeSecret after access should have LastAccessedDate set to today.
	rec = doR1Request(t, h, "secretsmanager.DescribeSecret", `{"SecretId":"access-tracked"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var desc2 secretsmanager.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc2))
	require.NotNil(t, desc2.LastAccessedDate)
	// The LastAccessedDate should be today (day-granularity as AWS does).
	accessTime := time.Unix(int64(*desc2.LastAccessedDate), 0).UTC()
	today := time.Now().UTC().Truncate(24 * time.Hour)
	assert.Equal(t, today.Day(), accessTime.Day())
	assert.Equal(t, today.Month(), accessTime.Month())
}

// TestGetSecretValue_LastAccessedDateReturned verifies that GetSecretValue returns
// LastAccessedDate directly in its response (not merely via a follow-up DescribeSecret).
// Real AWS includes this field.
func TestGetSecretValue_LastAccessedDateReturned(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         "access-date-secret",
		SecretString: "v",
	})
	require.NoError(t, err)

	out, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretID: "access-date-secret"})
	require.NoError(t, err)
	require.NotNil(t, out.LastAccessedDate,
		"real AWS: GetSecretValue must return LastAccessedDate after first access")
	assert.Greater(t, *out.LastAccessedDate, float64(0))
}

// ---------------------------------------------------------------------------
// Required-parameter validation
// ---------------------------------------------------------------------------

// TestGetSecretValue_RequiresSecretId verifies that GetSecretValue rejects
// requests with a missing or empty SecretId. Real AWS returns InvalidParameterException
// (400) for this case; the emulator previously returned ResourceNotFoundException
// because an empty SecretId resolved to a name lookup that found nothing.
func TestGetSecretValue_RequiresSecretId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     string
		name     string
		wantCode int
	}{
		{
			name:     "absent_secret_id_rejected",
			body:     `{}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_secret_id_rejected",
			body:     `{"SecretId":""}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newSMHandler()
			rec := doSMRequest(t, h, "secretsmanager.GetSecretValue", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"GetSecretValue status for case %q", tt.name)
		})
	}
}

// ---------------------------------------------------------------------------
// Backend scenarios
// ---------------------------------------------------------------------------

// TestGetSecretValue_BackendScenarios verifies getting a secret value directly via the backend.
func TestGetSecretValue_BackendScenarios(t *testing.T) {
	t.Parallel()

	t.Run("CurrentVersion", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
			Name:         "db-password",
			SecretString: "secretpassword",
		})

		out, err := backend.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
			SecretID: "db-password",
		})

		require.NoError(t, err)
		assert.Equal(t, "secretpassword", out.SecretString)
		assert.Contains(t, out.VersionStages, secretsmanager.StagingLabelCurrent)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()

		_, err := backend.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{SecretID: "missing"})
		require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
	})

	t.Run("DeletedSecretFails", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
			Name:         "deleted-secret",
			SecretString: "value",
		})
		_, _ = backend.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "deleted-secret"})

		_, err := backend.GetSecretValue(
			context.Background(),
			&secretsmanager.GetSecretValueInput{SecretID: "deleted-secret"},
		)
		require.ErrorIs(t, err, secretsmanager.ErrSecretDeleted)
	})
}
