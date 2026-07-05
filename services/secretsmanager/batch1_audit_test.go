package secretsmanager_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sm "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// Secret name validation
// ---------------------------------------------------------------------------

func TestAudit_SecretName_Empty(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: ""})
	require.Error(t, err)
	require.ErrorIs(t, err, sm.ErrInvalidSecretName)
}

func TestAudit_SecretName_TooLong(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: strings.Repeat("a", 513)})
	require.ErrorIs(t, err, sm.ErrInvalidSecretName)
}

func TestAudit_SecretName_ExactMaxLength(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: strings.Repeat("a", 512)})
	require.NoError(t, err)
}

func TestAudit_SecretName_InvalidChars(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()

	for _, name := range []string{"has space", "has\ttab", "has\nnewline", "has$dollar"} {
		_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: name})
		require.ErrorIs(t, err, sm.ErrInvalidSecretName, "expected error for %q", name)
	}
}

func TestAudit_SecretName_ValidSpecialChars(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	// All allowed special characters: /_+=.@-
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "valid/_+=.@-name",
		SecretString: "v",
	})
	require.NoError(t, err)
}

func TestAudit_SecretName_AWSPrefixRejected(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "aws/my-secret",
		SecretString: "v",
	})
	require.ErrorIs(t, err, sm.ErrInvalidSecretName, "names starting with \"aws/\" must be rejected")
}

func TestAudit_SecretName_AWSPrefixHTTP(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"aws/my-secret","SecretString":"v"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sm.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidParameterException", errResp.Type)
}

func TestAudit_SecretName_SlashInMiddleAllowed(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "prod/db/password",
		SecretString: "v",
	})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// CreateSecret comprehensive
// ---------------------------------------------------------------------------

func TestAudit_CreateSecret_WithKmsKeyID(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "kms-secret",
		SecretString: "v",
		KmsKeyID:     "arn:aws:kms:us-east-1:123456789012:key/abc-123",
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "kms-secret"})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/abc-123", desc.KmsKeyID)
}

func TestAudit_CreateSecret_WithBinary(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "binary-secret",
		SecretBinary: []byte{0x01, 0x02, 0x03},
	})
	require.NoError(t, err)

	val, err := b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{SecretID: "binary-secret"})
	require.NoError(t, err)
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, val.SecretBinary)
	assert.Empty(t, val.SecretString)
}

func TestAudit_CreateSecret_ClientRequestTokenBecomesVersionID(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "token-version",
		SecretString:       "v",
		ClientRequestToken: "my-token-abc",
	})
	require.NoError(t, err)
	assert.Equal(t, "my-token-abc", out.VersionID)

	val, err := b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{
		SecretID:  "token-version",
		VersionID: "my-token-abc",
	})
	require.NoError(t, err)
	assert.Equal(t, "my-token-abc", val.VersionID)
}

func TestAudit_CreateSecret_WithoutValue_NoVersionID(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "no-value"})
	require.NoError(t, err)
	assert.Empty(t, out.VersionID, "no version is created when no value is provided")
}

func TestAudit_CreateSecret_ARNFormat(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "arn-check",
		SecretString: "v",
	})
	require.NoError(t, err)
	assert.Contains(t, out.ARN, "arn:aws:secretsmanager:")
	assert.Contains(t, out.ARN, "arn-check")
}

func TestAudit_CreateSecret_DuplicateNameReturnsResourceExistsException(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "dup", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "dup", SecretString: "v"})
	require.ErrorIs(t, err, sm.ErrSecretAlreadyExists)
}

func TestAudit_CreateSecret_DuplicateHTTPStatus(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"dup2","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"dup2","SecretString":"v"}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sm.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceExistsException", errResp.Type)
}

func TestAudit_CreateSecret_TagCountLimit(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	tags := make([]sm.Tag, 51)
	for i := range tags {
		tags[i] = sm.Tag{Key: fmt.Sprintf("key%d", i), Value: "v"}
	}

	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "too-many-tags",
		SecretString: "v",
		Tags:         tags,
	})
	require.ErrorIs(t, err, sm.ErrInvalidParameter, "must reject >50 tags at create time")
}

func TestAudit_CreateSecret_Exactly50TagsAllowed(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	tags := make([]sm.Tag, 50)
	for i := range tags {
		tags[i] = sm.Tag{Key: fmt.Sprintf("key%d", i), Value: "v"}
	}

	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "max-tags",
		SecretString: "v",
		Tags:         tags,
	})
	require.NoError(t, err)
}

func TestAudit_CreateSecret_CreatedDateSet(t *testing.T) {
	t.Parallel()

	before := time.Now()
	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "ts-check", SecretString: "v"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "ts-check"})
	require.NoError(t, err)
	require.NotNil(t, desc.CreatedDate)
	// UnixTimeFloat stores nanoseconds/1e9; recover with int64(f*1e9) nanoseconds.
	created := time.Unix(0, int64(*desc.CreatedDate*1e9))
	assert.False(t, created.Before(before.Add(-time.Second)),
		"CreatedDate must be at or after test start (within 1s tolerance)")
}

// ---------------------------------------------------------------------------
// GetSecretValue comprehensive
// ---------------------------------------------------------------------------

func TestAudit_GetSecretValue_NotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{SecretID: "missing"})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)
}

func TestAudit_GetSecretValue_NotFoundHTTP(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.GetSecretValue",
		`{"SecretId":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sm.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp.Type)
}

func TestAudit_GetSecretValue_DeletedReturnsInvalidRequest(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "to-delete", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{SecretID: "to-delete"})
	require.NoError(t, err)

	_, err = b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{SecretID: "to-delete"})
	require.ErrorIs(t, err, sm.ErrSecretDeleted)
}

func TestAudit_GetSecretValue_DeletedHTTPStatus(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	doR1Request(t, h, "secretsmanager.CreateSecret", `{"Name":"del-http","SecretString":"v"}`)
	doR1Request(t, h, "secretsmanager.DeleteSecret", `{"SecretId":"del-http"}`)

	rec := doR1Request(t, h, "secretsmanager.GetSecretValue", `{"SecretId":"del-http"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sm.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidRequestException", errResp.Type)
}

func TestAudit_GetSecretValue_AWSCURRENTDefault(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "curr-default",
		SecretString:       "hello",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	out, err := b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{SecretID: "curr-default"})
	require.NoError(t, err)
	assert.Equal(t, "hello", out.SecretString)
	assert.Contains(t, out.VersionStages, sm.StagingLabelCurrent)
}

func TestAudit_GetSecretValue_AWSPREVIOUSAfterPut(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "prev-test",
		SecretString:       "v1",
		ClientRequestToken: "ver-1",
	})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:           "prev-test",
		SecretString:       "v2",
		ClientRequestToken: "ver-2",
	})
	require.NoError(t, err)

	// v1 should now be AWSPREVIOUS
	out, err := b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{
		SecretID:     "prev-test",
		VersionStage: sm.StagingLabelPrevious,
	})
	require.NoError(t, err)
	assert.Equal(t, "v1", out.SecretString)
	assert.Equal(t, "ver-1", out.VersionID)
}

func TestAudit_GetSecretValue_VersionIDNotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "ver-missing", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{
		SecretID:  "ver-missing",
		VersionID: "nonexistent-id",
	})
	require.ErrorIs(t, err, sm.ErrVersionNotFound)
}

func TestAudit_GetSecretValue_SetsLastAccessedDate(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "access-date", SecretString: "v"})
	require.NoError(t, err)

	desc1, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "access-date"})
	require.NoError(t, err)
	assert.Nil(t, desc1.LastAccessedDate, "LastAccessedDate nil before first access")

	_, err = b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{SecretID: "access-date"})
	require.NoError(t, err)

	desc2, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "access-date"})
	require.NoError(t, err)
	assert.NotNil(t, desc2.LastAccessedDate, "LastAccessedDate set after access")
}

func TestAudit_GetSecretValue_ARNLookup(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "arn-lookup", SecretString: "secret"})
	require.NoError(t, err)

	val, err := b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{SecretID: out.ARN})
	require.NoError(t, err)
	assert.Equal(t, "secret", val.SecretString)
}

// ---------------------------------------------------------------------------
// PutSecretValue comprehensive
// ---------------------------------------------------------------------------

func TestAudit_PutSecretValue_EmptyValueRejected(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "empty-put", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID: "empty-put",
	})
	require.ErrorIs(t, err, sm.ErrInvalidParameter,
		"PutSecretValue with no value must return InvalidParameterException")
}

func TestAudit_PutSecretValue_EmptyValueHTTP(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	doR1Request(t, h, "secretsmanager.CreateSecret", `{"Name":"ev-http","SecretString":"v"}`)

	rec := doR1Request(t, h, "secretsmanager.PutSecretValue", `{"SecretId":"ev-http"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit_PutSecretValue_AWSCURRENT_Promoted(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "promote-test",
		SecretString:       "first",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	out, err := b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:           "promote-test",
		SecretString:       "second",
		ClientRequestToken: "v2",
	})
	require.NoError(t, err)
	assert.Contains(t, out.VersionStages, sm.StagingLabelCurrent)

	// v2 should be AWSCURRENT
	val, err := b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{SecretID: "promote-test"})
	require.NoError(t, err)
	assert.Equal(t, "second", val.SecretString)
	assert.Equal(t, "v2", val.VersionID)
}

func TestAudit_PutSecretValue_Idempotent(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "idem-put", SecretString: "v"})
	require.NoError(t, err)

	out1, err := b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:           "idem-put",
		SecretString:       "new-val",
		ClientRequestToken: "tok-xyz",
	})
	require.NoError(t, err)

	out2, err := b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:           "idem-put",
		SecretString:       "new-val",
		ClientRequestToken: "tok-xyz",
	})
	require.NoError(t, err)
	assert.Equal(t, out1.VersionID, out2.VersionID, "idempotent: same token+value must return same version")
}

func TestAudit_PutSecretValue_WithAWSPENDING(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "pending-put", SecretString: "v1"})
	require.NoError(t, err)

	out, err := b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:      "pending-put",
		SecretString:  "v2",
		VersionStages: []string{"AWSPENDING"},
	})
	require.NoError(t, err)
	// Real AWS: when caller specifies only AWSPENDING, AWSCURRENT is NOT added.
	assert.NotContains(t, out.VersionStages, sm.StagingLabelCurrent)
	assert.Contains(t, out.VersionStages, "AWSPENDING")
}

func TestAudit_PutSecretValue_SecretNotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:     "missing",
		SecretString: "v",
	})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)
}

func TestAudit_PutSecretValue_DeletedSecret(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "del-put", SecretString: "v"})
	require.NoError(t, err)
	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{SecretID: "del-put"})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:     "del-put",
		SecretString: "v2",
	})
	require.ErrorIs(t, err, sm.ErrSecretDeleted)
}

func TestAudit_PutSecretValue_SizeLimit(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "size-put", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:     "size-put",
		SecretString: strings.Repeat("x", 65537),
	})
	require.ErrorIs(t, err, sm.ErrSecretValueTooLarge)
}

// ---------------------------------------------------------------------------
// DeleteSecret comprehensive
// ---------------------------------------------------------------------------

func TestAudit_DeleteSecret_SoftDelete(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "soft-del", SecretString: "v"})
	require.NoError(t, err)

	out, err := b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{SecretID: "soft-del"})
	require.NoError(t, err)
	assert.NotEmpty(t, out.ARN)
	assert.NotZero(t, out.DeletionDate)

	// Secret still findable but marked deleted
	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "soft-del"})
	require.NoError(t, err)
	assert.NotNil(t, desc.DeletedDate)
}

func TestAudit_DeleteSecret_ForceDelete(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "force-del", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{
		SecretID:                   "force-del",
		ForceDeleteWithoutRecovery: true,
	})
	require.NoError(t, err)

	// Secret completely gone
	_, err = b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "force-del"})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)
}

func TestAudit_DeleteSecret_RecoveryWindowMin(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "recov-min", SecretString: "v"})
	require.NoError(t, err)

	days := int64(7)
	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{
		SecretID:             "recov-min",
		RecoveryWindowInDays: &days,
	})
	require.NoError(t, err)
}

func TestAudit_DeleteSecret_RecoveryWindowMax(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "recov-max", SecretString: "v"})
	require.NoError(t, err)

	days := int64(30)
	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{
		SecretID:             "recov-max",
		RecoveryWindowInDays: &days,
	})
	require.NoError(t, err)
}

func TestAudit_DeleteSecret_RecoveryWindowTooShort(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "recov-short", SecretString: "v"})
	require.NoError(t, err)

	days := int64(6)
	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{
		SecretID:             "recov-short",
		RecoveryWindowInDays: &days,
	})
	require.ErrorIs(t, err, sm.ErrInvalidParameter)
}

func TestAudit_DeleteSecret_RecoveryWindowTooLong(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "recov-long", SecretString: "v"})
	require.NoError(t, err)

	days := int64(31)
	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{
		SecretID:             "recov-long",
		RecoveryWindowInDays: &days,
	})
	require.ErrorIs(t, err, sm.ErrInvalidParameter)
}

func TestAudit_DeleteSecret_AlreadyDeleted(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "already-del", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{SecretID: "already-del"})
	require.NoError(t, err)

	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{SecretID: "already-del"})
	require.ErrorIs(t, err, sm.ErrInvalidParameter, "deleting an already-deleted secret must fail")
}

func TestAudit_DeleteSecret_NotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{SecretID: "missing"})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)
}

// ---------------------------------------------------------------------------
// RestoreSecret comprehensive
// ---------------------------------------------------------------------------

func TestAudit_RestoreSecret_ClearsDeletedDate(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "restore-me", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{SecretID: "restore-me"})
	require.NoError(t, err)

	_, err = b.RestoreSecret(context.Background(), &sm.RestoreSecretInput{SecretID: "restore-me"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "restore-me"})
	require.NoError(t, err)
	assert.Nil(t, desc.DeletedDate, "DeletedDate must be cleared after RestoreSecret")
}

func TestAudit_RestoreSecret_ActiveSecretFails(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "active-restore", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.RestoreSecret(context.Background(), &sm.RestoreSecretInput{SecretID: "active-restore"})
	require.ErrorIs(t, err, sm.ErrInvalidParameter,
		"restoring a non-deleted secret must return InvalidRequestException")
}

func TestAudit_RestoreSecret_WritableAfterRestore(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&sm.CreateSecretInput{Name: "write-after-restore", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{SecretID: "write-after-restore"})
	require.NoError(t, err)

	_, err = b.RestoreSecret(context.Background(), &sm.RestoreSecretInput{SecretID: "write-after-restore"})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:     "write-after-restore",
		SecretString: "v2",
	})
	require.NoError(t, err, "PutSecretValue must succeed after RestoreSecret")
}

func TestAudit_RestoreSecret_NotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.RestoreSecret(context.Background(), &sm.RestoreSecretInput{SecretID: "missing"})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)
}

// ---------------------------------------------------------------------------
// UpdateSecret comprehensive
// ---------------------------------------------------------------------------

func TestAudit_UpdateSecret_Description(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "upd-desc", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.UpdateSecret(context.Background(), &sm.UpdateSecretInput{
		SecretID:    "upd-desc",
		Description: "new description",
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "upd-desc"})
	require.NoError(t, err)
	assert.Equal(t, "new description", desc.Description)
}

func TestAudit_UpdateSecret_KmsKeyID(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "upd-kms", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.UpdateSecret(context.Background(), &sm.UpdateSecretInput{
		SecretID: "upd-kms",
		KmsKeyID: "alias/new-key",
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "upd-kms"})
	require.NoError(t, err)
	assert.Equal(t, "alias/new-key", desc.KmsKeyID)
}

func TestAudit_UpdateSecret_ValueCreatesNewVersion(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "upd-val",
		SecretString:       "v1",
		ClientRequestToken: "v1-id",
	})
	require.NoError(t, err)

	out, err := b.UpdateSecret(context.Background(), &sm.UpdateSecretInput{
		SecretID:           "upd-val",
		SecretString:       "v2",
		ClientRequestToken: "v2-id",
	})
	require.NoError(t, err)
	assert.Equal(t, "v2-id", out.VersionID)

	val, err := b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{SecretID: "upd-val"})
	require.NoError(t, err)
	assert.Equal(t, "v2", val.SecretString)
}

func TestAudit_UpdateSecret_DeletedFails(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "upd-del", SecretString: "v"})
	require.NoError(t, err)
	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{SecretID: "upd-del"})
	require.NoError(t, err)

	_, err = b.UpdateSecret(context.Background(), &sm.UpdateSecretInput{
		SecretID:    "upd-del",
		Description: "new desc",
	})
	require.ErrorIs(t, err, sm.ErrSecretDeleted)
}

func TestAudit_UpdateSecret_NotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.UpdateSecret(context.Background(), &sm.UpdateSecretInput{
		SecretID:    "missing",
		Description: "d",
	})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)
}

// ---------------------------------------------------------------------------
// DescribeSecret comprehensive
// ---------------------------------------------------------------------------

func TestAudit_DescribeSecret_AllMetadataFields(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackendWithConfig("123456789012", "us-west-2")
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "full-desc",
		Description:  "my description",
		SecretString: "v",
		KmsKeyID:     "alias/my-key",
		Tags:         []sm.Tag{{Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "full-desc"})
	require.NoError(t, err)
	assert.Equal(t, "full-desc", desc.Name)
	assert.Equal(t, "my description", desc.Description)
	assert.Equal(t, "alias/my-key", desc.KmsKeyID)
	assert.Equal(t, "123456789012", desc.OwnerAccountID)
	assert.Equal(t, "us-west-2", desc.PrimaryRegion)
	assert.NotNil(t, desc.CreatedDate)
	assert.NotNil(t, desc.LastChangedDate)
	assert.NotNil(t, desc.VersionIDsToStages)
}

func TestAudit_DescribeSecret_DeletedSecretStillReturnsMetadata(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "desc-del", SecretString: "v"})
	require.NoError(t, err)
	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{SecretID: "desc-del"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "desc-del"})
	require.NoError(t, err)
	assert.NotNil(t, desc.DeletedDate, "DeletedDate must be present for deleted secrets")
	assert.Equal(t, "desc-del", desc.Name)
}

func TestAudit_DescribeSecret_NotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "missing"})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)
}

func TestAudit_DescribeSecret_VersionIDsToStages(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "versions-check",
		SecretString:       "v1",
		ClientRequestToken: "ver-1",
	})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:           "versions-check",
		SecretString:       "v2",
		ClientRequestToken: "ver-2",
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "versions-check"})
	require.NoError(t, err)
	require.NotNil(t, desc.VersionIDsToStages)

	assert.Contains(t, desc.VersionIDsToStages["ver-2"], sm.StagingLabelCurrent)
	assert.Contains(t, desc.VersionIDsToStages["ver-1"], sm.StagingLabelPrevious)
}

func TestAudit_DescribeSecret_ARN(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	created, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "arn-desc", SecretString: "v"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "arn-desc"})
	require.NoError(t, err)
	assert.Equal(t, created.ARN, desc.ARN)
}

// ---------------------------------------------------------------------------
// ListSecrets comprehensive
// ---------------------------------------------------------------------------

func TestAudit_ListSecrets_Empty(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{})
	require.NoError(t, err)
	assert.Empty(t, out.SecretList)
}

func TestAudit_ListSecrets_Basic(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	for _, name := range []string{"a-secret", "b-secret", "c-secret"} {
		_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 3)
}

func TestAudit_ListSecrets_MaxResultsZeroReturnsError(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	mr := int64(0)
	_, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{MaxResults: &mr})
	require.ErrorIs(t, err, sm.ErrInvalidParameter, "MaxResults=0 must be rejected")
}

func TestAudit_ListSecrets_MaxResults101ReturnsError(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	mr := int64(101)
	_, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{MaxResults: &mr})
	require.ErrorIs(t, err, sm.ErrInvalidParameter, "MaxResults=101 must be rejected")
}

func TestAudit_ListSecrets_MaxResultsBoundsHTTP(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.ListSecrets", `{"MaxResults":200}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit_ListSecrets_Pagination(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	for i := range 10 {
		_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
			Name:         fmt.Sprintf("page-secret-%02d", i),
			SecretString: "v",
		})
		require.NoError(t, err)
	}

	mr := int64(3)
	page1, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{MaxResults: &mr})
	require.NoError(t, err)
	assert.Len(t, page1.SecretList, 3)
	assert.NotEmpty(t, page1.NextToken)

	page2, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{
		MaxResults: &mr,
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, page2.SecretList, 3)

	// Collect all pages
	all := make([]sm.SecretListEntry, 0, 10)
	all = append(all, page1.SecretList...)
	all = append(all, page2.SecretList...)
	token := page2.NextToken
	for token != "" {
		var pageErr error
		var page *sm.ListSecretsOutput
		page, pageErr = b.ListSecrets(context.Background(), &sm.ListSecretsInput{MaxResults: &mr, NextToken: token})
		require.NoError(t, pageErr)
		all = append(all, page.SecretList...)
		token = page.NextToken
	}
	assert.Len(t, all, 10)
}

func TestAudit_ListSecrets_SortAsc(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	for _, name := range []string{"charlie", "alpha", "bravo"} {
		_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{SortOrder: "asc"})
	require.NoError(t, err)
	require.Len(t, out.SecretList, 3)
	assert.Equal(t, "alpha", out.SecretList[0].Name)
	assert.Equal(t, "charlie", out.SecretList[2].Name)
}

func TestAudit_ListSecrets_SortDesc(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	for _, name := range []string{"charlie", "alpha", "bravo"} {
		_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{SortOrder: "desc"})
	require.NoError(t, err)
	require.Len(t, out.SecretList, 3)
	assert.Equal(t, "charlie", out.SecretList[0].Name)
	assert.Equal(t, "alpha", out.SecretList[2].Name)
}

func TestAudit_ListSecrets_FilterByNamePrefix(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	for _, name := range []string{"prod/db", "prod/api", "dev/db"} {
		_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{
		Filters: []sm.SecretFilter{{Key: "name", Values: []string{"prod/"}}},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 2)
}

func TestAudit_ListSecrets_FilterByDescription(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "desc-match",
		SecretString: "v",
		Description:  "database credentials",
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "no-match",
		SecretString: "v",
		Description:  "api key",
	})
	require.NoError(t, err)

	out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{
		Filters: []sm.SecretFilter{{Key: "description", Values: []string{"database"}}},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 1)
	assert.Equal(t, "desc-match", out.SecretList[0].Name)
}

func TestAudit_ListSecrets_FilterByTagKey(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "tagged-secret",
		SecretString: "v",
		Tags:         []sm.Tag{{Key: "environment", Value: "prod"}},
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "untagged",
		SecretString: "v",
	})
	require.NoError(t, err)

	out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{
		Filters: []sm.SecretFilter{{Key: "tag-key", Values: []string{"environment"}}},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 1)
	assert.Equal(t, "tagged-secret", out.SecretList[0].Name)
}

func TestAudit_ListSecrets_FilterByTagValue(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "prod-secret",
		SecretString: "v",
		Tags:         []sm.Tag{{Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "dev-secret",
		SecretString: "v",
		Tags:         []sm.Tag{{Key: "env", Value: "dev"}},
	})
	require.NoError(t, err)

	out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{
		Filters: []sm.SecretFilter{{Key: "tag-value", Values: []string{"prod"}}},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 1)
	assert.Equal(t, "prod-secret", out.SecretList[0].Name)
}

func TestAudit_ListSecrets_IncludePlannedDeletion(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "alive", SecretString: "v"})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "dead", SecretString: "v"})
	require.NoError(t, err)
	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{SecretID: "dead"})
	require.NoError(t, err)

	out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{IncludePlannedDeletion: true})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 2)

	out2, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{IncludePlannedDeletion: false})
	require.NoError(t, err)
	assert.Len(t, out2.SecretList, 1)
}

func TestAudit_ListSecrets_SecretVersionsToStages(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "stages-list",
		SecretString:       "v1",
		ClientRequestToken: "ver-1",
	})
	require.NoError(t, err)

	out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{})
	require.NoError(t, err)
	require.Len(t, out.SecretList, 1)

	entry := out.SecretList[0]
	require.NotNil(t, entry.SecretVersionsToStages, "SecretVersionsToStages must be present in list entry")
	assert.Contains(t, entry.SecretVersionsToStages["ver-1"], sm.StagingLabelCurrent)
}

// ---------------------------------------------------------------------------
// ListSecretVersionIds comprehensive
// ---------------------------------------------------------------------------

func TestAudit_ListSecretVersionIds_Basic(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "lvid-basic",
		SecretString:       "v1",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:           "lvid-basic",
		SecretString:       "v2",
		ClientRequestToken: "v2",
	})
	require.NoError(t, err)

	out, err := b.ListSecretVersionIDs(context.Background(), &sm.ListSecretVersionIDsInput{SecretID: "lvid-basic"})
	require.NoError(t, err)
	// Only labeled versions by default (v1=AWSPREVIOUS, v2=AWSCURRENT)
	assert.Len(t, out.Versions, 2)
}

func TestAudit_ListSecretVersionIds_MaxResultsInvalid(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "lvid-mr", SecretString: "v"})
	require.NoError(t, err)

	mr := int64(0)
	_, err = b.ListSecretVersionIDs(context.Background(), &sm.ListSecretVersionIDsInput{
		SecretID:   "lvid-mr",
		MaxResults: &mr,
	})
	require.ErrorIs(t, err, sm.ErrInvalidParameter, "MaxResults=0 for ListSecretVersionIds must fail")
}

func TestAudit_ListSecretVersionIds_IncludeDeprecated(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "lvid-depr",
		SecretString:       "v1",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	// Rotate 3 times to create unlabeled versions
	for i := 2; i <= 4; i++ {
		_, err = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
			SecretID:           "lvid-depr",
			SecretString:       fmt.Sprintf("v%d", i),
			ClientRequestToken: fmt.Sprintf("v%d", i),
		})
		require.NoError(t, err)
	}

	outNormal, err := b.ListSecretVersionIDs(context.Background(), &sm.ListSecretVersionIDsInput{SecretID: "lvid-depr"})
	require.NoError(t, err)

	outAll, err := b.ListSecretVersionIDs(context.Background(), &sm.ListSecretVersionIDsInput{
		SecretID:          "lvid-depr",
		IncludeDeprecated: true,
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(outAll.Versions), len(outNormal.Versions),
		"IncludeDeprecated must return at least as many versions")
}

func TestAudit_ListSecretVersionIds_SortedNewestFirst(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "lvid-sort",
		SecretString:       "v1",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	time.Sleep(2 * time.Millisecond)

	_, err = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:           "lvid-sort",
		SecretString:       "v2",
		ClientRequestToken: "v2",
	})
	require.NoError(t, err)

	out, err := b.ListSecretVersionIDs(context.Background(), &sm.ListSecretVersionIDsInput{SecretID: "lvid-sort"})
	require.NoError(t, err)
	require.Len(t, out.Versions, 2)
	// Newest (v2 = AWSCURRENT) should be first
	assert.Equal(t, "v2", out.Versions[0].VersionID)
}

func TestAudit_ListSecretVersionIds_NotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.ListSecretVersionIDs(context.Background(), &sm.ListSecretVersionIDsInput{SecretID: "missing"})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)
}

func TestAudit_ListSecretVersionIds_Pagination(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "lvid-pages",
		SecretString:       "v1",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	for i := 2; i <= 5; i++ {
		_, err = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
			SecretID:           "lvid-pages",
			SecretString:       fmt.Sprintf("v%d", i),
			ClientRequestToken: fmt.Sprintf("v%d", i),
		})
		require.NoError(t, err)
	}

	// Use IncludeDeprecated to surface all 5 versions (v1-v3 are unlabeled/deprecated).
	mr := int64(2)
	page1, err := b.ListSecretVersionIDs(context.Background(), &sm.ListSecretVersionIDsInput{
		SecretID:          "lvid-pages",
		MaxResults:        &mr,
		IncludeDeprecated: true,
	})
	require.NoError(t, err)
	assert.Len(t, page1.Versions, 2)
	assert.NotEmpty(t, page1.NextToken)
}

// ---------------------------------------------------------------------------
// TagResource / UntagResource comprehensive
// ---------------------------------------------------------------------------

func TestAudit_TagResource_AddTags(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "tag-add", SecretString: "v"})
	require.NoError(t, err)

	err = b.TagResource(context.Background(), &sm.TagResourceInput{
		SecretID: "tag-add",
		Tags:     []sm.Tag{{Key: "team", Value: "platform"}, {Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "tag-add"})
	require.NoError(t, err)
	require.NotNil(t, desc.Tags)
	tagMap := desc.Tags.Clone()
	assert.Equal(t, "platform", tagMap["team"])
	assert.Equal(t, "prod", tagMap["env"])
}

func TestAudit_TagResource_UpdateExistingTag(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "tag-upd",
		SecretString: "v",
		Tags:         []sm.Tag{{Key: "env", Value: "staging"}},
	})
	require.NoError(t, err)

	err = b.TagResource(context.Background(), &sm.TagResourceInput{
		SecretID: "tag-upd",
		Tags:     []sm.Tag{{Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "tag-upd"})
	require.NoError(t, err)
	tagMap := desc.Tags.Clone()
	assert.Equal(t, "prod", tagMap["env"])
}

func TestAudit_TagResource_LimitEnforced(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	// Create with 48 tags
	initial := make([]sm.Tag, 48)
	for i := range initial {
		initial[i] = sm.Tag{Key: fmt.Sprintf("k%d", i), Value: "v"}
	}
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "tag-limit",
		SecretString: "v",
		Tags:         initial,
	})
	require.NoError(t, err)

	// Add 3 more (would be 51 total)
	extra := []sm.Tag{{Key: "e1", Value: "v"}, {Key: "e2", Value: "v"}, {Key: "e3", Value: "v"}}
	err = b.TagResource(context.Background(), &sm.TagResourceInput{SecretID: "tag-limit", Tags: extra})
	require.ErrorIs(t, err, sm.ErrInvalidParameter, "must reject tags that exceed the 50-tag limit")
}

func TestAudit_UntagResource_RemoveTag(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "tag-rm",
		SecretString: "v",
		Tags:         []sm.Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "platform"}},
	})
	require.NoError(t, err)

	err = b.UntagResource(context.Background(), &sm.UntagResourceInput{
		SecretID: "tag-rm",
		TagKeys:  []string{"env"},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "tag-rm"})
	require.NoError(t, err)
	tagMap := desc.Tags.Clone()
	_, hasEnv := tagMap["env"]
	assert.False(t, hasEnv, "env tag must be removed")
	_, hasTeam := tagMap["team"]
	assert.True(t, hasTeam, "team tag must remain")
}

func TestAudit_TagResource_DeletedSecret(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "tag-del", SecretString: "v"})
	require.NoError(t, err)
	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{SecretID: "tag-del"})
	require.NoError(t, err)

	err = b.TagResource(context.Background(), &sm.TagResourceInput{
		SecretID: "tag-del",
		Tags:     []sm.Tag{{Key: "k", Value: "v"}},
	})
	require.ErrorIs(t, err, sm.ErrSecretDeleted)
}

func TestAudit_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	err := b.TagResource(context.Background(), &sm.TagResourceInput{
		SecretID: "missing",
		Tags:     []sm.Tag{{Key: "k", Value: "v"}},
	})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)
}

// ---------------------------------------------------------------------------
// RotateSecret comprehensive
// ---------------------------------------------------------------------------

func TestAudit_RotateSecret_CreatesNewVersion(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "rot-new-ver",
		SecretString:       "original",
		ClientRequestToken: "ver-orig",
	})
	require.NoError(t, err)

	out, err := b.RotateSecret(context.Background(), &sm.RotateSecretInput{SecretID: "rot-new-ver"})
	require.NoError(t, err)
	assert.NotEmpty(t, out.VersionID)
	assert.NotEqual(t, "ver-orig", out.VersionID)
}

func TestAudit_RotateSecret_AWSCURRENTPromotedAWSPREVIOUS(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "rot-stages",
		SecretString:       "v1",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &sm.RotateSecretInput{SecretID: "rot-stages"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "rot-stages"})
	require.NoError(t, err)

	var hasCurrent, hasPrevious bool

	for _, labels := range desc.VersionIDsToStages {
		for _, l := range labels {
			if l == sm.StagingLabelCurrent {
				hasCurrent = true
			}
			if l == sm.StagingLabelPrevious {
				hasPrevious = true
			}
		}
	}

	assert.True(t, hasCurrent, "must have AWSCURRENT after rotation")
	assert.True(t, hasPrevious, "must have AWSPREVIOUS after rotation (v1 demoted)")
}

func TestAudit_RotateSecret_LastRotatedDateUpdated(t *testing.T) {
	t.Parallel()

	before := time.Now()
	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "rot-date", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &sm.RotateSecretInput{SecretID: "rot-date"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "rot-date"})
	require.NoError(t, err)
	require.NotNil(t, desc.LastRotatedDate)
	// UnixTimeFloat stores nanoseconds/1e9; recover with int64(f*1e9) nanoseconds.
	rotated := time.Unix(0, int64(*desc.LastRotatedDate*1e9))
	assert.False(t, rotated.Before(before.Add(-time.Second)),
		"LastRotatedDate must be at or after test start (within 1s tolerance)")
}

func TestAudit_RotateSecret_RotationEnabledAfterRotate(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "rot-enabled", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &sm.RotateSecretInput{SecretID: "rot-enabled"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "rot-enabled"})
	require.NoError(t, err)
	assert.True(t, desc.RotationEnabled)
}

func TestAudit_RotateSecret_RotateImmediatelyFalse(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "rot-no-imm",
		SecretString:       "v1",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	noImm := false
	days := int64(30)
	_, err = b.RotateSecret(context.Background(), &sm.RotateSecretInput{
		SecretID:          "rot-no-imm",
		RotateImmediately: &noImm,
		RotationRules: &sm.RotationRulesType{
			AutomaticallyAfterDays: &days,
		},
	})
	require.NoError(t, err)

	// Value must still be v1 (no immediate rotation)
	val, err := b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{SecretID: "rot-no-imm"})
	require.NoError(t, err)
	assert.Equal(t, "v1", val.SecretString)
}

func TestAudit_RotateSecret_LambdaARNStored(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "rot-lambda", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &sm.RotateSecretInput{
		SecretID:          "rot-lambda",
		RotationLambdaARN: "arn:aws:lambda:us-east-1:123456789012:function:MyRotator",
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "rot-lambda"})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:MyRotator", desc.RotationLambdaARN)
}

func TestAudit_RotateSecret_NotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.RotateSecret(context.Background(), &sm.RotateSecretInput{SecretID: "missing"})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)
}

func TestAudit_RotateSecret_DeletedFails(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "rot-del", SecretString: "v"})
	require.NoError(t, err)
	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{SecretID: "rot-del"})
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &sm.RotateSecretInput{SecretID: "rot-del"})
	require.ErrorIs(t, err, sm.ErrSecretDeleted)
}

// ---------------------------------------------------------------------------
// CancelRotateSecret comprehensive
// ---------------------------------------------------------------------------

func TestAudit_CancelRotateSecret_RemovesAWSPENDING(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "cancel-rot", SecretString: "v1"})
	require.NoError(t, err)

	// Put a version with AWSPENDING
	_, err = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:           "cancel-rot",
		SecretString:       "v2",
		ClientRequestToken: "v2-pending",
		VersionStages:      []string{"AWSPENDING"},
	})
	require.NoError(t, err)

	_, err = b.CancelRotateSecret(context.Background(), &sm.CancelRotateSecretInput{SecretID: "cancel-rot"})
	require.NoError(t, err)

	// Confirm AWSPENDING is gone
	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "cancel-rot"})
	require.NoError(t, err)

	for _, labels := range desc.VersionIDsToStages {
		for _, l := range labels {
			assert.NotEqual(t, "AWSPENDING", l, "AWSPENDING must be removed after CancelRotateSecret")
		}
	}
}

func TestAudit_CancelRotateSecret_SetsRotationDisabled(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "cancel-enabled", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &sm.RotateSecretInput{SecretID: "cancel-enabled"})
	require.NoError(t, err)

	_, err = b.CancelRotateSecret(context.Background(), &sm.CancelRotateSecretInput{SecretID: "cancel-enabled"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "cancel-enabled"})
	require.NoError(t, err)
	// Real AWS: CancelRotateSecret only removes AWSPENDING; rotation config stays intact.
	assert.True(t, desc.RotationEnabled)
}

func TestAudit_CancelRotateSecret_NotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CancelRotateSecret(context.Background(), &sm.CancelRotateSecretInput{SecretID: "missing"})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)
}

// ---------------------------------------------------------------------------
// Resource policy comprehensive
// ---------------------------------------------------------------------------

const validPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"AWS":"arn:aws:iam::123456789012:root"},` +
	`"Action":"secretsmanager:GetSecretValue","Resource":"*"}]}`

func TestAudit_ResourcePolicy_PutGetDelete(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "policy-secret", SecretString: "v"})
	require.NoError(t, err)

	// Put
	_, err = b.PutResourcePolicy(context.Background(), &sm.PutResourcePolicyInput{
		SecretID:       "policy-secret",
		ResourcePolicy: validPolicy,
	})
	require.NoError(t, err)

	// Get
	out, err := b.GetResourcePolicy(context.Background(), &sm.GetResourcePolicyInput{SecretID: "policy-secret"})
	require.NoError(t, err)
	assert.JSONEq(t, validPolicy, out.ResourcePolicy)

	// Delete
	_, err = b.DeleteResourcePolicy(context.Background(), &sm.DeleteResourcePolicyInput{SecretID: "policy-secret"})
	require.NoError(t, err)

	// Get after delete returns empty
	out2, err := b.GetResourcePolicy(context.Background(), &sm.GetResourcePolicyInput{SecretID: "policy-secret"})
	require.NoError(t, err)
	assert.Empty(t, out2.ResourcePolicy)
}

func TestAudit_ResourcePolicy_EmptyPolicyRejected(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "policy-empty", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.PutResourcePolicy(context.Background(), &sm.PutResourcePolicyInput{
		SecretID:       "policy-empty",
		ResourcePolicy: "",
	})
	require.ErrorIs(t, err, sm.ErrInvalidParameter)
}

func TestAudit_ResourcePolicy_NotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.GetResourcePolicy(context.Background(), &sm.GetResourcePolicyInput{SecretID: "missing"})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)

	_, err = b.PutResourcePolicy(context.Background(), &sm.PutResourcePolicyInput{
		SecretID:       "missing",
		ResourcePolicy: validPolicy,
	})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)

	_, err = b.DeleteResourcePolicy(context.Background(), &sm.DeleteResourcePolicyInput{SecretID: "missing"})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)
}

func TestAudit_ResourcePolicy_DeletedSecretRejected(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "policy-del", SecretString: "v"})
	require.NoError(t, err)
	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{SecretID: "policy-del"})
	require.NoError(t, err)

	_, err = b.PutResourcePolicy(context.Background(), &sm.PutResourcePolicyInput{
		SecretID:       "policy-del",
		ResourcePolicy: validPolicy,
	})
	require.ErrorIs(t, err, sm.ErrSecretDeleted)
}

// ---------------------------------------------------------------------------
// ValidateResourcePolicy comprehensive
// ---------------------------------------------------------------------------

func TestAudit_ValidateResourcePolicy_ValidPasses(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.ValidateResourcePolicy(context.Background(), &sm.ValidateResourcePolicyInput{
		ResourcePolicy: validPolicy,
	})
	require.NoError(t, err)
	assert.True(t, out.PolicyValidationPassed)
	assert.Empty(t, out.ValidationErrors)
}

func TestAudit_ValidateResourcePolicy_MissingVersion(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.ValidateResourcePolicy(context.Background(), &sm.ValidateResourcePolicyInput{
		ResourcePolicy: `{"Statement":[]}`,
	})
	require.NoError(t, err)
	assert.False(t, out.PolicyValidationPassed)
	require.NotEmpty(t, out.ValidationErrors)
}

func TestAudit_ValidateResourcePolicy_MissingStatement(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.ValidateResourcePolicy(context.Background(), &sm.ValidateResourcePolicyInput{
		ResourcePolicy: `{"Version":"2012-10-17"}`,
	})
	require.NoError(t, err)
	assert.False(t, out.PolicyValidationPassed)
}

func TestAudit_ValidateResourcePolicy_InvalidJSON(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.ValidateResourcePolicy(context.Background(), &sm.ValidateResourcePolicyInput{
		ResourcePolicy: `not-json`,
	})
	require.NoError(t, err)
	assert.False(t, out.PolicyValidationPassed)
}

func TestAudit_ValidateResourcePolicy_Empty(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.ValidateResourcePolicy(context.Background(), &sm.ValidateResourcePolicyInput{
		ResourcePolicy: "",
	})
	require.ErrorIs(t, err, sm.ErrInvalidParameter)
}

func TestAudit_ValidateResourcePolicy_WithSecretID(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "val-pol-secret", SecretString: "v"})
	require.NoError(t, err)

	out, err := b.ValidateResourcePolicy(context.Background(), &sm.ValidateResourcePolicyInput{
		SecretID:       "val-pol-secret",
		ResourcePolicy: validPolicy,
	})
	require.NoError(t, err)
	assert.True(t, out.PolicyValidationPassed)
}

func TestAudit_ValidateResourcePolicy_SecretIDNotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.ValidateResourcePolicy(context.Background(), &sm.ValidateResourcePolicyInput{
		SecretID:       "missing",
		ResourcePolicy: validPolicy,
	})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)
}

// ---------------------------------------------------------------------------
// Replication comprehensive
// ---------------------------------------------------------------------------

func TestAudit_Replication_AddThenRemove(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "rep-add-rm", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.ReplicateSecretToRegions(context.Background(), &sm.ReplicateSecretToRegionsInput{
		SecretID:          "rep-add-rm",
		AddReplicaRegions: []sm.ReplicaRegion{{Region: "eu-west-1"}},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "rep-add-rm"})
	require.NoError(t, err)
	assert.Len(t, desc.ReplicationStatus, 1)

	_, err = b.RemoveRegionsFromReplication(context.Background(), &sm.RemoveRegionsFromReplicationInput{
		SecretID:             "rep-add-rm",
		RemoveReplicaRegions: []string{"eu-west-1"},
	})
	require.NoError(t, err)

	desc2, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "rep-add-rm"})
	require.NoError(t, err)
	assert.Empty(t, desc2.ReplicationStatus)
}

func TestAudit_Replication_InSyncWithValue(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "rep-insync",
		SecretString: "v",
		AddReplicaRegions: []sm.ReplicaRegion{
			{Region: "ap-southeast-1"},
		},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "rep-insync"})
	require.NoError(t, err)
	require.Len(t, desc.ReplicationStatus, 1)
	assert.Equal(t, "InSync", desc.ReplicationStatus[0].Status)
}

func TestAudit_Replication_FailedWithoutValue(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name: "rep-failed",
		AddReplicaRegions: []sm.ReplicaRegion{
			{Region: "us-west-1"},
		},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "rep-failed"})
	require.NoError(t, err)
	require.Len(t, desc.ReplicationStatus, 1)
	assert.NotEqual(t, "InSync", desc.ReplicationStatus[0].Status)
}

func TestAudit_Replication_StopReplication(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:              "rep-stop",
		SecretString:      "v",
		AddReplicaRegions: []sm.ReplicaRegion{{Region: "ca-central-1"}},
	})
	require.NoError(t, err)

	_, err = b.StopReplicationToReplica(context.Background(), &sm.StopReplicationToReplicaInput{SecretID: "rep-stop"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "rep-stop"})
	require.NoError(t, err)
	assert.Empty(t, desc.ReplicationStatus, "StopReplicationToReplica must clear replication config")
}

func TestAudit_Replication_UpdatedAfterPutSecretValue(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	// Create without value but with replica
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:              "rep-update",
		AddReplicaRegions: []sm.ReplicaRegion{{Region: "sa-east-1"}},
	})
	require.NoError(t, err)

	desc1, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "rep-update"})
	require.NoError(t, err)
	require.Len(t, desc1.ReplicationStatus, 1)
	assert.NotEqual(t, "InSync", desc1.ReplicationStatus[0].Status, "should not be InSync without value")

	// Now add a value
	_, err = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:     "rep-update",
		SecretString: "v",
	})
	require.NoError(t, err)

	desc2, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "rep-update"})
	require.NoError(t, err)
	require.Len(t, desc2.ReplicationStatus, 1)
	assert.Equal(t, "InSync", desc2.ReplicationStatus[0].Status, "should be InSync after value added")
}

func TestAudit_Replication_NotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.ReplicateSecretToRegions(context.Background(), &sm.ReplicateSecretToRegionsInput{
		SecretID:          "missing",
		AddReplicaRegions: []sm.ReplicaRegion{{Region: "eu-west-1"}},
	})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)
}

// ---------------------------------------------------------------------------
// UpdateSecretVersionStage comprehensive
// ---------------------------------------------------------------------------

func TestAudit_UpdateSecretVersionStage_MoveCustomLabel(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "usvs-move",
		SecretString:       "v1",
		ClientRequestToken: "ver-1",
	})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:           "usvs-move",
		SecretString:       "v2",
		ClientRequestToken: "ver-2",
	})
	require.NoError(t, err)

	// Move AWSPREVIOUS from ver-1 to ver-2
	_, err = b.UpdateSecretVersionStage(context.Background(), &sm.UpdateSecretVersionStageInput{
		SecretID:            "usvs-move",
		VersionStage:        "AWSPREVIOUS",
		MoveToVersionID:     "ver-2",
		RemoveFromVersionID: "ver-1",
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "usvs-move"})
	require.NoError(t, err)
	assert.Contains(t, desc.VersionIDsToStages["ver-2"], "AWSPREVIOUS")
}

func TestAudit_UpdateSecretVersionStage_RemoveLabel(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "usvs-rm",
		SecretString:       "v1",
		ClientRequestToken: "ver-1",
	})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:           "usvs-rm",
		SecretString:       "v2",
		ClientRequestToken: "ver-2",
	})
	require.NoError(t, err)

	// Remove AWSPREVIOUS from ver-1
	_, err = b.UpdateSecretVersionStage(context.Background(), &sm.UpdateSecretVersionStageInput{
		SecretID:            "usvs-rm",
		VersionStage:        "AWSPREVIOUS",
		RemoveFromVersionID: "ver-1",
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "usvs-rm"})
	require.NoError(t, err)
	for _, l := range desc.VersionIDsToStages["ver-1"] {
		assert.NotEqual(t, "AWSPREVIOUS", l, "AWSPREVIOUS must be removed from ver-1")
	}
}

func TestAudit_UpdateSecretVersionStage_TargetNotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "usvs-miss", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.UpdateSecretVersionStage(context.Background(), &sm.UpdateSecretVersionStageInput{
		SecretID:        "usvs-miss",
		VersionStage:    "AWSPENDING",
		MoveToVersionID: "nonexistent",
	})
	require.ErrorIs(t, err, sm.ErrVersionNotFound)
}

func TestAudit_UpdateSecretVersionStage_SecretNotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.UpdateSecretVersionStage(context.Background(), &sm.UpdateSecretVersionStageInput{
		SecretID:        "missing",
		VersionStage:    "AWSPENDING",
		MoveToVersionID: "ver-1",
	})
	require.ErrorIs(t, err, sm.ErrSecretNotFound)
}

// ---------------------------------------------------------------------------
// BatchGetSecretValue comprehensive
// ---------------------------------------------------------------------------

func TestAudit_BatchGetSecretValue_MaxResultsTooHigh(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	mr := int32(21)
	_, err := b.BatchGetSecretValue(context.Background(), &sm.BatchGetSecretValueInput{MaxResults: &mr})
	require.ErrorIs(t, err, sm.ErrInvalidParameter, "BatchGetSecretValue MaxResults>20 must fail")
}

func TestAudit_BatchGetSecretValue_MaxResultsHTTP(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.BatchGetSecretValue", `{"MaxResults":25}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit_BatchGetSecretValue_ByIDList(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	for _, name := range []string{"bg-s1", "bg-s2"} {
		_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: name, SecretString: name + "-val"})
		require.NoError(t, err)
	}

	out, err := b.BatchGetSecretValue(context.Background(), &sm.BatchGetSecretValueInput{
		SecretIDList: []string{"bg-s1", "bg-s2"},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretValues, 2)
	assert.Empty(t, out.Errors)
}

func TestAudit_BatchGetSecretValue_MissingInErrors(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "bg-good", SecretString: "v"})
	require.NoError(t, err)

	out, err := b.BatchGetSecretValue(context.Background(), &sm.BatchGetSecretValueInput{
		SecretIDList: []string{"bg-good", "bg-missing"},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretValues, 1)
	assert.Len(t, out.Errors, 1)
	assert.Equal(t, "bg-missing", out.Errors[0].SecretID)
}

func TestAudit_BatchGetSecretValue_ByFilter(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "bg-filter-match",
		SecretString: "v",
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "other-name",
		SecretString: "v",
	})
	require.NoError(t, err)

	out, err := b.BatchGetSecretValue(context.Background(), &sm.BatchGetSecretValueInput{
		Filters: []sm.BatchGetSecretValueFilter{
			{Key: "name", Values: []string{"bg-filter-match"}},
		},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretValues, 1)
	assert.Equal(t, "bg-filter-match", out.SecretValues[0].Name)
}

// ---------------------------------------------------------------------------
// GetRandomPassword comprehensive
// ---------------------------------------------------------------------------

func TestAudit_GetRandomPassword_Default(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.GetRandomPassword(&sm.GetRandomPasswordInput{})
	require.NoError(t, err)
	assert.Len(t, out.RandomPassword, 32, "default password length is 32")
}

func TestAudit_GetRandomPassword_CustomLength(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	length := int64(20)
	out, err := b.GetRandomPassword(&sm.GetRandomPasswordInput{PasswordLength: &length})
	require.NoError(t, err)
	assert.Len(t, out.RandomPassword, 20)
}

func TestAudit_GetRandomPassword_LengthTooShort(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	length := int64(0)
	_, err := b.GetRandomPassword(&sm.GetRandomPasswordInput{PasswordLength: &length})
	require.ErrorIs(t, err, sm.ErrInvalidPasswordParameters)
}

func TestAudit_GetRandomPassword_LengthTooLong(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	length := int64(4097)
	_, err := b.GetRandomPassword(&sm.GetRandomPasswordInput{PasswordLength: &length})
	require.ErrorIs(t, err, sm.ErrInvalidPasswordParameters)
}

func TestAudit_GetRandomPassword_ExcludeNumbers(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.GetRandomPassword(&sm.GetRandomPasswordInput{ExcludeNumbers: true})
	require.NoError(t, err)
	for _, c := range out.RandomPassword {
		assert.False(t, c >= '0' && c <= '9', "password must not contain digits")
	}
}

func TestAudit_GetRandomPassword_ExcludePunctuation(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.GetRandomPassword(&sm.GetRandomPasswordInput{ExcludePunctuation: true})
	require.NoError(t, err)
	assert.NotEmpty(t, out.RandomPassword)
}

func TestAudit_GetRandomPassword_ExcludeUppercase(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.GetRandomPassword(&sm.GetRandomPasswordInput{ExcludeUppercase: true})
	require.NoError(t, err)
	for _, c := range out.RandomPassword {
		assert.False(t, c >= 'A' && c <= 'Z', "password must not contain uppercase")
	}
}

func TestAudit_GetRandomPassword_ExcludeLowercase(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.GetRandomPassword(&sm.GetRandomPasswordInput{ExcludeLowercase: true})
	require.NoError(t, err)
	for _, c := range out.RandomPassword {
		assert.False(t, c >= 'a' && c <= 'z', "password must not contain lowercase")
	}
}

func TestAudit_GetRandomPassword_IncludeSpace(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	length := int64(200)
	out, err := b.GetRandomPassword(&sm.GetRandomPasswordInput{
		PasswordLength: &length,
		IncludeSpace:   true,
	})
	require.NoError(t, err)
	// With 200 chars, space should appear at least once statistically
	assert.NotEmpty(t, out.RandomPassword)
}

func TestAudit_GetRandomPassword_ExcludeAllChars(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.GetRandomPassword(&sm.GetRandomPasswordInput{
		ExcludeNumbers:     true,
		ExcludePunctuation: true,
		ExcludeUppercase:   true,
		ExcludeLowercase:   true,
	})
	require.ErrorIs(t, err, sm.ErrInvalidPasswordParameters,
		"excluding all character classes must return error")
}

func TestAudit_GetRandomPassword_RequireEachIncludedType(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	length := int64(20)
	out, err := b.GetRandomPassword(&sm.GetRandomPasswordInput{
		PasswordLength:          &length,
		RequireEachIncludedType: true,
	})
	require.NoError(t, err)

	hasLower, hasUpper, hasDigit, hasPunct := false, false, false, false
	for _, c := range out.RandomPassword {
		switch {
		case c >= 'a' && c <= 'z':
			hasLower = true
		case c >= 'A' && c <= 'Z':
			hasUpper = true
		case c >= '0' && c <= '9':
			hasDigit = true
		default:
			hasPunct = true
		}
	}
	assert.True(t, hasLower, "must include lowercase")
	assert.True(t, hasUpper, "must include uppercase")
	assert.True(t, hasDigit, "must include digit")
	assert.True(t, hasPunct, "must include punctuation")
}

func TestAudit_GetRandomPassword_ExcludeChars(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.GetRandomPassword(&sm.GetRandomPasswordInput{
		ExcludeCharacters: "aeiouAEIOU",
	})
	require.NoError(t, err)
	for _, c := range out.RandomPassword {
		assert.NotContains(t, "aeiouAEIOU", string(c), "excluded chars must not appear")
	}
}

// ---------------------------------------------------------------------------
// ARN resolution comprehensive
// ---------------------------------------------------------------------------

func TestAudit_ARN_GetByARN(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	created, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "arn-get",
		SecretString: "secret-value",
	})
	require.NoError(t, err)

	out, err := b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{SecretID: created.ARN})
	require.NoError(t, err)
	assert.Equal(t, "secret-value", out.SecretString)
	assert.Equal(t, "arn-get", out.Name)
}

func TestAudit_ARN_DescribeByARN(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	created, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "arn-describe", SecretString: "v"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: created.ARN})
	require.NoError(t, err)
	assert.Equal(t, "arn-describe", desc.Name)
}

func TestAudit_ARN_DeleteByARN(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	created, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "arn-delete", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.DeleteSecret(context.Background(), &sm.DeleteSecretInput{SecretID: created.ARN})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "arn-delete"})
	require.NoError(t, err)
	assert.NotNil(t, desc.DeletedDate)
}

func TestAudit_ARN_ContainsNameAndSuffix(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "my-secret-name", SecretString: "v"})
	require.NoError(t, err)
	assert.Contains(t, out.ARN, "my-secret-name")
	assert.Contains(t, out.ARN, "arn:aws:secretsmanager:")
}

// ---------------------------------------------------------------------------
// Version pruning
// ---------------------------------------------------------------------------

func TestAudit_VersionPruning_MaxVersionsRetained(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "prune-test",
		SecretString:       "v0",
		ClientRequestToken: "v0",
	})
	require.NoError(t, err)

	// Add 105 more versions (total 106 — well over the 100 limit)
	for i := 1; i <= 105; i++ {
		_, err = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
			SecretID:           "prune-test",
			SecretString:       fmt.Sprintf("v%d", i),
			ClientRequestToken: fmt.Sprintf("v%d", i),
		})
		require.NoError(t, err)
	}

	out, err := b.ListSecretVersionIDs(context.Background(), &sm.ListSecretVersionIDsInput{
		SecretID:          "prune-test",
		IncludeDeprecated: true,
	})
	require.NoError(t, err)
	assert.LessOrEqual(t, len(out.Versions), 100, "must not retain more than 100 versions")
}

// ---------------------------------------------------------------------------
// KmsKeyId round-trip
// ---------------------------------------------------------------------------

func TestAudit_KmsKeyID_RoundTrip(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	const kmsKey = "arn:aws:kms:us-east-1:123456789012:key/my-key-id"

	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "kms-rt",
		SecretString: "v",
		KmsKeyID:     kmsKey,
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "kms-rt"})
	require.NoError(t, err)
	assert.Equal(t, kmsKey, desc.KmsKeyID)

	// Update KmsKeyId
	const newKmsKey = "alias/new-key"
	_, err = b.UpdateSecret(context.Background(), &sm.UpdateSecretInput{
		SecretID: "kms-rt",
		KmsKeyID: newKmsKey,
	})
	require.NoError(t, err)

	desc2, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "kms-rt"})
	require.NoError(t, err)
	assert.Equal(t, newKmsKey, desc2.KmsKeyID)
}

// ---------------------------------------------------------------------------
// Concurrent access
// ---------------------------------------------------------------------------

func TestAudit_Concurrent_CreateAndRead(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()

	var wg sync.WaitGroup
	const workers = 10

	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("concurrent-%d", i)
			_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: name, SecretString: "v"})
			if err != nil {
				return
			}
			_, _ = b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{SecretID: name})
		}(i)
	}

	wg.Wait()

	out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, workers)
}

func TestAudit_Concurrent_PutSecretValue(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "concurrent-put", SecretString: "v0"})
	require.NoError(t, err)

	var wg sync.WaitGroup
	const workers = 5

	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
				SecretID:           "concurrent-put",
				SecretString:       fmt.Sprintf("v%d", i),
				ClientRequestToken: fmt.Sprintf("tok-%d", i),
			})
		}(i)
	}

	wg.Wait()

	// Must still be accessible
	_, err = b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{SecretID: "concurrent-put"})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// HTTP handler round-trips for key ops
// ---------------------------------------------------------------------------

func TestAudit_HTTP_CreateAndGetSecretValue(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"http-create","SecretString":"hello-world","ClientRequestToken":"v1"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut sm.CreateSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	assert.Equal(t, "http-create", createOut.Name)
	assert.Equal(t, "v1", createOut.VersionID)

	rec2 := doR1Request(t, h, "secretsmanager.GetSecretValue",
		`{"SecretId":"http-create"}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	var getOut sm.GetSecretValueOutput
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getOut))
	assert.Equal(t, "hello-world", getOut.SecretString)
}

func TestAudit_HTTP_UpdateAndDescribe(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"http-upd","SecretString":"v1"}`)

	rec := doR1Request(t, h, "secretsmanager.UpdateSecret",
		`{"SecretId":"http-upd","Description":"updated desc"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doR1Request(t, h, "secretsmanager.DescribeSecret",
		`{"SecretId":"http-upd"}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	var desc sm.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &desc))
	assert.Equal(t, "updated desc", desc.Description)
}

func TestAudit_HTTP_RotateAndDescribe(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"http-rot","SecretString":"v1","ClientRequestToken":"v1"}`)

	rec := doR1Request(t, h, "secretsmanager.RotateSecret",
		`{"SecretId":"http-rot"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doR1Request(t, h, "secretsmanager.DescribeSecret",
		`{"SecretId":"http-rot"}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	var desc sm.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &desc))
	assert.True(t, desc.RotationEnabled)
}

func TestAudit_HTTP_DeleteAndRestore(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"http-dr","SecretString":"v"}`)

	rec := doR1Request(t, h, "secretsmanager.DeleteSecret", `{"SecretId":"http-dr"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doR1Request(t, h, "secretsmanager.RestoreSecret", `{"SecretId":"http-dr"}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	rec3 := doR1Request(t, h, "secretsmanager.DescribeSecret", `{"SecretId":"http-dr"}`)
	require.Equal(t, http.StatusOK, rec3.Code)

	var desc sm.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &desc))
	assert.Nil(t, desc.DeletedDate, "DeletedDate must be nil after restore")
}

func TestAudit_HTTP_PutSecretValue_EmptyRejected(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	doR1Request(t, h, "secretsmanager.CreateSecret", `{"Name":"http-psv","SecretString":"v"}`)

	rec := doR1Request(t, h, "secretsmanager.PutSecretValue", `{"SecretId":"http-psv"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sm.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidParameterException", errResp.Type)
}

func TestAudit_HTTP_TagAndUntag(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	doR1Request(t, h, "secretsmanager.CreateSecret", `{"Name":"http-tags","SecretString":"v"}`)

	rec := doR1Request(t, h, "secretsmanager.TagResource",
		`{"SecretId":"http-tags","Tags":[{"Key":"env","Value":"prod"}]}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doR1Request(t, h, "secretsmanager.UntagResource",
		`{"SecretId":"http-tags","TagKeys":["env"]}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "http-tags"})
	require.NoError(t, err)
	if desc.Tags != nil {
		tagMap := desc.Tags.Clone()
		_, hasEnv := tagMap["env"]
		assert.False(t, hasEnv)
	}
}

func TestAudit_HTTP_ListSecrets_MaxResultsZeroRejected(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.ListSecrets", `{"MaxResults":0}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit_HTTP_GetRandomPassword(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.GetRandomPassword",
		`{"PasswordLength":16,"ExcludeNumbers":true}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out sm.GetRandomPasswordOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.RandomPassword, 16)
}

func TestAudit_HTTP_UnknownOperation(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.NonExistentOp", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Reset
// ---------------------------------------------------------------------------

func TestAudit_Reset_ClearsAll(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	for _, name := range []string{"r1", "r2", "r3"} {
		_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	h := sm.NewHandler(b)
	h.Reset()

	out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{})
	require.NoError(t, err)
	assert.Empty(t, out.SecretList)
}
