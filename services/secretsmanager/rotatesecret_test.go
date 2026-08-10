package secretsmanager_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// RotateSecret comprehensive
// ---------------------------------------------------------------------------

func TestRotateSecret_CreatesNewVersion(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "rot-new-ver",
		SecretString:       "original",
		ClientRequestToken: "ver-orig",
	})
	require.NoError(t, err)

	out, err := b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:          "rot-new-ver",
		RotationLambdaARN: testLambdaARN,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.VersionID)
	assert.NotEqual(t, "ver-orig", out.VersionID)
}

func TestRotateSecret_AWSCURRENTPromotedToAWSPREVIOUS(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "rot-stages",
		SecretString:       "v1",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:          "rot-stages",
		RotationLambdaARN: testLambdaARN,
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "rot-stages"})
	require.NoError(t, err)

	var hasCurrent, hasPrevious bool

	for _, labels := range desc.VersionIDsToStages {
		for _, l := range labels {
			if l == secretsmanager.StagingLabelCurrent {
				hasCurrent = true
			}
			if l == secretsmanager.StagingLabelPrevious {
				hasPrevious = true
			}
		}
	}

	assert.True(t, hasCurrent, "must have AWSCURRENT after rotation")
	assert.True(t, hasPrevious, "must have AWSPREVIOUS after rotation (v1 demoted)")
}

func TestRotateSecret_LastRotatedDateUpdated(t *testing.T) {
	t.Parallel()

	before := time.Now()
	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "rot-date", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:          "rot-date",
		RotationLambdaARN: testLambdaARN,
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "rot-date"})
	require.NoError(t, err)
	require.NotNil(t, desc.LastRotatedDate)
	// UnixTimeFloat stores nanoseconds/1e9; recover with int64(f*1e9) nanoseconds.
	rotated := time.Unix(0, int64(*desc.LastRotatedDate*1e9))
	assert.False(t, rotated.Before(before.Add(-time.Second)),
		"LastRotatedDate must be at or after test start (within 1s tolerance)")
}

func TestRotateSecret_RotationEnabledAfterRotate(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "rot-enabled", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:          "rot-enabled",
		RotationLambdaARN: testLambdaARN,
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "rot-enabled"})
	require.NoError(t, err)
	assert.True(t, desc.RotationEnabled)
}

func TestRotateSecret_RotateImmediatelyFalse(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "rot-no-imm",
		SecretString:       "v1",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	noImm := false
	days := int64(30)
	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:          "rot-no-imm",
		RotationLambdaARN: testLambdaARN,
		RotateImmediately: &noImm,
		RotationRules: &secretsmanager.RotationRulesType{
			AutomaticallyAfterDays: &days,
		},
	})
	require.NoError(t, err)

	// Value must still be v1 (no immediate rotation)
	val, err := b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{SecretID: "rot-no-imm"})
	require.NoError(t, err)
	assert.Equal(t, "v1", val.SecretString)
}

func TestRotateSecret_LambdaARNStored(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "rot-lambda", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:          "rot-lambda",
		RotationLambdaARN: "arn:aws:lambda:us-east-1:123456789012:function:MyRotator",
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "rot-lambda"})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:MyRotator", desc.RotationLambdaARN)
}

func TestRotateSecret_NotFound(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{SecretID: "missing"})
	require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
}

func TestRotateSecret_DeletedFails(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "rot-del", SecretString: "v"},
	)
	require.NoError(t, err)
	_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "rot-del"})
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{SecretID: "rot-del"})
	require.ErrorIs(t, err, secretsmanager.ErrSecretDeleted)
}

// TestRotateSecret_NoRotationStrategyConfigured_Rejected verifies gopherstack-9wuh's
// fix: real AWS rejects RotateSecret with InvalidRequestException when no
// RotationLambdaARN is configured, neither already stored on the secret nor
// supplied on this request -- see aws-sdk-go-v2/service/secretsmanager@v1.44.4
// types/errors.go's InvalidRequestException doc comment ("You tried to enable
// rotation on a secret that doesn't already have a Lambda function ARN configured
// and you didn't include such an ARN as a parameter in this call"). The rejected
// call must also leave the secret's rotation state and version set untouched
// (parity-principles.md's "state mutated before validation" bug class).
func TestRotateSecret_NoRotationStrategyConfigured_Rejected(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "no-strategy",
		SecretString: "v1",
	})
	require.NoError(t, err)

	before, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "no-strategy"})
	require.NoError(t, err)

	days := int64(30)
	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID: "no-strategy",
		RotationRules: &secretsmanager.RotationRulesType{
			AutomaticallyAfterDays: &days,
		},
	})
	require.ErrorIs(t, err, secretsmanager.ErrRotationStrategyRequired)

	after, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "no-strategy"})
	require.NoError(t, err)
	assert.False(t, after.RotationEnabled, "rejected RotateSecret must not enable rotation")
	assert.Nil(t, after.RotationRules, "rejected RotateSecret must not persist RotationRules")
	assert.Equal(t, before.VersionIDsToStages, after.VersionIDsToStages,
		"rejected RotateSecret must not create a new version")

	val, err := b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{SecretID: "no-strategy"})
	require.NoError(t, err)
	assert.Equal(t, "v1", val.SecretString)
}

// ---------------------------------------------------------------------------
// RotateSecret HTTP + Lambda invocation
// ---------------------------------------------------------------------------

// TestRotateSecret_Backend tests the RotateSecret op end to end via HTTP.
func TestRotateSecret_Backend(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)

	_, err := backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "rotate-secret",
		SecretString: "original-value",
	})
	require.NoError(t, err)

	rotateBody := fmt.Sprintf(`{"SecretId":"rotate-secret","RotationLambdaARN":%q}`, testLambdaARN)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(rotateBody))
	req.Header.Set("X-Amz-Target", "secretsmanager.RotateSecret")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.RotateSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "rotate-secret", out.Name)
	assert.NotEmpty(t, out.VersionID)

	// New version should be AWSCURRENT. A Lambda ARN is configured but no invoker
	// is wired, so the backend promotes immediately with a freshly generated value
	// (see rotateSecretLocked) rather than carrying "original-value" forward.
	curr, err := backend.GetSecretValue(
		context.Background(),
		&secretsmanager.GetSecretValueInput{SecretID: "rotate-secret"},
	)
	require.NoError(t, err)
	assert.Equal(t, out.VersionID, curr.VersionID)
	assert.NotEqual(t, "original-value", curr.SecretString)
}

// TestRotateSecret_WithLambda tests RotateSecret invoking a rotation Lambda.
// Uses mockLambdaInvoker/lambdaCall defined in helpers_test.go (same package).
func TestRotateSecret_WithLambda(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)

	mock := &mockLambdaInvoker{}
	h.SetLambdaInvoker(mock)

	_, err := backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "lambda-rotate-secret",
		SecretString: "initial-value",
	})
	require.NoError(t, err)

	const lambdaRotateARN = "arn:aws:lambda:us-east-1:000000000000:function:my-rotator"
	rotateBody := fmt.Sprintf(
		`{"SecretId":"lambda-rotate-secret","RotationLambdaARN":%q}`,
		lambdaRotateARN,
	)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(rotateBody))
	req.Header.Set("X-Amz-Target", "secretsmanager.RotateSecret")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// Rotation Lambda should have been invoked 4 times (one per step).
	require.Len(t, mock.calls, 4)

	steps := []string{"createSecret", "setSecret", "testSecret", "finishSecret"}
	for i, call := range mock.calls {
		assert.Equal(t, "my-rotator", call.name)
		assert.Equal(t, "RequestResponse", call.invocationType)
		var event map[string]string
		require.NoError(t, json.Unmarshal(call.payload, &event))
		assert.Equal(t, "lambda-rotate-secret", event["SecretId"])
		assert.Equal(t, steps[i], event["Step"])
		assert.NotEmpty(t, event["ClientRequestToken"])
	}
}

// TestRotateSecret_OmittedARNUsesStoredLambda verifies that once a rotation
// Lambda ARN is configured on a secret, a later RotateSecret call that omits
// RotationLambdaARN (the normal case -- real callers configure it once, not
// on every call) still invokes that Lambda rather than silently promoting
// straight to AWSCURRENT.
func TestRotateSecret_OmittedARNUsesStoredLambda(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)

	mock := &mockLambdaInvoker{}
	h.SetLambdaInvoker(mock)

	_, err := backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "omitted-arn-secret",
		SecretString: "initial-value",
	})
	require.NoError(t, err)

	const lambdaRotateARN = "arn:aws:lambda:us-east-1:000000000000:function:my-rotator"

	firstBody := fmt.Sprintf(
		`{"SecretId":"omitted-arn-secret","RotationLambdaARN":%q}`,
		lambdaRotateARN,
	)
	firstReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(firstBody))
	firstReq.Header.Set("X-Amz-Target", "secretsmanager.RotateSecret")
	firstRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(firstReq, firstRec)))
	require.Equal(t, http.StatusOK, firstRec.Code)
	require.Len(t, mock.calls, 4)

	mock.calls = nil

	secondBody := `{"SecretId":"omitted-arn-secret"}`
	secondReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(secondBody))
	secondReq.Header.Set("X-Amz-Target", "secretsmanager.RotateSecret")
	secondRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(secondReq, secondRec)))
	assert.Equal(t, http.StatusOK, secondRec.Code)

	require.Len(t, mock.calls, 4)

	steps := []string{"createSecret", "setSecret", "testSecret", "finishSecret"}
	for i, call := range mock.calls {
		assert.Equal(t, "my-rotator", call.name)
		assert.Equal(t, steps[i], func() string {
			var event map[string]string
			require.NoError(t, json.Unmarshal(call.payload, &event))

			return event["Step"]
		}())
	}

	desc, err := backend.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{
		SecretID: "omitted-arn-secret",
	})
	require.NoError(t, err)
	assert.Equal(t, lambdaRotateARN, desc.RotationLambdaARN)
}

// TestRotateSecret_RotateImmediatelyFalseWithLambdaRunsTestSecretProbe verifies the
// gopherstack-avt gap: when RotateSecret is called with RotateImmediately=false and a
// rotation Lambda is configured, AWS runs ONLY the Lambda's testSecret step against a
// transient AWSPENDING version and then removes that version -- no createSecret/
// setSecret/finishSecret steps run, nothing is promoted to AWSCURRENT, and no
// AWSPENDING version is left behind.
func TestRotateSecret_RotateImmediatelyFalseWithLambdaRunsTestSecretProbe(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)

	mock := &mockLambdaInvoker{}
	h.SetLambdaInvoker(mock)

	_, err := backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "probe-secret",
		SecretString: "initial-value",
	})
	require.NoError(t, err)

	before, err := backend.GetSecretValue(
		context.Background(), &secretsmanager.GetSecretValueInput{SecretID: "probe-secret"},
	)
	require.NoError(t, err)

	const lambdaRotateARN = "arn:aws:lambda:us-east-1:000000000000:function:probe-rotator"
	rotateBody := fmt.Sprintf(
		`{"SecretId":"probe-secret","RotationLambdaARN":%q,"RotateImmediately":false}`,
		lambdaRotateARN,
	)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(rotateBody))
	req.Header.Set("X-Amz-Target", "secretsmanager.RotateSecret")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.RotateSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.VersionID, "no permanent version is created by a test probe")

	// Exactly one Lambda invocation: the testSecret step.
	require.Len(t, mock.calls, 1)
	assert.Equal(t, "probe-rotator", mock.calls[0].name)

	var event map[string]string
	require.NoError(t, json.Unmarshal(mock.calls[0].payload, &event))
	assert.Equal(t, "probe-secret", event["SecretId"])
	assert.Equal(t, "testSecret", event["Step"])

	// The current version must be unchanged, and no AWSPENDING version left behind.
	current, err := backend.GetSecretValue(
		context.Background(), &secretsmanager.GetSecretValueInput{SecretID: "probe-secret"},
	)
	require.NoError(t, err)
	assert.Equal(t, before.VersionID, current.VersionID)

	desc, err := backend.DescribeSecret(
		context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "probe-secret"},
	)
	require.NoError(t, err)
	for versionID, labels := range desc.VersionIDsToStages {
		assert.NotContains(t, labels, "AWSPENDING", "probe version %s must be removed", versionID)
	}
}

// TestRotateSecret_RotateImmediatelyFalseWithLambdaProbeFails verifies that a failing
// testSecret Lambda step surfaces an error and still leaves no AWSPENDING version behind.
func TestRotateSecret_RotateImmediatelyFalseWithLambdaProbeFails(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)

	mock := &mockLambdaInvoker{invokeErr: assert.AnError}
	h.SetLambdaInvoker(mock)

	_, err := backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "probe-fail-secret",
		SecretString: "initial-value",
	})
	require.NoError(t, err)

	const lambdaRotateARN = "arn:aws:lambda:us-east-1:000000000000:function:probe-fail-rotator"
	rotateBody := fmt.Sprintf(
		`{"SecretId":"probe-fail-secret","RotationLambdaARN":%q,"RotateImmediately":false}`,
		lambdaRotateARN,
	)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(rotateBody))
	req.Header.Set("X-Amz-Target", "secretsmanager.RotateSecret")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.NotEqual(t, http.StatusOK, rec.Code)

	desc, err := backend.DescribeSecret(
		context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "probe-fail-secret"},
	)
	require.NoError(t, err)
	for versionID, labels := range desc.VersionIDsToStages {
		assert.NotContains(t, labels, "AWSPENDING", "probe version %s must be removed even on failure", versionID)
	}
}

// TestRotateSecret_NoLambdaInvoker tests rotation without Lambda (stub only).
func TestRotateSecret_NoLambdaInvoker(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)
	// No lambda invoker set

	_, err := backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "no-lambda-rotate",
		SecretString: "value",
	})
	require.NoError(t, err)

	const noLambdaRotateARN = "arn:aws:lambda:us-east-1:000000000000:function:rotator"
	// Even with a RotationLambdaARN, if no invoker is wired, it should still succeed (stub rotation).
	rotateBody := fmt.Sprintf(
		`{"SecretId":"no-lambda-rotate","RotationLambdaARN":%q}`,
		noLambdaRotateARN,
	)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(rotateBody))
	req.Header.Set("X-Amz-Target", "secretsmanager.RotateSecret")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.RotateSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.VersionID)
}

// TestRotateSecret_RotationEnabledFlag verifies RotationEnabled is set after RotateSecret.
func TestRotateSecret_RotationEnabledFlag(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()

	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "rot-flag-test",
		SecretString: "initial",
	})
	require.NoError(t, err)

	// Before rotation: RotationEnabled should be false.
	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "rot-flag-test"})
	require.NoError(t, err)
	assert.False(t, desc.RotationEnabled)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:          "rot-flag-test",
		RotationLambdaARN: testLambdaARN,
	})
	require.NoError(t, err)

	// After rotation: RotationEnabled should be true and LastChangedDate set.
	desc, err = b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "rot-flag-test"})
	require.NoError(t, err)
	assert.True(t, desc.RotationEnabled)
	assert.NotNil(t, desc.LastChangedDate)
}

// ---------------------------------------------------------------------------
// RotateSecret Lambda ARN / LastRotatedDate
// ---------------------------------------------------------------------------

// TestRotateSecret_RotationLambdaARNStored verifies RotationLambdaARN is stored and returned.
func TestRotateSecret_RotationLambdaARNStored(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "rla-test", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:          "rla-test",
		RotationLambdaARN: "arn:aws:lambda:us-east-1:123:function:my-rotator",
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "rla-test"})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123:function:my-rotator", desc.RotationLambdaARN)
}

// TestRotateSecret_LastRotatedDate verifies LastRotatedDate is set after rotation.
func TestRotateSecret_LastRotatedDate(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "lrd-test", SecretString: "v"},
	)
	require.NoError(t, err)

	desc0, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "lrd-test"})
	require.NoError(t, err)
	assert.Nil(t, desc0.LastRotatedDate)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:          "lrd-test",
		RotationLambdaARN: testLambdaARN,
	})
	require.NoError(t, err)

	desc1, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "lrd-test"})
	require.NoError(t, err)
	require.NotNil(t, desc1.LastRotatedDate)
	assert.Greater(t, *desc1.LastRotatedDate, float64(0))
}

// ---------------------------------------------------------------------------
// RotateSecret validation
// ---------------------------------------------------------------------------

// TestRotateSecret_InvalidDays verifies that AutomaticallyAfterDays outside
// the 1-365 range is rejected. Uses doR1Request from helpers_test.go.
func TestRotateSecret_InvalidDays(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"days-validation","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		name string
		days int
	}{
		{name: "zero_days", days: 0},
		{name: "too_many_days", days: 366},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rotBody, _ := json.Marshal(map[string]any{
				"SecretId":      "days-validation",
				"RotationRules": map[string]any{"AutomaticallyAfterDays": tt.days},
			})
			rotRec := doR1Request(t, h, "secretsmanager.RotateSecret", string(rotBody))
			assert.Equal(t, http.StatusBadRequest, rotRec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// RotateSecret cron scheduling
// ---------------------------------------------------------------------------

// TestRotateSecret_CronScheduleTriggersRotation verifies that setting a
// ScheduleExpression with a cron expression enables automatic background rotation.
func TestRotateSecret_CronScheduleTriggersRotation(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "cron-sched-secret",
		SecretString: "initial",
	})
	require.NoError(t, err)

	before, err := b.GetSecretValue(
		context.Background(),
		&secretsmanager.GetSecretValueInput{SecretID: "cron-sched-secret"},
	)
	require.NoError(t, err)

	// Use a cron that fires every minute to trigger fast in tests.
	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:          "cron-sched-secret",
		RotationLambdaARN: testLambdaARN,
		RotationRules: &secretsmanager.RotationRulesType{
			ScheduleExpression: "cron(* * * * ? *)",
		},
	})
	require.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	rotated := false

	for time.Now().Before(deadline) {
		current, currentErr := b.GetSecretValue(
			context.Background(),
			&secretsmanager.GetSecretValueInput{SecretID: "cron-sched-secret"},
		)
		require.NoError(t, currentErr)

		if current.VersionID != before.VersionID {
			rotated = true

			break
		}

		time.Sleep(200 * time.Millisecond)
	}

	assert.True(t, rotated, "cron-scheduled rotation must fire within 5 seconds")
}

// TestRotateSecret_ScheduleExpressionPersisted verifies that a cron ScheduleExpression
// is persisted in RotationRules and visible in DescribeSecret.
func TestRotateSecret_ScheduleExpressionPersisted(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "cron-persist",
		SecretString: "v",
	})
	require.NoError(t, err)

	const expr = "cron(0 12 * * ? *)"
	rotateImmediately := false
	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:          "cron-persist",
		RotationLambdaARN: testLambdaARN,
		RotationRules: &secretsmanager.RotationRulesType{
			ScheduleExpression: expr,
		},
		RotateImmediately: &rotateImmediately,
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "cron-persist"})
	require.NoError(t, err)
	require.NotNil(t, desc.RotationRules)
	assert.Equal(t, expr, desc.RotationRules.ScheduleExpression)
}

// TestRotationDue_CronExpression verifies that rotationDue returns false before the next
// cron time and true at or after it.
func TestRotationDue_CronExpression(t *testing.T) {
	t.Parallel()

	// Base: March 15 midnight. Cron: daily at midnight.
	base := secretsmanager.UnixTimeFloat(time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC))
	rules := &secretsmanager.RotationRulesType{ScheduleExpression: "cron(0 0 * * ? *)"}

	// Before next midnight: not due.
	assert.False(t, secretsmanager.RotationDue(rules, time.Date(2024, 3, 15, 12, 0, 0, 0, time.UTC), &base))

	// At next midnight: due.
	assert.True(t, secretsmanager.RotationDue(rules, time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC), &base))

	// After next midnight: due.
	assert.True(t, secretsmanager.RotationDue(rules, time.Date(2024, 3, 17, 0, 0, 0, 0, time.UTC), &base))
}

// TestRotationDue_IntervalExpression verifies rotationDue for rate() expressions.
func TestRotationDue_IntervalExpression(t *testing.T) {
	t.Parallel()

	base := secretsmanager.UnixTimeFloat(time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC))
	rules := &secretsmanager.RotationRulesType{ScheduleExpression: "rate(1 day)"}

	before := time.Date(2024, 3, 15, 12, 0, 0, 0, time.UTC)
	after := time.Date(2024, 3, 16, 1, 0, 0, 0, time.UTC)

	assert.False(t, secretsmanager.RotationDue(rules, before, &base))
	assert.True(t, secretsmanager.RotationDue(rules, after, &base))
}

// ---------------------------------------------------------------------------
// RotateSecret rules + background scheduler
// ---------------------------------------------------------------------------

func TestRotateSecret_RulesAndScheduler(t *testing.T) {
	t.Parallel()

	afterSevenDays := int64(7)
	rotateImmediatelyFalse := false

	tests := []struct {
		name               string
		rotateInput        secretsmanager.RotateSecretInput
		waitForAutoRotate  time.Duration
		wantAutoRotation   bool
		wantImmediateEmpty bool
	}{
		{
			name: "rotate_immediately_false_sets_rules_only",
			rotateInput: secretsmanager.RotateSecretInput{
				SecretID:          "sched-secret",
				RotationLambdaARN: testLambdaARN,
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
				SecretID:          "sched-secret",
				RotationLambdaARN: testLambdaARN,
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
			_, err := backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
				Name:         "sched-secret",
				SecretString: "initial",
			})
			require.NoError(t, err)

			before, err := backend.GetSecretValue(
				context.Background(),
				&secretsmanager.GetSecretValueInput{SecretID: "sched-secret"},
			)
			require.NoError(t, err)

			out, err := backend.RotateSecret(context.Background(), &tt.rotateInput)
			require.NoError(t, err)

			desc, err := backend.DescribeSecret(
				context.Background(),
				&secretsmanager.DescribeSecretInput{SecretID: "sched-secret"},
			)
			require.NoError(t, err)
			require.NotNil(t, desc.RotationRules)

			if tt.wantImmediateEmpty {
				assert.Empty(t, out.VersionID)
				current, currentErr := backend.GetSecretValue(
					context.Background(),
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
					context.Background(),
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
