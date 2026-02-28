package sts_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/sts"
)

func TestGetSessionToken_DefaultDuration(t *testing.T) {
	t.Parallel()
	backend := sts.NewInMemoryBackend()
	resp, err := backend.GetSessionToken(&sts.GetSessionTokenInput{})
	require.NoError(t, err)

	creds := resp.GetSessionTokenResult.Credentials
	assert.True(t, strings.HasPrefix(creds.AccessKeyID, "ASIA"), "expected ASIA prefix, got %q", creds.AccessKeyID)
	assert.NotEmpty(t, creds.SecretAccessKey, "expected non-empty SecretAccessKey")
	assert.NotEmpty(t, creds.SessionToken, "expected non-empty SessionToken")

	exp, err := time.Parse(time.RFC3339, creds.Expiration)
	require.NoError(t, err, "parse expiration")

	diff := time.Until(exp)
	assert.InDelta(t, 12*time.Hour, diff, float64(time.Hour), "expected ~12h expiration")
}

func TestGetSessionToken_CustomDuration(t *testing.T) {
	t.Parallel()
	backend := sts.NewInMemoryBackend()
	resp, err := backend.GetSessionToken(&sts.GetSessionTokenInput{DurationSeconds: 3600})
	require.NoError(t, err)

	exp, err := time.Parse(time.RFC3339, resp.GetSessionTokenResult.Credentials.Expiration)
	require.NoError(t, err, "parse expiration")

	diff := time.Until(exp)
	assert.InDelta(t, time.Hour, diff, float64(time.Minute), "expected ~1h expiration")
}

func TestGetSessionToken_InvalidDuration(t *testing.T) {
	t.Parallel()
	backend := sts.NewInMemoryBackend()
	_, err := backend.GetSessionToken(&sts.GetSessionTokenInput{DurationSeconds: 100})
	require.Error(t, err, "expected error for duration=100")
	assert.Contains(t, err.Error(), "DurationSeconds")
}
