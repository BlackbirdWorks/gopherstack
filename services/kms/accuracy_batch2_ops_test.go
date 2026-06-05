package kms_test

// accuracy_batch2_ops_test.go — batch-2 ops AWS-accuracy audit for services/kms (go-k0nc).
// Covers two gaps beyond the earlier batch-2 audits (go-78bt, go-epvx):
//
//  1. EnableKeyRotation RotationPeriodInDays minimum: AWS rejects values below 90 days
//     with ValidationException. Previously the backend accepted values as low as 1.
//  2. TagResource / UntagResource PendingDeletion guard: AWS rejects both operations
//     for keys in PendingDeletion state with KMSInvalidStateException. Previously
//     both handlers only checked key existence, not key state.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func ab2NewBackend(t *testing.T) *kms.InMemoryBackend {
	t.Helper()

	return kms.NewInMemoryBackend()
}

func ab2NewHandler(t *testing.T) *kms.Handler {
	t.Helper()

	return kms.NewHandler(kms.NewInMemoryBackend())
}

func ab2MustCreateKey(t *testing.T, b *kms.InMemoryBackend) string {
	t.Helper()
	out, err := b.CreateKey(&kms.CreateKeyInput{})
	require.NoError(t, err)

	return out.KeyMetadata.KeyID
}

func ab2MustScheduleDeletion(t *testing.T, b *kms.InMemoryBackend, keyID string) {
	t.Helper()
	_, err := b.ScheduleKeyDeletion(&kms.ScheduleKeyDeletionInput{
		KeyID:               keyID,
		PendingWindowInDays: 7,
	})
	require.NoError(t, err)
}

func doKMSRequest(t *testing.T, h *kms.Handler, action string, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", fmt.Sprintf("TrentService.%s", action))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

// ── 1. EnableKeyRotation RotationPeriodInDays minimum ────────────────────────

func TestAB2_EnableKeyRotation_PeriodBelowMinimum_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		period int32
	}{
		{"one", 1},
		{"forty-five", 45},
		{"eighty-nine", 89},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := ab2NewBackend(t)
			keyID := ab2MustCreateKey(t, b)
			period := tc.period

			err := b.EnableKeyRotation(&kms.EnableKeyRotationInput{
				KeyID:                keyID,
				RotationPeriodInDays: &period,
			})
			require.Error(t, err)
			assert.ErrorIs(t, err, kms.ErrValidation,
				"period %d should be rejected as below minimum 90", tc.period)
		})
	}
}

func TestAB2_EnableKeyRotation_PeriodAtOrAboveMinimum_Accepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		period int32
	}{
		{"ninety", 90},
		{"one-year", 365},
		{"max", 2560},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := ab2NewBackend(t)
			keyID := ab2MustCreateKey(t, b)
			period := tc.period

			err := b.EnableKeyRotation(&kms.EnableKeyRotationInput{
				KeyID:                keyID,
				RotationPeriodInDays: &period,
			})
			assert.NoError(t, err, "period %d should be accepted", tc.period)
		})
	}
}

func TestAB2_EnableKeyRotation_PeriodAboveMaximum_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		period int32
	}{
		{"two-thousand-five-sixty-one", 2561},
		{"ten-thousand", 10000},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := ab2NewBackend(t)
			keyID := ab2MustCreateKey(t, b)
			period := tc.period

			err := b.EnableKeyRotation(&kms.EnableKeyRotationInput{
				KeyID:                keyID,
				RotationPeriodInDays: &period,
			})
			require.Error(t, err)
			assert.ErrorIs(t, err, kms.ErrValidation,
				"period %d should be rejected as above maximum 2560", tc.period)
		})
	}
}

// ── 2. TagResource / UntagResource PendingDeletion guard ─────────────────────

func TestAB2_TagResource_PendingDeletion_Rejected(t *testing.T) {
	t.Parallel()

	h := ab2NewHandler(t)
	b := h.Backend.(*kms.InMemoryBackend)
	keyID := ab2MustCreateKey(t, b)
	ab2MustScheduleDeletion(t, b, keyID)

	body, err := json.Marshal(map[string]any{
		"KeyId": keyID,
		"Tags":  []map[string]string{{"TagKey": "env", "TagValue": "test"}},
	})
	require.NoError(t, err)

	rec := doKMSRequest(t, h, "TagResource", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "KMSInvalidStateException", errResp.Type)
}

func TestAB2_TagResource_EnabledAndDisabled_Accepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(b *kms.InMemoryBackend, keyID string) error
	}{
		{"enabled", func(_ *kms.InMemoryBackend, _ string) error { return nil }},
		{
			"disabled",
			func(b *kms.InMemoryBackend, keyID string) error {
				return b.DisableKey(&kms.DisableKeyInput{KeyID: keyID})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := ab2NewHandler(t)
			b := h.Backend.(*kms.InMemoryBackend)
			keyID := ab2MustCreateKey(t, b)
			require.NoError(t, tc.setup(b, keyID))

			body, merr := json.Marshal(map[string]any{
				"KeyId": keyID,
				"Tags":  []map[string]string{{"TagKey": "env", "TagValue": "test"}},
			})
			require.NoError(t, merr)

			rec := doKMSRequest(t, h, "TagResource", string(body))
			assert.Equal(t, http.StatusOK, rec.Code,
				"TagResource on %s key should succeed", tc.name)
		})
	}
}

func TestAB2_UntagResource_PendingDeletion_Rejected(t *testing.T) {
	t.Parallel()

	h := ab2NewHandler(t)
	b := h.Backend.(*kms.InMemoryBackend)
	keyID := ab2MustCreateKey(t, b)
	ab2MustScheduleDeletion(t, b, keyID)

	body, err := json.Marshal(map[string]any{
		"KeyId":   keyID,
		"TagKeys": []string{"env"},
	})
	require.NoError(t, err)

	rec := doKMSRequest(t, h, "UntagResource", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "KMSInvalidStateException", errResp.Type)
}

func TestAB2_UntagResource_EnabledAndDisabled_Accepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(b *kms.InMemoryBackend, keyID string) error
	}{
		{"enabled", func(_ *kms.InMemoryBackend, _ string) error { return nil }},
		{
			"disabled",
			func(b *kms.InMemoryBackend, keyID string) error {
				return b.DisableKey(&kms.DisableKeyInput{KeyID: keyID})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := ab2NewHandler(t)
			b := h.Backend.(*kms.InMemoryBackend)
			keyID := ab2MustCreateKey(t, b)
			require.NoError(t, tc.setup(b, keyID))

			body, merr := json.Marshal(map[string]any{
				"KeyId":   keyID,
				"TagKeys": []string{"nonexistent-key"},
			})
			require.NoError(t, merr)

			rec := doKMSRequest(t, h, "UntagResource", string(body))
			assert.Equal(t, http.StatusOK, rec.Code,
				"UntagResource on %s key should succeed", tc.name)
		})
	}
}
