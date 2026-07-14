package kms_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"

	"log/slog"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

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
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	return out.KeyMetadata.KeyID
}

func ab2MustScheduleDeletion(t *testing.T, b *kms.InMemoryBackend, keyID string) {
	t.Helper()
	_, err := b.ScheduleKeyDeletion(context.Background(), &kms.ScheduleKeyDeletionInput{
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

func newBackend(t *testing.T) *kms.InMemoryBackend {
	t.Helper()

	return kms.NewInMemoryBackend()
}

func mustCreateSymKey(t *testing.T, b *kms.InMemoryBackend) string {
	t.Helper()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	return out.KeyMetadata.KeyID
}

func mustCreateHMACKey(t *testing.T, b *kms.InMemoryBackend, spec string) string {
	t.Helper()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  spec,
		KeyUsage: kms.KeyUsageGenerateMac,
	})
	require.NoError(t, err)

	return out.KeyMetadata.KeyID
}

func mustCreateRSAKey(t *testing.T, b *kms.InMemoryBackend) string {
	t.Helper()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "RSA_2048",
		KeyUsage: kms.KeyUsageSignVerify,
	})
	require.NoError(t, err)

	return out.KeyMetadata.KeyID
}

func mustCreateECKey(t *testing.T, b *kms.InMemoryBackend, spec string) string {
	t.Helper()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  spec,
		KeyUsage: kms.KeyUsageSignVerify,
	})
	require.NoError(t, err)

	return out.KeyMetadata.KeyID
}

func ptr[T any](v T) *T {
	p := new(T)
	*p = v

	return p
}

func b2newBackend(t *testing.T) *kms.InMemoryBackend {
	t.Helper()

	return kms.NewInMemoryBackend()
}

func b2newHandler(t *testing.T) *kms.Handler {
	t.Helper()

	return kms.NewHandler(kms.NewInMemoryBackend())
}

func b2mustCreateMultiRegionKey(t *testing.T, b *kms.InMemoryBackend) string {
	t.Helper()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		MultiRegion: true,
		Description: "primary key",
	})
	require.NoError(t, err)

	return out.KeyMetadata.KeyID
}

func b2mustCreateExternalKey(t *testing.T, b *kms.InMemoryBackend) string {
	t.Helper()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		Origin: "EXTERNAL",
	})
	require.NoError(t, err)

	return out.KeyMetadata.KeyID
}

func b2mustCreateECKey(t *testing.T, b *kms.InMemoryBackend, spec string, usage string) string {
	t.Helper()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  spec,
		KeyUsage: usage,
	})
	require.NoError(t, err)

	return out.KeyMetadata.KeyID
}

func b2importKeyMaterial(t *testing.T, b *kms.InMemoryBackend, keyID string) {
	t.Helper()
	mat := make([]byte, 32)
	for i := range mat {
		mat[i] = byte(i + 1)
	}
	require.NoError(t, b.ImportKeyMaterial(context.Background(), &kms.ImportKeyMaterialInput{
		KeyID:       keyID,
		KeyMaterial: mat,
	}))
}

func b2postKMSOp(t *testing.T, h *kms.Handler, op, body string) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "TrentService."+op)
	c := e.NewContext(req, rec)
	_ = h.Handler()(c)

	return rec
}

// encodeBase64 returns the standard base64-encoded string for a byte slice.
func encodeBase64(data []byte) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	if len(data) == 0 {
		return ""
	}

	encoded := make([]byte, 0, ((len(data)+2)/3)*4)
	for i := 0; i < len(data); i += 3 {
		b0 := data[i]
		var b1, b2 byte
		if i+1 < len(data) {
			b1 = data[i+1]
		}
		if i+2 < len(data) {
			b2 = data[i+2]
		}
		encoded = append(
			encoded,
			alphabet[b0>>2],
			alphabet[((b0&0x3)<<4)|(b1>>4)],
			alphabet[((b1&0xF)<<2)|(b2>>6)],
			alphabet[b2&0x3F],
		)
	}
	// Add padding
	switch len(data) % 3 {
	case 1:
		encoded[len(encoded)-2] = '='
		encoded[len(encoded)-1] = '='
	case 2:
		encoded[len(encoded)-1] = '='
	}

	return string(encoded)
}

// sendKMSOp is a helper that posts a KMS JSON request to the handler and returns the response recorder.
func sendKMSOp(t *testing.T, h *kms.Handler, operation, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "TrentService."+operation)
	c := e.NewContext(req, rec)
	_ = h.Handler()(c)

	return rec
}

// newTestBackend creates a fresh InMemoryBackend using default account/region.
func newTestBackend() *kms.InMemoryBackend {
	return kms.NewInMemoryBackend()
}

// doKMSHTTPRequest is a test helper for issuing HTTP requests to the KMS handler.
func doKMSHTTPRequest(t *testing.T, h *kms.Handler, action, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "TrentService."+action)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	ctx := logger.Save(req.Context(), slog.Default())
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// postKMSOp is a helper that sends a KMS operation request and returns the response.
func postKMSOp(t *testing.T, h *kms.Handler, operation, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "TrentService."+operation)
	c := e.NewContext(req, rec)

	_ = h.Handler()(c)

	return rec
}

// buildBodyFromResourceID constructs a JSON body for an operation using the given resource ID.
func buildBodyFromResourceID(operation, resourceID string) string {
	if resourceID == "" {
		return `{}`
	}

	switch operation {
	case "ConnectCustomKeyStore", "DeleteCustomKeyStore", "DisconnectCustomKeyStore":
		body, _ := json.Marshal(map[string]string{"CustomKeyStoreId": resourceID})

		return string(body)
	case "GenerateMac":
		body, _ := json.Marshal(map[string]any{
			"KeyId":        resourceID,
			"MacAlgorithm": "HMAC_SHA_256",
			"Message":      []byte("test message"),
		})

		return string(body)
	case "GenerateDataKeyPair":
		body, _ := json.Marshal(map[string]any{
			"KeyId":       resourceID,
			"KeyPairSpec": "ECC_NIST_P256",
		})

		return string(body)
	case "GenerateDataKeyPairWithoutPlaintext":
		body, _ := json.Marshal(map[string]any{
			"KeyId":       resourceID,
			"KeyPairSpec": "ECC_NIST_P256",
		})

		return string(body)
	case "DeriveSharedSecret":
		// Need a peer public key; generate one in a fresh backend.
		peerBackend := kms.NewInMemoryBackend()
		peerKey, _ := peerBackend.CreateKey(context.Background(), &kms.CreateKeyInput{
			KeySpec:  "ECC_NIST_P256",
			KeyUsage: kms.KeyUsageKeyAgreement,
		})
		pubOut, _ := peerBackend.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{
			KeyID: peerKey.KeyMetadata.KeyID,
		})
		body, _ := json.Marshal(map[string]any{
			"KeyId":                 resourceID,
			"KeyAgreementAlgorithm": "ECDH",
			"PublicKey":             pubOut.PublicKey,
		})

		return string(body)
	}

	return `{}`
}

func ops2NewBackend(t *testing.T) *kms.InMemoryBackend {
	t.Helper()

	return kms.NewInMemoryBackend()
}

func ops2MustCreateSymKey(t *testing.T, b *kms.InMemoryBackend) string {
	t.Helper()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	return out.KeyMetadata.KeyID
}
