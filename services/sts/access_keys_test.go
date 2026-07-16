package sts_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

// TestGetAccessKeyInfo verifies the GetAccessKeyInfo action.
func TestGetAccessKeyInfo(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()
	h := sts.NewHandler(backend)
	e := echo.New()

	// Obtain a real session key first so GetAccessKeyInfo can look it up.
	sessionOut, err := backend.GetSessionToken(&sts.GetSessionTokenInput{DurationSeconds: 3600})
	require.NoError(t, err)

	accessKeyID := sessionOut.GetSessionTokenResult.Credentials.AccessKeyID

	form := url.Values{
		"Action":      {"GetAccessKeyInfo"},
		"Version":     {"2011-06-15"},
		"AccessKeyId": {accessKeyID},
	}

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	ctxWithLogger := logger.Save(req.Context(), nil)
	req = req.WithContext(ctxWithLogger)

	err = h.Handler()(e.NewContext(req, rec))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"GetAccessKeyInfoResponse"`
		Result  struct {
			Account string `xml:"Account"`
		} `xml:"GetAccessKeyInfoResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, sts.MockAccountID, resp.Result.Account)
}

// TestGetAccessKeyInfo_WellFormedUnknownKey verifies that a well-formed key not in any session
// returns 200 with the backend account ID (Gap #14: AWS derives account from key prefix encoding).
func TestGetAccessKeyInfo_WellFormedUnknownKey(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()
	h := sts.NewHandler(backend)
	e := echo.New()

	form := url.Values{
		"Action":      {"GetAccessKeyInfo"},
		"Version":     {"2011-06-15"},
		"AccessKeyId": {"ASIAIOSFODNN7EXAMPLE"},
	}

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	ctxWithLogger := logger.Save(req.Context(), nil)
	req = req.WithContext(ctxWithLogger)

	err := h.Handler()(e.NewContext(req, rec))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"GetAccessKeyInfoResponse"`
		Result  struct {
			Account string `xml:"Account"`
		} `xml:"GetAccessKeyInfoResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, sts.MockAccountID, resp.Result.Account)
}

// TestGetAccessKeyInfo_MalformedKey verifies that a completely malformed key returns ValidationError.
func TestGetAccessKeyInfo_MalformedKey(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()
	h := sts.NewHandler(backend)
	e := echo.New()

	form := url.Values{
		"Action":      {"GetAccessKeyInfo"},
		"Version":     {"2011-06-15"},
		"AccessKeyId": {"not-a-real-key"},
	}

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	ctxWithLogger := logger.Save(req.Context(), nil)
	req = req.WithContext(ctxWithLogger)

	err := h.Handler()(e.NewContext(req, rec))
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sts.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ValidationError", errResp.Error.Code)
}

// TestGetAccessKeyInfo_EmptyKey verifies that an empty AccessKeyId returns ValidationError.
func TestGetAccessKeyInfo_EmptyKey(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()
	h := sts.NewHandler(backend)
	e := echo.New()

	form := url.Values{
		"Action":      {"GetAccessKeyInfo"},
		"Version":     {"2011-06-15"},
		"AccessKeyId": {""},
	}

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	ctxWithLogger := logger.Save(req.Context(), nil)
	req = req.WithContext(ctxWithLogger)

	err := h.Handler()(e.NewContext(req, rec))
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sts.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ValidationError", errResp.Error.Code)
}

// TestGetAccessKeyInfoWithAssumedRole verifies GetAccessKeyInfo uses the assumed-role account.
func TestGetAccessKeyInfoWithAssumedRole(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	b.AddSessionInternal(&sts.SessionInfo{
		AccessKeyID:    "ASIATEST000000000004",
		AssumedRoleArn: "arn:aws:sts::111111111111:assumed-role/test/session",
		AccountID:      "111111111111",
		SessionName:    "session",
		Expiration:     time.Now().Add(time.Hour),
	})

	rec := r1PostForm(t, sts.NewHandler(b), url.Values{
		"Action":      {"GetAccessKeyInfo"},
		"Version":     {"2011-06-15"},
		"AccessKeyId": {"ASIATEST000000000004"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var result sts.GetAccessKeyInfoResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.Equal(t, "111111111111", result.GetAccessKeyInfoResult.Account)
}

// TestGetAccessKeyInfoWellFormedPrefixes verifies well-formed keys of every
// known prefix decode to an account, and malformed keys are rejected
// (Accuracy Gap #14).
func TestGetAccessKeyInfoWellFormedPrefixes(t *testing.T) {
	t.Parallel()

	wellFormedPrefixes := []string{"AKIA", "ASIA", "AIDA", "AROA", "AGPA"}

	for _, prefix := range wellFormedPrefixes {
		t.Run("well_formed_"+prefix+"_returns_account", func(t *testing.T) {
			t.Parallel()

			h, _, e := accuracyHandler(t)
			key := prefix + "ABCDEFGHIJ123456"
			form := url.Values{
				"Action":      {"GetAccessKeyInfo"},
				"Version":     {"2011-06-15"},
				"AccessKeyId": {key},
			}
			rec := accuracyPost(t, h, e, form)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"GetAccessKeyInfoResponse"`
				Result  struct {
					Account string `xml:"Account"`
				} `xml:"GetAccessKeyInfoResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.Result.Account)
		})
	}

	t.Run("completely_malformed_key_returns_validation_error", func(t *testing.T) {
		t.Parallel()

		h, _, e := accuracyHandler(t)
		form := url.Values{
			"Action":      {"GetAccessKeyInfo"},
			"Version":     {"2011-06-15"},
			"AccessKeyId": {"badkey"},
		}
		rec := accuracyPost(t, h, e, form)
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		errResp := decodeError(t, rec.Body.Bytes())
		assert.Equal(t, "ValidationError", errResp.Error.Code)
	})

	t.Run("issued_key_returns_correct_account", func(t *testing.T) {
		t.Parallel()

		h, b, e := accuracyHandler(t)
		assumeResp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::987654321098:role/TestRole",
			RoleSessionName: "sess",
		})
		require.NoError(t, err)

		key := assumeResp.AssumeRoleResult.Credentials.AccessKeyID
		form := url.Values{
			"Action":      {"GetAccessKeyInfo"},
			"Version":     {"2011-06-15"},
			"AccessKeyId": {key},
		}
		rec := accuracyPost(t, h, e, form)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			XMLName xml.Name `xml:"GetAccessKeyInfoResponse"`
			Result  struct {
				Account string `xml:"Account"`
			} `xml:"GetAccessKeyInfoResult"`
		}
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "987654321098", resp.Result.Account)
	})
}
