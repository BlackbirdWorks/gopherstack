package azurefile_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/azurefile"
)

type env struct {
	h    *azurefile.Handler
	svc  *service.Client
	cred *service.SharedKeyCredential
	url  string
}

type envOpt func(*azurefile.Handler)

func newEnv(t *testing.T, opts ...envOpt) *env {
	t.Helper()

	h := azurefile.NewHandler(azurefile.NewInMemoryBackend())
	for _, o := range opts {
		o(h)
	}

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cred, err := service.NewSharedKeyCredential(azureauth.DefaultAccountName, azureauth.DefaultAccountKey)
	require.NoError(t, err)

	base := srv.URL + "/" + azureauth.DefaultAccountName + "/"
	svc, err := service.NewClientWithSharedKeyCredential(base, cred, nil)
	require.NoError(t, err)

	return &env{h: h, svc: svc, cred: cred, url: srv.URL}
}

func ctx(t *testing.T) context.Context {
	t.Helper()

	return t.Context()
}

func requireCode(t *testing.T, err error, code fileerror.Code) {
	t.Helper()
	require.Error(t, err)
	require.True(t, fileerror.HasCode(err, code), "want %s, got %v", code, err)
}

func rawDo(t *testing.T, method, url, host string, hdr map[string]string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, nil)
	require.NoError(t, err)

	if host != "" {
		req.Host = host
	}

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}

// setShareRetention enables share soft delete for the development account.
func (e *env) setShareRetention(_ *testing.T, days int) {
	e.h.Backend.SetShareRetention(azureauth.DefaultAccountName, true, int32(days))
}

type azurefileHandler = azurefile.Handler

type readSeekCloser struct{ *strings.Reader }

func (readSeekCloser) Close() error { return nil }

func newReadSeekCloser(s string) io.ReadSeekCloser { return readSeekCloser{strings.NewReader(s)} }

func downloadAll(t *testing.T, fc *file.Client) string {
	t.Helper()

	resp, err := fc.DownloadStream(t.Context(), nil)
	require.NoError(t, err)

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

// md reads a metadata value case-insensitively: response headers keep the
// stored case while list XML is lowercased by the SDK.
func md(m map[string]*string, key string) string {
	for k, v := range m {
		if strings.EqualFold(k, key) && v != nil {
			return *v
		}
	}

	return ""
}

func itoa(n int) string { return strconv.Itoa(n) }
