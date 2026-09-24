package sts_test

// Benchmarks for the gopherstack perf sweep's STS investigation (2026-09-24).
//
// The fresh profile showed sts's own (non-shared-middleware) Handler cost was
// almost entirely storeSession -> maybeEvictExpiredSessions ->
// evictExpiredSessionsLocked: once the sessions table crosses
// sessionEvictThreshold, EVERY credential-issuing call does a full O(n) Range
// scan for expired sessions, not just an amortized fraction of them -- this
// never lets up for a long-running server under sustained load, which is
// exactly the preload state BenchmarkAssumeRole and BenchmarkGetSessionToken
// below put the backend in.
//
// To regenerate goldens: STS_GOLDEN_UPDATE=1 go test -run TestGolden ./services/sts/...

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// preloadSweepThreshold sessions puts the backend's session count above
// store.go's sessionEvictThreshold (256) before the timer starts, so every
// benchmarked call runs with the opportunistic eviction sweep armed --
// matching a long-running server under sustained load rather than a
// freshly booted one.
const preloadSweepThreshold = 5000

func stsPostB(tb testing.TB, h *sts.Handler, form url.Values) *httptest.ResponseRecorder {
	tb.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(tb, h.Handler()(c))

	return rec
}

func preloadSessions(tb testing.TB, backend *sts.InMemoryBackend, n int) {
	tb.Helper()

	for range n {
		_, err := backend.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/PreloadRole",
			RoleSessionName: "preload-session",
		})
		require.NoError(tb, err)
	}
}

// BenchmarkAssumeRole runs AssumeRole through the full HTTP handler against a
// backend preloaded past sessionEvictThreshold.
func BenchmarkAssumeRole(b *testing.B) {
	backend := sts.NewInMemoryBackend()
	preloadSessions(b, backend, preloadSweepThreshold)
	h := sts.NewHandler(backend)

	form := url.Values{
		"Action":          {"AssumeRole"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::123456789012:role/BenchRole"},
		"RoleSessionName": {"bench-session"},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rec := stsPostB(b, h, form)
		if rec.Code != http.StatusOK {
			b.Fatalf("AssumeRole failed: %d %s", rec.Code, rec.Body.String())
		}
	}
}

// BenchmarkGetSessionToken runs GetSessionToken through the full HTTP handler
// against a backend preloaded past sessionEvictThreshold. GetSessionToken
// mints a session (storeSession) exactly like AssumeRole, so it hits the same
// eviction sweep.
func BenchmarkGetSessionToken(b *testing.B) {
	backend := sts.NewInMemoryBackend()
	preloadSessions(b, backend, preloadSweepThreshold)
	h := sts.NewHandler(backend)

	form := url.Values{
		"Action":  {"GetSessionToken"},
		"Version": {"2011-06-15"},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rec := stsPostB(b, h, form)
		if rec.Code != http.StatusOK {
			b.Fatalf("GetSessionToken failed: %d %s", rec.Code, rec.Body.String())
		}
	}
}

// BenchmarkGetCallerIdentity runs GetCallerIdentity through the full HTTP
// handler. GetCallerIdentity is read-only (no storeSession call) so it acts
// as a control: it should be unaffected by any session-eviction change.
func BenchmarkGetCallerIdentity(b *testing.B) {
	backend := sts.NewInMemoryBackend()
	h := sts.NewHandler(backend)

	form := url.Values{
		"Action":  {"GetCallerIdentity"},
		"Version": {"2011-06-15"},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rec := stsPostB(b, h, form)
		if rec.Code != http.StatusOK {
			b.Fatalf("GetCallerIdentity failed: %d %s", rec.Code, rec.Body.String())
		}
	}
}
