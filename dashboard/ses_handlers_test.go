package dashboard_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
)

// TestDashboard_SES_Index tests the SES inbox page.
func TestDashboard_SES_Index(t *testing.T) {
	t.Parallel()

	t.Run("empty inbox", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/ses", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "SES Inbox")
	})

	t.Run("shows sent email", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		_, err := stack.SESHandler.Backend.SendEmail(
			"sender@example.com",
			[]string{"recv@example.com"},
			"Test Subject",
			"<b>Hello</b>",
			"Hello",
		)
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/ses", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "Test Subject")
		assert.Contains(t, w.Body.String(), "sender@example.com")
	})

	t.Run("shows verified identities", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		err := stack.SESHandler.Backend.VerifyEmailIdentity("alice@example.com")
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/ses", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "alice@example.com")
	})

	t.Run("nil SESOps renders empty", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodGet, "/dashboard/ses", nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "SES Inbox")
	})
}

// TestDashboard_SES_EmailDetail tests the email detail page.
func TestDashboard_SES_EmailDetail(t *testing.T) {
	t.Parallel()

	t.Run("view email detail", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		msgID, err := stack.SESHandler.Backend.SendEmail(
			"sender@example.com",
			[]string{"recv@example.com"},
			"Hello Detail",
			"<b>HTML body</b>",
			"plain text body",
		)
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/ses/email?id="+msgID, nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "Hello Detail")
		assert.Contains(t, w.Body.String(), "sender@example.com")
	})

	t.Run("email not found returns 404", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/ses/email?id=nonexistent", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("missing id returns 400", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/ses/email", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("nil SESOps returns 404", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodGet, "/dashboard/ses/email?id=ses-abc", nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})
}

// TestDashboard_SES_VerifyIdentity tests the verify identity POST handler.
func TestDashboard_SES_VerifyIdentity(t *testing.T) {
	t.Parallel()

	t.Run("verify identity redirects", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/ses/identity/verify",
			strings.NewReader("identity=test@example.com"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusFound, w.Code)

		// Check identity now appears in index.
		req2 := httptest.NewRequest(http.MethodGet, "/dashboard/ses", nil)
		w2 := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w2, req2)
		assert.Contains(t, w2.Body.String(), "test@example.com")
	})

	t.Run("empty identity returns 400", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/ses/identity/verify",
			strings.NewReader("identity="))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("nil SESOps returns 503", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodPost, "/dashboard/ses/identity/verify",
			strings.NewReader("identity=test@example.com"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}

// TestDashboard_SES_DeleteIdentity tests the delete identity POST handler.
func TestDashboard_SES_DeleteIdentity(t *testing.T) {
	t.Parallel()

	t.Run("delete identity redirects", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Verify first.
		err := stack.SESHandler.Backend.VerifyEmailIdentity("del@example.com")
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/ses/identity/delete",
			strings.NewReader("identity=del@example.com"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusFound, w.Code)

		// Verify identity is gone.
		identities := stack.SESHandler.Backend.ListIdentities("", 0).Data
		assert.NotContains(t, identities, "del@example.com")
	})

	t.Run("delete non-existent identity returns 404", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/ses/identity/delete",
			strings.NewReader("identity=nobody@example.com"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("empty identity returns 400", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/ses/identity/delete",
			strings.NewReader("identity="))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("nil SESOps returns 503", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodPost, "/dashboard/ses/identity/delete",
			strings.NewReader("identity=test@example.com"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}
