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

// TestDashboard_Kinesis_Index tests the Kinesis streams index page.
func TestDashboard_Kinesis_Index(t *testing.T) {
	t.Parallel()

	t.Run("empty stream list", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/kinesis", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "Kinesis")
	})

	t.Run("shows created stream", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create a stream via form POST
		req := httptest.NewRequest(http.MethodPost, "/dashboard/kinesis/create",
			strings.NewReader("stream_name=test-stream"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusFound, w.Code)

		// Verify stream appears in index
		req = httptest.NewRequest(http.MethodGet, "/dashboard/kinesis", nil)
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "test-stream")
	})

	t.Run("nil KinesisOps renders empty", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodGet, "/dashboard/kinesis", nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "Kinesis")
	})
}

// TestDashboard_Kinesis_CreateStream tests creating a Kinesis stream via dashboard.
func TestDashboard_Kinesis_CreateStream(t *testing.T) {
	t.Parallel()

	t.Run("create stream successfully", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/kinesis/create",
			strings.NewReader("stream_name=my-stream"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusFound, w.Code)
		assert.Equal(t, "/dashboard/kinesis", w.Header().Get("Location"))
	})

	t.Run("empty stream name returns 400", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/kinesis/create",
			strings.NewReader("stream_name="))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("nil KinesisOps returns 503", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodPost, "/dashboard/kinesis/create",
			strings.NewReader("stream_name=test"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}

// TestDashboard_Kinesis_DeleteStream tests deleting a Kinesis stream via dashboard.
func TestDashboard_Kinesis_DeleteStream(t *testing.T) {
	t.Parallel()

	t.Run("delete existing stream", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create first
		req := httptest.NewRequest(http.MethodPost, "/dashboard/kinesis/create",
			strings.NewReader("stream_name=delete-me"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusFound, w.Code)

		// Delete via DELETE method
		req = httptest.NewRequest(http.MethodDelete, "/dashboard/kinesis/delete?name=delete-me", nil)
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusFound, w.Code)
		assert.Equal(t, "/dashboard/kinesis", w.Header().Get("Location"))
	})

	t.Run("nil KinesisOps returns 503", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/kinesis/delete?name=test", nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}

// TestDashboard_Kinesis_StreamDetail tests the stream detail page.
func TestDashboard_Kinesis_StreamDetail(t *testing.T) {
	t.Parallel()

	t.Run("detail page for existing stream", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create stream first
		req := httptest.NewRequest(http.MethodPost, "/dashboard/kinesis/create",
			strings.NewReader("stream_name=detail-stream"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusFound, w.Code)

		// Get detail page
		req = httptest.NewRequest(http.MethodGet, "/dashboard/kinesis/stream?name=detail-stream", nil)
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "detail-stream")
	})

	t.Run("detail page for non-existent stream returns 404", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/kinesis/stream?name=nonexistent", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("nil KinesisOps returns 503", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodGet, "/dashboard/kinesis/stream?name=test", nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}

// TestDashboard_Kinesis_PutRecord tests the put record form.
func TestDashboard_Kinesis_PutRecord(t *testing.T) {
	t.Parallel()

	t.Run("put record to existing stream", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create stream first
		req := httptest.NewRequest(http.MethodPost, "/dashboard/kinesis/create",
			strings.NewReader("stream_name=record-stream"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusFound, w.Code)

		// Put record
		req = httptest.NewRequest(http.MethodPost, "/dashboard/kinesis/record",
			strings.NewReader("stream_name=record-stream&partition_key=pk&data=hello"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusFound, w.Code)
		assert.Equal(t, "/dashboard/kinesis/stream?name=record-stream", w.Header().Get("Location"))
	})

	t.Run("put record with default partition key", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create stream
		req := httptest.NewRequest(http.MethodPost, "/dashboard/kinesis/create",
			strings.NewReader("stream_name=pk-stream"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusFound, w.Code)

		// Put record without partition key (should default)
		req = httptest.NewRequest(http.MethodPost, "/dashboard/kinesis/record",
			strings.NewReader("stream_name=pk-stream&data=test"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusFound, w.Code)
	})

	t.Run("nil KinesisOps returns 503", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodPost, "/dashboard/kinesis/record",
			strings.NewReader("stream_name=test&data=hello"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}
