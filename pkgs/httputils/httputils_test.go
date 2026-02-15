package httputils

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    io.ReadCloser
		want    []byte
		wantErr bool
	}{
		{
			name: "nil body",
			body: nil,
			want: nil,
		},
		{
			name: "empty body",
			body: io.NopCloser(bytes.NewReader([]byte(""))),
			want: []byte(""),
		},
		{
			name: "populated body",
			body: io.NopCloser(bytes.NewReader([]byte("test data"))),
			want: []byte("test data"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := &http.Request{Body: tt.body}
			got, err := ReadBody(req)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestDrainBody(t *testing.T) {
	t.Parallel()

	t.Run("nil body", func(t *testing.T) {
		t.Parallel()
		req := &http.Request{Body: nil}
		DrainBody(req)
	})

	t.Run("populated body", func(t *testing.T) {
		t.Parallel()
		body := io.NopCloser(bytes.NewReader([]byte("test data")))
		req := &http.Request{Body: body}
		DrainBody(req)
		// Body should be closed and drained
	})
}

func TestWriteError(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	err := errors.New("test error")

	WriteError(logger, rec, req, err, http.StatusBadRequest)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "test error")
	assert.Contains(t, buf.String(), "request failed")
}

func TestWriteJSON(t *testing.T) {
	t.Parallel()

	t.Run("successful write", func(t *testing.T) {
		t.Parallel()
		rec := httptest.NewRecorder()
		payload := map[string]string{"foo": "bar"}

		WriteJSON(nil, rec, http.StatusOK, payload)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

		var got map[string]string
		err := json.Unmarshal(rec.Body.Bytes(), &got)
		require.NoError(t, err)
		assert.Equal(t, payload, got)
	})

	t.Run("marshal error", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		rec := httptest.NewRecorder()
		// channel cannot be marshaled to JSON
		payload := make(chan int)

		WriteJSON(logger, rec, http.StatusOK, payload)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Contains(t, buf.String(), "failed to marshal JSON response")
	})

	t.Run("write error", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		mw := &errResponseWriter{}

		WriteJSON(logger, mw, http.StatusOK, map[string]string{"foo": "bar"})
		assert.Contains(t, buf.String(), "failed to write JSON response")
	})
}

type testXML struct {
	XMLName xml.Name `xml:"test"`
	Foo     string   `xml:"foo"`
}

func TestWriteXML(t *testing.T) {
	t.Parallel()

	t.Run("successful write", func(t *testing.T) {
		t.Parallel()
		rec := httptest.NewRecorder()
		payload := testXML{Foo: "bar"}

		WriteXML(nil, rec, http.StatusOK, payload)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/xml", rec.Header().Get("Content-Type"))
		assert.Contains(t, rec.Body.String(), xml.Header)

		var got testXML
		err := xml.Unmarshal(rec.Body.Bytes(), &got)
		require.NoError(t, err)
		assert.Equal(t, payload.Foo, got.Foo)
	})

	t.Run("header write error", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		mw := &errResponseWriter{}

		WriteXML(logger, mw, http.StatusOK, testXML{Foo: "bar"})
		assert.Contains(t, buf.String(), "failed to write XML header")
	})

	t.Run("encode error", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		// A ResponseWriter that succeeds for the header but fails for the payload
		mw := &headerSuccessErrorWriter{}

		WriteXML(logger, mw, http.StatusOK, testXML{Foo: "bar"})
		assert.Contains(t, buf.String(), "failed to write XML response")
	})
}

type errResponseWriter struct {
	httptest.ResponseRecorder
}

func (e *errResponseWriter) Write(b []byte) (int, error) {
	return 0, errors.New("write error")
}

func (e *errResponseWriter) WriteHeader(statusCode int) {}
func (e *errResponseWriter) Header() http.Header        { return make(http.Header) }

type headerSuccessErrorWriter struct {
	writeCount int
	httptest.ResponseRecorder
}

func (e *headerSuccessErrorWriter) Write(b []byte) (int, error) {
	e.writeCount++
	if e.writeCount == 1 {
		return len(b), nil // Header succeeds
	}
	return 0, errors.New("encode error")
}

func (e *headerSuccessErrorWriter) WriteHeader(statusCode int) {}
func (e *headerSuccessErrorWriter) Header() http.Header        { return make(http.Header) }
