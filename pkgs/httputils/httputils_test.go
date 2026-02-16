package httputils_test

import (
	"Gopherstack/pkgs/httputils"
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

var (
	errWrite = errors.New("write error")
	errTest  = errors.New("test error")
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
			got, err := httputils.ReadBody(req)
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
		httputils.DrainBody(req)
	})

	t.Run("populated body", func(t *testing.T) {
		t.Parallel()
		body := io.NopCloser(bytes.NewReader([]byte("test data")))
		req := &http.Request{Body: body}
		httputils.DrainBody(req)
	})
}

func TestWriteError(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)

	httputils.WriteError(logger, rec, req, errTest, http.StatusBadRequest)

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

		httputils.WriteJSON(nil, rec, http.StatusOK, payload)

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

		// A channel cannot be marshaled to JSON.
		httputils.WriteJSON(logger, rec, http.StatusOK, make(chan int))

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Contains(t, buf.String(), "failed to marshal JSON response")
	})

	t.Run("write error", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		mw := &errResponseWriter{}

		httputils.WriteJSON(logger, mw, http.StatusOK, map[string]string{"foo": "bar"})
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

		httputils.WriteXML(nil, rec, http.StatusOK, payload)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/xml", rec.Header().Get("Content-Type"))
		assert.Contains(t, rec.Body.String(), xml.Header)

		var got testXML
		err := xml.Unmarshal(rec.Body.Bytes(), &got)
		require.NoError(t, err)
		assert.Equal(t, payload.Foo, got.Foo)
	})

	t.Run("write error", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		mw := &errResponseWriter{}

		httputils.WriteXML(logger, mw, http.StatusOK, testXML{Foo: "bar"})
		assert.Contains(t, buf.String(), "failed to write XML response")
	})

	t.Run("encode error", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		rec := httptest.NewRecorder()

		// A channel cannot be marshaled to XML.
		httputils.WriteXML(logger, rec, http.StatusOK, make(chan int))
		assert.Contains(t, buf.String(), "failed to marshal XML response")
	})
}

type errResponseWriter struct {
	httptest.ResponseRecorder
}

func (e *errResponseWriter) Write(_ []byte) (int, error) {
	return 0, errWrite
}

func (e *errResponseWriter) WriteHeader(_ int) {}

func (e *errResponseWriter) Header() http.Header { return make(http.Header) }
