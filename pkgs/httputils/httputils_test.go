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
		body    io.ReadCloser
		name    string
		want    []byte
		wantErr bool
	}{
		{
			body: nil,
			name: "nil body",
			want: nil,
		},
		{
			body: io.NopCloser(bytes.NewReader([]byte(""))),
			name: "empty body",
			want: []byte(""),
		},
		{
			body: io.NopCloser(bytes.NewReader([]byte("test data"))),
			name: "populated body",
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

	t.Run("multiple reads", func(t *testing.T) {
		t.Parallel()
		data := []byte("multiple reads data")
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(data))

		// First read
		got1, err := httputils.ReadBody(req)
		require.NoError(t, err)
		assert.Equal(t, data, got1)

		// Second read
		got2, err := httputils.ReadBody(req)
		require.NoError(t, err)
		assert.Equal(t, data, got2)
	})
}

func TestDrainBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body io.ReadCloser
		name string
	}{
		{
			body: nil,
			name: "nil body",
		},
		{
			body: io.NopCloser(bytes.NewReader([]byte("test data"))),
			name: "populated body",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := &http.Request{Body: tt.body}
			httputils.DrainBody(req)
		})
	}
}

func TestWriteError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err        error
		name       string
		statusCode int
	}{
		{
			err:        errTest,
			name:       "bad request",
			statusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			rec := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/test", nil)

			httputils.WriteError(logger, rec, req, tt.err, tt.statusCode)

			assert.Equal(t, tt.statusCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.err.Error())
			assert.Contains(t, buf.String(), "request failed")
		})
	}
}

func TestWriteJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFunc  func() (http.ResponseWriter, *bytes.Buffer, *slog.Logger)
		checkFunc  func(t *testing.T, rec http.ResponseWriter, buf *bytes.Buffer)
		payload    any
		name       string
		statusCode int
	}{
		{
			checkFunc: func(t *testing.T, rec http.ResponseWriter, _ *bytes.Buffer) {
				t.Helper()
				res := rec.(*httptest.ResponseRecorder)
				assert.Equal(t, http.StatusOK, res.Code)
				assert.Equal(t, "application/json", res.Header().Get("Content-Type"))

				var got map[string]string
				err := json.Unmarshal(res.Body.Bytes(), &got)
				require.NoError(t, err)
				assert.Equal(t, map[string]string{"foo": "bar"}, got)
			},
			payload:    map[string]string{"foo": "bar"},
			name:       "successful write",
			statusCode: http.StatusOK,
		},
		{
			checkFunc: func(t *testing.T, rec http.ResponseWriter, buf *bytes.Buffer) {
				t.Helper()
				res := rec.(*httptest.ResponseRecorder)
				assert.Equal(t, http.StatusInternalServerError, res.Code)
				assert.Contains(t, buf.String(), "failed to marshal JSON response")
			},
			payload:    make(chan int),
			name:       "marshal error",
			statusCode: http.StatusOK,
		},
		{
			setupFunc: func() (http.ResponseWriter, *bytes.Buffer, *slog.Logger) {
				var buf bytes.Buffer
				logger := slog.New(slog.NewTextHandler(&buf, nil))

				return &errResponseWriter{}, &buf, logger
			},
			checkFunc: func(t *testing.T, _ http.ResponseWriter, buf *bytes.Buffer) {
				t.Helper()
				assert.Contains(t, buf.String(), "failed to write JSON response")
			},
			payload:    map[string]string{"foo": "bar"},
			name:       "write error",
			statusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			buf := &bytes.Buffer{}
			logger := slog.New(slog.NewTextHandler(buf, nil))

			if tt.setupFunc != nil {
				r, b, l := tt.setupFunc()
				httputils.WriteJSON(l, r, tt.statusCode, tt.payload)
				tt.checkFunc(t, r, b)
			} else {
				httputils.WriteJSON(logger, rec, tt.statusCode, tt.payload)
				tt.checkFunc(t, rec, buf)
			}
		})
	}
}

type testXML struct {
	XMLName xml.Name `xml:"test"`
	Foo     string   `xml:"foo"`
}

func TestWriteXML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFunc  func() (http.ResponseWriter, *bytes.Buffer, *slog.Logger)
		checkFunc  func(t *testing.T, _ http.ResponseWriter, buf *bytes.Buffer)
		payload    any
		name       string
		statusCode int
	}{
		{
			checkFunc: func(t *testing.T, rec http.ResponseWriter, _ *bytes.Buffer) {
				t.Helper()
				res := rec.(*httptest.ResponseRecorder)
				assert.Equal(t, http.StatusOK, res.Code)
				assert.Equal(t, "application/xml", res.Header().Get("Content-Type"))
				assert.Contains(t, res.Body.String(), xml.Header)

				var got testXML
				err := xml.Unmarshal(res.Body.Bytes(), &got)
				require.NoError(t, err)
				assert.Equal(t, "bar", got.Foo)
			},
			payload:    testXML{Foo: "bar"},
			name:       "successful write",
			statusCode: http.StatusOK,
		},
		{
			setupFunc: func() (http.ResponseWriter, *bytes.Buffer, *slog.Logger) {
				var buf bytes.Buffer
				logger := slog.New(slog.NewTextHandler(&buf, nil))

				return &errResponseWriter{}, &buf, logger
			},
			checkFunc: func(t *testing.T, _ http.ResponseWriter, buf *bytes.Buffer) {
				t.Helper()
				assert.Contains(t, buf.String(), "failed to write XML response")
			},
			payload:    testXML{Foo: "bar"},
			name:       "write error",
			statusCode: http.StatusOK,
		},
		{
			checkFunc: func(t *testing.T, _ http.ResponseWriter, buf *bytes.Buffer) {
				t.Helper()
				assert.Contains(t, buf.String(), "failed to marshal XML response")
			},
			payload:    make(chan int),
			name:       "encode error",
			statusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			buf := &bytes.Buffer{}
			logger := slog.New(slog.NewTextHandler(buf, nil))

			if tt.setupFunc != nil {
				r, b, l := tt.setupFunc()
				httputils.WriteXML(l, r, tt.statusCode, tt.payload)
				tt.checkFunc(t, r, b)
			} else {
				httputils.WriteXML(logger, rec, tt.statusCode, tt.payload)
				tt.checkFunc(t, rec, buf)
			}
		})
	}
}

type errResponseWriter struct {
	httptest.ResponseRecorder
}

func (e *errResponseWriter) Write(_ []byte) (int, error) {
	return 0, errWrite
}

func (e *errResponseWriter) WriteHeader(_ int) {}

func (e *errResponseWriter) Header() http.Header { return make(http.Header) }
