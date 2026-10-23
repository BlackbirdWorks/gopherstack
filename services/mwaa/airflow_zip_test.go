package mwaa_test

import (
	"archive/zip"
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

func buildZip(t *testing.T, names ...string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for _, n := range names {
		w, err := zw.Create(n)
		require.NoError(t, err)

		_, err = w.Write([]byte("x"))
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func TestUnzipFilesContainment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		entries []string
		wantErr bool
	}{
		{name: "plain", entries: []string{"a.py", "sub/b.py"}},
		{name: "dotdot", entries: []string{"../evil.py"}, wantErr: true},
		{name: "nested dotdot", entries: []string{"a/../../evil.py"}, wantErr: true},
		{name: "absolute", entries: []string{"/etc/evil.py"}, wantErr: true},
		{name: "backslash", entries: []string{`..\evil.py`}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			files, err := mwaa.UnzipFiles(buildZip(t, tt.entries...))
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, files, len(tt.entries))
		})
	}
}

func TestIsHTTPSRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		proto string
		want  bool
	}{
		{name: "plain", want: false},
		{name: "forwarded https", proto: "https", want: true},
		{name: "forwarded http", proto: "http", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.proto != "" {
				r.Header.Set("X-Forwarded-Proto", tt.proto)
			}

			assert.Equal(t, tt.want, mwaa.IsHTTPSRequest(r))
		})
	}
}
