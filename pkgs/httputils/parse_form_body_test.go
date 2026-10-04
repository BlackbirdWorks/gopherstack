package httputils_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func TestParseFormBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		wantKey string
		wantVal string
		wantErr bool
	}{
		{name: "simple", body: "Action=ListQueues&Version=2012-11-05", wantKey: "Version", wantVal: "2012-11-05"},
		{name: "empty", body: "", wantKey: "Action", wantVal: ""},
		{name: "bad escape", body: "Action=%zz&Version=1", wantKey: "Version", wantVal: "1", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))

			for range 2 {
				vals, err := httputils.ParseFormBody(r)
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}

				assert.Equal(t, tt.wantVal, vals.Get(tt.wantKey))
			}

			rest, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.body, string(rest))
		})
	}
}

func TestParseFormBodyNilBody(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Body = nil

	vals, err := httputils.ParseFormBody(r)
	require.NoError(t, err)
	assert.Empty(t, vals)
}
