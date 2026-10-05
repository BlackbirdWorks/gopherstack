package s3

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryParam(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		url   string
		param string
		want  string
	}{
		{"no_query", "/b/k", "versionId", ""},
		{"present", "/b/k?versionId=v1", "versionId", "v1"},
		{"absent", "/b/k?other=1", "versionId", ""},
		{"empty_value", "/b/k?versionId=", "versionId", ""},
		{"escaped", "/b/k?versionId=a%2Bb", "versionId", "a+b"},
		{"first_wins", "/b/k?versionId=a&versionId=b", "versionId", "a"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodGet, tc.url, nil)
			assert.Equal(t, tc.want, queryParam(r, tc.param))
			assert.Equal(t, r.URL.Query().Get(tc.param), queryParam(r, tc.param))
		})
	}
}
