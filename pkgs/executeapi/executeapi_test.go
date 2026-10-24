package executeapi_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/executeapi"
)

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		host     string
		path     string
		wantID   string
		wantRest string
		wantOK   bool
	}{
		{
			"localstack host",
			"abc123.execute-api.localhost.localstack.cloud:4566",
			"/dev/pets",
			"abc123",
			"/dev/pets",
			true,
		},
		{"aws host", "abc123.execute-api.us-east-1.amazonaws.com", "/dev", "abc123", "/dev", true},
		{
			"internal path",
			"localhost:4566",
			"/_aws/execute-api/abc123/dev/pets/1",
			"abc123",
			"/dev/pets/1",
			true,
		},
		{"internal path no tail", "localhost:4566", "/_aws/execute-api/abc123", "abc123", "/", true},
		{"internal path empty id", "localhost:4566", "/_aws/execute-api/", "", "", false},
		{"plain host", "localhost:4566", "/restapis", "", "", false},
		{"nested label", "a.b.execute-api.localhost", "/x", "", "", false},
		{"no domain after label", "abc.execute-api.", "/x", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := &http.Request{Host: tt.host, URL: &url.URL{Path: tt.path}}
			id, rest, ok := executeapi.Parse(r)

			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantID, id)
			assert.Equal(t, tt.wantRest, rest)
		})
	}
}

func TestMatchBasePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		wantKey  string
		wantRest string
		keys     []string
		wantOK   bool
	}{
		{"single", "/v1/pets", "v1", "/pets", []string{"v1"}, true},
		{"exact", "/v1", "v1", "/", []string{"v1"}, true},
		{"segment boundary", "/v10/pets", "", "", []string{"v1"}, false},
		{"longest wins", "/a/b/c", "a/b", "/c", []string{"a", "a/b", "(none)"}, true},
		{"none fallback", "/zzz/x", "(none)", "/zzz/x", []string{"a", "(none)"}, true},
		{"empty key root", "/x", "", "/x", []string{""}, true},
		{"specific beats root", "/a/x", "a", "/x", []string{"", "a"}, true},
		{"no match", "/q", "", "", []string{"a"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key, rest, ok := executeapi.MatchBasePath(tt.keys, tt.path)
			assert.Equal(t, tt.wantOK, ok)

			if tt.wantOK {
				assert.Equal(t, tt.wantKey, key)
				assert.Equal(t, tt.wantRest, rest)
			}
		})
	}
}

func TestHostName(t *testing.T) {
	t.Parallel()

	for host, want := range map[string]string{
		"Test.Example.com:4566": "test.example.com",
		"test.example.com":      "test.example.com",
	} {
		assert.Equal(t, want, executeapi.HostName(&http.Request{Host: host}))
	}
}
