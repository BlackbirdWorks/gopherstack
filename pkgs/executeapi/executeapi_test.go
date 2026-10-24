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
