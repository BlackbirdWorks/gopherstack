package httputils_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func TestSignedForOtherService(t *testing.T) {
	t.Parallel()

	const credFmt = "AWS4-HMAC-SHA256 Credential=AKIA/20260101/us-east-1/%s/aws4_request, SignedHeaders=host, Signature=00"

	tests := []struct {
		name  string
		auth  string
		names []string
		want  bool
	}{
		{name: "unsigned", auth: "", names: []string{"batch"}, want: false},
		{name: "same", auth: "batch", names: []string{"batch"}, want: false},
		{name: "other", auth: "signin", names: []string{"batch"}, want: true},
		{name: "any_of", auth: "bedrock", names: []string{"bedrock", "bedrock-agent"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.auth != "" {
				r.Header.Set("Authorization", fmt.Sprintf(credFmt, tt.auth))
			}

			assert.Equal(t, tt.want, httputils.SignedForOtherService(r, tt.names...))
		})
	}
}
