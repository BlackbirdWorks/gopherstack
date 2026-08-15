package appconfigdata_test

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real AppConfigData
// operation, extracted from appconfigdata@v1.26.4 serializers.go: each
// entry's "request.Method" and the string passed to httpbinding.SplitURI in
// that op's awsRestjson1_serializeOp<Op>.HandleSerialize. Both paths are
// fixed literals with no {label} member (the session/config identity
// travels in the body/query, never the path). 2 real ops here, matching
// AppConfigData's real op count exactly.
//
// The only two ops share no path, so there is no collision to check for.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"GetLatestConfiguration", "GET", "/configuration"},
		{"StartConfigurationSession", "POST", "/configurationsessions"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real AppConfigData op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the handler's literal path+method switch (handler.go) resolves it
// to the right op, both ops against AppConfigData's real op count. It then
// drives the same request through the real Handler() -- seeding a published
// configuration and a live session first, since both ops 400 on missing
// preconditions rather than 404 -- and asserts the response's decoded
// "message" field is never exactly "not found".
//
// "not found" was grepped across every non-test .go file in this package: it
// appears verbatim only once, at handler.go's Handler() default branch
// (writeAWSError(c, http.StatusNotFound, exceptionResourceNotFound, "not
// found")) for an unmatched path/method. It is NOT safe as a raw substring
// assertion -- ErrProfileNotFound's and ErrResourceRemoved's Error() text
// both contain "resource not found" as a literal substring -- but neither
// sentinel's err.Error() ever reaches the wire: every writeResourceNotFound
// call site in handler.go passes its own hand-written message ("...no
// longer exists.", "No deployment exists...") instead of err.Error(), so the
// exact-match "message" field check below is safe from that false-positive
// class while a substring check would not have been.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.NoError(t, h.Backend.SetConfiguration("myapp", "myenv", "myprofile", "hello", "text/plain"))

			path := tc.path
			var body []byte
			if tc.op == "StartConfigurationSession" {
				body = mustMarshalJSON(map[string]string{
					"ApplicationIdentifier":          "myapp",
					"EnvironmentIdentifier":          "myenv",
					"ConfigurationProfileIdentifier": "myprofile",
				})
			} else {
				token, startErr := h.Backend.StartSession("myapp", "myenv", "myprofile", 0)
				require.NoError(t, startErr)
				path += "?configuration_token=" + token
			}

			e := echo.New()
			req := httptest.NewRequest(tc.method, path, bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))

			// A real dispatch hit (success or a domain-specific error) never
			// produces this response shape; only the unmatched-route default
			// does, so a successful GetLatestConfiguration (an opaque content
			// blob, not JSON) is skipped rather than misparsed as an error.
			if rec.Code >= 400 {
				var decoded struct {
					Message string `json:"message"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &decoded))
				assert.NotEqual(t, "not found", decoded.Message,
					"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
			}
		})
	}
}
