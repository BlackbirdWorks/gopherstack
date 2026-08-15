package iotdataplane_test

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real IoT Data
// Plane operation, extracted from iotdataplane@v1.35.4 serializers.go: each
// entry's "request.Method" and the string passed to httpbinding.SplitURI in
// that op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands
// in for any {clientId}/{thingName}/{topic} URI label -- none of this
// handler's path parsers (isShadowPath, splitConnectionsWirePath,
// retainedMessagePathSlash trimming) validate identifier shape, so the
// literal value doesn't matter here, only path depth and static segments.
// 11 real ops here, matching IoT Data Plane's real op count exactly --
// GetSupportedOperations() lists 14 because it also carries
// ListConnections, ListThingsWithShadows and RegisterConnection, three
// gopherstack-only admin extensions with no real AWS wire operation (see
// handler.go's adminConnectionsPath and RouteMatcher doc comments), so
// those three are deliberately excluded from this table.
//
// A systematic check for a shared method+path across all 11 ops found zero
// collisions -- every op has its own unique (method, path) pair. Three ops
// (DeleteThingShadow, GetThingShadow, UpdateThingShadow) share the identical
// path "/things/{thingName}/shadow" and are disambiguated purely by method
// (DELETE/GET/POST), same as GetConnection/DeleteConnection sharing
// "/connections/{clientId}" (GET/DELETE).
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"DeleteConnection", "DELETE", "/connections/PLACEHOLDER"},
		{"DeleteThingShadow", "DELETE", "/things/PLACEHOLDER/shadow"},
		{"GetConnection", "GET", "/connections/PLACEHOLDER"},
		{"GetRetainedMessage", "GET", "/retainedMessage/PLACEHOLDER"},
		{"GetThingShadow", "GET", "/things/PLACEHOLDER/shadow"},
		{"ListNamedShadowsForThing", "GET", "/api/things/shadow/ListNamedShadowsForThing/PLACEHOLDER"},
		{"ListRetainedMessages", "GET", "/retainedMessage"},
		{"ListSubscriptions", "GET", "/connections/PLACEHOLDER/subscriptions"},
		{"Publish", "POST", "/topics/PLACEHOLDER"},
		{"SendDirectMessage", "POST", "/connections/PLACEHOLDER/messages"},
		{"UpdateThingShadow", "POST", "/things/PLACEHOLDER/shadow"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real IoT Data Plane op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the handler (handler.go) resolves it to the right op, all 11 ops
// against IoT Data Plane's real op count. It then drives the same request
// through the real Handler() and asserts the response's decoded "error"
// field is never exactly "not found" -- the literal Handler() emits from its
// default branch (c.JSON(http.StatusNotFound, map[string]string{keyError:
// "not found"})) when no case in its top-level switch matches.
//
// "not found" was grepped across every non-test .go file in this package
// and found NOT to be safe as a raw substring assertion: ErrShadowNotFound
// ("shadow not found"), ErrRetainedMessageNotFound ("retained message not
// found") and ErrConnectionNotFound ("connection not found") all contain
// "not found" as a substring, and handleError puts err.Error() verbatim into
// the response's "message" field for all three (mapped to
// ResourceNotFoundException, a real and expected 404 for e.g.
// GetThingShadow/GetConnection/GetRetainedMessage against this table's fresh,
// empty-backend test handler). A body-substring check would therefore fail
// on those three ops even though they dispatched correctly. Comparing only
// the exact "error" field is unaffected: it decodes to "ResourceNotFoundException"
// for all three, never the literal "not found" -- so the exact-match check
// below correctly distinguishes a genuine miss from a legitimate 404.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))

			var decoded struct {
				Error string `json:"error"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &decoded))
			assert.NotEqual(t, "not found", decoded.Error,
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
