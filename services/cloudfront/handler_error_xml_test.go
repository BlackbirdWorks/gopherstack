package cloudfront

import (
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCfErrorXML_EscapesMessage white-box tests the unexported cfErrorXML
// directly: several call sites (handler_dispatch.go's "unknown operation: "
// +operation, and every cfErrorXML(code, err.Error()) site) can carry
// caller-influenced text, and an unescaped "<"/"&" there both breaks the
// response's XML well-formedness for a legitimate client and lets a crafted
// value break out of the <Message> element (CodeQL: reflected XSS via
// user-provided value). Driving this through a real HTTP round trip would
// require reverse-engineering a reachable injection point into `operation`;
// testing the shared builder directly proves every current and future
// caller is covered.
func TestCfErrorXML_EscapesMessage(t *testing.T) {
	t.Parallel()

	const injected = `unknown operation: <script>alert(1)</script>&"'`

	body := cfErrorXML("NoSuchOperation", injected)

	assert.NotContains(t, body, "<script>", "a raw <script> tag must never reach the wire unescaped")

	var parsed struct {
		Error struct {
			Message string `xml:"Message"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &parsed), "escaped output must still be well-formed XML")
	assert.Equal(t, injected, parsed.Error.Message, "escaping must round-trip to the original text")
}
