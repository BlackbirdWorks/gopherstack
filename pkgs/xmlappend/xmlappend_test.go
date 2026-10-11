package xmlappend_test

import (
	"bytes"
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/xmlappend"
)

func TestEscaped_MatchesEncodingXML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
	}{
		{name: "empty", in: ""},
		{name: "plain", in: "hello world"},
		{name: "specials", in: `a"b'c&d<e>f`},
		{name: "whitespace", in: "a\tb\nc\rd"},
		{name: "unicode", in: "café ☃"},
		{name: "control", in: "a\x01b\x1f"},
		{name: "invalid_utf8", in: "a\xffb"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var want bytes.Buffer
			require.NoError(t, xml.EscapeText(&want, []byte(tt.in)))
			assert.Equal(t, want.String(), string(xmlappend.Escaped(nil, tt.in)))
		})
	}
}

func TestElem(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "<A>x&amp;y</A>", string(xmlappend.Elem(nil, "A", "x&y")))
}
