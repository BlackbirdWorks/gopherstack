package s3

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeDeleteRequestMatchesEncodingXML(t *testing.T) {
	t.Parallel()

	const ns = ` xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`

	tests := []struct {
		name string
		body string
	}{
		{
			name: "sdk shape",
			body: `<Delete` + ns + `><Object><Key>a/b</Key></Object>` +
				`<Object><Key>c</Key><VersionId>v1</VersionId></Object><Quiet>true</Quiet></Delete>`,
		},
		{name: "no namespace", body: `<Delete><Object><Key>k</Key></Object></Delete>`},
		{
			name: "xml decl and spaces",
			body: "<?xml version=\"1.0\"?>\n<Delete>\n <Object>\n  <Key>k</Key>\n </Object>\n</Delete>\n",
		},
		{name: "quiet false", body: `<Delete><Quiet>false</Quiet><Object><Key>k</Key></Object></Delete>`},
		{name: "quiet empty", body: `<Delete><Quiet></Quiet><Object><Key>k</Key></Object></Delete>`},
		{name: "quiet invalid", body: `<Delete><Quiet>maybe</Quiet></Delete>`},
		{name: "empty version id", body: `<Delete><Object><Key>k</Key><VersionId></VersionId></Object></Delete>`},
		{name: "entities", body: `<Delete><Object><Key>a&amp;b&lt;c</Key></Object></Delete>`},
		{name: "cdata", body: `<Delete><Object><Key><![CDATA[x y]]></Key></Object></Delete>`},
		{name: "non ascii key", body: `<Delete><Object><Key>caf` + "é" + `</Key></Object></Delete>`},
		{name: "unknown element", body: `<Delete><Foo>1</Foo><Object><Key>k</Key><Bar/></Object></Delete>`},
		{name: "prefixed", body: `<s:Delete xmlns:s="x"><s:Object><s:Key>k</s:Key></s:Object></s:Delete>`},
		{name: "wrong root", body: `<Other><Object><Key>k</Key></Object></Other>`},
		{name: "truncated", body: `<Delete><Object><Key>k</Key>`},
		{name: "trailing garbage", body: `<Delete><Object><Key>k</Key></Object></Delete>junk`},
		{name: "empty", body: ``},
		{name: "self closing", body: `<Delete/>`},
		{name: "control char", body: "<Delete><Object><Key>a\x01b</Key></Object></Delete>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var want DeleteRequest

			wantErr := xml.NewDecoder(strings.NewReader(tt.body)).Decode(&want)

			got, gotErr := decodeDeleteRequest(strings.NewReader(tt.body))

			if wantErr != nil {
				require.Error(t, gotErr)

				return
			}

			require.NoError(t, gotErr)

			want.XMLName, got.XMLName = xml.Name{}, xml.Name{} // never read by the handler
			assert.Equal(t, want, got)
		})
	}
}

func TestParseDeleteRequestFastLarge(t *testing.T) {
	t.Parallel()

	var sb bytes.Buffer

	sb.WriteString("<Delete>")

	for i := range 1000 {
		fmt.Fprintf(&sb, "<Object><Key>k%04d</Key></Object>", i)
	}

	sb.WriteString("</Delete>")

	req, ok := parseDeleteRequestFast(sb.Bytes())
	require.True(t, ok)
	assert.Len(t, req.Objects, 1000)
}
