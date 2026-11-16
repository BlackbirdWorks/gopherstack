package azuresearch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuresearch"
)

const learnMarkdown = "# Section 1\r\nContent for section 1.\r\n## Subsection 1.1\r\n" +
	"Content for subsection 1.1.\r\n# Section 2\r\nContent for section 2.\r\n"

func section(h1, h2 string) map[string]any { return map[string]any{"h1": h1, "h2": h2} }

func TestCrackMarkdownOneToMany(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg  map[string]any
		name string
		data string
		want []map[string]any
	}{
		{
			name: "learn_sample_depth2", data: learnMarkdown,
			cfg: map[string]any{"parsingMode": "markdown", "markdownHeaderDepth": "h2"},
			want: []map[string]any{
				{
					"content": "Content for section 1.\r\n", "sections": section("Section 1", ""),
					"ordinal_position": 1.0,
				},
				{
					"content": "Content for subsection 1.1.\r\n", "sections": section("Section 1", "Subsection 1.1"),
					"ordinal_position": 2.0,
				},
				{
					"content": "Content for section 2.\r\n", "sections": section("Section 2", ""),
					"ordinal_position": 3.0,
				},
			},
		},
		{
			name: "depth1_treats_h2_as_text", data: "# A\nbody\n## not header\nmore\n",
			cfg: map[string]any{"parsingMode": "markdown", "markdownHeaderDepth": "h1"},
			want: []map[string]any{
				{
					"content":          "body\n## not header\nmore\n",
					"sections":         map[string]any{"h1": "A"},
					"ordinal_position": 1.0,
				},
			},
		},
		{
			name: "preamble_and_fence", data: "intro\n# A\n```\n# not a header\n```\n",
			cfg: map[string]any{"parsingMode": "markdown", "markdownHeaderDepth": "h1"},
			want: []map[string]any{
				{"content": "intro\n", "sections": map[string]any{"h1": ""}, "ordinal_position": 1.0},
				{
					"content": "```\n# not a header\n```\n", "sections": map[string]any{"h1": "A"},
					"ordinal_position": 2.0,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := azuresearch.CrackBlob("a.md", "text/markdown", []byte(tt.data), tt.cfg)
			require.Len(t, got, len(tt.want))

			for i, w := range tt.want {
				for k, v := range w {
					assert.Equal(t, v, got[i][k], "doc %d field %s", i, k)
				}

				assert.Equal(t, "a.md", got[i]["metadata_storage_name"])
			}
		})
	}
}

func TestCrackMarkdownOneToOne(t *testing.T) {
	t.Parallel()

	got := azuresearch.CrackBlob("a.md", "", []byte(learnMarkdown), map[string]any{
		"parsingMode": "markdown", "markdownParsingSubmode": "oneToOne",
	})
	require.Len(t, got, 1)
	assert.Equal(t, learnMarkdown, got[0]["document_content"])

	top, ok := got[0]["sections"].([]any)
	require.True(t, ok)
	require.Len(t, top, 2)

	first, ok := top[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "h1", first["header_level"])
	assert.Equal(t, "Section 1", first["header_name"])
	assert.Equal(t, "Content for section 1.", first["content"])
	assert.InDelta(t, 1.0, first["ordinal_position"], 0)

	subs, ok := first["sections"].([]any)
	require.True(t, ok)
	require.Len(t, subs, 1)

	sub, ok := subs[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "h2", sub["header_level"])
	assert.InDelta(t, 2.0, sub["ordinal_position"], 0)

	second, ok := top[1].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 3.0, second["ordinal_position"], 0)
	assert.Empty(t, second["sections"])
}

func TestCrackHTMLContent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		file string
		ct   string
		data string
		want string
	}{
		{"by_content_type", "a.bin", "text/html; charset=utf-8", "<p>Hello <b>world</b></p>", "Hello world"},
		{
			"by_extension",
			"a.html",
			"",
			"<html><head><style>p{}</style></head><body>x<script>1</script> y</body></html>",
			"x y",
		},
		{"plain_untouched", "a.txt", "text/plain", "<p>kept</p>", "<p>kept</p>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := azuresearch.CrackBlob(tt.file, tt.ct, []byte(tt.data), map[string]any{})
			require.Len(t, got, 1)
			assert.Equal(t, tt.want, got[0]["content"])
		})
	}
}
