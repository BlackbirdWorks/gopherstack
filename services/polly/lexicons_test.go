package polly_test

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/polly"
)

func TestBackendConcurrentFriendlyCopies(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	require.NoError(t, backend.PutLexicon("copy", `<lexicon alphabet="ipa" xml:lang="en-US"></lexicon>`))
	first, err := backend.GetLexicon("copy")
	require.NoError(t, err)
	first.Content = "changed"
	second, err := backend.GetLexicon("copy")
	require.NoError(t, err)
	assert.NotEqual(t, first.Content, second.Content)
}

func TestBackendLexiconErrors(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	tests := []struct {
		run  func() error
		name string
	}{
		{name: "invalid_lexicon", run: func() error { return backend.PutLexicon("bad name", "xml") }},
		{name: "missing_delete", run: func() error { return backend.DeleteLexicon("absent") }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert.Error(t, test.run())
		})
	}
}

func TestLexiconNameValidation(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	content := `<lexicon alphabet="ipa" xml:lang="en-US"></lexicon>`
	tests := []struct {
		name    string
		lexName string
		wantErr bool
	}{
		{name: "valid_alpha", lexName: "medical", wantErr: false},
		{name: "valid_alphanumeric", lexName: "lexicon1", wantErr: false},
		{name: "empty", lexName: "", wantErr: true},
		{name: "space", lexName: "bad name", wantErr: true},
		{name: "hyphen", lexName: "my-lex", wantErr: true},
		{name: "underscore", lexName: "my_lex", wantErr: true},
		{name: "too_long", lexName: strings.Repeat("a", 21), wantErr: true},
		{name: "exactly_20", lexName: strings.Repeat("a", 20), wantErr: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := backend.PutLexicon(test.lexName, content)
			if test.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, polly.ErrValidation)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLexiconCRUD(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	tests := []struct {
		body   any
		name   string
		method string
		path   string
		find   string
		code   int
	}{
		{
			name: "put_first", method: http.MethodPut, path: "/v1/lexicons/zeta",
			body: map[string]any{"Content": `<lexicon alphabet="ipa" xml:lang="en-US"><lexeme></lexeme></lexicon>`},
			code: http.StatusOK,
		},
		{
			name: "put_second", method: http.MethodPut, path: "/v1/lexicons/alpha",
			body: map[string]any{"Content": `<lexicon alphabet="x-sampa" xml:lang="en-GB"></lexicon>`},
			code: http.StatusOK,
		},
		{
			name:   "get",
			method: http.MethodGet,
			path:   "/v1/lexicons/zeta",
			code:   http.StatusOK,
			find:   `"LanguageCode":"en-US"`,
		},
		{name: "list", method: http.MethodGet, path: "/v1/lexicons", code: http.StatusOK, find: `"Name":"alpha"`},
		{name: "delete", method: http.MethodDelete, path: "/v1/lexicons/zeta", code: http.StatusOK},
		{
			name:   "missing",
			method: http.MethodGet,
			path:   "/v1/lexicons/zeta",
			code:   http.StatusNotFound,
			find:   "LexiconNotFoundException",
		},
	}

	for _, test := range tests {
		rec := request(t, handler, test.method, test.path, test.body)
		assert.Equal(t, test.code, rec.Code, test.name)
		if test.find != "" {
			assert.Contains(t, rec.Body.String(), test.find, test.name)
		}
	}
}

func TestLexiconPaginationAndSynthesisUse(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	for _, name := range []string{"bravo", "alpha", "charlie"} {
		rec := request(t, handler, http.MethodPut, "/v1/lexicons/"+name, map[string]any{
			"Content": `<lexicon alphabet="ipa" xml:lang="en-US"><lexeme></lexeme></lexicon>`,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	first := request(t, handler, http.MethodGet, "/v1/lexicons?MaxResults=2", nil)
	require.Equal(t, http.StatusOK, first.Code)
	body := responseMap(t, first)
	lexicons := body["Lexicons"].([]any)
	require.Len(t, lexicons, 2)
	assert.Equal(t, "alpha", lexicons[0].(map[string]any)["Name"])
	assert.NotEmpty(t, body["NextToken"])

	for _, name := range []string{"alpha", "unknown"} {
		rec := request(t, handler, http.MethodPost, "/v1/speech", map[string]any{
			"LexiconNames": []string{name},
			"OutputFormat": "mp3",
			"Text":         "pronounce this",
			"VoiceId":      "Joanna",
		})
		want := http.StatusOK
		if name == "unknown" {
			want = http.StatusNotFound
		}
		assert.Equal(t, want, rec.Code)
	}
}

// TestPutLexiconNameValidation verifies that PutLexicon rejects lexicon names
// that are empty, too long (>20 chars), or non-alphanumeric. AWS returns
// InvalidParameterValueException for invalid names.
func TestPutLexiconNameValidation(t *testing.T) {
	t.Parallel()

	validContent := `<lexicon alphabet="ipa" xml:lang="en-US"><lexeme></lexeme></lexicon>`

	tests := []struct {
		name     string
		lexName  string
		wantCode int
	}{
		{name: "valid name", lexName: "Medical", wantCode: http.StatusOK},
		{name: "max length name", lexName: strings.Repeat("a", 20), wantCode: http.StatusOK},
		{name: "over max length rejected", lexName: strings.Repeat("a", 21), wantCode: http.StatusBadRequest},
		{name: "hyphen rejected", lexName: "my-lex", wantCode: http.StatusBadRequest},
		{name: "underscore rejected", lexName: "my_lex", wantCode: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, newHandler(), http.MethodPut, "/v1/lexicons/"+tc.lexName,
				map[string]any{"Content": validContent})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
			}
		})
	}
}

// TestPutLexiconContentValidation verifies that PutLexicon rejects invalid PLS
// lexicon content. AWS requires Content to be non-empty XML containing a
// <lexicon element, returning InvalidLexiconException.
func TestPutLexiconContentValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		content  string
		wantCode int
	}{
		{
			name:     "valid content",
			content:  `<lexicon alphabet="ipa" xml:lang="en-US"><lexeme></lexeme></lexicon>`,
			wantCode: http.StatusOK,
		},
		{name: "empty content rejected", content: "", wantCode: http.StatusBadRequest},
		{name: "missing lexicon element rejected", content: "<dict><entry/></dict>", wantCode: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, newHandler(), http.MethodPut, "/v1/lexicons/Medical",
				map[string]any{"Content": tc.content})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "InvalidLexiconException")
			}
		})
	}
}

// TestPutLexiconPlsSchemaValidation verifies that PutLexicon rejects lexicon
// content violating PLS quota/schema constraints from
// https://docs.aws.amazon.com/polly/latest/dg/limits.html#limits-lexicons:
// unsupported alphabet, unsupported xml:lang, oversized lexicon Content, and
// an oversized <phoneme>/<alias> lexeme replacement.
func TestPutLexiconPlsSchemaValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		content  string
		wantErr  string
		wantCode int
	}{
		{
			name:     "unsupported alphabet rejected",
			content:  `<lexicon alphabet="klatt" xml:lang="en-US"><lexeme></lexeme></lexicon>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "UnsupportedPlsAlphabetException",
		},
		{
			name:     "x-sampa alphabet accepted",
			content:  `<lexicon alphabet="x-sampa" xml:lang="en-US"><lexeme></lexeme></lexicon>`,
			wantCode: http.StatusOK,
		},
		{
			name:     "unsupported language rejected",
			content:  `<lexicon alphabet="ipa" xml:lang="xx-XX"><lexeme></lexeme></lexicon>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "UnsupportedPlsLanguageException",
		},
		{
			name: "oversized content rejected",
			content: `<lexicon alphabet="ipa" xml:lang="en-US">` +
				strings.Repeat("a", 40000) + `</lexicon>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "LexiconSizeExceededException",
		},
		{
			name: "oversized phoneme replacement rejected",
			content: `<lexicon alphabet="ipa" xml:lang="en-US"><lexeme><phoneme>` +
				strings.Repeat("a", 101) + `</phoneme></lexeme></lexicon>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "MaxLexemeLengthExceededException",
		},
		{
			name: "phoneme replacement at limit accepted",
			content: `<lexicon alphabet="ipa" xml:lang="en-US"><lexeme><phoneme>` +
				strings.Repeat("a", 100) + `</phoneme></lexeme></lexicon>`,
			wantCode: http.StatusOK,
		},
		{
			name: "oversized alias replacement rejected",
			content: `<lexicon alphabet="ipa" xml:lang="en-US"><lexeme><alias>` +
				strings.Repeat("a", 101) + `</alias></lexeme></lexicon>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "MaxLexemeLengthExceededException",
		},
		{
			name: "self-closing phoneme tag has no replacement text",
			content: `<lexicon alphabet="ipa" xml:lang="en-US">` +
				`<lexeme><phoneme ph="test"/></lexeme></lexicon>`,
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, newHandler(), http.MethodPut, "/v1/lexicons/Medical",
				map[string]any{"Content": tc.content})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantErr != "" {
				assert.Contains(t, rec.Body.String(), tc.wantErr)
			}
		})
	}
}

// TestPutLexiconMaxLexiconsNumberExceeded verifies that PutLexicon enforces
// the 100-lexicons-per-account quota for new lexicons (AWS:
// MaxLexiconsNumberExceededException), while overwriting an existing lexicon
// name never counts against the quota.
func TestPutLexiconMaxLexiconsNumberExceeded(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	content := `<lexicon alphabet="ipa" xml:lang="en-US"></lexicon>`

	for i := range 100 {
		require.NoError(t, backend.PutLexicon(fmt.Sprintf("lex%03d", i), content))
	}

	err := backend.PutLexicon("oneTooMany", content)
	require.ErrorIs(t, err, polly.ErrMaxLexiconsNumberExceeded)

	// Overwriting an existing name must not be blocked by the quota.
	assert.NoError(t, backend.PutLexicon("lex000", content))
}

// TestListLexiconsOpaqueToken verifies that the NextToken returned from a
// paginated ListLexicons response is opaque (base64-encoded), not a raw integer.
func TestListLexiconsOpaqueToken(t *testing.T) {
	t.Parallel()

	handler := newHandler()

	for _, name := range []string{"alpha", "bravo"} {
		rec := request(t, handler, http.MethodPut, "/v1/lexicons/"+name, map[string]any{
			"Content": `<?xml version="1.0" encoding="UTF-8"?><lexicon version="1.0"></lexicon>`,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := request(t, handler, http.MethodGet, "/v1/lexicons?MaxResults=1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := responseMap(t, rec)

	token, ok := out["NextToken"].(string)
	require.True(t, ok, "NextToken must be present when more results exist")
	assert.NotEmpty(t, token)

	decoded, err := base64.StdEncoding.DecodeString(token)
	require.NoError(t, err, "NextToken should be base64-encoded")
	assert.NotEmpty(t, decoded, "decoded token should contain position bytes")
}
