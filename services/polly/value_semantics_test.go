package polly_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pollysdk "github.com/aws/aws-sdk-go-v2/service/polly"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLexicon_AttributesDerivedFromRootAndLexemes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		content      string
		wantAlphabet string
		wantLang     string
		wantLexemes  int32
	}{
		{
			name: "lexemes with attributes counted",
			content: `<lexicon alphabet="ipa" xml:lang="en-US">` +
				`<lexeme role="x"><grapheme>a</grapheme></lexeme><lexeme><grapheme>b</grapheme></lexeme></lexicon>`,
			wantAlphabet: "ipa",
			wantLang:     "en-US",
			wantLexemes:  2,
		},
		{
			name:         "single quotes accepted",
			content:      `<lexicon alphabet='x-sampa' xml:lang='en-GB'><lexeme><grapheme>a</grapheme></lexeme></lexicon>`,
			wantAlphabet: "x-sampa",
			wantLang:     "en-GB",
			wantLexemes:  1,
		},
		{
			name: "phoneme alphabet does not leak to root",
			content: `<lexicon xml:lang="en-US"><lexeme><grapheme>a</grapheme>` +
				`<phoneme alphabet="x-sampa">a</phoneme></lexeme></lexicon>`,
			wantAlphabet: "ipa", wantLang: "en-US", wantLexemes: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newTestPollySDKClient(t, newHandler())

			_, err := client.PutLexicon(ctx, &pollysdk.PutLexiconInput{
				Name: aws.String("lex"), Content: aws.String(tt.content),
			})
			require.NoError(t, err)

			got, err := client.GetLexicon(ctx, &pollysdk.GetLexiconInput{Name: aws.String("lex")})
			require.NoError(t, err)
			require.NotNil(t, got.LexiconAttributes)
			assert.Equal(t, tt.wantAlphabet, aws.ToString(got.LexiconAttributes.Alphabet))
			assert.Equal(t, tt.wantLang, string(got.LexiconAttributes.LanguageCode))
			assert.Equal(t, tt.wantLexemes, got.LexiconAttributes.LexemesCount)
		})
	}
}
