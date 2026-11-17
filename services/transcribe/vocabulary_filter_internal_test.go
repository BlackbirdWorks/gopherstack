package transcribe

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type filterMedia map[string][]byte

func (m filterMedia) GetObject(_ context.Context, bucket, key string) ([]byte, error) {
	if d, ok := m[bucket+"/"+key]; ok {
		return d, nil
	}

	return nil, ErrMediaNotFound
}

type fixedEngine struct{ words []Word }

func (fixedEngine) SupportsLanguage(string) bool { return true }

func (e fixedEngine) Recognize(context.Context, string, int, []byte) ([]Word, error) {
	return append([]Word(nil), e.words...), nil
}

func tinyWAV() []byte {
	pcm := make([]byte, 3200)
	hdr := []byte("RIFF")
	hdr = binary.LittleEndian.AppendUint32(hdr, uint32(36+len(pcm)))
	hdr = append(hdr, "WAVEfmt "...)
	hdr = binary.LittleEndian.AppendUint32(hdr, 16)
	hdr = binary.LittleEndian.AppendUint16(hdr, 1)
	hdr = binary.LittleEndian.AppendUint16(hdr, 1)
	hdr = binary.LittleEndian.AppendUint32(hdr, 16000)
	hdr = binary.LittleEndian.AppendUint32(hdr, 32000)
	hdr = binary.LittleEndian.AppendUint16(hdr, 2)
	hdr = binary.LittleEndian.AppendUint16(hdr, 16)
	hdr = append(hdr, "data"...)
	hdr = binary.LittleEndian.AppendUint32(hdr, uint32(len(pcm)))

	return append(hdr, pcm...)
}

func TestPipelineAppliesVocabularyFilter(t *testing.T) {
	t.Parallel()

	engine := fixedEngine{words: []Word{
		{Text: "hello", Start: 0.1, End: 0.2, Confidence: 1},
		{Text: "Rude", Start: 0.3, End: 0.4, Confidence: 1},
		{Text: "world", Start: 0.5, End: 0.6, Confidence: 1},
	}}

	tests := []struct {
		name       string
		method     string
		uri        string
		wantText   string
		words      []string
		wantMatch  int
		wantTokens int
	}{
		{name: "mask", method: "mask", words: []string{"rude"}, wantText: "hello *** world", wantTokens: 3},
		{name: "remove", method: "remove", words: []string{"rude"}, wantText: "hello world", wantTokens: 2},
		{
			name: "tag", method: "tag", words: []string{"rude"}, wantText: "hello Rude world",
			wantMatch: 1, wantTokens: 3,
		},
		{name: "file", method: "mask", uri: "s3://b/filter.txt", wantText: "hello *** world", wantTokens: 3},
		{name: "no match", method: "mask", words: []string{"absent"}, wantText: "hello Rude world", wantTokens: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := pipelineDeps{
				reader: filterMedia{"b/a.wav": tinyWAV(), "b/filter.txt": []byte("\nRUDE\n")},
				engine: engine, accountID: "123456789012",
			}
			spec := jobSpec{
				Kind: kindTranscription, Name: "j", Language: "en-US", MediaURI: "s3://b/a.wav",
				FilterName: "f", FilterMethod: tt.method, FilterWords: tt.words, FilterURI: tt.uri,
			}

			out := runPipeline(t.Context(), deps, spec)
			require.Empty(t, out.failure)
			require.NotEmpty(t, out.files)

			body := string(out.files[0].data)
			assert.Contains(t, body, `"transcript":"`+tt.wantText+`"`)
			assert.Equal(t, tt.wantMatch, strings.Count(body, `"vocabularyFilterMatch":true`))
			assert.Equal(t, tt.wantTokens, strings.Count(body, `"type":"pronunciation"`))
		})
	}
}

func TestParseFilterFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
		want []string
	}{
		{name: "lines", data: []byte("A\n\n b \r\n"), want: []string{"a", "b"}},
		{name: "empty", data: nil, want: nil},
		{name: "oversized", data: []byte(strings.Repeat("x\n", maxFilterFileBytes)), want: nil},
		{name: "word cap", data: []byte(strings.Repeat("w\n", maxFilterWords+10))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := parseFilterFile(tt.data)
			if tt.name == "word cap" {
				assert.Len(t, got, maxFilterWords)

				return
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
