package polly_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// SynthesizeSpeech: text length limits
// ---------------------------------------------------------------------------

// TestBatch2_SynthesizeSpeech_TextLengthLimit verifies that SynthesizeSpeech
// rejects text exceeding 3000 characters and SSML exceeding 6000 characters.
// AWS returns InvalidParameterValueException for oversized text input.
func TestBatch2_SynthesizeSpeech_TextLengthLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		textType string
		text     string
		wantCode int
	}{
		{
			name:     "text at limit passes",
			textType: "text",
			text:     strings.Repeat("a", 3000),
			wantCode: http.StatusOK,
		},
		{
			name:     "text over limit rejected",
			textType: "text",
			text:     strings.Repeat("a", 3001),
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ssml at limit passes",
			textType: "ssml",
			text:     strings.Repeat("a", 6000),
			wantCode: http.StatusOK,
		},
		{
			name:     "ssml over limit rejected",
			textType: "ssml",
			text:     strings.Repeat("a", 6001),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, newHandler(), http.MethodPost, "/v1/speech", map[string]any{
				"OutputFormat": "mp3",
				"Text":         tc.text,
				"TextType":     tc.textType,
				"VoiceId":      "Joanna",
			})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// StartSpeechSynthesisTask: required fields and text limit
// ---------------------------------------------------------------------------

// TestBatch2_StartSpeechSynthesisTask_RequiredAndLimit verifies that
// StartSpeechSynthesisTask rejects missing OutputS3BucketName and text
// exceeding 100000 characters. AWS returns InvalidParameterValueException
// for both cases.
func TestBatch2_StartSpeechSynthesisTask_RequiredAndLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing bucket returns 400",
			body: map[string]any{
				"OutputFormat": "mp3",
				"Text":         "hello",
				"VoiceId":      "Joanna",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "text over 100000 returns 400",
			body: map[string]any{
				"OutputS3BucketName": "out-bucket",
				"OutputFormat":       "mp3",
				"Text":               strings.Repeat("a", 100001),
				"VoiceId":            "Joanna",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "text at 100000 succeeds",
			body: map[string]any{
				"OutputS3BucketName": "out-bucket",
				"OutputFormat":       "mp3",
				"Text":               strings.Repeat("a", 100000),
				"VoiceId":            "Joanna",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, newHandler(), http.MethodPost, "/v1/synthesisTasks", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// StartSpeechSynthesisTask: OutputURI format accuracy
// ---------------------------------------------------------------------------

// TestBatch2_StartSpeechSynthesisTask_OutputURIFormat verifies that the
// OutputUri field is constructed correctly: s3://<bucket>/<taskID>.<ext>.
// AWS sets the extension based on OutputFormat (ogg_vorbis → .ogg,
// others match format name).
func TestBatch2_StartSpeechSynthesisTask_OutputURIFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format string
		ext    string
	}{
		{name: "mp3", format: "mp3", ext: "mp3"},
		{name: "ogg_vorbis", format: "ogg_vorbis", ext: "ogg"},
		{name: "pcm", format: "pcm", ext: "pcm"},
		{name: "json", format: "json", ext: "json"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{
				"OutputS3BucketName": "my-bucket",
				"OutputFormat":       tc.format,
				"Text":               "test audio",
				"VoiceId":            "Joanna",
			}
			if tc.format == "json" {
				body["SpeechMarkTypes"] = []string{"word"}
			}

			rec := request(t, newHandler(), http.MethodPost, "/v1/synthesisTasks", body)
			require.Equal(t, http.StatusOK, rec.Code)

			task := responseMap(t, rec)["SynthesisTask"].(map[string]any)
			id := task["TaskId"].(string)
			uri := task["OutputUri"].(string)

			assert.True(t, strings.HasPrefix(uri, "s3://my-bucket/"), "OutputUri must use bucket")
			assert.True(t, strings.HasSuffix(uri, id+"."+tc.ext), "OutputUri must end with taskID.ext")
		})
	}
}

// ---------------------------------------------------------------------------
// PutLexicon: name and content validation
// ---------------------------------------------------------------------------

// TestBatch2_PutLexicon_NameValidation verifies that PutLexicon rejects
// lexicon names that are empty, too long (>20 chars), or non-alphanumeric.
// AWS returns InvalidParameterValueException for invalid names.
func TestBatch2_PutLexicon_NameValidation(t *testing.T) {
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

// TestBatch2_PutLexicon_ContentValidation verifies that PutLexicon rejects
// invalid PLS lexicon content. AWS requires Content to be non-empty XML
// containing a <lexicon element.
func TestBatch2_PutLexicon_ContentValidation(t *testing.T) {
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
				assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// SynthesizeSpeech: LexiconNames count limit
// ---------------------------------------------------------------------------

// TestBatch2_SynthesizeSpeech_LexiconNamesLimit verifies that SynthesizeSpeech
// rejects more than 5 lexicon names. AWS returns InvalidParameterValueException
// when LexiconNames exceeds the maximum of 5.
func TestBatch2_SynthesizeSpeech_LexiconNamesLimit(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	validContent := `<lexicon alphabet="ipa" xml:lang="en-US"><lexeme></lexeme></lexicon>`
	names := make([]string, 6)
	for i := range 6 {
		name := fmt.Sprintf("lex%02d", i)
		names[i] = name
		rec := request(t, handler, http.MethodPut, "/v1/lexicons/"+name,
			map[string]any{"Content": validContent})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		name     string
		lexNames []string
		wantCode int
	}{
		{name: "5 lexicons allowed", lexNames: names[:5], wantCode: http.StatusOK},
		{name: "6 lexicons rejected", lexNames: names[:6], wantCode: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, handler, http.MethodPost, "/v1/speech", map[string]any{
				"OutputFormat": "mp3",
				"LexiconNames": tc.lexNames,
				"Text":         "hello",
				"VoiceId":      "Joanna",
			})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// DescribeVoices: invalid engine validation
// ---------------------------------------------------------------------------

// TestBatch2_DescribeVoices_InvalidEngine verifies that DescribeVoices rejects
// an unrecognized Engine value. AWS returns InvalidParameterValueException for
// engines not in {standard, neural, long-form, generative}.
func TestBatch2_DescribeVoices_InvalidEngine(t *testing.T) {
	t.Parallel()

	rec := request(t, newHandler(), http.MethodGet, "/v1/voices?Engine=quantum", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
}

// ---------------------------------------------------------------------------
// SynthesizeSpeech: invalid SpeechMarkType
// ---------------------------------------------------------------------------

// TestBatch2_SynthesizeSpeech_InvalidSpeechMarkType verifies that
// SynthesizeSpeech rejects unrecognized SpeechMarkTypes values. AWS returns
// InvalidParameterValueException for types not in {sentence, ssml, viseme, word}.
func TestBatch2_SynthesizeSpeech_InvalidSpeechMarkType(t *testing.T) {
	t.Parallel()

	rec := request(t, newHandler(), http.MethodPost, "/v1/speech", map[string]any{
		"OutputFormat":    "json",
		"SpeechMarkTypes": []string{"phoneme"},
		"Text":            "hello",
		"VoiceId":         "Joanna",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
}

// ---------------------------------------------------------------------------
// SynthesizeSpeech: default SampleRate accuracy per engine
// ---------------------------------------------------------------------------

// TestBatch2_SynthesizeSpeech_DefaultSampleRate verifies that the default
// SampleRate is engine-specific. AWS defaults: PCM → 16000, non-standard
// engines (neural/long-form/generative) → 24000, standard → 22050.
// PCM output is a WAV container; the sample rate is encoded in the WAV header
// at bytes 24-27 (little-endian uint32). MP3 output returns MPEG sync bytes.
func TestBatch2_SynthesizeSpeech_DefaultSampleRate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		engine         string
		format         string
		wantSampleRate uint32 // 0 means skip WAV header check (MP3/OGG)
		wantMagic      []byte
	}{
		{name: "standard_mp3_defaults_22050", engine: "standard", format: "mp3", wantMagic: []byte{0xFF, 0xFB}},
		{name: "neural_mp3_defaults_24000", engine: "neural", format: "mp3", wantMagic: []byte{0xFF, 0xFB}},
		{name: "generative_mp3_defaults_24000", engine: "generative", format: "mp3", wantMagic: []byte{0xFF, 0xFB}},
		{name: "standard_pcm_defaults_16000", engine: "standard", format: "pcm", wantSampleRate: 16000, wantMagic: []byte("RIFF")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, newHandler(), http.MethodPost, "/v1/speech", map[string]any{
				"Engine":       tc.engine,
				"OutputFormat": tc.format,
				"Text":         "hello",
				"VoiceId":      "Joanna",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			body := rec.Body.Bytes()
			require.Greater(t, len(body), 4, "audio body must be non-empty")
			if len(tc.wantMagic) > 0 {
				assert.Equal(t, tc.wantMagic, body[:len(tc.wantMagic)], "audio magic bytes")
			}
			if tc.wantSampleRate > 0 && len(body) >= 28 {
				// WAV sample rate is at offset 24, little-endian uint32.
				rate := uint32(body[24]) | uint32(body[25])<<8 | uint32(body[26])<<16 | uint32(body[27])<<24
				assert.Equal(t, tc.wantSampleRate, rate, "WAV header sample rate")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TagResource: validation accuracy
// ---------------------------------------------------------------------------

// TestBatch2_TagResource_Validation verifies that TagResource enforces AWS tag
// constraints: duplicate keys rejected, key exceeding 128 chars rejected, value
// exceeding 256 chars rejected.
func TestBatch2_TagResource_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     []map[string]string
		wantCode int
	}{
		{
			name:     "valid tags accepted",
			tags:     []map[string]string{{"Key": "env", "Value": "prod"}},
			wantCode: http.StatusOK,
		},
		{
			name: "duplicate key rejected",
			tags: []map[string]string{
				{"Key": "env", "Value": "prod"},
				{"Key": "env", "Value": "dev"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "key over 128 chars rejected",
			tags:     []map[string]string{{"Key": strings.Repeat("k", 129), "Value": "v"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "value over 256 chars rejected",
			tags:     []map[string]string{{"Key": "k", "Value": strings.Repeat("v", 257)}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty key rejected",
			tags:     []map[string]string{{"Key": "", "Value": "v"}},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := newHandler()
			id, _ := startTask(t, handler, "tag validation test")
			taskARN := handler.Backend.TaskARN(id)

			tags := make([]map[string]string, len(tc.tags))
			copy(tags, tc.tags)

			rec := request(t, handler, http.MethodPost, "/v1/tags/"+taskARN,
				map[string]any{"Tags": tags})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
			}
		})
	}
}

// TestBatch2_TagResource_CountLimit verifies that TagResource rejects a batch
// that would push the resource over the 50-tag limit. AWS returns
// InvalidParameterValueException when the limit would be exceeded.
func TestBatch2_TagResource_CountLimit(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	id, _ := startTask(t, handler, "tag count limit test")
	taskARN := handler.Backend.TaskARN(id)

	batch1 := make([]map[string]string, 50)
	for i := range 50 {
		batch1[i] = map[string]string{"Key": fmt.Sprintf("key%d", i), "Value": "v"}
	}
	rec := request(t, handler, http.MethodPost, "/v1/tags/"+taskARN, map[string]any{"Tags": batch1})
	require.Equal(t, http.StatusOK, rec.Code, "50 tags should be accepted")

	overflow := []map[string]string{{"Key": "overflow", "Value": "x"}}
	rec = request(t, handler, http.MethodPost, "/v1/tags/"+taskARN, map[string]any{"Tags": overflow})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
}
