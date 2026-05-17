package transcribe

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
)

// marshalJSON marshals v to JSON bytes, returning nil on error (only used for synthetic output).
func marshalJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}

// jobNameRe validates TranscriptionJobName per AWS: alphanumeric, dots, underscores, hyphens, 1-200 chars.
var jobNameRe = regexp.MustCompile(`^[0-9a-zA-Z._-]{1,200}$`)

// supportedLanguageCodes returns the set of language codes supported by Amazon Transcribe.
func supportedLanguageCodes() []string {
	return []string{
		"af-ZA", "ar-AE", "ar-SA", "da-DK", "de-CH", "de-DE", "en-AB", "en-AU", "en-GB",
		"en-IE", "en-IN", "en-NZ", "en-US", "en-WL", "en-ZA", "es-ES", "es-US", "eu-ES",
		"fa-IR", "fi-FI", "fr-CA", "fr-FR", "he-IL", "hi-IN", "id-ID", "it-IT", "ja-JP",
		"ko-KR", "ms-MY", "nl-NL", "pt-BR", "pt-PT", "ru-RU", "sv-SE", "ta-IN", "te-IN",
		"th-TH", "tr-TR", "uk-UA", "vi-VN", "zh-CN", "zh-TW",
	}
}

// supportedMediaFormats returns the set of media formats accepted by Amazon Transcribe.
func supportedMediaFormats() []string {
	return []string{"mp3", "mp4", "wav", "flac", "ogg", "amr", "webm", "m4a"}
}

// supportedVocabularyFilterMethods returns the set of vocabulary filter methods.
func supportedVocabularyFilterMethods() []string {
	return []string{"remove", "mask", "tag"}
}

// supportedContentRedactionTypes returns the set of content redaction types.
func supportedContentRedactionTypes() []string { return []string{"PII"} }

// supportedRedactionOutputs returns the set of redaction output options.
func supportedRedactionOutputs() []string { return []string{"redacted", "redacted_and_unredacted"} }

// supportedSubtitleFormats returns the set of subtitle output formats.
func supportedSubtitleFormats() []string { return []string{"vtt", "srt"} }

// supportedBaseModelNames returns the set of base model names for custom language models.
func supportedBaseModelNames() []string { return []string{"NarrowBand", "WideBand"} }

// supportedMedicalSpecialties returns the set of supported medical specialties.
func supportedMedicalSpecialties() []string { return []string{"PRIMARYCARE"} }

// supportedMedicalTypes returns the set of supported medical transcription types.
func supportedMedicalTypes() []string { return []string{"CONVERSATION", "DICTATION"} }

// supportedMedicalContentIDTypes returns the set of medical content identification types.
func supportedMedicalContentIDTypes() []string { return []string{"PHI"} }

// supportedCallAnalyticsInputTypes returns the set of call analytics category input types.
func supportedCallAnalyticsInputTypes() []string { return []string{"REAL_TIME", "POST_CALL"} }

// mediaSampleRateHertzMin is the minimum sample rate accepted by Transcribe.
const mediaSampleRateHertzMin = 8000

// mediaSampleRateHertzMax is the maximum sample rate accepted by Transcribe.
const mediaSampleRateHertzMax = 48000

// maxSpeakerLabelsMin is the minimum number of speakers for diarization.
const maxSpeakerLabelsMin = 2

// maxSpeakerLabelsMax is the maximum number of speakers for diarization.
const maxSpeakerLabelsMax = 10

// maxAlternativesMin is the minimum number of alternative transcripts.
const maxAlternativesMin = 2

// maxAlternativesMax is the maximum number of alternative transcripts.
const maxAlternativesMax = 10

// maxVocabularyPhrases is the maximum number of phrases in a vocabulary.
const maxVocabularyPhrases = 256

// maxVocabularyPhraseLen is the maximum length of a vocabulary phrase.
const maxVocabularyPhraseLen = 256

// maxVocabularyFilterWords is the maximum number of words in a vocabulary filter.
const maxVocabularyFilterWords = 10000

// maxLanguageOptions is the maximum number of language options for language identification.
const maxLanguageOptions = 10

// minLanguageOptions is the minimum number of language options for language identification.
const minLanguageOptions = 2

// syntheticWordBaseDuration is the base duration in seconds for synthetic word timing.
const syntheticWordBaseDuration = 0.3

// syntheticWordDurationPerChar is the additional duration per character for synthetic word timing.
const syntheticWordDurationPerChar = 0.05

// syntheticWordGap is the pause between words in synthetic timing.
const syntheticWordGap = 0.05

// requiredChannelDefinitionCount is the exact number of channel definitions required for call analytics.
const requiredChannelDefinitionCount = 2

// validateJobName checks that a job name matches the AWS-allowed pattern.
func validateJobName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: TranscriptionJobName is required", ErrValidation)
	}

	if !jobNameRe.MatchString(name) {
		return fmt.Errorf("%w: TranscriptionJobName must match ^[0-9a-zA-Z._-]{1,200}$", ErrValidation)
	}

	return nil
}

// validateLanguageCode checks that a language code is in the supported set.
// Empty is allowed when identify flags are set (caller must check).
func validateLanguageCode(code string) error {
	if code == "" {
		return nil
	}

	if !slices.Contains(supportedLanguageCodes(), code) {
		return fmt.Errorf("%w: unsupported LanguageCode %q", ErrValidation, code)
	}

	return nil
}

// validateMediaFormat checks that a media format is in the supported set.
func validateMediaFormat(format string) error {
	if format == "" {
		return nil
	}

	if !slices.Contains(supportedMediaFormats(), format) {
		return fmt.Errorf(
			"%w: unsupported MediaFormat %q; must be one of %v",
			ErrValidation,
			format,
			supportedMediaFormats(),
		)
	}

	return nil
}

// validateMediaSampleRateHertz checks that the sample rate is in [8000, 48000].
func validateMediaSampleRateHertz(rate int32) error {
	if rate == 0 {
		return nil
	}

	if rate < mediaSampleRateHertzMin || rate > mediaSampleRateHertzMax {
		return fmt.Errorf("%w: MediaSampleRateHertz must be between %d and %d",
			ErrValidation, mediaSampleRateHertzMin, mediaSampleRateHertzMax)
	}

	return nil
}

// validateSettings validates Settings fields when Settings is non-nil.
func validateSettings(s *TranscriptionSettings) error {
	if s == nil {
		return nil
	}

	if s.ShowSpeakerLabels && s.MaxSpeakerLabels != 0 {
		if s.MaxSpeakerLabels < maxSpeakerLabelsMin || s.MaxSpeakerLabels > maxSpeakerLabelsMax {
			return fmt.Errorf("%w: MaxSpeakerLabels must be between %d and %d",
				ErrValidation, maxSpeakerLabelsMin, maxSpeakerLabelsMax)
		}
	}

	if s.ShowAlternatives && s.MaxAlternatives != 0 {
		if s.MaxAlternatives < maxAlternativesMin || s.MaxAlternatives > maxAlternativesMax {
			return fmt.Errorf("%w: MaxAlternatives must be between %d and %d",
				ErrValidation, maxAlternativesMin, maxAlternativesMax)
		}
	}

	if s.VocabularyFilterMethod != "" &&
		!slices.Contains(supportedVocabularyFilterMethods(), s.VocabularyFilterMethod) {
		return fmt.Errorf("%w: VocabularyFilterMethod %q must be one of %v",
			ErrValidation, s.VocabularyFilterMethod, supportedVocabularyFilterMethods())
	}

	return nil
}

// validateContentRedaction validates ContentRedaction fields when non-nil.
func validateContentRedaction(cr *ContentRedaction) error {
	if cr == nil {
		return nil
	}

	if cr.RedactionType == "" {
		return fmt.Errorf("%w: ContentRedaction.RedactionType is required", ErrValidation)
	}

	if !slices.Contains(supportedContentRedactionTypes(), cr.RedactionType) {
		return fmt.Errorf("%w: ContentRedaction.RedactionType %q must be one of %v",
			ErrValidation, cr.RedactionType, supportedContentRedactionTypes())
	}

	if cr.RedactionOutput != "" && !slices.Contains(supportedRedactionOutputs(), cr.RedactionOutput) {
		return fmt.Errorf("%w: ContentRedaction.RedactionOutput %q must be one of %v",
			ErrValidation, cr.RedactionOutput, supportedRedactionOutputs())
	}

	return nil
}

// validateSubtitles validates Subtitles input fields when non-nil.
func validateSubtitles(s *SubtitlesInput) error {
	if s == nil {
		return nil
	}

	for _, f := range s.Formats {
		if !slices.Contains(supportedSubtitleFormats(), f) {
			return fmt.Errorf("%w: Subtitles.Formats contains unsupported format %q; must be vtt or srt",
				ErrValidation, f)
		}
	}

	return nil
}

// validateLanguageOptions checks that LanguageOptions is within allowed range.
func validateLanguageOptions(opts []string) error {
	if len(opts) == 0 {
		return nil
	}

	if len(opts) < minLanguageOptions || len(opts) > maxLanguageOptions {
		return fmt.Errorf("%w: LanguageOptions must contain %d-%d codes, got %d",
			ErrValidation, minLanguageOptions, maxLanguageOptions, len(opts))
	}

	for _, code := range opts {
		if !slices.Contains(supportedLanguageCodes(), code) {
			return fmt.Errorf("%w: unsupported LanguageCode %q in LanguageOptions", ErrValidation, code)
		}
	}

	return nil
}

// validateBaseModelName checks that a CLM base model name is valid.
func validateBaseModelName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: BaseModelName is required", ErrValidation)
	}

	if !slices.Contains(supportedBaseModelNames(), name) {
		return fmt.Errorf("%w: BaseModelName %q must be one of %v",
			ErrValidation, name, supportedBaseModelNames())
	}

	return nil
}

// validateMedicalSpecialty checks that a medical specialty is valid.
func validateMedicalSpecialty(specialty string) error {
	if specialty == "" {
		return fmt.Errorf("%w: Specialty is required for medical transcription", ErrValidation)
	}

	if !slices.Contains(supportedMedicalSpecialties(), specialty) {
		return fmt.Errorf("%w: Specialty %q must be one of %v",
			ErrValidation, specialty, supportedMedicalSpecialties())
	}

	return nil
}

// validateMedicalType checks that a medical transcription type is valid.
func validateMedicalType(typ string) error {
	if typ == "" {
		return fmt.Errorf("%w: Type is required for medical transcription", ErrValidation)
	}

	if !slices.Contains(supportedMedicalTypes(), typ) {
		return fmt.Errorf("%w: Type %q must be one of %v",
			ErrValidation, typ, supportedMedicalTypes())
	}

	return nil
}

// validateCallAnalyticsInputType checks that an InputType is valid.
func validateCallAnalyticsInputType(inputType string) error {
	if inputType != "" && !slices.Contains(supportedCallAnalyticsInputTypes(), inputType) {
		return fmt.Errorf("%w: InputType %q must be one of %v",
			ErrValidation, inputType, supportedCallAnalyticsInputTypes())
	}

	return nil
}

// validateVocabularyPhrases checks phrase count and length limits.
func validateVocabularyPhrases(phrases []string) error {
	if len(phrases) > maxVocabularyPhrases {
		return fmt.Errorf("%w: Phrases list exceeds maximum of %d items", ErrValidation, maxVocabularyPhrases)
	}

	for i, p := range phrases {
		if len(p) > maxVocabularyPhraseLen {
			return fmt.Errorf("%w: phrase[%d] exceeds maximum length of %d characters",
				ErrValidation, i, maxVocabularyPhraseLen)
		}
	}

	return nil
}

// validateVocabularyFilterWords checks the word count limit.
func validateVocabularyFilterWords(words []string) error {
	if len(words) > maxVocabularyFilterWords {
		return fmt.Errorf("%w: Words list exceeds maximum of %d items", ErrValidation, maxVocabularyFilterWords)
	}

	return nil
}

// validateChannelDefinitions checks that exactly 2 channel definitions are provided with distinct roles.
func validateChannelDefinitions(defs []ChannelDefinition) error {
	if len(defs) == 0 {
		return nil
	}

	if len(defs) != requiredChannelDefinitionCount {
		return fmt.Errorf("%w: ChannelDefinitions must contain exactly 2 entries (AGENT and CUSTOMER), got %d",
			ErrValidation, len(defs))
	}

	roles := make(map[string]bool)
	for _, d := range defs {
		if d.ParticipantRole == "" {
			return fmt.Errorf("%w: each ChannelDefinition must have a ParticipantRole", ErrValidation)
		}

		if d.ParticipantRole != "AGENT" && d.ParticipantRole != "CUSTOMER" {
			return fmt.Errorf("%w: ParticipantRole %q must be AGENT or CUSTOMER", ErrValidation, d.ParticipantRole)
		}

		if roles[d.ParticipantRole] {
			return fmt.Errorf(
				"%w: duplicate ParticipantRole %q in ChannelDefinitions",
				ErrValidation,
				d.ParticipantRole,
			)
		}

		roles[d.ParticipantRole] = true
	}

	return nil
}

// synthesizeTranscriptJSON generates a realistic Transcribe JSON transcript document.
// The transcript contains word-level items with synthetic timing derived from the job name.
func synthesizeTranscriptJSON(jobName, transcriptText string) []byte {
	// Build word-level items with deterministic timing.
	words := splitWords(transcriptText)
	items := make([]transcriptItem, 0, len(words))
	startSec := 0.0

	for i, word := range words {
		dur := syntheticWordBaseDuration + float64(len(word))*syntheticWordDurationPerChar
		items = append(items, transcriptItem{
			StartTime:    fmt.Sprintf("%.3f", startSec),
			EndTime:      fmt.Sprintf("%.3f", startSec+dur),
			Alternatives: []transcriptAlternative{{Content: word, Confidence: "0.99"}},
			Type:         "pronunciation",
		})

		// Add punctuation after every 7th word.
		if (i+1)%7 == 0 {
			items = append(items, transcriptItem{
				Alternatives: []transcriptAlternative{{Content: "."}},
				Type:         "punctuation",
			})
		}

		startSec += dur + syntheticWordGap
	}

	doc := transcriptDocument{
		JobName:   jobName,
		AccountID: defaultAccountID,
		Results: transcriptResults{
			Transcripts: []transcriptEntry{{Transcript: transcriptText}},
			Items:       items,
		},
		Status: "COMPLETED",
	}

	b, _ := marshalJSON(doc)

	return b
}

// splitWords splits text into words by spaces, filtering empties.
func splitWords(text string) []string {
	var words []string
	current := ""

	for _, ch := range text {
		if ch == ' ' || ch == '\t' {
			if current != "" {
				words = append(words, current)
				current = ""
			}
		} else {
			current += string(ch)
		}
	}

	if current != "" {
		words = append(words, current)
	}

	return words
}

// transcriptDocument is the top-level structure of a Transcribe output JSON file.
type transcriptDocument struct {
	JobName   string            `json:"jobName"`
	AccountID string            `json:"accountId"`
	Status    string            `json:"status"`
	Results   transcriptResults `json:"results"`
}

// transcriptResults holds the transcript content.
type transcriptResults struct {
	Transcripts []transcriptEntry `json:"transcripts"`
	Items       []transcriptItem  `json:"items"`
}

// transcriptEntry is a full-transcript string.
type transcriptEntry struct {
	Transcript string `json:"transcript"`
}

// transcriptItem is a word or punctuation item with timing.
type transcriptItem struct {
	StartTime    string                  `json:"start_time,omitempty"`
	EndTime      string                  `json:"end_time,omitempty"`
	Type         string                  `json:"type"`
	Alternatives []transcriptAlternative `json:"alternatives"`
}

// transcriptAlternative holds a word alternative with confidence.
type transcriptAlternative struct {
	Content    string `json:"content"`
	Confidence string `json:"confidence,omitempty"`
}
