package translate

import (
	"encoding/base64"
	"fmt"
	"strings"
)

// --- Translation ---

// syncTextMaxBytes is TranslateText's synchronous translation quota: 10,000
// bytes, confirmed against BoundedLengthString's "max" in the smithy model
// and the Amazon Translate guidelines/quotas page's "Maximum input text:
// 10,000 bytes".
const syncTextMaxBytes = 10000

// syncDocumentMaxBytes is TranslateDocument's document size quota: 100,000
// bytes, confirmed against the DocumentContent blob shape's "max" in the
// smithy model.
const syncDocumentMaxBytes = 102400

// validSettingsEnums validates TranslationSettings' three fields against
// their modeled enums (Formality: FORMAL|INFORMAL, Profanity: MASK,
// Brevity: ON -- confirmed against the Formality/Profanity/Brevity shapes in
// the smithy model). allowBrevity is false for StartTextTranslationJob,
// whose API reference documents "Brevity: not supported" for batch jobs
// (unlike TranslateText/TranslateDocument, which both support it).
func validSettingsEnums(settings map[string]any, allowBrevity bool) error {
	if formality, ok := settings["Formality"].(string); ok && formality != "" &&
		formality != "FORMAL" && formality != "INFORMAL" {
		return fmt.Errorf("%w: Settings.Formality must be FORMAL or INFORMAL", ErrValidation)
	}

	if profanity, ok := settings["Profanity"].(string); ok && profanity != "" && profanity != "MASK" {
		return fmt.Errorf("%w: Settings.Profanity must be MASK", ErrValidation)
	}

	if brevity, ok := settings["Brevity"].(string); ok && brevity != "" {
		if !allowBrevity {
			return fmt.Errorf("%w: Settings.Brevity is not supported for this operation", ErrValidation)
		}

		if brevity != "ON" {
			return fmt.Errorf("%w: Settings.Brevity must be ON", ErrValidation)
		}
	}

	return nil
}

func (h *Handler) translateText(input map[string]any) (map[string]any, error) {
	text, _ := input["Text"].(string)
	if text == "" {
		return nil, fmt.Errorf("%w: Text is required", ErrValidation)
	}

	if len(text) > syncTextMaxBytes {
		return nil, fmt.Errorf("%w: Text exceeds the %d byte limit", ErrTextSizeLimitExceeded, syncTextMaxBytes)
	}

	sourceLang, _ := input[keySourceLanguageCode].(string)
	targetLang, _ := input[keyTargetLanguageCode].(string)

	if targetLang == "" {
		return nil, fmt.Errorf("%w: TargetLanguageCode is required", ErrValidation)
	}

	if sourceLang == "" {
		sourceLang = sourceLangAuto
	}

	if err := validateLanguagePair(sourceLang, []string{targetLang}); err != nil {
		return nil, err
	}

	settings, _ := input["Settings"].(map[string]any)
	if err := validSettingsEnums(settings, true); err != nil {
		return nil, err
	}

	termNames := strSliceField(input, "TerminologyNames")

	terms, err := h.Backend.LookupTerminologies(termNames)
	if err != nil {
		return nil, err
	}

	translated, applied := applyTranslation(text, sourceLang, targetLang, terms)

	appliedSettings := map[string]any{}
	if len(settings) > 0 {
		appliedSettings = settings
	}

	return map[string]any{
		"TranslatedText":       translated,
		keySourceLanguageCode:  sourceLang,
		keyTargetLanguageCode:  targetLang,
		"AppliedSettings":      appliedSettings,
		"AppliedTerminologies": buildAppliedTerminologies(terms, applied),
	}, nil
}

func (h *Handler) translateDocument(input map[string]any) (map[string]any, error) {
	sourceLang, _ := input[keySourceLanguageCode].(string)
	targetLang, _ := input[keyTargetLanguageCode].(string)

	if targetLang == "" {
		return nil, fmt.Errorf("%w: TargetLanguageCode is required", ErrValidation)
	}

	if sourceLang == "" {
		sourceLang = sourceLangAuto
	}

	if err := validateLanguagePair(sourceLang, []string{targetLang}); err != nil {
		return nil, err
	}

	doc, ok := input["Document"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: Document is required", ErrValidation)
	}

	// ContentType is a required member of Document (api-2.json); real AWS
	// uses it to select how the document is parsed/reassembled (plain text,
	// HTML, Word/PowerPoint/Excel, XLIFF). This emulator does not need to
	// branch on it to produce synthetic translated text, but a request that
	// omits it is still malformed and must be rejected the way a real
	// client-side-validated request never would reach the server missing it.
	if strField(doc, "ContentType") == "" {
		return nil, fmt.Errorf("%w: Document.ContentType is required", ErrValidation)
	}

	// Document.Content (and the response's TranslatedDocument.Content) is a
	// blob on the wire: the real SDK base64-encodes request content before
	// sending and base64-decodes the response, so both directions must be
	// handled here rather than treated as literal text.
	contentB64, _ := doc["Content"].(string)

	content, err := base64.StdEncoding.DecodeString(contentB64)
	if err != nil {
		return nil, fmt.Errorf("%w: Document.Content must be base64-encoded: %w", ErrValidation, err)
	}

	if len(content) > syncDocumentMaxBytes {
		return nil, fmt.Errorf("%w: Document.Content exceeds the %d byte limit", ErrLimitExceeded, syncDocumentMaxBytes)
	}

	settings, _ := input["Settings"].(map[string]any)
	if err = validSettingsEnums(settings, true); err != nil {
		return nil, err
	}

	termNames := strSliceField(input, "TerminologyNames")

	terms, err := h.Backend.LookupTerminologies(termNames)
	if err != nil {
		return nil, err
	}

	translated, applied := applyTranslation(string(content), sourceLang, targetLang, terms)

	appliedSettings := map[string]any{}
	if len(settings) > 0 {
		appliedSettings = settings
	}

	return map[string]any{
		"TranslatedDocument": map[string]any{
			"Content": base64.StdEncoding.EncodeToString([]byte(translated)),
		},
		keySourceLanguageCode:  sourceLang,
		keyTargetLanguageCode:  targetLang,
		"AppliedSettings":      appliedSettings,
		"AppliedTerminologies": buildAppliedTerminologies(terms, applied),
	}, nil
}

// termMatch is a single source/target term pair that was actually substituted
// into translated text by a custom terminology.
type termMatch struct {
	Source string
	Target string
}

// buildAppliedTerminologies builds the AppliedTerminologies response field from
// terminologies that were found in the backend. Real AWS returns each applied
// terminology by name, with a Terms slice listing the source/target pairs that
// were actually matched in the input (empty if none matched); matches holds
// those pairs keyed by terminology name, as produced by applyTranslation.
func buildAppliedTerminologies(terms []*Terminology, matches map[string][]termMatch) []map[string]any {
	out := make([]map[string]any, 0, len(terms))

	for _, t := range terms {
		termPairs := matches[t.Name]
		termList := make([]any, 0, len(termPairs))

		for _, m := range termPairs {
			termList = append(termList, map[string]any{
				"SourceText": m.Source,
				"TargetText": m.Target,
			})
		}

		out = append(out, map[string]any{
			"Name":  t.Name,
			"Terms": termList,
		})
	}

	return out
}

// applyTranslation applies terminology substitutions and a simple language transform.
// Terminologies take priority; remaining text gets a minimal transform to avoid echo.
// It returns the translated text and, per terminology name, the source/target
// pairs that were actually substituted (for AppliedTerminologies.Terms).
func applyTranslation(
	text, sourceLang, targetLang string,
	terms []*Terminology,
) (string, map[string][]termMatch) {
	if text == "" || sourceLang == targetLang {
		return text, nil
	}

	// Apply terminology CSV substitutions first.
	matches := make(map[string][]termMatch, len(terms))

	for _, term := range terms {
		if term.TerminologyData == nil || len(term.TerminologyData.File) == 0 {
			continue
		}

		var m []termMatch
		text, m = applyCSVTerminology(text, term.TerminologyData.File)

		if len(m) > 0 {
			matches[term.Name] = m
		}
	}

	// Simple lang-to-prefix map for a handful of common targets, so output
	// differs visibly from the source without requiring a real translation engine.
	langPrefixes := map[string]string{
		"es": "es: ", "fr": "fr: ", "de": "de: ", "ja": "ja: ", "zh": "zh: ",
		"pt": "pt: ", "it": "it: ", "ru": "ru: ", "ar": "ar: ", "ko": "ko: ",
		"nl": "nl: ", "pl": "pl: ", "sv": "sv: ", "tr": "tr: ", "hi": "hi: ",
	}

	// Only add prefix if text doesn't already have one (avoids double-prefixing on retry).
	prefix := langPrefixes[targetLang]
	if prefix != "" && !strings.HasPrefix(text, prefix) {
		text = prefix + text
	}

	return text, matches
}

// applyCSVTerminology parses a simple two-column CSV (source,target) and replaces
// occurrences of source terms in text with the corresponding target terms. The
// first line is the header row (source/target language codes, see
// parseCSVLanguages in terminologies.go) and is never treated as a term pair. It
// returns the transformed text and the pairs that were actually found and
// substituted (for AppliedTerminologies.Terms).
func applyCSVTerminology(text string, csvBytes []byte) (string, []termMatch) {
	const (
		csvColumns       = 2 // source,target
		minLinesWithData = 2 // header row + at least one term row
	)

	lines := strings.Split(strings.TrimSpace(string(csvBytes)), "\n")
	if len(lines) < minLinesWithData {
		// Only a header row (or no content): no term rows to apply.
		return text, nil
	}

	var matched []termMatch

	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ",", csvColumns)
		if len(parts) != csvColumns {
			continue
		}

		src := strings.TrimSpace(parts[0])
		tgt := strings.TrimSpace(parts[1])

		if src == "" || tgt == "" {
			continue
		}

		if strings.Contains(text, src) {
			text = strings.ReplaceAll(text, src, tgt)
			matched = append(matched, termMatch{Source: src, Target: tgt})
		}
	}

	return text, matched
}
