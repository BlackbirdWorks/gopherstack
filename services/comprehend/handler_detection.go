package comprehend

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// PII detection patterns, compiled once at package init rather than per request.
var (
	piiEmailRe = regexp.MustCompile(`[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}`)
	piiSSNRe   = regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)
)

const (
	sentimentMixedScore   = 0.45
	sentimentBaseScore    = 0.92
	sentimentMinScore     = 0.01
	sentimentNeutralScore = 0.06
	sentimentMaxScore     = 0.99
	sentimentMixedMin     = 0.05

	// Per-operation text byte limits, field-diffed against each op's doc
	// comment in aws-sdk-go-v2/service/comprehend (e.g. DetectSentimentInput.Text:
	// "The maximum string size is 5 KB"; DetectKeyPhrasesInput.Text: "must
	// contain less than 100 KB"). Real AWS returns TextSizeLimitExceededException
	// when a request's Text exceeds its operation's limit.
	textLimit5KB   = 5000
	textLimit100KB = 100000

	// batchMaxItems/batchItemLimit back BatchSizeLimitExceededException (>25
	// documents in one TextList, a whole-request rejection) and the
	// per-item 5KB limit every Batch* doc comment documents
	// ("A list containing... maximum of 25 documents. The maximum size of
	// each document is 5 KB.") -- an oversized item is a per-item
	// BatchItemError, not a whole-request rejection.
	batchMaxItems   = 25
	batchItemLimit  = 5000
	toxicSegmentCap = 1024  // "Each string has a maximum size of 1 KB"
	toxicTotalCap   = 10240 // "the maximum size of the list is 10 KB"
)

// generalLanguageCodes/syntaxLanguageCodes/englishOnlyLanguageCodes back
// LanguageCode validation, field-diffed against types.LanguageCode's 12
// enum values and DetectSyntaxInput's narrower types.SyntaxLanguageCode (6
// values) in aws-sdk-go-v2/service/comprehend/types/enums.go. Computed once
// at package init (not rebuilt per request like the word lists above) since
// these are consulted on every Detect*/BatchDetect* call.
//
//nolint:gochecknoglobals // read-only lookup tables, analogous to apigatewayv2's onceOpTable
var (
	generalLanguageCodes = map[string]bool{
		"en": true, "es": true, "fr": true, "de": true, "it": true, "pt": true,
		"ar": true, "hi": true, "ja": true, "ko": true, "zh": true, "zh-TW": true,
	}
	syntaxLanguageCodes = map[string]bool{
		"en": true, "es": true, "fr": true, "de": true, "it": true, "pt": true,
	}
	// englishOnlyLanguageCodes backs DetectToxicContent/DetectTargetedSentiment
	// (and their Batch counterparts), whose input still types LanguageCode as
	// the general 12-value enum but whose doc comments state "Currently,
	// English is the only supported language" -- any other (otherwise valid)
	// LanguageCode value must be rejected with UnsupportedLanguageException.
	englishOnlyLanguageCodes = map[string]bool{"en": true}
)

func positiveWordList() []string {
	return []string{
		"great", "love", "excellent", "wonderful", "amazing", "good", "happy", "best",
		"fantastic", "awesome", "beautiful", "perfect", "superb", "outstanding", "brilliant",
		"delightful", "pleased", "enjoy", "liked", "satisfied",
	}
}

func negativeWordList() []string {
	return []string{
		"bad", "hate", "terrible", "awful", "horrible", "worst", "angry", "sad",
		"disappointing", "poor", "dreadful", "disgusting", "upset", "frustrating",
		"dislike", "failed", "useless", "broken", "wrong", "awful",
	}
}

func countSentimentWords(wordSet map[string]bool, lower string, words []string) int {
	count := 0
	for _, w := range words {
		if wordSet[w] || strings.Contains(lower, w) {
			count++
		}
	}

	return count
}

func sentimentResult(posCount, negCount int) (string, float64, float64, float64, float64) {
	switch {
	case posCount > 0 && negCount > 0:
		return "MIXED", sentimentMixedScore, sentimentMixedScore, sentimentMixedMin, sentimentMixedMin
	case posCount > 0:
		ps := min(sentimentBaseScore+float64(posCount)*sentimentMinScore, sentimentMaxScore)

		return "POSITIVE", ps, sentimentMinScore, sentimentNeutralScore, sentimentMinScore
	case negCount > 0:
		ns := min(sentimentBaseScore+float64(negCount)*sentimentMinScore, sentimentMaxScore)

		return "NEGATIVE", sentimentMinScore, ns, sentimentNeutralScore, sentimentMinScore
	default:
		return "NEUTRAL", lowSentimentScore, lowSentimentScore, neutralSentimentScore, lowSentimentScore
	}
}

func (h *Handler) detectSentiment(input map[string]any) (map[string]any, error) {
	if err := requireLanguageCode(input, generalLanguageCodes); err != nil {
		return nil, err
	}
	text, err := documentText(input, textLimit5KB)
	if err != nil {
		return nil, err
	}
	lower := strings.ToLower(text)
	words := strings.Fields(lower)
	wordSet := make(map[string]bool, len(words))
	for _, w := range words {
		wordSet[strings.Trim(w, ".,!?;:")] = true
	}

	posCount := countSentimentWords(wordSet, lower, positiveWordList())
	negCount := countSentimentWords(wordSet, lower, negativeWordList())
	sentiment, posScore, negScore, neuScore, mixScore := sentimentResult(posCount, negCount)

	return map[string]any{
		"Sentiment": sentiment,
		"SentimentScore": map[string]float64{
			"Positive": posScore, "Negative": negScore,
			"Neutral": neuScore, "Mixed": mixScore,
		},
	}, nil
}

func orgSuffixList() []string {
	return []string{
		" inc", " inc.", " corp", " corp.", " ltd", " ltd.", " llc", " co.", " company",
		" university", " institute", " foundation", " association", " corporation",
		" group", " holdings", " technologies", " solutions",
	}
}

func locSuffixList() []string {
	return []string{
		" street", " avenue", " road", " drive", " boulevard", " lane", " way",
		" city", " town", " village", " county", " state", " country", " nation",
		" river", " lake", " mountain", " park",
	}
}

func locPrefixList() []string {
	return []string{
		"mount ", "lake ", "north ", "south ", "east ", "west ", "new ",
	}
}

func quantityWordList() []string {
	return []string{
		"thousand", "million", "billion", "percent", "kg", "lb", "km", "mile",
	}
}

func dateWordList() []string {
	return []string{
		"january", "february", "march", "april", "may", "june", "july", "august",
		"september", "october", "november", "december", "monday", "tuesday",
		"wednesday", "thursday", "friday", "saturday", "sunday", "yesterday",
		"tomorrow", "today",
	}
}

func entityType(word, textLower string) string {
	wl := strings.ToLower(word)
	for _, sfx := range orgSuffixList() {
		if strings.HasSuffix(textLower, wl+sfx) || strings.Contains(textLower, wl+sfx+" ") {
			return "ORGANIZATION"
		}
	}
	for _, sfx := range locSuffixList() {
		if strings.HasSuffix(wl, strings.TrimSpace(sfx)) {
			return "LOCATION"
		}
	}
	for _, pfx := range locPrefixList() {
		if strings.HasPrefix(wl, strings.TrimSpace(pfx)) {
			return "LOCATION"
		}
	}
	for _, q := range quantityWordList() {
		if strings.Contains(wl, q) {
			return "QUANTITY"
		}
	}
	for _, d := range dateWordList() {
		if strings.EqualFold(word, d) {
			return "DATE"
		}
	}

	return "PERSON"
}

func (h *Handler) detectEntities(input map[string]any) (map[string]any, error) {
	// LanguageCode is NOT required on DetectEntities (unlike the other
	// Detect* ops): a caller may supply EndpointArn for a custom entity
	// recognition model instead, in which case AWS uses the custom model's
	// language and ignores LanguageCode entirely. Only validate the value
	// when one is actually supplied.
	if code := stringValue(input, fieldLanguageCode, ""); code != "" {
		if err := validateLanguageCode(code, generalLanguageCodes); err != nil {
			return nil, err
		}
	}
	text, err := documentText(input, textLimit100KB)
	if err != nil {
		return nil, err
	}
	textLower := strings.ToLower(text)
	entities := make([]map[string]any, 0)
	for word := range strings.FieldsSeq(text) {
		cleaned := strings.Trim(word, ".,!?;:")
		if cleaned == "" {
			continue
		}
		r, _ := utf8.DecodeRuneInString(cleaned)
		if !unicode.IsUpper(r) {
			continue
		}
		kind := entityType(cleaned, textLower)
		entities = append(entities, matchResult(text, cleaned, kind))
	}

	return map[string]any{"Entities": entities}, nil
}

func (h *Handler) detectKeyPhrases(input map[string]any) (map[string]any, error) {
	if err := requireLanguageCode(input, generalLanguageCodes); err != nil {
		return nil, err
	}
	text, err := documentText(input, textLimit100KB)
	if err != nil {
		return nil, err
	}
	phrase := strings.TrimSpace(text)
	if index := strings.IndexAny(phrase, ".!?"); index >= 0 {
		phrase = phrase[:index]
	}

	return map[string]any{"KeyPhrases": []map[string]any{matchResult(text, phrase, "")}}, nil
}

func (h *Handler) detectPIIEntities(input map[string]any) (map[string]any, error) {
	if err := requireLanguageCode(input, generalLanguageCodes); err != nil {
		return nil, err
	}
	text, err := documentText(input, textLimit100KB)
	if err != nil {
		return nil, err
	}
	patterns := []struct {
		expression *regexp.Regexp
		kind       string
	}{
		{piiEmailRe, "EMAIL"},
		{piiSSNRe, "SSN"},
	}
	entities := make([]map[string]any, 0)
	for _, pattern := range patterns {
		for _, match := range pattern.expression.FindAllString(text, -1) {
			entities = append(entities, matchResult(text, match, pattern.kind))
		}
	}

	return map[string]any{"Entities": entities}, nil
}

func (h *Handler) detectSyntax(input map[string]any) (map[string]any, error) {
	// DetectSyntax types LanguageCode as the narrower 6-value SyntaxLanguageCode
	// enum (de/en/es/fr/it/pt), not the general 12-value LanguageCode enum
	// every other Detect* op here uses.
	if err := requireLanguageCode(input, syntaxLanguageCodes); err != nil {
		return nil, err
	}
	text, err := documentText(input, textLimit5KB)
	if err != nil {
		return nil, err
	}
	tokens := make([]map[string]any, 0)
	searchFrom := 0
	for index, token := range strings.Fields(text) {
		idx := strings.Index(text[searchFrom:], token)
		if idx < 0 {
			continue
		}
		begin := searchFrom + idx
		end := begin + len(token)
		tokens = append(tokens, map[string]any{
			"TokenId": index + 1, fieldText: token, fieldBeginOffset: begin, fieldEndOffset: end,
			"PartOfSpeech": map[string]any{"Tag": "NOUN", fieldScore: defaultScore},
		})
		searchFrom = end
	}

	return map[string]any{"SyntaxTokens": tokens}, nil
}

func (h *Handler) detectDominantLanguage(input map[string]any) (map[string]any, error) {
	// DetectDominantLanguageInput has no LanguageCode field at all -- it is
	// the one op that infers the language rather than requiring it.
	text, err := documentText(input, textLimit100KB)
	if err != nil {
		return nil, err
	}

	lang := dominantLanguage(text)

	return map[string]any{
		"Languages": []map[string]any{{fieldLanguageCode: lang, fieldScore: defaultScore}},
	}, nil
}

const asciiMaxChar = 127

type scriptCounts struct {
	cjk, cyrillic, arabic, devanagari, hebrew, latin, nonASCII int
}

func isCJK(r rune) bool {
	return (r >= 0x4E00 && r <= 0x9FFF) || // CJK Unified Ideographs
		(r >= 0x3040 && r <= 0x30FF) || // Hiragana/Katakana
		(r >= 0xAC00 && r <= 0xD7AF) // Hangul
}

func isLatinLetter(r rune) bool {
	return (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z')
}

func classifyRune(r rune, c *scriptCounts) {
	switch {
	case isCJK(r):
		c.cjk++
	case r >= 0x0400 && r <= 0x04FF: // Cyrillic
		c.cyrillic++
	case r >= 0x0600 && r <= 0x06FF: // Arabic
		c.arabic++
	case r >= 0x0590 && r <= 0x05FF: // Hebrew
		c.hebrew++
	case r >= 0x0900 && r <= 0x097F: // Devanagari
		c.devanagari++
	case isLatinLetter(r):
		c.latin++
	case r > asciiMaxChar:
		c.nonASCII++
	}
}

func countScripts(text string) scriptCounts {
	var c scriptCounts
	for _, r := range text {
		classifyRune(r, &c)
	}

	return c
}

func dominantLanguage(text string) string {
	c := countScripts(text)
	total := c.cjk + c.cyrillic + c.arabic + c.devanagari + c.hebrew + c.nonASCII
	if total == 0 {
		return "en"
	}
	switch {
	case c.cjk*2 > total:
		return "zh"
	case c.cyrillic*2 > total:
		return "ru"
	case c.arabic*2 > total:
		return "ar"
	case c.devanagari*2 > total:
		return "hi"
	case c.hebrew*2 > total:
		return "he"
	case c.nonASCII > c.latin:
		return "fr"
	default:
		return "en"
	}
}

func (h *Handler) detectToxicContent(input map[string]any) (map[string]any, error) {
	// "Currently, English is the only supported language" per
	// DetectToxicContentInput's doc comment, despite LanguageCode being
	// typed as the general enum.
	if err := requireLanguageCode(input, englishOnlyLanguageCodes); err != nil {
		return nil, err
	}

	segments, _ := input["TextSegments"].([]any)
	totalBytes := 0
	for _, segment := range segments {
		entry, _ := segment.(map[string]any)
		text := stringValue(entry, fieldText, "")
		if len(text) > toxicSegmentCap {
			return nil, fmt.Errorf("%w: text segment of %d bytes exceeds the %d byte per-segment limit",
				ErrTextSizeLimitExceeded, len(text), toxicSegmentCap)
		}
		totalBytes += len(text)
	}
	if totalBytes > toxicTotalCap {
		return nil, fmt.Errorf("%w: TextSegments total of %d bytes exceeds the %d byte limit",
			ErrTextSizeLimitExceeded, totalBytes, toxicTotalCap)
	}

	result := make([]map[string]any, 0, len(segments))
	for _, segment := range segments {
		entry, _ := segment.(map[string]any)
		text := stringValue(entry, fieldText, "")
		score := 0.01
		if strings.Contains(strings.ToLower(text), "hate") {
			score = 0.99
		}
		result = append(result, map[string]any{
			"Toxicity":  score,
			fieldLabels: []map[string]any{{fieldName: "HATE_SPEECH", fieldScore: score}},
		})
	}

	return map[string]any{"ResultList": result}, nil
}

// batchItemErrorCode maps an internal validation error to the wire ErrorCode
// string a real BatchItemError entry carries for that failure class.
func batchItemErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrTextSizeLimitExceeded):
		return "TEXT_SIZE_LIMIT_EXCEEDED"
	case errors.Is(err, ErrUnsupportedLanguage):
		return "UNSUPPORTED_LANGUAGE"
	default:
		return "INVALID_REQUEST"
	}
}

// batch wraps a single-document detector as a Batch* operation: it enforces
// the whole-request 25-item limit (BatchSizeLimitExceededException) and the
// shared LanguageCode once up front, then processes each TextList entry
// against the per-item 5KB limit, routing any per-item failure into
// ErrorList (matching each item's Index) instead of the ResultList rather
// than aborting the whole batch -- "If there are no errors in the batch, the
// ErrorList is empty" per every Batch*Output doc comment, implying
// per-item failures are expected, ordinary batch outcomes.
func (h *Handler) batch(detector operation, allowedLanguages map[string]bool) operation {
	return func(input map[string]any) (map[string]any, error) {
		texts, _ := input["TextList"].([]any)
		if len(texts) > batchMaxItems {
			return nil, fmt.Errorf("%w: TextList has %d documents, exceeding the %d document limit",
				ErrBatchSizeLimitExceeded, len(texts), batchMaxItems)
		}
		if allowedLanguages != nil {
			if err := requireLanguageCode(input, allowedLanguages); err != nil {
				return nil, err
			}
		}

		results := make([]map[string]any, 0, len(texts))
		errorList := make([]map[string]any, 0)
		for index, rawText := range texts {
			text, _ := rawText.(string)
			if len(text) > batchItemLimit {
				errorList = append(errorList, map[string]any{
					"Index":     index,
					"ErrorCode": "TEXT_SIZE_LIMIT_EXCEEDED",
					"ErrorMessage": fmt.Sprintf(
						"input text of %d bytes exceeds the %d byte limit",
						len(text),
						batchItemLimit,
					),
				})

				continue
			}

			result, err := detector(map[string]any{fieldText: rawText, fieldLanguageCode: input[fieldLanguageCode]})
			if err != nil {
				errorList = append(errorList, map[string]any{
					"Index": index, "ErrorCode": batchItemErrorCode(err), "ErrorMessage": err.Error(),
				})

				continue
			}
			result["Index"] = index
			results = append(results, result)
		}

		return map[string]any{"ResultList": results, "ErrorList": errorList}, nil
	}
}

// documentText extracts and validates the Text field of a single-document
// request against limit (an operation-specific byte cap; see the textLimit*
// constants). limit <= 0 means no cap.
func documentText(input map[string]any, limit int) (string, error) {
	text := stringValue(input, fieldText, "")
	if strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("%w: Text is required", ErrValidation)
	}
	if limit > 0 && len(text) > limit {
		return "", fmt.Errorf("%w: input text of %d bytes exceeds the %d byte limit for this operation",
			ErrTextSizeLimitExceeded, len(text), limit)
	}

	return text, nil
}

// validateLanguageCode rejects a LanguageCode not present in allowed.
func validateLanguageCode(code string, allowed map[string]bool) error {
	if !allowed[code] {
		return fmt.Errorf("%w: language %q is not supported by this operation", ErrUnsupportedLanguage, code)
	}

	return nil
}

// requireLanguageCode validates a required LanguageCode field against
// allowed. Real AWS models LanguageCode as required on every
// Detect*/BatchDetect* operation here except DetectEntities (which allows an
// EndpointArn instead) and DetectDominantLanguage (which has no LanguageCode
// field at all).
func requireLanguageCode(input map[string]any, allowed map[string]bool) error {
	code := stringValue(input, fieldLanguageCode, "")
	if code == "" {
		return fmt.Errorf("%w: LanguageCode is required", ErrValidation)
	}

	return validateLanguageCode(code, allowed)
}

func matchResult(text, match, kind string) map[string]any {
	begin := strings.Index(text, match)
	begin = max(begin, 0)
	out := map[string]any{
		fieldText: match, fieldScore: defaultScore, fieldBeginOffset: begin, fieldEndOffset: begin + len(match),
	}
	if kind != "" {
		out["Type"] = kind
	}

	return out
}

func (h *Handler) detectTargetedSentiment(input map[string]any) (map[string]any, error) {
	// "Currently, English is the only supported language" per
	// DetectTargetedSentimentInput's doc comment.
	if err := requireLanguageCode(input, englishOnlyLanguageCodes); err != nil {
		return nil, err
	}
	text, err := documentText(input, textLimit5KB)
	if err != nil {
		return nil, err
	}

	entities := make([]map[string]any, 0)
	lower := strings.ToLower(text)
	sentiment := "NEUTRAL"
	switch {
	case strings.Contains(lower, "great") || strings.Contains(lower, "love") || strings.Contains(lower, "excellent"):
		sentiment = "POSITIVE"
	case strings.Contains(lower, "bad") || strings.Contains(lower, "hate") || strings.Contains(lower, "terrible"):
		sentiment = "NEGATIVE"
	}

	for word := range strings.FieldsSeq(text) {
		cleaned := strings.Trim(word, ".,!?")
		if cleaned != "" && unicode.IsUpper(rune(cleaned[0])) {
			mention := matchResult(text, cleaned, "PERSON")
			mention["MentionSentiment"] = map[string]any{
				"Sentiment": sentiment,
				"SentimentScore": map[string]float64{
					"Positive": lowSentimentScore, "Negative": lowSentimentScore,
					"Neutral": neutralSentimentScore, "Mixed": lowSentimentScore,
				},
			}
			// types.TargetedSentimentEntity carries only
			// DescriptiveMentionIndex+Mentions -- Text/Score/BeginOffset/
			// EndOffset/Type live one level down, on each
			// types.TargetedSentimentMention in Mentions.
			entities = append(entities, map[string]any{
				"DescriptiveMentionIndex": []int{0},
				"Mentions":                []map[string]any{mention},
			})
		}
	}

	return map[string]any{
		fieldEntities: entities,
	}, nil
}
