package comprehend

import (
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
	text, err := documentText(input)
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
	text, err := documentText(input)
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
	text, err := documentText(input)
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
	text, err := documentText(input)
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
	text, err := documentText(input)
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
	text, err := documentText(input)
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
	segments, _ := input["TextSegments"].([]any)
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

func (h *Handler) batch(detector operation) operation {
	return func(input map[string]any) (map[string]any, error) {
		texts, _ := input["TextList"].([]any)
		results := make([]map[string]any, 0, len(texts))
		for index, rawText := range texts {
			result, err := detector(map[string]any{fieldText: rawText, fieldLanguageCode: input[fieldLanguageCode]})
			if err != nil {
				return nil, err
			}
			result["Index"] = index
			results = append(results, result)
		}

		return map[string]any{"ResultList": results, "ErrorList": []any{}}, nil
	}
}

func documentText(input map[string]any) (string, error) {
	text := stringValue(input, fieldText, "")
	if strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("%w: Text is required", ErrValidation)
	}

	return text, nil
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
	text, err := documentText(input)
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
			entity := matchResult(text, cleaned, "PERSON")
			// Targeted sentiment specific fields
			mentions := []map[string]any{
				{
					"Score": defaultScore, "Text": cleaned, "Type": "PERSON",
					fieldBeginOffset: entity[fieldBeginOffset], fieldEndOffset: entity[fieldEndOffset],
					"MentionSentiment": map[string]any{
						"Sentiment": sentiment,
						"SentimentScore": map[string]float64{
							"Positive": lowSentimentScore, "Negative": lowSentimentScore,
							"Neutral": neutralSentimentScore, "Mixed": lowSentimentScore,
						},
					},
				},
			}
			entity["Mentions"] = mentions
			entities = append(entities, entity)
		}
	}

	return map[string]any{
		fieldEntities: entities,
	}, nil
}
