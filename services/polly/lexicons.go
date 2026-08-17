package polly

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// PutLexicon creates or replaces lexicon content.
func (b *InMemoryBackend) PutLexicon(name, content string) error {
	if err := validateLexicon(name, content); err != nil {
		return err
	}

	b.mu.Lock("PutLexicon")
	defer b.mu.Unlock()

	// A new lexicon (not an overwrite of an existing name) counts against the
	// per-account lexicon quota; overwriting an existing lexicon never does.
	if !b.lexicons.Has(name) && b.lexicons.Len() >= maxLexiconsPerAccount {
		return fmt.Errorf(
			"%w: account already has the maximum of %d lexicons", ErrMaxLexiconsNumberExceeded, maxLexiconsPerAccount,
		)
	}

	b.lexicons.Put(&Lexicon{
		Name:         name,
		ARN:          arn.Build("polly", b.region, b.accountID, "lexicon/"+name),
		Content:      content,
		Alphabet:     lexiconAttribute(content, "alphabet", "ipa"),
		LanguageCode: lexiconAttribute(content, "xml:lang", defaultLanguageCode),
		LexemesCount: strings.Count(content, "<lexeme>"),
		Size:         len(content),
		LastModified: time.Now().UTC(),
	})

	return nil
}

// GetLexicon returns named lexicon.
func (b *InMemoryBackend) GetLexicon(name string) (*Lexicon, error) {
	b.mu.RLock("GetLexicon")
	defer b.mu.RUnlock()

	lexicon, ok := b.lexicons.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: lexicon %q", ErrLexiconNotFound, name)
	}

	return cloneLexicon(lexicon), nil
}

// DeleteLexicon removes named lexicon.
func (b *InMemoryBackend) DeleteLexicon(name string) error {
	b.mu.Lock("DeleteLexicon")
	defer b.mu.Unlock()

	if !b.lexicons.Delete(name) {
		return fmt.Errorf("%w: lexicon %q", ErrLexiconNotFound, name)
	}

	return nil
}

// ListLexicons lists lexicons ordered by name.
func (b *InMemoryBackend) ListLexicons() []*Lexicon {
	var out []*Lexicon
	func() {
		b.mu.RLock("ListLexicons")
		defer b.mu.RUnlock()
		all := b.lexicons.All()
		out = make([]*Lexicon, 0, len(all))
		for _, lexicon := range all {
			out = append(out, cloneLexicon(lexicon))
		}
	}()

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

func validateLexicon(name, content string) error {
	if !validLexiconName(name) {
		return fmt.Errorf("%w: lexicon name must be 1-%d alphanumeric characters", ErrValidation, maxLexiconNameLen)
	}
	if content == "" || !strings.Contains(content, "<lexicon") {
		return fmt.Errorf("%w: Content must be PLS lexicon XML", ErrInvalidLexicon)
	}
	if len(content) > maxLexiconSize {
		return fmt.Errorf(
			"%w: lexicon Content exceeds maximum size of %d characters", ErrLexiconSizeExceeded, maxLexiconSize,
		)
	}

	alphabet := lexiconAttribute(content, "alphabet", "ipa")
	if alphabet != "ipa" && alphabet != "x-sampa" {
		return fmt.Errorf("%w: alphabet %q must be ipa or x-sampa", ErrUnsupportedPlsAlphabet, alphabet)
	}

	language := lexiconAttribute(content, "xml:lang", defaultLanguageCode)
	if !slices.Contains(validPollyLanguageCodes(), language) {
		return fmt.Errorf("%w: xml:lang %q is not a supported Polly language code", ErrUnsupportedPlsLanguage, language)
	}

	if oversized, tag := oversizedLexemeReplacement(content); oversized {
		return fmt.Errorf(
			"%w: <%s> replacement exceeds maximum length of %d characters",
			ErrMaxLexemeLengthExceeded, tag, maxLexemeReplacementLen,
		)
	}

	return nil
}

// oversizedLexemeReplacement scans content for <phoneme>/<alias> lexeme
// replacements (AWS: "up to 100 characters for each <phoneme> or <alias>
// replacement in a lexicon") and reports the first one exceeding the limit.
// Self-closing tags (<phoneme .../>) carry no inline replacement text and are
// skipped.
func oversizedLexemeReplacement(content string) (bool, string) {
	for _, tag := range []string{"phoneme", "alias"} {
		if lexemeReplacementTooLong(content, tag) {
			return true, tag
		}
	}

	return false, ""
}

func lexemeReplacementTooLong(content, tag string) bool {
	openTag, closeTag := "<"+tag, "</"+tag+">"
	pos := 0

	for {
		idx := strings.Index(content[pos:], openTag)
		if idx < 0 {
			return false
		}
		open := pos + idx

		nextEnd := open + len(openTag)
		if nextEnd < len(content) && content[nextEnd] != '>' && content[nextEnd] != ' ' && content[nextEnd] != '/' {
			// Not an exact tag match (e.g. a hypothetical "<phonemeX"); keep scanning.
			pos = open + len(openTag)

			continue
		}

		closeAngle := strings.IndexByte(content[open:], '>')
		if closeAngle < 0 {
			return false
		}
		tagEnd := open + closeAngle

		if content[tagEnd-1] == '/' {
			// Self-closing: <phoneme ph="..."/> has no inline replacement text.
			pos = tagEnd + 1

			continue
		}

		bodyStart := tagEnd + 1
		bodyLen := strings.Index(content[bodyStart:], closeTag)
		if bodyLen < 0 {
			return false
		}
		if bodyLen > maxLexemeReplacementLen {
			return true
		}
		pos = bodyStart + bodyLen + len(closeTag)
	}
}

// validPollyLanguageCodes returns every LanguageCode enum value from
// aws-sdk-go-v2/service/polly/types (pinned SDK version, see PARITY.md),
// used to validate a lexicon's xml:lang attribute.
func validPollyLanguageCodes() []string {
	return []string{
		"arb", "cmn-CN", "cy-GB", "da-DK", "de-DE", "en-AU", "en-GB", "en-GB-WLS",
		"en-IN", "en-US", "es-ES", "es-MX", "es-US", "fr-CA", "fr-FR", "is-IS",
		"it-IT", "ja-JP", "hi-IN", "ko-KR", "nb-NO", "nl-NL", "pl-PL", "pt-BR",
		"pt-PT", "ro-RO", "ru-RU", "sv-SE", "tr-TR", "en-NZ", "en-ZA", "ca-ES",
		"de-AT", "yue-CN", "ar-AE", "fi-FI", "en-IE", "nl-BE", "fr-BE", "cs-CZ",
		"de-CH", "en-SG",
	}
}

func validLexiconName(name string) bool {
	if name == "" || len(name) > maxLexiconNameLen {
		return false
	}
	for _, ch := range name {
		if (ch < 'A' || ch > 'Z') && (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') {
			return false
		}
	}

	return true
}

func lexiconAttribute(content, attr, fallback string) string {
	token := attr + `="`
	start := strings.Index(content, token)
	if start < 0 {
		return fallback
	}
	start += len(token)
	end := strings.IndexByte(content[start:], '"')
	if end < 0 {
		return fallback
	}

	return content[start : start+end]
}

func cloneLexicon(lexicon *Lexicon) *Lexicon {
	copyLexicon := *lexicon

	return &copyLexicon
}
