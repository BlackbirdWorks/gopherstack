package polly

import (
	"fmt"
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

	b.mu.Lock()
	defer b.mu.Unlock()

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	lexicon, ok := b.lexicons.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: lexicon %q", ErrLexiconNotFound, name)
	}

	return cloneLexicon(lexicon), nil
}

// DeleteLexicon removes named lexicon.
func (b *InMemoryBackend) DeleteLexicon(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.lexicons.Delete(name) {
		return fmt.Errorf("%w: lexicon %q", ErrLexiconNotFound, name)
	}

	return nil
}

// ListLexicons lists lexicons ordered by name.
func (b *InMemoryBackend) ListLexicons() []*Lexicon {
	b.mu.RLock()
	all := b.lexicons.All()
	out := make([]*Lexicon, 0, len(all))
	for _, lexicon := range all {
		out = append(out, cloneLexicon(lexicon))
	}
	b.mu.RUnlock()

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

	return nil
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
