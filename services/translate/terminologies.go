package translate

import (
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) terminologyARN(name string) string {
	return arn.Build("translate", b.region, b.accountID, "terminology/"+name)
}

// parseCSVLanguages extracts source/target language codes and term count from CSV bytes.
// CSV header row is: sourceLang,targetLang1[,targetLang2,...]; subsequent rows are terms.
func parseCSVLanguages(csvBytes []byte) (string, []string, int) {
	const minCols = 2

	lines := strings.Split(strings.TrimSpace(string(csvBytes)), "\n")
	if len(lines) == 0 {
		return "", nil, 0
	}

	var srcLang string
	var targets []string

	// Parse header line.
	header := strings.Split(strings.TrimSpace(lines[0]), ",")
	if len(header) >= minCols {
		srcLang = strings.TrimSpace(header[0])
		for _, col := range header[1:] {
			if t := strings.TrimSpace(col); t != "" {
				targets = append(targets, t)
			}
		}
	}

	// Count non-empty, non-comment data rows.
	termCount := 0
	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			termCount++
		}
	}

	return srcLang, targets, termCount
}

// ImportTerminology creates or overwrites a custom terminology.
func (b *InMemoryBackend) ImportTerminology(
	name, description string,
	data *TerminologyData,
	encKey *EncryptionKey,
	tags map[string]string,
) (*Terminology, error) {
	// ImportTerminology models InvalidParameterValueException but never
	// InvalidRequestException (api-2.json), so validation failures here use
	// ErrInvalidParameter, matching handler_terminologies.go's Get/Delete
	// counterparts.
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	if data == nil {
		return nil, fmt.Errorf("%w: TerminologyData is required", ErrInvalidParameter)
	}

	if !validDataFormatsTable()[data.Format] {
		return nil, fmt.Errorf("%w: TerminologyData.Format must be one of CSV, TMX, TSV", ErrInvalidParameter)
	}

	directionality := data.Directionality
	switch directionality {
	case "":
		directionality = directionalityUni
	case directionalityUni, directionalityMulti:
		// valid, keep as specified
	default:
		return nil, fmt.Errorf("%w: Directionality must be UNI or MULTI", ErrInvalidParameter)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	resourceARN := b.terminologyARN(name)

	// ImportTerminology's Tags replaces the resource's tag set wholesale
	// (unlike TagResource's add-or-replace merge below), so the limit
	// applies to the new set's size directly rather than a union with
	// whatever tags the resource already carries.
	if len(tags) > maxTagsPerResource {
		return nil, fmt.Errorf(
			"%w: resource %q would exceed the %d-tag limit",
			ErrTooManyTags,
			resourceARN,
			maxTagsPerResource,
		)
	}

	now := time.Now().UTC()

	srcLang, targetLangs, termCount := parseCSVLanguages(data.File)
	if srcLang == "" {
		srcLang = "en"
	}

	existing, exists := b.terminologies.Get(name)
	if exists {
		existing.Description = description
		existing.TerminologyData = data
		existing.EncryptionKey = encKey
		existing.LastUpdatedAt = now
		existing.Format = data.Format
		existing.SizeBytes = len(data.File)
		existing.SourceLanguage = srcLang
		existing.TargetLanguages = targetLangs
		existing.TermCount = termCount
		existing.Directionality = directionality

		if tags != nil {
			existing.Tags = tags
			b.tags[resourceARN] = copyMap(tags)
		}

		return existing, nil
	}

	term := &Terminology{
		ARN:             resourceARN,
		Name:            name,
		Description:     description,
		TerminologyData: data,
		EncryptionKey:   encKey,
		Tags:            tags,
		CreatedAt:       now,
		LastUpdatedAt:   now,
		Format:          data.Format,
		SizeBytes:       len(data.File),
		Directionality:  directionality,
		SourceLanguage:  srcLang,
		TargetLanguages: targetLangs,
		TermCount:       termCount,
	}
	b.terminologies.Put(term)

	if tags != nil {
		b.tags[resourceARN] = copyMap(tags)
	}

	return term, nil
}

// GetTerminology retrieves a terminology by name.
func (b *InMemoryBackend) GetTerminology(name string) (*Terminology, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, ok := b.terminologies.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: terminology %q not found", ErrNotFound, name)
	}

	return t, nil
}

// LookupTerminologies returns terminology entries for the given names. A
// name that does not correspond to any stored terminology is a
// ResourceNotFoundException (TranslateText/TranslateDocument both model this
// exception, and TerminologyNames is the only named-resource reference
// either operation makes -- real AWS's ListTerminologies doc note "Use the
// ListTerminologies operation to get the available terminology lists"
// implies the reference is validated, not silently ignored).
func (b *InMemoryBackend) LookupTerminologies(names []string) ([]*Terminology, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*Terminology, 0, len(names))

	for _, name := range names {
		t, ok := b.terminologies.Get(name)
		if !ok {
			return nil, fmt.Errorf("%w: terminology %q not found", ErrNotFound, name)
		}

		out = append(out, t)
	}

	return out, nil
}

// DeleteTerminology removes a terminology by name.
func (b *InMemoryBackend) DeleteTerminology(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.terminologies.Has(name) {
		return fmt.Errorf("%w: terminology %q not found", ErrNotFound, name)
	}

	resourceARN := b.terminologyARN(name)
	b.terminologies.Delete(name)
	delete(b.tags, resourceARN)

	return nil
}

// ListTerminologies returns a paginated list of terminologies.
func (b *InMemoryBackend) ListTerminologies(maxResults int, nextToken string) ([]*Terminology, string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := sortedNames(b.terminologies.All(), func(t *Terminology) string { return t.Name })

	return paginate(names, func(n string) *Terminology { return tableGet(b.terminologies, n) }, maxResults, nextToken)
}
