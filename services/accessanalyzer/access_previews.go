package accessanalyzer

import (
	"encoding/json"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"
)

// CreateAccessPreview creates a new access preview. Configurations is
// required and must contain exactly one element (api_op_CreateAccessPreview.go:39-43).
func (b *InMemoryBackend) CreateAccessPreview(
	analyzerArn string,
	configurations map[string]json.RawMessage,
) (*AccessPreview, error) {
	if len(configurations) != 1 {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateAccessPreview")
	defer b.mu.Unlock()

	var found bool

	for _, a := range b.analyzers.All() {
		if a.Arn == analyzerArn {
			found = true

			break
		}
	}

	if !found {
		return nil, ErrAnalyzerNotFound
	}

	now := time.Now().UTC()
	ap := &AccessPreview{
		ID:             uuid.NewString(),
		AnalyzerArn:    analyzerArn,
		Status:         AccessPreviewStatusCompleted,
		CreatedAt:      now,
		Configurations: maps.Clone(configurations),
	}

	b.accessPreviews.Put(ap)

	return copyAccessPreview(ap), nil
}

// GetAccessPreview returns an access preview by ID.
func (b *InMemoryBackend) GetAccessPreview(accessPreviewID string) (*AccessPreview, error) {
	b.mu.RLock("GetAccessPreview")
	defer b.mu.RUnlock()

	ap, ok := b.accessPreviews.Get(accessPreviewID)
	if !ok {
		return nil, ErrAccessPreviewNotFound
	}

	return copyAccessPreview(ap), nil
}

// ListAccessPreviews returns all access previews for a given analyzerArn.
func (b *InMemoryBackend) ListAccessPreviews(analyzerArn string) ([]*AccessPreview, error) {
	b.mu.RLock("ListAccessPreviews")
	defer b.mu.RUnlock()

	result := make([]*AccessPreview, 0)

	for _, ap := range b.accessPreviews.All() {
		if analyzerArn != "" && ap.AnalyzerArn != analyzerArn {
			continue
		}

		result = append(result, copyAccessPreview(ap))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result, nil
}

// ListAccessPreviewFindings returns findings from the analyzer associated with the preview.
func (b *InMemoryBackend) ListAccessPreviewFindings(
	accessPreviewID string,
	maxResults int,
	nextToken string,
) ([]*Finding, string, error) {
	b.mu.RLock("ListAccessPreviewFindings")
	defer b.mu.RUnlock()

	ap, ok := b.accessPreviews.Get(accessPreviewID)
	if !ok {
		return nil, "", ErrAccessPreviewNotFound
	}

	// Find the analyzer by ARN.
	var analyzerName string

	for _, a := range b.analyzers.All() {
		if a.Arn == ap.AnalyzerArn {
			analyzerName = a.Name

			break
		}
	}

	if analyzerName == "" {
		return []*Finding{}, "", nil
	}

	group := b.findingsByAnalyzer.Get(analyzerName)
	findings := make([]*Finding, 0, len(group))

	for _, f := range group {
		findings = append(findings, copyFinding(f))
	}

	sort.Slice(findings, func(i, j int) bool {
		return findings[i].ID < findings[j].ID
	})

	start := 0

	if nextToken != "" {
		for i, f := range findings {
			if f.ID == nextToken {
				start = i

				break
			}
		}
	}

	findings = findings[start:]

	if maxResults > 0 && len(findings) > maxResults {
		return findings[:maxResults], findings[maxResults].ID, nil
	}

	return findings, "", nil
}
