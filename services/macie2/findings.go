package macie2

import (
	"sort"
	"time"

	"github.com/google/uuid"
)

// GetFindings retrieves findings by ID.
func (b *InMemoryBackend) GetFindings(findingIDs []string) ([]*Finding, error) {
	b.mu.RLock("GetFindings")
	defer b.mu.RUnlock()

	result := make([]*Finding, 0, len(findingIDs))

	for _, id := range findingIDs {
		f, ok := b.findings.Get(id)
		if !ok {
			return nil, ErrFindingNotFound
		}

		cp := f.Finding
		result = append(result, &cp)
	}

	return result, nil
}

// ListFindings returns finding IDs (optionally filtered).
func (b *InMemoryBackend) ListFindings(criteria map[string]any, limit int, token string) ([]string, string, error) {
	b.mu.RLock("ListFindings")
	defer b.mu.RUnlock()

	var filtered []string
	for _, finding := range b.findings.All() {
		if matchesFindingCriteria(finding, criteria) {
			filtered = append(filtered, finding.ID)
		}
	}

	sort.Strings(filtered)

	data, next := paginate(filtered, token, b.paginationSecret, limit)

	return data, next, nil
}

func getFindingFieldValue(finding *storedFinding, key string) string {
	switch key {
	case keyType:
		return finding.Type
	case "category":
		return finding.Category
	case "updatedAt":
		return finding.UpdatedAt.Format(time.RFC3339)
	case "severity.description":
		return finding.Severity.Description
	case "accountId":
		return finding.AccountID
	case "region":
		return finding.Region
	}

	return ""
}

func matchEq(fVal string, eqVals []any) bool {
	for _, eqV := range eqVals {
		if strV, sOk := eqV.(string); sOk && strV == fVal {
			return true
		}
	}

	return false
}

func matchNeq(fVal string, neqVals []any) bool {
	for _, neqV := range neqVals {
		if strV, sOk := neqV.(string); sOk && strV == fVal {
			return false
		}
	}

	return true
}

func matchesFindingCriteria(finding *storedFinding, criteria map[string]any) bool {
	if len(criteria) == 0 {
		return true
	}

	criterion, ok := criteria["criterion"].(map[string]any)
	if !ok || len(criterion) == 0 {
		return true
	}

	for k, v := range criterion {
		cond, cOk := v.(map[string]any)
		if !cOk {
			continue
		}

		fVal := getFindingFieldValue(finding, k)

		if eqVals, eqOk := cond["eq"].([]any); eqOk {
			if !matchEq(fVal, eqVals) {
				return false
			}
		}
		if neqVals, neqOk := cond["neq"].([]any); neqOk {
			if !matchNeq(fVal, neqVals) {
				return false
			}
		}
	}

	return true
}

// CreateSampleFindings creates sample findings.
func (b *InMemoryBackend) CreateSampleFindings(findingTypes []string) error {
	b.mu.Lock("CreateSampleFindings")
	defer b.mu.Unlock()

	types := findingTypes
	if len(types) == 0 {
		types = []string{"SensitiveData:S3Object/Personal"}
	}

	now := time.Now().UTC()

	for _, ft := range types {
		id := uuid.New().String()
		b.findings.Put(&storedFinding{
			Finding: Finding{
				AccountID:   b.accountID,
				Archived:    false,
				Category:    categorySensitiveData,
				CreatedAt:   now,
				Description: "Sample finding of type " + ft,
				ID:          id,
				Region:      b.region,
				Severity:    Severity{Description: "Medium", Score: defaultFindingScore},
				Title:       "Sample: " + ft,
				Type:        ft,
				UpdatedAt:   now,
			},
		})
	}

	return nil
}

// GetFindingStatistics returns statistics grouped by the given field.
func (b *InMemoryBackend) GetFindingStatistics(groupBy string, _ map[string]any) ([]FindingStatisticsGroup, error) {
	b.mu.RLock("GetFindingStatistics")
	defer b.mu.RUnlock()

	counts := make(map[string]int64)

	for _, f := range b.findings.All() {
		var key string

		switch groupBy {
		case "type":
			key = f.Type
		case "severity.description":
			key = f.Severity.Description
		case "resourcesAffected.s3Bucket.name":
			key = "unknown-bucket"
		case "classificationDetails.jobId":
			key = "unknown-job"
		default:
			key = f.Type
		}

		counts[key]++
	}

	result := make([]FindingStatisticsGroup, 0, len(counts))

	for k, v := range counts {
		result = append(result, FindingStatisticsGroup{GroupKey: k, Count: v})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].GroupKey < result[j].GroupKey })

	return result, nil
}

// GetFindingsPublicationConfiguration returns the findings publication config.
func (b *InMemoryBackend) GetFindingsPublicationConfiguration() (*FindingsPublicationConfig, error) {
	b.mu.RLock("GetFindingsPublicationConfiguration")
	defer b.mu.RUnlock()

	if b.findingsPubConfig == nil {
		return &FindingsPublicationConfig{}, nil
	}

	cp := *b.findingsPubConfig

	return &cp, nil
}

// PutFindingsPublicationConfiguration stores the findings publication config.
func (b *InMemoryBackend) PutFindingsPublicationConfiguration(cfg *FindingsPublicationConfig) error {
	b.mu.Lock("PutFindingsPublicationConfiguration")
	defer b.mu.Unlock()

	if cfg == nil {
		b.findingsPubConfig = nil

		return nil
	}

	cp := *cfg
	b.findingsPubConfig = &cp

	return nil
}
