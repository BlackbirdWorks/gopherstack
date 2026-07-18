package comprehend

import (
	"regexp"
	"strings"
)

func (h *Handler) classifyDocument(input map[string]any) (map[string]any, error) {
	text, err := documentText(input)
	if err != nil {
		return nil, err
	}

	lower := strings.ToLower(text)
	className := "OTHER"
	switch {
	case strings.Contains(lower, "finance") || strings.Contains(lower, "money"):
		className = "FINANCE"
	case strings.Contains(lower, "sports") || strings.Contains(lower, "game"):
		className = "SPORTS"
	case strings.Contains(lower, "tech") || strings.Contains(lower, "computer"):
		className = "TECHNOLOGY"
	}

	return map[string]any{
		"Classes": []map[string]any{
			{fieldName: className, fieldScore: defaultScore},
		},
		fieldLabels: []map[string]any{
			{fieldName: className, fieldScore: defaultScore},
		},
	}, nil
}

func (h *Handler) containsPIIEntities(input map[string]any) (map[string]any, error) {
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
	seen := make(map[string]bool)
	labels := []map[string]any{}
	for _, pattern := range patterns {
		if pattern.expression.MatchString(text) && !seen[pattern.kind] {
			seen[pattern.kind] = true
			labels = append(labels, map[string]any{fieldName: pattern.kind, fieldScore: defaultScore})
		}
	}

	return map[string]any{
		fieldLabels: labels,
	}, nil
}
