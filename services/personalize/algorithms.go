package personalize

import (
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// getBuiltinAlgorithms returns the static set of AWS Personalize built-in algorithms.
// Both the current (aws-* prefix) and legacy ARN styles are included.
func getBuiltinAlgorithms() []map[string]any {
	epoch := awstime.Epoch(time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC))
	const prefix = "arn:aws:personalize:::algorithm/"

	type algoEntry struct {
		arn     string
		name    string
		aliases []string
	}

	entries := []algoEntry{
		{prefix + "aws-user-personalization", "aws-user-personalization", []string{prefix + "user-personalization"}},
		{prefix + "aws-hrnn", "aws-hrnn", nil},
		{prefix + "aws-hrnn-coldstart", "aws-hrnn-coldstart", nil},
		{prefix + "aws-hrnn-metadata", "aws-hrnn-metadata", nil},
		{prefix + "aws-similar-items", "aws-similar-items", []string{prefix + "sims"}},
		{prefix + "aws-popularity-count", "aws-popularity-count", nil},
		{prefix + "aws-personalized-ranking", "aws-personalized-ranking", []string{prefix + "personalized-ranking"}},
		{prefix + "aws-sims", "aws-sims", nil},
	}

	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		m := map[string]any{
			"algorithmArn":         e.arn,
			keyName:                e.name,
			keyStatus:              statusActive,
			keyCreationDateTime:    epoch,
			keyLastUpdatedDateTime: epoch,
		}
		out = append(out, m)
		for _, alias := range e.aliases {
			aliasM := map[string]any{
				"algorithmArn":         alias,
				keyName:                e.name,
				keyStatus:              statusActive,
				keyCreationDateTime:    epoch,
				keyLastUpdatedDateTime: epoch,
			}
			out = append(out, aliasM)
		}
	}

	return out
}

// --- Algorithm (read-only) ---

func (h *Handler) describeAlgorithm(input map[string]any) (map[string]any, error) {
	algorithmArn, _ := input["algorithmArn"].(string)

	for _, algo := range getBuiltinAlgorithms() {
		if algo["algorithmArn"] == algorithmArn {
			return map[string]any{"algorithm": algo}, nil
		}
	}

	// Fall back to name-based match (strip the ARN prefix).
	name := strings.TrimPrefix(algorithmArn, "arn:aws:personalize:::algorithm/")
	for _, algo := range getBuiltinAlgorithms() {
		if algo[keyName] == name {
			return map[string]any{"algorithm": algo}, nil
		}
	}

	return nil, fmt.Errorf("%w: algorithm %q not found", ErrNotFound, algorithmArn)
}
