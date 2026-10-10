package ecr

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
)

const (
	repoNameMinLen  = 2
	repoNameMaxLen  = 256
	maxTagsPerRepo  = 50
	repoNamePattern = `(?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*[a-z0-9]+(?:[._-][a-z0-9]+)*`
)

var repoNameRe = regexp.MustCompile(`^` + repoNamePattern + `$`)

func invalidParam(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errInvalidRequest, fmt.Sprintf(format, args...))
}

func validateRepositoryName(name string) error {
	if len(name) < repoNameMinLen || len(name) > repoNameMaxLen || !repoNameRe.MatchString(name) {
		return invalidParam(
			"Invalid parameter at 'repositoryName' failed to satisfy constraint: "+
				"'must satisfy regular expression '%s' and be %d-%d characters'",
			repoNamePattern, repoNameMinLen, repoNameMaxLen)
	}

	return nil
}

func validateJSONObject(field, text string) error {
	var doc map[string]any
	if err := json.Unmarshal([]byte(text), &doc); err != nil || doc == nil {
		return invalidParam("Invalid parameter at '%s' failed to satisfy constraint: 'Invalid JSON syntax'", field)
	}

	return nil
}

func validateRepositoryPolicyText(text string) error {
	if err := validateJSONObject("policyText", text); err != nil {
		return invalidParam("Invalid repository policy provided")
	}

	return nil
}

func validateLifecyclePolicyText(text string) error {
	var doc lifecyclePolicyDoc
	if err := json.Unmarshal([]byte(text), &doc); err != nil {
		return invalidParam("Lifecycle policy validation failure: Invalid JSON")
	}

	seen := make(map[int]bool, len(doc.Rules))

	for _, r := range doc.Rules {
		switch {
		case r.RulePriority < 1:
			return invalidParam("Lifecycle policy validation failure: rulePriority must be at least 1")
		case seen[r.RulePriority]:
			return invalidParam("Lifecycle policy validation failure: duplicate rulePriority %d", r.RulePriority)
		case !slices.Contains([]string{"expire", "transition"}, r.Action.Type):
			return invalidParam("Lifecycle policy validation failure: action.type must be expire or transition")
		case !slices.Contains([]string{"tagged", "untagged", "any"}, r.Selection.TagStatus):
			return invalidParam(
				"Lifecycle policy validation failure: selection.tagStatus must be tagged, untagged or any",
			)
		case r.Selection.CountType == "":
			return invalidParam("Lifecycle policy validation failure: selection.countType is required")
		}

		seen[r.RulePriority] = true
	}

	return nil
}
