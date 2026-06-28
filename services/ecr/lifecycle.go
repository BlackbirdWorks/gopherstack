package ecr

import (
	"encoding/json"
	"sort"
	"strings"
	"time"
)

const weeksPerDays = 7

// lifecyclePolicyDoc is the top-level structure of an ECR lifecycle policy.
type lifecyclePolicyDoc struct {
	Rules []lifecyclePolicyRule `json:"rules"`
}

// lifecyclePolicyRule is a single rule in an ECR lifecycle policy.
type lifecyclePolicyRule struct {
	Description  string                `json:"description,omitempty"`
	Action       lifecyclePolicyAction `json:"action"`
	Selection    lifecyclePolicySelect `json:"selection"`
	RulePriority int                   `json:"rulePriority"`
}

// lifecyclePolicySelect describes which images a rule targets.
type lifecyclePolicySelect struct {
	TagStatus      string   `json:"tagStatus"`
	CountType      string   `json:"countType"`
	CountUnit      string   `json:"countUnit,omitempty"`
	TagPatternList []string `json:"tagPatternList,omitempty"`
	TagPrefixList  []string `json:"tagPrefixList,omitempty"`
	CountNumber    int      `json:"countNumber"`
}

// lifecyclePolicyAction specifies what to do with matched images.
type lifecyclePolicyAction struct {
	Type string `json:"type"`
}

// imageEntry is used internally by lifecycle policy evaluation to track which
// images have already been matched by a rule.
type imageEntry struct {
	img     *Image
	allTags []string // all tags for this image, from digestTagsIndex
	matched bool
}

// evaluateLifecyclePolicy applies the lifecycle policy rules against the given
// images and returns the identifiers of images that would be deleted.
// Rules are evaluated in ascending rulePriority order. An image may only match
// one rule (first-match wins by priority).
// digestTags maps image digest → all tags for that image (from digestTagsIndex).
func evaluateLifecyclePolicy(
	policyText string,
	images map[string]*Image,
	digestTags map[string][]string,
) []ImageIdentifier {
	if policyText == "" {
		return nil
	}

	var doc lifecyclePolicyDoc
	if err := json.Unmarshal([]byte(policyText), &doc); err != nil {
		return nil
	}

	if len(doc.Rules) == 0 {
		return nil
	}

	// Sort rules by priority (ascending — lower number = higher precedence).
	rules := make([]lifecyclePolicyRule, len(doc.Rules))
	copy(rules, doc.Rules)
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].RulePriority < rules[j].RulePriority
	})

	// Convert to a slice so we can sort and mark matched.
	entries := make([]*imageEntry, 0, len(images))
	for _, img := range images {
		cp := img
		// Collect all tags for this image: from digestTagsIndex first, then fall back
		// to the primary tag stored on the image itself.
		tags := digestTags[img.ImageDigest]
		if len(tags) == 0 && img.ImageID.ImageTag != "" {
			tags = []string{img.ImageID.ImageTag}
		}
		entries = append(entries, &imageEntry{img: cp, allTags: tags})
	}

	// Sort images by push time descending (newest first) so count-based rules
	// keep the N most recent images and expire the rest.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].img.ImagePushedAt.After(entries[j].img.ImagePushedAt)
	})

	var expired []ImageIdentifier

	for _, rule := range rules {
		if strings.ToLower(rule.Action.Type) != "expire" {
			continue
		}

		matched := applyRule(rule, entries)
		for _, e := range matched {
			if !e.matched {
				e.matched = true
				expired = append(expired, e.img.ImageID)
			}
		}
	}

	return expired
}

// applyRule returns the entries that match the given rule (ignoring already-matched ones).
func applyRule(rule lifecyclePolicyRule, entries []*imageEntry) []*imageEntry {
	sel := rule.Selection
	now := time.Now()

	// Filter candidates that match the tag status / pattern criteria.
	candidates := make([]*imageEntry, 0, len(entries))

	for _, e := range entries {
		if e.matched {
			continue
		}

		if !matchesTagStatus(sel, e.img, e.allTags) {
			continue
		}

		candidates = append(candidates, e)
	}

	switch sel.CountType {
	case "imageCountMoreThan":
		// Keep the first CountNumber images; expire the rest.
		if len(candidates) <= sel.CountNumber {
			return nil
		}

		return candidates[sel.CountNumber:]

	case "sinceImagePushed":
		if sel.CountUnit == "" {
			sel.CountUnit = "days"
		}

		threshold := ageThreshold(now, sel.CountNumber, sel.CountUnit)
		var expired []*imageEntry

		for _, e := range candidates {
			if e.img.ImagePushedAt.Before(threshold) {
				expired = append(expired, e)
			}
		}

		return expired
	}

	return nil
}

// matchesTagStatus reports whether an image matches the tagStatus (and optional
// tag pattern) portion of a lifecycle rule selection.
// allTags is the full set of tags for the image from digestTagsIndex.
func matchesTagStatus(sel lifecyclePolicySelect, _ *Image, allTags []string) bool {
	switch strings.ToLower(sel.TagStatus) {
	case "untagged":
		return len(allTags) == 0

	case "tagged":
		if len(allTags) == 0 {
			return false
		}

		// If patterns are specified, at least one tag must match at least one pattern.
		//nolint:gocritic // intentional append-to-new-slice
		patterns := append(sel.TagPatternList, sel.TagPrefixList...)
		if len(patterns) == 0 {
			return true
		}

		for _, tag := range allTags {
			for _, p := range patterns {
				if tagMatchesPattern(tag, p) {
					return true
				}
			}
		}

		return false

	case "any":
		return true
	}

	return false
}

// tagMatchesPattern does simple prefix/wildcard matching for ECR tag patterns.
// ECR supports patterns like "v*" (prefix with wildcard at end).
func tagMatchesPattern(tag, pattern string) bool {
	if pattern == "*" {
		return true
	}

	if prefix, ok := strings.CutSuffix(pattern, "*"); ok {
		return strings.HasPrefix(tag, prefix)
	}

	return tag == pattern
}

// ageThreshold returns the time before which images should be expired.
func ageThreshold(now time.Time, count int, unit string) time.Time {
	switch strings.ToLower(unit) {
	case "hours":
		return now.Add(-time.Duration(count) * time.Hour)
	case "days":
		return now.AddDate(0, 0, -count)
	case "weeks":
		return now.AddDate(0, 0, -count*weeksPerDays)
	case "months":
		return now.AddDate(0, -count, 0)
	case "years":
		return now.AddDate(-count, 0, 0)
	default:
		return now.AddDate(0, 0, -count)
	}
}
