package ecr

import (
	"context"
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
				expired = append(expired, expiredIdentifier(e))
			}
		}
	}

	return expired
}

// expiredIdentifier builds a stable [ImageIdentifier] for an expired image,
// always populating the canonical digest (the map key on the image) and a
// representative tag when the image is tagged.
func expiredIdentifier(e *imageEntry) ImageIdentifier {
	id := ImageIdentifier{ImageDigest: e.img.ImageDigest}
	if id.ImageDigest == "" {
		id.ImageDigest = e.img.ImageID.ImageDigest
	}

	switch {
	case len(e.allTags) > 0:
		id.ImageTag = e.allTags[0]
	default:
		id.ImageTag = e.img.ImageID.ImageTag
	}

	return id
}

// applyLifecyclePolicyLocked evaluates the repository's stored lifecycle policy
// and deletes every image the policy selects for expiration, mirroring the AWS
// ECR lifecycle evaluation job. It records the evaluation timestamp and returns
// the identifiers of the images that were actually deleted. The write lock must
// be held by the caller.
func (b *InMemoryBackend) applyLifecyclePolicyLocked(repositoryName string) []ImageIdentifier {
	b.lifecycleLastEvaluated[repositoryName] = time.Now()

	policyText := b.lifecyclePolicies[repositoryName]
	if policyText == "" {
		return nil
	}

	expired := evaluateLifecyclePolicy(
		policyText,
		b.images[repositoryName],
		b.digestTagsIndex[repositoryName],
	)
	if len(expired) == 0 {
		return nil
	}

	repoImages := b.images[repositoryName]
	repoTags := b.tagIndex[repositoryName]
	deleted := make([]ImageIdentifier, 0, len(expired))

	for _, id := range expired {
		digest := id.ImageDigest
		if digest == "" && id.ImageTag != "" {
			digest = repoTags[id.ImageTag]
		}

		if digest == "" {
			continue
		}

		if !deleteByDigestLocked(repoImages, repoTags, digest) {
			continue
		}

		b.clearDigestTagsLocked(repositoryName, digest)

		if findings := b.imageScanFindings[repositoryName]; findings != nil {
			delete(findings, digest)
		}

		deleted = append(deleted, ImageIdentifier{ImageDigest: digest, ImageTag: id.ImageTag})
	}

	return deleted
}

// RunLifecycleExpiry evaluates the lifecycle policy of every repository that has
// one and deletes any expired images. It is invoked by the ECR janitor on a
// timer so that count/age-based expirations happen in the background exactly as
// they do in AWS, independent of any API call. It returns the total number of
// images deleted across all repositories.
func (b *InMemoryBackend) RunLifecycleExpiry(ctx context.Context) int {
	b.mu.Lock("RunLifecycleExpiry")
	defer b.mu.Unlock()

	total := 0

	for repositoryName := range b.lifecyclePolicies {
		select {
		case <-ctx.Done():
			return total
		default:
		}

		total += len(b.applyLifecyclePolicyLocked(repositoryName))
	}

	return total
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
		return matchesTaggedSelection(sel, allTags)
	case "any":
		return true
	default:
		return false
	}
}

// matchesTaggedSelection evaluates the tagStatus=="tagged" branch of a lifecycle
// selection against an image's tags. When neither a prefix nor a pattern list is
// specified, every tagged image matches. Otherwise the image matches when any of
// its tags satisfies any prefix (literal HasPrefix) or any pattern (glob).
func matchesTaggedSelection(sel lifecyclePolicySelect, allTags []string) bool {
	if len(allTags) == 0 {
		return false
	}

	if len(sel.TagPrefixList) == 0 && len(sel.TagPatternList) == 0 {
		return true
	}

	for _, tag := range allTags {
		if tagMatchesAnyPrefix(tag, sel.TagPrefixList) || tagMatchesAnyPattern(tag, sel.TagPatternList) {
			return true
		}
	}

	return false
}

// tagMatchesAnyPrefix reports whether tag starts with any of the given prefixes.
func tagMatchesAnyPrefix(tag string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(tag, prefix) {
			return true
		}
	}

	return false
}

// tagMatchesAnyPattern reports whether tag matches any of the given glob patterns.
func tagMatchesAnyPattern(tag string, patterns []string) bool {
	for _, pattern := range patterns {
		if tagMatchesPattern(tag, pattern) {
			return true
		}
	}

	return false
}

// tagMatchesPattern matches an ECR tagPatternList entry against a tag. ECR
// patterns use '*' as a zero-or-more-characters wildcard and may contain
// multiple wildcards anywhere in the pattern (e.g. "v*.*-rc", "*-prod").
func tagMatchesPattern(tag, pattern string) bool {
	if pattern == "" {
		return false
	}

	return wildcardMatch(pattern, tag)
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
