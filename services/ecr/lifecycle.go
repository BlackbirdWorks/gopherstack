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

// lifecyclePolicySelect describes which images a rule targets. StorageClass
// ("standard"|"archive") is a general selection filter, not exclusive to
// countType=sinceImageTransitioned -- docs.aws.amazon.com/AmazonECR/latest/
// userguide/lifecycle_policy_examples.html's policy template lists it as a
// sibling of tagStatus.
type lifecyclePolicySelect struct {
	TagStatus      string   `json:"tagStatus"`
	CountType      string   `json:"countType"`
	CountUnit      string   `json:"countUnit,omitempty"`
	StorageClass   string   `json:"storageClass,omitempty"`
	TagPatternList []string `json:"tagPatternList,omitempty"`
	TagPrefixList  []string `json:"tagPrefixList,omitempty"`
	CountNumber    int      `json:"countNumber"`
}

// lifecyclePolicyAction specifies what to do with matched images. Real AWS
// ImageActionType is "expire"|"transition" (there is no "archive" action
// type); TargetStorageClass is only meaningful when Type=="transition" and
// its only real value is "archive" (types.LifecyclePolicyTargetStorageClass).
type lifecyclePolicyAction struct {
	Type               string `json:"type"`
	TargetStorageClass string `json:"targetStorageClass,omitempty"`
}

// imageEntry is used internally by lifecycle policy evaluation to track which
// images have already been matched by a rule.
type imageEntry struct {
	img     *Image
	allTags []string // all tags for this image, from digestTagsIndex
	matched bool
}

// evaluateLifecyclePolicy applies the lifecycle policy rules against the given
// images and returns a preview entry for each image that would be expired,
// carrying the AWS-shaped detail (action, applied rule priority, push time,
// tags, storage class) that GetLifecyclePolicyPreview/StartLifecyclePolicyPreview
// must report. Rules are evaluated in ascending rulePriority order. An image
// may only match one rule (first-match wins by priority).
// digestTags maps image digest → all tags for that image (from digestTagsIndex).
func evaluateLifecyclePolicy(
	policyText string,
	images []*Image,
	digestTags map[string][]string,
) []LifecyclePolicyPreviewEntry {
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

	var expired []LifecyclePolicyPreviewEntry

	for _, rule := range rules {
		actionType := strings.ToLower(rule.Action.Type)
		if actionType != "expire" && actionType != "transition" {
			continue
		}

		matched := applyRule(rule, entries)
		for _, e := range matched {
			if !e.matched {
				e.matched = true
				expired = append(expired, previewEntryFor(e, rule))
			}
		}
	}

	return expired
}

// previewEntryFor builds the AWS-shaped [LifecyclePolicyPreviewEntry] for an
// image matched by rule, always populating the canonical digest (the map key
// on the image) and the full tag list.
func previewEntryFor(e *imageEntry, rule lifecyclePolicyRule) LifecyclePolicyPreviewEntry {
	digest := e.img.ImageDigest
	if digest == "" {
		digest = e.img.ImageID.ImageDigest
	}

	storageClass := e.img.StorageClass
	if storageClass == "" {
		storageClass = "STANDARD"
	}

	return LifecyclePolicyPreviewEntry{
		ImageDigest:         digest,
		ImageTags:           append([]string(nil), e.allTags...),
		StorageClass:        storageClass,
		ActionType:          strings.ToUpper(rule.Action.Type),
		TargetStorageClass:  strings.ToUpper(rule.Action.TargetStorageClass),
		AppliedRulePriority: rule.RulePriority,
		ImagePushedAt:       e.img.ImagePushedAt,
	}
}

// applyLifecyclePolicyLocked evaluates the repository's stored lifecycle policy
// and applies every image the policy selects: "expire" rules delete the image
// (mirroring the AWS ECR lifecycle evaluation job), "transition" rules (with
// targetStorageClass="archive") transition it to StorageClass=ARCHIVE the same
// way UpdateImageStorageClass does. It records the evaluation timestamp and
// returns the identifiers of the images that were actually deleted (archived
// images are not deleted, so they are not included). The write lock must be
// held by the caller.
func (b *InMemoryBackend) applyLifecyclePolicyLocked(repositoryName string) []ImageIdentifier {
	b.lifecycleLastEvaluated[repositoryName] = time.Now()

	entry, ok := b.lifecyclePolicies.Get(repositoryName)
	if !ok || entry.PolicyText == "" {
		return nil
	}

	expired := evaluateLifecyclePolicy(
		entry.PolicyText,
		b.imagesByRepo.Get(repositoryName),
		b.digestTagsIndex[repositoryName],
	)
	if len(expired) == 0 {
		return nil
	}

	repoTags := b.tagIndex[repositoryName]
	deleted := make([]ImageIdentifier, 0, len(expired))

	for _, pe := range expired {
		digest := pe.ImageDigest
		if digest == "" {
			continue
		}

		if pe.ActionType == "TRANSITION" && pe.TargetStorageClass == storageClassArchive {
			b.archiveImageLocked(repositoryName, digest)

			continue
		}

		var tag string
		if len(pe.ImageTags) > 0 {
			tag = pe.ImageTags[0]
		}

		if !deleteByDigestLocked(b.images, repoTags, repositoryName, digest) {
			continue
		}

		b.clearDigestTagsLocked(repositoryName, digest)
		b.imageScanFindings.Delete(findingsTableKey(repositoryName, digest))

		deleted = append(deleted, ImageIdentifier{ImageDigest: digest, ImageTag: tag})
	}

	return deleted
}

// archiveImageLocked performs the same StorageClass/ImageStatus/LastArchivedAt
// transition UpdateImageStorageClass(target="ARCHIVE") performs, for a
// lifecycle-policy action.type=="transition" rule. The write lock must be
// held by the caller.
func (b *InMemoryBackend) archiveImageLocked(repositoryName, digest string) {
	img, ok := findImageLocked(b.images, b.imagesByRepo, repositoryName, b.tagIndex[repositoryName],
		ImageIdentifier{ImageDigest: digest})
	if !ok {
		return
	}

	img.StorageClass = storageClassArchive
	img.ImageStatus = imageStatusArchived
	img.LastArchivedAt = time.Now()
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

	for _, entry := range b.lifecyclePolicies.All() {
		select {
		case <-ctx.Done():
			return total
		default:
		}

		total += len(b.applyLifecyclePolicyLocked(entry.RepositoryName))
	}

	return total
}

// applyRule returns the entries that match the given rule (ignoring already-matched ones).
func applyRule(rule lifecyclePolicyRule, entries []*imageEntry) []*imageEntry {
	sel := rule.Selection
	candidates := selectionCandidates(sel, entries)

	switch sel.CountType {
	case "imageCountMoreThan":
		// Keep the first CountNumber images; expire the rest.
		if len(candidates) <= sel.CountNumber {
			return nil
		}

		return candidates[sel.CountNumber:]

	case "sinceImagePushed":
		return byAgeThreshold(sel, candidates, func(e *imageEntry) time.Time { return e.img.ImagePushedAt })

	case "sinceImagePulled":
		return byAgeThreshold(sel, candidates, func(e *imageEntry) time.Time { return effectiveLastPullTime(e.img) })

	case "sinceImageTransitioned":
		lastArchivedAt := func(e *imageEntry) time.Time { return e.img.LastArchivedAt }

		return byAgeThreshold(sel, archivedOnly(candidates), lastArchivedAt)
	}

	return nil
}

// selectionCandidates filters entries down to those matching the rule's
// tagStatus/tagPrefixList/tagPatternList and (if set) storageClass criteria,
// excluding images already claimed by a higher-priority rule.
func selectionCandidates(sel lifecyclePolicySelect, entries []*imageEntry) []*imageEntry {
	candidates := make([]*imageEntry, 0, len(entries))

	for _, e := range entries {
		if e.matched || !matchesTagStatus(sel, e.img, e.allTags) || !matchesStorageClass(sel, e.img) {
			continue
		}

		candidates = append(candidates, e)
	}

	return candidates
}

// matchesStorageClass reports whether an image satisfies the selection's
// storageClass filter ("standard"|"archive"), or true when unset.
func matchesStorageClass(sel lifecyclePolicySelect, img *Image) bool {
	if sel.StorageClass == "" {
		return true
	}

	isArchived := img.ImageStatus == imageStatusArchived
	if strings.EqualFold(sel.StorageClass, storageClassArchive) {
		return isArchived
	}

	return !isArchived
}

// archivedOnly filters to images already in archive storage --
// countType=sinceImageTransitioned only ever considers archived images
// ("all archived images whose last_archived_at is older than ...").
func archivedOnly(candidates []*imageEntry) []*imageEntry {
	out := make([]*imageEntry, 0, len(candidates))

	for _, e := range candidates {
		if e.img.ImageStatus == imageStatusArchived {
			out = append(out, e)
		}
	}

	return out
}

// byAgeThreshold returns the candidates whose timeOf(e) is older than
// sel.CountNumber sel.CountUnit ago (defaulting to days).
func byAgeThreshold(
	sel lifecyclePolicySelect, candidates []*imageEntry, timeOf func(*imageEntry) time.Time,
) []*imageEntry {
	unit := sel.CountUnit
	if unit == "" {
		unit = lifecycleDefaultCountUnit
	}

	threshold := ageThreshold(time.Now(), sel.CountNumber, unit)
	var expired []*imageEntry

	for _, e := range candidates {
		if timeOf(e).Before(threshold) {
			expired = append(expired, e)
		}
	}

	return expired
}

// effectiveLastPullTime resolves the timestamp countType=sinceImagePulled
// measures against, per its documented fallback chain: LastRecordedPullTime
// when present and not stale relative to a later restore, else
// LastActivatedAt (archived and restored, but never pulled since), else
// ImagePushedAt (never pulled at all).
func effectiveLastPullTime(img *Image) time.Time {
	t := img.ImagePushedAt
	if !img.LastActivatedAt.IsZero() {
		t = img.LastActivatedAt
	}
	if !img.LastRecordedPullTime.IsZero() && img.LastRecordedPullTime.After(img.LastActivatedAt) {
		t = img.LastRecordedPullTime
	}

	return t
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
