package appconfig

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	maxTagsPerResource = 50
	maxTagKeyLen       = 128
	maxTagValueLen     = 256
	reservedTagPrefix  = "aws:"
	arnPartCount       = 6
	pathAppIdx         = 1
	pathKindIdx        = 2
	pathChildIdx       = 3
	pathGrandKindIdx   = 4
	pathGrandIDIdx     = 5
	pathLenApp         = 2
	pathLenChild       = 4
	pathLenGrand       = 6
)

// validateTags enforces the documented tag limits and the reserved aws: prefix.
func validateTags(tags map[string]string) error {
	for k, v := range tags {
		switch {
		case k == "" || utf8.RuneCountInString(k) > maxTagKeyLen:
			return fmt.Errorf("%w: tag key must be 1-%d characters", ErrBadRequest, maxTagKeyLen)
		case utf8.RuneCountInString(v) > maxTagValueLen:
			return fmt.Errorf("%w: tag value must be at most %d characters", ErrBadRequest, maxTagValueLen)
		case strings.HasPrefix(strings.ToLower(k), reservedTagPrefix):
			return fmt.Errorf("%w: tag keys may not start with %q", ErrBadRequest, reservedTagPrefix)
		}
	}

	return nil
}

// resourceExistsLocked reports whether arnStr names a live taggable AppConfig resource.
func (b *InMemoryBackend) resourceExistsLocked(arnStr string) bool {
	parts := strings.SplitN(arnStr, ":", arnPartCount)
	if len(parts) != arnPartCount || parts[0] != "arn" || parts[2] != "appconfig" ||
		parts[3] != b.region || parts[4] != b.accountID {
		return false
	}

	path := strings.Split(parts[5], "/")

	switch path[0] {
	case "deploymentstrategy":
		return len(path) == pathLenApp && b.deploymentStrategies.Has(path[pathAppIdx])
	case "extension":
		return len(path) == pathLenApp && len(b.extensionsByID.Get(path[pathAppIdx])) > 0
	case "extensionassociation":
		return len(path) == pathLenApp && b.extensionAssociations.Has(path[pathAppIdx])
	case "application":
		return b.applicationPathExistsLocked(path)
	default:
		return false
	}
}

func (b *InMemoryBackend) applicationPathExistsLocked(path []string) bool {
	if len(path) < pathLenApp || !b.applications.Has(path[pathAppIdx]) {
		return false
	}

	appID := path[pathAppIdx]

	switch len(path) {
	case pathLenApp:
		return true
	case pathLenChild:
		return b.appChildExistsLocked(appID, path[pathKindIdx], path[pathChildIdx])
	case pathLenGrand:
		return b.appGrandchildExistsLocked(appID, path)
	}

	return false
}

func (b *InMemoryBackend) appChildExistsLocked(appID, kind, id string) bool {
	switch kind {
	case "environment":
		env, ok := b.environments.Get(id)

		return ok && env.ApplicationID == appID
	case "configurationprofile":
		p, ok := b.configProfiles.Get(id)

		return ok && p.ApplicationID == appID
	case "experimentdefinition":
		d, ok := b.experimentDefinitions.Get(id)

		return ok && d.ApplicationID == appID
	}

	return false
}

func (b *InMemoryBackend) appGrandchildExistsLocked(appID string, path []string) bool {
	n, err := strconv.ParseInt(path[pathGrandIDIdx], 10, 32)
	if err != nil {
		return false
	}

	switch {
	case path[pathKindIdx] == "environment" && path[pathGrandKindIdx] == "deployment":
		return b.deployments.Has(deploymentKey(appID, path[pathChildIdx], int32(n)))
	case path[pathKindIdx] == "experimentdefinition" && path[pathGrandKindIdx] == "experimentrun":
		return b.experimentRuns.Has(experimentRunKey(path[pathChildIdx], int32(n)))
	}

	return false
}

func resourceNotFound(arnStr string) error {
	return fmt.Errorf("%w: resource %s", ErrApplicationNotFound, arnStr)
}

// ListTagsForResource returns the tags for the given resource ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.resourceExistsLocked(resourceArn) {
		return nil, resourceNotFound(resourceArn)
	}

	t := b.tags[resourceArn]
	result := make(map[string]string, len(t))
	maps.Copy(result, t)

	return result, nil
}

// TagResource adds or replaces tags on the given resource ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.resourceExistsLocked(resourceArn) {
		return resourceNotFound(resourceArn)
	}

	merged := maps.Clone(b.tags[resourceArn])
	if merged == nil {
		merged = make(map[string]string, len(tags))
	}

	maps.Copy(merged, tags)

	if len(merged) > maxTagsPerResource {
		return fmt.Errorf("%w: at most %d tags allowed on %s", ErrBadRequest, maxTagsPerResource, resourceArn)
	}

	b.tags[resourceArn] = merged

	return nil
}

// UntagResource removes the specified tag keys from the given resource ARN.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.resourceExistsLocked(resourceArn) {
		return resourceNotFound(resourceArn)
	}

	t := b.tags[resourceArn]
	if t == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(t, k)
	}

	return nil
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's wireTaggingAppConfig).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every AppConfig resource ARN (applications,
// environments, configuration profiles, deployment strategies, deployments,
// extensions, extension associations, experiment definitions) that
// currently has at least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))

	for arn, t := range b.tags {
		if len(t) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: arn, Tags: maps.Clone(t)})
	}

	return out
}
