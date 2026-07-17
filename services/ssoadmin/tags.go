package ssoadmin

import (
	"fmt"
	"maps"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// validateTags validates tag keys and values against AWS limits and character rules.
func validateTags(tags map[string]string) error {
	for k, v := range tags {
		if len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key exceeds maximum length of %d", awserr.ErrInvalidParameter, maxTagKeyLen)
		}
		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value exceeds maximum length of %d", awserr.ErrInvalidParameter, maxTagValueLen)
		}
		if strings.HasPrefix(strings.ToLower(k), "aws:") {
			return fmt.Errorf("%w: tag keys with prefix 'aws:' are reserved", awserr.ErrInvalidParameter)
		}
	}

	return nil
}

// applyTagsWithLimit applies tags to an existing map, enforcing the 50-tag limit.
func applyTagsWithLimit(existing map[string]string, newTags map[string]string) error {
	if err := validateTags(newTags); err != nil {
		return err
	}
	// Count net-new keys (keys not already present).
	netNew := 0
	for k := range newTags {
		if _, exists := existing[k]; !exists {
			netNew++
		}
	}
	if len(existing)+netNew > maxTagsPerResource {
		return ErrServiceQuotaExceeded
	}
	maps.Copy(existing, newTags)

	return nil
}

// TagResource adds tags to a resource (permission set, instance, application, or trusted token issuer).
func (b *InMemoryBackend) TagResource(instanceArn, resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if ps, ok := b.permissionSets.Get(resourceArn); ok && ps.InstanceArn == instanceArn {
		if ps.Tags == nil {
			ps.Tags = make(map[string]string)
		}

		return applyTagsWithLimit(ps.Tags, tags)
	}

	if inst, ok := b.instances.Get(resourceArn); ok {
		if inst.Tags == nil {
			inst.Tags = make(map[string]string)
		}

		return applyTagsWithLimit(inst.Tags, tags)
	}

	if app, ok := b.applications.Get(resourceArn); ok && app.InstanceArn == instanceArn {
		if app.Tags == nil {
			app.Tags = make(map[string]string)
		}

		return applyTagsWithLimit(app.Tags, tags)
	}

	if tti, ok := b.trustedTokenIssuers.Get(resourceArn); ok && tti.InstanceArn == instanceArn {
		if tti.Tags == nil {
			tti.Tags = make(map[string]string)
		}

		return applyTagsWithLimit(tti.Tags, tags)
	}

	return ErrInstanceNotFound
}

// UntagResource removes tags from a resource (permission set, instance, or application).
func (b *InMemoryBackend) UntagResource(instanceArn, resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if ps, ok := b.permissionSets.Get(resourceArn); ok && ps.InstanceArn == instanceArn {
		for _, k := range tagKeys {
			delete(ps.Tags, k)
		}

		return nil
	}

	if inst, ok := b.instances.Get(resourceArn); ok {
		for _, k := range tagKeys {
			delete(inst.Tags, k)
		}

		return nil
	}

	if app, ok := b.applications.Get(resourceArn); ok && app.InstanceArn == instanceArn {
		for _, k := range tagKeys {
			delete(app.Tags, k)
		}

		return nil
	}

	if tti, ok := b.trustedTokenIssuers.Get(resourceArn); ok && tti.InstanceArn == instanceArn {
		for _, k := range tagKeys {
			delete(tti.Tags, k)
		}

		return nil
	}

	return ErrInstanceNotFound
}

// ListTagsForResource returns the tags on a resource (permission set, instance, or application).
func (b *InMemoryBackend) ListTagsForResource(instanceArn, resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if ps, ok := b.permissionSets.Get(resourceArn); ok && ps.InstanceArn == instanceArn {
		result := make(map[string]string, len(ps.Tags))
		maps.Copy(result, ps.Tags)

		return result, nil
	}

	if inst, ok := b.instances.Get(resourceArn); ok {
		result := make(map[string]string, len(inst.Tags))
		maps.Copy(result, inst.Tags)

		return result, nil
	}

	if app, ok := b.applications.Get(resourceArn); ok && app.InstanceArn == instanceArn {
		result := make(map[string]string, len(app.Tags))
		maps.Copy(result, app.Tags)

		return result, nil
	}

	if tti, ok := b.trustedTokenIssuers.Get(resourceArn); ok && tti.InstanceArn == instanceArn {
		result := make(map[string]string, len(tti.Tags))
		maps.Copy(result, tti.Tags)

		return result, nil
	}

	return nil, ErrInstanceNotFound
}
