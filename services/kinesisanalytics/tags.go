package kinesisanalytics

import (
	"context"
	"fmt"
	"maps"
	"strings"
)

// validateTagKey returns an error if the tag key violates AWS rules.
func validateTagKey(key string) error {
	if len(key) == 0 || len(key) > maxTagKeyLen {
		return fmt.Errorf("%w: tag key must be 1-%d characters", ErrValidation, maxTagKeyLen)
	}

	if strings.HasPrefix(key, "aws:") {
		return fmt.Errorf("%w: tag key must not start with \"aws:\"", ErrValidation)
	}

	return nil
}

// validateTagValue returns an error if the tag value violates AWS rules.
func validateTagValue(value string) error {
	if len(value) > maxTagValueLen {
		return fmt.Errorf("%w: tag value must be 0-%d characters", ErrValidation, maxTagValueLen)
	}

	return nil
}

// validateAndMergeTags validates incoming tags and checks the per-resource cap.
func validateAndMergeTags(existing, incoming map[string]string) error {
	for k, v := range incoming {
		if err := validateTagKey(k); err != nil {
			return err
		}

		if err := validateTagValue(v); err != nil {
			return err
		}
	}

	total := len(existing)

	for k := range incoming {
		if _, alreadyPresent := existing[k]; !alreadyPresent {
			total++
		}
	}

	if total > maxTagsPerResource {
		return fmt.Errorf("%w: resource may not have more than %d tags", ErrTooManyTags, maxTagsPerResource)
	}

	return nil
}

// validateARNShape verifies that an ARN refers to a kinesisanalytics application in this backend.
// ARN format: arn:{partition}:kinesisanalytics:{region}:{accountID}:application/{name}.
func (b *InMemoryBackend) validateARNShape(resourceARN, region string) error {
	const arnFieldCount = 6

	parts := strings.SplitN(resourceARN, ":", arnFieldCount)
	if len(parts) != arnFieldCount ||
		parts[0] != "arn" ||
		parts[2] != "kinesisanalytics" ||
		!strings.HasPrefix(parts[5], "application/") {
		return fmt.Errorf("%w: ResourceARN is not a valid kinesisanalytics application ARN", ErrValidation)
	}

	if parts[3] != region || parts[4] != b.accountID {
		return fmt.Errorf("%w: ResourceARN region/account does not match this endpoint", ErrValidation)
	}

	return nil
}

// appByARN looks up the application registered under resourceARN via the
// byARN index. An ApplicationARN embeds its owning region (see
// applicationARN), so it uniquely identifies at most one application across
// every region -- callers must still validate the ARN's shape/region/account
// via validateARNShape before calling this, since the index performs no
// validation of its own. Must be called under b.mu held for reading or
// writing.
func (b *InMemoryBackend) appByARN(resourceARN string) (*Application, bool) {
	matches := b.appsByARN.Get(resourceARN)
	if len(matches) == 0 {
		return nil, false
	}

	return matches[0], true
}

// ListTagsForResource returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	region := getRegion(ctx, b.defaultRegion)

	if err := b.validateARNShape(resourceARN, region); err != nil {
		return nil, err
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	app, ok := b.appByARN(resourceARN)
	if !ok {
		return nil, ErrNotFound
	}

	result := make(map[string]string, len(app.Tags))
	maps.Copy(result, app.Tags)

	return result, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, tags map[string]string) error {
	region := getRegion(ctx, b.defaultRegion)

	if err := b.validateARNShape(resourceARN, region); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	app, ok := b.appByARN(resourceARN)
	if !ok {
		return ErrNotFound
	}

	if app.Tags == nil {
		app.Tags = make(map[string]string)
	}

	if err := validateAndMergeTags(app.Tags, tags); err != nil {
		return err
	}

	maps.Copy(app.Tags, tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	region := getRegion(ctx, b.defaultRegion)

	if err := b.validateARNShape(resourceARN, region); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	app, ok := b.appByARN(resourceARN)
	if !ok {
		return ErrNotFound
	}

	for _, k := range tagKeys {
		delete(app.Tags, k)
	}

	return nil
}
