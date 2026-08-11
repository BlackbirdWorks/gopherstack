package secretsmanager

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// maxTagsPerSecret is the maximum number of tags allowed per secret.
const maxTagsPerSecret = 50

// validateTagCount returns an error when the total number of tags would exceed the AWS limit.
func validateTagCount(existing int, adding int) error {
	if existing+adding > maxTagsPerSecret {
		return fmt.Errorf(
			"%w: maximum of %d tags per secret exceeded",
			ErrInvalidParameter,
			maxTagsPerSecret,
		)
	}

	return nil
}

// tagsToSlice converts the internal tags.Tags store into the []Tag shape the
// real DescribeSecret/ListSecrets responses use (secretsmanager@v1.44.4
// types/types.go:265, SecretListEntry.Tags []Tag) -- not the map shape
// tags.Tags itself marshals to.
func tagsToSlice(t *tags.Tags) []Tag {
	if t == nil {
		return nil
	}

	kvs := tags.MapToKV(t.Clone())
	if len(kvs) == 0 {
		return nil
	}

	out := make([]Tag, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, Tag{Key: kv.Key, Value: kv.Value})
	}

	return out
}

// TaggedSecretInfo contains a secret's ARN and tag snapshot.
// Used by the Resource Groups Tagging API cross-service listing.
type TaggedSecretInfo struct {
	Tags map[string]string
	ARN  string
}

// TagResource adds or updates tags on a secret.
func (b *InMemoryBackend) TagResource(ctx context.Context, input *TagResourceInput) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	id := resolveSecretID(input.SecretID)
	secret, ok := b.secretGet(region, id)
	if !ok {
		return ErrSecretNotFound
	}
	if secret.DeletedDate != nil {
		return ErrSecretDeleted
	}

	// Count only net new keys: keys already present are updates, not additions.
	var existingKeys map[string]string
	if secret.Tags != nil {
		existingKeys = secret.Tags.Clone()
	}

	netNew := 0

	for _, t := range input.Tags {
		if _, alreadyExists := existingKeys[t.Key]; !alreadyExists {
			netNew++
		}
	}

	existingCount := len(existingKeys)

	if err := validateTagCount(existingCount, netNew); err != nil {
		return err
	}

	if secret.Tags == nil {
		secret.Tags = tags.New(id + ".tags")
	}
	for _, t := range input.Tags {
		secret.Tags.Set(t.Key, t.Value)
	}

	return nil
}

// UntagResource removes tags from a secret.
func (b *InMemoryBackend) UntagResource(ctx context.Context, input *UntagResourceInput) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	id := resolveSecretID(input.SecretID)
	secret, ok := b.secretGet(region, id)
	if !ok {
		return ErrSecretNotFound
	}
	if secret.DeletedDate != nil {
		return ErrSecretDeleted
	}
	if secret.Tags != nil {
		secret.Tags.DeleteKeys(input.TagKeys)
	}

	return nil
}

// TaggedSecrets returns a snapshot of all secrets with their ARNs and tags.
// Intended for use by the Resource Groups Tagging API provider.
func (b *InMemoryBackend) TaggedSecrets(_ context.Context) []TaggedSecretInfo {
	b.mu.RLock("TaggedSecrets")
	defer b.mu.RUnlock()

	var result []TaggedSecretInfo

	for _, secret := range b.secrets.All() {
		if secret.DeletedDate != nil {
			continue
		}

		var tagMap map[string]string
		if secret.Tags != nil {
			tagMap = secret.Tags.Clone()
		}

		result = append(result, TaggedSecretInfo{ARN: secret.ARN, Tags: tagMap})
	}

	return result
}

// TagSecretByARN applies tags to the secret identified by its ARN. The region is taken
// from the ARN so cross-service callers (Resource Groups Tagging API) reach the right region.
func (b *InMemoryBackend) TagSecretByARN(_ context.Context, secretARN string, newTags map[string]string) error {
	region := regionFromARN(secretARN, b.region)

	b.mu.Lock("TagSecretByARN")
	defer b.mu.Unlock()

	name := resolveSecretID(secretARN)

	secret, ok := b.secretGet(region, name)
	if !ok {
		return fmt.Errorf("%w: %s", ErrSecretNotFound, secretARN)
	}

	if secret.Tags == nil {
		secret.Tags = tags.New(secret.Name + ".tags")
	}

	secret.Tags.Merge(newTags)

	return nil
}

// UntagSecretByARN removes the specified tag keys from the secret identified by its ARN.
func (b *InMemoryBackend) UntagSecretByARN(_ context.Context, secretARN string, tagKeys []string) error {
	region := regionFromARN(secretARN, b.region)

	b.mu.Lock("UntagSecretByARN")
	defer b.mu.Unlock()

	name := resolveSecretID(secretARN)

	secret, ok := b.secretGet(region, name)
	if !ok {
		return fmt.Errorf("%w: %s", ErrSecretNotFound, secretARN)
	}

	if secret.Tags != nil {
		secret.Tags.DeleteKeys(tagKeys)
	}

	return nil
}
