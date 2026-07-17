package amplify

import (
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// TagResource adds or updates tags on an Amplify resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tagMap map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, err := b.findTagsByARN(resourceARN)
	if err != nil {
		return err
	}

	t.Merge(tagMap)

	return nil
}

// UntagResource removes tags from an Amplify resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, err := b.findTagsByARN(resourceARN)
	if err != nil {
		return err
	}

	t.DeleteKeys(tagKeys)

	return nil
}

// ListTagsForResource returns all tags for an Amplify resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, err := b.findTagsByARN(resourceARN)
	if err != nil {
		return nil, err
	}

	return t.Clone(), nil
}

// findTagsByARN resolves a resource ARN to its *tags.Tags. Must be called while
// holding at least a read lock; callers that modify the tags must hold a write lock.
// ARN format: arn:aws:amplify:{region}:{accountID}:apps/{appID}[/branches/{branchName}].
func (b *InMemoryBackend) findTagsByARN(resourceARN string) (*tags.Tags, error) {
	// Strip the common ARN prefix to get the resource path.
	// Expected prefix: "arn:aws:amplify:{region}:{accountID}:"
	const arnParts = 6
	parts := strings.SplitN(resourceARN, ":", arnParts)

	if len(parts) < arnParts || parts[2] != "amplify" {
		return nil, fmt.Errorf("%w: invalid Amplify ARN: %s", ErrNotFound, resourceARN)
	}

	resource := parts[5] // e.g. "apps/abc123" or "apps/abc123/branches/main"
	resourceParts := strings.Split(resource, "/")

	// apps/{appID}
	if len(resourceParts) == 2 && resourceParts[0] == arnResourceApps {
		appID := resourceParts[1]

		app, ok := b.apps.Get(appID)
		if !ok {
			return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
		}

		return app.Tags, nil
	}

	// apps/{appID}/branches/{branchName}
	if len(resourceParts) == 4 && resourceParts[0] == arnResourceApps &&
		resourceParts[2] == arnResourceBranches {
		appID := resourceParts[1]
		branchName := resourceParts[3]

		branch, ok := b.branches.Get(branchKey(appID, branchName))
		if !ok {
			return nil, fmt.Errorf(
				"%w: branch %s not found for app %s",
				ErrNotFound,
				branchName,
				appID,
			)
		}

		return branch.Tags, nil
	}

	return nil, fmt.Errorf("%w: unsupported Amplify ARN resource path: %s", ErrNotFound, resource)
}
