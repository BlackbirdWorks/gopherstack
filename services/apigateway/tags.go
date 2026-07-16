package apigateway

import (
	"fmt"
	"strings"
)

// GetResourceTags returns the tags for a resource identified by its ARN.
// For simplicity, we parse the ARN to extract the resource type and ID.
func (b *InMemoryBackend) GetResourceTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("GetResourceTags")
	defer b.mu.RUnlock()

	// ARN format: arn:aws:apigateway:{region}::/restapis/{id}
	// We strip down to find the resource.
	parts := strings.SplitN(resourceARN, "/restapis/", arnSplitParts)
	if len(parts) != arnSplitParts {
		return map[string]string{}, nil
	}

	apiID := strings.Split(parts[1], "/")[0]

	api, ok := b.restApis.Get(apiID)
	if !ok {
		return map[string]string{}, nil
	}

	return api.Tags.Clone(), nil
}

// TagResource adds or updates tags on a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, newTags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	parts := strings.SplitN(resourceARN, "/restapis/", arnSplitParts)
	if len(parts) != arnSplitParts {
		return fmt.Errorf("%w: unsupported resource ARN format", ErrInvalidParameter)
	}

	apiID := strings.Split(parts[1], "/")[0]

	api, ok := b.restApis.Get(apiID)
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, apiID)
	}

	for k, v := range newTags {
		api.Tags.Set(k, v)
	}

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	parts := strings.SplitN(resourceARN, "/restapis/", arnSplitParts)
	if len(parts) != arnSplitParts {
		return fmt.Errorf("%w: unsupported resource ARN format", ErrInvalidParameter)
	}

	apiID := strings.Split(parts[1], "/")[0]

	api, ok := b.restApis.Get(apiID)
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, apiID)
	}

	for _, k := range tagKeys {
		api.Tags.Delete(k)
	}

	return nil
}
