package codedeploy

import (
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	maxTagsPerResource = 50
	maxTagKeyLen       = 128
	maxTagValueLen     = 256
	tagReservedPrefix  = "aws:"

	arnSegmentCount = 7
)

// validateTagUpdate checks AWS tag limit rules.
func validateTagUpdate(existing map[string]string, additions map[string]string) error {
	for k, v := range additions {
		if strings.HasPrefix(k, tagReservedPrefix) {
			return fmt.Errorf("%w: tag key %q uses reserved prefix %q", ErrValidation, k, tagReservedPrefix)
		}
		if len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key exceeds maximum length of %d", ErrValidation, maxTagKeyLen)
		}
		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value for key %q exceeds maximum length of %d", ErrValidation, k, maxTagValueLen)
		}
	}

	projected := len(existing) + len(additions)
	// Overwriting existing keys does not increase count.
	for k := range additions {
		if _, alreadyExists := existing[k]; alreadyExists {
			projected--
		}
	}

	if projected > maxTagsPerResource {
		return fmt.Errorf("%w: resource would have %d tags, exceeding the maximum of %d",
			ErrTagLimitExceeded, projected, maxTagsPerResource)
	}

	return nil
}

// TagResource adds tags to a resource (application or deployment group) by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, err := b.findResourceTagsLocked(resourceARN)
	if err != nil {
		return err
	}

	existing := t.Clone()
	if valErr := validateTagUpdate(existing, kv); valErr != nil {
		return valErr
	}

	t.Merge(kv)

	return nil
}

// UntagResource removes tags from a resource (application or deployment group) by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, err := b.findResourceTagsLocked(resourceARN)
	if err != nil {
		return err
	}

	t.DeleteKeys(keys)

	return nil
}

// ListTagsForResource returns the tags for a resource (application or deployment group) by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, err := b.findResourceTagsLocked(resourceARN)
	if err != nil {
		return nil, err
	}

	return t.Clone(), nil
}

// findResourceTagsLocked looks up the tags.Tags for a resource ARN.
// Supports application ARNs (arn:…:application:{name}) and deployment group ARNs
// (arn:…:deploymentgroup:{appName}/{groupName}).
// The caller must hold at least a read lock on b.mu before calling this method.
func (b *InMemoryBackend) findResourceTagsLocked(resourceARN string) (*tags.Tags, error) {
	parsed := parseARN(resourceARN)
	if parsed == nil {
		return nil, fmt.Errorf("%w: invalid ARN %s", ErrNotFound, resourceARN)
	}

	resourceType := parsed.resourceType
	resourceID := parsed.resourceID

	switch resourceType {
	case "application":
		app, ok := b.applications.Get(resourceID)
		if !ok {
			return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, resourceID)
		}

		return app.Tags, nil

	case "deploymentgroup":
		// deploymentgroup resource ID is "{appName}/{groupName}"
		appName, dgName, ok := strings.Cut(resourceID, "/")
		if !ok {
			return nil, fmt.Errorf("%w: invalid deployment group ARN %s", ErrNotFound, resourceARN)
		}

		dg, ok := b.deploymentGroups.Get(dgKey(appName, dgName))
		if !ok {
			return nil, fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, dgName)
		}

		return dg.Tags, nil

	default:
		return nil, fmt.Errorf("%w: unsupported resource type %s", ErrNotFound, resourceType)
	}
}

// parsedARN holds the parsed components of an AWS ARN relevant to CodeDeploy lookups.
type parsedARN struct {
	resourceType string
	resourceID   string
}

// parseARN parses an AWS ARN into resource type and resource ID.
// Format: arn:{partition}:{service}:{region}:{account}:{resourceType}:{resourceID}
// Handles non-standard partitions (aws-cn, aws-us-gov, etc.) correctly.
func parseARN(arnStr string) *parsedARN {
	if !strings.HasPrefix(arnStr, "arn:") {
		return nil
	}

	// Fixed-position split: arn : partition : service : region : account : resourceType : resourceID
	// Use SplitN with limit 7 to correctly handle resource IDs containing colons.
	parts := strings.SplitN(arnStr, ":", arnSegmentCount)
	if len(parts) != arnSegmentCount {
		return nil
	}

	return &parsedARN{
		resourceType: parts[5],
		resourceID:   parts[6],
	}
}
