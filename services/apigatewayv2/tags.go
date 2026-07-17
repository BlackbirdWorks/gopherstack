package apigatewayv2

import (
	"maps"
	"strings"
)

const (
	arnResourceTypeAPIs        = "apis"
	arnResourceTypeVpcLinks    = "vpclinks"
	arnResourceTypeDomainNames = "domainnames"
	arnResourceTypeStages      = "stages"

	// arnMinPartsWithResourceType is the minimum number of slash-separated
	// parts in an ARN that carries an explicit resource type segment.
	arnMinPartsWithResourceType = 2

	// arnStageParts is the number of trailing slash-separated segments in a
	// Stage resource ARN: "apis", "{apiId}", "stages", "{stageName}".
	arnStageParts = 4
)

// parseStageARN extracts the API ID and stage name from a Stage resource ARN
// of the form ".../apis/{apiId}/stages/{stageName}", the AWS-modeled ARN for
// a Stage (Stages, unlike APIs/VPC links/domain names, are nested one level
// under their owning API so a single trailing "type/id" pair is not enough to
// resolve them). Returns ok=false for any ARN that does not match this shape.
func parseStageARN(arn string) (string, string, bool) {
	parts := strings.Split(arn, "/")
	if len(parts) < arnStageParts {
		return "", "", false
	}

	tail := parts[len(parts)-arnStageParts:]
	if tail[0] != arnResourceTypeAPIs || tail[2] != arnResourceTypeStages {
		return "", "", false
	}

	return tail[1], tail[3], true
}

// lookupStageLocked resolves a Stage by API ID and stage name. Callers must
// already hold b.mu (read or write).
func (b *InMemoryBackend) lookupStageLocked(apiID, stageName string) (*Stage, error) {
	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	s, ok := b.stages.Get(stageKey(apiID, stageName))
	if !ok {
		return nil, ErrStageNotFound
	}

	return s, nil
}

// arnResourceType returns the resource type and ID extracted from an ARN.
// For ARNs like "arn:aws:apigateway:us-east-1::/apis/abc123" the resource
// type would be "apis" and the ID "abc123".
// For a bare resource ID (no slashes besides the leading one) the type
// defaults to "apis" to preserve backwards-compatible behaviour.
func arnResourceType(arn string) (string, string) {
	parts := strings.Split(arn, "/")
	if len(parts) >= arnMinPartsWithResourceType {
		return parts[len(parts)-2], parts[len(parts)-1]
	}

	return arnResourceTypeAPIs, arn
}

// TagResource adds tags to a resource identified by ARN.
// Supports APIs, stages, VPC links, and domain names.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if apiID, stageName, ok := parseStageARN(resourceARN); ok {
		s, err := b.lookupStageLocked(apiID, stageName)
		if err != nil {
			return err
		}

		if s.Tags == nil {
			s.Tags = make(map[string]string)
		}

		maps.Copy(s.Tags, tags)

		return nil
	}

	resourceType, resourceID := arnResourceType(resourceARN)

	switch resourceType {
	case arnResourceTypeAPIs:
		api, ok := b.apis.Get(resourceID)
		if !ok {
			return ErrAPINotFound
		}

		if api.Tags == nil {
			api.Tags = make(map[string]string)
		}

		maps.Copy(api.Tags, tags)
	case arnResourceTypeVpcLinks:
		v, ok := b.vpcLinks.Get(resourceID)
		if !ok {
			return ErrVpcLinkNotFound
		}

		if v.Tags == nil {
			v.Tags = make(map[string]string)
		}

		maps.Copy(v.Tags, tags)
	case arnResourceTypeDomainNames:
		dn, ok := b.domainNames.Get(resourceID)
		if !ok {
			return ErrDomainNameNotFound
		}

		if dn.Tags == nil {
			dn.Tags = make(map[string]string)
		}

		maps.Copy(dn.Tags, tags)
	default:
		return ErrAPINotFound
	}

	return nil
}

// UntagResource removes tag keys from a resource identified by ARN.
// Supports APIs, stages, VPC links, and domain names.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if apiID, stageName, ok := parseStageARN(resourceARN); ok {
		s, err := b.lookupStageLocked(apiID, stageName)
		if err != nil {
			return err
		}

		for _, k := range tagKeys {
			delete(s.Tags, k)
		}

		return nil
	}

	resourceType, resourceID := arnResourceType(resourceARN)

	switch resourceType {
	case arnResourceTypeAPIs:
		api, ok := b.apis.Get(resourceID)
		if !ok {
			return ErrAPINotFound
		}

		for _, k := range tagKeys {
			delete(api.Tags, k)
		}
	case arnResourceTypeVpcLinks:
		v, ok := b.vpcLinks.Get(resourceID)
		if !ok {
			return ErrVpcLinkNotFound
		}

		for _, k := range tagKeys {
			delete(v.Tags, k)
		}
	case arnResourceTypeDomainNames:
		dn, ok := b.domainNames.Get(resourceID)
		if !ok {
			return ErrDomainNameNotFound
		}

		for _, k := range tagKeys {
			delete(dn.Tags, k)
		}
	default:
		return ErrAPINotFound
	}

	return nil
}

// GetTags retrieves all tags for a resource identified by ARN.
// Supports APIs, stages, VPC links, and domain names.
func (b *InMemoryBackend) GetTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("GetTags")
	defer b.mu.RUnlock()

	if apiID, stageName, ok := parseStageARN(resourceARN); ok {
		s, err := b.lookupStageLocked(apiID, stageName)
		if err != nil {
			return nil, err
		}

		return copyTags(s.Tags), nil
	}

	resourceType, resourceID := arnResourceType(resourceARN)

	switch resourceType {
	case arnResourceTypeAPIs:
		api, ok := b.apis.Get(resourceID)
		if !ok {
			return nil, ErrAPINotFound
		}

		return copyTags(api.Tags), nil
	case arnResourceTypeVpcLinks:
		v, ok := b.vpcLinks.Get(resourceID)
		if !ok {
			return nil, ErrVpcLinkNotFound
		}

		return copyTags(v.Tags), nil
	case arnResourceTypeDomainNames:
		dn, ok := b.domainNames.Get(resourceID)
		if !ok {
			return nil, ErrDomainNameNotFound
		}

		return copyTags(dn.Tags), nil
	default:
		return nil, ErrAPINotFound
	}
}
