package apigatewayv2

import (
	"maps"
	"strings"
)

const (
	arnResourceTypeAPIs           = "apis"
	arnResourceTypeVpcLinks       = "vpclinks"
	arnResourceTypeDomainNames    = "domainnames"
	arnResourceTypeStages         = "stages"
	arnResourceTypePortals        = "portals"
	arnResourceTypePortalProducts = "portalproducts"

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

// tagAPILocked adds tags to an API. Callers must hold b.mu.
func (b *InMemoryBackend) tagAPILocked(id string, tagMap map[string]string) error {
	api, ok := b.apis.Get(id)
	if !ok {
		return ErrAPINotFound
	}

	if api.Tags == nil {
		api.Tags = make(map[string]string)
	}

	maps.Copy(api.Tags, tagMap)

	return nil
}

// tagVpcLinkLocked adds tags to a VPC link. Callers must hold b.mu.
func (b *InMemoryBackend) tagVpcLinkLocked(id string, tagMap map[string]string) error {
	v, ok := b.vpcLinks.Get(id)
	if !ok {
		return ErrVpcLinkNotFound
	}

	if v.Tags == nil {
		v.Tags = make(map[string]string)
	}

	maps.Copy(v.Tags, tagMap)

	return nil
}

// tagDomainNameLocked adds tags to a domain name. Callers must hold b.mu.
func (b *InMemoryBackend) tagDomainNameLocked(id string, tagMap map[string]string) error {
	dn, ok := b.domainNames.Get(id)
	if !ok {
		return ErrDomainNameNotFound
	}

	if dn.Tags == nil {
		dn.Tags = make(map[string]string)
	}

	maps.Copy(dn.Tags, tagMap)

	return nil
}

// tagPortalLocked adds tags to a portal. Callers must hold b.mu.
func (b *InMemoryBackend) tagPortalLocked(id string, tagMap map[string]string) error {
	p, ok := b.portals.Get(id)
	if !ok {
		return ErrPortalNotFound
	}

	if p.Tags == nil {
		p.Tags = make(map[string]string)
	}

	maps.Copy(p.Tags, tagMap)

	return nil
}

// tagPortalProductLocked adds tags to a portal product. Callers must hold b.mu.
func (b *InMemoryBackend) tagPortalProductLocked(id string, tagMap map[string]string) error {
	pp, ok := b.portalProducts.Get(id)
	if !ok {
		return ErrPortalProductNotFound
	}

	if pp.Tags == nil {
		pp.Tags = make(map[string]string)
	}

	maps.Copy(pp.Tags, tagMap)

	return nil
}

// TagResource adds tags to a resource identified by ARN.
// Supports APIs, stages, VPC links, domain names, portals, and portal products.
func (b *InMemoryBackend) TagResource(resourceARN string, tagMap map[string]string) error {
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

		maps.Copy(s.Tags, tagMap)

		return nil
	}

	resourceType, resourceID := arnResourceType(resourceARN)

	switch resourceType {
	case arnResourceTypeAPIs:
		return b.tagAPILocked(resourceID, tagMap)
	case arnResourceTypeVpcLinks:
		return b.tagVpcLinkLocked(resourceID, tagMap)
	case arnResourceTypeDomainNames:
		return b.tagDomainNameLocked(resourceID, tagMap)
	case arnResourceTypePortals:
		return b.tagPortalLocked(resourceID, tagMap)
	case arnResourceTypePortalProducts:
		return b.tagPortalProductLocked(resourceID, tagMap)
	default:
		return ErrAPINotFound
	}
}

// untagAPILocked removes tag keys from an API. Callers must hold b.mu.
func (b *InMemoryBackend) untagAPILocked(id string, tagKeys []string) error {
	api, ok := b.apis.Get(id)
	if !ok {
		return ErrAPINotFound
	}

	for _, k := range tagKeys {
		delete(api.Tags, k)
	}

	return nil
}

// untagVpcLinkLocked removes tag keys from a VPC link. Callers must hold b.mu.
func (b *InMemoryBackend) untagVpcLinkLocked(id string, tagKeys []string) error {
	v, ok := b.vpcLinks.Get(id)
	if !ok {
		return ErrVpcLinkNotFound
	}

	for _, k := range tagKeys {
		delete(v.Tags, k)
	}

	return nil
}

// untagDomainNameLocked removes tag keys from a domain name. Callers must hold b.mu.
func (b *InMemoryBackend) untagDomainNameLocked(id string, tagKeys []string) error {
	dn, ok := b.domainNames.Get(id)
	if !ok {
		return ErrDomainNameNotFound
	}

	for _, k := range tagKeys {
		delete(dn.Tags, k)
	}

	return nil
}

// untagPortalLocked removes tag keys from a portal. Callers must hold b.mu.
func (b *InMemoryBackend) untagPortalLocked(id string, tagKeys []string) error {
	p, ok := b.portals.Get(id)
	if !ok {
		return ErrPortalNotFound
	}

	for _, k := range tagKeys {
		delete(p.Tags, k)
	}

	return nil
}

// untagPortalProductLocked removes tag keys from a portal product. Callers must hold b.mu.
func (b *InMemoryBackend) untagPortalProductLocked(id string, tagKeys []string) error {
	pp, ok := b.portalProducts.Get(id)
	if !ok {
		return ErrPortalProductNotFound
	}

	for _, k := range tagKeys {
		delete(pp.Tags, k)
	}

	return nil
}

// UntagResource removes tag keys from a resource identified by ARN.
// Supports APIs, stages, VPC links, domain names, portals, and portal products.
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
		return b.untagAPILocked(resourceID, tagKeys)
	case arnResourceTypeVpcLinks:
		return b.untagVpcLinkLocked(resourceID, tagKeys)
	case arnResourceTypeDomainNames:
		return b.untagDomainNameLocked(resourceID, tagKeys)
	case arnResourceTypePortals:
		return b.untagPortalLocked(resourceID, tagKeys)
	case arnResourceTypePortalProducts:
		return b.untagPortalProductLocked(resourceID, tagKeys)
	default:
		return ErrAPINotFound
	}
}

// GetTags retrieves all tags for a resource identified by ARN.
// Supports APIs, stages, VPC links, domain names, portals, and portal products.
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
	case arnResourceTypePortals:
		p, ok := b.portals.Get(resourceID)
		if !ok {
			return nil, ErrPortalNotFound
		}

		return copyTags(p.Tags), nil
	case arnResourceTypePortalProducts:
		pp, ok := b.portalProducts.Get(resourceID)
		if !ok {
			return nil, ErrPortalProductNotFound
		}

		return copyTags(pp.Tags), nil
	default:
		return nil, ErrAPINotFound
	}
}
