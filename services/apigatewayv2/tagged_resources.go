package apigatewayv2

import "maps"

// TaggedEntry pairs a taggable resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

func (b *InMemoryBackend) taggingARN(path string) string {
	return "arn:aws:apigateway:" + b.region + "::/" + path
}

// TaggedResources returns every tagged API, stage, VPC link, domain name, portal and portal product.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	add := func(tags map[string]string, path string) {
		if len(tags) > 0 {
			out = append(out, TaggedEntry{ARN: b.taggingARN(path), Tags: maps.Clone(tags)})
		}
	}

	for _, a := range b.apis.All() {
		add(a.Tags, arnResourceTypeAPIs+"/"+a.APIID)
	}

	for _, s := range b.stages.All() {
		add(s.Tags, arnResourceTypeAPIs+"/"+s.APIID+"/"+arnResourceTypeStages+"/"+s.StageName)
	}

	for _, v := range b.vpcLinks.All() {
		add(v.Tags, arnResourceTypeVpcLinks+"/"+v.VpcLinkID)
	}

	for _, d := range b.domainNames.All() {
		add(d.Tags, arnResourceTypeDomainNames+"/"+d.DomainNameValue)
	}

	for _, p := range b.portals.All() {
		add(p.Tags, arnResourceTypePortals+"/"+p.PortalID)
	}

	for _, p := range b.portalProducts.All() {
		add(p.Tags, arnResourceTypePortalProducts+"/"+p.PortalProductID)
	}

	return out
}
