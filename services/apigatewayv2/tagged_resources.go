package apigatewayv2

import (
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// TaggedEntry pairs a taggable resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

func (b *InMemoryBackend) taggingARN(path string) string {
	return arn.Build("apigateway", b.region, "", "/"+path)
}

// TaggedResources lists every API, stage, VPC link, domain name, portal and portal product that carries at least
// one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(true)
}

// InventoryResources lists every API, stage, VPC link, domain name, portal and portal product, tagged or not.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(false)
}

func (b *InMemoryBackend) entries(taggedOnly bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	add := func(tags map[string]string, path string) {
		if !taggedOnly || len(tags) > 0 {
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
