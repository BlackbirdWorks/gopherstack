package wafv2

import (
	"context"
	"fmt"
	"sort"
)

// AssociateWebACL associates a WebACL with a resource ARN.
func (b *InMemoryBackend) AssociateWebACL(ctx context.Context, webACLARN, resourceARN string) error {
	b.mu.Lock("AssociateWebACL")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	webACLID, ok := b.webACLIDByARNInRegion(webACLARN, region)
	if !ok {
		return fmt.Errorf("%w: web ACL with ARN %q not found", ErrWebACLNotFound, webACLARN)
	}

	b.associationsStore(region)[resourceARN] = webACLID

	return nil
}

// webACLIDByARNInRegion resolves webACLARN to its WebACL ID, but only if the
// ARN's own embedded region (see regionFromARN) matches region. This
// reproduces the old map[region]map[arn]string index's behavior exactly: a
// WebACL is only found under the region bucket it was created into, so
// looking it up from a mismatched ctx region must still miss (see
// AssociateWebACL/DeleteFirewallManagerRuleGroups/ListResourcesForWebACL,
// none of which merge in the "" CLOUDFRONT bucket the way lookupWebACLByID does).
func (b *InMemoryBackend) webACLIDByARNInRegion(webACLARN, region string) (string, bool) {
	if regionFromARN(webACLARN) != region {
		return "", false
	}

	ws := b.webACLsByARN.Get(webACLARN)
	if len(ws) == 0 {
		return "", false
	}

	return ws[0].ID, true
}

// DisassociateWebACL removes the WebACL association from a resource ARN.
// Per AWS behaviour, this is a no-op if no association exists (idempotent).
func (b *InMemoryBackend) DisassociateWebACL(ctx context.Context, resourceARN string) error {
	b.mu.Lock("DisassociateWebACL")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	delete(b.associations[region], resourceARN)

	return nil
}

// GetWebACLForResource returns the WebACL associated with the given resource ARN.
func (b *InMemoryBackend) GetWebACLForResource(ctx context.Context, resourceARN string) (*WebACL, error) {
	b.mu.RLock("GetWebACLForResource")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	webACLID, ok := b.associations[region][resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: no web ACL association found for resource %q", ErrAssociationNotFound, resourceARN)
	}

	w, ok := b.webACLs.Get(regionKey(region, webACLID))
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, webACLID)
	}

	return cloneWebACL(w), nil
}

// ListResourcesForWebACL returns all resource ARNs associated with the given WebACL ARN.
func (b *InMemoryBackend) ListResourcesForWebACL(ctx context.Context, webACLARN string) ([]string, error) {
	b.mu.RLock("ListResourcesForWebACL")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	webACLID, ok := b.webACLIDByARNInRegion(webACLARN, region)
	if !ok {
		return nil, fmt.Errorf("%w: web ACL with ARN %q not found", ErrWebACLNotFound, webACLARN)
	}

	regionAssoc := b.associations[region]
	result := make([]string, 0, len(regionAssoc))

	for resourceARN, wID := range regionAssoc {
		if wID == webACLID {
			result = append(result, resourceARN)
		}
	}

	sort.Strings(result)

	return result, nil
}
