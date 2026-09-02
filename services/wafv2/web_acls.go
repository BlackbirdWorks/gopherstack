package wafv2

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) buildWebACLARN(name, id, scope, region string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", arnRegionForScope(scope, region), b.accountID, prefix+"/webacl/"+name+"/"+id)
}

// WebACLARN builds an ARN for a WebACL.
func (b *InMemoryBackend) WebACLARN(name, id, scope string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", b.arnRegion(scope), b.accountID, prefix+"/webacl/"+name+"/"+id)
}

// CreateWebACL creates a new WebACL.
func (b *InMemoryBackend) CreateWebACL(
	ctx context.Context,
	name, scope, description string,
	defaultAction, visibilityConfig json.RawMessage,
	rules []map[string]any,
	tokenDomains []string,
	customResponseBodies, associationConfig, captchaConfig, challengeConfig json.RawMessage,
	monetizationConfig, dataProtectionConfig, applicationConfig, onSourceDDoSProtectionConfig json.RawMessage,
	tags map[string]string,
) (*WebACL, error) {
	b.mu.Lock("CreateWebACL")
	defer b.mu.Unlock()

	region := storeRegion(scope, getRegion(ctx, b.region))

	if len(b.webACLsByNameScope.Get(regionKey(region, nameScope(name, scope)))) > 0 {
		return nil, fmt.Errorf("%w: web ACL %q already exists in scope %s", ErrWebACLAlreadyExists, name, scope)
	}

	id := uuid.NewString()
	arnStr := b.buildWebACLARN(name, id, scope, region)
	w := &WebACL{
		ARN:                          arnStr,
		ID:                           id,
		Name:                         name,
		Scope:                        scope,
		Description:                  description,
		DefaultAction:                defaultAction,
		VisibilityConfig:             visibilityConfig,
		Rules:                        cloneRules(rules),
		TokenDomains:                 cloneAddresses(tokenDomains),
		CustomResponseBodies:         customResponseBodies,
		AssociationConfig:            associationConfig,
		CaptchaConfig:                captchaConfig,
		ChallengeConfig:              challengeConfig,
		MonetizationConfig:           monetizationConfig,
		DataProtectionConfig:         dataProtectionConfig,
		ApplicationConfig:            applicationConfig,
		OnSourceDDoSProtectionConfig: onSourceDDoSProtectionConfig,
		LockToken:                    uuid.NewString(),
		Tags:                         cloneTags(tags),
	}
	b.webACLs.Put(w)

	return cloneWebACL(w), nil
}

// lookupWebACLByID finds a WebACL in requestRegion first, then the global CLOUDFRONT
// store ("") so that CLOUDFRONT resources are always accessible.
func (b *InMemoryBackend) lookupWebACLByID(requestRegion, id string) (*WebACL, bool) {
	if w, ok := b.webACLs.Get(regionKey(requestRegion, id)); ok {
		return w, true
	}

	if requestRegion != "" {
		if w, ok := b.webACLs.Get(regionKey("", id)); ok {
			return w, true
		}
	}

	return nil, false
}

// GetWebACL returns a WebACL by ID.
func (b *InMemoryBackend) GetWebACL(ctx context.Context, id string) (*WebACL, error) {
	b.mu.RLock("GetWebACL")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	w, ok := b.lookupWebACLByID(region, id)
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, id)
	}

	return cloneWebACL(w), nil
}

// GetWebACLByARN returns a WebACL by ARN. Real GetWebACLInput
// (wafv2@v1.77.3 api_op_GetWebACL.go) accepts ARN as an alternative to
// Name+Scope+Id -- none of the four members is marked required, since
// exactly one addressing mode is used per call -- gopherstack-4ly2.
func (b *InMemoryBackend) GetWebACLByARN(ctx context.Context, webACLARN string) (*WebACL, error) {
	b.mu.RLock("GetWebACLByARN")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	id, ok := b.webACLIDByARNInRegion(webACLARN, region)
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, webACLARN)
	}

	w, ok := b.lookupWebACLByID(region, id)
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, webACLARN)
	}

	return cloneWebACL(w), nil
}

// UpdateWebACL updates a WebACL by ID.
func (b *InMemoryBackend) UpdateWebACL(
	ctx context.Context,
	id, description, lockToken string,
	defaultAction, visibilityConfig json.RawMessage,
	rules []map[string]any,
	tokenDomains []string,
	customResponseBodies, associationConfig, captchaConfig, challengeConfig json.RawMessage,
	monetizationConfig, dataProtectionConfig, applicationConfig, onSourceDDoSProtectionConfig json.RawMessage,
) (*WebACL, error) {
	b.mu.Lock("UpdateWebACL")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	w, ok := b.lookupWebACLByID(region, id)
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, id)
	}

	if lockToken != "" && lockToken != w.LockToken {
		return nil, fmt.Errorf("%w: lock token mismatch for web ACL %q", ErrOptimisticLock, id)
	}

	if description != "" {
		w.Description = description
	}

	if len(defaultAction) > 0 {
		w.DefaultAction = defaultAction
	}

	if len(visibilityConfig) > 0 {
		w.VisibilityConfig = visibilityConfig
	}

	if rules != nil {
		w.Rules = cloneRules(rules)
	}

	if tokenDomains != nil {
		w.TokenDomains = cloneAddresses(tokenDomains)
	}

	applyOptionalWebACLConfigs(
		w,
		customResponseBodies, associationConfig, captchaConfig, challengeConfig,
		monetizationConfig, dataProtectionConfig, applicationConfig, onSourceDDoSProtectionConfig,
	)

	w.LockToken = uuid.NewString()

	return cloneWebACL(w), nil
}

// applyOptionalWebACLConfigs sets each opaque config field on w when the
// caller supplied a non-empty value, leaving the existing value otherwise.
// Extracted from UpdateWebACL to keep its cyclomatic complexity down.
func applyOptionalWebACLConfigs(
	w *WebACL,
	customResponseBodies, associationConfig, captchaConfig, challengeConfig json.RawMessage,
	monetizationConfig, dataProtectionConfig, applicationConfig, onSourceDDoSProtectionConfig json.RawMessage,
) {
	if len(customResponseBodies) > 0 {
		w.CustomResponseBodies = customResponseBodies
	}

	if len(associationConfig) > 0 {
		w.AssociationConfig = associationConfig
	}

	if len(captchaConfig) > 0 {
		w.CaptchaConfig = captchaConfig
	}

	if len(challengeConfig) > 0 {
		w.ChallengeConfig = challengeConfig
	}

	if len(monetizationConfig) > 0 {
		w.MonetizationConfig = monetizationConfig
	}

	if len(dataProtectionConfig) > 0 {
		w.DataProtectionConfig = dataProtectionConfig
	}

	if len(applicationConfig) > 0 {
		w.ApplicationConfig = applicationConfig
	}

	if len(onSourceDDoSProtectionConfig) > 0 {
		w.OnSourceDDoSProtectionConfig = onSourceDDoSProtectionConfig
	}
}

// DeleteWebACL deletes a WebACL by ID.
func (b *InMemoryBackend) DeleteWebACL(ctx context.Context, id, lockToken string) error {
	b.mu.Lock("DeleteWebACL")
	defer b.mu.Unlock()

	requestRegion := getRegion(ctx, b.region)
	w, ok := b.lookupWebACLByID(requestRegion, id)
	if !ok {
		return fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, id)
	}

	// Use the resource's own store region (derived from its ARN).
	region := regionFromARN(w.ARN)

	if lockToken != "" && lockToken != w.LockToken {
		return fmt.Errorf("%w: lock token mismatch for web ACL %q", ErrOptimisticLock, id)
	}

	for _, assocID := range b.associations[region] {
		if assocID == id {
			return fmt.Errorf(
				"%w: web ACL %q is still associated with a resource; disassociate first",
				ErrAssociatedItem,
				id,
			)
		}
	}

	webACLArnStr := w.ARN

	b.webACLs.Delete(regionKey(region, id))
	delete(b.loggingConfigs[region], webACLArnStr)
	delete(b.permissionPolicies[region], webACLArnStr)

	return nil
}

// ListWebACLs returns all WebACLs sorted by name.
// For a REGIONAL request, returns REGIONAL resources from the ctx region PLUS
// any CLOUDFRONT (global) resources.
func (b *InMemoryBackend) ListWebACLs(ctx context.Context) []*WebACL {
	b.mu.RLock("ListWebACLs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	list := make([]*WebACL, 0)

	for _, w := range b.webACLsByRegion.Get(region) {
		list = append(list, cloneWebACL(w))
	}

	if region != "" {
		for _, w := range b.webACLsByRegion.Get("") {
			list = append(list, cloneWebACL(w))
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return list
}
func cloneWebACL(w *WebACL) *WebACL {
	cp := *w
	cp.Tags = maps.Clone(w.Tags)
	cp.Rules = cloneRules(w.Rules)
	cp.TokenDomains = cloneAddresses(w.TokenDomains)

	// Clone RawMessage fields (byte slices).
	if w.DefaultAction != nil {
		da := make(json.RawMessage, len(w.DefaultAction))
		copy(da, w.DefaultAction)
		cp.DefaultAction = da
	}

	if w.VisibilityConfig != nil {
		vc := make(json.RawMessage, len(w.VisibilityConfig))
		copy(vc, w.VisibilityConfig)
		cp.VisibilityConfig = vc
	}

	if w.CustomResponseBodies != nil {
		crb := make(json.RawMessage, len(w.CustomResponseBodies))
		copy(crb, w.CustomResponseBodies)
		cp.CustomResponseBodies = crb
	}

	if w.AssociationConfig != nil {
		ac := make(json.RawMessage, len(w.AssociationConfig))
		copy(ac, w.AssociationConfig)
		cp.AssociationConfig = ac
	}

	if w.CaptchaConfig != nil {
		cc := make(json.RawMessage, len(w.CaptchaConfig))
		copy(cc, w.CaptchaConfig)
		cp.CaptchaConfig = cc
	}

	if w.ChallengeConfig != nil {
		chc := make(json.RawMessage, len(w.ChallengeConfig))
		copy(chc, w.ChallengeConfig)
		cp.ChallengeConfig = chc
	}

	if w.MonetizationConfig != nil {
		mc := make(json.RawMessage, len(w.MonetizationConfig))
		copy(mc, w.MonetizationConfig)
		cp.MonetizationConfig = mc
	}

	if w.DataProtectionConfig != nil {
		dpc := make(json.RawMessage, len(w.DataProtectionConfig))
		copy(dpc, w.DataProtectionConfig)
		cp.DataProtectionConfig = dpc
	}

	if w.ApplicationConfig != nil {
		ac := make(json.RawMessage, len(w.ApplicationConfig))
		copy(ac, w.ApplicationConfig)
		cp.ApplicationConfig = ac
	}

	if w.OnSourceDDoSProtectionConfig != nil {
		osdc := make(json.RawMessage, len(w.OnSourceDDoSProtectionConfig))
		copy(osdc, w.OnSourceDDoSProtectionConfig)
		cp.OnSourceDDoSProtectionConfig = osdc
	}

	return &cp
}

// DeleteFirewallManagerRuleGroups removes all Firewall Manager rule group
// associations from the WebACL identified by webACLARN, then returns a fresh
// copy of the updated WebACL. lockToken is checked the same way every other
// Update*/Delete* op does: an empty token skips the match check by design
// (see PARITY.md), a non-empty mismatched one is rejected.
func (b *InMemoryBackend) DeleteFirewallManagerRuleGroups(
	ctx context.Context, webACLARN, lockToken string,
) (*WebACL, error) {
	b.mu.Lock("DeleteFirewallManagerRuleGroups")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	webACLID, ok := b.webACLIDByARNInRegion(webACLARN, region)
	if !ok {
		return nil, fmt.Errorf("%w: web ACL with ARN %q not found", ErrWebACLNotFound, webACLARN)
	}

	w, ok := b.webACLs.Get(regionKey(region, webACLID))
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, webACLID)
	}

	if lockToken != "" && lockToken != w.LockToken {
		return nil, fmt.Errorf("%w: lock token mismatch for web ACL %q", ErrOptimisticLock, webACLID)
	}

	w.LockToken = uuid.NewString()

	return cloneWebACL(w), nil
}
