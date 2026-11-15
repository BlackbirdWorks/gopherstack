package azurearm

import (
	"fmt"
	"maps"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	acrCacheRulesType     = "cacheRules"
	acrCredentialSetsType = "credentialSets"
	acrMaxCacheRules      = 1000
	acrPropLoginServer    = "loginServer"
	acrPropAuthCreds      = "authCredentials"
	acrPropSourceRepo     = "sourceRepository"
	acrPropTargetRepo     = "targetRepository"
	acrPropCredSetID      = "credentialSetResourceId"
	acrCredentialName     = "Credential1"
	acrWildcardSuffix     = "/*"
	acrHealthy            = "Healthy"
)

func isACRCacheKind(kind acrKind) bool {
	return kind == acrKindCacheRule || kind == acrKindCredentialSet
}

func (p *ContainerRegistryProvider) cacheChildren(rg, registry string, kind acrKind) []*storedACR {
	var out []*storedACR

	for _, r := range p.res {
		if r.kind == kind && resourceGroupsEqual(r.resourceGroup, rg) && strings.EqualFold(r.registry, registry) {
			out = append(out, r)
		}
	}

	return out
}

func acrInvalid(format string, args ...any) error {
	return badACRRequest(fmt.Errorf("%w: %s", ErrInvalidRequestBody, fmt.Sprintf(format, args...)))
}

func (p *ContainerRegistryProvider) putCacheChild(
	id ResourceID, kind acrKind, body map[string]any,
) (map[string]any, error) {
	parent, ok := p.registry(id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrACRRegistryNotFound
	}

	props := ehMerge(map[string]any{}, mapOf(body[fieldProperties]), nil)
	delete(props, fieldProvisioningState)
	delete(props, acrPropCreationDate)

	rec := &storedACR{
		name: id.LeafName(), resourceGroup: id.ResourceGroup, location: parent.location, createdAt: time.Now().UTC(),
		props: props, kind: kind, registry: parent.name,
	}

	old, exists := p.child(id, kind)
	if exists {
		rec.createdAt = old.createdAt
	}

	var err error

	if kind == acrKindCredentialSet {
		err = p.validateCredentialSet(rec, old, body)
	} else {
		err = p.applyCacheRule(id, rec)
	}

	if err != nil {
		return nil, err
	}

	p.res[acrKey(id.ResourceGroup, kind, id.Names[0], rec.name)] = rec

	return p.childBody(id, rec), nil
}

func (p *ContainerRegistryProvider) validateCredentialSet(rec, old *storedACR, body map[string]any) error {
	login := stringField(rec.props, acrPropLoginServer)
	if login == "" {
		return acrInvalid("credential set needs a loginServer")
	}

	creds, _ := rec.props[acrPropAuthCreds].([]any)
	if len(creds) == 0 || len(creds) > 1 {
		return acrInvalid("credential set needs exactly one %s", acrCredentialName)
	}

	for i, c := range creds {
		m := mapOf(c)
		if stringField(m, fieldName) != acrCredentialName {
			return acrInvalid("authCredentials name must be %s", acrCredentialName)
		}

		for _, key := range []string{"usernameSecretIdentifier", "passwordSecretIdentifier"} {
			if u, err := url.Parse(stringField(m, key)); err != nil || u.Scheme != acaSchemeHTTPS || u.Host == "" ||
				!strings.HasPrefix(u.Path, "/secrets/") {
				return acrInvalid("%s must be a Key Vault secret URI", key)
			}
		}

		delete(m, "credentialHealth")
		creds[i] = m
	}

	rec.identity = mapOf(body["identity"])
	if rec.identity == nil {
		rec.identity = map[string]any{fieldType: "SystemAssigned"}
	}

	rec.identity = maps.Clone(rec.identity)

	if old != nil && old.identity != nil && stringField(old.identity, "principalId") != "" {
		rec.identity["principalId"], rec.identity["tenantId"] = old.identity["principalId"], old.identity["tenantId"]
	} else {
		rec.identity["principalId"], rec.identity["tenantId"] = uuid.NewString(), uuid.NewString()
	}

	return nil
}

func (p *ContainerRegistryProvider) credentialSetOf(rec *storedACR) (*storedACR, bool) {
	rid := stringField(rec.props, acrPropCredSetID)
	if rid == "" {
		return nil, false
	}

	segs := strings.Split(strings.Trim(rid, "/"), "/")
	if n := len(segs); n >= 2 && strings.EqualFold(segs[n-2], acrCredentialSetsType) {
		return p.res[acrKey(rec.resourceGroup, acrKindCredentialSet, rec.registry, segs[n-1])], true
	}

	return nil, true
}

func sourceHost(src string) string {
	host, _, _ := strings.Cut(src, "/")

	return strings.ToLower(host)
}

func (p *ContainerRegistryProvider) applyCacheRule(id ResourceID, rec *storedACR) error {
	src, target := stringField(rec.props, acrPropSourceRepo), stringField(rec.props, acrPropTargetRepo)
	if !strings.Contains(src, "/") || target == "" {
		return acrInvalid("cache rule needs sourceRepository (host/path) and targetRepository")
	}

	if strings.HasSuffix(src, acrWildcardSuffix) != strings.HasSuffix(target, acrWildcardSuffix) ||
		strings.Count(src, "*") > 1 || strings.Count(target, "*") > 1 {
		return acrInvalid("sourceRepository and targetRepository must both end in /* or neither")
	}

	siblings := p.cacheChildren(id.ResourceGroup, rec.registry, acrKindCacheRule)
	if len(siblings) >= acrMaxCacheRules {
		return acrInvalid("a registry supports at most %d cache rules", acrMaxCacheRules)
	}

	for _, other := range siblings {
		if strings.EqualFold(other.name, rec.name) {
			continue
		}

		if acrRulesOverlap(target, stringField(other.props, acrPropTargetRepo)) {
			return acrInvalid("cache rule overlaps rule %s", other.name)
		}
	}

	var creds *ContainerRegistryCacheCredentials

	if cs, wanted := p.credentialSetOf(rec); wanted {
		if cs == nil {
			return acrInvalid("credentialSetResourceId does not name a credential set of this registry")
		}

		if !strings.EqualFold(stringField(cs.props, acrPropLoginServer), sourceHost(src)) {
			return acrInvalid("the credential set's loginServer does not match the source registry")
		}

		first := mapOf(firstOf(cs.props[acrPropAuthCreds]))
		creds = &ContainerRegistryCacheCredentials{
			LoginServer:    stringField(cs.props, acrPropLoginServer),
			UsernameSecret: stringField(first, "usernameSecretIdentifier"),
			PasswordSecret: stringField(first, "passwordSecretIdentifier"),
		}
	}

	if err := p.dataPlane.SetCacheRule(rec.registry, ContainerRegistryCacheRule{
		Name: rec.name, Source: src, Target: target, Credentials: creds,
	}); err != nil {
		return badACRRequest(err)
	}

	return nil
}

func firstOf(v any) any {
	if l, _ := v.([]any); len(l) > 0 {
		return l[0]
	}

	return nil
}

func acrRulesOverlap(a, b string) bool {
	pa, pb := strings.TrimSuffix(a, "*"), strings.TrimSuffix(b, "*")
	wa, wb := strings.HasSuffix(a, "*"), strings.HasSuffix(b, "*")

	switch {
	case wa && wb:
		return strings.HasPrefix(pa, pb) || strings.HasPrefix(pb, pa)
	case wa:
		return strings.HasPrefix(b, pa)
	case wb:
		return strings.HasPrefix(a, pb)
	}

	return strings.EqualFold(a, b)
}

func (p *ContainerRegistryProvider) patchCacheChild(
	id ResourceID, kind acrKind, body map[string]any,
) (map[string]any, error) {
	rec, ok := p.child(id, kind)
	if !ok {
		return nil, acrNotFound(kind)
	}

	merged := ehMerge(map[string]any{}, rec.props, mapOf(body[fieldProperties]))

	next := map[string]any{fieldProperties: merged}
	if v := mapOf(body["identity"]); v != nil {
		next["identity"] = v
	} else if rec.identity != nil {
		next["identity"] = rec.identity
	}

	return p.putCacheChild(id, kind, next)
}

// beforeCacheDelete removes the data-plane rule of a cache rule and refuses to delete a credential set in use.
func (p *ContainerRegistryProvider) beforeCacheDelete(id ResourceID, kind acrKind) error {
	switch kind {
	case acrKindCacheRule:
		return p.dataPlane.DeleteCacheRule(id.Names[0], id.LeafName())
	case acrKindCredentialSet:
		for _, rule := range p.cacheChildren(id.ResourceGroup, id.Names[0], acrKindCacheRule) {
			if cs, _ := p.credentialSetOf(rule); cs != nil && strings.EqualFold(cs.name, id.LeafName()) {
				return ErrACRCredentialSetInUse
			}
		}
	case acrKindRegistry, acrKindReplication, acrKindWebhook, acrKindScopeMap, acrKindToken, acrKindUnsupported:
	}

	return nil
}

// withCacheFields adds creationDate, identity and credential health to cache rule and credential set bodies.
func (p *ContainerRegistryProvider) withCacheFields(out map[string]any, rec *storedACR) map[string]any {
	if !isACRCacheKind(rec.kind) {
		return out
	}

	props := mapOf(out[fieldProperties])
	props[acrPropCreationDate] = rec.createdAt.UTC().Format(time.RFC3339)

	if rec.kind == acrKindCredentialSet {
		delete(out, fieldLocation)
		delete(out, fieldTags)

		if rec.identity != nil {
			out["identity"] = rec.identity
		}

		props[acrPropAuthCreds] = p.withHealth(rec.props[acrPropAuthCreds])
	} else {
		delete(out, fieldLocation)
		delete(out, fieldTags)
	}

	return out
}

func (p *ContainerRegistryProvider) withHealth(raw any) []any {
	in, _ := raw.([]any)
	out := make([]any, 0, len(in))

	for _, c := range in {
		m := maps.Clone(mapOf(c))

		healthy, code, msg := p.dataPlane.CredentialHealth(
			stringField(m, "usernameSecretIdentifier"), stringField(m, "passwordSecretIdentifier"))
		if healthy {
			m["credentialHealth"] = map[string]any{acrPropStatus: acrHealthy}
		} else {
			m["credentialHealth"] = map[string]any{acrPropStatus: "Unhealthy", "errorCode": code, "errorMessage": msg}
		}

		out = append(out, m)
	}

	return out
}
