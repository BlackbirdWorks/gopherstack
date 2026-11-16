package azurearm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	acrScopeMapsType      = "scopeMaps"
	acrTokensType         = "tokens"
	acrPropScopeMapID     = "scopeMapId"
	acrPropCreationDate   = "creationDate"
	acrPropCredentials    = "credentials"
	acrTokenEnabled       = "enabled"
	acrActionGenCreds     = "generateCredentials"
	acrScopeMapTypeSystem = "SystemDefined"
	acrScopeMapTypeUser   = "UserDefined"
)

func isACRDataChild(kind acrKind) bool { return kind == acrKindScopeMap || kind == acrKindToken }

func (p *ContainerRegistryProvider) scopeMapID(id ResourceID, name string) string {
	return ResourceID{
		SubscriptionID: id.SubscriptionID, ResourceGroup: id.ResourceGroup, Namespace: id.Namespace,
		Types: []string{acrRegistriesType, acrScopeMapsType}, Names: []string{id.Names[0], name},
	}.ARMID()
}

func (p *ContainerRegistryProvider) scopeMapBody(id ResourceID, sm ContainerRegistryScopeMap) map[string]any {
	typ := acrScopeMapTypeUser
	if sm.System {
		typ = acrScopeMapTypeSystem
	}

	actions := make([]any, 0, len(sm.Actions))
	for _, a := range sm.Actions {
		actions = append(actions, a)
	}

	cid := ResourceID{
		SubscriptionID: id.SubscriptionID, ResourceGroup: id.ResourceGroup, Namespace: id.Namespace,
		Types: []string{acrRegistriesType, acrScopeMapsType}, Names: []string{id.Names[0], sm.Name},
	}

	return map[string]any{
		"id": cid.ARMID(), fieldName: sm.Name, fieldType: acrTypeName(acrKindScopeMap),
		fieldProperties: map[string]any{
			fieldProvisioningState: provisioningStateSucceeded, authFieldDescription: sm.Description,
			fieldType: typ, acrPropCreationDate: sm.CreatedAt.UTC().Format(time.RFC3339), acrPropActions: actions,
		},
	}
}

func passwordMeta(pw ContainerRegistryTokenPassword) map[string]any {
	out := map[string]any{
		"creationTime": pw.Created.UTC().Format(time.RFC3339), fieldName: pw.Name,
	}

	if pw.Expiry != nil {
		out["expiry"] = pw.Expiry.UTC().Format(time.RFC3339)
	}

	return out
}

func (p *ContainerRegistryProvider) tokenBody(id ResourceID, t ContainerRegistryToken) map[string]any {
	status := acrTokenEnabled
	if !t.Enabled {
		status = acrStatusDisabled
	}

	passwords := make([]any, 0, len(t.Passwords))
	for _, pw := range t.Passwords {
		passwords = append(passwords, passwordMeta(pw))
	}

	cid := ResourceID{
		SubscriptionID: id.SubscriptionID, ResourceGroup: id.ResourceGroup, Namespace: id.Namespace,
		Types: []string{acrRegistriesType, acrTokensType}, Names: []string{id.Names[0], t.Name},
	}

	return map[string]any{
		"id": cid.ARMID(), fieldName: t.Name, fieldType: acrTypeName(acrKindToken),
		fieldProperties: map[string]any{
			fieldProvisioningState: provisioningStateSucceeded,
			acrPropCreationDate:    t.CreatedAt.UTC().Format(time.RFC3339),
			acrPropScopeMapID:      p.scopeMapID(id, t.ScopeMap),
			acrPropStatus:          status,
			acrPropCredentials:     map[string]any{acrFieldPasswords: passwords, "certificates": []any{}},
		},
	}
}

func stringsOf(v any) []string {
	raw, _ := v.([]any)

	out := make([]string, 0, len(raw))

	for _, e := range raw {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

// scopeMapName extracts the scope map name from a scope map resource ID of the same registry.
func scopeMapName(scopeMapID, registry string) (string, error) {
	segs := strings.Split(strings.Trim(scopeMapID, "/"), "/")

	n := len(segs)
	if n < 2 || !strings.EqualFold(segs[n-2], acrScopeMapsType) || segs[n-1] == "" ||
		n < 4 || !strings.EqualFold(segs[n-3], registry) || !strings.EqualFold(segs[n-4], acrRegistriesType) {
		return "", fmt.Errorf("%w: scopeMapId must name a scope map of registry %s", ErrInvalidRequestBody, registry)
	}

	return segs[n-1], nil
}

// putDataChild creates or updates a scope map or token; the data plane owns their state.
func (p *ContainerRegistryProvider) putDataChild(
	id ResourceID, kind acrKind, body map[string]any,
) (map[string]any, error) {
	reg, ok := p.registry(id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrACRRegistryNotFound
	}

	props := mapOf(body[fieldProperties])

	if kind == acrKindScopeMap {
		desc, _ := props[authFieldDescription].(string)

		if err := p.dataPlane.SetScopeMap(reg.name, id.LeafName(), desc, stringsOf(props[acrPropActions])); err != nil {
			return nil, badACRRequest(err)
		}

		return p.getDataChild(id, kind)
	}

	sm, err := scopeMapName(stringField(props, acrPropScopeMapID), reg.name)
	if err != nil {
		return nil, err
	}

	status, _ := props[acrPropStatus].(string)
	if status != "" && status != acrTokenEnabled && status != acrStatusDisabled {
		return nil, badACRRequest(fmt.Errorf("%w: token status must be enabled or disabled", ErrInvalidRequestBody))
	}

	if err = p.dataPlane.SetToken(reg.name, id.LeafName(), sm, status != acrStatusDisabled); err != nil {
		return nil, badACRRequest(err)
	}

	return p.getDataChild(id, kind)
}

func stringField(m map[string]any, key string) string {
	s, _ := m[key].(string)

	return s
}

func (p *ContainerRegistryProvider) getDataChild(id ResourceID, kind acrKind) (map[string]any, error) {
	reg, ok := p.registry(id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrACRRegistryNotFound
	}

	items, err := p.listDataChildren(id, kind, reg.name)
	if err != nil {
		return nil, err
	}

	for _, item := range items {
		if strings.EqualFold(nameField(item), id.LeafName()) {
			return item, nil
		}
	}

	return nil, acrNotFound(kind)
}

func (p *ContainerRegistryProvider) listDataChildren(
	id ResourceID, kind acrKind, registry string,
) ([]map[string]any, error) {
	out := []map[string]any{}

	if kind == acrKindScopeMap {
		maps, err := p.dataPlane.ScopeMaps(registry)
		if err != nil {
			return nil, err
		}

		for _, sm := range maps {
			out = append(out, p.scopeMapBody(id, sm))
		}

		return out, nil
	}

	tokens, err := p.dataPlane.Tokens(registry)
	if err != nil {
		return nil, err
	}

	for _, t := range tokens {
		out = append(out, p.tokenBody(id, t))
	}

	return out, nil
}

func (p *ContainerRegistryProvider) deleteDataChild(id ResourceID, kind acrKind) error {
	reg, ok := p.registry(id.ResourceGroup, id.Names[0])
	if !ok {
		return ErrACRRegistryNotFound
	}

	if kind == acrKindScopeMap {
		return p.dataPlane.DeleteScopeMap(reg.name, id.LeafName())
	}

	return p.dataPlane.DeleteToken(reg.name, id.LeafName())
}

// patchDataChild applies a scope map or token update; omitted properties stay.
func (p *ContainerRegistryProvider) patchDataChild(
	id ResourceID, kind acrKind, body map[string]any,
) (map[string]any, error) {
	cur, err := p.getDataChild(id, kind)
	if err != nil {
		return nil, err
	}

	merged := ehMerge(map[string]any{}, mapOf(cur[fieldProperties]), mapOf(body[fieldProperties]))

	return p.putDataChild(id, kind, map[string]any{fieldProperties: merged})
}

// GenerateCredentials answers POST .../generateCredentials: new passwords for a token, shown once.
func (p *ContainerRegistryProvider) GenerateCredentials(
	_ context.Context, id ResourceID, body map[string]any,
) (map[string]any, error) {
	p.mu.RLock("GenerateCredentials")
	defer p.mu.RUnlock()

	reg, ok := p.registry(id.ResourceGroup, id.LeafName())
	if !ok {
		return nil, ErrACRRegistryNotFound
	}

	tokenID, _ := body["tokenId"].(string)
	segs := strings.Split(strings.Trim(tokenID, "/"), "/")

	if len(segs) < 2 || !strings.EqualFold(segs[len(segs)-2], acrTokensType) {
		return nil, badACRRequest(fmt.Errorf("%w: tokenId must name a token", ErrInvalidRequestBody))
	}

	token := segs[len(segs)-1]

	var expiry *time.Time

	if raw, _ := body["expiry"].(string); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return nil, badACRRequest(fmt.Errorf("%w: expiry: %w", ErrInvalidRequestBody, err))
		}

		expiry = &parsed
	}

	names := []string{"password1", "password2"}
	if name, _ := body[fieldName].(string); name != "" {
		names = []string{name}
	}

	passwords := make([]any, 0, len(names))

	for _, name := range names {
		pw, err := p.dataPlane.GenerateTokenPassword(reg.name, token, name, expiry)
		if err != nil {
			return nil, badACRRequest(err)
		}

		entry := passwordMeta(pw)
		entry[fieldValue] = pw.Value
		passwords = append(passwords, entry)
	}

	return map[string]any{acrFieldUsername: token, acrFieldPasswords: passwords}, nil
}

// ImportImage answers POST .../importImage.
func (p *ContainerRegistryProvider) ImportImage(
	ctx context.Context, id ResourceID, body map[string]any,
) (map[string]any, error) {
	p.mu.RLock("ImportImage")
	defer p.mu.RUnlock()

	reg, ok := p.registry(id.ResourceGroup, id.LeafName())
	if !ok {
		return nil, ErrACRRegistryNotFound
	}

	src := mapOf(body["source"])
	req := ContainerRegistryImport{
		SourceImage: stringField(src, "sourceImage"), SourceURI: stringField(src, "registryUri"),
		TargetTags: stringsOf(body["targetTags"]), UntaggedTargets: stringsOf(body["untaggedTargetRepositories"]),
		Force: strings.EqualFold(stringField(body, "mode"), "Force"),
	}

	if rid := stringField(src, "resourceId"); rid != "" {
		name, err := registryNameOf(rid)
		if err != nil {
			return nil, err
		}

		req.SourceRegistry = name
	}

	creds := mapOf(src["credentials"])
	req.Username, req.Password = stringField(creds, "username"), stringField(creds, "password")

	if err := p.dataPlane.ImportImage(ctx, reg.name, req); err != nil {
		if errors.Is(err, ErrACRImportNotFound) || errors.Is(err, ErrACRImportTagExists) {
			return nil, err
		}

		return nil, badACRRequest(err)
	}

	return map[string]any{}, nil
}

func registryNameOf(resourceID string) (string, error) {
	segs := strings.Split(strings.Trim(resourceID, "/"), "/")
	if n := len(segs); n >= 2 && strings.EqualFold(segs[n-2], acrRegistriesType) && segs[n-1] != "" {
		return segs[n-1], nil
	}

	return "", badACRRequest(fmt.Errorf("%w: source.resourceId must name a container registry", ErrInvalidRequestBody))
}
