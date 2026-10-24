package azurearm

import (
	"context"
	"maps"
	"strings"
	"time"
)

const (
	acrFieldDefaultAction = "defaultAction"
	acrStatusDisabled     = "disabled"
	acrRetentionDays      = 7
	acrCredPassword       = "password"
	acrCredPassword2      = "password2"
)

func (p *ContainerRegistryProvider) registryBody(
	id ResourceID, rec *storedACR, info ContainerRegistryInfo,
) (map[string]any, error) {
	props := ehMerge(acrDefaultProps(), rec.extra, map[string]any{
		fieldProvisioningState: provisioningStateSucceeded,
		"loginServer":          p.loginServer(rec.name, rec.host),
		"creationDate":         rec.createdAt.UTC().Format(time.RFC3339),
		acrPropAdmin:           info.AdminEnabled,
		acrPropStatus:          map[string]any{"displayStatus": "Ready", fieldMessage: "The registry is ready."},
	})

	out := map[string]any{
		"id": id.ARMID(), fieldName: rec.name, fieldType: acrTypeName(acrKindRegistry),
		fieldLocation: rec.location, fieldTags: tagsOrEmpty(rec.tags), fieldProperties: props,
		acrFieldSKU: map[string]any{fieldName: rec.sku, cosmosFieldTier: rec.sku},
	}

	if rec.identity != nil {
		out["identity"] = rec.identity
	}

	return out, nil
}

func acrDefaultProps() map[string]any {
	return map[string]any{
		"networkRuleSet":           map[string]any{acrFieldDefaultAction: "Allow", ehFieldIPRules: []any{}},
		cosmosFieldPublicNetwork:   kvPublicNetworkEnabled,
		"networkRuleBypassOptions": "AzureServices",
		"dataEndpointEnabled":      false,
		"dataEndpointHostNames":    []any{},
		"zoneRedundancy":           redisPublicDisabled,
		"anonymousPullEnabled":     false,
		"encryption":               map[string]any{acrPropStatus: acrStatusDisabled},
		"policies": map[string]any{
			"quarantinePolicy": map[string]any{acrPropStatus: acrStatusDisabled},
			"trustPolicy":      map[string]any{"type": "Notary", acrPropStatus: acrStatusDisabled},
			"retentionPolicy":  map[string]any{"days": acrRetentionDays, acrPropStatus: acrStatusDisabled},
			"exportPolicy":     map[string]any{"status": "enabled"},
			"azureADAuthenticationAsArmPolicy": map[string]any{
				"status": "enabled",
			},
			"softDeletePolicy": map[string]any{"retentionDays": acrRetentionDays, acrPropStatus: acrStatusDisabled},
		},
	}
}

func (p *ContainerRegistryProvider) childBody(id ResourceID, rec *storedACR) map[string]any {
	props := map[string]any{fieldProvisioningState: provisioningStateSucceeded}

	if rec.kind == acrKindReplication {
		props["status"] = map[string]any{"displayStatus": "Ready", fieldMessage: "The replication is ready."}
		props["regionEndpointEnabled"] = true
		props["zoneRedundancy"] = "Disabled"
	}

	maps.Copy(props, rec.props)

	if rec.kind == acrKindWebhook {
		delete(props, "customHeaders")
	}

	return map[string]any{
		"id": id.ARMID(), fieldName: rec.name, fieldType: acrTypeName(rec.kind),
		fieldLocation: rec.location, fieldTags: tagsOrEmpty(rec.tags), fieldProperties: props,
	}
}

func credentialBody(c ContainerRegistryCredentials) map[string]any {
	return map[string]any{
		"username": c.Username,
		"passwords": []any{
			map[string]any{fieldName: acrCredPassword, fieldValue: c.Password},
			map[string]any{fieldName: acrCredPassword2, fieldValue: c.Password2},
		},
	}
}

// ListCredentials answers POST .../listCredentials.
func (p *ContainerRegistryProvider) ListCredentials(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("ListCredentials")
	defer p.mu.RUnlock()

	rec, ok := p.registry(id.ResourceGroup, id.LeafName())
	if !ok {
		return nil, ErrACRRegistryNotFound
	}

	creds, err := p.dataPlane.Credentials(rec.name)
	if err != nil {
		return nil, err
	}

	return credentialBody(creds), nil
}

// RegenerateCredential answers POST .../regenerateCredential.
func (p *ContainerRegistryProvider) RegenerateCredential(
	_ context.Context, id ResourceID, which string,
) (map[string]any, error) {
	if !strings.EqualFold(which, acrCredPassword) && !strings.EqualFold(which, acrCredPassword2) {
		return nil, ErrACRInvalidCredential
	}

	p.mu.RLock("RegenerateCredential")
	defer p.mu.RUnlock()

	rec, ok := p.registry(id.ResourceGroup, id.LeafName())
	if !ok {
		return nil, ErrACRRegistryNotFound
	}

	if info, found := p.dataPlane.RegistryInfo(rec.name); !found || !info.AdminEnabled {
		return nil, ErrACRAdminDisabled
	}

	creds, err := p.dataPlane.RegenerateCredential(rec.name, strings.ToLower(which))
	if err != nil {
		return nil, err
	}

	return credentialBody(creds), nil
}

// CallbackConfig answers POST .../webhooks/{name}/getCallbackConfig.
func (p *ContainerRegistryProvider) CallbackConfig(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("CallbackConfig")
	defer p.mu.RUnlock()

	rec, ok := p.child(id, acrKindWebhook)
	if !ok {
		return nil, ErrACRWebhookNotFound
	}

	headers, _ := rec.props["customHeaders"].(map[string]any)
	if headers == nil {
		headers = map[string]any{}
	}

	return map[string]any{acrPropServiceURI: rec.props[acrPropServiceURI], "customHeaders": headers}, nil
}

// PingWebhook answers POST .../webhooks/{name}/ping with the id of the ping event it delivers.
func (p *ContainerRegistryProvider) PingWebhook(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("PingWebhook")
	defer p.mu.RUnlock()

	rec, ok := p.child(id, acrKindWebhook)
	if !ok {
		return nil, ErrACRWebhookNotFound
	}

	eventID, err := p.dataPlane.PingWebhook(rec.registry, rec.name)
	if err != nil {
		return nil, err
	}

	return map[string]any{"id": eventID}, nil
}

// WebhookEvents answers POST .../webhooks/{name}/listEvents.
func (p *ContainerRegistryProvider) WebhookEvents(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("WebhookEvents")
	defer p.mu.RUnlock()

	rec, ok := p.child(id, acrKindWebhook)
	if !ok {
		return nil, ErrACRWebhookNotFound
	}

	events, err := p.dataPlane.WebhookEvents(rec.registry, rec.name)
	if err != nil {
		return nil, err
	}

	value := make([]any, 0, len(events))
	for _, e := range events {
		value = append(value, e)
	}

	return map[string]any{fieldValue: value}, nil
}

type acrRecord struct {
	CreatedAt     time.Time         `json:"createdAt"`
	Tags          map[string]string `json:"tags,omitempty"`
	Extra         map[string]any    `json:"extra,omitempty"`
	Props         map[string]any    `json:"props,omitempty"`
	Identity      map[string]any    `json:"identity,omitempty"`
	Name          string            `json:"name"`
	ResourceGroup string            `json:"resourceGroup"`
	Location      string            `json:"location"`
	Host          string            `json:"host,omitempty"`
	SKU           string            `json:"sku,omitempty"`
	Registry      string            `json:"registry,omitempty"`
	Kind          int               `json:"kind"`
}

// containerRegistrySnapshot is the Microsoft.ContainerRegistry provider's persisted ARM-side state.
type containerRegistrySnapshot struct {
	Resources map[string]acrRecord `json:"resources,omitempty"`
}

func (p *ContainerRegistryProvider) snapshot() *containerRegistrySnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &containerRegistrySnapshot{Resources: make(map[string]acrRecord, len(p.res))}

	for k, r := range p.res {
		snap.Resources[k] = acrRecord{
			CreatedAt: r.createdAt, Tags: r.tags, Extra: r.extra, Props: r.props, Identity: r.identity,
			Name: r.name, ResourceGroup: r.resourceGroup, Location: r.location, Host: r.host, SKU: r.sku,
			Registry: r.registry, Kind: int(r.kind),
		}
	}

	return snap
}

// restore replaces the provider's state; nil clears it. The data plane is not touched.
func (p *ContainerRegistryProvider) restore(snap *containerRegistrySnapshot) {
	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.res = map[string]*storedACR{}

	if snap == nil {
		return
	}

	for k, r := range snap.Resources {
		p.res[k] = &storedACR{
			createdAt: r.CreatedAt, tags: r.Tags, extra: r.Extra, props: r.Props, identity: r.Identity,
			name: r.Name, resourceGroup: r.ResourceGroup, location: r.Location, host: r.Host, sku: r.SKU,
			registry: r.Registry, kind: acrKind(r.Kind),
		}
	}
}

func (h *Handler) containerRegistryProvider() (*ContainerRegistryProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftContainerRegistry)
	if !ok {
		return nil, false
	}

	cr, ok := p.(*ContainerRegistryProvider)

	return cr, ok
}
