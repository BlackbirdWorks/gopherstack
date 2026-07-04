package apigateway

import (
	"encoding/json"
)

type apiDataSnapshot struct {
	Resources             map[string]*Resource             `json:"resources"`
	Deployments           map[string]*Deployment           `json:"deployments"`
	Stages                map[string]*Stage                `json:"stages"`
	Authorizers           map[string]*Authorizer           `json:"authorizers"`
	RequestValidators     map[string]*RequestValidator     `json:"requestValidators"`
	DocumentationParts    map[string]*DocumentationPart    `json:"documentationParts"`
	DocumentationVersions map[string]*DocumentationVersion `json:"documentationVersions"`
	Models                map[string]*Model                `json:"models"`
	API                   RestAPI                          `json:"api"`
}

type backendSnapshot struct {
	APIs                         map[string]*apiDataSnapshot             `json:"apis"`
	APIKeys                      map[string]*APIKey                      `json:"apiKeys"`
	BasePathMappings             map[string]*BasePathMapping             `json:"basePathMappings"`
	DomainNames                  map[string]*DomainName                  `json:"domainNames"`
	DomainNameAccessAssociations map[string]*DomainNameAccessAssociation `json:"domainNameAccessAssociations"`
	UsagePlans                   map[string]*UsagePlan                   `json:"usagePlans"`
	UsagePlanKeys                map[string]map[string]*UsagePlanKey     `json:"usagePlanKeys"`
	Account                      *Account                                `json:"account,omitempty"`
	GatewayResponses             map[string]*GatewayResponse             `json:"gatewayResponses,omitempty"`
	ClientCertificates           map[string]*ClientCertificate           `json:"clientCertificates,omitempty"`
	VpcLinks                     map[string]*VpcLink                     `json:"vpcLinks,omitempty"`
	UsageOverrides               map[string]map[string]int64             `json:"usageOverrides,omitempty"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		APIs:                         make(map[string]*apiDataSnapshot, len(b.apis)),
		APIKeys:                      b.apiKeys,
		BasePathMappings:             b.basePathMappings,
		DomainNames:                  b.domainNames,
		DomainNameAccessAssociations: b.domainNameAccessAssociations,
		UsagePlans:                   b.usagePlans,
		UsagePlanKeys:                b.usagePlanKeys,
		Account:                      b.account,
		GatewayResponses:             b.gatewayResponses,
		ClientCertificates:           b.clientCertificates,
		VpcLinks:                     b.vpcLinks,
		UsageOverrides:               b.usageOverrides,
	}

	for id, d := range b.apis {
		snap.APIs[id] = &apiDataSnapshot{
			API:                   d.api,
			Resources:             d.resources,
			Deployments:           d.deployments,
			Stages:                d.stages,
			Authorizers:           d.authorizers,
			RequestValidators:     d.requestValidators,
			DocumentationParts:    d.documentationParts,
			DocumentationVersions: d.documentationVersions,
			Models:                d.models,
		}
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.restoreAPIs(snap.APIs)
	b.restoreMaps(snap)

	return nil
}

// restoreAPIs restores API data from the snapshot.
func (b *InMemoryBackend) restoreAPIs(apis map[string]*apiDataSnapshot) {
	b.apis = make(map[string]*apiData, len(apis))

	for id, d := range apis {
		ensureAPIDataMaps(d)
		b.apis[id] = &apiData{
			api:                   d.API,
			resources:             d.Resources,
			deployments:           d.Deployments,
			stages:                d.Stages,
			authorizers:           d.Authorizers,
			requestValidators:     d.RequestValidators,
			documentationParts:    d.DocumentationParts,
			documentationVersions: d.DocumentationVersions,
			models:                d.Models,
		}
	}
}

// ensureAPIDataMaps initialises nil map fields in an API data snapshot.
func ensureAPIDataMaps(d *apiDataSnapshot) {
	if d.Resources == nil {
		d.Resources = make(map[string]*Resource)
	}

	if d.Deployments == nil {
		d.Deployments = make(map[string]*Deployment)
	}

	if d.Stages == nil {
		d.Stages = make(map[string]*Stage)
	}

	if d.Authorizers == nil {
		d.Authorizers = make(map[string]*Authorizer)
	}

	if d.RequestValidators == nil {
		d.RequestValidators = make(map[string]*RequestValidator)
	}

	if d.DocumentationParts == nil {
		d.DocumentationParts = make(map[string]*DocumentationPart)
	}

	if d.DocumentationVersions == nil {
		d.DocumentationVersions = make(map[string]*DocumentationVersion)
	}

	if d.Models == nil {
		d.Models = make(map[string]*Model)
	}
}

// restoreMaps restores the flat map fields from the snapshot.
func (b *InMemoryBackend) restoreMaps(snap backendSnapshot) {
	if snap.APIKeys != nil {
		b.apiKeys = snap.APIKeys
	} else {
		b.apiKeys = make(map[string]*APIKey)
	}

	if snap.BasePathMappings != nil {
		b.basePathMappings = snap.BasePathMappings
	} else {
		b.basePathMappings = make(map[string]*BasePathMapping)
	}

	if snap.DomainNames != nil {
		b.domainNames = snap.DomainNames
	} else {
		b.domainNames = make(map[string]*DomainName)
	}

	if snap.DomainNameAccessAssociations != nil {
		b.domainNameAccessAssociations = snap.DomainNameAccessAssociations
	} else {
		b.domainNameAccessAssociations = make(map[string]*DomainNameAccessAssociation)
	}

	if snap.UsagePlans != nil {
		b.usagePlans = snap.UsagePlans
	} else {
		b.usagePlans = make(map[string]*UsagePlan)
	}

	if snap.UsagePlanKeys != nil {
		b.usagePlanKeys = snap.UsagePlanKeys
	} else {
		b.usagePlanKeys = make(map[string]map[string]*UsagePlanKey)
	}

	if snap.Account != nil {
		b.account = snap.Account
	} else {
		b.account = &Account{}
	}

	b.restoreMapsExt(snap)
}

// restoreMapsExt restores the remaining flat map fields, split out from
// restoreMaps to keep cognitive complexity down.
func (b *InMemoryBackend) restoreMapsExt(snap backendSnapshot) {
	if snap.GatewayResponses != nil {
		b.gatewayResponses = snap.GatewayResponses
	} else {
		b.gatewayResponses = make(map[string]*GatewayResponse)
	}

	if snap.ClientCertificates != nil {
		b.clientCertificates = snap.ClientCertificates
	} else {
		b.clientCertificates = make(map[string]*ClientCertificate)
	}

	if snap.VpcLinks != nil {
		b.vpcLinks = snap.VpcLinks
	} else {
		b.vpcLinks = make(map[string]*VpcLink)
	}

	if snap.UsageOverrides != nil {
		b.usageOverrides = snap.UsageOverrides
	} else {
		b.usageOverrides = make(map[string]map[string]int64)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
