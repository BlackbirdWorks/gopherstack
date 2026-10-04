package apigateway

import (
	"fmt"
	"maps"
	"slices"
)

// DeploymentConfig is the immutable snapshot of a REST API's invocable
// configuration captured by CreateDeployment. Real API Gateway serves a
// stage's traffic from the snapshot taken when it was deployed, not from the
// live (possibly since-edited) resources/methods/integrations --
// api-gateway-basic-concept.html: "deploying an API...creates a snapshot of
// the API and makes it callable"; edits after that point are invisible to
// the stage until the next CreateDeployment. The data-plane proxy (proxy*.go)
// resolves every request against a stage's deployment Config instead of
// InMemoryBackend's live tables.
type DeploymentConfig struct {
	Models                 map[string]*Model            `json:"models,omitempty"`
	RequestValidators      map[string]*RequestValidator `json:"requestValidators,omitempty"`
	Authorizers            map[string]*Authorizer       `json:"authorizers,omitempty"`
	GatewayResponses       map[string]*GatewayResponse  `json:"gatewayResponses,omitempty"`
	Resources              []Resource                   `json:"resources,omitempty"`
	MinimumCompressionSize int                          `json:"minimumCompressionSize,omitempty"`
}

// snapshotDeploymentConfig deep-copies restAPIID's current resources (with
// their nested methods, integrations, and method/integration responses),
// models, request validators, authorizers, gateway responses, and the
// RestApi-level settings the invoke path reads (minimumCompressionSize), for
// CreateDeployment to attach to the new Deployment. A deep copy is required,
// not a slice/map copy: Resource/Method/Integration are stored as pointers
// mutated in place by PutMethod/PutIntegration/etc, so anything less would
// let a later edit leak into deployments that already captured this state.
// Caller must hold b.mu (CreateDeployment does).
func (b *InMemoryBackend) snapshotDeploymentConfig(restAPIID string) *DeploymentConfig {
	live := b.resourcesByAPI.Get(restAPIID)
	resources := make([]Resource, 0, len(live))

	for _, r := range live {
		resources = append(resources, deepCopyResource(r))
	}

	cfg := &DeploymentConfig{
		Resources:         resources,
		Models:            make(map[string]*Model),
		RequestValidators: make(map[string]*RequestValidator),
		Authorizers:       make(map[string]*Authorizer),
		GatewayResponses:  make(map[string]*GatewayResponse),
	}

	for _, m := range b.modelsByAPI.Get(restAPIID) {
		cp := *m
		cfg.Models[cp.Name] = &cp
	}

	for _, v := range b.requestValidatorsByAPI.Get(restAPIID) {
		cp := *v
		cfg.RequestValidators[cp.ID] = &cp
	}

	for _, a := range b.authorizersByAPI.Get(restAPIID) {
		cfg.Authorizers[a.ID] = deepCopyAuthorizer(a)
	}

	for _, rt := range gatewayResponseTypes {
		if gr, ok := b.gatewayResponses.Get(gatewayResponseKey(restAPIID, rt)); ok {
			cfg.GatewayResponses[rt] = deepCopyGatewayResponse(gr)
		}
	}

	if api, ok := b.restApis.Get(restAPIID); ok {
		cfg.MinimumCompressionSize = api.MinimumCompressionSize
	}

	return cfg
}

// deepCopyResource copies r and everything the invoke path reads through it:
// its per-method map (methods, their integration, and method/integration
// responses) and its CORS configuration.
func deepCopyResource(r *Resource) Resource {
	cp := *r

	if r.ResourceMethods != nil {
		cp.ResourceMethods = make(map[string]*Method, len(r.ResourceMethods))
		for httpMethod, m := range r.ResourceMethods {
			mc := deepCopyMethod(m)
			cp.ResourceMethods[httpMethod] = &mc
		}
	}

	if r.CorsConfiguration != nil {
		corsCopy := *r.CorsConfiguration
		cp.CorsConfiguration = &corsCopy
	}

	return cp
}

func deepCopyMethod(m *Method) Method {
	cp := *m
	cp.RequestParameters = maps.Clone(m.RequestParameters)
	cp.RequestModels = maps.Clone(m.RequestModels)

	if m.MethodIntegration != nil {
		ic := deepCopyIntegration(m.MethodIntegration)
		cp.MethodIntegration = &ic
	}

	if m.MethodResponses != nil {
		cp.MethodResponses = make(map[string]*MethodResponse, len(m.MethodResponses))
		for status, resp := range m.MethodResponses {
			rc := *resp
			rc.ResponseModels = maps.Clone(resp.ResponseModels)
			rc.ResponseParameters = maps.Clone(resp.ResponseParameters)
			cp.MethodResponses[status] = &rc
		}
	}

	return cp
}

func deepCopyIntegration(i *Integration) Integration {
	cp := *i
	cp.RequestTemplates = maps.Clone(i.RequestTemplates)
	cp.RequestParameters = maps.Clone(i.RequestParameters)
	cp.CacheKeyParameters = slices.Clone(i.CacheKeyParameters)

	if i.IntegrationResponses != nil {
		cp.IntegrationResponses = make(map[string]*IntegrationResponse, len(i.IntegrationResponses))
		for status, resp := range i.IntegrationResponses {
			rc := *resp
			rc.ResponseTemplates = maps.Clone(resp.ResponseTemplates)
			rc.ResponseParameters = maps.Clone(resp.ResponseParameters)
			cp.IntegrationResponses[status] = &rc
		}
	}

	return cp
}

func deepCopyAuthorizer(a *Authorizer) *Authorizer {
	cp := *a
	cp.ProviderARNs = slices.Clone(a.ProviderARNs)

	return &cp
}

func deepCopyGatewayResponse(gr *GatewayResponse) *GatewayResponse {
	cp := *gr
	cp.ResponseParameters = maps.Clone(gr.ResponseParameters)
	cp.ResponseTemplates = maps.Clone(gr.ResponseTemplates)

	return &cp
}

// DeploymentConfig returns the deployment snapshot deploymentID captured, for
// the data-plane proxy to resolve a request against instead of live state.
func (b *InMemoryBackend) DeploymentConfig(restAPIID, deploymentID string) (*DeploymentConfig, error) {
	b.mu.RLock("DeploymentConfig")
	defer b.mu.RUnlock()

	depl, ok := b.deployments.Get(deploymentKey(restAPIID, deploymentID))
	if !ok {
		return nil, fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	if depl.Config == nil {
		return nil, fmt.Errorf("%w: deployment %s has no snapshot", ErrDeploymentNotFound, deploymentID)
	}

	return depl.Config, nil
}
