package apigateway

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

// CreateRestAPI creates a new REST API and its root resource.
func (b *InMemoryBackend) CreateRestAPI(input CreateRestAPIInput) (*RestAPI, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	input, err := normalizeRestAPIInput(input)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateRestAPI")
	defer b.mu.Unlock()

	if input.CloneFrom != "" && !b.restApis.Has(input.CloneFrom) {
		return nil, fmt.Errorf("%w: Invalid API identifier specified: %s", ErrRestAPINotFound, input.CloneFrom)
	}

	id := randomID(apiIDLength)
	backendTags := initTagsFromInput("apigw.api."+id+".tags", input.Tags)
	rootID := randomID(resourceIDLength)

	api := &RestAPI{
		ID:                     id,
		Name:                   input.Name,
		Description:            input.Description,
		CreatedDate:            unixEpochTime{time.Now()},
		Tags:                   backendTags,
		RootResourceID:         rootID,
		BinaryMediaTypes:       input.BinaryMediaTypes,
		EndpointConfiguration:  input.EndpointConfiguration,
		Policy:                 input.Policy,
		APIKeySource:           input.APIKeySource,
		MinimumCompressionSize: input.MinimumCompressionSize,
		// APIStatus is AWS-managed and always AVAILABLE: gopherstack creates
		// RestApis synchronously with no UPDATING/PENDING/FAILED transition.
		APIStatus:                 statusAvailable,
		DisableExecuteAPIEndpoint: input.DisableExecuteAPIEndpoint,
		EndpointAccessMode:        input.EndpointAccessMode,
		SecurityPolicy:            input.SecurityPolicy,
		Version:                   input.Version,
	}

	root := &Resource{
		ID:              rootID,
		ParentID:        "",
		PathPart:        "",
		Path:            "/",
		RestAPIID:       id,
		ResourceMethods: make(map[string]*Method),
	}

	b.restApis.Put(api)
	b.resources.Put(root)

	if input.CloneFrom != "" {
		b.cloneAPIContentsLocked(input.CloneFrom, id, rootID)
	}

	cp := *api

	return &cp, nil
}

// DeleteRestAPI removes a REST API and all its resources.
func (b *InMemoryBackend) DeleteRestAPI(restAPIID string) error {
	b.mu.Lock("DeleteRestAPI")
	defer b.mu.Unlock()

	api, ok := b.restApis.Get(restAPIID)
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	api.Tags.Close()
	b.restApis.Delete(restAPIID)
	b.deleteAPIChildrenLocked(restAPIID)

	return nil
}

// deleteAPIChildrenLocked removes every resource-family entry scoped to
// restAPIID (resources, deployments, stages, authorizers, requestValidators,
// documentationParts, documentationVersions, models, gatewayResponses) via
// each table's "byAPI" index (gatewayResponses has none, so it's scanned
// directly). Callers must hold b.mu.
func (b *InMemoryBackend) deleteAPIChildrenLocked(restAPIID string) {
	for _, r := range append([]*Resource{}, b.resourcesByAPI.Get(restAPIID)...) {
		b.resources.Delete(resourceKeyFn(r))
	}
	for _, d := range append([]*Deployment{}, b.deploymentsByAPI.Get(restAPIID)...) {
		b.deployments.Delete(deploymentKeyFn(d))
	}
	for _, s := range append([]*Stage{}, b.stagesByAPI.Get(restAPIID)...) {
		b.stages.Delete(stageKeyFn(s))
		b.clearStageThrottleBuckets(restAPIID, s.StageName)
	}
	for _, a := range append([]*Authorizer{}, b.authorizersByAPI.Get(restAPIID)...) {
		b.authorizers.Delete(authorizerKeyFn(a))
	}
	for _, v := range append([]*RequestValidator{}, b.requestValidatorsByAPI.Get(restAPIID)...) {
		b.requestValidators.Delete(requestValidatorKeyFn(v))
	}
	for _, p := range append([]*DocumentationPart{}, b.documentationPartsByAPI.Get(restAPIID)...) {
		b.documentationParts.Delete(documentationPartKeyFn(p))
	}
	for _, v := range append([]*DocumentationVersion{}, b.documentationVersionsByAPI.Get(restAPIID)...) {
		b.documentationVersions.Delete(documentationVersionKeyFn(v))
	}
	for _, m := range append([]*Model{}, b.modelsByAPI.Get(restAPIID)...) {
		b.models.Delete(modelKeyFn(m))
	}
	b.deleteGatewayResponsesForAPILocked(restAPIID)
	delete(b.resourceVersions, restAPIID)
}

// GetRestAPI returns a single REST API.
func (b *InMemoryBackend) GetRestAPI(restAPIID string) (*RestAPI, error) {
	b.mu.RLock("GetRestAPI")
	defer b.mu.RUnlock()

	api, ok := b.restApis.Get(restAPIID)
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	cp := *api

	return &cp, nil
}

// GetRestAPIs returns all REST APIs with pagination.
func (b *InMemoryBackend) GetRestAPIs(limit int, position string) ([]RestAPI, string, error) {
	b.mu.RLock("GetRestAPIs")
	defer b.mu.RUnlock()

	all := make([]RestAPI, 0, b.restApis.Len())
	for _, api := range b.restApis.All() {
		all = append(all, *api)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	page, pos := paginatePageByKey(all, limit, position, func(a RestAPI) string { return a.ID })

	return page, pos, nil
}

// UpdateRestAPI updates the name and/or description of a REST API.
func (b *InMemoryBackend) UpdateRestAPI(restAPIID string, input UpdateRestAPIInput) (*RestAPI, error) {
	b.mu.Lock("UpdateRestAPI")
	defer b.mu.Unlock()

	api, ok := b.restApis.Get(restAPIID)
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	if input.Name != "" {
		api.Name = input.Name
	}

	// Description is a *string (see UpdateRestAPIInput's doc comment): a
	// non-nil pointer means the PATCH touched this field at all, including an
	// explicit "remove" (which patch.go encodes as a pointer to "").
	if input.Description != nil {
		api.Description = *input.Description
	}

	if input.Policy != "" {
		api.Policy = input.Policy
	}

	if input.APIKeySource != "" {
		api.APIKeySource = input.APIKeySource
	}

	if input.EndpointAccessMode != "" {
		api.EndpointAccessMode = input.EndpointAccessMode
	}

	if input.SecurityPolicy != "" {
		api.SecurityPolicy = input.SecurityPolicy
	}

	if input.DisableExecuteAPIEndpoint != nil {
		api.DisableExecuteAPIEndpoint = *input.DisableExecuteAPIEndpoint
	}

	if input.BinaryMediaTypes != nil {
		api.BinaryMediaTypes = input.BinaryMediaTypes
	}

	if input.EndpointConfiguration != nil {
		api.EndpointConfiguration = input.EndpointConfiguration
	}

	if input.MinimumCompressionSize != nil {
		api.MinimumCompressionSize = *input.MinimumCompressionSize
	}

	cp := *api

	return &cp, nil
}

// cloneAPIContentsLocked copies srcID's resources and methods, models, authorizers,
// request validators and gateway responses into dstID (CreateRestApi CloneFrom).
// Deployments, stages, keys and documentation are not cloned. Callers must hold b.mu.
func (b *InMemoryBackend) cloneAPIContentsLocked(srcID, dstID, dstRootID string) {
	authorizerIDs := make(map[string]string)

	for _, a := range b.authorizersByAPI.Get(srcID) {
		cp := deepCopyAuthorizer(a)
		cp.ID = randomID(resourceIDLength)
		cp.RestAPIID = dstID
		authorizerIDs[a.ID] = cp.ID
		b.authorizers.Put(cp)
	}

	validatorIDs := make(map[string]string)

	for _, v := range b.requestValidatorsByAPI.Get(srcID) {
		cp := *v
		cp.ID = randomID(resourceIDLength)
		cp.RestAPIID = dstID
		validatorIDs[v.ID] = cp.ID
		b.requestValidators.Put(&cp)
	}

	for _, m := range b.modelsByAPI.Get(srcID) {
		cp := *m
		cp.ID = randomID(resourceIDLength)
		cp.RestAPIID = dstID
		b.models.Put(&cp)
	}

	for _, rt := range gatewayResponseTypes {
		if gr, ok := b.gatewayResponses.Get(gatewayResponseKey(srcID, rt)); ok {
			cp := deepCopyGatewayResponse(gr)
			cp.RestAPIID = dstID
			b.gatewayResponses.Put(cp)
		}
	}

	b.cloneResourcesLocked(srcID, dstID, dstRootID, authorizerIDs, validatorIDs)
	b.resourceVersions[dstID]++
}

func (b *InMemoryBackend) cloneResourcesLocked(
	srcID, dstID, dstRootID string, authorizerIDs, validatorIDs map[string]string,
) {
	src := slices.Clone(b.resourcesByAPI.Get(srcID))
	sort.Slice(src, func(i, j int) bool {
		di, dj := strings.Count(src[i].Path, "/"), strings.Count(src[j].Path, "/")
		if di != dj {
			return di < dj
		}

		return src[i].Path < src[j].Path
	})

	ids := make(map[string]string, len(src))

	for _, r := range src {
		if r.ParentID == "" {
			ids[r.ID] = dstRootID
			b.copyMethodsInto(dstID, dstRootID, r, authorizerIDs, validatorIDs)

			continue
		}

		cp := deepCopyResource(r)
		cp.ID = randomID(resourceIDLength)
		cp.RestAPIID = dstID
		cp.ParentID = ids[r.ParentID]
		ids[r.ID] = cp.ID

		for _, m := range cp.ResourceMethods {
			m.AuthorizerID = authorizerIDs[m.AuthorizerID]
			m.RequestValidatorID = validatorIDs[m.RequestValidatorID]
		}

		b.resources.Put(&cp)
	}
}

// copyMethodsInto copies src's methods onto the already-created destination root resource.
func (b *InMemoryBackend) copyMethodsInto(
	dstID, dstRootID string, src *Resource, authorizerIDs, validatorIDs map[string]string,
) {
	root, ok := b.resources.Get(resourceKey(dstID, dstRootID))
	if !ok {
		return
	}

	cp := deepCopyResource(src)

	for httpMethod, m := range cp.ResourceMethods {
		m.AuthorizerID = authorizerIDs[m.AuthorizerID]
		m.RequestValidatorID = validatorIDs[m.RequestValidatorID]
		root.ResourceMethods[httpMethod] = m
	}

	root.CorsConfiguration = cp.CorsConfiguration
}

// normalizeRestAPIInput validates endpoint types and API key source and fills AWS's defaults (EDGE, HEADER).
func normalizeRestAPIInput(input CreateRestAPIInput) (CreateRestAPIInput, error) {
	switch input.APIKeySource {
	case "":
		input.APIKeySource = "HEADER"
	case "HEADER", "AUTHORIZER":
	default:
		return input, fmt.Errorf("%w: Invalid API Key Source specified: %s", ErrInvalidParameter, input.APIKeySource)
	}

	if input.EndpointConfiguration == nil {
		input.EndpointConfiguration = &EndpointConfiguration{}
	}

	cfg := *input.EndpointConfiguration

	for _, t := range cfg.Types {
		if t != "EDGE" && t != "REGIONAL" && t != "PRIVATE" {
			return input, fmt.Errorf(
				"%w: Endpoint type %s is not valid; use EDGE, REGIONAL or PRIVATE",
				ErrInvalidParameter,
				t,
			)
		}
	}

	if len(cfg.Types) == 0 {
		cfg.Types = []string{"EDGE"}
	}

	input.EndpointConfiguration = &cfg

	return input, nil
}
