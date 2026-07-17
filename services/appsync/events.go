package appsync

import (
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateAPI creates a new Event API.
func (b *InMemoryBackend) CreateAPI(
	name, ownerContact string, tagMap map[string]string, eventConfig *EventConfig,
) (*API, error) {
	b.mu.Lock("CreateAPI")
	defer b.mu.Unlock()

	apiID := randomAPIID()
	apiARN := arn.Build("appsync", b.region, b.accountID, "apis/"+apiID)

	httpEndpoint := fmt.Sprintf("%s.appsync-api.%s.amazonaws.com", apiID, b.region)
	realtimeEndpoint := fmt.Sprintf("%s.appsync-realtime-api.%s.amazonaws.com", apiID, b.region)

	api := &API{
		APIID:        apiID,
		ARN:          apiARN,
		Name:         name,
		Tags:         tagMap,
		OwnerContact: ownerContact,
		DNS: map[string]string{
			"HTTP":     httpEndpoint,
			"REALTIME": realtimeEndpoint,
		},
		EventConfig: eventConfig,
	}

	b.eventAPIs.Put(api)

	cp := *api

	return &cp, nil
}

// GetAPI returns an Event API by ID.
func (b *InMemoryBackend) GetAPI(apiID string) (*API, error) {
	b.mu.RLock("GetApi")
	defer b.mu.RUnlock()

	api, ok := b.eventAPIs.Get(apiID)
	if !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	cp := *api

	return &cp, nil
}

// ListAPIs returns all Event APIs.
func (b *InMemoryBackend) ListAPIs() ([]*API, error) {
	b.mu.RLock("ListApis")
	defer b.mu.RUnlock()

	apis := b.eventAPIs.All()
	out := make([]*API, 0, len(apis))

	for _, api := range apis {
		cp := *api
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *API) int {
		return strings.Compare(a.Name, b.Name)
	})

	return out, nil
}

// DeleteAPI deletes an Event API and all its channel namespaces.
func (b *InMemoryBackend) DeleteAPI(apiID string) error {
	b.mu.Lock("DeleteApi")
	defer b.mu.Unlock()

	if !b.eventAPIs.Has(apiID) {
		return fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	b.eventAPIs.Delete(apiID)

	for _, ns := range slices.Clone(b.channelNamespacesByAPI.Get(apiID)) {
		b.channelNamespaces.Delete(channelNamespaceKey(apiID, ns.Name))
	}

	return nil
}

// UpdateAPI updates an Event API's name, owner contact, or event config.
func (b *InMemoryBackend) UpdateAPI(apiID, name, ownerContact string, eventConfig *EventConfig) (*API, error) {
	b.mu.Lock("UpdateApi")
	defer b.mu.Unlock()

	api, ok := b.eventAPIs.Get(apiID)
	if !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if name != "" {
		api.Name = name
	}

	if ownerContact != "" {
		api.OwnerContact = ownerContact
	}

	if eventConfig != nil {
		api.EventConfig = eventConfig
	}

	cp := *api

	return &cp, nil
}
