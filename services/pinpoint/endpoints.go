package pinpoint

// Endpoint represents a Pinpoint endpoint.
type Endpoint struct {
	Attributes     map[string][]string `json:"Attributes,omitempty"`
	UserAttributes map[string][]string `json:"UserAttributes,omitempty"`
	Metrics        map[string]float64  `json:"Metrics,omitempty"`
	Demographic    map[string]any      `json:"Demographic,omitempty"`
	Location       map[string]any      `json:"Location,omitempty"`
	ApplicationID  string              `json:"ApplicationId"`
	ID             string              `json:"Id"`
	ChannelType    string              `json:"ChannelType,omitempty"`
	Address        string              `json:"Address,omitempty"`
	UserID         string              `json:"UserId,omitempty"`
	EffectiveDate  string              `json:"EffectiveDate,omitempty"`
	CreationDate   string              `json:"CreationDate,omitempty"`
	EndpointStatus string              `json:"EndpointStatus,omitempty"`
	OptOut         string              `json:"OptOut,omitempty"`
	RequestID      string              `json:"RequestId,omitempty"`
}

// GetEndpoint retrieves a Pinpoint endpoint by appID and endpointID.
func (b *InMemoryBackend) GetEndpoint(appID, endpointID string) (*Endpoint, error) {
	b.mu.RLock("GetEndpoint")
	defer b.mu.RUnlock()

	key := appID + "/" + endpointID
	e, ok := b.endpoints.Get(key)

	if !ok {
		return nil, ErrAppNotFound
	}

	return cloneEndpoint(e), nil
}

// UpdateEndpoint creates or updates a Pinpoint endpoint.
func (b *InMemoryBackend) UpdateEndpoint(
	appID, endpointID string,
	req updateEndpointRequest,
) (*Endpoint, error) {
	if req.ChannelType != "" {
		if !isValidEndpointChannelType(req.ChannelType) {
			return nil, ErrValidation
		}
	}

	b.mu.Lock("UpdateEndpoint")
	defer b.mu.Unlock()

	key := appID + "/" + endpointID

	e, ok := b.endpoints.Get(key)
	if !ok {
		e = &Endpoint{
			ApplicationID: appID,
			ID:            endpointID,
			CreationDate:  nowRFC3339(),
		}
		b.endpoints.Put(e)
	}

	applyEndpointFields(e, req)

	return cloneEndpoint(e), nil
}

// DeleteEndpoint deletes a Pinpoint endpoint.
func (b *InMemoryBackend) DeleteEndpoint(appID, endpointID string) (*Endpoint, error) {
	b.mu.Lock("DeleteEndpoint")
	defer b.mu.Unlock()

	key := appID + "/" + endpointID

	e, ok := b.endpoints.Get(key)
	if !ok {
		return nil, ErrAppNotFound
	}

	b.endpoints.Delete(key)

	return cloneEndpoint(e), nil
}

// GetUserEndpoints retrieves all endpoints for a user in an application.
func (b *InMemoryBackend) GetUserEndpoints(appID, userID string) ([]*Endpoint, error) {
	b.mu.RLock("GetUserEndpoints")
	defer b.mu.RUnlock()

	var endpoints []*Endpoint

	for _, e := range b.endpoints.All() {
		if e.ApplicationID == appID && e.UserID == userID {
			endpoints = append(endpoints, cloneEndpoint(e))
		}
	}

	return endpoints, nil
}

// DeleteUserEndpoints deletes all endpoints for a user in an application.
func (b *InMemoryBackend) DeleteUserEndpoints(appID, userID string) error {
	b.mu.Lock("DeleteUserEndpoints")
	defer b.mu.Unlock()

	for _, e := range b.endpoints.All() {
		if e.ApplicationID == appID && e.UserID == userID {
			b.endpoints.Delete(e.ApplicationID + "/" + e.ID)
		}
	}

	return nil
}

// applyEndpointFields merges request fields into an Endpoint.
func applyEndpointFields(e *Endpoint, req updateEndpointRequest) {
	if req.ChannelType != "" {
		e.ChannelType = req.ChannelType
	}

	if req.Address != "" {
		e.Address = req.Address
	}

	if req.User.UserID != "" {
		e.UserID = req.User.UserID
	}

	if len(req.User.UserAttributes) > 0 {
		e.UserAttributes = nonNilStringSliceMapCopy(req.User.UserAttributes)
	}

	if len(req.Attributes) > 0 {
		e.Attributes = nonNilStringSliceMapCopy(req.Attributes)
	}

	if len(req.Metrics) > 0 {
		e.Metrics = nonNilFloat64MapCopy(req.Metrics)
	}

	if len(req.Demographic) > 0 {
		e.Demographic = cloneAnyMap(req.Demographic)
	}

	if len(req.Location) > 0 {
		e.Location = cloneAnyMap(req.Location)
	}

	if req.EndpointStatus != "" {
		e.EndpointStatus = req.EndpointStatus
	}

	if req.OptOut != "" {
		e.OptOut = req.OptOut
	}

	if req.EffectiveDate != "" {
		e.EffectiveDate = req.EffectiveDate
	}

	if req.RequestID != "" {
		e.RequestID = req.RequestID
	}
}

// UpdateEndpointsBatch updates multiple endpoints in a single call.
func (b *InMemoryBackend) UpdateEndpointsBatch(
	appID string,
	endpoints map[string]updateEndpointRequest,
) error {
	b.mu.Lock("UpdateEndpointsBatch")
	defer b.mu.Unlock()

	for endpointID, req := range endpoints {
		key := appID + "/" + endpointID

		e, ok := b.endpoints.Get(key)
		if !ok {
			e = &Endpoint{
				ApplicationID: appID,
				ID:            endpointID,
				CreationDate:  nowRFC3339(),
			}
			b.endpoints.Put(e)
		}

		applyEndpointFields(e, req)
	}

	return nil
}

// cloneEndpoint returns a deep copy of an Endpoint.
func cloneEndpoint(e *Endpoint) *Endpoint {
	cp := *e
	cp.Attributes = nonNilStringSliceMapCopy(e.Attributes)
	cp.UserAttributes = nonNilStringSliceMapCopy(e.UserAttributes)
	cp.Metrics = nonNilFloat64MapCopy(e.Metrics)
	cp.Demographic = cloneAnyMap(e.Demographic)
	cp.Location = cloneAnyMap(e.Location)

	return &cp
}
