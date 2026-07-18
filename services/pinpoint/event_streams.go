package pinpoint

// EventStream represents a Pinpoint event stream.
type EventStream struct {
	ApplicationID        string `json:"ApplicationId"`
	DestinationStreamArn string `json:"DestinationStreamArn"`
	RoleArn              string `json:"RoleArn"`
	LastModifiedDate     string `json:"LastModifiedDate,omitempty"`
}

// GetEventStream retrieves the event stream for an application.
func (b *InMemoryBackend) GetEventStream(appID string) (*EventStream, error) {
	b.mu.RLock("GetEventStream")
	defer b.mu.RUnlock()

	e, ok := b.eventStreams.Get(appID)
	if !ok {
		return nil, ErrAppNotFound
	}

	cp := *e

	return &cp, nil
}

// PutEventStream creates or updates the event stream for an application.
func (b *InMemoryBackend) PutEventStream(
	appID string,
	req putEventStreamRequest,
) (*EventStream, error) {
	b.mu.Lock("PutEventStream")
	defer b.mu.Unlock()

	e := &EventStream{
		ApplicationID:        appID,
		DestinationStreamArn: req.DestinationStreamArn,
		RoleArn:              req.RoleArn,
		LastModifiedDate:     nowRFC3339(),
	}

	b.eventStreams.Put(e)

	cp := *e

	return &cp, nil
}

// DeleteEventStream deletes the event stream for an application.
func (b *InMemoryBackend) DeleteEventStream(appID string) (*EventStream, error) {
	b.mu.Lock("DeleteEventStream")
	defer b.mu.Unlock()

	e, ok := b.eventStreams.Get(appID)
	if !ok {
		return nil, ErrAppNotFound
	}

	b.eventStreams.Delete(appID)

	cp := *e

	return &cp, nil
}
