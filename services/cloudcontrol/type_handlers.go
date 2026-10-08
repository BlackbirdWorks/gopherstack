package cloudcontrol

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// TypeHandler serves one resource type from the service backend that owns it. Errors it
// returns should wrap ErrNotFound, ErrAlreadyExists or ErrValidation where they apply.
type TypeHandler interface {
	Create(ctx context.Context, desired map[string]any) (string, error)
	Read(ctx context.Context, identifier string) (map[string]any, error)
	Update(ctx context.Context, identifier string, current, desired map[string]any) error
	Delete(ctx context.Context, identifier string) error
	List(ctx context.Context) ([]string, error)
}

// RegisterTypeHandler routes typeName to h ahead of the generic resource store.
func (b *InMemoryBackend) RegisterTypeHandler(typeName string, h TypeHandler) {
	b.mu.Lock("RegisterTypeHandler")
	defer b.mu.Unlock()

	b.typeHandlers[typeName] = h
}

func (b *InMemoryBackend) typeHandler(typeName string) TypeHandler {
	b.mu.RLock("typeHandler")
	defer b.mu.RUnlock()

	return b.typeHandlers[typeName]
}

func marshalModel(model map[string]any) (string, error) {
	raw, err := json.Marshal(model)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrValidation, err)
	}

	return string(raw), nil
}

func (b *InMemoryBackend) recordDelegatedEvent(
	op, typeName, identifier, model, clientToken, fingerprint string,
) *ProgressEvent {
	b.mu.Lock("recordDelegatedEvent")
	defer b.mu.Unlock()

	token := uuid.NewString()
	event := &ProgressEvent{
		EventTime:       unixEpochTime{time.Now()},
		TypeName:        typeName,
		Identifier:      identifier,
		RequestToken:    token,
		Operation:       op,
		OperationStatus: opStatusSuccess,
		ResourceModel:   model,
	}
	b.requests.Put(event)
	b.rememberClientToken(clientToken, token, fingerprint)

	return copyEvent(event)
}

func (b *InMemoryBackend) replayedEvent(clientToken, fingerprint string) (*ProgressEvent, bool, error) {
	b.mu.RLock("replayedEvent")
	defer b.mu.RUnlock()

	return b.cachedEventForToken(clientToken, fingerprint)
}

// CreateResourceContext is CreateResource routed through a registered TypeHandler when one exists.
func (b *InMemoryBackend) CreateResourceContext(
	ctx context.Context, typeName, desiredState, clientToken string,
) (*ProgressEvent, error) {
	h := b.typeHandler(typeName)
	if h == nil {
		return b.CreateResource(typeName, desiredState, clientToken)
	}

	var desired map[string]any
	if err := json.Unmarshal([]byte(desiredState), &desired); err != nil {
		return nil, fmt.Errorf("%w: DesiredState must be a JSON object", ErrValidation)
	}

	fingerprint := clientTokenFingerprint("CREATE", typeName, "", desiredState)

	if cached, found, err := b.replayedEvent(clientToken, fingerprint); err != nil || found {
		return cached, err
	}

	id, err := h.Create(ctx, desired)
	if err != nil {
		return nil, err
	}

	model, err := b.readModel(ctx, h, id)
	if err != nil {
		return nil, err
	}

	return b.recordDelegatedEvent("CREATE", typeName, id, model, clientToken, fingerprint), nil
}

func (b *InMemoryBackend) readModel(ctx context.Context, h TypeHandler, identifier string) (string, error) {
	model, err := h.Read(ctx, identifier)
	if err != nil {
		return "", err
	}

	return marshalModel(model)
}

// GetResourceContext is GetResource routed through a registered TypeHandler when one exists.
func (b *InMemoryBackend) GetResourceContext(ctx context.Context, typeName, identifier string) (*Resource, error) {
	h := b.typeHandler(typeName)
	if h == nil {
		return b.GetResource(typeName, identifier)
	}

	model, err := b.readModel(ctx, h, identifier)
	if err != nil {
		return nil, err
	}

	return &Resource{TypeName: typeName, Identifier: identifier, Properties: model}, nil
}

// DeleteResourceContext is DeleteResource routed through a registered TypeHandler when one exists.
func (b *InMemoryBackend) DeleteResourceContext(
	ctx context.Context, typeName, identifier, clientToken string,
) (*ProgressEvent, error) {
	h := b.typeHandler(typeName)
	if h == nil {
		return b.DeleteResource(typeName, identifier, clientToken)
	}

	fingerprint := clientTokenFingerprint("DELETE", typeName, identifier, "")

	if cached, found, err := b.replayedEvent(clientToken, fingerprint); err != nil || found {
		return cached, err
	}

	if err := h.Delete(ctx, identifier); err != nil {
		return nil, err
	}

	return b.recordDelegatedEvent("DELETE", typeName, identifier, "", clientToken, fingerprint), nil
}

// UpdateResourceContext is UpdateResource routed through a registered TypeHandler when one exists.
func (b *InMemoryBackend) UpdateResourceContext(
	ctx context.Context, typeName, identifier, patchDocument, clientToken string,
) (*ProgressEvent, error) {
	h := b.typeHandler(typeName)
	if h == nil {
		return b.UpdateResource(typeName, identifier, patchDocument, clientToken)
	}

	fingerprint := clientTokenFingerprint("UPDATE", typeName, identifier, patchDocument)

	if cached, found, err := b.replayedEvent(clientToken, fingerprint); err != nil || found {
		return cached, err
	}

	current, err := h.Read(ctx, identifier)
	if err != nil {
		return nil, err
	}

	currentJSON, err := marshalModel(current)
	if err != nil {
		return nil, err
	}

	patched, err := applyPatch(currentJSON, patchDocument)
	if err != nil {
		return nil, err
	}

	var desired map[string]any
	if jsonErr := json.Unmarshal([]byte(patched), &desired); jsonErr != nil {
		return nil, fmt.Errorf("%w: patched document is not a JSON object", ErrValidation)
	}

	if updErr := h.Update(ctx, identifier, current, desired); updErr != nil {
		return nil, updErr
	}

	model, err := b.readModel(ctx, h, identifier)
	if err != nil {
		return nil, err
	}

	return b.recordDelegatedEvent("UPDATE", typeName, identifier, model, clientToken, fingerprint), nil
}

// ListResourcesContext is ListResources routed through a registered TypeHandler when one exists.
func (b *InMemoryBackend) ListResourcesContext(
	ctx context.Context, typeName string, maxResults int, nextToken, resourceModel string,
) ([]*Resource, string, error) {
	h := b.typeHandler(typeName)
	if h == nil {
		out, token := b.ListResources(typeName, maxResults, nextToken, resourceModel)

		return out, token, nil
	}

	modelFilter, validFilter := parseModelFilter(resourceModel)
	if !validFilter {
		return []*Resource{}, "", nil
	}

	ids, err := h.List(ctx)
	if err != nil {
		return nil, "", err
	}

	sort.Strings(ids)

	all := make([]*Resource, 0, len(ids))

	for _, id := range ids {
		model, readErr := b.readModel(ctx, h, id)
		if readErr != nil {
			continue
		}

		if matchesResourceModel(model, modelFilter) {
			all = append(all, &Resource{TypeName: typeName, Identifier: id, Properties: model})
		}
	}

	pg := page.New(all, nextToken, maxResults, defaultListMaxResults)

	return pg.Data, pg.Next, nil
}

func parseModelFilter(resourceModel string) (map[string]any, bool) {
	if resourceModel == "" {
		return nil, true
	}

	var filter map[string]any

	return filter, json.Unmarshal([]byte(resourceModel), &filter) == nil
}
