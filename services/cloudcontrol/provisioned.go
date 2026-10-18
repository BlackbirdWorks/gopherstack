package cloudcontrol

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// Provisioner creates, changes and destroys the real resource behind a Cloud
// Control type. Errors that wrap ErrUnsupportedAction or ErrNotUpdatable keep
// their wire code; any other error surfaces as InvalidRequestException.
type Provisioner interface {
	Provision(ctx context.Context, typeName string, props map[string]any) (identifier string, err error)
	Update(ctx context.Context, typeName, identifier string, current, desired map[string]any) error
	Deprovision(ctx context.Context, typeName, identifier string, props map[string]any) error
}

const awsTypePrefix = "AWS::"

// RegisterProvisionedTypes serves every listed type through p unless a dedicated
// TypeHandler already owns it. The desired state is kept in the resource store.
func (b *InMemoryBackend) RegisterProvisionedTypes(p Provisioner, typeNames []string) {
	b.mu.Lock("RegisterProvisionedTypes")
	defer b.mu.Unlock()

	for _, name := range typeNames {
		if _, owned := b.typeHandlers[name]; !owned {
			b.typeHandlers[name] = &provisionedHandler{b: b, typeName: name, p: p}
		}
	}
}

// RejectUnprovisionedAWSTypes makes every AWS:: type without a handler answer
// UnsupportedActionException instead of falling back to the generic store.
func (b *InMemoryBackend) RejectUnprovisionedAWSTypes() {
	b.mu.Lock("RejectUnprovisionedAWSTypes")
	defer b.mu.Unlock()

	b.strictAWSTypes = true
}

func (b *InMemoryBackend) unsupportedType(typeName, action string) error {
	b.mu.RLock("unsupportedType")
	defer b.mu.RUnlock()

	if !b.strictAWSTypes || !strings.HasPrefix(typeName, awsTypePrefix) || !isValidTypeName(typeName) {
		return nil
	}

	return unsupportedActionError(typeName, action)
}

func unsupportedActionError(typeName, action string) error {
	return fmt.Errorf("%w: Resource type %s does not support %s action", ErrUnsupportedAction, typeName, action)
}

type provisionedHandler struct {
	b        *InMemoryBackend
	p        Provisioner
	typeName string
}

func (h *provisionedHandler) Create(ctx context.Context, desired map[string]any) (string, error) {
	id, err := h.p.Provision(ctx, h.typeName, desired)
	if err != nil {
		return "", err
	}

	raw, err := marshalModel(desired)
	if err != nil {
		return "", err
	}

	h.b.mu.Lock("provisionedCreate")
	defer h.b.mu.Unlock()

	if h.b.resources.Has(resourceKey(h.typeName, id)) {
		return "", fmt.Errorf("%w: Resource of type '%s' with identifier '%s' already exists",
			ErrAlreadyExists, h.typeName, id)
	}

	h.b.resources.Put(&Resource{TypeName: h.typeName, Identifier: id, Properties: raw})

	return id, nil
}

func (h *provisionedHandler) stored(identifier string) (map[string]any, error) {
	h.b.mu.RLock("provisionedRead")
	r, ok := h.b.resources.Get(resourceKey(h.typeName, identifier))
	h.b.mu.RUnlock()

	if !ok {
		return nil, notFoundError(h.typeName, identifier)
	}

	var model map[string]any
	if err := json.Unmarshal([]byte(r.Properties), &model); err != nil {
		return nil, fmt.Errorf("%w: stored model is not a JSON object", ErrValidation)
	}

	return model, nil
}

func (h *provisionedHandler) Read(_ context.Context, identifier string) (map[string]any, error) {
	return h.stored(identifier)
}

func (h *provisionedHandler) Update(ctx context.Context, identifier string, current, desired map[string]any) error {
	if err := h.p.Update(ctx, h.typeName, identifier, current, desired); err != nil {
		return err
	}

	raw, err := marshalModel(desired)
	if err != nil {
		return err
	}

	h.b.mu.Lock("provisionedUpdate")
	defer h.b.mu.Unlock()

	r, ok := h.b.resources.Get(resourceKey(h.typeName, identifier))
	if !ok {
		return notFoundError(h.typeName, identifier)
	}

	r.Properties = raw

	return nil
}

func (h *provisionedHandler) Delete(ctx context.Context, identifier string) error {
	props, err := h.stored(identifier)
	if err != nil {
		return err
	}

	if err = h.p.Deprovision(ctx, h.typeName, identifier, props); err != nil {
		return err
	}

	h.b.mu.Lock("provisionedDelete")
	defer h.b.mu.Unlock()

	h.b.resources.Delete(resourceKey(h.typeName, identifier))

	return nil
}

func (h *provisionedHandler) List(_ context.Context) ([]string, error) {
	h.b.mu.RLock("provisionedList")
	defer h.b.mu.RUnlock()

	var ids []string

	h.b.resources.Range(func(r *Resource) bool {
		if r.TypeName == h.typeName {
			ids = append(ids, r.Identifier)
		}

		return true
	})

	slices.Sort(ids)

	return ids, nil
}
