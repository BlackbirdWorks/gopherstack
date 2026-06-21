package acm

import (
	"context"
	"encoding/json"
	"time"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// backendSnapshot mirrors the region-nested backend maps (outer key = region).
type backendSnapshot struct {
	Certs              map[string]map[string]*Certificate            `json:"certs"`
	IdempotencyMap     map[string]map[string]certIdempotencyEntry    `json:"idempotencyMap,omitempty"`
	AccountIdempotency map[string]map[string]accountIdempotencyEntry `json:"accountIdempotency,omitempty"`
	AccountConfig      map[string]AccountConfig                      `json:"accountConfig"`
	AccountID          string                                        `json:"accountID"`
	Region             string                                        `json:"region"`
}

type handlerSnapshot struct {
	Tags    map[string]map[string]string `json:"tags"`
	Backend backendSnapshot              `json:"backend"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Certs:              b.certs,
		IdempotencyMap:     b.idempotencyMap,
		AccountIdempotency: b.accountIdempotency,
		AccountConfig:      b.accountConfig,
		AccountID:          b.accountID,
		Region:             b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Certs == nil {
		snap.Certs = make(map[string]map[string]*Certificate)
	}

	if snap.IdempotencyMap == nil {
		snap.IdempotencyMap = make(map[string]map[string]certIdempotencyEntry)
	}

	if snap.AccountIdempotency == nil {
		snap.AccountIdempotency = make(map[string]map[string]accountIdempotencyEntry)
	}

	if snap.AccountConfig == nil {
		snap.AccountConfig = make(map[string]AccountConfig)
	}

	b.certs = snap.Certs
	b.idempotencyMap = snap.IdempotencyMap
	b.accountIdempotency = snap.AccountIdempotency
	b.accountConfig = snap.AccountConfig
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.timers = make(map[string]map[string]*time.Timer)

	// Restart timers for pending validations, per region.
	for region, regionCerts := range b.certs {
		for arn, cert := range regionCerts {
			switch {
			case cert.Status == statusPendingValidation:
				t := time.AfterFunc(autoValidateDelayMS*time.Millisecond, func(r, a string) func() {
					return func() { b.autoValidate(r, a) }
				}(region, arn))
				b.timersStore(region)[arn] = t
			case cert.RenewalSummary != nil &&
				cert.RenewalSummary.RenewalStatus == renewalStatusPendingValidation:
				t := time.AfterFunc(autoValidateDelayMS*time.Millisecond, func(r, a string) func() {
					return func() { b.autoValidateRenewal(r, a) }
				}(region, arn))
				b.timersStore(region)[arn] = t
			}
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend
// and also capturing the handler's tag state.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	h.tagsMu.RLock("Snapshot")
	tagsMap := make(map[string]map[string]string, len(h.tags))
	for k, t := range h.tags {
		tagsMap[k] = t.Clone()
	}
	h.tagsMu.RUnlock()

	snap := handlerSnapshot{
		Tags: tagsMap,
	}
	if err := json.Unmarshal(h.Backend.Snapshot(ctx), &snap.Backend); err != nil {
		return nil
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore implements persistence.Persistable by delegating to the backend
// and restoring the handler's tag state.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	var snap handlerSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		// Fall back to backend-only snapshot for backward compatibility.
		return h.Backend.Restore(ctx, data)
	}

	backendData, err := json.Marshal(snap.Backend)
	if err != nil {
		return err
	}

	if restoreErr := h.Backend.Restore(ctx, backendData); restoreErr != nil {
		return restoreErr
	}

	h.tagsMu.Lock("Restore")
	defer h.tagsMu.Unlock()

	for _, t := range h.tags {
		t.Close()
	}

	h.tags = make(map[string]*svcTags.Tags, len(snap.Tags))
	for k, m := range snap.Tags {
		h.tags[k] = svcTags.FromMap("acm."+k+".tags", m)
	}

	return nil
}
