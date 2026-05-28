package acm

import (
	"encoding/json"
	"time"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Certs              map[string]*Certificate            `json:"certs"`
	IdempotencyMap     map[string]certIdempotencyEntry    `json:"idempotencyMap,omitempty"`
	AccountIdempotency map[string]accountIdempotencyEntry `json:"accountIdempotency,omitempty"`
	AccountID          string                             `json:"accountID"`
	Region             string                             `json:"region"`
	AccountConfig      AccountConfig                      `json:"accountConfig"`
}

type handlerSnapshot struct {
	Tags    map[string]map[string]string `json:"tags"`
	Backend backendSnapshot              `json:"backend"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
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
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Certs == nil {
		snap.Certs = make(map[string]*Certificate)
	}

	if snap.IdempotencyMap == nil {
		snap.IdempotencyMap = make(map[string]certIdempotencyEntry)
	}

	if snap.AccountIdempotency == nil {
		snap.AccountIdempotency = make(map[string]accountIdempotencyEntry)
	}

	// Preserve default if snapshot was taken before accountConfig was tracked.
	if snap.AccountConfig.DaysBeforeExpiry == 0 {
		snap.AccountConfig.DaysBeforeExpiry = defaultDaysBeforeExpiry
	}

	b.certs = snap.Certs
	b.idempotencyMap = snap.IdempotencyMap
	b.accountIdempotency = snap.AccountIdempotency
	b.accountConfig = snap.AccountConfig
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Restart timers for pending validations.
	for arn, cert := range b.certs {
		if cert.Status == statusPendingValidation {
			t := time.AfterFunc(autoValidateDelayMS*time.Millisecond, func(a string) func() {
				return func() { b.autoValidate(a) }
			}(arn))
			b.timers[arn] = t
		} else if cert.RenewalSummary != nil && cert.RenewalSummary.RenewalStatus == renewalStatusPendingValidation {
			t := time.AfterFunc(autoValidateDelayMS*time.Millisecond, func(a string) func() {
				return func() { b.autoValidateRenewal(a) }
			}(arn))
			b.timers[arn] = t
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend
// and also capturing the handler's tag state.
func (h *Handler) Snapshot() []byte {
	h.tagsMu.RLock("Snapshot")
	tagsMap := make(map[string]map[string]string, len(h.tags))
	for k, t := range h.tags {
		tagsMap[k] = t.Clone()
	}
	h.tagsMu.RUnlock()

	snap := handlerSnapshot{
		Tags: tagsMap,
	}
	if err := json.Unmarshal(h.Backend.Snapshot(), &snap.Backend); err != nil {
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
func (h *Handler) Restore(data []byte) error {
	var snap handlerSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		// Fall back to backend-only snapshot for backward compatibility.
		return h.Backend.Restore(data)
	}

	backendData, err := json.Marshal(snap.Backend)
	if err != nil {
		return err
	}

	if restoreErr := h.Backend.Restore(backendData); restoreErr != nil {
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
