package fis

import "fmt"

// ----------------------------------------
// Phase 3 — Safety Lever
// ----------------------------------------

// resolveSafetyLeverID maps the "default" alias to the backend's account ID.
func (b *InMemoryBackend) resolveSafetyLeverID(id string) string {
	if id == "default" {
		return b.accountID
	}

	return id
}

// GetSafetyLever returns the current state of the account's safety lever.
// Accepts either the account ID or the conventional "default" alias.
func (b *InMemoryBackend) GetSafetyLever(id string) (*SafetyLever, error) {
	b.mu.RLock("GetSafetyLever")
	defer b.mu.RUnlock()

	if b.safetyLever == nil {
		return nil, fmt.Errorf("%w: %s", ErrSafetyLeverNotFound, id)
	}

	cp := *b.safetyLever

	return &cp, nil
}

// UpdateSafetyLeverState updates the state of the account's safety lever.
// Accepts either the account ID or the conventional "default" alias.
// Setting status to "engaged" blocks new experiments from starting.
func (b *InMemoryBackend) UpdateSafetyLeverState(
	id string,
	input *updateSafetyLeverStateRequest,
) (*SafetyLever, error) {
	status := input.State.Status
	if status != statusDisengaged && status != "engaged" {
		return nil, fmt.Errorf(
			"%w: safetyLever status must be \"engaged\" or \"disengaged\"; got %q",
			ErrValidation, status,
		)
	}

	b.mu.Lock("UpdateSafetyLeverState")
	defer b.mu.Unlock()

	resolved := b.resolveSafetyLeverID(id)

	if b.safetyLever == nil || b.safetyLever.ID != resolved {
		return nil, fmt.Errorf("%w: %s", ErrSafetyLeverNotFound, id)
	}

	b.safetyLever.State = SafetyLeverState{
		Status: status,
		Reason: input.State.Reason,
	}

	cp := *b.safetyLever

	return &cp, nil
}
