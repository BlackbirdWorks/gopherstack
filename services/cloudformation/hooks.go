package cloudformation

import "fmt"

func (b *InMemoryBackend) RecordHandlerProgress(bearerToken, operationStatus string) error {
	b.mu.Lock("RecordHandlerProgress")
	defer b.mu.Unlock()
	b.handlerProgress[bearerToken] = operationStatus

	return nil
}

func (b *InMemoryBackend) GetHookResult(hookResultID string) (string, error) {
	b.mu.RLock("GetHookResult")
	defer b.mu.RUnlock()
	r, ok := b.hookResults.Get(hookResultID)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrHookResultNotFound, hookResultID)
	}

	return r.HookStatus, nil
}

func (b *InMemoryBackend) ListHookResults(hookResultToken, _ string) ([]HookResult, error) {
	b.mu.RLock("ListHookResults")
	defer b.mu.RUnlock()
	var results []HookResult
	if hookResultToken != "" {
		if r, ok := b.hookResults.Get(hookResultToken); ok {
			results = append(results, *r)
		}
	} else {
		for _, r := range b.hookResults.All() {
			results = append(results, *r)
		}
	}

	return results, nil
}

func (b *InMemoryBackend) DescribeChangeSetHooks(_, _ string) ([]ChangeSetHook, error) {
	// Hook configurations are not emulated; return empty list (valid AWS response
	// when no hook configurations are active for the change set).
	return []ChangeSetHook{}, nil
}
