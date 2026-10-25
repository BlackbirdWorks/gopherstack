package sagemakerruntime

import "time"

// RecordAsyncInvocation stores an accepted asynchronous inference request.
func (b *InMemoryBackend) RecordAsyncInvocation(
	endpointName, inferenceID, input, outputLocation, failureLocation string,
) *AsyncInvocation {
	b.mu.Lock("RecordAsyncInvocation")
	defer b.mu.Unlock()

	invocation := &AsyncInvocation{
		InferenceID:     inferenceID,
		EndpointName:    endpointName,
		Input:           input,
		OutputLocation:  outputLocation,
		FailureLocation: failureLocation,
		CreatedAt:       time.Now().UTC(),
	}
	b.asyncInvocations.Put(invocation)
	evictOldest(
		b.asyncInvocations, maxAsyncInvocations, asyncInvocationKeyFn,
		func(a *AsyncInvocation) time.Time { return a.CreatedAt },
	)

	return cloneAsyncInvocation(invocation)
}

// ListAsyncInvocations returns accepted asynchronous inference work.
func (b *InMemoryBackend) ListAsyncInvocations() []*AsyncInvocation {
	b.mu.RLock("ListAsyncInvocations")
	defer b.mu.RUnlock()

	all := b.asyncInvocations.All()
	out := make([]*AsyncInvocation, 0, len(all))

	for _, invocation := range all {
		out = append(out, cloneAsyncInvocation(invocation))
	}

	return out
}

func cloneAsyncInvocation(invocation *AsyncInvocation) *AsyncInvocation {
	cp := *invocation

	return &cp
}
