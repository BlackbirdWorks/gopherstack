package sagemakerruntime

import (
	"fmt"
	"strings"
	"time"
)

// RecordAsyncInvocation stores accepted asynchronous inference work.
// If outputLocation is empty a fake S3 location is synthesised, using
// filename as its final path segment when the caller supplied one --
// matching InvokeEndpointAsyncInput.Filename's documented behaviour
// ("If not specified, Amazon SageMaker AI generates a filename based on
// the inference ID").
func (b *InMemoryBackend) RecordAsyncInvocation(
	endpointName, requestedID, input, outputLocation, filename string,
) *AsyncInvocation {
	b.mu.Lock("RecordAsyncInvocation")
	defer b.mu.Unlock()

	inferenceID := requestedID
	if inferenceID == "" {
		b.nextID++
		inferenceID = fmt.Sprintf("gopherstack-inference-%d", b.nextID)
	}

	loc := outputLocation

	var failureLoc string
	if loc == "" {
		if filename == "" {
			filename = "output"
		}

		loc = fmt.Sprintf("s3://sagemaker-runtime-mock/%s/%s/%s", endpointName, inferenceID, filename)
		failureLoc = fmt.Sprintf("s3://sagemaker-runtime-mock/%s/%s/failure", endpointName, inferenceID)
	} else {
		failureLoc = deriveFailureLocation(loc)
	}

	invocation := &AsyncInvocation{
		InferenceID:     inferenceID,
		EndpointName:    endpointName,
		Input:           input,
		OutputLocation:  loc,
		FailureLocation: failureLoc,
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

// deriveFailureLocation synthesises the S3 URI where a failed async
// invocation's error payload would be written, following the AWS
// InvokeEndpointAsync convention of mirroring OutputLocation with a
// distinct suffix (real AWS derives both from the endpoint's
// AsyncInferenceConfig; without that cross-service config gopherstack
// mirrors OutputLocation deterministically instead).
func deriveFailureLocation(outputLocation string) string {
	switch {
	case strings.HasSuffix(outputLocation, "/output"):
		return strings.TrimSuffix(outputLocation, "output") + "failure"
	case strings.HasSuffix(outputLocation, "/"):
		return outputLocation + "failure"
	default:
		return outputLocation + "-failure"
	}
}

func cloneAsyncInvocation(invocation *AsyncInvocation) *AsyncInvocation {
	cp := *invocation

	return &cp
}
