package sagemakerruntime

import "time"

// maxInvocationHistory is the maximum number of invocations retained in memory.
const maxInvocationHistory = 1000

// MaxInvocationHistory is the exported value for testing.
const MaxInvocationHistory = maxInvocationHistory

// maxSessions bounds the number of stateful endpoint sessions retained in memory.
// InvokeEndpoint with a NEW session header is a per-request hot path; without a
// cap the sessions map would grow without bound in a long-running emulator. When
// the cap is exceeded the oldest session (by CreatedAt) is evicted FIFO-style.
const maxSessions = 1000

// MaxSessions is the exported value for testing.
const MaxSessions = maxSessions

// maxAsyncInvocations bounds the number of accepted async inference records
// retained in memory. RecordAsyncInvocation runs on every InvokeEndpointAsync
// request; without a cap the map would grow without bound. When the cap is
// exceeded the oldest record (by CreatedAt) is evicted FIFO-style.
const maxAsyncInvocations = 1000

// MaxAsyncInvocations is the exported value for testing.
const MaxAsyncInvocations = maxAsyncInvocations

const sessionDuration = 5 * time.Minute

// Invocation records a single SageMaker Runtime endpoint invocation.
type Invocation struct {
	CreatedAt    time.Time `json:"createdAt"`
	EndpointName string    `json:"endpointName"`
	Operation    string    `json:"operation"`
	Input        string    `json:"input"`
	Output       string    `json:"output"`
}

// Session records a stateful endpoint session established by InvokeEndpoint.
type Session struct {
	CreatedAt     time.Time `json:"createdAt"`
	LastInvokedAt time.Time `json:"lastInvokedAt"`
	ExpiresAt     time.Time `json:"expiresAt"`
	ID            string    `json:"id"`
	EndpointName  string    `json:"endpointName"`
}

// AsyncInvocation records accepted asynchronous inference work.
type AsyncInvocation struct {
	CreatedAt       time.Time `json:"createdAt"`
	InferenceID     string    `json:"inferenceId"`
	EndpointName    string    `json:"endpointName"`
	Input           string    `json:"input"`
	OutputLocation  string    `json:"outputLocation"`
	FailureLocation string    `json:"failureLocation"`
}
