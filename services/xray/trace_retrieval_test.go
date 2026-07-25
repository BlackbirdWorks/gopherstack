package xray_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

// TestAddTraceRetrievalInternal verifies the seed helper and retrieval status.
func TestAddTraceRetrievalInternal(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddTraceRetrievalInternal(xray.TraceRetrieval{
		RetrievalToken: "tok-1",
		Status:         "RUNNING",
		StartTime:      time.Now(),
	})

	status, _, err := b.GetRetrievedTracesGraph("tok-1")
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", status)

	// After cancel, the token no longer resolves: real AWS returns
	// ResourceNotFoundException (CancelTraceRetrieval/GetRetrievedTracesGraph both
	// declare it in their modeled error set) rather than a synthetic COMPLETE status.
	require.NoError(t, b.CancelTraceRetrieval("tok-1"))
	_, _, err = b.GetRetrievedTracesGraph("tok-1")
	require.Error(t, err)
	assert.ErrorIs(t, err, xray.ErrTraceRetrievalNotFound)
}

// TestCancelTraceRetrieval_UnknownTokenReturnsNotFound verifies CancelTraceRetrieval
// itself declares ResourceNotFoundException for a token that was never created by
// StartTraceRetrieval (not a silent idempotent no-op).
func TestCancelTraceRetrieval_UnknownTokenReturnsNotFound(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	err := b.CancelTraceRetrieval("no-such-token")
	require.Error(t, err)
	assert.ErrorIs(t, err, xray.ErrTraceRetrievalNotFound)
}

// TestListRetrievedTraces_UnknownTokenReturnsNotFound verifies ListRetrievedTraces
// declares ResourceNotFoundException for an unknown token.
func TestListRetrievedTraces_UnknownTokenReturnsNotFound(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	_, _, err := b.ListRetrievedTraces("no-such-token")
	require.Error(t, err)
	assert.ErrorIs(t, err, xray.ErrTraceRetrievalNotFound)
}
