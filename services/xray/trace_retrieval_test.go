package xray_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

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

	status, _ := b.GetRetrievedTracesGraph("tok-1")
	assert.Equal(t, "RUNNING", status)

	// After cancel, status should be COMPLETE for the token.
	b.CancelTraceRetrieval("tok-1")
	status2, _ := b.GetRetrievedTracesGraph("tok-1")
	assert.Equal(t, "COMPLETE", status2)
}
