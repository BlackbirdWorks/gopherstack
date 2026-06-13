package opsworks_test

// OpsWorks was not migrated to AWS SDK v2 — it was deprecated and removed from
// the SDK. There is no aws-sdk-go-v2/service/opsworks package to compare against.
// Instead, this file verifies internal consistency between GetSupportedOperations()
// and the handler dispatch table.

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// TestSDKCompleteness verifies that GetSupportedOperations() has no duplicates
// and matches the handler's internal dispatch table exactly. Because OpsWorks is
// not present in aws-sdk-go-v2, no SDK client comparison is performed.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := opsworks.NewInMemoryBackend("000000000000", "us-east-1")
	h := opsworks.NewHandler(backend)

	supportedOps := h.GetSupportedOperations()

	seen := make(map[string]bool, len(supportedOps))
	var dups []string

	for _, op := range supportedOps {
		if seen[op] {
			dups = append(dups, op)
		}
		seen[op] = true
	}

	assert.Empty(t, dups, "GetSupportedOperations() contains duplicate entries — remove the duplicates.")

	dispatchKeys := opsworks.HandlerDispatchKeys(h)
	supported := make([]string, len(supportedOps))
	copy(supported, supportedOps)
	sort.Strings(supported)

	assert.Equal(t, dispatchKeys, supported,
		"GetSupportedOperations() and the dispatch table must contain exactly the same operations.")
}
