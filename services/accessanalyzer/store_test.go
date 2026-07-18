package accessanalyzer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// newBackend constructs a fresh InMemoryBackend for tests across this package.
func newBackend(t *testing.T) *accessanalyzer.InMemoryBackend {
	t.Helper()

	return accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
}

func TestReset_ClearsState(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateAnalyzer("reset-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)

	b.Reset()

	analyzers, _ := b.ListAnalyzers("")
	assert.Empty(t, analyzers)
}
