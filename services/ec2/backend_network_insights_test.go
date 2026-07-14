package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParityFinal_EnableReachabilityAnalyzerOrganizationSharing(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	assert.True(t, b.EnableReachabilityAnalyzerOrganizationSharing())
}
