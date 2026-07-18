package shield_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestAudit_Gap6_ListAttacksMultipleARNs verifies multiple ARN filtering.
func TestInMemoryBackend_ListAttacksMultipleARNs(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddAttackInternal("atk-1", eipARN("1"))
	b.AddAttackInternal("atk-2", eipARN("2"))
	b.AddAttackInternal("atk-3", eipARN("3"))

	attacks := b.ListAttacks([]string{eipARN("1"), eipARN("2")}, 0, 0)
	assert.Len(t, attacks, 2)

	ids := make(map[string]bool)
	for _, a := range attacks {
		ids[a.AttackID] = true
	}

	assert.True(t, ids["atk-1"])
	assert.True(t, ids["atk-2"])
	assert.False(t, ids["atk-3"])
}

// TestAudit_Gap25_SimulateAttackCreatesAttack verifies SimulateAttack backend method.
func TestInMemoryBackend_SimulateAttackCreatesAttack(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	_, err := b.CreateProtection("prot", eipARN("1"), nil)
	require.NoError(t, err)

	atk, err := b.SimulateAttack(eipARN("1"), []string{"SYN_FLOOD", "UDP_TRAFFIC"})
	require.NoError(t, err)
	require.NotNil(t, atk)
	assert.NotEmpty(t, atk.AttackID)
	assert.Equal(t, eipARN("1"), atk.ResourceARN)
	assert.Len(t, atk.AttackVectors, 2)
	assert.Equal(t, 1, shield.AttackCount(b))
}

// TestRefinement1_ListAttacks tests ListAttacks with filters.
func TestInMemoryBackend_ListAttacks(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddAttackInternal("atk-1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")
	b.AddAttackInternal("atk-2", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-2")

	tests := []struct {
		name         string
		resourceARNs []string
		wantLen      int
	}{
		{name: "all", resourceARNs: nil, wantLen: 2},
		{name: "filtered", resourceARNs: []string{"arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1"}, wantLen: 1},
		{name: "no_match", resourceARNs: []string{"arn:aws:ec2:us-east-1::eip-allocation/eipalloc-99"}, wantLen: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			attacks := b.ListAttacks(tt.resourceARNs, 0, 0)
			assert.Len(t, attacks, tt.wantLen)
		})
	}
}
