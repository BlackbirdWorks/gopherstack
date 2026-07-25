package shield_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

func TestBackend_ListProtections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*shield.InMemoryBackend)
		name    string
		wantLen int
	}{
		{
			name:    "empty",
			setup:   func(_ *shield.InMemoryBackend) {},
			wantLen: 0,
		},
		{
			name: "one protection",
			setup: func(b *shield.InMemoryBackend) {
				_ = b.CreateSubscription()
				_, _ = b.CreateProtection("p1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1", nil)
			},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			list := b.ListProtections()
			assert.Len(t, list, tt.wantLen)
		})
	}
}

func TestInMemoryBackend_ProtectionIndexConsistency(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, b.CreateSubscription())

	const resourceARN = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app"

	p, err := b.CreateProtection("app-protection", resourceARN, nil)
	require.NoError(t, err)

	// Lookup by ARN works via index.
	byARN, err := b.DescribeProtection("", resourceARN)
	require.NoError(t, err)
	assert.Equal(t, p.ID, byARN.ID)

	// Duplicate name rejected.
	const otherARN = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/other"

	_, err = b.CreateProtection("app-protection", otherARN, nil)
	require.Error(t, err)

	// Duplicate resource ARN rejected.
	_, err = b.CreateProtection("other-name", resourceARN, nil)
	require.Error(t, err)

	// Delete cleans up indexes.
	require.NoError(t, b.DeleteProtection(p.ID))

	// Can now create with same name/ARN.
	_, err = b.CreateProtection("app-protection", resourceARN, nil)
	require.NoError(t, err)
}

// TestParity_ListProtections_SortOutsideLock verifies concurrent reads don't block each other.
func TestInMemoryBackend_ListProtectionsSortOutsideLock(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddSubscriptionInternal()

	for i := range 20 {
		b.AddProtectionInternal(
			"prot-sort-"+string(rune('a'+i%26))+string(rune('0'+i%10)),
			"arn:aws:ec2:us-east-1:123456789012:eip/eipalloc-sort"+string(rune('0'+i)),
		)
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		for range 100 {
			_ = b.ListProtections()
		}
	}()

	for range 100 {
		_ = b.ListProtections()
	}

	<-done
}

// TestParity_IndexEviction_NoPanic verifies the index eviction path doesn't panic.
func TestInMemoryBackend_IndexEvictionNoPanic(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddSubscriptionInternal()

	// Create many protections; subscription cap is 1000, which is below maxIndexEntries (10 000).
	// Just verify creating many entries and listing doesn't panic.
	for i := range 50 {
		resourceARN := "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/lb" +
			string(rune('a'+i%26)) + string(rune('0'+i%10)) + "/abc"
		name := "evict-" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		_, _ = b.CreateProtection(name, resourceARN, nil)
	}

	assert.NotPanics(t, func() {
		_ = b.ListProtections()
	})
}

// TestRefinement1_AddProtectionInternal verifies the seed helper.
func TestInMemoryBackend_AddProtectionInternal(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	p := b.AddProtectionInternal("my-prot", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-123")

	require.NotNil(t, p)
	assert.Equal(t, "my-prot", p.Name)
	assert.Equal(t, 1, shield.ProtectionCount(b))
}

// TestRefinement1_CreateProtectionRequiresSubscription verifies subscription check.
func TestInMemoryBackend_CreateProtectionRequiresSubscription(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateProtection("prot", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1", nil)
	require.Error(t, err)
}

// TestRefinement1_CreateProtection tests protection creation.
func TestInMemoryBackend_CreateProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setup       func(*shield.InMemoryBackend)
		protName    string
		resourceARN string
		wantErr     bool
	}{
		{
			name: "happy_path",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
			},
			protName:    "my-prot",
			resourceARN: "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1",
		},
		{
			name: "duplicate_name",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
				_, err := b.CreateProtection("my-prot", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1", nil)
				require.NoError(t, err)
			},
			protName:    "my-prot",
			resourceARN: "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-2",
			wantErr:     true,
		},
		{
			name: "duplicate_resource",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
				_, err := b.CreateProtection("prot-1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1", nil)
				require.NoError(t, err)
			},
			protName:    "prot-2",
			resourceARN: "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			p, err := b.CreateProtection(tt.protName, tt.resourceARN, map[string]string{"env": "test"})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, p)
			assert.Equal(t, tt.protName, p.Name)
			assert.Equal(t, 1, shield.ProtectionCount(b))
		})
	}
}

// TestRefinement1_DescribeProtection tests DescribeProtection by ID and ARN.
func TestInMemoryBackend_DescribeProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		protectionID string
		resourceARN  string
		wantErr      bool
	}{
		{
			name:         "by_id",
			protectionID: "", // filled in by setup
			resourceARN:  "",
		},
		{
			name:         "by_resource_arn",
			protectionID: "",
			resourceARN:  "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-123",
		},
		{
			name:         "not_found_by_id",
			protectionID: "no-such-id",
			resourceARN:  "",
			wantErr:      true,
		},
		{
			name:         "not_found_by_arn",
			protectionID: "",
			resourceARN:  "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-999",
			wantErr:      true,
		},
		{
			name:         "both_empty",
			protectionID: "",
			resourceARN:  "",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			p := b.AddProtectionInternal("test-prot", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-123")

			protID := tt.protectionID
			if tt.name == "by_id" {
				protID = p.ID
			}

			result, err := b.DescribeProtection(protID, tt.resourceARN)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
		})
	}
}

// TestRefinement1_DeleteProtection tests protection deletion.
func TestInMemoryBackend_DeleteProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*shield.InMemoryBackend) string
		name    string
		wantErr bool
	}{
		{
			name: "delete_existing",
			setup: func(b *shield.InMemoryBackend) string {
				p := b.AddProtectionInternal("prot-1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")

				return p.ID
			},
		},
		{
			name: "delete_not_found",
			setup: func(*shield.InMemoryBackend) string {
				return "no-such-id"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(b)

			err := b.DeleteProtection(id)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 0, shield.ProtectionCount(b))
		})
	}
}

// TestRefinement1_ListProtectionsSorted verifies sorted listing.
func TestInMemoryBackend_ListProtectionsSorted(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")

	for i, name := range []string{"z-prot", "a-prot", "m-prot"} {
		b.AddProtectionInternal(name, "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-"+string(rune('0'+i)))
	}

	list := b.ListProtections()
	require.Len(t, list, 3)
	assert.Equal(t, "a-prot", list[0].Name)
	assert.Equal(t, "m-prot", list[1].Name)
	assert.Equal(t, "z-prot", list[2].Name)
}

// TestRefinement1_ProtectionIDIsUUID verifies protection IDs are UUID-like hex strings.
func TestInMemoryBackend_ProtectionIDIsUUID(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	p, err := b.CreateProtection("test-prot", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1", nil)
	require.NoError(t, err)

	// ID should be a 32-char hex string (16 bytes), not a full ARN.
	assert.Len(t, p.ID, 32)
	assert.NotContains(t, p.ID, "arn:")

	// ProtectionArn should be the full ARN.
	assert.Contains(t, p.ProtectionArn, "arn:aws:shield::")
	assert.Contains(t, p.ProtectionArn, "protection/")
}

// TestInMemoryBackend_DeleteProtectionCascadeCleansALARConfig verifies that deleting a
// protection also removes its ApplicationLayerAutomaticResponseConfiguration entry (stored
// internally in a separate alarConfigs table keyed by ResourceARN). In the real AWS wire shape
// ALAR config is a field ON the Protection object itself (types.Protection
// .ApplicationLayerAutomaticResponseConfiguration), so a leftover row after deletion would let a
// brand new, never-configured protection for the same resource ARN incorrectly inherit stale
// ALAR settings from a protection that no longer exists.
func TestInMemoryBackend_DeleteProtectionCascadeCleansALARConfig(t *testing.T) {
	t.Parallel()

	const resourceARN = "arn:aws:ec2:us-east-1:000000000000:eip-allocation/eipalloc-cascade"

	b := shield.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, b.CreateSubscription())

	p, err := b.CreateProtection("cascade-prot", resourceARN, nil)
	require.NoError(t, err)
	require.NoError(t, b.EnableApplicationLayerAutomaticResponse(resourceARN, "BLOCK"))

	require.NotNil(t, b.GetALARConfig(resourceARN), "ALAR config must exist before delete")

	require.NoError(t, b.DeleteProtection(p.ID))

	assert.Nil(t, b.GetALARConfig(resourceARN), "ALAR config must be cascade-cleaned after protection delete")

	// A brand new protection for the same resource ARN must start with no ALAR config, not
	// inherit the deleted protection's leftover row.
	_, err = b.CreateProtection("cascade-prot-2", resourceARN, nil)
	require.NoError(t, err)
	assert.Nil(t, b.GetALARConfig(resourceARN))
}
