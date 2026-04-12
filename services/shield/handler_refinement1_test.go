package shield_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/shield"
)

func newR1Backend() *shield.InMemoryBackend {
	return shield.NewInMemoryBackend("000000000000", "us-east-1")
}

// TestRefinement1_HandlerOpsLen verifies 10 operations are supported.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := shield.NewHandler(newR1Backend())
	assert.Equal(t, 10, shield.HandlerOpsLen(h))
}

// TestRefinement1_AccountID verifies AccountID returns the configured value.
func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	assert.Equal(t, "000000000000", b.AccountID())
}

// TestRefinement1_Region verifies Region returns the configured value.
func TestRefinement1_Region(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	assert.Equal(t, "us-east-1", b.Region())
}

// TestRefinement1_ErrNilAppContext verifies the nil guard in provider.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &shield.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, shield.ErrNilAppContext)
}

// TestRefinement1_Reset verifies Reset clears all state.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	b.AddProtectionInternal("my-prot", "arn:aws:ec2:us-east-1::instance/i-123")
	assert.Equal(t, 1, shield.ProtectionCount(b))

	b.Reset()
	assert.Equal(t, 0, shield.ProtectionCount(b))
}

// TestRefinement1_HandlerReset verifies Handler.Reset delegates to backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	h := shield.NewHandler(b)
	b.AddProtectionInternal("my-prot", "arn:aws:ec2:us-east-1::instance/i-123")

	h.Reset()
	assert.Equal(t, 0, shield.ProtectionCount(b))
}

// TestRefinement1_StorageBackendInterface verifies var_ assertion compiles.
func TestRefinement1_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ shield.StorageBackend = (*shield.InMemoryBackend)(nil)
}

// TestRefinement1_SnapshotRestore verifies snapshot and restore.
func TestRefinement1_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	b.AddProtectionInternal("prot-1", "arn:aws:ec2:us-east-1::instance/i-111")
	require.NoError(t, b.CreateSubscription())

	snap := b.Snapshot()
	require.NotEmpty(t, snap)

	b2 := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, 1, shield.ProtectionCount(b2))

	sub, err := b2.DescribeSubscription()
	require.NoError(t, err)
	assert.NotNil(t, sub)
}

// TestRefinement1_AddProtectionInternal verifies the seed helper.
func TestRefinement1_AddProtectionInternal(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	p := b.AddProtectionInternal("my-prot", "arn:aws:ec2:us-east-1::instance/i-123")

	require.NotNil(t, p)
	assert.Equal(t, "my-prot", p.Name)
	assert.Equal(t, 1, shield.ProtectionCount(b))
}

// TestRefinement1_CreateSubscription tests subscription creation.
func TestRefinement1_CreateSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*shield.InMemoryBackend)
		name    string
		wantErr bool
	}{
		{
			name:    "create_new",
			setup:   func(*shield.InMemoryBackend) {},
			wantErr: false,
		},
		{
			name: "duplicate_silently_accepted",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newR1Backend()
			tt.setup(b)

			err := b.CreateSubscription()

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestRefinement1_CreateProtectionRequiresSubscription verifies subscription check.
func TestRefinement1_CreateProtectionRequiresSubscription(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	_, err := b.CreateProtection("prot", "arn:aws:ec2:us-east-1::instance/i-1", nil)
	require.Error(t, err)
}

// TestRefinement1_CreateProtection tests protection creation.
func TestRefinement1_CreateProtection(t *testing.T) {
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
			resourceARN: "arn:aws:ec2:us-east-1::instance/i-1",
		},
		{
			name: "duplicate_name",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
				_, err := b.CreateProtection("my-prot", "arn:aws:ec2:us-east-1::instance/i-1", nil)
				require.NoError(t, err)
			},
			protName:    "my-prot",
			resourceARN: "arn:aws:ec2:us-east-1::instance/i-2",
			wantErr:     true,
		},
		{
			name: "duplicate_resource",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
				_, err := b.CreateProtection("prot-1", "arn:aws:ec2:us-east-1::instance/i-1", nil)
				require.NoError(t, err)
			},
			protName:    "prot-2",
			resourceARN: "arn:aws:ec2:us-east-1::instance/i-1",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newR1Backend()
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
func TestRefinement1_DescribeProtection(t *testing.T) {
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
			resourceARN:  "arn:aws:ec2:us-east-1::instance/i-123",
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
			resourceARN:  "arn:aws:ec2:us-east-1::instance/i-999",
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

			b := newR1Backend()
			p := b.AddProtectionInternal("test-prot", "arn:aws:ec2:us-east-1::instance/i-123")

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
func TestRefinement1_DeleteProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*shield.InMemoryBackend) string
		name    string
		wantErr bool
	}{
		{
			name: "delete_existing",
			setup: func(b *shield.InMemoryBackend) string {
				p := b.AddProtectionInternal("prot-1", "arn:aws:ec2:us-east-1::instance/i-1")

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

			b := newR1Backend()
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
func TestRefinement1_ListProtectionsSorted(t *testing.T) {
	t.Parallel()

	b := newR1Backend()

	for i, name := range []string{"z-prot", "a-prot", "m-prot"} {
		b.AddProtectionInternal(name, "arn:aws:ec2:us-east-1::instance/i-"+string(rune('0'+i)))
	}

	list := b.ListProtections()
	require.Len(t, list, 3)
	assert.Equal(t, "a-prot", list[0].Name)
	assert.Equal(t, "m-prot", list[1].Name)
	assert.Equal(t, "z-prot", list[2].Name)
}

// TestRefinement1_TagResource tests tagging.
func TestRefinement1_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*shield.InMemoryBackend) string
		tags    map[string]string
		name    string
		wantErr bool
	}{
		{
			name: "tag_existing",
			setup: func(b *shield.InMemoryBackend) string {
				p := b.AddProtectionInternal("prot-1", "arn:aws:ec2:us-east-1::instance/i-1")

				return p.ID
			},
			tags: map[string]string{"env": "prod"},
		},
		{
			name: "tag_not_found",
			setup: func(*shield.InMemoryBackend) string {
				return "no-such-id"
			},
			tags:    map[string]string{"env": "prod"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newR1Backend()
			id := tt.setup(b)

			err := b.TagResource(id, tt.tags)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestRefinement1_UntagResource tests untagging.
func TestRefinement1_UntagResource(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	p := b.AddProtectionInternal("prot-1", "arn:aws:ec2:us-east-1::instance/i-1")
	require.NoError(t, b.TagResource(p.ID, map[string]string{"env": "prod", "owner": "team"}))

	err := b.UntagResource(p.ID, []string{"env"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(p.ID)
	require.NoError(t, err)
	assert.NotContains(t, tags, "env")
	assert.Contains(t, tags, "owner")
}

// TestRefinement1_GetSubscriptionState tests GetSubscriptionState.
func TestRefinement1_GetSubscriptionState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(*shield.InMemoryBackend)
		wantState string
	}{
		{
			name:      "inactive_no_subscription",
			setup:     func(*shield.InMemoryBackend) {},
			wantState: "INACTIVE",
		},
		{
			name: "active_with_subscription",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
			},
			wantState: "ACTIVE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newR1Backend()
			tt.setup(b)

			assert.Equal(t, tt.wantState, b.GetSubscriptionState())
		})
	}
}

// TestRefinement1_SDKOpsSorted verifies GetSupportedOperations is sorted.
func TestRefinement1_SDKOpsSorted(t *testing.T) {
	t.Parallel()

	h := shield.NewHandler(newR1Backend())
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}

// TestRefinement1_HTTPCreateSubscription tests via HTTP.
func TestRefinement1_HTTPCreateSubscription(t *testing.T) {
	t.Parallel()

	h := shield.NewHandler(newR1Backend())
	rec := doShieldRequest(t, h, "CreateSubscription", nil)
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_HTTPGetSubscriptionState tests via HTTP.
func TestRefinement1_HTTPGetSubscriptionState(t *testing.T) {
	t.Parallel()

	h := shield.NewHandler(newR1Backend())
	rec := doShieldRequest(t, h, "GetSubscriptionState", nil)
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_HTTPDescribeSubscriptionNotFound tests 404 when no subscription.
func TestRefinement1_HTTPDescribeSubscriptionNotFound(t *testing.T) {
	t.Parallel()

	h := shield.NewHandler(newR1Backend())
	rec := doShieldRequest(t, h, "DescribeSubscription", nil)
	assert.Equal(t, 400, rec.Code)
}

// TestRefinement1_HTTPCreateProtection tests HTTP create protection.
func TestRefinement1_HTTPCreateProtection(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	require.NoError(t, b.CreateSubscription())
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "CreateProtection", map[string]any{
		"Name":        "test-prot",
		"ResourceArn": "arn:aws:ec2:us-east-1::instance/i-123",
	})
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_HTTPListProtections tests HTTP list protections.
func TestRefinement1_HTTPListProtections(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	b.AddProtectionInternal("p1", "arn:aws:ec2:us-east-1::instance/i-1")
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "ListProtections", nil)
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_ProviderInit tests Provider Init with valid context.
func TestRefinement1_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &shield.Provider{}
	reg, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}
