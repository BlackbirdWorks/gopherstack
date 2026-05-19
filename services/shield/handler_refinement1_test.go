package shield_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/shield"
)

func newR1Backend() *shield.InMemoryBackend {
	return shield.NewInMemoryBackend("000000000000", "us-east-1")
}

// TestRefinement1_HandlerOpsLen verifies 37 operations are supported.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := shield.NewHandler(newR1Backend())
	assert.Equal(t, 37, shield.HandlerOpsLen(h))
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
	b.AddProtectionInternal("my-prot", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-123")
	assert.Equal(t, 1, shield.ProtectionCount(b))

	b.Reset()
	assert.Equal(t, 0, shield.ProtectionCount(b))
}

// TestRefinement1_HandlerReset verifies Handler.Reset delegates to backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	h := shield.NewHandler(b)
	b.AddProtectionInternal("my-prot", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-123")

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
	b.AddProtectionInternal("prot-1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-111")
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
	p := b.AddProtectionInternal("my-prot", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-123")

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
	_, err := b.CreateProtection("prot", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1", nil)
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

			b := newR1Backend()
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
		b.AddProtectionInternal(name, "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-"+string(rune('0'+i)))
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
				require.NoError(t, b.CreateSubscription())
				p := b.AddProtectionInternal("prot-1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")

				return p.ProtectionArn
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
	require.NoError(t, b.CreateSubscription())
	p := b.AddProtectionInternal("prot-1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")
	require.NoError(t, b.TagResource(p.ProtectionArn, map[string]string{"env": "prod", "owner": "team"}))

	err := b.UntagResource(p.ProtectionArn, []string{"env"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(p.ProtectionArn)
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
		"ResourceArn": "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-123",
	})
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_HTTPListProtections tests HTTP list protections.
func TestRefinement1_HTTPListProtections(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	b.AddProtectionInternal("p1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")
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

// TestRefinement1_AddSubscriptionInternal tests the AddSubscriptionInternal seed helper.
func TestRefinement1_AddSubscriptionInternal(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	b.AddSubscriptionInternal()

	assert.Equal(t, "ACTIVE", b.GetSubscriptionState())

	sub, err := b.DescribeSubscription()
	require.NoError(t, err)
	assert.Equal(t, shield.AutoRenewEnabled, sub.AutoRenew)
}

// TestRefinement1_UpdateSubscription tests updating subscription auto-renew.
func TestRefinement1_UpdateSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*shield.InMemoryBackend)
		name      string
		autoRenew string
		wantErr   bool
	}{
		{
			name: "set_disabled",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
			},
			autoRenew: shield.AutoRenewDisabled,
		},
		{
			name: "set_enabled",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
			},
			autoRenew: shield.AutoRenewEnabled,
		},
		{
			name:      "no_subscription",
			setup:     func(*shield.InMemoryBackend) {},
			autoRenew: shield.AutoRenewDisabled,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newR1Backend()
			tt.setup(b)

			err := b.UpdateSubscription(tt.autoRenew)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			sub, err := b.DescribeSubscription()
			require.NoError(t, err)
			assert.Equal(t, tt.autoRenew, sub.AutoRenew)
		})
	}
}

// TestRefinement1_HTTPUpdateSubscription tests via HTTP.
func TestRefinement1_HTTPUpdateSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.InMemoryBackend)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "disable_auto_renew",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
			},
			body:       map[string]any{"AutoRenew": shield.AutoRenewDisabled},
			wantStatus: 200,
		},
		{
			name:       "invalid_auto_renew_value",
			setup:      func(*shield.InMemoryBackend) {},
			body:       map[string]any{"AutoRenew": "MAYBE"},
			wantStatus: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newR1Backend()
			tt.setup(b)
			h := shield.NewHandler(b)
			rec := doShieldRequest(t, h, "UpdateSubscription", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_DisassociateDRTLogBucket tests DRT bucket disassociation.
func TestRefinement1_DisassociateDRTLogBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*shield.InMemoryBackend)
		bucket  string
		name    string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
				require.NoError(t, b.AssociateDRTLogBucket("my-bucket"))
			},
			bucket: "my-bucket",
		},
		{
			name: "bucket_not_associated",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
			},
			bucket:  "no-such-bucket",
			wantErr: true,
		},
		{
			name:    "no_drt_access_at_all",
			setup:   func(*shield.InMemoryBackend) {},
			bucket:  "my-bucket",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newR1Backend()
			tt.setup(b)

			err := b.DisassociateDRTLogBucket(tt.bucket)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			access := b.DescribeDRTAccess()
			assert.NotContains(t, access.LogBucketList, tt.bucket)
		})
	}
}

// TestRefinement1_HTTPDisassociateDRTLogBucket tests via HTTP.
func TestRefinement1_HTTPDisassociateDRTLogBucket(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateDRTLogBucket("my-bucket"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DisassociateDRTLogBucket", map[string]any{"LogBucket": "my-bucket"})
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_DisassociateDRTRole tests DRT role disassociation.
func TestRefinement1_DisassociateDRTRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*shield.InMemoryBackend)
		name    string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
				require.NoError(t, b.AssociateDRTRole("arn:aws:iam::123:role/DRTRole"))
			},
		},
		{
			name:    "no_role_associated_idempotent",
			setup:   func(*shield.InMemoryBackend) {},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newR1Backend()
			tt.setup(b)

			err := b.DisassociateDRTRole()

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Empty(t, b.DescribeDRTAccess().RoleArn)
		})
	}
}

// TestRefinement1_HTTPDisassociateDRTRole tests via HTTP.
func TestRefinement1_HTTPDisassociateDRTRole(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateDRTRole("arn:aws:iam::123:role/DRTRole"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DisassociateDRTRole", nil)
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_DisassociateHealthCheck tests health check disassociation.
func TestRefinement1_DisassociateHealthCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*shield.InMemoryBackend) string
		healthCheckARN string
		name           string
		wantErr        bool
	}{
		{
			name: "success",
			setup: func(b *shield.InMemoryBackend) string {
				p := b.AddProtectionInternal("p1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")
				require.NoError(t, b.AssociateHealthCheck(p.ID, "arn:aws:route53:::healthcheck/hc-1"))

				return p.ID
			},
			healthCheckARN: "arn:aws:route53:::healthcheck/hc-1",
		},
		{
			name: "health_check_not_associated",
			setup: func(b *shield.InMemoryBackend) string {
				p := b.AddProtectionInternal("p2", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-2")

				return p.ID
			},
			healthCheckARN: "arn:aws:route53:::healthcheck/hc-2",
			wantErr:        true,
		},
		{
			name: "protection_not_found",
			setup: func(*shield.InMemoryBackend) string {
				return "no-such-id"
			},
			healthCheckARN: "arn:aws:route53:::healthcheck/hc-3",
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newR1Backend()
			id := tt.setup(b)

			err := b.DisassociateHealthCheck(id, tt.healthCheckARN)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestRefinement1_HTTPDisassociateHealthCheck tests via HTTP.
func TestRefinement1_HTTPDisassociateHealthCheck(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	p := b.AddProtectionInternal("p1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")
	require.NoError(t, b.AssociateHealthCheck(p.ID, "arn:aws:route53:::healthcheck/hc-1"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DisassociateHealthCheck", map[string]any{
		"ProtectionId":   p.ID,
		"HealthCheckArn": "arn:aws:route53:::healthcheck/hc-1",
	})
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_DescribeProtectionGroup tests DescribeProtectionGroup.
func TestRefinement1_DescribeProtectionGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*shield.InMemoryBackend)
		groupID string
		name    string
		wantErr bool
	}{
		{
			name: "found",
			setup: func(b *shield.InMemoryBackend) {
				b.AddProtectionGroupInternal("grp-1", shield.AggregationMax, shield.PatternAll)
			},
			groupID: "grp-1",
		},
		{
			name:    "not_found",
			setup:   func(*shield.InMemoryBackend) {},
			groupID: "no-such-group",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newR1Backend()
			tt.setup(b)

			pg, err := b.DescribeProtectionGroup(tt.groupID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupID, pg.ID)
			assert.NotEmpty(t, pg.ProtectionGroupArn)
		})
	}
}

// TestRefinement1_HTTPDescribeProtectionGroup tests via HTTP.
func TestRefinement1_HTTPDescribeProtectionGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"ProtectionGroupId": "grp-1"},
			wantStatus: 200,
		},
		{
			name:       "not_found",
			body:       map[string]any{"ProtectionGroupId": "no-such"},
			wantStatus: 400,
		},
		{
			name:       "missing_id",
			body:       map[string]any{},
			wantStatus: 400,
		},
	}

	b := newR1Backend()
	b.AddProtectionGroupInternal("grp-1", shield.AggregationMax, shield.PatternAll)
	h := shield.NewHandler(b)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doShieldRequest(t, h, "DescribeProtectionGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_ListProtectionGroups tests ListProtectionGroups.
func TestRefinement1_ListProtectionGroups(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	b.AddProtectionGroupInternal("grp-b", shield.AggregationSum, shield.PatternAll)
	b.AddProtectionGroupInternal("grp-a", shield.AggregationMax, shield.PatternAll)

	groups := b.ListProtectionGroups()
	require.Len(t, groups, 2)
	assert.Equal(t, "grp-a", groups[0].ID)
	assert.Equal(t, "grp-b", groups[1].ID)
}

// TestRefinement1_HTTPListProtectionGroups tests via HTTP.
func TestRefinement1_HTTPListProtectionGroups(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	b.AddProtectionGroupInternal("grp-1", shield.AggregationMax, shield.PatternAll)
	b.AddProtectionGroupInternal("grp-2", shield.AggregationSum, shield.PatternAll)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListProtectionGroups", nil)
	require.Equal(t, 200, rec.Code)
}

// TestRefinement1_UpdateProtectionGroup tests UpdateProtectionGroup.
func TestRefinement1_UpdateProtectionGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*shield.InMemoryBackend)
		groupID      string
		aggregation  string
		pattern      string
		resourceType string
		name         string
		members      []string
		wantErr      bool
	}{
		{
			name: "success",
			setup: func(b *shield.InMemoryBackend) {
				b.AddProtectionGroupInternal("grp-1", shield.AggregationMax, shield.PatternAll)
			},
			groupID:     "grp-1",
			aggregation: shield.AggregationSum,
			pattern:     shield.PatternAll,
		},
		{
			name:        "not_found",
			setup:       func(*shield.InMemoryBackend) {},
			groupID:     "no-such-group",
			aggregation: shield.AggregationMax,
			pattern:     shield.PatternAll,
			wantErr:     true,
		},
		{
			name: "invalid_aggregation",
			setup: func(b *shield.InMemoryBackend) {
				b.AddProtectionGroupInternal("grp-2", shield.AggregationMax, shield.PatternAll)
			},
			groupID:     "grp-2",
			aggregation: "INVALID",
			pattern:     shield.PatternAll,
			wantErr:     true,
		},
		{
			name: "arbitrary_requires_members",
			setup: func(b *shield.InMemoryBackend) {
				b.AddProtectionGroupInternal("grp-3", shield.AggregationMax, shield.PatternAll)
			},
			groupID:     "grp-3",
			aggregation: shield.AggregationMax,
			pattern:     shield.PatternArbitrary,
			members:     nil,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newR1Backend()
			tt.setup(b)

			err := b.UpdateProtectionGroup(tt.groupID, tt.aggregation, tt.pattern, tt.resourceType, tt.members)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestRefinement1_HTTPUpdateProtectionGroup tests via HTTP.
func TestRefinement1_HTTPUpdateProtectionGroup(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	b.AddProtectionGroupInternal("grp-1", shield.AggregationMax, shield.PatternAll)
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "UpdateProtectionGroup", map[string]any{
		"ProtectionGroupId": "grp-1",
		"Aggregation":       shield.AggregationSum,
		"Pattern":           shield.PatternAll,
	})
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_ListAttacks tests ListAttacks with filters.
func TestRefinement1_ListAttacks(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
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

// TestRefinement1_HTTPListAttacks tests via HTTP.
func TestRefinement1_HTTPListAttacks(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	b.AddAttackInternal("atk-1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListAttacks", map[string]any{})
	require.Equal(t, 200, rec.Code)
}

// TestRefinement1_UpdateEmergencyContactSettings tests updating emergency contacts.
func TestRefinement1_UpdateEmergencyContactSettings(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	contacts := []shield.EmergencyContact{
		{EmailAddress: "sec@example.com", PhoneNumber: "+15551234567"},
	}

	require.NoError(t, b.UpdateEmergencyContactSettings(contacts))
	assert.Equal(t, 1, shield.EmergencyContactCount(b))
}

// TestRefinement1_DescribeEmergencyContactSettings tests retrieval of contacts.
func TestRefinement1_DescribeEmergencyContactSettings(t *testing.T) {
	t.Parallel()

	b := newR1Backend()

	// Initially empty.
	contacts := b.DescribeEmergencyContactSettings()
	assert.Empty(t, contacts)

	// After update.
	require.NoError(t, b.UpdateEmergencyContactSettings([]shield.EmergencyContact{
		{EmailAddress: "sec@example.com"},
	}))

	contacts = b.DescribeEmergencyContactSettings()
	require.Len(t, contacts, 1)
	assert.Equal(t, "sec@example.com", contacts[0].EmailAddress)
}

// TestRefinement1_HTTPDescribeEmergencyContactSettings tests via HTTP.
func TestRefinement1_HTTPDescribeEmergencyContactSettings(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	require.NoError(t, b.UpdateEmergencyContactSettings([]shield.EmergencyContact{
		{EmailAddress: "ops@example.com"},
	}))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DescribeEmergencyContactSettings", nil)
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_HTTPUpdateEmergencyContactSettings tests via HTTP.
func TestRefinement1_HTTPUpdateEmergencyContactSettings(t *testing.T) {
	t.Parallel()

	h := shield.NewHandler(newR1Backend())
	rec := doShieldRequest(t, h, "UpdateEmergencyContactSettings", map[string]any{
		"EmergencyContactList": []map[string]any{
			{"EmailAddress": "sec@example.com"},
		},
	})
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_EnableDisableProactiveEngagement tests proactive engagement ops.
func TestRefinement1_EnableDisableProactiveEngagement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*shield.InMemoryBackend)
		action  func(*shield.InMemoryBackend) error
		name    string
		wantStr string
		wantErr bool
	}{
		{
			name:  "enable_requires_subscription",
			setup: func(*shield.InMemoryBackend) {},
			action: func(b *shield.InMemoryBackend) error {
				return b.EnableProactiveEngagement()
			},
			wantErr: true,
		},
		{
			name: "enable_with_subscription",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
				require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
					{EmailAddress: "sec@example.com"},
				}))
			},
			action: func(b *shield.InMemoryBackend) error {
				return b.EnableProactiveEngagement()
			},
			wantStr: shield.ProactiveEngagementEnabled,
		},
		{
			name: "disable_with_subscription",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
				require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
					{EmailAddress: "sec@example.com"},
				}))
				require.NoError(t, b.EnableProactiveEngagement())
			},
			action: func(b *shield.InMemoryBackend) error {
				return b.DisableProactiveEngagement()
			},
			wantStr: shield.ProactiveEngagementDisabled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newR1Backend()
			tt.setup(b)

			err := tt.action(b)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStr, shield.GetProactiveEngagementStatus(b))
		})
	}
}

// TestRefinement1_HTTPEnableProactiveEngagement tests via HTTP.
func TestRefinement1_HTTPEnableProactiveEngagement(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "sec@example.com"},
	}))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "EnableProactiveEngagement", nil)
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_HTTPDisableProactiveEngagement tests via HTTP.
func TestRefinement1_HTTPDisableProactiveEngagement(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "sec@example.com"},
	}))
	require.NoError(t, b.EnableProactiveEngagement())

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DisableProactiveEngagement", nil)
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_ProtectionIDIsUUID verifies protection IDs are UUID-like hex strings.
func TestRefinement1_ProtectionIDIsUUID(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
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

// TestRefinement1_ProtectionGroupARN verifies protection group ARN format.
func TestRefinement1_ProtectionGroupARN(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	pg := b.AddProtectionGroupInternal("grp-1", shield.AggregationMax, shield.PatternAll)

	require.NotNil(t, pg)
	assert.Contains(t, pg.ProtectionGroupArn, "arn:aws:shield::")
	assert.Contains(t, pg.ProtectionGroupArn, "protection-group/grp-1")
}

// TestRefinement1_SnapshotDeepCopy verifies snapshot is isolated from live mutations.
func TestRefinement1_SnapshotDeepCopy(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	p := b.AddProtectionInternal("prot-1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")

	snap := b.Snapshot()
	require.NotEmpty(t, snap)

	// Mutate backend after snapshot.
	require.NoError(t, b.DeleteProtection(p.ID))
	assert.Equal(t, 0, shield.ProtectionCount(b))

	// Restore should see the pre-mutation state.
	b2 := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))
	assert.Equal(t, 1, shield.ProtectionCount(b2))
}

// TestRefinement1_ProactiveEngagementStatusPersistedInSnapshot tests persistence.
func TestRefinement1_ProactiveEngagementStatusPersistedInSnapshot(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "sec@example.com"},
	}))
	require.NoError(t, b.EnableProactiveEngagement())

	snap := b.Snapshot()
	require.NotEmpty(t, snap)

	b2 := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))
	assert.Equal(t, shield.ProactiveEngagementEnabled, shield.GetProactiveEngagementStatus(b2))
}

// TestRefinement1_ValidAggregationConstants tests that constants are valid aggregation values.
func TestRefinement1_ValidAggregationConstants(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	require.NoError(t, b.CreateSubscription())

	for _, agg := range []string{shield.AggregationSum, shield.AggregationMean, shield.AggregationMax} {
		_, err := b.CreateProtectionGroup(agg+"-grp", agg, shield.PatternAll, "", nil)
		require.NoError(t, err, "aggregation %q should be valid", agg)
	}
}

// TestRefinement1_InvalidAggregation tests invalid aggregation is rejected.
func TestRefinement1_InvalidAggregation(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	require.NoError(t, b.CreateSubscription())

	_, err := b.CreateProtectionGroup("grp", "INVALID", shield.PatternAll, "", nil)
	require.Error(t, err)
}

// TestRefinement1_PatternArbitraryRequiresMembers tests ARBITRARY requires members.
func TestRefinement1_PatternArbitraryRequiresMembers(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	require.NoError(t, b.CreateSubscription())

	_, err := b.CreateProtectionGroup("grp", shield.AggregationMax, shield.PatternArbitrary, "", nil)
	require.Error(t, err)
}

// TestRefinement1_PatternByResourceTypeRequiresResourceType tests requirement.
func TestRefinement1_PatternByResourceTypeRequiresResourceType(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	require.NoError(t, b.CreateSubscription())

	_, err := b.CreateProtectionGroup("grp", shield.AggregationMax, shield.PatternByResourceType, "", nil)
	require.Error(t, err)
}

// TestRefinement1_AddProtectionGroupInternal verifies the seed helper.
func TestRefinement1_AddProtectionGroupInternal(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	pg := b.AddProtectionGroupInternal("grp-1", shield.AggregationMax, shield.PatternAll)

	require.NotNil(t, pg)
	assert.Equal(t, "grp-1", pg.ID)
	assert.Equal(t, shield.AggregationMax, pg.Aggregation)
	assert.Equal(t, shield.PatternAll, pg.Pattern)
	assert.Equal(t, 1, shield.ProtectionGroupCount(b))
}

// TestRefinement1_EmergencyContactCount tests EmergencyContactCount export.
func TestRefinement1_EmergencyContactCount(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	assert.Equal(t, 0, shield.EmergencyContactCount(b))

	require.NoError(t, b.UpdateEmergencyContactSettings([]shield.EmergencyContact{
		{EmailAddress: "a@a.com"},
		{EmailAddress: "b@b.com"},
	}))
	assert.Equal(t, 2, shield.EmergencyContactCount(b))
}

// TestRefinement1_DescribeSubscriptionIncludesArn verifies SubscriptionArn in response.
func TestRefinement1_DescribeSubscriptionIncludesArn(t *testing.T) {
	t.Parallel()

	b := newR1Backend()
	require.NoError(t, b.CreateSubscription())

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DescribeSubscription", nil)
	require.Equal(t, 200, rec.Code)

	var result map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
	sub, ok := result["Subscription"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, sub, "SubscriptionArn")
}
