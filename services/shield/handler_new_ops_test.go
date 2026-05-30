package shield_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestHandler_AssociateDRTLogBucket tests the AssociateDRTLogBucket operation.
func TestHandler_AssociateDRTLogBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) {
				require.NoError(t, h.Backend.CreateSubscription())
			},
			body:       map[string]any{"LogBucket": "my-shield-logs"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing log bucket",
			setup:      func(_ *shield.Handler) {},
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "no subscription returns error",
			setup:      func(_ *shield.Handler) {},
			body:       map[string]any{"LogBucket": "my-shield-logs"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)
			rec := doShieldRequest(t, h, "AssociateDRTLogBucket", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_AssociateDRTRole tests the AssociateDRTRole operation.
func TestHandler_AssociateDRTRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) {
				require.NoError(t, h.Backend.CreateSubscription())
			},
			body:       map[string]any{"RoleArn": "arn:aws:iam::123456789012:role/DRTRole"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing role arn",
			setup:      func(_ *shield.Handler) {},
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "no subscription returns error",
			setup:      func(_ *shield.Handler) {},
			body:       map[string]any{"RoleArn": "arn:aws:iam::123456789012:role/DRTRole"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)
			rec := doShieldRequest(t, h, "AssociateDRTRole", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DescribeDRTAccess tests the DescribeDRTAccess operation.
func TestHandler_DescribeDRTAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*shield.Handler)
		name        string
		wantRoleArn string
		wantBuckets int
		wantStatus  int
	}{
		{
			name:        "empty drt access",
			setup:       func(_ *shield.Handler) {},
			wantStatus:  http.StatusOK,
			wantBuckets: 0,
			wantRoleArn: "",
		},
		{
			name: "with role and bucket",
			setup: func(h *shield.Handler) {
				require.NoError(t, h.Backend.CreateSubscription())
				require.NoError(t, h.Backend.AssociateDRTRole("arn:aws:iam::123:role/DRTRole"))
				require.NoError(t, h.Backend.AssociateDRTLogBucket("my-logs-bucket"))
			},
			wantStatus:  http.StatusOK,
			wantBuckets: 1,
			wantRoleArn: "arn:aws:iam::123:role/DRTRole",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doShieldRequest(t, h, "DescribeDRTAccess", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var result map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))

			if tt.wantRoleArn != "" {
				assert.Equal(t, tt.wantRoleArn, result["RoleArn"])
			}

			buckets, _ := result["LogBucketList"].([]any)
			assert.Len(t, buckets, tt.wantBuckets)
		})
	}
}

// TestHandler_AssociateHealthCheck tests the AssociateHealthCheck operation.
func TestHandler_AssociateHealthCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) string {
				require.NoError(t, h.Backend.CreateSubscription())
				p, err := h.Backend.CreateProtection("p1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1", nil)
				require.NoError(t, err)

				return p.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{
					"ProtectionId":   id,
					"HealthCheckArn": "arn:aws:route53:::healthcheck/abc123",
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "protection not found",
			setup: func(_ *shield.Handler) string {
				return "nonexistent"
			},
			body: func(id string) map[string]any {
				return map[string]any{
					"ProtectionId":   id,
					"HealthCheckArn": "arn:aws:route53:::healthcheck/abc123",
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing protection id",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{
					"HealthCheckArn": "arn:aws:route53:::healthcheck/abc123",
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing health check arn",
			setup: func(h *shield.Handler) string {
				require.NoError(t, h.Backend.CreateSubscription())
				p, err := h.Backend.CreateProtection("p2", "arn:aws:ec2:us-east-1:123:eip/eipalloc-2", nil)
				require.NoError(t, err)

				return p.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{
					"ProtectionId": id,
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doShieldRequest(t, h, "AssociateHealthCheck", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_AssociateProactiveEngagementDetails tests AssociateProactiveEngagementDetails.
func TestHandler_AssociateProactiveEngagementDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success with email",
			body: map[string]any{
				"EmergencyContactList": []map[string]any{
					{"EmailAddress": "security@example.com"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "success with full contact details",
			body: map[string]any{
				"EmergencyContactList": []map[string]any{
					{
						"EmailAddress": "security@example.com",
						"PhoneNumber":  "+15555551234",
						"ContactNotes": "24/7 security team",
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "empty contact list",
			body: map[string]any{
				"EmergencyContactList": []map[string]any{},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing email in contact",
			body: map[string]any{
				"EmergencyContactList": []map[string]any{
					{"PhoneNumber": "+15555551234"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing contact list",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doShieldRequest(t, h, "AssociateProactiveEngagementDetails", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_CreateProtectionGroup tests CreateProtectionGroup.
func TestHandler_CreateProtectionGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success all fields",
			setup: func(h *shield.Handler) {
				require.NoError(t, h.Backend.CreateSubscription())
			},
			body: map[string]any{
				"ProtectionGroupId": "group-1",
				"Aggregation":       "MAX",
				"Pattern":           "ALL",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "success with members and resource type",
			setup: func(h *shield.Handler) {
				require.NoError(t, h.Backend.CreateSubscription())
			},
			body: map[string]any{
				"ProtectionGroupId": "group-2",
				"Aggregation":       "SUM",
				"Pattern":           "ARBITRARY",
				"ResourceType":      "ELASTIC_IP_ALLOCATION",
				"Members":           []string{"arn:aws:ec2:us-east-1:123:eip/eipalloc-1"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "duplicate group",
			setup: func(h *shield.Handler) {
				require.NoError(t, h.Backend.CreateSubscription())
				_, err := h.Backend.CreateProtectionGroup("group-dup", "MAX", "ALL", "", nil)
				require.NoError(t, err)
			},
			body: map[string]any{
				"ProtectionGroupId": "group-dup",
				"Aggregation":       "MAX",
				"Pattern":           "ALL",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:  "missing protection group id",
			setup: func(_ *shield.Handler) {},
			body: map[string]any{
				"Aggregation": "MAX",
				"Pattern":     "ALL",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:  "missing aggregation",
			setup: func(_ *shield.Handler) {},
			body: map[string]any{
				"ProtectionGroupId": "group-3",
				"Pattern":           "ALL",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:  "missing pattern",
			setup: func(_ *shield.Handler) {},
			body: map[string]any{
				"ProtectionGroupId": "group-4",
				"Aggregation":       "MAX",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)
			rec := doShieldRequest(t, h, "CreateProtectionGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DeleteProtectionGroup tests DeleteProtectionGroup.
func TestHandler_DeleteProtectionGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) string {
				require.NoError(t, h.Backend.CreateSubscription())
				_, err := h.Backend.CreateProtectionGroup("group-1", "MAX", "ALL", "", nil)
				require.NoError(t, err)

				return "group-1"
			},
			body: func(id string) map[string]any {
				return map[string]any{"ProtectionGroupId": id}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *shield.Handler) string {
				return "nonexistent"
			},
			body: func(id string) map[string]any {
				return map[string]any{"ProtectionGroupId": id}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing protection group id",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doShieldRequest(t, h, "DeleteProtectionGroup", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DeleteSubscription tests DeleteSubscription.
func TestHandler_DeleteSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) {
				require.NoError(t, h.Backend.CreateSubscription())
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "no active subscription",
			setup:      func(_ *shield.Handler) {},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doShieldRequest(t, h, "DeleteSubscription", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DescribeAttack tests DescribeAttack.
func TestHandler_DescribeAttack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.InMemoryBackend) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
		wantFields bool
	}{
		{
			name: "success",
			setup: func(b *shield.InMemoryBackend) string {
				a := b.AddAttackInternal("attack-001", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1")

				return a.AttackID
			},
			body: func(id string) map[string]any {
				return map[string]any{"AttackId": id}
			},
			wantStatus: http.StatusOK,
			wantFields: true,
		},
		{
			name: "not found",
			setup: func(_ *shield.InMemoryBackend) string {
				return "nonexistent-attack"
			},
			body: func(id string) map[string]any {
				return map[string]any{"AttackId": id}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing attack id",
			setup: func(_ *shield.InMemoryBackend) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			h := shield.NewHandler(b)
			id := tt.setup(b)
			rec := doShieldRequest(t, h, "DescribeAttack", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantFields {
				var result map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				attack, ok := result["Attack"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, id, attack["AttackId"])
				assert.NotEmpty(t, attack["ResourceArn"])
			}
		})
	}
}

// TestHandler_DescribeAttackStatistics tests DescribeAttackStatistics.
func TestHandler_DescribeAttackStatistics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.InMemoryBackend)
		name       string
		wantCount  int64
		wantStatus int
	}{
		{
			name:       "no attacks",
			setup:      func(_ *shield.InMemoryBackend) {},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "two attacks",
			setup: func(b *shield.InMemoryBackend) {
				b.AddAttackInternal("atk-1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1")
				b.AddAttackInternal("atk-2", "arn:aws:ec2:us-east-1:123:eip/eipalloc-2")
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			h := shield.NewHandler(b)
			tt.setup(b)

			rec := doShieldRequest(t, h, "DescribeAttackStatistics", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var result map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))

			items, ok := result["DataItems"].([]any)
			require.True(t, ok)
			require.Len(t, items, 1)

			item, ok := items[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantCount, int64(item["AttackCount"].(float64)))
		})
	}
}

// TestBackend_AssociateDRTLogBucket tests backend DRT log bucket association.
func TestBackend_AssociateDRTLogBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		buckets []string
		wantErr bool
	}{
		{
			name:    "single bucket",
			buckets: []string{"my-logs"},
		},
		{
			name:    "two distinct buckets",
			buckets: []string{"bucket-a", "bucket-b"},
		},
		{
			name:    "empty bucket returns error",
			buckets: []string{""},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.CreateSubscription())

			for _, bucket := range tt.buckets {
				err := b.AssociateDRTLogBucket(bucket)

				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
			}

			access := b.DescribeDRTAccess()
			assert.Len(t, access.LogBucketList, len(tt.buckets))
		})
	}
}

// TestBackend_AssociateDRTRole tests backend DRT role association.
func TestBackend_AssociateDRTRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		roleARN string
		wantErr bool
	}{
		{
			name:    "success",
			roleARN: "arn:aws:iam::123:role/DRTRole",
		},
		{
			name:    "empty role arn returns error",
			roleARN: "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.CreateSubscription())
			err := b.AssociateDRTRole(tt.roleARN)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			access := b.DescribeDRTAccess()
			assert.Equal(t, tt.roleARN, access.RoleArn)
		})
	}
}

// TestBackend_AssociateHealthCheck tests backend health check association.
func TestBackend_AssociateHealthCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*shield.InMemoryBackend) string
		name           string
		healthCheckARN string
		wantErr        bool
	}{
		{
			name: "success",
			setup: func(b *shield.InMemoryBackend) string {
				p := b.AddProtectionInternal("p1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1")

				return p.ID
			},
			healthCheckARN: "arn:aws:route53:::healthcheck/hc-001",
		},
		{
			name: "idempotent",
			setup: func(b *shield.InMemoryBackend) string {
				p := b.AddProtectionInternal("p2", "arn:aws:ec2:us-east-1:123:eip/eipalloc-2")
				require.NoError(t, b.AssociateHealthCheck(p.ID, "arn:aws:route53:::healthcheck/hc-002"))

				return p.ID
			},
			healthCheckARN: "arn:aws:route53:::healthcheck/hc-002",
		},
		{
			name: "protection not found",
			setup: func(_ *shield.InMemoryBackend) string {
				return "no-such-id"
			},
			healthCheckARN: "arn:aws:route53:::healthcheck/hc-001",
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(b)

			err := b.AssociateHealthCheck(id, tt.healthCheckARN)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestBackend_CreateDeleteProtectionGroup tests backend protection group operations.
func TestBackend_CreateDeleteProtectionGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		aggregation  string
		pattern      string
		resourceType string
		groupID      string
		members      []string
		wantErr      bool
	}{
		{
			name:        "success all required",
			groupID:     "grp-1",
			aggregation: "MAX",
			pattern:     "ALL",
		},
		{
			name:         "success with members",
			groupID:      "grp-2",
			aggregation:  "SUM",
			pattern:      "ARBITRARY",
			resourceType: "ELASTIC_IP_ALLOCATION",
			members:      []string{"arn:aws:ec2:us-east-1:123:eip/eipalloc-1"},
		},
		{
			name:        "missing id",
			groupID:     "",
			aggregation: "MAX",
			pattern:     "ALL",
			wantErr:     true,
		},
		{
			name:        "missing aggregation",
			groupID:     "grp-3",
			aggregation: "",
			pattern:     "ALL",
			wantErr:     true,
		},
		{
			name:        "missing pattern",
			groupID:     "grp-4",
			aggregation: "MAX",
			pattern:     "",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.CreateSubscription())

			pg, err := b.CreateProtectionGroup(tt.groupID, tt.aggregation, tt.pattern, tt.resourceType, tt.members)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, pg)
			assert.Equal(t, tt.groupID, pg.ID)
			assert.Equal(t, 1, shield.ProtectionGroupCount(b))

			// Test delete.
			require.NoError(t, b.DeleteProtectionGroup(tt.groupID))
			assert.Equal(t, 0, shield.ProtectionGroupCount(b))
		})
	}
}

// TestBackend_DeleteSubscription tests backend subscription deletion.
func TestBackend_DeleteSubscription(t *testing.T) {
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
			},
		},
		{
			name:    "no subscription",
			setup:   func(_ *shield.InMemoryBackend) {},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			err := b.DeleteSubscription()

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "INACTIVE", b.GetSubscriptionState())
		})
	}
}

// TestBackend_SnapshotRestoreNewFields verifies new fields are persisted across snapshot/restore.
func TestBackend_SnapshotRestoreNewFields(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")

	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateDRTRole("arn:aws:iam::123:role/DRTRole"))
	require.NoError(t, b.AssociateDRTLogBucket("my-bucket"))

	_, err := b.CreateProtectionGroup("grp-1", "MAX", "ALL", "", nil)
	require.NoError(t, err)

	b.AddAttackInternal("atk-1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1")

	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "sec@example.com"},
	}))

	snap := b.Snapshot()
	require.NotEmpty(t, snap)

	b2 := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	access := b2.DescribeDRTAccess()
	assert.Equal(t, "arn:aws:iam::123:role/DRTRole", access.RoleArn)
	assert.Len(t, access.LogBucketList, 1)

	assert.Equal(t, 1, shield.ProtectionGroupCount(b2))
	assert.Equal(t, 1, shield.AttackCount(b2))
}

// TestHandler_GetSupportedOperationsNewOps verifies new ops are in supported list.
func TestHandler_GetSupportedOperationsNewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"AssociateDRTLogBucket",
		"AssociateDRTRole",
		"AssociateHealthCheck",
		"AssociateProactiveEngagementDetails",
		"CreateProtectionGroup",
		"DeleteProtectionGroup",
		"DeleteSubscription",
		"DescribeAttack",
		"DescribeAttackStatistics",
		"DescribeDRTAccess",
	} {
		assert.Contains(t, ops, op, "missing op: %s", op)
	}
}
