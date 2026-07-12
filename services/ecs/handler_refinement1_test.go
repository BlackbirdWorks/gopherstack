package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestRefinement1_Backend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFunc func(*ecs.InMemoryBackend)
		name      string
		wantCount int
	}{
		{
			name: "reset clears capacity providers",
			setupFunc: func(b *ecs.InMemoryBackend) {
				b.AddCapacityProviderInternal(&ecs.CapacityProvider{
					Name:                "test-cp",
					CapacityProviderArn: "arn:aws:ecs:us-east-1:000000000000:capacity-provider/test-cp",
					Status:              "ACTIVE",
				})
			},
			wantCount: 0,
		},
		{
			name:      "reset on empty backend is a no-op",
			setupFunc: func(*ecs.InMemoryBackend) {},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
			tt.setupFunc(b)
			b.Reset()

			assert.Equal(t, tt.wantCount, b.CapacityProviderCount())
			assert.Equal(t, tt.wantCount, b.ServiceDeploymentCount())
			assert.Equal(t, tt.wantCount, b.AccountSettingCount())
			assert.Equal(t, tt.wantCount, b.ExpressGatewayServiceCount())
		})
	}
}

func TestRefinement1_Handler_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset clears backend state via handler"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": "cp1"})
			doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": "cp2"})

			h.Reset()

			rec := doECSRequest(t, h, "DescribeCapacityProviders", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			cps, ok := resp["capacityProviders"].([]any)
			require.True(t, ok)
			assert.Empty(t, cps)
		})
	}
}

func TestRefinement1_BuildOps_Cached(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input  map[string]any
		name   string
		action string
		want   int
	}{
		{
			name:   "CreateCapacityProvider is available via cached ops",
			action: "CreateCapacityProvider",
			input:  map[string]any{"name": "ops-test-cp"},
			want:   http.StatusOK,
		},
		{
			name:   "unknown action returns 400",
			action: "NonExistentOperation",
			input:  map[string]any{},
			want:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, tt.action, tt.input)
			assert.Equal(t, tt.want, rec.Code)
		})
	}
}

func TestRefinement1_DeepCopy_CapacityProviderTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tagKey  string
		tagVal  string
		wantTag string
		mutate  bool
	}{
		{
			name:    "tags are independent after create",
			tagKey:  "env",
			tagVal:  "prod",
			mutate:  true,
			wantTag: "prod",
		},
		{
			name:    "no tags returns nil slice",
			tagKey:  "",
			tagVal:  "",
			mutate:  false,
			wantTag: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

			tags := []ecs.Tag{}
			if tt.tagKey != "" {
				tags = append(tags, ecs.Tag{Key: tt.tagKey, Value: tt.tagVal})
			}

			cp, err := b.CreateCapacityProvider(ecs.CreateCapacityProviderInput{
				Name: "deep-copy-test",
				Tags: tags,
			})
			require.NoError(t, err)

			if tt.mutate && len(cp.Tags) > 0 {
				cp.Tags[0].Value = "mutated"
			}

			cps, _, err := b.DescribeCapacityProviders([]string{"deep-copy-test"})
			require.NoError(t, err)
			require.Len(t, cps, 1)

			if tt.tagKey != "" {
				require.Len(t, cps[0].Tags, 1)
				assert.Equal(t, tt.wantTag, cps[0].Tags[0].Value)
			} else {
				assert.Empty(t, cps[0].Tags)
			}
		})
	}
}

func TestRefinement1_DeepCopy_ExpressGatewayTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tagKey  string
		tagVal  string
		wantTag string
		mutate  bool
	}{
		{
			name:    "tags are independent after create",
			tagKey:  "team",
			tagVal:  "platform",
			mutate:  true,
			wantTag: "platform",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

			tags := []ecs.Tag{{Key: tt.tagKey, Value: tt.tagVal}}
			svc, err := b.CreateExpressGatewayService(ecs.CreateExpressGatewayServiceInput{
				ExecutionRoleArn:      "arn:aws:iam::000000000000:role/exec",
				InfrastructureRoleArn: "arn:aws:iam::000000000000:role/infra",
				ServiceName:           "gw-test",
				Tags:                  tags,
			})
			require.NoError(t, err)

			if tt.mutate && len(svc.Tags) > 0 {
				svc.Tags[0].Value = "mutated"
			}

			got, err := b.DescribeExpressGatewayService(svc.ServiceArn)
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, tt.wantTag, got.Tags[0].Value)
		})
	}
}

func TestRefinement1_CountHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFunc   func(*ecs.InMemoryBackend)
		name        string
		attrCluster string
		wantCP      int
		wantSD      int
		wantAS      int
		wantEGS     int
		wantAttr    int
	}{
		{
			name:      "empty backend has zero counts",
			setupFunc: func(*ecs.InMemoryBackend) {},
		},
		{
			name: "one of each increments count",
			setupFunc: func(b *ecs.InMemoryBackend) {
				b.AddCapacityProviderInternal(&ecs.CapacityProvider{
					Name:                "cp1",
					CapacityProviderArn: "arn:aws:ecs:us-east-1:000000000000:capacity-provider/cp1",
				})

				now := time.Now()
				b.AddServiceDeploymentInternal(&ecs.ServiceDeployment{
					ServiceDeploymentArn: "arn:aws:ecs:us-east-1:000000000000:service-deployment/sd1",
					ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/default",
					ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/default/svc1",
					Status:               "COMPLETED",
					CreatedAt:            &now,
				})

				b.AddAccountSettingInternal(":containerInsights", &ecs.AccountSetting{
					Name:  "containerInsights",
					Value: "enabled",
				})

				b.AddAttributeInternal("default", &ecs.Attribute{
					Name:     "custom",
					Value:    "value",
					TargetID: "ci-abc",
				})
			},
			wantCP:      1,
			wantSD:      1,
			wantAS:      1,
			wantEGS:     0,
			wantAttr:    1,
			attrCluster: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
			tt.setupFunc(b)

			assert.Equal(t, tt.wantCP, b.CapacityProviderCount())
			assert.Equal(t, tt.wantSD, b.ServiceDeploymentCount())
			assert.Equal(t, tt.wantAS, b.AccountSettingCount())
			assert.Equal(t, tt.wantEGS, b.ExpressGatewayServiceCount())

			if tt.attrCluster != "" {
				assert.Equal(t, tt.wantAttr, b.AttributeCount(tt.attrCluster))
			}
		})
	}
}

func TestRefinement1_Provider_NilAppContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		nilAppCtx bool
	}{
		{
			name:      "nil AppContext returns ErrNilAppContext",
			nilAppCtx: true,
			wantErr:   ecs.ErrNilAppContext,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &ecs.Provider{}
			_, err := p.Init(nil)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRefinement1_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cpName   string
		wantName string
	}{
		{
			name:     "roundtrip preserves capacity provider",
			cpName:   "snap-cp",
			wantName: "snap-cp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": tt.cpName})

			snap := h.Snapshot(t.Context())
			require.NotNil(t, snap)

			h2 := newTestHandler(t)
			require.NoError(t, h2.Restore(t.Context(), snap))

			rec := doECSRequest(t, h2, "DescribeCapacityProviders", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			cps, ok := resp["capacityProviders"].([]any)
			require.True(t, ok)
			require.Len(t, cps, 1)

			cp := cps[0].(map[string]any)
			assert.Equal(t, tt.wantName, cp["name"])
		})
	}
}

func TestRefinement1_NonNilEmptySlices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "DescribeCapacityProviders returns non-nil empty slice when no providers"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
			cps, _, err := b.DescribeCapacityProviders(nil)
			require.NoError(t, err)
			assert.NotNil(t, cps)
			assert.Empty(t, cps)
		})
	}
}

func TestRefinement1_DescribeCapacityProviders_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cpNames   []string
		wantOrder []string
	}{
		{
			name:      "multiple providers returned",
			cpNames:   []string{"alpha", "beta", "gamma"},
			wantOrder: []string{"alpha", "beta", "gamma"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range tt.cpNames {
				rec := doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": name})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doECSRequest(t, h, "DescribeCapacityProviders", map[string]any{
				"capacityProviders": tt.cpNames,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			cps, ok := resp["capacityProviders"].([]any)
			require.True(t, ok)
			require.Len(t, cps, len(tt.wantOrder))

			for i, wantName := range tt.wantOrder {
				cp := cps[i].(map[string]any)
				assert.Equal(t, wantName, cp["name"])
			}
		})
	}
}
