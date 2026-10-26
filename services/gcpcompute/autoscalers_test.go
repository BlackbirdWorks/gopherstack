package gcpcompute_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

const migTargetPath = "zones/" + zone + "/instanceGroupManagers/web"

func scalerSpec(name string, minN, maxN int32) *computepb.Autoscaler {
	return &computepb.Autoscaler{
		Name: s(name), Target: s(migTargetPath),
		AutoscalingPolicy: &computepb.AutoscalingPolicy{
			MinNumReplicas: proto.Int32(minN), MaxNumReplicas: proto.Int32(maxN),
			CpuUtilization: &computepb.AutoscalingPolicyCpuUtilization{UtilizationTarget: proto.Float64(0.5)},
		},
	}
}

func (e *env) scaler(t *testing.T, spec *computepb.Autoscaler) error {
	t.Helper()

	op, err := e.autoscalers(t).Insert(t.Context(), &computepb.InsertAutoscalerRequest{
		Project: project, Zone: zone, AutoscalerResource: spec,
	})
	if err == nil {
		e.settle()
		err = op.Wait(t.Context())
	}

	return err
}

func TestAutoscalerBoundsDriveTargetSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		start      int32
		minN, maxN int32
		mode       string
		want       int32
	}{
		{name: "raised to min", start: 1, minN: 3, maxN: 6, want: 3},
		{name: "lowered to max", start: 8, minN: 1, maxN: 5, want: 5},
		{name: "inside bounds", start: 4, minN: 2, maxN: 6, want: 4},
		{name: "mode off ignores bounds", start: 1, minN: 3, maxN: 6, mode: "OFF", want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			e.mig(t, "web", e.template(t, "tpl"), tt.start)

			spec := scalerSpec("as", tt.minN, tt.maxN)
			if tt.mode != "" {
				spec.AutoscalingPolicy.Mode = s(tt.mode)
			}

			require.NoError(t, e.scaler(t, spec))
			assert.Equal(t, tt.want, e.getMig(t, "web").GetTargetSize())
			assert.Len(t, e.vmNames(t), int(tt.want))

			a, err := e.autoscalers(t).Get(t.Context(), &computepb.GetAutoscalerRequest{Project: project, Zone: zone, Autoscaler: "as"})
			require.NoError(t, err)
			assert.Equal(t, "ACTIVE", a.GetStatus())
			assert.Equal(t, tt.want, a.GetRecommendedSize())
			assert.Contains(t, e.getMig(t, "web").GetStatus().GetAutoscaler(), "/zones/"+zone+"/autoscalers/as")
		})
	}
}

func TestAutoscalerResizeIsClamped(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.mig(t, "web", e.template(t, "tpl"), 2)
	require.NoError(t, e.scaler(t, scalerSpec("as", 2, 4)))

	for _, c := range []struct{ ask, want int32 }{{ask: 10, want: 4}, {ask: 0, want: 2}} {
		_, err := e.migs(t).Resize(t.Context(), &computepb.ResizeInstanceGroupManagerRequest{
			Project: project, Zone: zone, InstanceGroupManager: "web", Size: c.ask,
		})
		require.NoError(t, err)
		assert.Equal(t, c.want, e.getMig(t, "web").GetTargetSize())
	}
}

func TestAutoscalerValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod    func(a *computepb.Autoscaler)
		name   string
		reason string
		status int
	}{
		{name: "no target", mod: func(a *computepb.Autoscaler) { a.Target = nil }, status: 400, reason: "required"},
		{name: "unknown target", mod: func(a *computepb.Autoscaler) { a.Target = s("zones/" + zone + "/instanceGroupManagers/none") }, status: 404, reason: "notFound"},
		{name: "other zone", mod: func(a *computepb.Autoscaler) { a.Target = s("zones/" + otherZone + "/instanceGroupManagers/web") }, status: 400, reason: "invalid"},
		{name: "no policy", mod: func(a *computepb.Autoscaler) { a.AutoscalingPolicy = nil }, status: 400, reason: "required"},
		{name: "max zero", mod: func(a *computepb.Autoscaler) { a.AutoscalingPolicy.MaxNumReplicas = proto.Int32(0) }, status: 400, reason: "invalid"},
		{name: "min over max", mod: func(a *computepb.Autoscaler) { a.AutoscalingPolicy.MinNumReplicas = proto.Int32(9) }, status: 400, reason: "invalid"},
		{
			name: "cpu target over one", status: 400, reason: "invalid",
			mod: func(a *computepb.Autoscaler) {
				a.AutoscalingPolicy.CpuUtilization.UtilizationTarget = proto.Float64(1.5)
			},
		},
		{name: "bad mode", mod: func(a *computepb.Autoscaler) { a.AutoscalingPolicy.Mode = s("SIDEWAYS") }, status: 400, reason: "invalid"},
		{
			name: "custom metric without name", status: 400, reason: "required",
			mod: func(a *computepb.Autoscaler) {
				a.AutoscalingPolicy.CustomMetricUtilizations = []*computepb.AutoscalingPolicyCustomMetricUtilization{
					{UtilizationTarget: proto.Float64(1)},
				}
			},
		},
		{
			name: "custom metric bad type", status: 400, reason: "invalid",
			mod: func(a *computepb.Autoscaler) {
				a.AutoscalingPolicy.CustomMetricUtilizations = []*computepb.AutoscalingPolicyCustomMetricUtilization{
					{Metric: s("custom.googleapis.com/q"), UtilizationTarget: proto.Float64(1), UtilizationTargetType: s("NOPE")},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			e.mig(t, "web", e.template(t, "tpl"), 1)

			spec := scalerSpec("as", 1, 3)
			tt.mod(spec)

			assertAPIError(t, e.scaler(t, spec), tt.status, tt.reason)
			assert.Equal(t, int32(1), e.getMig(t, "web").GetTargetSize())
		})
	}

	t.Run("duplicate and second autoscaler", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		e.baseNet(t)
		e.mig(t, "web", e.template(t, "tpl"), 1)
		require.NoError(t, e.scaler(t, scalerSpec("as", 1, 3)))

		assertAPIError(t, e.scaler(t, scalerSpec("as", 1, 3)), http.StatusConflict, "alreadyExists")
		assertAPIError(t, e.scaler(t, scalerSpec("as2", 1, 3)), http.StatusBadRequest, "invalid")
	})

	t.Run("defaults", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		e.baseNet(t)
		e.mig(t, "web", e.template(t, "tpl"), 1)

		spec := &computepb.Autoscaler{
			Name: s("as"), Target: s(migTargetPath),
			AutoscalingPolicy: &computepb.AutoscalingPolicy{MaxNumReplicas: proto.Int32(3)},
		}
		require.NoError(t, e.scaler(t, spec))

		a, err := e.autoscalers(t).Get(t.Context(), &computepb.GetAutoscalerRequest{Project: project, Zone: zone, Autoscaler: "as"})
		require.NoError(t, err)
		assert.Equal(t, int32(1), a.GetAutoscalingPolicy().GetMinNumReplicas())
		assert.Equal(t, int32(60), a.GetAutoscalingPolicy().GetCoolDownPeriodSec())
		assert.Equal(t, "ON", a.GetAutoscalingPolicy().GetMode())
		assert.InDelta(t, 0.6, a.GetAutoscalingPolicy().GetCpuUtilization().GetUtilizationTarget(), 0.0001)
	})
}

func TestAutoscalerPatchUpdateDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.mig(t, "web", e.template(t, "tpl"), 1)
	require.NoError(t, e.scaler(t, scalerSpec("as", 1, 3)))

	op, err := e.autoscalers(t).Patch(t.Context(), &computepb.PatchAutoscalerRequest{
		Project: project, Zone: zone, Autoscaler: s("as"),
		AutoscalerResource: &computepb.Autoscaler{AutoscalingPolicy: &computepb.AutoscalingPolicy{MinNumReplicas: proto.Int32(2)}},
	})
	e.done(t, op, err)

	a, err := e.autoscalers(t).Get(t.Context(), &computepb.GetAutoscalerRequest{Project: project, Zone: zone, Autoscaler: "as"})
	require.NoError(t, err)
	assert.Equal(t, int32(2), a.GetAutoscalingPolicy().GetMinNumReplicas())
	assert.Equal(t, int32(3), a.GetAutoscalingPolicy().GetMaxNumReplicas())
	assert.Equal(t, int32(2), e.getMig(t, "web").GetTargetSize())

	op, err = e.autoscalers(t).Update(t.Context(), &computepb.UpdateAutoscalerRequest{
		Project: project, Zone: zone, Autoscaler: s("as"), AutoscalerResource: scalerSpec("as", 4, 6),
	})
	e.done(t, op, err)
	assert.Equal(t, int32(4), e.getMig(t, "web").GetTargetSize())

	_, err = e.autoscalers(t).Update(t.Context(), &computepb.UpdateAutoscalerRequest{
		Project: project, Zone: zone, Autoscaler: s("as"), AutoscalerResource: scalerSpec("as", 9, 6),
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	it := e.autoscalers(t).List(t.Context(), &computepb.ListAutoscalersRequest{Project: project, Zone: zone})
	n := 0

	for {
		_, nerr := it.Next()
		if nerr == iterator.Done {
			break
		}

		require.NoError(t, nerr)

		n++
	}

	assert.Equal(t, 1, n)

	op, err = e.autoscalers(t).Delete(t.Context(), &computepb.DeleteAutoscalerRequest{Project: project, Zone: zone, Autoscaler: "as"})
	e.done(t, op, err)
	assert.Equal(t, int32(4), e.getMig(t, "web").GetTargetSize(), "deleting the autoscaler keeps the size")

	_, err = e.autoscalers(t).Get(t.Context(), &computepb.GetAutoscalerRequest{Project: project, Zone: zone, Autoscaler: "as"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestRegionalAutoscalerAndCascade(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	tmpl := e.template(t, "tpl")

	op, err := e.regionMigs(t).Insert(t.Context(), &computepb.InsertRegionInstanceGroupManagerRequest{
		Project: project, Region: region, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
			Name: s("rweb"), BaseInstanceName: s("rweb"), InstanceTemplate: s(tmpl), TargetSize: proto.Int32(1),
		},
	})
	e.done(t, op, err)

	spec := scalerSpec("ras", 4, 8)
	spec.Target = s("regions/" + region + "/instanceGroupManagers/rweb")

	op, err = e.regionAutoscalers(t).Insert(t.Context(), &computepb.InsertRegionAutoscalerRequest{
		Project: project, Region: region, AutoscalerResource: spec,
	})
	e.done(t, op, err)

	m, err := e.regionMigs(t).Get(t.Context(), &computepb.GetRegionInstanceGroupManagerRequest{
		Project: project, Region: region, InstanceGroupManager: "rweb",
	})
	require.NoError(t, err)
	assert.Equal(t, int32(4), m.GetTargetSize())

	a, err := e.regionAutoscalers(t).Get(t.Context(), &computepb.GetRegionAutoscalerRequest{
		Project: project, Region: region, Autoscaler: "ras",
	})
	require.NoError(t, err)
	assert.Contains(t, a.GetRegion(), "/regions/"+region)

	op, err = e.regionMigs(t).Delete(t.Context(), &computepb.DeleteRegionInstanceGroupManagerRequest{
		Project: project, Region: region, InstanceGroupManager: "rweb",
	})
	e.done(t, op, err)

	_, err = e.regionAutoscalers(t).Get(t.Context(), &computepb.GetRegionAutoscalerRequest{Project: project, Region: region, Autoscaler: "ras"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}
