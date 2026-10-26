package gcpcompute_test

import (
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpcompute"
)

func TestGroupsSurviveSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	src.baseNet(t)
	src.healthCheck(t, "hc")
	tmpl := src.template(t, "tpl")
	src.mig(t, "web", tmpl, 2)
	src.group(t, "ug")
	require.NoError(t, src.scaler(t, scalerSpec("as", 2, 5)))
	src.settle()

	before := src.managed(t)
	data := src.backend.Snapshot(t.Context())

	dst := newEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), data))

	dst.clock.Advance(src.clock.Now().Sub(dst.clock.Now()))
	dst.settle()

	m := dst.getMig(t)
	assert.Equal(t, int32(2), m.GetTargetSize())
	assert.True(t, m.GetStatus().GetIsStable())
	assert.Contains(t, m.GetStatus().GetAutoscaler(), "/autoscalers/as")

	after := dst.managed(t)
	require.Len(t, after, 2)
	assert.Equal(t, before[0].GetName(), after[0].GetName())
	assert.Equal(t, before[0].GetId(), after[0].GetId())

	_, err := dst.templates(t).
		Get(t.Context(), &computepb.GetInstanceTemplateRequest{Project: project, InstanceTemplate: "tpl"})
	require.NoError(t, err)

	_, err = dst.healthChecks(t).Get(t.Context(), &computepb.GetHealthCheckRequest{Project: project, HealthCheck: "hc"})
	require.NoError(t, err)

	_, err = dst.groups(t).
		Get(t.Context(), &computepb.GetInstanceGroupRequest{Project: project, Zone: zone, InstanceGroup: "ug"})
	require.NoError(t, err)

	_, err = dst.autoscalers(t).
		Get(t.Context(), &computepb.GetAutoscalerRequest{Project: project, Zone: zone, Autoscaler: "as"})
	require.NoError(t, err)

	_, err = dst.migs(t).Resize(t.Context(), &computepb.ResizeInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web", Size: 4,
	})
	require.NoError(t, err)
	assert.Len(t, dst.vmNames(t), 4, "a restored group keeps reconciling")

	dst.backend.Reset()
	assert.Empty(t, dst.vmNames(t))
}

func TestRestoreRejectsNullGroupRecords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
	}{
		{"null template", `{"templates":{"p/global/x":null},"version":1}`},
		{"null health check", `{"healthChecks":{"p/global/x":null},"version":1}`},
		{"null group manager", `{"instanceGroupManagers":{"p/z/x":null},"version":1}`},
		{"group manager without body", `{"instanceGroupManagers":{"p/z/x":{}},"version":1}`},
		{"group without body", `{"instanceGroups":{"p/z/x":{}},"version":1}`},
		{"null autoscaler", `{"autoscalers":{"p/z/x":null},"version":1}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, gcpcompute.NewInMemoryBackend().Restore(t.Context(), []byte(tt.payload)))
		})
	}
}
