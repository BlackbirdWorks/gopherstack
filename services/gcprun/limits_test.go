package gcprun_test

import (
	"testing"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestService_ResourceLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		limits map[string]string
		name   string
		mem    int64
		cpu    int64
		code   codes.Code
	}{
		{name: "binary and cores", limits: map[string]string{"cpu": "2", "memory": "512Mi"}, mem: 512 << 20, cpu: 2e9},
		{name: "millicores", limits: map[string]string{"cpu": "500m", "memory": "1G"}, mem: 1e9, cpu: 5e8},
		{name: "fraction", limits: map[string]string{"cpu": "0.5"}, cpu: 5e8},
		{name: "gpu ignored", limits: map[string]string{"nvidia.com/gpu": "1"}},
		{name: "bad cpu", limits: map[string]string{"cpu": "fast"}, code: codes.InvalidArgument},
		{name: "bad memory", limits: map[string]string{"memory": "12Xi"}, code: codes.InvalidArgument},
		{name: "zero memory", limits: map[string]string{"memory": "0"}, code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			svc := svcSpec("img")
			svc.Template.Containers[0].Resources = &runpb.ResourceRequirements{Limits: tt.limits}

			op, err := e.services.CreateService(t.Context(), &runpb.CreateServiceRequest{
				Parent: parent, ServiceId: "app", Service: svc,
			})
			if tt.code != codes.OK {
				assert.Equal(t, tt.code, status.Code(err))

				return
			}

			require.NoError(t, err)

			_, err = op.Wait(t.Context())
			require.NoError(t, err)

			specs := e.rt.specs()
			require.NotEmpty(t, specs)
			assert.Equal(t, tt.mem, specs[0].MemoryBytes)
			assert.Equal(t, tt.cpu, specs[0].NanoCPUs)
		})
	}
}

func TestRunJob_ResourceLimits(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	spec := jobSpec("img", 1, 1, 0)
	spec.Template.Template.Containers[0].Resources = &runpb.ResourceRequirements{
		Limits: map[string]string{"cpu": "1", "memory": "256Mi"},
	}
	job := e.createJob(t, "capped", spec)

	op, err := e.jobs.RunJob(t.Context(), &runpb.RunJobRequest{Name: job.GetName()})
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	require.NoError(t, err)

	specs := e.rt.specs()
	require.Len(t, specs, 1)
	assert.Equal(t, int64(256<<20), specs[0].MemoryBytes)
	assert.Equal(t, int64(1e9), specs[0].NanoCPUs)
}
