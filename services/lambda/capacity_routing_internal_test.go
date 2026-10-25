package lambda

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCapacityFixture(t *testing.T, maxVCpu *int32, perEnv *int32, memory int) *InMemoryBackend {
	t.Helper()

	b := NewInMemoryBackend(nil, nil, DefaultSettings(), "000000000000", "us-east-1")
	t.Cleanup(func() { b.Close(context.Background()) })

	cp, err := b.CreateCapacityProvider(&CreateCapacityProviderInput{
		CapacityProviderName:          "cp1",
		CapacityProviderScalingConfig: &CapacityProviderScalingConfig{MaxVCpuCount: maxVCpu},
	})
	require.NoError(t, err)

	require.NoError(t, b.CreateFunction(&FunctionConfiguration{
		FunctionName: "lmi", Runtime: "python3.12", Handler: "index.handler",
		Role: "arn:aws:iam::000000000000:role/r", MemorySize: memory,
		CapacityProviderConfig: &CapacityProviderConfig{ManagedInstances: &ManagedInstancesConfig{
			CapacityProviderArn:                   cp.CapacityProviderArn,
			PerExecutionEnvironmentMaxConcurrency: perEnv,
		}},
	}))

	return b
}

func TestCapacityRouting_PerEnvironmentConcurrency(t *testing.T) {
	t.Parallel()

	two, one, three := int32(2), int32(1), int32(3)

	tests := []struct {
		maxVCpu      *int32
		perEnv       *int32
		minEnvs      *int32
		maxEnvs      *int32
		name         string
		wantAdmitted int
		wantEnvs     int
	}{
		{name: "scales_out_per_env_limit", perEnv: &two, wantAdmitted: 20, wantEnvs: 10},
		{name: "max_envs_caps", perEnv: &two, minEnvs: &one, maxEnvs: &two, wantAdmitted: 4, wantEnvs: 2},
		{name: "provider_vcpu_budget", maxVCpu: &three, perEnv: &two, wantAdmitted: 6, wantEnvs: 3},
		{name: "provider_vcpu_budget_one", maxVCpu: &one, perEnv: &two, wantAdmitted: 2, wantEnvs: 1},
		{name: "deactivated", perEnv: &two, minEnvs: new(int32), maxEnvs: new(int32), wantAdmitted: 0, wantEnvs: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newCapacityFixture(t, tt.maxVCpu, tt.perEnv, 2048)

			if tt.minEnvs != nil || tt.maxEnvs != nil {
				_, err := b.PutFunctionScalingConfig("lmi", versionLatest, &PutFunctionScalingConfigInput{
					FunctionScalingConfig: &FunctionScalingConfig{
						MinExecutionEnvironments: tt.minEnvs, MaxExecutionEnvironments: tt.maxEnvs,
					},
				})
				require.NoError(t, err)
			}

			admitted := 0

			for range 20 {
				if _, err := b.acquireConcurrencySlot("lmi", versionLatest); err != nil {
					require.ErrorIs(t, err, ErrTooManyRequests)

					break
				}

				admitted++
			}

			assert.Equal(t, tt.wantAdmitted, admitted)

			envs, inFlight := b.CapacityEnvironments("lmi")
			assert.Equal(t, tt.wantEnvs, envs)
			assert.Equal(t, tt.wantAdmitted, inFlight)

			for range admitted {
				b.releaseConcurrencySlot("lmi")
			}

			_, inFlight = b.CapacityEnvironments("lmi")
			assert.Zero(t, inFlight)
		})
	}
}

func TestCapacityRouting_ProviderErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate  func(b *InMemoryBackend)
		wantErr error
		name    string
	}{
		{
			name:    "provider_missing",
			mutate:  func(b *InMemoryBackend) { b.capacityProviders.Delete("cp1") },
			wantErr: ErrFunctionNotFound,
		},
		{
			name: "provider_not_active",
			mutate: func(b *InMemoryBackend) {
				cp, _ := b.capacityProviders.Get("cp1")
				cp.State = CapacityProviderStateFailed
			},
			wantErr: ErrResourceNotReady,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newCapacityFixture(t, nil, nil, 2048)
			tt.mutate(b)

			_, err := b.acquireConcurrencySlot("lmi", versionLatest)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestCapacityRouting_VCpuFromMemoryRatio(t *testing.T) {
	t.Parallel()

	four := 4.0
	fn := &FunctionConfiguration{MemorySize: 8192}

	assert.Equal(t, 4, capacityEnvVCpu(fn, &ManagedInstancesConfig{}))
	assert.Equal(t, 2, capacityEnvVCpu(fn, &ManagedInstancesConfig{ExecutionEnvironmentMemoryGiBPerVCpu: &four}))
	assert.Equal(t, 1, capacityEnvVCpu(&FunctionConfiguration{MemorySize: 512}, &ManagedInstancesConfig{}))
}

func TestCapacityRouting_InvokeSurfacesProviderErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate  func(b *InMemoryBackend)
		wantErr error
		name    string
	}{
		{
			name:    "missing",
			mutate:  func(b *InMemoryBackend) { b.capacityProviders.Delete("cp1") },
			wantErr: ErrFunctionNotFound,
		},
		{
			name: "not_active",
			mutate: func(b *InMemoryBackend) {
				cp, _ := b.capacityProviders.Get("cp1")
				cp.State = CapacityProviderStateDeleting
			},
			wantErr: ErrResourceNotReady,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newCapacityFixture(t, nil, nil, 2048)
			tt.mutate(b)

			_, _, err := b.InvokeFunction(context.Background(), "lmi", InvocationTypeRequestResponse, []byte(`{}`))
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestCapacityDefaultConcurrencyPerRuntime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		runtime string
		want    int
	}{
		{"nodejs22.x", 128},
		{"java21", 64},
		{"dotnet8", 64},
		{"python3.13", 32},
		{"provided.al2023", math.MaxInt32},
	}

	for _, tt := range tests {
		t.Run(tt.runtime, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, capacityPerEnvConcurrency(&ManagedInstancesConfig{}, tt.runtime, 2))
		})
	}

	explicit := int32(5)
	assert.Equal(t, 5, capacityPerEnvConcurrency(
		&ManagedInstancesConfig{PerExecutionEnvironmentMaxConcurrency: &explicit}, "nodejs22.x", 2))
}
