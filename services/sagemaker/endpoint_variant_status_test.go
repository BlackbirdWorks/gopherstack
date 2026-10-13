package sagemaker //nolint:testpackage // needs transition delay constants

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEndpointVariantStatus_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act        func(t *testing.T, b *InMemoryBackend, name string)
		name       string
		wantStatus string
	}{
		{
			name:       "create",
			wantStatus: "Creating",
			act: func(t *testing.T, b *InMemoryBackend, name string) {
				t.Helper()

				_, err := b.CreateEndpointFSM(context.Background(), CreateEndpointOptions{
					Name: name, EndpointConfigName: "cfg",
				})
				require.NoError(t, err)
			},
		},
		{
			name:       "update",
			wantStatus: "Updating",
			act: func(t *testing.T, b *InMemoryBackend, name string) {
				t.Helper()

				_, err := b.CreateEndpointFSM(context.Background(), CreateEndpointOptions{
					Name: name, EndpointConfigName: "cfg",
				})
				require.NoError(t, err)

				time.Sleep(endpointCreatingToInService + time.Millisecond)
				synctest.Wait()

				_, err = b.UpdateEndpointFSM(context.Background(), name, UpdateEndpointOptions{
					EndpointConfigName: "cfg",
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := NewInMemoryBackend("000000000000", "us-east-1")
				defer b.Shutdown(context.Background())

				seedEndpointConfig(t, b, "cfg")
				tt.act(t, b, "ep")

				ep, err := b.DescribeEndpoint(context.Background(), "ep")
				require.NoError(t, err)
				require.Len(t, ep.ProductionVariants[0].VariantStatus, 1)
				assert.Equal(t, tt.wantStatus, ep.ProductionVariants[0].VariantStatus[0].Status)
				assert.NotZero(t, ep.ProductionVariants[0].VariantStatus[0].StartTime)

				time.Sleep(endpointUpdatingToInService + endpointCreatingToInService + time.Millisecond)
				synctest.Wait()

				ep, err = b.DescribeEndpoint(context.Background(), "ep")
				require.NoError(t, err)
				assert.Equal(t, statusInService, ep.EndpointStatus)
				assert.Empty(t, ep.ProductionVariants[0].VariantStatus)
			})
		})
	}
}

func TestEndpointServerlessConfig_DesiredThenCurrent(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := NewInMemoryBackend("000000000000", "us-east-1")
		defer b.Shutdown(context.Background())

		_, err := b.CreateEndpointConfig(context.Background(), "cfg", []ProductionVariant{{
			VariantName: "v1", ModelName: "m", InitialVariantWeight: 1,
			ServerlessConfig: &ServerlessConfig{MemorySizeInMB: 2048, MaxConcurrency: 5},
		}}, nil)
		require.NoError(t, err)

		_, err = b.CreateEndpointFSM(context.Background(), CreateEndpointOptions{Name: "ep", EndpointConfigName: "cfg"})
		require.NoError(t, err)

		ep, err := b.DescribeEndpoint(context.Background(), "ep")
		require.NoError(t, err)
		require.NotNil(t, ep.ProductionVariants[0].DesiredServerless)
		assert.Equal(t, int32(2048), ep.ProductionVariants[0].DesiredServerless.MemorySizeInMB)
		assert.Nil(t, ep.ProductionVariants[0].CurrentServerless)

		time.Sleep(endpointCreatingToInService + time.Millisecond)
		synctest.Wait()

		ep, err = b.DescribeEndpoint(context.Background(), "ep")
		require.NoError(t, err)
		require.NotNil(t, ep.ProductionVariants[0].CurrentServerless)
		assert.Equal(t, int32(5), ep.ProductionVariants[0].CurrentServerless.MaxConcurrency)
	})
}
