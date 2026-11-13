package ocicore_test

import (
	"sync"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrentCreateRetryToken(t *testing.T) {
	t.Parallel()

	const workers = 16

	tests := []struct {
		create func(t *testing.T, e *env) (string, error)
		count  func(t *testing.T, e *env) int
		name   string
	}{
		{
			name: "vcn",
			create: func(t *testing.T, e *env) (string, error) {
				t.Helper()

				r, err := e.client.CreateVcn(t.Context(), core.CreateVcnRequest{
					OpcRetryToken: new("race"),
					CompartmentId: new(testComp), CidrBlocks: []string{"10.0.0.0/16"}, DisplayName: new("rv"),
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
			count: func(t *testing.T, e *env) int {
				t.Helper()

				r, err := e.client.ListVcns(t.Context(), core.ListVcnsRequest{CompartmentId: new(testComp)})
				require.NoError(t, err)

				return len(r.Items)
			},
		},
		{
			name: "volume",
			create: func(t *testing.T, e *env) (string, error) {
				t.Helper()

				r, err := e.block.CreateVolume(t.Context(), core.CreateVolumeRequest{
					OpcRetryToken: new("race"),
					CompartmentId: new(testComp), AvailabilityDomain: new(testAD), SizeInGBs: new(int64(50)),
					DisplayName: new("rvol"),
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
			count: func(t *testing.T, e *env) int {
				t.Helper()

				r, err := e.block.ListVolumes(t.Context(), core.ListVolumesRequest{CompartmentId: new(testComp)})
				require.NoError(t, err)

				return len(r.Items)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			ids := make([]string, workers)
			errs := make([]error, workers)
			start := make(chan struct{})

			var wg sync.WaitGroup

			for i := range workers {
				wg.Go(func() {
					<-start

					ids[i], errs[i] = tt.create(t, e)
				})
			}

			close(start)
			wg.Wait()

			for i := range workers {
				require.NoError(t, errs[i])
				assert.Equal(t, ids[0], ids[i])
			}

			assert.Equal(t, 1, tt.count(t, e))
		})
	}
}
