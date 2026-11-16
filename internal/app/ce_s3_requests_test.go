package app

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdks3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const meterAllocRuns = 1000

//nolint:paralleltest // testing.AllocsPerRun panics under t.Parallel
func TestS3RequestMeterObserveAllocatesNothing(t *testing.T) {
	tests := []struct {
		name string
		ev   s3backend.RequestEvent
	}{
		{name: "get", ev: s3backend.RequestEvent{
			Bucket: "b", Method: http.MethodGet, Operation: "GetObject", Status: http.StatusOK, HasKey: true,
		}},
		{name: "put", ev: s3backend.RequestEvent{
			Bucket: "b", Method: http.MethodPut, Operation: "PutObject", Status: http.StatusOK, HasKey: true,
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := s3backend.NewInMemoryBackend(nil)
			_, err := mem.CreateBucket(t.Context(), &sdks3.CreateBucketInput{Bucket: aws.String("b")})
			require.NoError(t, err)

			m := &s3RequestMeter{mem: mem, lock: lockmetrics.New("test")}
			m.observe(tc.ev)

			allocs := testing.AllocsPerRun(meterAllocRuns, func() { m.observe(tc.ev) })
			assert.Zero(t, allocs)

			got := m.drain()
			require.Len(t, got, 1)
			assert.InDelta(t, float64(meterAllocRuns+2), got[0].Quantity, 0)
			assert.Empty(t, m.drain())
		})
	}
}
