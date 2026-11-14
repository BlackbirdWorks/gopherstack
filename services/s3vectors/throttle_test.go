package s3vectors_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3vectors"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteThrottling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantCode string
		name     string
		batches  int
		batch    int
	}{
		{name: "within vector rate", batches: 5, batch: 500},
		{name: "vectors over rate", batches: 6, batch: 500, wantCode: "TooManyRequestsException"},
		{name: "requests over rate", batches: 1001, batch: 1, wantCode: "TooManyRequestsException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.bucket(t, "thr-bucket")
			f.index(t, "thr-bucket", "thrx", "euclidean", 1)

			var tick atomic.Int64

			f.backend.SetClock(func() time.Time {
				return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Add(time.Duration(tick.Load()) * time.Second)
			})

			var last error

			for i := range tt.batches {
				vs := make([]types.PutInputVector, tt.batch)
				for j := range vs {
					vs[j] = vec(fmt.Sprintf("k%d-%d", i, j), nil, 1)
				}

				_, last = f.client.PutVectors(t.Context(), &sdk.PutVectorsInput{
					VectorBucketName: aws.String("thr-bucket"), IndexName: aws.String("thrx"), Vectors: vs,
				}, func(o *sdk.Options) { o.RetryMaxAttempts = 1 })
				if last != nil {
					break
				}
			}

			if tt.wantCode == "" {
				require.NoError(t, last)

				return
			}

			assert.Equal(t, tt.wantCode, apiCode(last))

			tick.Add(2)

			_, err := f.client.PutVectors(t.Context(), &sdk.PutVectorsInput{
				VectorBucketName: aws.String("thr-bucket"), IndexName: aws.String("thrx"),
				Vectors: []types.PutInputVector{vec("again", nil, 1)},
			})
			require.NoError(t, err, "a new second opens a new window")
		})
	}
}
