package app

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/awswire/kinesis"
)

func TestAwaitStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bodies   []string
		status   []int
		gone     bool
		wantErr  bool
		wantCall int32
	}{
		{
			name:     "active_after_creating",
			bodies:   []string{creating, creating, active},
			status:   []int{200, 200, 200},
			wantCall: 3,
		},
		{
			name:     "gone",
			gone:     true,
			bodies:   []string{active, notFound},
			status:   []int{200, 400},
			wantCall: 2,
		},
		{
			name:     "other_error_stops",
			gone:     true,
			bodies:   []string{`{"__type":"AccessDeniedException","message":"no"}`},
			status:   []int{400},
			wantErr:  true,
			wantCall: 1,
		},
		{
			name:    "times_out",
			bodies:  []string{creating},
			status:  []int{200},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				var n atomic.Int32

				rt := roundTripFunc(func(*http.Request) (*http.Response, error) {
					i := min(int(n.Add(1))-1, len(tt.bodies)-1)

					return &http.Response{
						StatusCode: tt.status[i],
						Header:     http.Header{"Content-Type": {"application/x-amz-json-1.1"}},
						Body:       io.NopCloser(strings.NewReader(tt.bodies[i])),
					}, nil
				})

				c := kinesis.NewFromConfig(aws.Config{
					Region: "us-east-1", BaseEndpoint: aws.String("http://kinesis.invalid"),
					HTTPClient:  &http.Client{Transport: rt},
					Credentials: credentials.NewStaticCredentialsProvider("a", "b", ""),
				})

				err := awaitStream(context.Background(), c, "s", tt.gone, 300*time.Millisecond)
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}

				if tt.wantCall > 0 {
					assert.Equal(t, tt.wantCall, n.Load())
				}
			})
		})
	}
}

const (
	creating = `{"StreamDescription":{"StreamStatus":"CREATING"}}`
	active   = `{"StreamDescription":{"StreamStatus":"ACTIVE"}}`
	notFound = `{"__type":"ResourceNotFoundException","message":"gone"}`
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
