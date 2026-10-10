package s3_test

import (
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

func TestRequestMetrics_TagFilter(t *testing.T) {
	t.Parallel()

	const single = `<MetricsConfiguration><Id>m1</Id><Filter>` +
		`<Tag><Key>env</Key><Value>prod</Value></Tag></Filter></MetricsConfiguration>`

	const multi = `<MetricsConfiguration><Id>m1</Id><Filter><And><Prefix>k</Prefix>` +
		`<Tag><Key>env</Key><Value>prod</Value></Tag><Tag><Key>team</Key><Value>a</Value></Tag>` +
		`</And></Filter></MetricsConfiguration>`

	tests := []struct {
		name      string
		config    string
		tagged    []types.Tag
		wantCount int
	}{
		{
			name:      "single tag match",
			config:    single,
			tagged:    []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
			wantCount: 1,
		},
		{
			name:      "single tag mismatch",
			config:    single,
			tagged:    []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
			wantCount: 0,
		},
		{name: "untagged", config: single, wantCount: 0},
		{
			name:   "and all tags",
			config: multi,
			tagged: []types.Tag{
				{Key: aws.String("env"), Value: aws.String("prod")}, {Key: aws.String("team"), Value: aws.String("a")},
			},
			wantCount: 1,
		},
		{
			name:      "and partial tags",
			config:    multi,
			tagged:    []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			mustPutObject(t, backend, "bkt", "k1", []byte("data"))

			if tt.tagged != nil {
				_, err := backend.PutObjectTagging(t.Context(), &sdk_s3.PutObjectTaggingInput{
					Bucket: aws.String("bkt"), Key: aws.String("k1"), Tagging: &types.Tagging{TagSet: tt.tagged},
				})
				require.NoError(t, err)
			}

			var (
				mu        sync.Mutex
				count     int
				firstByte int
			)

			handler.SetMetricEmitter(cwmetric.EmitterFunc(func(p cwmetric.Point) error {
				mu.Lock()
				defer mu.Unlock()

				if p.Name == "AllRequests" {
					count++
				}

				if p.Name == "FirstByteLatency" {
					firstByte++
				}

				return nil
			}))

			rec := doRequest(handler, http.MethodPut, "/bkt?metrics&id=m1", strings.NewReader(tt.config), nil)
			require.Equal(t, http.StatusOK, rec.Code)

			mu.Lock()
			count, firstByte = 0, 0
			mu.Unlock()

			rec = doRequest(handler, http.MethodGet, "/bkt/k1", nil, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			mu.Lock()
			defer mu.Unlock()

			assert.Equal(t, tt.wantCount, count)
			assert.Equal(t, tt.wantCount, firstByte)
		})
	}
}
