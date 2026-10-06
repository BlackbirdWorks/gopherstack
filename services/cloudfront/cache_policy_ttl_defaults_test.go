package cloudfront_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCachePolicy_TTLDefaults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		def     *int64
		maxTTL  *int64
		name    string
		minTTL  int64
		wantDef int64
		wantMax int64
	}{
		{name: "omitted", wantDef: 86400, wantMax: 31536000},
		{name: "min above default", minTTL: 100000, wantDef: 100000, wantMax: 31536000},
		{name: "explicit", def: aws.Int64(10), maxTTL: aws.Int64(20), wantDef: 10, wantMax: 20},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestCloudFrontClient(t, newTestHandler(t))
			created, err := c.CreateCachePolicy(t.Context(), &cfsdk.CreateCachePolicyInput{
				CachePolicyConfig: &types.CachePolicyConfig{
					Name: aws.String("p"), MinTTL: aws.Int64(tc.minTTL), DefaultTTL: tc.def, MaxTTL: tc.maxTTL,
				},
			})
			require.NoError(t, err)

			got, err := c.GetCachePolicy(t.Context(), &cfsdk.GetCachePolicyInput{Id: created.CachePolicy.Id})
			require.NoError(t, err)
			assert.Equal(t, tc.wantDef, aws.ToInt64(got.CachePolicy.CachePolicyConfig.DefaultTTL))
			assert.Equal(t, tc.wantMax, aws.ToInt64(got.CachePolicy.CachePolicyConfig.MaxTTL))
		})
	}
}
