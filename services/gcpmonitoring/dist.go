package gcpmonitoring

import (
	"math"

	distpb "google.golang.org/genproto/googleapis/api/distribution"
)

// bucketCount is the number of buckets the options imply, or -1 when no options are set.
func bucketCount(d *distpb.Distribution) int {
	switch o := d.GetBucketOptions().GetOptions().(type) {
	case *distpb.Distribution_BucketOptions_ExplicitBuckets:
		return len(o.ExplicitBuckets.GetBounds()) + 1
	case *distpb.Distribution_BucketOptions_LinearBuckets:
		return int(o.LinearBuckets.GetNumFiniteBuckets()) + overflowBuckets
	case *distpb.Distribution_BucketOptions_ExponentialBuckets:
		return int(o.ExponentialBuckets.GetNumFiniteBuckets()) + overflowBuckets
	}

	return -1
}

// bucketBounds returns the [lower, upper) bounds of bucket i; the open ends repeat the nearest finite bound.
func bucketBounds(d *distpb.Distribution, i int) (float64, float64) {
	n := bucketCount(d)
	if n < 0 {
		return 0, 0
	}

	lower, upper := edge(d, i-1), edge(d, i)
	if i == 0 {
		lower = upper
	}

	if i == n-1 {
		upper = lower
	}

	return lower, upper
}

// edge is the upper boundary of bucket k (the lower boundary of bucket k+1).
func edge(d *distpb.Distribution, k int) float64 {
	switch o := d.GetBucketOptions().GetOptions().(type) {
	case *distpb.Distribution_BucketOptions_ExplicitBuckets:
		bounds := o.ExplicitBuckets.GetBounds()
		if k < 0 {
			k = 0
		}

		if k >= len(bounds) {
			k = len(bounds) - 1
		}

		return bounds[k]
	case *distpb.Distribution_BucketOptions_LinearBuckets:
		l := o.LinearBuckets

		return l.GetOffset() + l.GetWidth()*float64(max(k, 0))
	case *distpb.Distribution_BucketOptions_ExponentialBuckets:
		e := o.ExponentialBuckets

		return e.GetScale() * math.Pow(e.GetGrowthFactor(), float64(max(k, 0)))
	}

	return 0
}

// percentile estimates the p-th percentile (0..100) by interpolating inside the bucket holding the rank.
func percentile(d *distpb.Distribution, p float64) (float64, bool) {
	counts := d.GetBucketCounts()
	if d.GetCount() == 0 || len(counts) == 0 || bucketCount(d) != len(counts) {
		return 0, false
	}

	rank := p / percentScale * float64(d.GetCount())

	var seen float64

	for i, c := range counts {
		if c == 0 {
			continue
		}

		if seen+float64(c) >= rank {
			lo, hi := bucketBounds(d, i)

			return lo + (hi-lo)*(rank-seen)/float64(c), true
		}

		seen += float64(c)
	}

	lo, _ := bucketBounds(d, len(counts)-1)

	return lo, true
}

const (
	// overflowBuckets counts the underflow and overflow buckets around the finite ones.
	overflowBuckets = 2
	latencyBuckets  = 64
	latencyGrowth   = 1.4
	latencyScale    = 1.0
)

// latencyDistribution builds a one-sample distribution with the exponential buckets uptime checks use.
func latencyDistribution(ms float64) *distpb.Distribution {
	opts := &distpb.Distribution_BucketOptions{Options: &distpb.Distribution_BucketOptions_ExponentialBuckets{
		ExponentialBuckets: &distpb.Distribution_BucketOptions_Exponential{
			NumFiniteBuckets: latencyBuckets, GrowthFactor: latencyGrowth, Scale: latencyScale,
		},
	}}
	counts := make([]int64, latencyBuckets+overflowBuckets)

	idx := 0

	if ms >= latencyScale {
		idx = min(int(math.Floor(math.Log(ms/latencyScale)/math.Log(latencyGrowth)))+1, latencyBuckets+1)
	}

	counts[idx] = 1

	return &distpb.Distribution{
		Count: 1, Mean: ms, BucketOptions: opts, BucketCounts: counts,
		Range: &distpb.Distribution_Range{Min: ms, Max: ms},
	}
}
