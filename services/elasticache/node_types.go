package elasticache

import "sort"

// nodeTypeMemoryGiB is Amazon ElastiCache's documented current-generation
// node types and their memory sizes, from
// https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/CacheNodes.SupportedTypes.html
// (T3/T4g general purpose, M5/M6g/M7g general purpose, R5/R6g/R7g memory
// optimized -- the families common to Memcached, Valkey and Redis OSS).
//
//nolint:gochecknoglobals,mnd // read-only lookup table of AWS-documented GiB sizes, initialized once at package load
var nodeTypeMemoryGiB = map[string]float64{
	"cache.t3.micro":   0.50,
	"cache.t3.small":   1.37,
	"cache.t3.medium":  3.09,
	"cache.t4g.micro":  0.50,
	"cache.t4g.small":  1.37,
	"cache.t4g.medium": 3.09,

	"cache.m5.large":   6.38,
	"cache.m5.xlarge":  12.93,
	"cache.m5.2xlarge": 26.04,
	"cache.m5.4xlarge": 52.26,

	"cache.m6g.large":   6.38,
	"cache.m6g.xlarge":  12.93,
	"cache.m6g.2xlarge": 26.04,
	"cache.m6g.4xlarge": 52.26,

	"cache.m7g.large":   6.38,
	"cache.m7g.xlarge":  12.93,
	"cache.m7g.2xlarge": 26.04,
	"cache.m7g.4xlarge": 52.26,

	"cache.r5.large":   13.07,
	"cache.r5.xlarge":  26.32,
	"cache.r5.2xlarge": 52.82,
	"cache.r5.4xlarge": 105.81,

	"cache.r6g.large":   13.07,
	"cache.r6g.xlarge":  26.32,
	"cache.r6g.2xlarge": 52.82,
	"cache.r6g.4xlarge": 105.81,

	"cache.r7g.large":   13.07,
	"cache.r7g.xlarge":  26.32,
	"cache.r7g.2xlarge": 52.82,
	"cache.r7g.4xlarge": 105.81,
}

// allowedNodeTypeModifications splits nodeTypeMemoryGiB into node types with
// more memory than current (scale-up) and less memory (scale-down), each
// ascending by memory size. ok is false when current isn't in the catalog,
// in which case both lists are empty -- an unrecognized node type has no
// known modifications rather than being an error.
func allowedNodeTypeModifications(current string) ([]string, []string, bool) {
	currentMem, found := nodeTypeMemoryGiB[current]
	if !found {
		return nil, nil, false
	}

	var scaleUp, scaleDown []string
	for name, mem := range nodeTypeMemoryGiB {
		switch {
		case mem > currentMem:
			scaleUp = append(scaleUp, name)
		case mem < currentMem:
			scaleDown = append(scaleDown, name)
		}
	}

	sortByMemory(scaleUp)
	sortByMemory(scaleDown)

	return scaleUp, scaleDown, true
}

// Families tie exactly (m5/m6g/m7g.large are all 6.38 GiB), and the caller
// builds names by map iteration, so name is the tiebreak that keeps the
// response order stable run to run.
func sortByMemory(names []string) {
	sort.Slice(names, func(i, j int) bool {
		if mi, mj := nodeTypeMemoryGiB[names[i]], nodeTypeMemoryGiB[names[j]]; mi != mj {
			return mi < mj
		}

		return names[i] < names[j]
	})
}
