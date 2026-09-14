package opensearch

import (
	"strconv"
	"strings"
)

// Engine version list and in-place upgrade compatibility, per AWS's own
// documentation (fetched 2026-09-11):
//   - supported versions:
//     https://docs.aws.amazon.com/opensearch-service/latest/developerguide/what-is.html#choosing-version
//   - upgrade paths ("Supported upgrade paths" section):
//     https://docs.aws.amazon.com/opensearch-service/latest/developerguide/version-migration.html
//
// ListVersions/GetCompatibleVersions both read from this one table instead of
// each carrying their own invented catalog.

// openSearchVersionNumbers and elasticsearchVersionNumbers are exactly the
// version numbers (without the "OpenSearch_"/"Elasticsearch_" EngineVersion
// prefix) the "Supported versions" doc section above lists, in AWS's own
// listed (newest-first) order.
//
//nolint:gochecknoglobals // fixed lookup table, mirrors errCodeLookup-style tables elsewhere (persistence.go)
var openSearchVersionNumbers = []string{
	"3.5", "3.3", "3.1",
	"2.19", "2.17", "2.15", "2.13", "2.11", "2.9", "2.7", "2.5", "2.3",
	"1.3", "1.2", "1.1", "1.0",
}

//nolint:gochecknoglobals // fixed lookup table, mirrors errCodeLookup-style tables elsewhere (persistence.go)
var elasticsearchVersionNumbers = []string{
	"7.10", "7.9", "7.8", "7.7", "7.4", "7.1",
	"6.8", "6.7", "6.5", "6.4", "6.3", "6.2", "6.0",
	"5.6", "5.5", "5.3", "5.1",
	"2.3", "1.5",
}

const (
	engineFamilyOpenSearch    = "OpenSearch"
	engineFamilyElasticsearch = "Elasticsearch"
)

// allSupportedEngineVersions returns every EngineVersion string
// ("OpenSearch_X.Y"/"Elasticsearch_X.Y") this table documents, OpenSearch
// versions first. The real ListVersions response order is not documented by
// the SDK; this is a stable pick, not a claim about AWS's own ordering.
func allSupportedEngineVersions() []string {
	out := make([]string, 0, len(openSearchVersionNumbers)+len(elasticsearchVersionNumbers))
	for _, v := range openSearchVersionNumbers {
		out = append(out, engineFamilyOpenSearch+"_"+v)
	}

	for _, v := range elasticsearchVersionNumbers {
		out = append(out, engineFamilyElasticsearch+"_"+v)
	}

	return out
}

// engineVersionNum is a parsed family + major.minor engine version.
type engineVersionNum struct {
	family string
	major  int
	minor  int
}

func parseEngineVersionNum(v string) (engineVersionNum, bool) {
	family, num, ok := strings.Cut(v, "_")
	if !ok || (family != engineFamilyOpenSearch && family != engineFamilyElasticsearch) {
		return engineVersionNum{}, false
	}

	majorStr, minorStr, ok := strings.Cut(num, ".")
	if !ok {
		return engineVersionNum{}, false
	}

	major, majErr := strconv.Atoi(majorStr)
	minor, minErr := strconv.Atoi(minorStr)

	if majErr != nil || minErr != nil {
		return engineVersionNum{}, false
	}

	return engineVersionNum{family: family, major: major, minor: minor}, true
}

func (v engineVersionNum) after(o engineVersionNum) bool {
	if v.major != o.major {
		return v.major > o.major
	}

	return v.minor > o.minor
}

func (v engineVersionNum) String() string {
	return v.family + "_" + strconv.Itoa(v.major) + "." + strconv.Itoa(v.minor)
}

func versionNumsInFamily(family string) []engineVersionNum {
	nums := openSearchVersionNumbers
	if family == engineFamilyElasticsearch {
		nums = elasticsearchVersionNumbers
	}

	out := make([]engineVersionNum, 0, len(nums))

	for _, n := range nums {
		v, _ := parseEngineVersionNum(family + "_" + n)
		out = append(out, v)
	}

	return out
}

func filterVersions(all []engineVersionNum, keep func(engineVersionNum) bool) []string {
	out := []string{}

	for _, v := range all {
		if keep(v) {
			out = append(out, v.String())
		}
	}

	return out
}

// compatibleTargetVersions returns the EngineVersions sourceVersion can be
// upgraded to in a single in-place upgrade, per the "Supported upgrade paths"
// table cited above. Multi-hop paths AWS documents as requiring an
// intermediate version first (e.g. OpenSearch 2.x -> 3.x needs 2.19 first)
// are intentionally excluded: this models direct compatibility, not
// reachability through a chain of upgrades.
func compatibleTargetVersions(sourceVersion string) []string {
	src, ok := parseEngineVersionNum(sourceVersion)
	if !ok {
		return []string{}
	}

	if src.family == engineFamilyOpenSearch {
		return compatibleOpenSearchTargets(src)
	}

	return compatibleElasticsearchTargets(src)
}

// Major version numbers named for the upgrade-path rules below (see the
// file-level citation) -- not arbitrary magic numbers.
const (
	majorOpenSearch1    = 1
	majorOpenSearch2    = 2
	majorOpenSearch3    = 3
	majorElasticsearch5 = 5
	majorElasticsearch6 = 6
	majorElasticsearch7 = 7

	// minorOpenSearch219 is the documented required stepping stone to 3.x.
	minorOpenSearch219 = 19
	// minorOpenSearch13 is the highest OpenSearch 1.x release, documented as
	// able to jump directly to any 2.x.
	minorOpenSearch13 = 3
	// minorElasticsearch68 is the last Elasticsearch 6.x release, documented
	// as able to jump directly to any 7.x (or OpenSearch 1.x).
	minorElasticsearch68 = 8
	// minorElasticsearch56 is the last Elasticsearch 5.x release, documented
	// as able to jump directly to any 6.x.
	minorElasticsearch56 = 6
)

func compatibleOpenSearchTargets(src engineVersionNum) []string {
	all := versionNumsInFamily(engineFamilyOpenSearch)

	switch {
	case src.major == majorOpenSearch3:
		return filterVersions(all, func(v engineVersionNum) bool {
			return v.major == majorOpenSearch3 && v.after(src)
		})
	case src.major == majorOpenSearch2 && src.minor == minorOpenSearch219:
		// 2.19 is the documented required stepping stone to 3.x, so unlike
		// other 2.x versions it can go directly to 3.x too.
		return filterVersions(all, func(v engineVersionNum) bool {
			return (v.major == majorOpenSearch2 && v.after(src)) || v.major == majorOpenSearch3
		})
	case src.major == majorOpenSearch2:
		return filterVersions(all, func(v engineVersionNum) bool {
			return v.major == majorOpenSearch2 && v.after(src)
		})
	case src.major == majorOpenSearch1 && src.minor == minorOpenSearch13:
		// "OpenSearch 1.3 or 2.x -> OpenSearch 2.x": 1.3 can jump directly to
		// any 2.x, not just the next one up.
		return filterVersions(all, func(v engineVersionNum) bool { return v.major == majorOpenSearch2 })
	case src.major == majorOpenSearch1:
		return filterVersions(all, func(v engineVersionNum) bool {
			return v.major == majorOpenSearch1 && v.after(src)
		})
	default:
		return []string{}
	}
}

func compatibleElasticsearchTargets(src engineVersionNum) []string {
	es := versionNumsInFamily(engineFamilyElasticsearch)
	openSearch1x := filterVersions(versionNumsInFamily(engineFamilyOpenSearch), func(v engineVersionNum) bool {
		return v.major == majorOpenSearch1
	})

	switch {
	case src.major == majorElasticsearch7:
		return append(
			filterVersions(es, func(v engineVersionNum) bool {
				return v.major == majorElasticsearch7 && v.after(src)
			}),
			openSearch1x...,
		)
	case src.major == majorElasticsearch6 && src.minor == minorElasticsearch68:
		return append(
			filterVersions(es, func(v engineVersionNum) bool { return v.major == majorElasticsearch7 }),
			openSearch1x...,
		)
	case src.major == majorElasticsearch6:
		return filterVersions(es, func(v engineVersionNum) bool {
			return v.major == majorElasticsearch6 && v.after(src)
		})
	case src.major == majorElasticsearch5 && src.minor == minorElasticsearch56:
		return filterVersions(es, func(v engineVersionNum) bool { return v.major == majorElasticsearch6 })
	case src.major == majorElasticsearch5:
		return filterVersions(es, func(v engineVersionNum) bool {
			return v.major == majorElasticsearch5 && v.after(src)
		})
	default:
		// Elasticsearch 2.3 and 1.5 predate the documented in-place-upgrade
		// floor ("offers in-place upgrades for domains that run OpenSearch
		// 1.0 or later, or Elasticsearch 5.1 or later") -- no compatible
		// target, not a gap.
		return []string{}
	}
}
