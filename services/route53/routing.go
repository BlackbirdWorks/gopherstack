package route53

import (
	"crypto/rand"
	"encoding/binary"
	"math"
	"net"
	"sort"
	"strings"
)

// healthUnhealthy is the health-check status string that excludes a record from
// a DNS answer. All other statuses (including the "Healthy" default and an empty
// status) are treated as healthy for routing purposes.
const healthUnhealthy = "Unhealthy"

const (
	// maxAliasDepth bounds recursive in-zone alias resolution to prevent loops.
	maxAliasDepth = 8
	// maxMultiValueRecords is the number of records Route 53 returns for a
	// multivalue-answer query when more than that many healthy records exist.
	maxMultiValueRecords = 8
	// randShiftBits discards the low bits of a 64-bit draw to build a 53-bit
	// mantissa uniform in [0,1).
	randShiftBits = 11
	randMantissa  = 1 << 53
)

// AWS region identifiers referenced by the latency and geo-IP tables. Declared
// as constants so the string literals are not duplicated across the package.
const (
	regionUSEast2      = "us-east-2"
	regionUSWest1      = "us-west-1"
	regionUSWest2      = "us-west-2"
	regionCACentral1   = "ca-central-1"
	regionSAEast1      = "sa-east-1"
	regionEUWest1      = "eu-west-1"
	regionEUWest2      = "eu-west-2"
	regionEUWest3      = "eu-west-3"
	regionEUCentral1   = "eu-central-1"
	regionEUNorth1     = "eu-north-1"
	regionEUSouth1     = "eu-south-1"
	regionAFSouth1     = "af-south-1"
	regionMESouth1     = "me-south-1"
	regionAPSouth1     = "ap-south-1"
	regionAPSoutheast1 = "ap-southeast-1"
	regionAPSoutheast2 = "ap-southeast-2"
	regionAPNortheast1 = "ap-northeast-1"
	regionAPNortheast2 = "ap-northeast-2"
	regionAPNortheast3 = "ap-northeast-3"
	regionAPEast1      = "ap-east-1"
)

// Continent, country and subdivision codes referenced 3+ times by the geo-IP
// table (declared as constants to satisfy the no-duplicate-string rule).
const (
	continentNA   = "NA"
	continentEU   = "EU"
	continentAS   = "AS"
	continentOC   = "OC"
	continentSA   = "SA"
	continentAF   = "AF"
	countryUS     = "US"
	subdivisionVA = "VA"
)

// DNSQueryContext carries the client-side signals Route 53 uses to resolve
// routing-policy records: the client's AWS region (latency routing), the
// client's geographic location (geolocation routing) and the resolver IP.
// randFloat is an injectable source of pseudo-random floats in [0,1) used for
// weighted selection; when nil a crypto/rand-backed source is used. Tests set
// it to obtain deterministic weighted draws.
type DNSQueryContext struct {
	randFloat       func() float64
	ClientRegion    string
	ContinentCode   string
	CountryCode     string
	SubdivisionCode string
	ResolverIP      string
}

// rngFloat returns a float64 in [0,1) using the injected source, or a
// crypto/rand-backed value when none was provided.
func (q DNSQueryContext) rngFloat() float64 {
	if q.randFloat != nil {
		return q.randFloat()
	}

	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0
	}

	return float64(binary.BigEndian.Uint64(b[:])>>randShiftBits) / float64(randMantissa)
}

// geoIPEntry maps a public IP prefix to the AWS region and geolocation Route 53
// would associate with a resolver in that range.
type geoIPEntry struct {
	cidr        string
	region      string
	continent   string
	country     string
	subdivision string
}

//nolint:gochecknoglobals // static reference table initialized once at startup
var geoIPTable = []geoIPEntry{
	{"3.0.0.0/8", defaultRegion, continentNA, countryUS, subdivisionVA},
	{"18.0.0.0/8", defaultRegion, continentNA, countryUS, subdivisionVA},
	{"52.0.0.0/8", regionUSWest2, continentNA, countryUS, "OR"},
	{"54.0.0.0/8", regionUSWest1, continentNA, countryUS, "CA"},
	{"99.79.0.0/16", regionCACentral1, continentNA, "CA", "QC"},
	{"8.8.8.0/24", defaultRegion, continentNA, countryUS, "CA"},
	{"34.240.0.0/13", regionEUWest1, continentEU, "IE", ""},
	{"35.176.0.0/13", regionEUWest2, continentEU, "GB", ""},
	{"35.180.0.0/14", regionEUWest3, continentEU, "FR", ""},
	{"18.184.0.0/13", regionEUCentral1, continentEU, "DE", ""},
	{"13.48.0.0/14", regionEUNorth1, continentEU, "SE", ""},
	{"13.230.0.0/15", regionAPNortheast1, continentAS, "JP", ""},
	{"13.124.0.0/16", regionAPNortheast2, continentAS, "KR", ""},
	{"13.228.0.0/15", regionAPSoutheast1, continentAS, "SG", ""},
	{"13.54.0.0/15", regionAPSoutheast2, continentOC, "AU", "NSW"},
	{"13.232.0.0/14", regionAPSouth1, continentAS, "IN", ""},
	{"18.228.0.0/14", regionSAEast1, continentSA, "BR", ""},
	{"13.244.0.0/14", regionAFSouth1, continentAF, "ZA", ""},
}

// defaultGeoIP is returned when the resolver IP is empty or unrecognised.
//
//nolint:gochecknoglobals // static default paired with geoIPTable
var defaultGeoIP = geoIPEntry{
	region: defaultRegion, continent: continentNA, country: countryUS, subdivision: subdivisionVA,
}

// geoIPLookup maps a resolver/client IP to the AWS region and geolocation
// Route 53 associates with it. Unknown or empty IPs fall back to us-east-1 in
// the United States (a deterministic, documented approximation — not a full
// GeoIP database).
func geoIPLookup(ip string) (string, string, string, string) {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return defaultGeoIP.region, defaultGeoIP.continent, defaultGeoIP.country, defaultGeoIP.subdivision
	}

	for _, e := range geoIPTable {
		_, network, err := net.ParseCIDR(e.cidr)
		if err != nil {
			continue
		}
		if network.Contains(parsed) {
			return e.region, e.continent, e.country, e.subdivision
		}
	}

	return defaultGeoIP.region, defaultGeoIP.continent, defaultGeoIP.country, defaultGeoIP.subdivision
}

// regionCoord is an approximate geographic centre of an AWS region, used to
// rank latency-routing candidates by proximity to the client's region.
type regionCoord struct {
	lat float64
	lon float64
}

//nolint:gochecknoglobals // static reference table initialized once at startup
var awsRegionCoords = map[string]regionCoord{
	defaultRegion:      {38.13, -78.45},
	regionUSEast2:      {39.96, -83.00},
	regionUSWest1:      {37.35, -121.96},
	regionUSWest2:      {45.87, -119.69},
	regionCACentral1:   {45.50, -73.60},
	regionSAEast1:      {-23.55, -46.63},
	regionEUWest1:      {53.41, -8.24},
	regionEUWest2:      {51.51, -0.13},
	regionEUWest3:      {48.86, 2.35},
	regionEUCentral1:   {50.11, 8.68},
	regionEUNorth1:     {59.33, 18.06},
	regionEUSouth1:     {45.46, 9.19},
	regionAFSouth1:     {-33.92, 18.42},
	regionMESouth1:     {26.07, 50.55},
	regionAPSouth1:     {19.08, 72.88},
	regionAPSoutheast1: {1.35, 103.82},
	regionAPSoutheast2: {-33.87, 151.21},
	regionAPNortheast1: {35.69, 139.69},
	regionAPNortheast2: {37.57, 126.98},
	regionAPNortheast3: {34.69, 135.50},
	regionAPEast1:      {22.32, 114.17},
}

const (
	earthRadiusKm = 6371.0
	degToRad      = math.Pi / 180.0
	halfDivisor   = 2.0
)

// haversineKm returns the great-circle distance in kilometres between two
// lat/lon coordinates.
func haversineKm(a, b regionCoord) float64 {
	lat1 := a.lat * degToRad
	lat2 := b.lat * degToRad
	dLat := (b.lat - a.lat) * degToRad
	dLon := (b.lon - a.lon) * degToRad

	h := math.Sin(dLat/halfDivisor)*math.Sin(dLat/halfDivisor) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/halfDivisor)*math.Sin(dLon/halfDivisor)

	return halfDivisor * earthRadiusKm * math.Asin(math.Min(1, math.Sqrt(h)))
}

// recordHealthy reports whether a record set may be included in an answer.
// A record with no health check (or one referencing an unknown check) is
// always healthy; a record whose health check reports "Unhealthy" is excluded.
// The caller must hold at least a read lock.
func (b *InMemoryBackend) recordHealthy(rrs *ResourceRecordSet) bool {
	if rrs.HealthCheckID == "" {
		return true
	}

	hc, ok := b.healthChecks[rrs.HealthCheckID]
	if !ok {
		return true
	}

	return !strings.EqualFold(hc.Status, healthUnhealthy)
}

// routingKind classifies a set of candidate record sets into a single routing
// policy. AWS requires every record set sharing a name/type to use the same
// policy, so the first policy-bearing candidate determines the kind.
type routingKind int

const (
	routingSimple routingKind = iota
	routingWeighted
	routingLatency
	routingGeo
	routingFailover
	routingMultiValue
)

func classifyRouting(candidates []*ResourceRecordSet) routingKind {
	for _, rrs := range candidates {
		switch {
		case rrs.Weight != nil:
			return routingWeighted
		case rrs.Region != "":
			return routingLatency
		case rrs.GeoLocation != nil:
			return routingGeo
		case rrs.Failover != "":
			return routingFailover
		case rrs.MultiValueAnswer:
			return routingMultiValue
		}
	}

	return routingSimple
}

// filterHealthy returns the subset of candidates whose health checks are not
// reporting unhealthy, preserving order.
func (b *InMemoryBackend) filterHealthy(candidates []*ResourceRecordSet) []*ResourceRecordSet {
	healthy := make([]*ResourceRecordSet, 0, len(candidates))
	for _, rrs := range candidates {
		if b.recordHealthy(rrs) {
			healthy = append(healthy, rrs)
		}
	}

	return healthy
}

// sortBySetIdentifier returns a deterministically-ordered copy of candidates for
// stable tie-breaks. It copies the slice so callers never mutate a shared record
// set in place (concurrent DNS queries would otherwise race on the backing array).
func sortBySetIdentifier(candidates []*ResourceRecordSet) []*ResourceRecordSet {
	sorted := make([]*ResourceRecordSet, len(candidates))
	copy(sorted, candidates)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].SetIdentifier < sorted[j].SetIdentifier
	})

	return sorted
}

// selectWeighted picks one record set proportional to its Weight. Healthy
// candidates only. Weight 0 records receive no traffic unless every candidate
// has weight 0, in which case traffic is spread equally (AWS behaviour).
func selectWeighted(candidates []*ResourceRecordSet, qctx DNSQueryContext) *ResourceRecordSet {
	if len(candidates) == 0 {
		return nil
	}

	candidates = sortBySetIdentifier(candidates)

	var total int64
	var lastPositive *ResourceRecordSet
	for _, rrs := range candidates {
		if rrs.Weight != nil && *rrs.Weight > 0 {
			total += *rrs.Weight
			lastPositive = rrs
		}
	}

	// All-zero weights: equal probability across every candidate.
	if total == 0 {
		idx := int(qctx.rngFloat() * float64(len(candidates)))
		if idx >= len(candidates) {
			idx = len(candidates) - 1
		}

		return candidates[idx]
	}

	target := qctx.rngFloat() * float64(total)

	var cumulative float64
	for _, rrs := range candidates {
		if rrs.Weight == nil || *rrs.Weight <= 0 {
			continue
		}
		cumulative += float64(*rrs.Weight)
		if target < cumulative {
			return rrs
		}
	}

	// Floating-point edge at the very top of the range.
	return lastPositive
}

// selectLatency picks the healthy candidate whose region is geographically
// closest to the client's region. When the client region is unknown it defaults
// to us-east-1. Ties break on SetIdentifier.
func selectLatency(candidates []*ResourceRecordSet, qctx DNSQueryContext) *ResourceRecordSet {
	if len(candidates) == 0 {
		return nil
	}

	candidates = sortBySetIdentifier(candidates)

	clientCoord, ok := awsRegionCoords[qctx.ClientRegion]
	if !ok {
		clientCoord = awsRegionCoords[defaultRegion]
	}

	var best *ResourceRecordSet
	bestDist := math.MaxFloat64

	for _, rrs := range candidates {
		// Exact region match always wins (zero latency).
		if rrs.Region == qctx.ClientRegion {
			return rrs
		}

		dist := math.MaxFloat64
		if coord, found := awsRegionCoords[rrs.Region]; found {
			dist = haversineKm(clientCoord, coord)
		}

		if dist < bestDist {
			bestDist = dist
			best = rrs
		}
	}

	if best == nil {
		return candidates[0]
	}

	return best
}

// Geolocation match specificity scores. Higher is more specific.
const (
	geoScoreNone        = -1
	geoScoreDefault     = 0
	geoScoreContinent   = 1
	geoScoreCountry     = 2
	geoScoreSubdivision = 3
)

// geoSpecificity scores how closely a geolocation candidate matches the client,
// returning geoScoreNone when it does not match at all.
func geoSpecificity(loc *GeoLocation, qctx DNSQueryContext) int {
	if loc == nil {
		return geoScoreNone
	}

	// Default record ("Everywhere else") — Route 53 encodes it as CountryCode "*".
	if loc.CountryCode == "*" {
		return geoScoreDefault
	}

	switch {
	case loc.SubdivisionCode != "":
		if loc.CountryCode == qctx.CountryCode &&
			loc.SubdivisionCode == qctx.SubdivisionCode &&
			qctx.CountryCode != "" {
			return geoScoreSubdivision
		}
	case loc.CountryCode != "":
		if loc.CountryCode == qctx.CountryCode && qctx.CountryCode != "" {
			return geoScoreCountry
		}
	case loc.ContinentCode != "":
		if loc.ContinentCode == qctx.ContinentCode && qctx.ContinentCode != "" {
			return geoScoreContinent
		}
	}

	return geoScoreNone
}

// selectGeo picks the most specific geolocation candidate matching the client,
// falling back to the default ("*") record. Returns nil when nothing matches
// and no default exists (Route 53 returns no answer).
func selectGeo(candidates []*ResourceRecordSet, qctx DNSQueryContext) *ResourceRecordSet {
	if len(candidates) == 0 {
		return nil
	}

	candidates = sortBySetIdentifier(candidates)

	var best *ResourceRecordSet
	bestScore := geoScoreNone

	for _, rrs := range candidates {
		score := geoSpecificity(rrs.GeoLocation, qctx)
		if score > bestScore {
			bestScore = score
			best = rrs
		}
	}

	if bestScore == geoScoreNone {
		return nil
	}

	return best
}

// selectFailover returns the PRIMARY record when it is healthy, otherwise the
// SECONDARY record. Candidates passed in must already be health-filtered so an
// unhealthy PRIMARY is absent; this selects among what remains.
func selectFailover(healthy []*ResourceRecordSet) *ResourceRecordSet {
	if len(healthy) == 0 {
		return nil
	}

	healthy = sortBySetIdentifier(healthy)

	var secondary *ResourceRecordSet
	for _, rrs := range healthy {
		if rrs.Failover == FailoverPrimary {
			return rrs
		}
		if rrs.Failover == FailoverSecondary && secondary == nil {
			secondary = rrs
		}
	}

	if secondary != nil {
		return secondary
	}

	return healthy[0]
}
