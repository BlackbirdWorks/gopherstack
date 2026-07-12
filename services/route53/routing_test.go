package route53

import (
	"math"
	"strconv"
	"testing"
)

// i64 returns a pointer to an int64 literal (weights are *int64).
func i64(v int64) *int64 {
	p := new(int64)
	*p = v

	return p
}

// seededRandFloat returns a deterministic [0,1) source backed by a small
// linear-congruential generator (Knuth MMIX constants). Production code uses
// crypto/rand or an injected source; tests inject this LCG so weighted
// selection is reproducible without depending on math/rand.
func seededRandFloat(seed int64) func() float64 {
	state := uint64(seed) + 0x9E3779B97F4A7C15

	return func() float64 {
		state = state*6364136223846793005 + 1442695040888963407
		// Use the top 53 bits for a uniform value in [0,1).
		const mantissa = 1 << 53

		return float64(state>>11) / float64(mantissa)
	}
}

// newTestZone spins up a backend with a single hosted zone and returns both.
func newTestZone(t *testing.T) (*InMemoryBackend, string) {
	t.Helper()

	b := NewInMemoryBackend()
	hz, err := b.CreateHostedZone("example.com", "ref-"+t.Name(), "", false, "")
	if err != nil {
		t.Fatalf("CreateHostedZone: %v", err)
	}

	return b, hz.ID
}

// addRecords applies a batch of CREATE changes, failing the test on error.
func addRecords(t *testing.T, b *InMemoryBackend, zoneID string, sets ...ResourceRecordSet) {
	t.Helper()

	changes := make([]Change, len(sets))
	for i, rrs := range sets {
		changes[i] = Change{Action: ChangeActionCreate, ResourceRecordSet: rrs}
	}

	if _, err := b.ChangeResourceRecordSets(zoneID, changes); err != nil {
		t.Fatalf("ChangeResourceRecordSets: %v", err)
	}
}

// newHealthCheck creates a health check and returns its ID.
func newHealthCheck(t *testing.T, b *InMemoryBackend, status string) string {
	t.Helper()

	hc, err := b.CreateHealthCheck("ref-hc-"+t.Name()+"-"+status, HealthCheckConfig{
		Type:             HealthCheckTypeHTTP,
		IPAddress:        "192.0.2.1",
		Port:             80,
		ResourcePath:     "/",
		RequestInterval:  30,
		FailureThreshold: 3,
	})
	if err != nil {
		t.Fatalf("CreateHealthCheck: %v", err)
	}

	if setErr := b.SetHealthCheckStatus(hc.ID, status); setErr != nil {
		t.Fatalf("SetHealthCheckStatus: %v", setErr)
	}

	return hc.ID
}

func TestValidateCharacterStrings(t *testing.T) {
	t.Parallel()

	long := `"` + string(make([]byte, 300)) + `"` // 300-octet char-string > 255

	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "simple_quoted", value: `"v=spf1 -all"`},
		{name: "multi_string", value: `"chunk-a" "chunk-b"`},
		{name: "escaped_quote", value: `"say \"hi\""`},
		{name: "empty", value: "", wantErr: true},
		{name: "unquoted", value: "v=spf1 -all", wantErr: true},
		{name: "missing_close", value: `"open`, wantErr: true},
		{name: "unquoted_between", value: `"a" bad "b"`, wantErr: true},
		{name: "over_255", value: long, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ok := isValidCharacterStrings(tt.value)
			if tt.wantErr && ok {
				t.Fatalf("want invalid, got valid for %q", tt.value)
			}
			if !tt.wantErr && !ok {
				t.Fatalf("want valid, got invalid for %q", tt.value)
			}
		})
	}
}

func TestValidateRecordValueNewTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rrType  string
		value   string
		wantErr bool
	}{
		{name: "txt_ok", rrType: recordTypeTXT, value: `"hello world"`},
		{name: "txt_unquoted", rrType: recordTypeTXT, value: "hello", wantErr: true},
		{name: "spf_ok", rrType: recordTypeSPF, value: `"v=spf1 mx -all"`},
		{name: "spf_unquoted", rrType: recordTypeSPF, value: "v=spf1", wantErr: true},
		{name: "cname_ok", rrType: recordTypeCNAME, value: "target.example.com"},
		{name: "cname_service", rrType: recordTypeCNAME, value: "_sip._tcp.example.com"},
		{name: "cname_bad", rrType: recordTypeCNAME, value: "not a hostname", wantErr: true},
		{name: "ns_ok", rrType: recordTypeNS, value: "ns1.example.com."},
		{name: "ns_bad", rrType: recordTypeNS, value: "bad host", wantErr: true},
		{name: "ptr_ok", rrType: recordTypePTR, value: "host.example.com"},
		{name: "ptr_bad", rrType: recordTypePTR, value: "no spaces here!", wantErr: true},
		{name: "ds_ok", rrType: recordTypeDS, value: "12345 13 2 49FD46E6C4B45C55D4AC"},
		{name: "ds_bad_digest", rrType: recordTypeDS, value: "12345 13 2 ZZZZ", wantErr: true},
		{name: "ds_bad_shape", rrType: recordTypeDS, value: "12345 13", wantErr: true},
		{name: "naptr_ok", rrType: recordTypeNAPTR, value: `100 10 "u" "E2U+sip" "!^.*$!sip:x@y!" .`},
		{name: "naptr_bad", rrType: recordTypeNAPTR, value: "100 10 u", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateRecordValue(tt.rrType, tt.value)
			if tt.wantErr && err == nil {
				t.Fatalf("want error for %s=%q, got nil", tt.rrType, tt.value)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("want no error for %s=%q, got %v", tt.rrType, tt.value, err)
			}
		})
	}
}

func TestGeoIPLookup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		ip            string
		wantRegion    string
		wantCountry   string
		wantContinent string
	}{
		{name: "empty_defaults", ip: "", wantRegion: "us-east-1", wantCountry: "US", wantContinent: "NA"},
		{name: "invalid_defaults", ip: "not-an-ip", wantRegion: "us-east-1", wantCountry: "US", wantContinent: "NA"},
		{name: "ireland", ip: "34.240.1.2", wantRegion: "eu-west-1", wantCountry: "IE", wantContinent: "EU"},
		{name: "tokyo", ip: "13.230.0.5", wantRegion: "ap-northeast-1", wantCountry: "JP", wantContinent: "AS"},
		{name: "sydney", ip: "13.54.0.9", wantRegion: "ap-southeast-2", wantCountry: "AU", wantContinent: "OC"},
		{name: "unknown_range_defaults", ip: "100.64.0.1", wantRegion: "us-east-1", wantCountry: "US"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			region, continent, country, _ := geoIPLookup(tt.ip)
			if region != tt.wantRegion {
				t.Errorf("region: got %q want %q", region, tt.wantRegion)
			}
			if country != tt.wantCountry {
				t.Errorf("country: got %q want %q", country, tt.wantCountry)
			}
			if tt.wantContinent != "" && continent != tt.wantContinent {
				t.Errorf("continent: got %q want %q", continent, tt.wantContinent)
			}
		})
	}
}

func TestClassifyRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cand []*ResourceRecordSet
		want routingKind
	}{
		{name: "simple", cand: []*ResourceRecordSet{{}}, want: routingSimple},
		{name: "weighted", cand: []*ResourceRecordSet{{Weight: i64(0)}}, want: routingWeighted},
		{name: "latency", cand: []*ResourceRecordSet{{Region: "us-east-1"}}, want: routingLatency},
		{name: "geo", cand: []*ResourceRecordSet{{GeoLocation: &GeoLocation{CountryCode: "US"}}}, want: routingGeo},
		{name: "failover", cand: []*ResourceRecordSet{{Failover: FailoverPrimary}}, want: routingFailover},
		{name: "multivalue", cand: []*ResourceRecordSet{{MultiValueAnswer: true}}, want: routingMultiValue},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := classifyRouting(tt.cand); got != tt.want {
				t.Fatalf("classifyRouting = %d want %d", got, tt.want)
			}
		})
	}
}

func TestSelectWeightedDistribution(t *testing.T) {
	t.Parallel()

	// Weights 70/20/10 over 30_000 draws should approximate the ratios.
	cands := []*ResourceRecordSet{
		{SetIdentifier: "a", Weight: i64(70), Records: []ResourceRecord{{Value: "a"}}},
		{SetIdentifier: "b", Weight: i64(20), Records: []ResourceRecord{{Value: "b"}}},
		{SetIdentifier: "c", Weight: i64(10), Records: []ResourceRecord{{Value: "c"}}},
	}

	qctx := DNSQueryContext{randFloat: seededRandFloat(42)}
	const draws = 30000
	counts := map[string]int{}

	for range draws {
		chosen := selectWeighted(cands, qctx)
		counts[chosen.Records[0].Value]++
	}

	wantRatio := map[string]float64{"a": 0.70, "b": 0.20, "c": 0.10}
	for k, want := range wantRatio {
		got := float64(counts[k]) / float64(draws)
		if math.Abs(got-want) > 0.03 {
			t.Errorf("value %q ratio = %.3f want ~%.2f (counts=%v)", k, got, want, counts)
		}
	}
}

func TestSelectWeightedZeroExcludedAndAllZero(t *testing.T) {
	t.Parallel()

	t.Run("zero_weight_excluded", func(t *testing.T) {
		t.Parallel()

		cands := []*ResourceRecordSet{
			{SetIdentifier: "live", Weight: i64(10), Records: []ResourceRecord{{Value: "live"}}},
			{SetIdentifier: "dead", Weight: i64(0), Records: []ResourceRecord{{Value: "dead"}}},
		}
		qctx := DNSQueryContext{randFloat: seededRandFloat(1)}
		for range 1000 {
			if v := selectWeighted(cands, qctx).Records[0].Value; v == "dead" {
				t.Fatalf("weight-0 record was selected")
			}
		}
	})

	t.Run("all_zero_equal", func(t *testing.T) {
		t.Parallel()

		cands := []*ResourceRecordSet{
			{SetIdentifier: "a", Weight: i64(0), Records: []ResourceRecord{{Value: "a"}}},
			{SetIdentifier: "b", Weight: i64(0), Records: []ResourceRecord{{Value: "b"}}},
		}
		qctx := DNSQueryContext{randFloat: seededRandFloat(7)}
		seen := map[string]bool{}
		for range 1000 {
			seen[selectWeighted(cands, qctx).Records[0].Value] = true
		}
		if !seen["a"] || !seen["b"] {
			t.Fatalf("all-zero weights should route to both records, saw %v", seen)
		}
	})
}

func TestSelectLatency(t *testing.T) {
	t.Parallel()

	cands := []*ResourceRecordSet{
		{SetIdentifier: "east", Region: "us-east-1", Records: []ResourceRecord{{Value: "east"}}},
		{SetIdentifier: "eu", Region: "eu-west-1", Records: []ResourceRecord{{Value: "eu"}}},
		{SetIdentifier: "ap", Region: "ap-southeast-2", Records: []ResourceRecord{{Value: "ap"}}},
	}

	tests := []struct {
		name         string
		clientRegion string
		want         string
	}{
		{name: "exact_match", clientRegion: "eu-west-1", want: "eu"},
		{name: "west_closest_to_east", clientRegion: "us-west-2", want: "east"},
		{name: "frankfurt_closest_eu", clientRegion: "eu-central-1", want: "eu"},
		{name: "near_sydney", clientRegion: "ap-southeast-1", want: "ap"},
		{name: "unknown_defaults_useast", clientRegion: "zz-nowhere-1", want: "east"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := selectLatency(cands, DNSQueryContext{ClientRegion: tt.clientRegion})
			if got.Records[0].Value != tt.want {
				t.Fatalf("latency for %s = %q want %q", tt.clientRegion, got.Records[0].Value, tt.want)
			}
		})
	}
}

func TestSelectGeo(t *testing.T) {
	t.Parallel()

	withDefault := []*ResourceRecordSet{
		{SetIdentifier: "def", GeoLocation: &GeoLocation{CountryCode: "*"}, Records: []ResourceRecord{{Value: "def"}}},
		{SetIdentifier: "us", GeoLocation: &GeoLocation{CountryCode: "US"}, Records: []ResourceRecord{{Value: "us"}}},
		{
			SetIdentifier: "usca",
			GeoLocation:   &GeoLocation{CountryCode: "US", SubdivisionCode: "CA"},
			Records:       []ResourceRecord{{Value: "usca"}},
		},
		{SetIdentifier: "eu", GeoLocation: &GeoLocation{ContinentCode: "EU"}, Records: []ResourceRecord{{Value: "eu"}}},
	}
	noDefault := []*ResourceRecordSet{
		{SetIdentifier: "us", GeoLocation: &GeoLocation{CountryCode: "US"}, Records: []ResourceRecord{{Value: "us"}}},
	}

	tests := []struct {
		qctx DNSQueryContext
		name string
		want string // "" means expect nil
		cand []*ResourceRecordSet
	}{
		{
			name: "subdivision_wins",
			cand: withDefault,
			qctx: DNSQueryContext{CountryCode: "US", SubdivisionCode: "CA", ContinentCode: "NA"},
			want: "usca",
		},
		{
			name: "country_match",
			cand: withDefault,
			qctx: DNSQueryContext{CountryCode: "US", SubdivisionCode: "TX", ContinentCode: "NA"},
			want: "us",
		},
		{
			name: "continent_match",
			cand: withDefault,
			qctx: DNSQueryContext{CountryCode: "FR", ContinentCode: "EU"},
			want: "eu",
		},
		{
			name: "falls_to_default",
			cand: withDefault,
			qctx: DNSQueryContext{CountryCode: "JP", ContinentCode: "AS"},
			want: "def",
		},
		{
			name: "no_match_no_default_nil",
			cand: noDefault,
			qctx: DNSQueryContext{CountryCode: "JP", ContinentCode: "AS"},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := selectGeo(tt.cand, tt.qctx)
			if tt.want == "" {
				if got != nil {
					t.Fatalf("want nil, got %q", got.Records[0].Value)
				}

				return
			}
			if got == nil {
				t.Fatalf("want %q, got nil", tt.want)
			}
			if got.Records[0].Value != tt.want {
				t.Fatalf("geo = %q want %q", got.Records[0].Value, tt.want)
			}
		})
	}
}

func TestTestDNSAnswerFailoverHealth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		primaryHealth string
		want          string
	}{
		{name: "healthy_primary_wins", primaryHealth: "Healthy", want: "10.0.0.1"},
		{name: "unhealthy_primary_fails_over", primaryHealth: "Unhealthy", want: "10.0.0.2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, zoneID := newTestZone(t)
			primaryHC := newHealthCheck(t, b, tt.primaryHealth)

			addRecords(t, b, zoneID,
				ResourceRecordSet{
					Name: "app.example.com.", Type: "A", TTL: 60,
					SetIdentifier: "primary", Failover: FailoverPrimary,
					HealthCheckID: primaryHC,
					Records:       []ResourceRecord{{Value: "10.0.0.1"}},
				},
				ResourceRecordSet{
					Name: "app.example.com.", Type: "A", TTL: 60,
					SetIdentifier: "secondary", Failover: FailoverSecondary,
					Records: []ResourceRecord{{Value: "10.0.0.2"}},
				},
			)

			vals, err := b.TestDNSAnswer(zoneID, "app.example.com", "A", DNSQueryContext{})
			if err != nil {
				t.Fatalf("TestDNSAnswer: %v", err)
			}
			if len(vals) != 1 || vals[0] != tt.want {
				t.Fatalf("failover answer = %v want [%s]", vals, tt.want)
			}
		})
	}
}

func TestTestDNSAnswerMultiValueHealth(t *testing.T) {
	t.Parallel()

	b, zoneID := newTestZone(t)
	badHC := newHealthCheck(t, b, "Unhealthy")

	addRecords(t, b, zoneID,
		ResourceRecordSet{
			Name: "mv.example.com.", Type: "A", TTL: 60,
			SetIdentifier: "one", MultiValueAnswer: true,
			Records: []ResourceRecord{{Value: "10.0.0.1"}},
		},
		ResourceRecordSet{
			Name: "mv.example.com.", Type: "A", TTL: 60,
			SetIdentifier: "two", MultiValueAnswer: true,
			Records: []ResourceRecord{{Value: "10.0.0.2"}},
		},
		ResourceRecordSet{
			Name: "mv.example.com.", Type: "A", TTL: 60,
			SetIdentifier: "bad", MultiValueAnswer: true,
			HealthCheckID: badHC,
			Records:       []ResourceRecord{{Value: "10.0.0.3"}},
		},
	)

	vals, err := b.TestDNSAnswer(zoneID, "mv.example.com", "A", DNSQueryContext{})
	if err != nil {
		t.Fatalf("TestDNSAnswer: %v", err)
	}

	if len(vals) != 2 {
		t.Fatalf("multivalue answer = %v, want 2 healthy records", vals)
	}
	for _, v := range vals {
		if v == "10.0.0.3" {
			t.Fatalf("unhealthy record present in answer: %v", vals)
		}
	}
}

func TestTestDNSAnswerWeightedHealthExclusion(t *testing.T) {
	t.Parallel()

	b, zoneID := newTestZone(t)
	badHC := newHealthCheck(t, b, "Unhealthy")

	addRecords(t, b, zoneID,
		ResourceRecordSet{
			Name: "w.example.com.", Type: "A", TTL: 60,
			SetIdentifier: "healthy", Weight: i64(50),
			Records: []ResourceRecord{{Value: "10.0.0.1"}},
		},
		ResourceRecordSet{
			Name: "w.example.com.", Type: "A", TTL: 60,
			SetIdentifier: "unhealthy", Weight: i64(50),
			HealthCheckID: badHC,
			Records:       []ResourceRecord{{Value: "10.0.0.2"}},
		},
	)

	// Even across many draws the unhealthy record must never be returned.
	for i := range 500 {
		vals, err := b.TestDNSAnswer(zoneID, "w.example.com", "A",
			DNSQueryContext{randFloat: seededRandFloat(int64(i))})
		if err != nil {
			t.Fatalf("TestDNSAnswer: %v", err)
		}
		if len(vals) != 1 || vals[0] != "10.0.0.1" {
			t.Fatalf("weighted answer = %v, want [10.0.0.1] (unhealthy excluded)", vals)
		}
	}
}

func TestTestDNSAnswerGeoEndToEnd(t *testing.T) {
	t.Parallel()

	b, zoneID := newTestZone(t)
	addRecords(t, b, zoneID,
		ResourceRecordSet{
			Name: "geo.example.com.", Type: "A", TTL: 60,
			SetIdentifier: "default", GeoLocation: &GeoLocation{CountryCode: "*"},
			Records: []ResourceRecord{{Value: "10.0.0.9"}},
		},
		ResourceRecordSet{
			Name: "geo.example.com.", Type: "A", TTL: 60,
			SetIdentifier: "us", GeoLocation: &GeoLocation{CountryCode: "US"},
			Records: []ResourceRecord{{Value: "10.0.0.1"}},
		},
	)

	tests := []struct {
		name string
		qctx DNSQueryContext
		want string
	}{
		{name: "us_client", qctx: DNSQueryContext{CountryCode: "US", ContinentCode: "NA"}, want: "10.0.0.1"},
		{name: "other_default", qctx: DNSQueryContext{CountryCode: "JP", ContinentCode: "AS"}, want: "10.0.0.9"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vals, err := b.TestDNSAnswer(zoneID, "geo.example.com", "A", tt.qctx)
			if err != nil {
				t.Fatalf("TestDNSAnswer: %v", err)
			}
			if len(vals) != 1 || vals[0] != tt.want {
				t.Fatalf("geo answer = %v want [%s]", vals, tt.want)
			}
		})
	}
}

func TestTestDNSAnswerAliasRecursion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		want          []string
		evalHealth    bool
		targetHealthy bool
		outOfZone     bool
	}{
		{name: "in_zone_recurses", want: []string{"10.1.2.3"}},
		{name: "eval_health_healthy", evalHealth: true, targetHealthy: true, want: []string{"10.1.2.3"}},
		{name: "eval_health_unhealthy_empty", evalHealth: true, targetHealthy: false, want: []string{}},
		{name: "out_of_zone_literal", outOfZone: true, want: []string{"external.other.net"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, zoneID := newTestZone(t)

			target := ResourceRecordSet{
				Name: "target.example.com.", Type: "A", TTL: 60,
				Records: []ResourceRecord{{Value: "10.1.2.3"}},
			}
			if tt.evalHealth {
				status := "Healthy"
				if !tt.targetHealthy {
					status = "Unhealthy"
				}
				target.HealthCheckID = newHealthCheck(t, b, status)
			}

			dnsName := "target.example.com"
			if tt.outOfZone {
				dnsName = "external.other.net"
			} else {
				addRecords(t, b, zoneID, target)
			}

			addRecords(t, b, zoneID, ResourceRecordSet{
				Name: "www.example.com.", Type: "A",
				AliasTarget: &AliasTarget{
					HostedZoneID:         zoneID,
					DNSName:              dnsName,
					EvaluateTargetHealth: tt.evalHealth,
				},
			})

			vals, err := b.TestDNSAnswer(zoneID, "www.example.com", "A", DNSQueryContext{})
			if err != nil {
				t.Fatalf("TestDNSAnswer: %v", err)
			}
			if len(vals) != len(tt.want) {
				t.Fatalf("alias answer = %v want %v", vals, tt.want)
			}
			for i := range vals {
				if vals[i] != tt.want[i] {
					t.Fatalf("alias answer = %v want %v", vals, tt.want)
				}
			}
		})
	}
}

func TestSelectFailoverEdges(t *testing.T) {
	t.Parallel()

	primary := &ResourceRecordSet{
		SetIdentifier: "p", Failover: FailoverPrimary, Records: []ResourceRecord{{Value: "p"}},
	}
	secondary := &ResourceRecordSet{
		SetIdentifier: "s", Failover: FailoverSecondary, Records: []ResourceRecord{{Value: "s"}},
	}

	tests := []struct {
		name  string
		want  string // "" => nil
		input []*ResourceRecordSet
	}{
		{name: "primary_present", input: []*ResourceRecordSet{primary, secondary}, want: "p"},
		{name: "secondary_only", input: []*ResourceRecordSet{secondary}, want: "s"},
		{name: "empty_nil", input: nil, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := selectFailover(tt.input)
			if tt.want == "" {
				if got != nil {
					t.Fatalf("want nil, got %q", got.Records[0].Value)
				}

				return
			}
			if got == nil || got.Records[0].Value != tt.want {
				t.Fatalf("selectFailover = %v want %q", got, tt.want)
			}
		})
	}
}

func TestTestDNSAnswerLatencyEndToEnd(t *testing.T) {
	t.Parallel()

	b, zoneID := newTestZone(t)
	addRecords(t, b, zoneID,
		ResourceRecordSet{
			Name: "lat.example.com.", Type: "A", TTL: 60,
			SetIdentifier: "us", Region: defaultRegion,
			Records: []ResourceRecord{{Value: "10.0.0.1"}},
		},
		ResourceRecordSet{
			Name: "lat.example.com.", Type: "A", TTL: 60,
			SetIdentifier: "eu", Region: regionEUWest1,
			Records: []ResourceRecord{{Value: "10.0.0.2"}},
		},
	)

	tests := []struct {
		name         string
		clientRegion string
		want         string
	}{
		{name: "us_client_gets_us", clientRegion: regionUSWest2, want: "10.0.0.1"},
		{name: "eu_client_gets_eu", clientRegion: regionEUCentral1, want: "10.0.0.2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vals, err := b.TestDNSAnswer(
				zoneID, "lat.example.com", "A",
				DNSQueryContext{ClientRegion: tt.clientRegion},
			)
			if err != nil {
				t.Fatalf("TestDNSAnswer: %v", err)
			}
			if len(vals) != 1 || vals[0] != tt.want {
				t.Fatalf("latency answer = %v want [%s]", vals, tt.want)
			}
		})
	}
}

func TestTestDNSAnswerMultiValueCap(t *testing.T) {
	t.Parallel()

	b, zoneID := newTestZone(t)

	// Create 10 healthy multivalue records; Route 53 answers at most 8.
	sets := make([]ResourceRecordSet, 0, 10)
	for i := range 10 {
		sets = append(sets, ResourceRecordSet{
			Name: "big.example.com.", Type: "A", TTL: 60,
			SetIdentifier:    "id-" + strconv.Itoa(i),
			MultiValueAnswer: true,
			Records:          []ResourceRecord{{Value: "10.0.0." + strconv.Itoa(i+1)}},
		})
	}
	addRecords(t, b, zoneID, sets...)

	vals, err := b.TestDNSAnswer(zoneID, "big.example.com", "A", DNSQueryContext{})
	if err != nil {
		t.Fatalf("TestDNSAnswer: %v", err)
	}
	if len(vals) != maxMultiValueRecords {
		t.Fatalf("multivalue answer returned %d records, want cap of %d", len(vals), maxMultiValueRecords)
	}
}
