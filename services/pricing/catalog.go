package pricing

import (
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
)

const (
	catalogVersion  = "20261001000000"
	publicationDate = "2026-10-01T00:00:00Z"
	currency        = "USD"
	onDemandTerm    = "JRTCKXETXF"
	reserved1yTerm  = "4NA7Y494T4"
	reserved3yTerm  = "NQ3QZPMQV9"
	reserved1yRatio = 0.62
	reserved3yRatio = 0.42
	skuLen          = 16
	rateSuffixLen   = 10
	infinity        = "Inf"
)

var regions = []region{ //nolint:gochecknoglobals // immutable catalog input
	{"us-east-1", "US East (N. Virginia)", 1.0},
	{"us-west-2", "US West (Oregon)", 1.0},
	{"eu-west-1", "EU (Ireland)", 1.1},
	{"eu-central-1", "EU (Frankfurt)", 1.15},
	{"ap-south-1", "Asia Pacific (Mumbai)", 1.08},
	{"ap-northeast-1", "Asia Pacific (Tokyo)", 1.27},
	{"us-east-2", "US East (Ohio)", 1.0},
	{"us-west-1", "US West (N. California)", 1.17},
	{"eu-west-2", "EU (London)", 1.12},
	{"ap-southeast-1", "Asia Pacific (Singapore)", 1.18},
	{"ap-southeast-2", "Asia Pacific (Sydney)", 1.2},
	{"ap-northeast-2", "Asia Pacific (Seoul)", 1.2},
	{"sa-east-1", "South America (Sao Paulo)", 1.5},
	{"ca-central-1", "Canada (Central)", 1.1},
}

var serviceNames = map[string]string{ //nolint:gochecknoglobals // immutable catalog input
	svcEC2:      "Amazon Elastic Compute Cloud",
	svcS3:       "Amazon Simple Storage Service",
	svcLambda:   "AWS Lambda",
	svcDynamoDB: "Amazon DynamoDB",
	svcRDS:      "Amazon Relational Database Service",

	svcSQS:         "Amazon Simple Queue Service",
	svcSNS:         "Amazon Simple Notification Service",
	svcCloudFront:  "Amazon CloudFront",
	svcElastiCache: "Amazon ElastiCache",
	svcECS:         "Amazon Elastic Container Service",
}

type catalog struct {
	byService map[string][]*entry
	codes     []string
}

// theCatalog builds the embedded price catalog once.
var theCatalog = sync.OnceValue(buildCatalog) //nolint:gochecknoglobals // immutable embedded catalog

func buildCatalog() *catalog {
	specs := slices.Concat(ec2Specs(), s3Specs(), lambdaSpecs(), dynamoSpecs(), rdsSpecs(),
		sqsSpecs(), snsSpecs(), cloudFrontSpecs(), elastiCacheSpecs(), ecsSpecs())
	c := &catalog{byService: map[string][]*entry{}}

	for i := range specs {
		e := render(&specs[i])
		c.byService[e.service] = append(c.byService[e.service], e)
	}

	for code, list := range c.byService {
		c.codes = append(c.codes, code)
		sort.Slice(list, func(i, j int) bool { return list[i].item.Product.SKU < list[j].item.Product.SKU })
	}

	sort.Strings(c.codes)

	return c
}

func usd(v float64) map[string]string { return map[string]string{currency: fmt.Sprintf("%.10f", v)} }

func skuOf(s *spec) string {
	keys := make([]string, 0, len(s.attrs))
	for k := range s.attrs {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	var sb strings.Builder

	sb.WriteString(s.service + "|" + s.family + "|" + s.region.code)

	for _, k := range keys {
		sb.WriteString("|" + k + "=" + s.attrs[k])
	}

	sum := sha256.Sum256([]byte(sb.String()))

	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:])[:skuLen]
}

func render(s *spec) *entry {
	sku := skuOf(s)
	attrs := map[string]string{
		"servicecode": s.service, "servicename": serviceNames[s.service], "regionCode": s.region.code,
		"location": s.region.location, "locationType": "AWS Region",
	}

	maps.Copy(attrs, s.attrs)

	terms := termsJSON{OnDemand: map[string]termJSON{
		sku + "." + onDemandTerm: buildTerm(sku, onDemandTerm, nil, s.dims, 1),
	}}

	if s.reserved {
		terms.Reserved = reservedTerms(sku, s.dims)
	}

	item := priceItem{
		Product:     productJSON{Attributes: attrs, ProductFamily: s.family, SKU: sku},
		ServiceCode: s.service, Terms: terms, Version: catalogVersion, PublicationDate: publicationDate,
	}
	raw, _ := json.Marshal(item)

	return &entry{item: item, json: string(raw), region: s.region.code, service: s.service}
}

func hourly(dims []dim) []dim {
	var out []dim

	for _, d := range dims {
		if d.unit == "Hrs" {
			out = append(out, dim{unit: d.unit, desc: "Reserved " + d.desc, begin: "0", end: infinity, usd: d.usd})
		}
	}

	return out
}

func buildTerm(sku, code string, termAttrs map[string]string, dims []dim, ratio float64) termJSON {
	if termAttrs == nil {
		termAttrs = map[string]string{}
	}

	t := termJSON{
		PriceDimensions: map[string]dimensionJSON{}, TermAttributes: termAttrs, SKU: sku,
		EffectiveDate: publicationDate, OfferTermCode: code,
	}

	for i, d := range dims {
		sum := sha256.Sum256(fmt.Appendf(nil, "%s.%s.%d", sku, code, i))
		rate := fmt.Sprintf("%s.%s.%s", sku, code,
			base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:])[:rateSuffixLen])
		t.PriceDimensions[rate] = dimensionJSON{
			Unit: d.unit, EndRange: d.end, Description: d.desc, AppliesTo: []string{}, RateCode: rate,
			BeginRange: d.begin, PricePerUnit: usd(d.usd * ratio),
		}
	}

	return t
}
