package ce

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// syntheticServiceDef describes a synthetic AWS service used in the cost ledger seed
// and as a fallback when real ledger entries are absent for a queried time period.
type syntheticServiceDef struct {
	name      string
	usageType string
	weight    float64
}

// syntheticServiceCatalog is the canonical list of services used in both the cost
// ledger seed and the synthetic-group fallback for GroupBy queries.
//
//nolint:gochecknoglobals // package-level catalog shared by seed and fallback
var syntheticServiceCatalog = []syntheticServiceDef{
	{"Amazon Elastic Compute Cloud - Compute", "BoxUsage:t3.medium", 0.40},
	{"Amazon Simple Storage Service", "TimedStorage-ByteHrs", 0.15},
	{"Amazon Relational Database Service", "InstanceUsage:db.t3.medium", 0.10},
	{"AWS Lambda", "Lambda-GB-Second", 0.08},
	{"Amazon CloudFront", "US-DataTransfer-Out-Bytes", 0.07},
	{"Amazon DynamoDB", "TimedStorage-ByteHrs", 0.05},
	{"Amazon Elastic Load Balancing", "LoadBalancerUsage", 0.04},
	{"AWS Key Management Service", "KMS-Requests", 0.03},
	{"Amazon Route 53", "DNS-Queries", 0.02},
	{"AWS CloudTrail", "EUS-DataScanned", 0.01},
	{"Amazon SNS", "DeliveryAttempts-HTTP", 0.005},
	{"Amazon SQS", "SQS-Requests", 0.005},
}

// syntheticBaseMonthlyTotal is the base monthly cost used when no ledger entries
// exist for the queried period (e.g. historical ranges before the seed window).
const syntheticBaseMonthlyTotal = 3000.0

// syntheticGroupsFallback generates GroupBy result groups from the service catalog
// when the cost ledger contains no entries for the queried period.
// The groups mirror what would appear for real data, giving callers a valid
// non-empty response shape for any time period.
func syntheticGroupsFallback(accountID, region string, groupBy []GroupBySpec, metrics []string) []CostGroup {
	groups := make([]CostGroup, 0, len(syntheticServiceCatalog))

	for _, svc := range syntheticServiceCatalog {
		keys := make([]string, 0, len(groupBy))
		for _, g := range groupBy {
			switch strings.ToUpper(g.Key) {
			case dimKeyService:
				keys = append(keys, svc.name)
			case dimKeyRegion:
				keys = append(keys, region)
			case dimKeyUsageType:
				keys = append(keys, svc.usageType)
			case dimKeyLinkedAccount:
				keys = append(keys, accountID)
			default:
				keys = append(keys, "Other")
			}
		}

		amount := syntheticBaseMonthlyTotal * svc.weight
		amounts := make(map[string]float64, len(metrics))
		for _, m := range metrics {
			amounts[m] = amount
		}

		groups = append(groups, CostGroup{
			Keys:    keys,
			Metrics: buildMetricValues(amounts, metrics),
		})
	}

	return groups
}

// seedCostLedger populates the cost ledger with 90 days of synthetic data.
// Seeded distributions: EC2~40%, S3~15%, RDS~10%, Lambda~8%, CloudFront~7%, others rest.
func (b *InMemoryBackend) seedCostLedger() {
	services := syntheticServiceCatalog

	now := time.Now().UTC()
	totalDailyBase := 150.0

	for day := 89; day >= 0; day-- {
		d := now.AddDate(0, 0, -day)
		dateStr := d.Format("2006-01-02")

		dayMultiplier := 1.0
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			dayMultiplier = 0.8
		}

		jitter := 1.0 + float64(d.YearDay()%10-syntheticJitterRange)*syntheticJitterScale

		for _, svc := range services {
			amount := totalDailyBase * svc.weight * dayMultiplier * jitter
			b.costLedger = append(b.costLedger, CostEntry{
				Date:          dateStr,
				Service:       svc.name,
				Region:        b.region,
				UsageType:     svc.usageType,
				Account:       b.accountID,
				BlendedCost:   amount,
				UnblendedCost: amount * unblendedCostFactor,
				UsageQuantity: amount * usageQuantityFactor,
			})
		}
	}
}

// costLedgerInBucket returns entries where start <= date < end (no lock — caller holds).
func (b *InMemoryBackend) costLedgerInBucket(start, end string) []CostEntry {
	var out []CostEntry

	for _, e := range b.costLedger {
		if e.Date >= start && e.Date < end {
			out = append(out, e)
		}
	}

	return out
}

type timeBucket struct{ start, end string }

// buildTimeBuckets generates date-bucket boundaries for DAILY/MONTHLY/HOURLY granularity.
func buildTimeBuckets(start, end, granularity string) []timeBucket {
	var buckets []timeBucket

	startT, err1 := time.Parse("2006-01-02", start)
	endT, err2 := time.Parse("2006-01-02", end)

	if err1 != nil || err2 != nil {
		return buckets
	}

	switch strings.ToUpper(granularity) {
	case granularityMonthly:
		cur := time.Date(startT.Year(), startT.Month(), 1, 0, 0, 0, 0, time.UTC)
		for cur.Before(endT) {
			next := cur.AddDate(0, 1, 0)
			buckets = append(buckets, timeBucket{
				start: cur.Format("2006-01-02"),
				end:   next.Format("2006-01-02"),
			})
			cur = next
		}
	default: // DAILY (HOURLY treated as DAILY for simplicity)
		cur := startT
		for cur.Before(endT) {
			next := cur.AddDate(0, 0, 1)
			buckets = append(buckets, timeBucket{
				start: cur.Format("2006-01-02"),
				end:   next.Format("2006-01-02"),
			})
			cur = next
		}
	}

	return buckets
}

func extractGroupKeys(e CostEntry, groupBy []GroupBySpec) []string {
	keys := make([]string, 0, len(groupBy))

	for _, g := range groupBy {
		switch strings.ToUpper(g.Key) {
		case dimKeyService:
			keys = append(keys, e.Service)
		case dimKeyRegion:
			keys = append(keys, e.Region)
		case dimKeyUsageType:
			keys = append(keys, e.UsageType)
		case dimKeyLinkedAccount:
			keys = append(keys, e.Account)
		default:
			keys = append(keys, e.Tags[g.Key])
		}
	}

	return keys
}

func getMetricValue(e CostEntry, metric string) float64 {
	switch strings.ToUpper(metric) {
	case "BLENDEDCOST":
		return e.BlendedCost
	case "UNBLENDEDCOST":
		return e.UnblendedCost
	case "AMORTIZEDCOST", "NETAMORTIZEDCOST":
		return e.UnblendedCost * amortizedCostFactor
	case "NETUNBLENDEDCOST":
		return e.UnblendedCost * netUnblendedCostFactor
	case "USAGEQUANTITY":
		return e.UsageQuantity
	case "NORMALIZEDUSAGEAMOUNT":
		return e.UsageQuantity * normalizedUsageFactor
	default:
		return e.BlendedCost
	}
}

func metricUnit(metric string) string {
	switch strings.ToUpper(metric) {
	case "USAGEQUANTITY", "NORMALIZEDUSAGEAMOUNT":
		return metricUnitNA
	default:
		return metricUnitUSD
	}
}

func calcMeanStddev(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 1
	}

	var avg float64

	for _, v := range values {
		avg += v
	}

	avg /= float64(len(values))

	var sd float64

	for _, v := range values {
		diff := v - avg
		sd += diff * diff
	}

	if len(values) > 1 {
		sd /= float64(len(values) - 1)
	}

	sd = math.Sqrt(sd)

	if sd < stddevMinThreshold {
		sd = avg * stddevFallbackRatio
	}

	return avg, sd
}

// buildMetricValues converts a map of metric amounts into MetricValue structs.
func buildMetricValues(amounts map[string]float64, metrics []string) map[string]MetricValue {
	mv := make(map[string]MetricValue, len(metrics))
	for _, m := range metrics {
		mv[m] = MetricValue{
			Amount: fmt.Sprintf("%.4f", amounts[m]),
			Unit:   metricUnit(m),
		}
	}

	return mv
}

// aggregateByGroup groups entries by GroupBy keys and returns sorted CostGroups.
func aggregateByGroup(entries []CostEntry, groupBy []GroupBySpec, metrics []string) []CostGroup {
	groupMap := make(map[string]map[string]float64)
	keyMap := make(map[string][]string)

	for _, e := range entries {
		keys := extractGroupKeys(e, groupBy)
		gkey := strings.Join(keys, "|")

		if _, ok := groupMap[gkey]; !ok {
			groupMap[gkey] = make(map[string]float64)
			keyMap[gkey] = keys
		}

		for _, m := range metrics {
			groupMap[gkey][m] += getMetricValue(e, m)
		}
	}

	gkeys := collections.SortedKeys(groupMap)

	groups := make([]CostGroup, 0, len(gkeys))
	for _, gk := range gkeys {
		groups = append(groups, CostGroup{
			Keys:    keyMap[gk],
			Metrics: buildMetricValues(groupMap[gk], metrics),
		})
	}

	return groups
}

// aggregateTotals sums entries across all metrics and returns MetricValue structs.
func aggregateTotals(entries []CostEntry, metrics []string) map[string]MetricValue {
	totals := make(map[string]float64)
	for _, e := range entries {
		for _, m := range metrics {
			totals[m] += getMetricValue(e, m)
		}
	}

	return buildMetricValues(totals, metrics)
}

// GetCostAndUsage aggregates cost ledger entries by granularity, applying optional GroupBy.
func (b *InMemoryBackend) GetCostAndUsage(
	start, end, granularity string,
	metrics []string,
	groupBy []GroupBySpec,
) []ResultByTime {
	b.mu.RLock("GetCostAndUsage")
	defer b.mu.RUnlock()

	if len(metrics) == 0 {
		metrics = []string{"BlendedCost"}
	}

	buckets := buildTimeBuckets(start, end, granularity)
	results := make([]ResultByTime, 0, len(buckets))
	now := time.Now().UTC().Format("2006-01-02")

	for _, bucket := range buckets {
		entries := b.costLedgerInBucket(bucket.start, bucket.end)
		r := ResultByTime{
			TimePeriod: map[string]string{timePeriodKeyStart: bucket.start, timePeriodKeyEnd: bucket.end},
			Estimated:  bucket.start >= now || bucket.end > now,
			Groups:     []CostGroup{},
		}

		if len(groupBy) > 0 {
			r.Groups = aggregateByGroup(entries, groupBy, metrics)
			if len(r.Groups) == 0 {
				r.Groups = syntheticGroupsFallback(b.accountID, b.region, groupBy, metrics)
			}

			r.Total = map[string]MetricValue{}
		} else {
			r.Total = aggregateTotals(entries, metrics)
		}

		results = append(results, r)
	}

	return results
}

// GetDimensionValues returns unique values for the given dimension from the cost ledger.
func (b *InMemoryBackend) GetDimensionValues(dimension string) []string {
	b.mu.RLock("GetDimensionValues")
	defer b.mu.RUnlock()

	seen := make(map[string]struct{})

	for _, e := range b.costLedger {
		var val string

		switch strings.ToUpper(dimension) {
		case dimKeyService:
			val = e.Service
		case dimKeyRegion, "AZ":
			val = e.Region
		case dimKeyUsageType:
			val = e.UsageType
		case dimKeyLinkedAccount:
			val = e.Account
		case "INSTANCE_TYPE":
			val = syntheticInstanceType
		case "OPERATING_SYSTEM":
			val = "Linux"
		case "TENANCY":
			val = "Shared"
		case "PURCHASE_TYPE":
			val = "On Demand"
		case "RECORD_TYPE":
			val = "Usage"
		default:
			continue
		}

		if val != "" {
			seen[val] = struct{}{}
		}
	}

	vals := collections.SortedKeys(seen)

	return vals
}

// GetTagKeys returns all distinct tag keys used across the cost ledger.
func (b *InMemoryBackend) GetTagKeys() []string {
	b.mu.RLock("GetTagKeys")
	defer b.mu.RUnlock()

	seen := make(map[string]struct{})

	for _, e := range b.costLedger {
		for k := range e.Tags {
			seen[k] = struct{}{}
		}
	}

	keys := collections.SortedKeys(seen)

	return keys
}

// GetTagValues returns distinct values for a tag key.
func (b *InMemoryBackend) GetTagValues(tagKey string) []string {
	b.mu.RLock("GetTagValues")
	defer b.mu.RUnlock()

	seen := make(map[string]struct{})

	for _, e := range b.costLedger {
		if v, ok := e.Tags[tagKey]; ok && v != "" {
			seen[v] = struct{}{}
		}
	}

	vals := collections.SortedKeys(seen)

	return vals
}

// GetForecastByTime returns per-bucket cost forecasts for a time range.
func (b *InMemoryBackend) GetForecastByTime(
	start, end, granularity string,
	predictionIntervalLevel int,
) ([]ForecastResult, float64, float64, float64) {
	b.mu.RLock("GetForecastByTime")
	defer b.mu.RUnlock()

	histEnd := time.Now().UTC().Format("2006-01-02")
	histStart := time.Now().UTC().AddDate(0, 0, -30).Format("2006-01-02")

	histBuckets := buildTimeBuckets(histStart, histEnd, granularity)
	histValues := make([]float64, 0, len(histBuckets))

	for _, hb := range histBuckets {
		var bucketTotal float64
		for _, e := range b.costLedgerInBucket(hb.start, hb.end) {
			bucketTotal += e.BlendedCost
		}
		histValues = append(histValues, bucketTotal)
	}

	mean, stddev := calcMeanStddev(histValues)

	if predictionIntervalLevel < forecastMinLevel {
		predictionIntervalLevel = forecastDefaultLevel
	}

	if predictionIntervalLevel > forecastMaxLevel {
		predictionIntervalLevel = forecastMaxLevel
	}

	z := forecastBaseZ + float64(predictionIntervalLevel-forecastDefaultLevel)*forecastZScalePerPct

	buckets := buildTimeBuckets(start, end, granularity)
	forecasts := make([]ForecastResult, 0, len(buckets))

	totalMean := mean * float64(len(buckets))

	for _, bucket := range buckets {
		lo := mean - z*stddev
		if lo < 0 {
			lo = 0
		}

		forecasts = append(forecasts, ForecastResult{
			TimePeriod: map[string]string{
				timePeriodKeyStart: bucket.start,
				timePeriodKeyEnd:   bucket.end,
			},
			MeanValue:                    fmt.Sprintf("%.4f", mean),
			PredictionIntervalLowerBound: fmt.Sprintf("%.4f", lo),
			PredictionIntervalUpperBound: fmt.Sprintf("%.4f", mean+z*stddev),
		})
	}

	return forecasts, totalMean, totalMean - z*stddev*float64(
			len(buckets),
		), totalMean + z*stddev*float64(
			len(buckets),
		)
}
