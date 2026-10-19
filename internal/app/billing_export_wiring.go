package app

import (
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	bcmbackend "github.com/blackbirdworks/gopherstack/services/bcmdataexports"
	cebackend "github.com/blackbirdworks/gopherstack/services/ce"
	curbackend "github.com/blackbirdworks/gopherstack/services/cur"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const (
	ceDayLayout = "2006-01-02"
	ceDimension = "DIMENSION"
)

// ceCostSource derives daily cost lines from the Cost Explorer ledger.
type ceCostSource struct{ ce *cebackend.InMemoryBackend }

func (c ceCostSource) CostLines(start, end time.Time) []billingexport.CostLine {
	groupBy := []cebackend.GroupBySpec{
		{Type: ceDimension, Key: "SERVICE"},
		{Type: ceDimension, Key: "REGION"},
		{Type: ceDimension, Key: "USAGE_TYPE"},
		{Type: ceDimension, Key: "LINKED_ACCOUNT"},
	}
	metrics := []string{"BlendedCost", "UnblendedCost", "UsageQuantity"}

	var lines []billingexport.CostLine

	period := c.ce.GetCostAndUsage(start.Format(ceDayLayout), end.Format(ceDayLayout), "DAILY", metrics, groupBy, nil)

	for _, r := range period {
		day, err := time.Parse(ceDayLayout, r.TimePeriod["Start"])
		if err != nil {
			continue
		}

		for _, g := range r.Groups {
			if len(g.Keys) < len(groupBy) {
				continue
			}

			lines = append(lines, billingexport.CostLine{
				Day: day, Service: g.Keys[0], Region: g.Keys[1], UsageType: g.Keys[2], Account: g.Keys[3],
				Blended:   metricAmount(g.Metrics, "BlendedCost"),
				Unblended: metricAmount(g.Metrics, "UnblendedCost"),
				Usage:     metricAmount(g.Metrics, "UsageQuantity"),
			})
		}
	}

	return lines
}

func metricAmount(m map[string]cebackend.MetricValue, name string) float64 {
	v, _ := strconv.ParseFloat(m[name].Amount, 64)

	return v
}

// wireBillingExports connects CUR and Data Exports to S3 (delivery sink) and Cost Explorer (row source).
func wireBillingExports(byName map[string]service.Registerable) {
	var store billingexport.ObjectStore

	if s3H, ok := byName["S3"].(*s3backend.S3Handler); ok {
		if bk, bkOk := s3H.Backend.(*s3backend.InMemoryBackend); bkOk {
			store = bk
		}
	}

	var costs billingexport.CostSource

	if ceH, ok := byName["Ce"].(*cebackend.Handler); ok {
		costs = ceCostSource{ce: ceH.Backend}
	}

	if h, ok := byName["CUR"].(*curbackend.Handler); ok {
		if store != nil {
			h.Backend.SetObjectStore(store)
		}

		if costs != nil {
			h.Backend.SetCostSource(costs)
		}
	}

	if h, ok := byName["BCMDataExports"].(*bcmbackend.Handler); ok {
		if store != nil {
			h.Backend.SetObjectStore(store)
		}

		if costs != nil {
			h.Backend.SetCostSource(costs)
		}
	}
}
