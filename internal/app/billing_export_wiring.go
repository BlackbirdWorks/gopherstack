package app

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	bcmbackend "github.com/blackbirdworks/gopherstack/services/bcmdataexports"
	cebackend "github.com/blackbirdworks/gopherstack/services/ce"
	curbackend "github.com/blackbirdworks/gopherstack/services/cur"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const ceDayLayout = "2006-01-02"

// ceCostSource derives daily cost lines from the Cost Explorer ledger.
type ceCostSource struct{ ce *cebackend.InMemoryBackend }

func (c ceCostSource) CostLines(start, end time.Time) []billingexport.CostLine {
	entries := c.ce.LedgerEntries(start.Format(ceDayLayout), end.Format(ceDayLayout))
	lines := make([]billingexport.CostLine, 0, len(entries))

	for i := range entries {
		e := &entries[i]

		day, err := time.Parse(ceDayLayout, e.Date)
		if err != nil {
			continue
		}

		lines = append(lines, billingexport.CostLine{
			Day: day, Service: e.Service, Region: e.Region, UsageType: e.UsageType, Account: e.Account,
			AvailabilityZone: e.AvailabilityZone, InstanceType: e.InstanceType, Operation: e.Operation,
			ChargeType: e.ChargeType, PurchaseType: e.PurchaseType, ProductFamily: e.ProductFamily,
			UsageTypeGroup: e.UsageTypeGroup, ResourceID: e.ResourceID, Tags: e.Tags,
			Blended: e.BlendedCost, Unblended: e.UnblendedCost, Usage: e.UsageQuantity,
		})
	}

	return lines
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
