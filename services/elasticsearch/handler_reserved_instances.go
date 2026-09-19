package elasticsearch

import (
	"net/http"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultReservedInstancePageSize is elasticsearchservice@v1.45.4's documented
// default for DescribeReservedElasticsearchInstanceOfferings/Instances.MaxResults
// ("If not specified, defaults to 100.").
const defaultReservedInstancePageSize = 100

func (h *Handler) handleDescribeReservedElasticsearchInstanceOfferings(w http.ResponseWriter, r *http.Request) {
	offerings := h.Backend.DescribeReservedElasticsearchInstanceOfferings()

	maxResults, _ := strconv.Atoi(r.URL.Query().Get("maxResults"))
	pg := page.New(offerings, r.URL.Query().Get("nextToken"), maxResults, defaultReservedInstancePageSize)

	result := make([]map[string]any, 0, len(pg.Data))
	for _, offering := range pg.Data {
		result = append(result, map[string]any{
			"ReservedElasticsearchInstanceOfferingId": offering.OfferingID,
			"ElasticsearchInstanceType":               offering.InstanceType,
			"PaymentOption":                           offering.PaymentOption,
			"CurrencyCode":                            offering.Currency,
			"FixedPrice":                              offering.FixedPrice,
			"UsagePrice":                              offering.UsagePrice,
			"Duration":                                offering.Duration,
		})
	}

	resp := map[string]any{"ReservedElasticsearchInstanceOfferings": result}
	if pg.Next != "" {
		resp["NextToken"] = pg.Next
	}

	h.writeJSON(r, w, resp)
}

func (h *Handler) handleDescribeReservedElasticsearchInstances(w http.ResponseWriter, r *http.Request) {
	instances := h.Backend.DescribeReservedElasticsearchInstances(h.reqContext(r))

	maxResults, _ := strconv.Atoi(r.URL.Query().Get("maxResults"))
	pg := page.New(instances, r.URL.Query().Get("nextToken"), maxResults, defaultReservedInstancePageSize)

	result := make([]map[string]any, 0, len(pg.Data))
	for _, instance := range pg.Data {
		result = append(result, map[string]any{
			"ReservedElasticsearchInstanceId":         instance.ReservationID,
			"ReservationName":                         instance.ReservationName,
			"ReservedElasticsearchInstanceOfferingId": instance.OfferingID,
			"ElasticsearchInstanceType":               instance.InstanceType,
			"State":                                   instance.State,
			"ElasticsearchInstanceCount":              instance.Count,
		})
	}

	resp := map[string]any{"ReservedElasticsearchInstances": result}
	if pg.Next != "" {
		resp["NextToken"] = pg.Next
	}

	h.writeJSON(r, w, resp)
}

func (h *Handler) handlePurchaseReservedElasticsearchInstanceOffering(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OfferingID      string `json:"ReservedElasticsearchInstanceOfferingId"`
		ReservationName string `json:"ReservationName"`
		InstanceCount   int    `json:"InstanceCount"`
	}
	if !h.decodeRequest(w, r, &req) {
		return
	}

	instance, err := h.Backend.PurchaseReservedElasticsearchInstanceOffering(
		h.reqContext(r),
		req.OfferingID,
		req.ReservationName,
		req.InstanceCount,
	)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{
		"ReservedElasticsearchInstanceId": instance.ReservationID,
		"ReservationName":                 instance.ReservationName,
	})
}
