package elasticsearch

import "net/http"

func (h *Handler) handleDescribeReservedElasticsearchInstanceOfferings(w http.ResponseWriter, r *http.Request) {
	offerings := h.Backend.DescribeReservedElasticsearchInstanceOfferings()
	result := make([]map[string]any, 0, len(offerings))
	for _, offering := range offerings {
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

	h.writeJSON(r, w, map[string]any{
		"ReservedElasticsearchInstanceOfferings": result,
	})
}

func (h *Handler) handleDescribeReservedElasticsearchInstances(w http.ResponseWriter, r *http.Request) {
	instances := h.Backend.DescribeReservedElasticsearchInstances(h.reqContext(r))
	result := make([]map[string]any, 0, len(instances))
	for _, instance := range instances {
		result = append(result, map[string]any{
			"ReservedElasticsearchInstanceId":         instance.ReservationID,
			"ReservationName":                         instance.ReservationName,
			"ReservedElasticsearchInstanceOfferingId": instance.OfferingID,
			"ElasticsearchInstanceType":               instance.InstanceType,
			"State":                                   instance.State,
			"ElasticsearchInstanceCount":              instance.Count,
		})
	}

	h.writeJSON(r, w, map[string]any{
		"ReservedElasticsearchInstances": result,
	})
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
