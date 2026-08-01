package outposts

import (
	"context"
	"net/http"
)

func (h *Handler) handleCreateOutpost(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req createOutpostRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	o, err := h.Backend.CreateOutpost(&req)
	if err != nil {
		return nil, err
	}

	wire := toOutpostWire(o)

	return marshalResponse(outpostResponse{Outpost: &wire})
}

func (h *Handler) handleGetOutpost(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	o, err := h.Backend.GetOutpost(segs[1])
	if err != nil {
		return nil, err
	}

	wire := toOutpostWire(o)

	return marshalResponse(outpostResponse{Outpost: &wire})
}

func (h *Handler) handleUpdateOutpost(_ context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	var req updateOutpostRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	o, err := h.Backend.UpdateOutpost(segs[1], &req)
	if err != nil {
		return nil, err
	}

	wire := toOutpostWire(o)

	return marshalResponse(outpostResponse{Outpost: &wire})
}

func (h *Handler) handleDeleteOutpost(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	if err := h.Backend.DeleteOutpost(segs[1]); err != nil {
		return nil, err
	}

	return nil, nil // void response, matches services/grafana precedent
}

func (h *Handler) handleListOutposts(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()
	f := listOutpostsFilter{
		availabilityZones:   q["AvailabilityZoneFilter"],
		availabilityZoneIDs: q["AvailabilityZoneIdFilter"],
		lifeCycleStatuses:   q["LifeCycleStatusFilter"],
	}

	p := h.Backend.ListOutposts(f, q.Get("NextToken"), queryMaxResults(q))

	resp := listOutpostsResponse{NextToken: p.Next, Outposts: make([]outpostWire, 0, len(p.Data))}
	for _, o := range p.Data {
		resp.Outposts = append(resp.Outposts, toOutpostWire(o))
	}

	return marshalResponse(resp)
}

func (h *Handler) handleStartOutpostDecommission(_ context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	var req struct {
		ValidateOnly bool `json:"ValidateOnly"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	status, err := h.Backend.StartOutpostDecommission(segs[1], req.ValidateOnly)
	if err != nil {
		return nil, err
	}

	return marshalResponse(startOutpostDecommissionResponse{
		Status:                status,
		BlockingResourceTypes: []string{},
	})
}

func (h *Handler) handleGetOutpostBillingInformation(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	o, err := h.Backend.GetOutpostBillingInformation(segs[1])
	if err != nil {
		return nil, err
	}

	resp := getOutpostBillingInformationResponse{
		PaymentOption: o.PaymentOption,
		PaymentTerm:   o.PaymentTerm,
		Subscriptions: make([]subscriptionWire, 0, len(o.Subscriptions)),
	}

	if !o.ContractEndDate.IsZero() {
		resp.ContractEndDate = o.ContractEndDate.Format("2006-01-02")
	}

	for _, s := range o.Subscriptions {
		resp.Subscriptions = append(resp.Subscriptions, toSubscriptionWire(s))
	}

	return marshalResponse(resp)
}

func (h *Handler) handleGetOutpostInstanceTypes(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	o, capacities, err := h.Backend.GetOutpostInstanceTypes(segs[1])
	if err != nil {
		return nil, err
	}

	resp := getOutpostInstanceTypesResponse{
		OutpostArn:    o.ARN,
		OutpostId:     o.ID,
		InstanceTypes: make([]instanceTypeItemWire, 0, len(capacities)),
	}

	for _, c := range capacities {
		item := instanceTypeItemWire{InstanceType: c.InstanceType}
		if it, ok := findOrderableInstanceType(c.InstanceType); ok {
			item.VCPUs = new(it.VCPUs)
		}

		resp.InstanceTypes = append(resp.InstanceTypes, item)
	}

	return marshalResponse(resp)
}

func (h *Handler) handleGetOutpostSupportedInstanceTypes(
	_ context.Context, r *http.Request, _ []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	q := r.URL.Query()

	types, err := h.Backend.GetOutpostSupportedInstanceTypes(segs[1], q.Get("AssetId"), q.Get("OrderId"))
	if err != nil {
		return nil, err
	}

	resp := getOutpostSupportedInstanceTypesResponse{InstanceTypes: make([]instanceTypeItemWire, 0, len(types))}
	for _, it := range types {
		resp.InstanceTypes = append(resp.InstanceTypes, toInstanceTypeItemWire(it))
	}

	return marshalResponse(resp)
}

func (h *Handler) handleGetRenewalPricing(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	result, options, err := h.Backend.GetRenewalPricing(segs[1])
	if err != nil {
		return nil, err
	}

	resp := getRenewalPricingResponse{PricingResult: result, PricingOptions: make([]pricingOptionWire, 0, len(options))}
	for _, p := range options {
		resp.PricingOptions = append(resp.PricingOptions, toPricingOptionWire(p))
	}

	return marshalResponse(resp)
}

func (h *Handler) handleCreateRenewal(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req createRenewalRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	result, err := h.Backend.CreateRenewal(req.OutpostIdentifier, &req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(createRenewalResponse{
		Currency:              result.Currency,
		OutpostId:             result.OutpostID,
		PaymentOption:         result.PaymentOption,
		PaymentTerm:           result.PaymentTerm,
		MonthlyRecurringPrice: result.MonthlyRecurringPrice,
		UpfrontPrice:          result.UpfrontPrice,
	})
}
