package outposts

import (
	"context"
	"net/http"
)

func (h *Handler) handleCreateOrder(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req createOrderRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	o, err := h.Backend.CreateOrder(&req)
	if err != nil {
		return nil, err
	}

	wire := toOrderWire(o)

	return marshalResponse(orderResponse{Order: &wire})
}

func (h *Handler) handleGetOrder(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	o, err := h.Backend.GetOrder(segs[1])
	if err != nil {
		return nil, err
	}

	wire := toOrderWire(o)

	return marshalResponse(orderResponse{Order: &wire})
}

func (h *Handler) handleCancelOrder(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	if err := h.Backend.CancelOrder(segs[1]); err != nil {
		return nil, err
	}

	return nil, nil // void response, matches services/grafana precedent
}

func (h *Handler) handleListOrders(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()

	p := h.Backend.ListOrders(q.Get("OutpostIdentifierFilter"), q.Get("NextToken"), queryMaxResults(q))

	resp := listOrdersResponse{NextToken: p.Next, Orders: make([]orderSummaryWire, 0, len(p.Data))}
	for _, o := range p.Data {
		resp.Orders = append(resp.Orders, toOrderSummaryWire(o))
	}

	return marshalResponse(resp)
}
