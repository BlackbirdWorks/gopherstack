package outposts

import (
	"context"
	"net/http"
)

func (h *Handler) handleCreateQuote(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req createQuoteRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	q, err := h.Backend.CreateQuote(&req)
	if err != nil {
		return nil, err
	}

	wire := toQuoteWire(q)

	return marshalResponse(quoteResponse{Quote: &wire})
}

func (h *Handler) handleGetQuote(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	q, err := h.Backend.GetQuote(segs[1])
	if err != nil {
		return nil, err
	}

	wire := toQuoteWire(q)

	return marshalResponse(quoteResponse{Quote: &wire})
}

func (h *Handler) handleUpdateQuote(_ context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	var req updateQuoteRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	q, err := h.Backend.UpdateQuote(segs[1], &req)
	if err != nil {
		return nil, err
	}

	wire := toQuoteWire(q)

	return marshalResponse(quoteResponse{Quote: &wire})
}

func (h *Handler) handleDeleteQuote(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	if err := h.Backend.DeleteQuote(segs[1]); err != nil {
		return nil, err
	}

	return nil, nil // void response, matches services/grafana precedent
}

func (h *Handler) handleListQuotes(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()

	p := h.Backend.ListQuotes(q.Get("NextToken"), queryMaxResults(q))

	resp := listQuotesResponse{NextToken: p.Next, Quotes: make([]quoteWire, 0, len(p.Data))}
	for _, quote := range p.Data {
		resp.Quotes = append(resp.Quotes, toQuoteSummaryWire(quote))
	}

	return marshalResponse(resp)
}
