package outposts

import (
	"context"
	"net/http"
)

func (h *Handler) handleGetCatalogItem(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	item, err := h.Backend.GetCatalogItem(segs[2])
	if err != nil {
		return nil, err
	}

	wire := toCatalogItemWire(item)

	return marshalResponse(getCatalogItemResponse{CatalogItem: &wire})
}

func (h *Handler) handleListCatalogItems(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()
	f := catalogItemFilter{
		ec2Families:      q["EC2FamilyFilter"],
		itemClasses:      q["ItemClassFilter"],
		supportedStorage: q["SupportedStorageFilter"],
	}

	p := h.Backend.ListCatalogItems(f, q.Get("NextToken"), queryMaxResults(q))

	resp := listCatalogItemsResponse{NextToken: p.Next, CatalogItems: make([]catalogItemWire, 0, len(p.Data))}
	for _, c := range p.Data {
		resp.CatalogItems = append(resp.CatalogItems, toCatalogItemWire(c))
	}

	return marshalResponse(resp)
}

func (h *Handler) handleListOrderableInstanceTypes(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()

	p := h.Backend.ListOrderableInstanceTypes(q.Get("OutpostGenerationFilter"), q.Get("NextToken"), queryMaxResults(q))

	resp := listOrderableInstanceTypesResponse{
		NextToken:     p.Next,
		InstanceTypes: make([]detailedInstanceTypeItemWire, 0, len(p.Data)),
	}
	for _, it := range p.Data {
		resp.InstanceTypes = append(resp.InstanceTypes, toDetailedInstanceTypeItemWire(it))
	}

	return marshalResponse(resp)
}
