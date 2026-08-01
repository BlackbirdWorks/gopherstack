package outposts

import (
	"context"
	"net/http"
)

func (h *Handler) handleListAssets(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	q := r.URL.Query()

	f := assetFilter{
		assetTypes: q["AssetTypeFilter"],
		hostIDs:    q["HostIdFilter"],
		statuses:   q["StatusFilter"],
	}

	assets, err := h.Backend.ListAssets(segs[1], f)
	if err != nil {
		return nil, err
	}

	resp := listAssetsResponse{Assets: make([]assetInfoWire, 0, len(assets))}
	for _, a := range assets {
		resp.Assets = append(resp.Assets, toAssetInfoWire(a))
	}

	return marshalResponse(resp)
}

func (h *Handler) handleListAssetInstances(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	if err := h.Backend.ListAssetInstances(segs[1]); err != nil {
		return nil, err
	}

	return marshalResponse(listAssetInstancesResponse{AssetInstances: []assetInstanceWire{}})
}
