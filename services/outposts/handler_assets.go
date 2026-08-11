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
	q := r.URL.Query()

	f := assetInstanceFilter{
		accountIDs:    q["AccountIdFilter"],
		assetIDs:      q["AssetIdFilter"],
		awsServices:   q["AwsServiceFilter"],
		instanceTypes: q["InstanceTypeFilter"],
	}

	instances, err := h.Backend.ListAssetInstances(segs[1], f)
	if err != nil {
		return nil, err
	}

	resp := listAssetInstancesResponse{AssetInstances: make([]assetInstanceWire, 0, len(instances))}
	for _, ri := range instances {
		resp.AssetInstances = append(resp.AssetInstances, assetInstanceWire{
			AccountId:      ri.AccountID,
			AssetId:        ri.AssetID,
			AwsServiceName: awsServiceNameEC2,
			InstanceId:     ri.InstanceID,
			InstanceType:   ri.InstanceType,
		})
	}

	return marshalResponse(resp)
}
