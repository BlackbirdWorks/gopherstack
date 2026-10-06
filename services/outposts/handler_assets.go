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

	p, err := paginate(assets, q)
	if err != nil {
		return nil, err
	}

	resp := listAssetsResponse{NextToken: p.Next, Assets: make([]assetInfoWire, 0, len(p.Data))}
	for _, a := range p.Data {
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

	p, err := paginate(instances, q)
	if err != nil {
		return nil, err
	}

	resp := listAssetInstancesResponse{NextToken: p.Next, AssetInstances: make([]assetInstanceWire, 0, len(p.Data))}
	for _, ri := range p.Data {
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
