package lightsail

import "context"

// bucketOps returns the dispatch table for family S (10 ops).
func (h *Handler) bucketOps() map[string]opFunc {
	return map[string]opFunc{
		"CreateBucket":               h.handleCreateBucket,
		"DeleteBucket":               h.handleDeleteBucket,
		"UpdateBucket":               h.handleUpdateBucket,
		"UpdateBucketBundle":         h.handleUpdateBucketBundle,
		"GetBuckets":                 h.handleGetBuckets,
		"SetResourceAccessForBucket": h.handleSetResourceAccessForBucket,
		"GetBucketMetricData":        h.handleGetBucketMetricData,
		"CreateBucketAccessKey":      h.handleCreateBucketAccessKey,
		"DeleteBucketAccessKey":      h.handleDeleteBucketAccessKey,
		"GetBucketAccessKeys":        h.handleGetBucketAccessKeys,
	}
}

type bucketWire struct {
	State                  *bucketStateWire      `json:"state,omitempty"`
	CreatedAt              *float64              `json:"createdAt,omitempty"`
	Location               *resourceLocationWire `json:"location,omitempty"`
	Arn                    string                `json:"arn,omitempty"`
	BundleID               string                `json:"bundleId,omitempty"`
	Name                   string                `json:"name,omitempty"`
	ObjectVersioning       string                `json:"objectVersioning,omitempty"`
	ResourceType           string                `json:"resourceType,omitempty"`
	SupportCode            string                `json:"supportCode,omitempty"`
	URL                    string                `json:"url,omitempty"`
	ReadonlyAccessAccounts []string              `json:"readonlyAccessAccounts,omitempty"`
	Tags                   []tagWire             `json:"tags,omitempty"`
	AbleToUpdateBundle     bool                  `json:"ableToUpdateBundle,omitempty"`
}

type bucketStateWire struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

func bucketToWire(bk *Bucket) bucketWire {
	return bucketWire{
		AbleToUpdateBundle:     bk.AbleToUpdateBundle,
		Arn:                    bk.Arn,
		BundleID:               bk.BundleID,
		CreatedAt:              epochPtr(bk.CreatedAt),
		Location:               locationToWire(bk.Location),
		Name:                   bk.Name,
		ObjectVersioning:       bk.ObjectVersioning,
		ReadonlyAccessAccounts: bk.ReadonlyAccessAccounts,
		ResourceType:           "Bucket",
		State:                  &bucketStateWire{Code: bk.State, Message: bk.StateMessage},
		SupportCode:            bk.SupportCode,
		Tags:                   mapFromTags(bk.Tags),
		URL:                    bk.URL,
	}
}

type createBucketRequest struct {
	BucketName             string    `json:"bucketName"`
	BundleID               string    `json:"bundleId"`
	Tags                   []tagWire `json:"tags,omitempty"`
	EnableObjectVersioning bool      `json:"enableObjectVersioning,omitempty"`
}

func (h *Handler) handleCreateBucket(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createBucketRequest](body)
	if err != nil {
		return nil, err
	}

	ops, createErr := h.Backend.CreateBucket(
		req.BucketName,
		req.BundleID,
		req.EnableObjectVersioning,
		tagsFromWire(req.Tags),
	)
	if createErr != nil {
		return nil, createErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type bucketNameRequest struct {
	BucketName string `json:"bucketName"`
}

type deleteBucketRequest struct {
	BucketName  string `json:"bucketName"`
	ForceDelete bool   `json:"forceDelete,omitempty"`
}

func (h *Handler) handleDeleteBucket(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[deleteBucketRequest](body)
	if err != nil {
		return nil, err
	}

	ops, delErr := h.Backend.DeleteBucket(req.BucketName, req.ForceDelete)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type updateBucketRequest struct {
	BucketName             string   `json:"bucketName"`
	Versioning             string   `json:"versioning,omitempty"`
	ReadonlyAccessAccounts []string `json:"readonlyAccessAccounts,omitempty"`
}

type bucketAndOpsResponse struct {
	Bucket     *bucketWire     `json:"bucket,omitempty"`
	Operations []operationWire `json:"operations,omitempty"`
}

func (h *Handler) handleUpdateBucket(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateBucketRequest](body)
	if err != nil {
		return nil, err
	}

	bk, ops, updateErr := h.Backend.UpdateBucket(req.BucketName, req.Versioning, req.ReadonlyAccessAccounts)
	if updateErr != nil {
		return nil, updateErr
	}

	w := bucketToWire(bk)

	return marshalResponse(bucketAndOpsResponse{Bucket: &w, Operations: operationsToWire(ops)})
}

type updateBucketBundleRequest struct {
	BucketName string `json:"bucketName"`
	BundleID   string `json:"bundleId"`
}

func (h *Handler) handleUpdateBucketBundle(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateBucketBundleRequest](body)
	if err != nil {
		return nil, err
	}

	ops, updateErr := h.Backend.UpdateBucketBundle(req.BucketName, req.BundleID)
	if updateErr != nil {
		return nil, updateErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type getBucketsRequest struct {
	BucketName string `json:"bucketName,omitempty"`
}

type bucketsListResponse struct {
	Buckets []bucketWire `json:"buckets,omitempty"`
}

func (h *Handler) handleGetBuckets(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getBucketsRequest](body)
	if err != nil {
		return nil, err
	}

	bks, getErr := h.Backend.GetBuckets(req.BucketName)
	if getErr != nil {
		return nil, getErr
	}

	out := make([]bucketWire, len(bks))
	for i, bk := range bks {
		out[i] = bucketToWire(bk)
	}

	return marshalResponse(bucketsListResponse{Buckets: out})
}

type setResourceAccessForBucketRequest struct {
	Access       string `json:"access"`
	BucketName   string `json:"bucketName"`
	ResourceName string `json:"resourceName"`
}

func (h *Handler) handleSetResourceAccessForBucket(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[setResourceAccessForBucketRequest](body)
	if err != nil {
		return nil, err
	}

	ops, setErr := h.Backend.SetResourceAccessForBucket(req.ResourceName, req.BucketName, req.Access)
	if setErr != nil {
		return nil, setErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type bucketMetricDataRequest struct {
	BucketName string `json:"bucketName"`
	MetricName string `json:"metricName,omitempty"`
}

type bucketMetricDataResponse struct {
	MetricName string     `json:"metricName,omitempty"`
	MetricData []struct{} `json:"metricData"`
}

func (h *Handler) handleGetBucketMetricData(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[bucketMetricDataRequest](body)
	if err != nil {
		return nil, err
	}

	if getErr := h.Backend.GetBucketMetricData(req.BucketName); getErr != nil {
		return nil, getErr
	}

	return marshalResponse(bucketMetricDataResponse{MetricData: []struct{}{}, MetricName: req.MetricName})
}

type accessKeyWire struct {
	AccessKeyID     string   `json:"accessKeyId,omitempty"`
	CreatedAt       *float64 `json:"createdAt,omitempty"`
	SecretAccessKey string   `json:"secretAccessKey,omitempty"`
	Status          string   `json:"status,omitempty"`
}

type accessKeyEnvelope struct {
	AccessKey  *accessKeyWire  `json:"accessKey,omitempty"`
	Operations []operationWire `json:"operations,omitempty"`
}

func (h *Handler) handleCreateBucketAccessKey(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[bucketNameRequest](body)
	if err != nil {
		return nil, err
	}

	key, ops, createErr := h.Backend.CreateBucketAccessKey(req.BucketName)
	if createErr != nil {
		return nil, createErr
	}

	w := &accessKeyWire{
		AccessKeyID:     key.AccessKeyID,
		CreatedAt:       epochPtr(key.CreatedAt),
		SecretAccessKey: key.SecretAccessKey,
		Status:          key.Status,
	}

	return marshalResponse(accessKeyEnvelope{AccessKey: w, Operations: operationsToWire(ops)})
}

type deleteBucketAccessKeyRequest struct {
	AccessKeyID string `json:"accessKeyId"`
	BucketName  string `json:"bucketName"`
}

func (h *Handler) handleDeleteBucketAccessKey(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[deleteBucketAccessKeyRequest](body)
	if err != nil {
		return nil, err
	}

	ops, delErr := h.Backend.DeleteBucketAccessKey(req.BucketName, req.AccessKeyID)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type bucketAccessKeysListResponse struct {
	AccessKeys []accessKeyWire `json:"accessKeys,omitempty"`
}

func (h *Handler) handleGetBucketAccessKeys(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[bucketNameRequest](body)
	if err != nil {
		return nil, err
	}

	keys, getErr := h.Backend.GetBucketAccessKeys(req.BucketName)
	if getErr != nil {
		return nil, getErr
	}

	out := make([]accessKeyWire, len(keys))
	for i, k := range keys {
		out[i] = accessKeyWire{AccessKeyID: k.AccessKeyID, CreatedAt: epochPtr(k.CreatedAt), Status: k.Status}
	}

	return marshalResponse(bucketAccessKeysListResponse{AccessKeys: out})
}
