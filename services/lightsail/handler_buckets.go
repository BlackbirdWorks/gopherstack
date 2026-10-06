package lightsail

import (
	"context"

	lstypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
)

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
	State                    *bucketStateWire              `json:"state,omitempty"`
	CreatedAt                *float64                      `json:"createdAt,omitempty"`
	Location                 *resourceLocationWire         `json:"location,omitempty"`
	Arn                      string                        `json:"arn,omitempty"`
	BundleID                 string                        `json:"bundleId,omitempty"`
	Name                     string                        `json:"name,omitempty"`
	ObjectVersioning         string                        `json:"objectVersioning,omitempty"`
	ResourceType             string                        `json:"resourceType,omitempty"`
	SupportCode              string                        `json:"supportCode,omitempty"`
	URL                      string                        `json:"url,omitempty"`
	Cors                     *bucketCorsWire               `json:"cors,omitempty"`
	AccessRules              *accessRulesWire              `json:"accessRules,omitempty"`
	AccessLogConfig          *bucketAccessLogConfigWire    `json:"accessLogConfig,omitempty"`
	ReadonlyAccessAccounts   []string                      `json:"readonlyAccessAccounts,omitempty"`
	ResourcesReceivingAccess []resourceReceivingAccessWire `json:"resourcesReceivingAccess,omitempty"`
	Tags                     []tagWire                     `json:"tags,omitempty"`
	AbleToUpdateBundle       bool                          `json:"ableToUpdateBundle,omitempty"`
}

// accessRulesWire mirrors types.AccessRules.
type accessRulesWire struct {
	AllowPublicOverrides *bool  `json:"allowPublicOverrides,omitempty"`
	GetObject            string `json:"getObject,omitempty"`
}

// bucketAccessLogConfigWire mirrors types.BucketAccessLogConfig.
type bucketAccessLogConfigWire struct {
	Enabled     *bool  `json:"enabled"`
	Destination string `json:"destination,omitempty"`
	Prefix      string `json:"prefix,omitempty"`
}

func (w *accessRulesWire) toUpdate() *BucketAccessRulesUpdate {
	if w == nil {
		return nil
	}

	return &BucketAccessRulesUpdate{GetObject: w.GetObject, AllowPublicOverrides: w.AllowPublicOverrides}
}

func (w *bucketAccessLogConfigWire) toModel() (*BucketAccessLogConfig, error) {
	if w == nil {
		return nil, nil //nolint:nilnil // absent AccessLogConfig is not an error
	}

	if w.Enabled == nil {
		return nil, validationError("AccessLogConfig.Enabled is required")
	}

	return &BucketAccessLogConfig{Enabled: *w.Enabled, Destination: w.Destination, Prefix: w.Prefix}, nil
}

func accessRulesToWire(r *BucketAccessRules) *accessRulesWire {
	rules := defaultBucketAccessRules()
	if r != nil {
		rules = *r
	}

	return &accessRulesWire{GetObject: rules.GetObject, AllowPublicOverrides: &rules.AllowPublicOverrides}
}

func accessLogConfigToWire(c *BucketAccessLogConfig) *bucketAccessLogConfigWire {
	if c == nil {
		return nil
	}

	return &bucketAccessLogConfigWire{Enabled: &c.Enabled, Destination: c.Destination, Prefix: c.Prefix}
}

type bucketCorsWire struct {
	Rules []bucketCorsRuleWire `json:"rules"`
}

type bucketCorsRuleWire struct {
	MaxAgeSeconds  *int32   `json:"maxAgeSeconds,omitempty"`
	ID             string   `json:"id,omitempty"`
	AllowedMethods []string `json:"allowedMethods"`
	AllowedOrigins []string `json:"allowedOrigins"`
	AllowedHeaders []string `json:"allowedHeaders,omitempty"`
	ExposeHeaders  []string `json:"exposeHeaders,omitempty"`
}

func (w *bucketCorsWire) toModel() *BucketCORS {
	if w == nil {
		return nil
	}

	out := &BucketCORS{Rules: make([]BucketCORSRule, len(w.Rules))}
	for i, r := range w.Rules {
		out.Rules[i] = BucketCORSRule(r)
	}

	return out
}

func bucketCorsToWire(c *BucketCORS) *bucketCorsWire {
	if c == nil {
		return nil
	}

	out := &bucketCorsWire{Rules: make([]bucketCorsRuleWire, len(c.Rules))}
	for i, r := range c.Rules {
		out.Rules[i] = bucketCorsRuleWire(r)
	}

	return out
}

type bucketStateWire struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// resourceReceivingAccessWire mirrors types.ResourceReceivingAccess.
type resourceReceivingAccessWire struct {
	Name         string `json:"name,omitempty"`
	ResourceType string `json:"resourceType,omitempty"`
}

func resourcesReceivingAccessToWire(in []ResourceReceivingAccess) []resourceReceivingAccessWire {
	if len(in) == 0 {
		return nil
	}

	out := make([]resourceReceivingAccessWire, len(in))
	for i, r := range in {
		out[i] = resourceReceivingAccessWire(r)
	}

	return out
}

func bucketToWire(bk *Bucket) bucketWire {
	return bucketWire{
		AbleToUpdateBundle:       bk.AbleToUpdateBundle,
		Arn:                      bk.Arn,
		BundleID:                 bk.BundleID,
		CreatedAt:                epochPtr(bk.CreatedAt),
		Location:                 locationToWire(bk.Location),
		Name:                     bk.Name,
		ObjectVersioning:         bk.ObjectVersioning,
		Cors:                     bucketCorsToWire(bk.CORS),
		AccessRules:              accessRulesToWire(bk.AccessRules),
		AccessLogConfig:          accessLogConfigToWire(bk.AccessLogConfig),
		ReadonlyAccessAccounts:   bk.ReadonlyAccessAccounts,
		ResourcesReceivingAccess: resourcesReceivingAccessToWire(bk.ResourcesReceivingAccess),
		ResourceType:             "Bucket",
		State:                    &bucketStateWire{Code: bk.State, Message: bk.StateMessage},
		SupportCode:              bk.SupportCode,
		Tags:                     mapFromTags(bk.Tags),
		URL:                      bk.URL,
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
	Cors                   *bucketCorsWire            `json:"cors,omitempty"`
	AccessRules            *accessRulesWire           `json:"accessRules,omitempty"`
	AccessLogConfig        *bucketAccessLogConfigWire `json:"accessLogConfig,omitempty"`
	BucketName             string                     `json:"bucketName"`
	Versioning             string                     `json:"versioning,omitempty"`
	ReadonlyAccessAccounts []string                   `json:"readonlyAccessAccounts,omitempty"`
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

	logCfg, err := req.AccessLogConfig.toModel()
	if err != nil {
		return nil, err
	}

	bk, ops, updateErr := h.Backend.UpdateBucket(req.BucketName, BucketUpdate{
		Versioning:             req.Versioning,
		ReadonlyAccessAccounts: req.ReadonlyAccessAccounts,
		CORS:                   req.Cors.toModel(),
		AccessRules:            req.AccessRules.toUpdate(),
		AccessLogConfig:        logCfg,
	})
	if updateErr != nil {
		return nil, updateErr
	}

	w := bucketToWire(bk)
	if req.Cors == nil {
		w.Cors = nil
	}

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
	BucketName                string `json:"bucketName,omitempty"`
	PageToken                 string `json:"pageToken,omitempty"`
	IncludeCors               bool   `json:"includeCors,omitempty"`
	IncludeConnectedResources bool   `json:"includeConnectedResources,omitempty"`
}

type bucketsListResponse struct {
	NextPageToken string       `json:"nextPageToken,omitempty"`
	Buckets       []bucketWire `json:"buckets,omitempty"`
}

func (h *Handler) handleGetBuckets(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getBucketsRequest](body)
	if err != nil {
		return nil, err
	}

	pg, getErr := h.Backend.GetBuckets(req.BucketName, req.PageToken)
	if getErr != nil {
		return nil, getErr
	}

	out := make([]bucketWire, len(pg.Data))
	for i, bk := range pg.Data {
		out[i] = bucketToWire(bk)
		if !req.IncludeCors || req.BucketName == "" {
			out[i].Cors = nil
		}

		if !req.IncludeConnectedResources {
			out[i].ResourcesReceivingAccess = nil
		}
	}

	return marshalResponse(bucketsListResponse{Buckets: out, NextPageToken: pg.Next})
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

	if vErr := checkEnum("access", lstypes.ResourceBucketAccess(req.Access)); vErr != nil {
		return nil, vErr
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

	if vErr := checkEnum("metricName", lstypes.BucketMetricName(req.MetricName)); vErr != nil {
		return nil, vErr
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
