package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// createTableBucketRequest is the request body for CreateTableBucket. Real
// CreateTableBucketInput also accepts encryptionConfiguration,
// storageClassConfiguration, and tags alongside the required name -- see
// CreateTableBucketInput in aws-sdk-go-v2/service/s3tables.
type createTableBucketRequest struct {
	EncryptionConfiguration   map[string]any    `json:"encryptionConfiguration"`
	StorageClassConfiguration map[string]any    `json:"storageClassConfiguration"`
	Tags                      map[string]string `json:"tags"`
	Name                      string            `json:"name"`
}

func (h *Handler) handleCreateTableBucket(ctx context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req createTableBucketRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	tb, err := h.Backend.CreateTableBucket(req.Name, CreateTableBucketOptions{
		Encryption:   req.EncryptionConfiguration,
		StorageClass: storageClassFromConfig(req.StorageClassConfiguration),
		Tags:         req.Tags,
	})
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: created table bucket", keyName, tb.Name, keyArn, tb.ARN)

	return json.Marshal(map[string]string{
		keyArn: tb.ARN,
	})
}

func (h *Handler) handleGetTableBucket(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	tb, err := h.Backend.GetTableBucket(bucketARN)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table bucket", keyArn, tb.ARN)

	return json.Marshal(map[string]any{
		keyArn:            tb.ARN,
		keyName:           tb.Name,
		keyOwnerAccountID: tb.OwnerAccountID,
		keyCreatedAt:      tb.CreatedAt.Format("2006-01-02T15:04:05.999Z"),
		keyType:           bucketTypeCustomer,
	})
}

func (h *Handler) handleDeleteTableBucket(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	if err := h.Backend.DeleteTableBucket(bucketARN); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted table bucket", keyArn, bucketARN)

	return nil, nil
}

func (h *Handler) handleListTableBuckets(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()

	pg, err := h.Backend.ListTableBuckets(ListTableBucketsParams{
		Prefix:            q.Get("prefix"),
		Type:              q.Get(keyType),
		ContinuationToken: q.Get(keyContinuationToken),
		MaxBuckets:        queryInt(q, "maxBuckets"),
	})
	if err != nil {
		return nil, err
	}

	summaries := make([]map[string]any, 0, len(pg.Data))

	for _, tb := range pg.Data {
		summaries = append(summaries, map[string]any{
			keyArn:            tb.ARN,
			keyName:           tb.Name,
			keyOwnerAccountID: tb.OwnerAccountID,
			keyCreatedAt:      tb.CreatedAt.Format("2006-01-02T15:04:05.999Z"),
			keyType:           bucketTypeCustomer,
		})
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: listed table buckets", "count", len(summaries))

	resp := map[string]any{
		"tableBuckets": summaries,
	}
	if pg.Next != "" {
		resp[keyContinuationToken] = pg.Next
	}

	return json.Marshal(resp)
}

func (h *Handler) handleGetTableBucketMaintenanceConfiguration(
	ctx context.Context,
	r *http.Request,
	_ []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	cfg, err := h.Backend.GetTableBucketMaintenanceConfiguration(bucketARN)
	if err != nil {
		return nil, err
	}

	if cfg == nil {
		cfg = make(map[string]any)
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table bucket maintenance configuration", keyArn, bucketARN)

	return json.Marshal(map[string]any{
		keyTableBucketARN: bucketARN,
		keyConfiguration:  cfg,
	})
}

// putTableBucketMaintenanceRequest is the request body for PutTableBucketMaintenanceConfiguration.
type putTableBucketMaintenanceRequest struct {
	Value map[string]any `json:"value"`
}

func (h *Handler) handlePutTableBucketMaintenanceConfiguration(
	ctx context.Context,
	r *http.Request,
	body []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 4 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN or type", errInvalidRequest)
	}

	bucketARN := segs[1]
	maintenanceType := segs[3]

	var req putTableBucketMaintenanceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if maintenanceType != maintenanceTypeIcebergUnreferencedFileRemoval {
		return nil, fmt.Errorf("%w: unsupported table bucket maintenance type %q", errInvalidRequest, maintenanceType)
	}

	if req.Value == nil {
		return nil, fmt.Errorf("%w: value is required", errInvalidRequest)
	}

	if err := h.Backend.PutTableBucketMaintenanceConfiguration(bucketARN, maintenanceType, req.Value); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"s3tables: put table bucket maintenance configuration",
		keyArn,
		bucketARN,
		"type",
		maintenanceType,
	)

	return nil, nil
}

func (h *Handler) handleGetTableBucketEncryption(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	tb, err := h.Backend.GetTableBucket(bucketARN)
	if err != nil {
		return nil, err
	}

	if tb.Encryption == nil {
		return nil, awserr.ErrNotFound
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table bucket encryption", keyArn, bucketARN)

	return json.Marshal(map[string]any{
		"encryptionConfiguration": tb.Encryption,
	})
}

func (h *Handler) handleDeleteTableBucketEncryption(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	if err := h.Backend.DeleteTableBucketEncryption(bucketARN); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted table bucket encryption", keyArn, bucketARN)

	return nil, nil
}

func (h *Handler) handleGetTableBucketMetricsConfiguration(
	ctx context.Context,
	r *http.Request,
	_ []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	configID, enabled, err := h.Backend.GetTableBucketMetricsConfiguration(bucketARN)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table bucket metrics configuration", keyArn, bucketARN)

	resp := map[string]any{
		keyTableBucketARN: bucketARN,
	}
	if enabled {
		resp["id"] = configID
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteTableBucketMetricsConfiguration(
	ctx context.Context,
	r *http.Request,
	_ []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	if err := h.Backend.DeleteTableBucketMetricsConfiguration(bucketARN); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted table bucket metrics configuration", keyArn, bucketARN)

	return nil, nil
}

func (h *Handler) handleGetTableBucketStorageClass(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	tb, err := h.Backend.GetTableBucket(bucketARN)
	if err != nil {
		return nil, err
	}

	sc := tb.StorageClass
	if sc == "" {
		sc = storageClassStandard
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table bucket storage class", keyArn, bucketARN)

	return json.Marshal(map[string]any{
		keyTableBucketARN: bucketARN,
		"storageClass":    sc,
	})
}

func (h *Handler) handleGetTableBucketReplication(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	bucketARN := r.URL.Query().Get(keyTableBucketARN)
	if bucketARN == "" {
		return nil, fmt.Errorf("%w: tableBucketARN is required", errInvalidRequest)
	}

	cfg, err := h.Backend.GetTableBucketReplication(bucketARN)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table bucket replication", keyArn, bucketARN)

	return json.Marshal(map[string]any{
		keyTableBucketARN: bucketARN,
		"replicationConfiguration": map[string]any{
			"destinations": cfg.Destinations,
		},
	})
}

func (h *Handler) handleDeleteTableBucketReplication(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	bucketARN := r.URL.Query().Get(keyTableBucketARN)
	if bucketARN == "" {
		return nil, fmt.Errorf("%w: tableBucketARN is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTableBucketReplication(bucketARN); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted table bucket replication", keyArn, bucketARN)

	return nil, nil
}

func (h *Handler) handleGetTableBucketPolicy(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	policy, err := h.Backend.GetTableBucketPolicy(bucketARN)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table bucket policy", keyArn, bucketARN)

	return json.Marshal(map[string]string{
		"resourcePolicy": policy,
	})
}

// putTableBucketPolicyRequest is the request body for PutTableBucketPolicy.
type putTableBucketPolicyRequest struct {
	ResourcePolicy string `json:"resourcePolicy"`
}

func (h *Handler) handlePutTableBucketPolicy(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	var req putTableBucketPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.PutTableBucketPolicy(bucketARN, req.ResourcePolicy); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table bucket policy", keyArn, bucketARN)

	return nil, nil
}

func (h *Handler) handleDeleteTableBucketPolicy(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	if err := h.Backend.DeleteTableBucketPolicy(bucketARN); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted table bucket policy", keyArn, bucketARN)

	return nil, nil
}

// putTableBucketEncryptionRequest is the request body for PutTableBucketEncryption.
type putTableBucketEncryptionRequest struct {
	EncryptionConfiguration map[string]any `json:"encryptionConfiguration"`
}

func (h *Handler) handlePutTableBucketEncryption(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	var req putTableBucketEncryptionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.PutTableBucketEncryption(bucketARN, req.EncryptionConfiguration); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table bucket encryption", keyArn, bucketARN)

	return nil, nil
}

func (h *Handler) handlePutTableBucketMetricsConfiguration(
	ctx context.Context,
	r *http.Request,
	_ []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	if err := h.Backend.PutTableBucketMetricsConfiguration(bucketARN); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table bucket metrics configuration", keyArn, bucketARN)

	return nil, nil
}

// putTableBucketStorageClassRequest is the request body for PutTableBucketStorageClass.
type putTableBucketStorageClassRequest struct {
	StorageClassConfiguration map[string]any `json:"storageClassConfiguration"`
}

func (h *Handler) handlePutTableBucketStorageClass(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	var req putTableBucketStorageClassRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	storageClass := storageClassFromConfig(req.StorageClassConfiguration)
	if storageClass == "" {
		storageClass = storageClassStandard
	}

	if err := h.Backend.PutTableBucketStorageClass(bucketARN, storageClass); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table bucket storage class", keyArn, bucketARN)

	return nil, nil
}

// putTableBucketReplicationRequest is the request body for PutTableBucketReplication.
type putTableBucketReplicationRequest struct {
	Configuration map[string]any `json:"configuration"`
}

func (h *Handler) handlePutTableBucketReplication(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	bucketARN := r.URL.Query().Get(keyTableBucketARN)
	if bucketARN == "" {
		return nil, fmt.Errorf("%w: tableBucketARN is required", errInvalidRequest)
	}

	var req putTableBucketReplicationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cfg := &BucketReplicationConfig{
		Destinations: parseBucketReplicationDestinations(req.Configuration),
	}

	if err := h.Backend.PutTableBucketReplication(bucketARN, cfg); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table bucket replication", keyArn, bucketARN)

	return nil, nil
}
