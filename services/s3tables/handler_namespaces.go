package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// createNamespaceRequest is the request body for CreateNamespace.
type createNamespaceRequest struct {
	Namespace []string `json:"namespace"`
}

func (h *Handler) handleCreateNamespace(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	var req createNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if len(req.Namespace) == 0 {
		return nil, fmt.Errorf("%w: namespace is required", errInvalidRequest)
	}

	ns, err := h.Backend.CreateNamespace(bucketARN, req.Namespace)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: created namespace", keyNamespace, joinNamespace(ns.Namespace), "bucket", bucketARN)

	return json.Marshal(map[string]any{
		keyNamespace:      ns.Namespace,
		keyTableBucketARN: ns.TableBucketARN,
	})
}

func (h *Handler) handleGetNamespace(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 3 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN or namespace", errInvalidRequest)
	}

	bucketARN := segs[1]
	nsName := segs[2]

	ns, err := h.Backend.GetNamespace(bucketARN, splitNamespace(nsName))
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got namespace", keyNamespace, nsName, "bucket", bucketARN)

	return json.Marshal(map[string]any{
		keyNamespace:      ns.Namespace,
		keyTableBucketARN: ns.TableBucketARN,
		keyCreatedAt:      ns.CreatedAt.UTC().Format("2006-01-02T15:04:05.999Z"),
		keyCreatedBy:      ns.CreatedBy,
		keyOwnerAccountID: ns.OwnerAccountID,
		keyNamespaceID:    ns.NamespaceID,
	})
}

func (h *Handler) handleDeleteNamespace(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 3 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN or namespace", errInvalidRequest)
	}

	bucketARN := segs[1]
	nsName := segs[2]

	if err := h.Backend.DeleteNamespace(bucketARN, splitNamespace(nsName)); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted namespace", keyNamespace, nsName, "bucket", bucketARN)

	return nil, nil
}

func (h *Handler) handleListNamespaces(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]
	q := r.URL.Query()

	pg, err := h.Backend.ListNamespaces(bucketARN, ListNamespacesParams{
		Prefix:            q.Get("prefix"),
		ContinuationToken: q.Get(keyContinuationToken),
		MaxNamespaces:     queryInt(q, "maxNamespaces"),
	})
	if err != nil {
		return nil, err
	}

	summaries := make([]map[string]any, 0, len(pg.Data))

	for _, ns := range pg.Data {
		summaries = append(summaries, map[string]any{
			keyNamespace:      ns.Namespace,
			keyTableBucketARN: ns.TableBucketARN,
			keyCreatedAt:      ns.CreatedAt.UTC().Format("2006-01-02T15:04:05.999Z"),
			keyCreatedBy:      ns.CreatedBy,
			keyOwnerAccountID: ns.OwnerAccountID,
			keyNamespaceID:    ns.NamespaceID,
		})
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: listed namespaces", "bucket", bucketARN, "count", len(summaries))

	resp := map[string]any{
		"namespaces": summaries,
	}
	if pg.Next != "" {
		resp[keyContinuationToken] = pg.Next
	}

	return json.Marshal(resp)
}
