package rekognition

import (
	"context"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) collectionOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateCollection":   service.WrapOp(h.handleCreateCollection),
		"DeleteCollection":   service.WrapOp(h.handleDeleteCollection),
		"DescribeCollection": service.WrapOp(h.handleDescribeCollection),
		"ListCollections":    service.WrapOp(h.handleListCollections),
	}
}

// --- Collection requests ---

type createCollectionReq struct {
	Tags         map[string]string `json:"Tags"`
	CollectionID string            `json:"CollectionId"`
}

type createCollectionResp struct {
	CollectionArn    string `json:"CollectionArn"`
	FaceModelVersion string `json:"FaceModelVersion"`
	StatusCode       int    `json:"StatusCode"`
}

func (h *Handler) handleCreateCollection(_ context.Context, req *createCollectionReq) (*createCollectionResp, error) {
	if req.CollectionID == "" {
		return nil, fmt.Errorf("%w: CollectionId is required", ErrValidation)
	}

	coll, err := h.Backend.CreateCollection(req.CollectionID, req.Tags)
	if err != nil {
		return nil, err
	}

	return &createCollectionResp{
		CollectionArn:    coll.CollectionARN,
		FaceModelVersion: coll.FaceModelVersion,
		StatusCode:       http.StatusOK,
	}, nil
}

type deleteCollectionReq struct {
	CollectionID string `json:"CollectionId"`
}

type deleteCollectionResp struct {
	StatusCode int `json:"StatusCode"`
}

func (h *Handler) handleDeleteCollection(_ context.Context, req *deleteCollectionReq) (*deleteCollectionResp, error) {
	if req.CollectionID == "" {
		return nil, fmt.Errorf("%w: CollectionId is required", ErrValidation)
	}

	if err := h.Backend.DeleteCollection(req.CollectionID); err != nil {
		return nil, err
	}

	return &deleteCollectionResp{StatusCode: http.StatusOK}, nil
}

type describeCollectionReq struct {
	CollectionID string `json:"CollectionId"`
}

type describeCollectionResp struct {
	CollectionARN     string  `json:"CollectionARN"`
	FaceModelVersion  string  `json:"FaceModelVersion"`
	CreationTimestamp float64 `json:"CreationTimestamp"`
	FaceCount         int64   `json:"FaceCount"`
}

func (h *Handler) handleDescribeCollection(
	_ context.Context,
	req *describeCollectionReq,
) (*describeCollectionResp, error) {
	if req.CollectionID == "" {
		return nil, fmt.Errorf("%w: CollectionId is required", ErrValidation)
	}

	coll, err := h.Backend.DescribeCollection(req.CollectionID)
	if err != nil {
		return nil, err
	}

	faces, _, err := h.Backend.ListFaces(req.CollectionID, 0, "")
	if err != nil {
		return nil, err
	}

	return &describeCollectionResp{
		CollectionARN:     coll.CollectionARN,
		CreationTimestamp: epochSeconds(coll.CreationTimestamp),
		FaceCount:         int64(len(faces)),
		FaceModelVersion:  coll.FaceModelVersion,
	}, nil
}

type listCollectionsReq struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listCollectionsResp struct {
	NextToken         string   `json:"NextToken,omitempty"`
	CollectionIDs     []string `json:"CollectionIds"`
	FaceModelVersions []string `json:"FaceModelVersions"`
}

func (h *Handler) handleListCollections(_ context.Context, req *listCollectionsReq) (*listCollectionsResp, error) {
	colls, nextToken, err := h.Backend.ListCollections(req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(colls))
	versions := make([]string, 0, len(colls))

	for _, c := range colls {
		ids = append(ids, c.CollectionID)
		versions = append(versions, c.FaceModelVersion)
	}

	return &listCollectionsResp{
		CollectionIDs:     ids,
		FaceModelVersions: versions,
		NextToken:         nextToken,
	}, nil
}
