package rekognition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	rekognitionTargetPrefix = "RekognitionService."
	rekognitionContentType  = "application/x-amz-json-1.1"
	rekognitionServiceName  = "Rekognition"

	keyTypeField    = "__type"
	keyMessageField = "message"
)

// Handler handles Rekognition HTTP requests using X-Amz-Target routing.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	h := &Handler{Backend: b}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return rekognitionServiceName }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// MatchPriority returns routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// RouteMatcher returns a matcher that accepts Rekognition X-Amz-Target headers.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), rekognitionTargetPrefix)
	}
}

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, rekognitionTargetPrefix)

	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the resource identifier from the request body.
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for name := range h.ops {
		ops = append(ops, name)
	}

	return ops
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			rekognitionServiceName, rekognitionContentType,
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	ops := map[string]service.JSONOpFunc{
		"CreateCollection":        service.WrapOp(h.handleCreateCollection),
		"DeleteCollection":        service.WrapOp(h.handleDeleteCollection),
		"DescribeCollection":      service.WrapOp(h.handleDescribeCollection),
		"ListCollections":         service.WrapOp(h.handleListCollections),
		"IndexFaces":              service.WrapOp(h.handleIndexFaces),
		"DeleteFaces":             service.WrapOp(h.handleDeleteFaces),
		"ListFaces":               service.WrapOp(h.handleListFaces),
		"SearchFaces":             service.WrapOp(h.handleSearchFaces),
		"SearchFacesByImage":      service.WrapOp(h.handleSearchFacesByImage),
		"CreateStreamProcessor":   service.WrapOp(h.handleCreateStreamProcessor),
		"DeleteStreamProcessor":   service.WrapOp(h.handleDeleteStreamProcessor),
		"DescribeStreamProcessor": service.WrapOp(h.handleDescribeStreamProcessor),
		"ListStreamProcessors":    service.WrapOp(h.handleListStreamProcessors),
		"StartStreamProcessor":    service.WrapOp(h.handleStartStreamProcessor),
		"StopStreamProcessor":     service.WrapOp(h.handleStopStreamProcessor),
		"UpdateStreamProcessor":   service.WrapOp(h.handleUpdateStreamProcessor),
		"TagResource":             service.WrapOp(h.handleTagResource),
		"UntagResource":           service.WrapOp(h.handleUntagResource),
		"ListTagsForResource":     service.WrapOp(h.handleListTagsForResource),
	}

	maps.Copy(ops, h.appendixAOps())

	return ops
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: operation %q not implemented", ErrUnknownOperation, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ResourceNotFoundException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ResourceAlreadyExistsException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "InvalidParameterException",
			keyMessageField: err.Error(),
		})
	default:
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "UnknownOperationException",
			keyMessageField: err.Error(),
		})
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
	CollectionARN     string `json:"CollectionARN"`
	CreationTimestamp string `json:"CreationTimestamp"`
	FaceModelVersion  string `json:"FaceModelVersion"`
	FaceCount         int64  `json:"FaceCount"`
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
		CreationTimestamp: coll.CreationTimestamp.Format("2006-01-02T15:04:05.000Z"),
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

// --- Face requests ---

type indexFacesReq struct {
	CollectionID    string `json:"CollectionId"`
	ExternalImageID string `json:"ExternalImageId"`
}

type faceRecord struct {
	Face struct {
		FaceID          string  `json:"FaceId"`
		ImageID         string  `json:"ImageId"`
		ExternalImageID string  `json:"ExternalImageId"`
		Confidence      float64 `json:"Confidence"`
	} `json:"Face"`
}

type indexFacesResp struct {
	FaceModelVersion string       `json:"FaceModelVersion"`
	FaceRecords      []faceRecord `json:"FaceRecords"`
}

func (h *Handler) handleIndexFaces(_ context.Context, req *indexFacesReq) (*indexFacesResp, error) {
	if req.CollectionID == "" {
		return nil, fmt.Errorf("%w: CollectionId is required", ErrValidation)
	}

	faces, err := h.Backend.IndexFaces(req.CollectionID, req.ExternalImageID)
	if err != nil {
		return nil, err
	}

	records := make([]faceRecord, 0, len(faces))

	for _, f := range faces {
		var rec faceRecord
		rec.Face.FaceID = f.FaceID
		rec.Face.ImageID = f.ImageID
		rec.Face.ExternalImageID = f.ExternalImageID
		rec.Face.Confidence = f.Confidence
		records = append(records, rec)
	}

	return &indexFacesResp{
		FaceModelVersion: faceModelVersion,
		FaceRecords:      records,
	}, nil
}

type deleteFacesReq struct {
	CollectionID string   `json:"CollectionId"`
	FaceIDs      []string `json:"FaceIds"`
}

type deleteFacesResp struct {
	DeletedFaces []string `json:"DeletedFaces"`
}

func (h *Handler) handleDeleteFaces(_ context.Context, req *deleteFacesReq) (*deleteFacesResp, error) {
	if req.CollectionID == "" {
		return nil, fmt.Errorf("%w: CollectionId is required", ErrValidation)
	}

	deleted, err := h.Backend.DeleteFaces(req.CollectionID, req.FaceIDs)
	if err != nil {
		return nil, err
	}

	if deleted == nil {
		deleted = []string{}
	}

	return &deleteFacesResp{DeletedFaces: deleted}, nil
}

type listFacesReq struct {
	CollectionID string `json:"CollectionId"`
	NextToken    string `json:"NextToken"`
	MaxResults   int32  `json:"MaxResults"`
}

type faceEntry struct {
	FaceID          string  `json:"FaceId"`
	ImageID         string  `json:"ImageId"`
	ExternalImageID string  `json:"ExternalImageId"`
	Confidence      float64 `json:"Confidence"`
}

type listFacesResp struct {
	FaceModelVersion string      `json:"FaceModelVersion"`
	NextToken        string      `json:"NextToken,omitempty"`
	Faces            []faceEntry `json:"Faces"`
}

func (h *Handler) handleListFaces(_ context.Context, req *listFacesReq) (*listFacesResp, error) {
	if req.CollectionID == "" {
		return nil, fmt.Errorf("%w: CollectionId is required", ErrValidation)
	}

	faces, nextToken, err := h.Backend.ListFaces(req.CollectionID, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	entries := make([]faceEntry, 0, len(faces))

	for _, f := range faces {
		entries = append(entries, faceEntry{
			FaceID:          f.FaceID,
			ImageID:         f.ImageID,
			ExternalImageID: f.ExternalImageID,
			Confidence:      f.Confidence,
		})
	}

	return &listFacesResp{
		FaceModelVersion: faceModelVersion,
		Faces:            entries,
		NextToken:        nextToken,
	}, nil
}

type searchFacesReq struct {
	CollectionID string `json:"CollectionId"`
	FaceID       string `json:"FaceId"`
	MaxFaces     int32  `json:"MaxFaces"`
}

type faceMatchEntry struct {
	Face       faceEntry `json:"Face"`
	Similarity float64   `json:"Similarity"`
}

type searchFacesResp struct {
	FaceModelVersion string           `json:"FaceModelVersion"`
	SearchedFaceID   string           `json:"SearchedFaceId"`
	FaceMatches      []faceMatchEntry `json:"FaceMatches"`
}

func (h *Handler) handleSearchFaces(_ context.Context, req *searchFacesReq) (*searchFacesResp, error) {
	if req.CollectionID == "" {
		return nil, fmt.Errorf("%w: CollectionId is required", ErrValidation)
	}

	if req.FaceID == "" {
		return nil, fmt.Errorf("%w: FaceId is required", ErrValidation)
	}

	matches, err := h.Backend.SearchFaces(req.CollectionID, req.FaceID, req.MaxFaces)
	if err != nil {
		return nil, err
	}

	entries := make([]faceMatchEntry, 0, len(matches))

	for _, m := range matches {
		entries = append(entries, faceMatchEntry{
			Similarity: m.Similarity,
			Face: faceEntry{
				FaceID:          m.Face.FaceID,
				ImageID:         m.Face.ImageID,
				ExternalImageID: m.Face.ExternalImageID,
				Confidence:      m.Face.Confidence,
			},
		})
	}

	return &searchFacesResp{
		FaceMatches:      entries,
		FaceModelVersion: faceModelVersion,
		SearchedFaceID:   req.FaceID,
	}, nil
}

type searchFacesByImageReq struct {
	CollectionID string `json:"CollectionId"`
	MaxFaces     int32  `json:"MaxFaces"`
}

type searchFacesByImageResp struct {
	FaceModelVersion string           `json:"FaceModelVersion"`
	FaceMatches      []faceMatchEntry `json:"FaceMatches"`
}

func (h *Handler) handleSearchFacesByImage(
	_ context.Context,
	req *searchFacesByImageReq,
) (*searchFacesByImageResp, error) {
	if req.CollectionID == "" {
		return nil, fmt.Errorf("%w: CollectionId is required", ErrValidation)
	}

	matches, err := h.Backend.SearchFacesByImage(req.CollectionID, req.MaxFaces)
	if err != nil {
		return nil, err
	}

	entries := make([]faceMatchEntry, 0, len(matches))

	for _, m := range matches {
		entries = append(entries, faceMatchEntry{
			Similarity: m.Similarity,
			Face: faceEntry{
				FaceID:          m.Face.FaceID,
				ImageID:         m.Face.ImageID,
				ExternalImageID: m.Face.ExternalImageID,
				Confidence:      m.Face.Confidence,
			},
		})
	}

	return &searchFacesByImageResp{
		FaceMatches:      entries,
		FaceModelVersion: faceModelVersion,
	}, nil
}

// --- Stream processor requests ---

type createStreamProcessorReq struct {
	Tags    map[string]string `json:"Tags"`
	Name    string            `json:"Name"`
	RoleArn string            `json:"RoleArn"`
}

type createStreamProcessorResp struct {
	StreamProcessorArn string `json:"StreamProcessorArn"`
}

func (h *Handler) handleCreateStreamProcessor(
	_ context.Context,
	req *createStreamProcessorReq,
) (*createStreamProcessorResp, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	proc, err := h.Backend.CreateStreamProcessor(req.Name, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return &createStreamProcessorResp{StreamProcessorArn: proc.StreamProcessorARN}, nil
}

type deleteStreamProcessorReq struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteStreamProcessor(_ context.Context, req *deleteStreamProcessorReq) (*struct{}, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if err := h.Backend.DeleteStreamProcessor(req.Name); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeStreamProcessorReq struct {
	Name string `json:"Name"`
}

type describeStreamProcessorResp struct {
	CreationTimestamp  string `json:"CreationTimestamp"`
	Name               string `json:"Name"`
	RoleArn            string `json:"RoleArn"`
	Status             string `json:"Status"`
	StreamProcessorArn string `json:"StreamProcessorArn"`
}

func (h *Handler) handleDescribeStreamProcessor(
	_ context.Context,
	req *describeStreamProcessorReq,
) (*describeStreamProcessorResp, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	proc, err := h.Backend.DescribeStreamProcessor(req.Name)
	if err != nil {
		return nil, err
	}

	return &describeStreamProcessorResp{
		CreationTimestamp:  proc.CreationTimestamp.Format("2006-01-02T15:04:05.000Z"),
		Name:               proc.Name,
		RoleArn:            proc.RoleARN,
		Status:             proc.Status,
		StreamProcessorArn: proc.StreamProcessorARN,
	}, nil
}

type listStreamProcessorsReq struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type streamProcessorEntry struct {
	Name   string `json:"Name"`
	Status string `json:"Status"`
}

type listStreamProcessorsResp struct {
	NextToken        string                 `json:"NextToken,omitempty"`
	StreamProcessors []streamProcessorEntry `json:"StreamProcessors"`
}

func (h *Handler) handleListStreamProcessors(
	_ context.Context,
	req *listStreamProcessorsReq,
) (*listStreamProcessorsResp, error) {
	procs, nextToken, err := h.Backend.ListStreamProcessors(req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	entries := make([]streamProcessorEntry, 0, len(procs))

	for _, p := range procs {
		entries = append(entries, streamProcessorEntry{
			Name:   p.Name,
			Status: p.Status,
		})
	}

	return &listStreamProcessorsResp{
		NextToken:        nextToken,
		StreamProcessors: entries,
	}, nil
}

type streamProcessorNameReq struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStartStreamProcessor(_ context.Context, req *streamProcessorNameReq) (*struct{}, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if err := h.Backend.StartStreamProcessor(req.Name); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleStopStreamProcessor(_ context.Context, req *streamProcessorNameReq) (*struct{}, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if err := h.Backend.StopStreamProcessor(req.Name); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type updateStreamProcessorReq struct {
	Name string `json:"Name"`
}

func (h *Handler) handleUpdateStreamProcessor(_ context.Context, req *updateStreamProcessorReq) (*struct{}, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if err := h.Backend.UpdateStreamProcessor(req.Name); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- Tag requests ---

type tagResourceReq struct {
	Tags        map[string]string `json:"Tags"`
	ResourceArn string            `json:"ResourceArn"`
}

func (h *Handler) handleTagResource(_ context.Context, req *tagResourceReq) (*struct{}, error) {
	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	if err := h.Backend.TagResource(req.ResourceArn, req.Tags); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type untagResourceReq struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, req *untagResourceReq) (*struct{}, error) {
	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	if err := h.Backend.UntagResource(req.ResourceArn, req.TagKeys); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type listTagsForResourceReq struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceResp struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	req *listTagsForResourceReq,
) (*listTagsForResourceResp, error) {
	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	tags, err := h.Backend.ListTagsForResource(req.ResourceArn)
	if err != nil {
		return nil, err
	}

	if tags == nil {
		tags = map[string]string{}
	}

	return &listTagsForResourceResp{Tags: tags}, nil
}
