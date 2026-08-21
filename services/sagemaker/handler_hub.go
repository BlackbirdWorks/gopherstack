package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// keyHubName is the JSON field name for a hub's name, used as both a request
// field and a response map key across the Hub / HubContent family.
const keyHubName = "HubName"

// Operation name constants for the Hub / HubContent family.
const (
	opCreateHub                     = "CreateHub"
	opDescribeHub                   = "DescribeHub"
	opUpdateHub                     = "UpdateHub"
	opDeleteHub                     = "DeleteHub"
	opListHubs                      = "ListHubs"
	opImportHubContent              = "ImportHubContent"
	opDescribeHubContent            = "DescribeHubContent"
	opListHubContents               = "ListHubContents"
	opListHubContentVersions        = "ListHubContentVersions"
	opDeleteHubContent              = "DeleteHubContent"
	opCreateHubContentReference     = "CreateHubContentReference"
	opDeleteHubContentReference     = "DeleteHubContentReference"
	opCreateHubContentPresignedURLs = "CreateHubContentPresignedUrls"
	opUpdateHubContent              = "UpdateHubContent"
	opUpdateHubContentReference     = "UpdateHubContentReference"
)

// hubOpsSupported returns the operation names implemented by this family.
func hubOpsSupported() []string {
	return []string{
		opCreateHub,
		opDescribeHub,
		opUpdateHub,
		opDeleteHub,
		opListHubs,
		opImportHubContent,
		opDescribeHubContent,
		opListHubContents,
		opListHubContentVersions,
		opDeleteHubContent,
		opCreateHubContentReference,
		opDeleteHubContentReference,
		opCreateHubContentPresignedURLs,
		opUpdateHubContent,
		opUpdateHubContentReference,
	}
}

// dispatchHubOps handles the Hub / HubContent family of operations.
func (h *Handler) dispatchHubOps(ctx context.Context, op string, body []byte) ([]byte, bool, error) {
	if r, ok, err := h.dispatchHubResourceOps(ctx, op, body); ok {
		return r, ok, err
	}

	return h.dispatchHubContentOps(ctx, op, body)
}

func (h *Handler) dispatchHubResourceOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case opCreateHub:
		r, err := h.handleCreateHub(ctx, body)

		return r, true, err
	case opDescribeHub:
		r, err := h.handleDescribeHub(ctx, body)

		return r, true, err
	case opUpdateHub:
		r, err := h.handleUpdateHub(ctx, body)

		return r, true, err
	case opDeleteHub:
		return nil, true, h.handleDeleteHub(ctx, body)
	case opListHubs:
		r, err := h.handleListHubs(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchHubContentOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case opImportHubContent:
		r, err := h.handleImportHubContent(ctx, body)

		return r, true, err
	case opDescribeHubContent:
		r, err := h.handleDescribeHubContent(ctx, body)

		return r, true, err
	case opListHubContents:
		r, err := h.handleListHubContents(ctx, body)

		return r, true, err
	case opListHubContentVersions:
		r, err := h.handleListHubContentVersions(ctx, body)

		return r, true, err
	case opDeleteHubContent:
		return nil, true, h.handleDeleteHubContent(ctx, body)
	case opCreateHubContentReference:
		r, err := h.handleCreateHubContentReference(ctx, body)

		return r, true, err
	case opDeleteHubContentReference:
		return nil, true, h.handleDeleteHubContentReference(ctx, body)
	case opCreateHubContentPresignedURLs:
		r, err := h.handleCreateHubContentPresignedURLs(ctx, body)

		return r, true, err
	case opUpdateHubContent:
		r, err := h.handleUpdateHubContent(ctx, body)

		return r, true, err
	case opUpdateHubContentReference:
		r, err := h.handleUpdateHubContentReference(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

// ---------------------------------------------------------------------------
// Hub handlers
// ---------------------------------------------------------------------------

type hubS3StorageConfigJSON struct {
	S3OutputPath string `json:"S3OutputPath,omitempty"`
}

// createHubInput is the CreateHub request shape (named, not inline — see
// gopherstack-oc9v). Matches CreateHubInput (api_op_CreateHub.go) exactly.
type createHubInput struct {
	HubName           string                  `json:"HubName"`
	HubDescription    string                  `json:"HubDescription"`
	HubDisplayName    string                  `json:"HubDisplayName"`
	HubSearchKeywords []string                `json:"HubSearchKeywords"`
	S3StorageConfig   *hubS3StorageConfigJSON `json:"S3StorageConfig"`
	Tags              []tagObject             `json:"Tags"`
}

func (h *Handler) handleCreateHub(ctx context.Context, body []byte) ([]byte, error) {
	var req createHubInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HubName == "" {
		return nil, fmt.Errorf("%w: HubName is required", errInvalidRequest)
	}

	if req.HubDescription == "" {
		return nil, fmt.Errorf("%w: HubDescription is required", errInvalidRequest)
	}

	s3OutputPath := ""
	if req.S3StorageConfig != nil {
		s3OutputPath = req.S3StorageConfig.S3OutputPath
	}

	hub, err := h.Backend.CreateHub(
		ctx, req.HubName, req.HubDescription, req.HubDisplayName, s3OutputPath,
		req.HubSearchKeywords, fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created hub", "name", hub.HubName, "arn", hub.HubArn)

	return json.Marshal(map[string]string{keyHubArn: hub.HubArn})
}

func hubToDescribeResponse(hub *Hub) map[string]any {
	resp := map[string]any{
		keyHubName:          hub.HubName,
		keyHubArn:           hub.HubArn,
		"HubStatus":         hub.HubStatus,
		keyCreationTime:     epochSeconds(hub.CreationTime),
		keyLastModifiedTime: epochSeconds(hub.LastModifiedTime),
	}

	if hub.HubDisplayName != "" {
		resp["HubDisplayName"] = hub.HubDisplayName
	}

	if hub.HubDescription != "" {
		resp["HubDescription"] = hub.HubDescription
	}

	if len(hub.HubSearchKeywords) > 0 {
		resp["HubSearchKeywords"] = hub.HubSearchKeywords
	}

	if hub.S3OutputPath != "" {
		resp["S3StorageConfig"] = hubS3StorageConfigJSON{S3OutputPath: hub.S3OutputPath}
	}

	if hub.FailureReason != "" {
		resp["FailureReason"] = hub.FailureReason
	}

	return resp
}

// describeHubInput is the DescribeHub request shape (named, not inline — see
// gopherstack-oc9v). Matches DescribeHubInput (api_op_DescribeHub.go) exactly.
type describeHubInput struct {
	HubName string `json:"HubName"`
}

func (h *Handler) handleDescribeHub(ctx context.Context, body []byte) ([]byte, error) {
	var req describeHubInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HubName == "" {
		return nil, fmt.Errorf("%w: HubName is required", errInvalidRequest)
	}

	hub, err := h.Backend.DescribeHub(ctx, req.HubName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(hubToDescribeResponse(hub))
}

type hubInfoSummary struct {
	HubName           string   `json:"HubName"`
	HubArn            string   `json:"HubArn"`
	HubDisplayName    string   `json:"HubDisplayName,omitempty"`
	HubDescription    string   `json:"HubDescription,omitempty"`
	HubStatus         string   `json:"HubStatus"`
	HubSearchKeywords []string `json:"HubSearchKeywords,omitempty"`
	CreationTime      float64  `json:"CreationTime"`
	LastModifiedTime  float64  `json:"LastModifiedTime"`
}

// listHubsInput is the ListHubs request shape (named, not inline — see
// gopherstack-oc9v). Matches ListHubsInput (api_op_ListHubs.go) exactly.
type listHubsInput struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore"`
	NameContains           string   `json:"NameContains"`
	NextToken              string   `json:"NextToken"`
	SortBy                 string   `json:"SortBy"`
	SortOrder              string   `json:"SortOrder"`
	MaxResults             int32    `json:"MaxResults"`
}

func (h *Handler) handleListHubs(ctx context.Context, body []byte) ([]byte, error) {
	var req listHubsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	hubs, nextToken := h.Backend.ListHubs(ctx, ListHubsParams{
		NameContains:           req.NameContains,
		NextToken:              req.NextToken,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		MaxResults:             req.MaxResults,
		CreationTimeAfter:      timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreationTimeBefore:     timeFromEpochSecondsPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  timeFromEpochSecondsPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: timeFromEpochSecondsPtr(req.LastModifiedTimeBefore),
	})
	summaries := make([]hubInfoSummary, 0, len(hubs))

	for _, hub := range hubs {
		summaries = append(summaries, hubInfoSummary{
			HubName:           hub.HubName,
			HubArn:            hub.HubArn,
			HubDisplayName:    hub.HubDisplayName,
			HubDescription:    hub.HubDescription,
			HubSearchKeywords: hub.HubSearchKeywords,
			HubStatus:         hub.HubStatus,
			CreationTime:      epochSeconds(hub.CreationTime),
			LastModifiedTime:  epochSeconds(hub.LastModifiedTime),
		})
	}

	resp := map[string]any{"HubSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// updateHubInput is the UpdateHub request shape (named, not inline — see
// gopherstack-oc9v). Matches UpdateHubInput (api_op_UpdateHub.go) exactly.
type updateHubInput struct {
	HubDescription    *string   `json:"HubDescription"`
	HubDisplayName    *string   `json:"HubDisplayName"`
	HubSearchKeywords *[]string `json:"HubSearchKeywords"`
	HubName           string    `json:"HubName"`
}

func (h *Handler) handleUpdateHub(ctx context.Context, body []byte) ([]byte, error) {
	var req updateHubInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HubName == "" {
		return nil, fmt.Errorf("%w: HubName is required", errInvalidRequest)
	}

	opts := UpdateHubOptions{HubDescription: req.HubDescription, HubDisplayName: req.HubDisplayName}
	if req.HubSearchKeywords != nil {
		opts.HasSearchKeywords = true
		opts.HubSearchKeywords = *req.HubSearchKeywords
	}

	hub, err := h.Backend.UpdateHub(ctx, req.HubName, opts)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyHubArn: hub.HubArn})
}

// deleteHubInput is the DeleteHub request shape (named, not inline — see
// gopherstack-oc9v). Matches DeleteHubInput (api_op_DeleteHub.go) exactly.
type deleteHubInput struct {
	HubName string `json:"HubName"`
}

func (h *Handler) handleDeleteHub(ctx context.Context, body []byte) error {
	var req deleteHubInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HubName == "" {
		return fmt.Errorf("%w: HubName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteHub(ctx, req.HubName); err != nil {
		return err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted hub", "name", req.HubName)

	return nil
}

// ---------------------------------------------------------------------------
// HubContent handlers
// ---------------------------------------------------------------------------

// importHubContentInput is the ImportHubContent request shape (named, not
// inline — see gopherstack-oc9v). Matches ImportHubContentInput
// (api_op_ImportHubContent.go) exactly.
type importHubContentInput struct {
	HubContentName           string      `json:"HubContentName"`
	HubContentVersion        string      `json:"HubContentVersion"`
	HubContentType           string      `json:"HubContentType"`
	DocumentSchemaVersion    string      `json:"DocumentSchemaVersion"`
	HubName                  string      `json:"HubName"`
	HubContentDisplayName    string      `json:"HubContentDisplayName"`
	HubContentDescription    string      `json:"HubContentDescription"`
	HubContentMarkdown       string      `json:"HubContentMarkdown"`
	HubContentDocument       string      `json:"HubContentDocument"`
	SupportStatus            string      `json:"SupportStatus"`
	HubContentSearchKeywords []string    `json:"HubContentSearchKeywords"`
	Tags                     []tagObject `json:"Tags"`
}

func (h *Handler) handleImportHubContent(ctx context.Context, body []byte) ([]byte, error) {
	var req importHubContentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if missing := requireImportHubContentFields(req.HubName, req.HubContentName,
		req.HubContentType, req.DocumentSchemaVersion, req.HubContentDocument); missing != "" {
		return nil, fmt.Errorf("%w: %s is required", errInvalidRequest, missing)
	}

	hc, err := h.Backend.ImportHubContent(ctx, ImportHubContentInput{
		HubName:                  req.HubName,
		HubContentName:           req.HubContentName,
		HubContentVersion:        req.HubContentVersion,
		HubContentType:           req.HubContentType,
		DocumentSchemaVersion:    req.DocumentSchemaVersion,
		HubContentDisplayName:    req.HubContentDisplayName,
		HubContentDescription:    req.HubContentDescription,
		HubContentMarkdown:       req.HubContentMarkdown,
		HubContentDocument:       req.HubContentDocument,
		SupportStatus:            req.SupportStatus,
		HubContentSearchKeywords: req.HubContentSearchKeywords,
		Tags:                     fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: imported hub content",
		"hub", hc.HubName, "name", hc.HubContentName, "version", hc.HubContentVersion)

	return json.Marshal(map[string]string{keyHubArn: hc.HubArn, keyHubContentArn: hc.HubContentArn})
}

// requireImportHubContentFields returns the name of the first missing required
// field, or "" if all are present.
func requireImportHubContentFields(hubName, contentName, contentType, schemaVersion, document string) string {
	switch {
	case hubName == "":
		return keyHubName
	case contentName == "":
		return "HubContentName"
	case contentType == "":
		return "HubContentType"
	case schemaVersion == "":
		return "DocumentSchemaVersion"
	case document == "":
		return "HubContentDocument"
	default:
		return ""
	}
}

func hubContentDependenciesJSON(deps []HubContentDependency) []map[string]string {
	if len(deps) == 0 {
		return nil
	}

	out := make([]map[string]string, 0, len(deps))
	for _, d := range deps {
		out = append(out, map[string]string{
			"DependencyOriginPath": d.DependencyOriginPath,
			"DependencyCopyPath":   d.DependencyCopyPath,
		})
	}

	return out
}

func hubContentToDescribeResponse(hc *HubContent) map[string]any {
	resp := map[string]any{
		"HubContentName":        hc.HubContentName,
		keyHubContentArn:        hc.HubContentArn,
		"HubContentVersion":     hc.HubContentVersion,
		"HubContentType":        hc.HubContentType,
		"DocumentSchemaVersion": hc.DocumentSchemaVersion,
		keyHubName:              hc.HubName,
		keyHubArn:               hc.HubArn,
		"HubContentDocument":    hc.HubContentDocument,
		keyHubContentStatus:     hc.HubContentStatus,
		keyCreationTime:         epochSeconds(hc.CreationTime),
	}

	if !hc.LastModifiedTime.IsZero() {
		resp[keyLastModifiedTime] = epochSeconds(hc.LastModifiedTime)
	}

	addOptionalHubContentFields(resp, hc)

	return resp
}

func addOptionalHubContentFields(resp map[string]any, hc *HubContent) {
	if hc.HubContentDisplayName != "" {
		resp["HubContentDisplayName"] = hc.HubContentDisplayName
	}

	if hc.HubContentDescription != "" {
		resp["HubContentDescription"] = hc.HubContentDescription
	}

	if hc.HubContentMarkdown != "" {
		resp["HubContentMarkdown"] = hc.HubContentMarkdown
	}

	if hc.SageMakerPublicHubContentArn != "" {
		resp["SageMakerPublicHubContentArn"] = hc.SageMakerPublicHubContentArn
	}

	if hc.ReferenceMinVersion != "" {
		resp["ReferenceMinVersion"] = hc.ReferenceMinVersion
	}

	if hc.SupportStatus != "" {
		resp["SupportStatus"] = hc.SupportStatus
	}

	if len(hc.HubContentSearchKeywords) > 0 {
		resp["HubContentSearchKeywords"] = hc.HubContentSearchKeywords
	}

	if deps := hubContentDependenciesJSON(hc.HubContentDependencies); deps != nil {
		resp["HubContentDependencies"] = deps
	}

	if hc.FailureReason != "" {
		resp["FailureReason"] = hc.FailureReason
	}
}

// describeHubContentInput is the DescribeHubContent request shape (named,
// not inline — see gopherstack-oc9v). Matches DescribeHubContentInput
// (api_op_DescribeHubContent.go) exactly.
type describeHubContentInput struct {
	HubName           string `json:"HubName"`
	HubContentType    string `json:"HubContentType"`
	HubContentName    string `json:"HubContentName"`
	HubContentVersion string `json:"HubContentVersion"`
}

func (h *Handler) handleDescribeHubContent(ctx context.Context, body []byte) ([]byte, error) {
	var req describeHubContentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HubName == "" || req.HubContentType == "" || req.HubContentName == "" {
		return nil, fmt.Errorf(
			"%w: HubName, HubContentType, and HubContentName are required", errInvalidRequest,
		)
	}

	hc, err := h.Backend.DescribeHubContent(
		ctx, req.HubName, req.HubContentType, req.HubContentName, req.HubContentVersion,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(hubContentToDescribeResponse(hc))
}

type hubContentInfoSummary struct {
	HubContentName               string   `json:"HubContentName"`
	HubContentArn                string   `json:"HubContentArn"`
	SageMakerPublicHubContentArn string   `json:"SageMakerPublicHubContentArn,omitempty"`
	HubContentVersion            string   `json:"HubContentVersion"`
	HubContentType               string   `json:"HubContentType"`
	DocumentSchemaVersion        string   `json:"DocumentSchemaVersion"`
	HubContentDisplayName        string   `json:"HubContentDisplayName,omitempty"`
	HubContentDescription        string   `json:"HubContentDescription,omitempty"`
	SupportStatus                string   `json:"SupportStatus,omitempty"`
	HubContentStatus             string   `json:"HubContentStatus"`
	HubContentSearchKeywords     []string `json:"HubContentSearchKeywords,omitempty"`
	CreationTime                 float64  `json:"CreationTime"`
}

func toHubContentInfoSummary(hc *HubContent) hubContentInfoSummary {
	return hubContentInfoSummary{
		HubContentName:               hc.HubContentName,
		HubContentArn:                hc.HubContentArn,
		SageMakerPublicHubContentArn: hc.SageMakerPublicHubContentArn,
		HubContentVersion:            hc.HubContentVersion,
		HubContentType:               hc.HubContentType,
		DocumentSchemaVersion:        hc.DocumentSchemaVersion,
		HubContentDisplayName:        hc.HubContentDisplayName,
		HubContentDescription:        hc.HubContentDescription,
		SupportStatus:                hc.SupportStatus,
		HubContentSearchKeywords:     hc.HubContentSearchKeywords,
		HubContentStatus:             hc.HubContentStatus,
		CreationTime:                 epochSeconds(hc.CreationTime),
	}
}

// listHubContentsInput is the ListHubContents request shape (named, not
// inline — see gopherstack-oc9v). Matches ListHubContentsInput
// (api_op_ListHubContents.go) exactly.
type listHubContentsInput struct {
	CreationTimeAfter  *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore *float64 `json:"CreationTimeBefore"`
	HubName            string   `json:"HubName"`
	HubContentType     string   `json:"HubContentType"`
	MaxSchemaVersion   string   `json:"MaxSchemaVersion"`
	NameContains       string   `json:"NameContains"`
	NextToken          string   `json:"NextToken"`
	SortBy             string   `json:"SortBy"`
	SortOrder          string   `json:"SortOrder"`
	MaxResults         int32    `json:"MaxResults"`
}

func (h *Handler) handleListHubContents(ctx context.Context, body []byte) ([]byte, error) {
	var req listHubContentsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HubName == "" || req.HubContentType == "" {
		return nil, fmt.Errorf("%w: HubName and HubContentType are required", errInvalidRequest)
	}

	items, nextToken := h.Backend.ListHubContents(ctx, req.HubName, req.HubContentType, ListHubContentsParams{
		NameContains:       req.NameContains,
		NextToken:          req.NextToken,
		SortBy:             req.SortBy,
		SortOrder:          req.SortOrder,
		MaxSchemaVersion:   req.MaxSchemaVersion,
		MaxResults:         req.MaxResults,
		CreationTimeAfter:  timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreationTimeBefore: timeFromEpochSecondsPtr(req.CreationTimeBefore),
	})
	summaries := make([]hubContentInfoSummary, 0, len(items))

	for _, hc := range items {
		summaries = append(summaries, toHubContentInfoSummary(hc))
	}

	resp := map[string]any{"HubContentSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// listHubContentVersionsInput is the ListHubContentVersions request shape
// (named, not inline — see gopherstack-oc9v). Matches
// ListHubContentVersionsInput (api_op_ListHubContentVersions.go) exactly.
type listHubContentVersionsInput struct {
	CreationTimeAfter  *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore *float64 `json:"CreationTimeBefore"`
	HubName            string   `json:"HubName"`
	HubContentType     string   `json:"HubContentType"`
	HubContentName     string   `json:"HubContentName"`
	MaxSchemaVersion   string   `json:"MaxSchemaVersion"`
	MinVersion         string   `json:"MinVersion"`
	NextToken          string   `json:"NextToken"`
	SortBy             string   `json:"SortBy"`
	SortOrder          string   `json:"SortOrder"`
	MaxResults         int32    `json:"MaxResults"`
}

func (h *Handler) handleListHubContentVersions(ctx context.Context, body []byte) ([]byte, error) {
	var req listHubContentVersionsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HubName == "" || req.HubContentType == "" || req.HubContentName == "" {
		return nil, fmt.Errorf(
			"%w: HubName, HubContentType, and HubContentName are required", errInvalidRequest,
		)
	}

	items, nextToken := h.Backend.ListHubContentVersions(
		ctx, req.HubName, req.HubContentType, req.HubContentName, ListHubContentVersionsParams{
			NextToken:          req.NextToken,
			SortBy:             req.SortBy,
			SortOrder:          req.SortOrder,
			MaxSchemaVersion:   req.MaxSchemaVersion,
			MinVersion:         req.MinVersion,
			MaxResults:         req.MaxResults,
			CreationTimeAfter:  timeFromEpochSecondsPtr(req.CreationTimeAfter),
			CreationTimeBefore: timeFromEpochSecondsPtr(req.CreationTimeBefore),
		},
	)
	summaries := make([]hubContentInfoSummary, 0, len(items))

	for _, hc := range items {
		summaries = append(summaries, toHubContentInfoSummary(hc))
	}

	resp := map[string]any{"HubContentSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// deleteHubContentInput is the DeleteHubContent request shape (named, not
// inline — see gopherstack-oc9v). Matches DeleteHubContentInput
// (api_op_DeleteHubContent.go) exactly.
type deleteHubContentInput struct {
	HubName           string `json:"HubName"`
	HubContentType    string `json:"HubContentType"`
	HubContentName    string `json:"HubContentName"`
	HubContentVersion string `json:"HubContentVersion"`
}

func (h *Handler) handleDeleteHubContent(ctx context.Context, body []byte) error {
	var req deleteHubContentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HubName == "" || req.HubContentType == "" || req.HubContentName == "" || req.HubContentVersion == "" {
		return fmt.Errorf(
			"%w: HubName, HubContentType, HubContentName, and HubContentVersion are required", errInvalidRequest,
		)
	}

	if err := h.Backend.DeleteHubContent(
		ctx, req.HubName, req.HubContentType, req.HubContentName, req.HubContentVersion,
	); err != nil {
		return err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted hub content",
		"hub", req.HubName, "name", req.HubContentName, "version", req.HubContentVersion)

	return nil
}

// createHubContentReferenceInput is the CreateHubContentReference request
// shape (named, not inline — see gopherstack-oc9v). Matches
// CreateHubContentReferenceInput (api_op_CreateHubContentReference.go) exactly.
type createHubContentReferenceInput struct {
	HubName                      string      `json:"HubName"`
	SageMakerPublicHubContentArn string      `json:"SageMakerPublicHubContentArn"`
	HubContentName               string      `json:"HubContentName"`
	MinVersion                   string      `json:"MinVersion"`
	Tags                         []tagObject `json:"Tags"`
}

func (h *Handler) handleCreateHubContentReference(ctx context.Context, body []byte) ([]byte, error) {
	var req createHubContentReferenceInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HubName == "" || req.SageMakerPublicHubContentArn == "" {
		return nil, fmt.Errorf(
			"%w: HubName and SageMakerPublicHubContentArn are required", errInvalidRequest,
		)
	}

	hc, err := h.Backend.CreateHubContentReference(
		ctx, req.HubName, req.SageMakerPublicHubContentArn, req.HubContentName, req.MinVersion,
		fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyHubArn: hc.HubArn, keyHubContentArn: hc.HubContentArn})
}

// deleteHubContentReferenceInput is the DeleteHubContentReference request
// shape (named, not inline — see gopherstack-oc9v). Matches
// DeleteHubContentReferenceInput (api_op_DeleteHubContentReference.go) exactly.
type deleteHubContentReferenceInput struct {
	HubName        string `json:"HubName"`
	HubContentType string `json:"HubContentType"`
	HubContentName string `json:"HubContentName"`
}

func (h *Handler) handleDeleteHubContentReference(ctx context.Context, body []byte) error {
	var req deleteHubContentReferenceInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HubName == "" || req.HubContentType == "" || req.HubContentName == "" {
		return fmt.Errorf(
			"%w: HubName, HubContentType, and HubContentName are required", errInvalidRequest,
		)
	}

	return h.Backend.DeleteHubContentReference(ctx, req.HubName, req.HubContentType, req.HubContentName)
}

// updateHubContentInput is the UpdateHubContent request shape (named, not
// inline — see gopherstack-oc9v). Matches UpdateHubContentInput
// (api_op_UpdateHubContent.go) exactly.
type updateHubContentInput struct {
	HubName                  string   `json:"HubName"`
	HubContentType           string   `json:"HubContentType"`
	HubContentName           string   `json:"HubContentName"`
	HubContentVersion        string   `json:"HubContentVersion"`
	HubContentDescription    string   `json:"HubContentDescription,omitempty"`
	HubContentDisplayName    string   `json:"HubContentDisplayName,omitempty"`
	HubContentMarkdown       string   `json:"HubContentMarkdown,omitempty"`
	SupportStatus            string   `json:"SupportStatus,omitempty"`
	HubContentSearchKeywords []string `json:"HubContentSearchKeywords,omitempty"`
}

func (h *Handler) handleUpdateHubContent(ctx context.Context, body []byte) ([]byte, error) {
	var req updateHubContentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HubName == "" || req.HubContentType == "" || req.HubContentName == "" || req.HubContentVersion == "" {
		return nil, fmt.Errorf(
			"%w: HubName, HubContentType, HubContentName, and HubContentVersion are required", errInvalidRequest,
		)
	}

	hc, err := h.Backend.UpdateHubContent(
		ctx, req.HubName, req.HubContentType, req.HubContentName, req.HubContentVersion,
		UpdateHubContentOptions{
			HubContentDescription:    req.HubContentDescription,
			HubContentDisplayName:    req.HubContentDisplayName,
			HubContentMarkdown:       req.HubContentMarkdown,
			SupportStatus:            req.SupportStatus,
			HubContentSearchKeywords: req.HubContentSearchKeywords,
		},
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyHubArn: hc.HubArn, keyHubContentArn: hc.HubContentArn})
}

// updateHubContentReferenceInput is the UpdateHubContentReference request
// shape (named, not inline — see gopherstack-oc9v). Matches
// UpdateHubContentReferenceInput (api_op_UpdateHubContentReference.go) exactly.
type updateHubContentReferenceInput struct {
	HubName        string `json:"HubName"`
	HubContentType string `json:"HubContentType"`
	HubContentName string `json:"HubContentName"`
	MinVersion     string `json:"MinVersion,omitempty"`
}

func (h *Handler) handleUpdateHubContentReference(ctx context.Context, body []byte) ([]byte, error) {
	var req updateHubContentReferenceInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HubName == "" || req.HubContentType == "" || req.HubContentName == "" {
		return nil, fmt.Errorf(
			"%w: HubName, HubContentType, and HubContentName are required", errInvalidRequest,
		)
	}

	hc, err := h.Backend.UpdateHubContentReference(
		ctx, req.HubName, req.HubContentType, req.HubContentName, req.MinVersion,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyHubArn: hc.HubArn, keyHubContentArn: hc.HubContentArn})
}

// presignedURLAccessConfigJSON is the wire shape of PresignedUrlAccessConfig
// (types/types.go:17716-17729).
type presignedURLAccessConfigJSON struct {
	ExpectedS3URL string `json:"ExpectedS3Url,omitempty"`
	AcceptEula    bool   `json:"AcceptEula,omitempty"`
}

// createHubContentPresignedURLsInput is the CreateHubContentPresignedUrls
// request shape (named, not inline — see gopherstack-oc9v). Matches
// CreateHubContentPresignedUrlsInput (api_op_CreateHubContentPresignedUrls.go)
// exactly.
type createHubContentPresignedURLsInput struct {
	AccessConfig      *presignedURLAccessConfigJSON `json:"AccessConfig"`
	HubName           string                        `json:"HubName"`
	HubContentType    string                        `json:"HubContentType"`
	HubContentName    string                        `json:"HubContentName"`
	HubContentVersion string                        `json:"HubContentVersion"`
	NextToken         string                        `json:"NextToken"`
	MaxResults        int32                         `json:"MaxResults"`
}

func (h *Handler) handleCreateHubContentPresignedURLs(ctx context.Context, body []byte) ([]byte, error) {
	var req createHubContentPresignedURLsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HubName == "" || req.HubContentType == "" || req.HubContentName == "" {
		return nil, fmt.Errorf(
			"%w: HubName, HubContentType, and HubContentName are required", errInvalidRequest,
		)
	}

	var accessConfig PresignedURLAccessConfig
	if req.AccessConfig != nil {
		accessConfig = PresignedURLAccessConfig{
			AcceptEula:    req.AccessConfig.AcceptEula,
			ExpectedS3URL: req.AccessConfig.ExpectedS3URL,
		}
	}

	urls, nextToken, err := h.Backend.CreateHubContentPresignedURLs(
		ctx, req.HubName, req.HubContentType, req.HubContentName, req.HubContentVersion,
		CreateHubContentPresignedURLsParams{
			AccessConfig: accessConfig,
			NextToken:    req.NextToken,
			MaxResults:   req.MaxResults,
		},
	)
	if err != nil {
		return nil, err
	}

	configs := make([]map[string]string, 0, len(urls))
	for _, u := range urls {
		configs = append(configs, map[string]string{keyURL: u.URL, "LocalPath": u.LocalPath})
	}

	resp := map[string]any{"AuthorizedUrlConfigs": configs}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}
