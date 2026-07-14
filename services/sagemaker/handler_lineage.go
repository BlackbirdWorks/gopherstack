package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// Key constants for ML Lineage resource ARNs, reused across request/response bodies.
const (
	keyActionArn       = "ActionArn"
	keyArtifactArn     = "ArtifactArn"
	keyContextArn      = "ContextArn"
	keyLineageGroupArn = "LineageGroupArn"
	keySource          = "Source"
)

// lineageOpsSupported returns the ML Lineage operations implemented with real
// backend state (CreateAction/AddAssociation are declared in the core op list
// in handler.go since they predate this family).
func lineageOpsSupported() []string {
	return []string{
		"CreateArtifact",
		"DescribeArtifact",
		"UpdateArtifact",
		"DeleteArtifact",
		"ListArtifacts",
		"CreateContext",
		"DescribeContext",
		"UpdateContext",
		"DeleteContext",
		"ListContexts",
		"DescribeAction",
		"UpdateAction",
		"DeleteAction",
		"ListActions",
		"DeleteAssociation",
		"ListAssociations",
		"QueryLineage",
		"DescribeLineageGroup",
		"ListLineageGroups",
		"GetLineageGroupPolicy",
	}
}

// lineageHandlerFunc is the signature shared by every ML Lineage op handler.
type lineageHandlerFunc func(*Handler, context.Context, []byte) ([]byte, error)

// lineageHandlers builds the op-name -> handler routing table for the ML
// Lineage family. It is data-driven (rather than a switch) so that adding an
// op is a one-line table entry instead of another near-identical case arm.
func lineageHandlers() map[string]lineageHandlerFunc {
	return map[string]lineageHandlerFunc{
		"CreateArtifact":        (*Handler).handleCreateArtifact,
		"DescribeArtifact":      (*Handler).handleDescribeArtifact,
		"UpdateArtifact":        (*Handler).handleUpdateArtifact,
		"DeleteArtifact":        (*Handler).handleDeleteArtifact,
		"ListArtifacts":         (*Handler).handleListArtifacts,
		"CreateContext":         (*Handler).handleCreateContext,
		"DescribeContext":       (*Handler).handleDescribeContext,
		"UpdateContext":         (*Handler).handleUpdateContext,
		"DeleteContext":         (*Handler).handleDeleteContext,
		"ListContexts":          (*Handler).handleListContexts,
		"DescribeAction":        (*Handler).handleDescribeAction,
		"UpdateAction":          (*Handler).handleUpdateAction,
		"DeleteAction":          (*Handler).handleDeleteAction,
		"ListActions":           (*Handler).handleListActions,
		"DeleteAssociation":     (*Handler).handleDeleteAssociation,
		"ListAssociations":      (*Handler).handleListAssociations,
		"QueryLineage":          (*Handler).handleQueryLineage,
		"DescribeLineageGroup":  (*Handler).handleDescribeLineageGroup,
		"ListLineageGroups":     (*Handler).handleListLineageGroups,
		"GetLineageGroupPolicy": (*Handler).handleGetLineageGroupPolicy,
	}
}

// dispatchLineageOps handles the ML Lineage family (Action/Artifact/Context/
// Association/LineageGroup CRUD and QueryLineage graph traversal).
func (h *Handler) dispatchLineageOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	fn, ok := lineageHandlers()[op]
	if !ok {
		return nil, false, nil
	}

	r, err := fn(h, ctx, body)

	return r, true, err
}

// ---------------------------------------------------------------------------
// Artifact handlers
// ---------------------------------------------------------------------------

type artifactSourceTypeObject struct {
	SourceIDType string `json:"SourceIdType"`
	Value        string `json:"Value"`
}

type artifactSourceObject struct {
	SourceURI   string                     `json:"SourceUri"`
	SourceTypes []artifactSourceTypeObject `json:"SourceTypes"`
}

func toArtifactSource(src ArtifactSource) artifactSourceObject {
	types := make([]artifactSourceTypeObject, 0, len(src.SourceTypes))
	for _, st := range src.SourceTypes {
		types = append(types, artifactSourceTypeObject(st))
	}

	return artifactSourceObject{SourceURI: src.SourceURI, SourceTypes: types}
}

func fromArtifactSource(src artifactSourceObject) ArtifactSource {
	types := make([]ArtifactSourceType, 0, len(src.SourceTypes))
	for _, st := range src.SourceTypes {
		types = append(types, ArtifactSourceType(st))
	}

	return ArtifactSource{SourceURI: src.SourceURI, SourceTypes: types}
}

func (h *Handler) handleCreateArtifact(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ArtifactName string               `json:"ArtifactName"`
		ArtifactType string               `json:"ArtifactType"`
		Source       artifactSourceObject `json:"Source"`
		Properties   map[string]string    `json:"Properties"`
		Tags         []tagObject          `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ArtifactType == "" {
		return nil, fmt.Errorf("%w: ArtifactType is required", errInvalidRequest)
	}

	if req.Source.SourceURI == "" {
		return nil, fmt.Errorf("%w: Source.SourceUri is required", errInvalidRequest)
	}

	ar, err := h.Backend.CreateArtifact(
		ctx,
		req.ArtifactName,
		req.ArtifactType,
		fromArtifactSource(req.Source),
		req.Properties,
		fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created artifact", "arn", ar.ArtifactArn)

	return json.Marshal(map[string]string{keyArtifactArn: ar.ArtifactArn})
}

func (h *Handler) handleDescribeArtifact(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ArtifactArn string `json:"ArtifactArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ArtifactArn == "" {
		return nil, fmt.Errorf("%w: ArtifactArn is required", errInvalidRequest)
	}

	ar, err := h.Backend.DescribeArtifact(ctx, req.ArtifactArn)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		keyArtifactArn:      ar.ArtifactArn,
		"ArtifactType":      ar.ArtifactType,
		keySource:           toArtifactSource(ar.Source),
		keyCreationTime:     epochSeconds(ar.CreationTime),
		keyLastModifiedTime: epochSeconds(ar.LastModifiedTime),
	}
	if ar.ArtifactName != "" {
		resp["ArtifactName"] = ar.ArtifactName
	}

	if len(ar.Properties) > 0 {
		resp["Properties"] = ar.Properties
	}

	return json.Marshal(resp)
}

func (h *Handler) handleUpdateArtifact(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ArtifactArn        string            `json:"ArtifactArn"`
		ArtifactName       string            `json:"ArtifactName"`
		Properties         map[string]string `json:"Properties"`
		PropertiesToRemove []string          `json:"PropertiesToRemove"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ArtifactArn == "" {
		return nil, fmt.Errorf("%w: ArtifactArn is required", errInvalidRequest)
	}

	ar, err := h.Backend.UpdateArtifact(ctx, req.ArtifactArn, req.ArtifactName, req.Properties, req.PropertiesToRemove)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated artifact", "arn", ar.ArtifactArn)

	return json.Marshal(map[string]string{keyArtifactArn: ar.ArtifactArn})
}

func (h *Handler) handleDeleteArtifact(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ArtifactArn string `json:"ArtifactArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ArtifactArn == "" {
		return nil, fmt.Errorf("%w: ArtifactArn is required", errInvalidRequest)
	}

	ar, err := h.Backend.DeleteArtifact(ctx, req.ArtifactArn)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted artifact", "arn", req.ArtifactArn)

	return json.Marshal(map[string]string{keyArtifactArn: ar.ArtifactArn})
}

type artifactSummary struct {
	ArtifactArn      string               `json:"ArtifactArn"`
	ArtifactName     string               `json:"ArtifactName,omitempty"`
	ArtifactType     string               `json:"ArtifactType"`
	Source           artifactSourceObject `json:"Source"`
	CreationTime     float64              `json:"CreationTime"`
	LastModifiedTime float64              `json:"LastModifiedTime"`
}

// marshalSummaryPage converts items to their wire summary shape via conv and
// marshals them under key, alongside NextToken when paginated.
func marshalSummaryPage[T, S any](key, nextToken string, items []*T, conv func(*T) S) ([]byte, error) {
	summaries := make([]S, 0, len(items))
	for _, item := range items {
		summaries = append(summaries, conv(item))
	}

	resp := map[string]any{key: summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListArtifacts(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ArtifactType string `json:"ArtifactType"`
		SourceURI    string `json:"SourceUri"`
		NextToken    string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	artifacts, nextToken := h.Backend.ListArtifacts(ctx, req.ArtifactType, req.SourceURI, req.NextToken)

	return marshalSummaryPage("ArtifactSummaries", nextToken, artifacts, artifactToSummary)
}

func artifactToSummary(ar *Artifact) artifactSummary {
	return artifactSummary{
		ArtifactArn:      ar.ArtifactArn,
		ArtifactName:     ar.ArtifactName,
		ArtifactType:     ar.ArtifactType,
		Source:           toArtifactSource(ar.Source),
		CreationTime:     epochSeconds(ar.CreationTime),
		LastModifiedTime: epochSeconds(ar.LastModifiedTime),
	}
}

// ---------------------------------------------------------------------------
// Context handlers
// ---------------------------------------------------------------------------

type contextSourceObject struct {
	SourceURI  string `json:"SourceUri"`
	SourceID   string `json:"SourceId,omitempty"`
	SourceType string `json:"SourceType,omitempty"`
}

func toContextSource(src ContextSource) contextSourceObject {
	return contextSourceObject(src)
}

func fromContextSource(src contextSourceObject) ContextSource {
	return ContextSource(src)
}

func (h *Handler) handleCreateContext(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ContextName string              `json:"ContextName"`
		ContextType string              `json:"ContextType"`
		Description string              `json:"Description"`
		Source      contextSourceObject `json:"Source"`
		Properties  map[string]string   `json:"Properties"`
		Tags        []tagObject         `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ContextName == "" {
		return nil, fmt.Errorf("%w: ContextName is required", errInvalidRequest)
	}

	if req.ContextType == "" {
		return nil, fmt.Errorf("%w: ContextType is required", errInvalidRequest)
	}

	if req.Source.SourceURI == "" {
		return nil, fmt.Errorf("%w: Source.SourceUri is required", errInvalidRequest)
	}

	c, err := h.Backend.CreateContext(
		ctx,
		req.ContextName,
		req.ContextType,
		req.Description,
		fromContextSource(req.Source),
		req.Properties,
		fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created context", "name", c.ContextName)

	return json.Marshal(map[string]string{keyContextArn: c.ContextArn})
}

func (h *Handler) handleDescribeContext(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ContextName string `json:"ContextName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ContextName == "" {
		return nil, fmt.Errorf("%w: ContextName is required", errInvalidRequest)
	}

	c, err := h.Backend.DescribeContext(ctx, req.ContextName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"ContextName":       c.ContextName,
		keyContextArn:       c.ContextArn,
		"ContextType":       c.ContextType,
		keySource:           toContextSource(c.Source),
		keyCreationTime:     epochSeconds(c.CreationTime),
		keyLastModifiedTime: epochSeconds(c.LastModifiedTime),
	}
	if c.Description != "" {
		resp["Description"] = c.Description
	}

	if len(c.Properties) > 0 {
		resp["Properties"] = c.Properties
	}

	return json.Marshal(resp)
}

func (h *Handler) handleUpdateContext(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ContextName        string            `json:"ContextName"`
		Description        string            `json:"Description"`
		Properties         map[string]string `json:"Properties"`
		PropertiesToRemove []string          `json:"PropertiesToRemove"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ContextName == "" {
		return nil, fmt.Errorf("%w: ContextName is required", errInvalidRequest)
	}

	c, err := h.Backend.UpdateContext(ctx, req.ContextName, req.Description, req.Properties, req.PropertiesToRemove)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated context", "name", c.ContextName)

	return json.Marshal(map[string]string{keyContextArn: c.ContextArn})
}

func (h *Handler) handleDeleteContext(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ContextName string `json:"ContextName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ContextName == "" {
		return nil, fmt.Errorf("%w: ContextName is required", errInvalidRequest)
	}

	c, err := h.Backend.DeleteContext(ctx, req.ContextName)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted context", "name", req.ContextName)

	return json.Marshal(map[string]string{keyContextArn: c.ContextArn})
}

type contextSummary struct {
	Source           contextSourceObject `json:"Source"`
	ContextName      string              `json:"ContextName"`
	ContextArn       string              `json:"ContextArn"`
	ContextType      string              `json:"ContextType"`
	CreationTime     float64             `json:"CreationTime"`
	LastModifiedTime float64             `json:"LastModifiedTime"`
}

func (h *Handler) handleListContexts(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ContextType string `json:"ContextType"`
		SourceURI   string `json:"SourceUri"`
		NextToken   string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	contexts, nextToken := h.Backend.ListContexts(ctx, req.ContextType, req.SourceURI, req.NextToken)

	return marshalSummaryPage("ContextSummaries", nextToken, contexts, contextToSummary)
}

func contextToSummary(c *Context) contextSummary {
	return contextSummary{
		ContextName:      c.ContextName,
		ContextArn:       c.ContextArn,
		ContextType:      c.ContextType,
		Source:           toContextSource(c.Source),
		CreationTime:     epochSeconds(c.CreationTime),
		LastModifiedTime: epochSeconds(c.LastModifiedTime),
	}
}

// ---------------------------------------------------------------------------
// Action handlers (Describe/Update/Delete/List; Create lives in handler.go)
// ---------------------------------------------------------------------------

func (h *Handler) handleDescribeAction(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ActionName string `json:"ActionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ActionName == "" {
		return nil, fmt.Errorf("%w: ActionName is required", errInvalidRequest)
	}

	a, err := h.Backend.DescribeAction(ctx, req.ActionName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"ActionName": a.ActionName,
		keyActionArn: a.ActionArn,
		"ActionType": a.ActionType,
		keySource: map[string]any{
			"SourceUri":  a.Source.SourceURI,
			"SourceType": a.Source.SourceType,
		},
		keyCreationTime:     epochSeconds(a.CreationTime),
		keyLastModifiedTime: epochSeconds(a.LastModifiedTime),
	}
	if a.Description != "" {
		resp["Description"] = a.Description
	}

	if a.Status != "" {
		resp[keyStatus] = a.Status
	}

	if len(a.Properties) > 0 {
		resp["Properties"] = a.Properties
	}

	return json.Marshal(resp)
}

func (h *Handler) handleUpdateAction(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ActionName         string            `json:"ActionName"`
		Description        string            `json:"Description"`
		Status             string            `json:"Status"`
		Properties         map[string]string `json:"Properties"`
		PropertiesToRemove []string          `json:"PropertiesToRemove"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ActionName == "" {
		return nil, fmt.Errorf("%w: ActionName is required", errInvalidRequest)
	}

	a, err := h.Backend.UpdateAction(
		ctx, req.ActionName, req.Description, req.Status, req.Properties, req.PropertiesToRemove,
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated action", "name", a.ActionName)

	return json.Marshal(map[string]string{keyActionArn: a.ActionArn})
}

func (h *Handler) handleDeleteAction(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ActionName string `json:"ActionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ActionName == "" {
		return nil, fmt.Errorf("%w: ActionName is required", errInvalidRequest)
	}

	a, err := h.Backend.DeleteAction(ctx, req.ActionName)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted action", "name", req.ActionName)

	return json.Marshal(map[string]string{keyActionArn: a.ActionArn})
}

type actionSummary struct {
	ActionName       string  `json:"ActionName"`
	ActionArn        string  `json:"ActionArn"`
	ActionType       string  `json:"ActionType"`
	Status           string  `json:"Status,omitempty"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListActions(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ActionType string `json:"ActionType"`
		SourceURI  string `json:"SourceUri"`
		NextToken  string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	actions, nextToken := h.Backend.ListActions(ctx, req.ActionType, req.SourceURI, req.NextToken)

	return marshalSummaryPage("ActionSummaries", nextToken, actions, actionToSummary)
}

func actionToSummary(a *Action) actionSummary {
	return actionSummary{
		ActionName:       a.ActionName,
		ActionArn:        a.ActionArn,
		ActionType:       a.ActionType,
		Status:           a.Status,
		CreationTime:     epochSeconds(a.CreationTime),
		LastModifiedTime: epochSeconds(a.LastModifiedTime),
	}
}

// ---------------------------------------------------------------------------
// Association handlers (Delete/List; AddAssociation lives in handler.go)
// ---------------------------------------------------------------------------

func (h *Handler) handleDeleteAssociation(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		SourceArn      string `json:"SourceArn"`
		DestinationArn string `json:"DestinationArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.SourceArn == "" {
		return nil, fmt.Errorf("%w: SourceArn is required", errInvalidRequest)
	}

	if req.DestinationArn == "" {
		return nil, fmt.Errorf("%w: DestinationArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAssociation(ctx, req.SourceArn, req.DestinationArn); err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted association",
		"source", req.SourceArn, "destination", req.DestinationArn)

	return json.Marshal(map[string]string{"SourceArn": req.SourceArn, "DestinationArn": req.DestinationArn})
}

type associationSummary struct {
	SourceArn       string  `json:"SourceArn"`
	SourceType      string  `json:"SourceType,omitempty"`
	SourceName      string  `json:"SourceName,omitempty"`
	DestinationArn  string  `json:"DestinationArn"`
	DestinationType string  `json:"DestinationType,omitempty"`
	DestinationName string  `json:"DestinationName,omitempty"`
	AssociationType string  `json:"AssociationType,omitempty"`
	CreationTime    float64 `json:"CreationTime"`
}

func (h *Handler) handleListAssociations(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		SourceArn       string `json:"SourceArn"`
		DestinationArn  string `json:"DestinationArn"`
		AssociationType string `json:"AssociationType"`
		NextToken       string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	assocs, nextToken := h.Backend.ListAssociations(
		ctx, req.SourceArn, req.DestinationArn, req.AssociationType, req.NextToken,
	)

	return marshalSummaryPage("AssociationSummaries", nextToken, assocs, func(a *Association) associationSummary {
		srcName, srcType, _, _ := h.Backend.LineageEntityInfo(ctx, a.SourceArn)
		dstName, dstType, _, _ := h.Backend.LineageEntityInfo(ctx, a.DestinationArn)

		return associationSummary{
			SourceArn:       a.SourceArn,
			SourceName:      srcName,
			SourceType:      srcType,
			DestinationArn:  a.DestinationArn,
			DestinationName: dstName,
			DestinationType: dstType,
			AssociationType: a.AssociationType,
			CreationTime:    epochSeconds(a.CreationTime),
		}
	})
}

// ---------------------------------------------------------------------------
// LineageGroup handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleDescribeLineageGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		LineageGroupName string `json:"LineageGroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.LineageGroupName == "" {
		return nil, fmt.Errorf("%w: LineageGroupName is required", errInvalidRequest)
	}

	lineageGroupARN, createdAt, err := h.Backend.DescribeLineageGroup(ctx, req.LineageGroupName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyLineageGroupArn:  lineageGroupARN,
		"LineageGroupName":  req.LineageGroupName,
		keyCreationTime:     epochSeconds(createdAt),
		keyLastModifiedTime: epochSeconds(createdAt),
	})
}

func (h *Handler) handleListLineageGroups(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	lineageGroupARN, createdAt := h.Backend.ListLineageGroups(ctx)

	summaries := []map[string]any{
		{
			keyLineageGroupArn:  lineageGroupARN,
			"LineageGroupName":  defaultLineageGroupName,
			keyCreationTime:     epochSeconds(createdAt),
			keyLastModifiedTime: epochSeconds(createdAt),
		},
	}

	return json.Marshal(map[string]any{"LineageGroupSummaries": summaries})
}

func (h *Handler) handleGetLineageGroupPolicy(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		LineageGroupName string `json:"LineageGroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.LineageGroupName == "" {
		return nil, fmt.Errorf("%w: LineageGroupName is required", errInvalidRequest)
	}

	resourcePolicy, err := h.Backend.GetLineageGroupPolicy(ctx, req.LineageGroupName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"ResourcePolicy": resourcePolicy})
}

// ---------------------------------------------------------------------------
// QueryLineage handler
// ---------------------------------------------------------------------------

func (h *Handler) handleQueryLineage(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MaxDepth     *int32   `json:"MaxDepth"`
		IncludeEdges *bool    `json:"IncludeEdges"`
		Direction    string   `json:"Direction"`
		StartArns    []string `json:"StartArns"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if len(req.StartArns) == 0 {
		return nil, fmt.Errorf("%w: StartArns is required", errInvalidRequest)
	}

	maxDepth := 0
	if req.MaxDepth != nil {
		maxDepth = int(*req.MaxDepth)
	}

	includeEdges := true
	if req.IncludeEdges != nil {
		includeEdges = *req.IncludeEdges
	}

	vertices, edges, err := h.Backend.QueryLineage(ctx, req.StartArns, req.Direction, maxDepth, includeEdges)
	if err != nil {
		return nil, err
	}

	if vertices == nil {
		vertices = []Vertex{}
	}

	if edges == nil {
		edges = []Edge{}
	}

	return json.Marshal(map[string]any{"Vertices": vertices, "Edges": edges})
}

// addAssociationRequest is the request body for AddAssociation.
type addAssociationRequest struct {
	SourceArn       string      `json:"SourceArn"`
	DestinationArn  string      `json:"DestinationArn"`
	AssociationType string      `json:"AssociationType"`
	Tags            []tagObject `json:"Tags"`
}

func (h *Handler) handleAddAssociation(ctx context.Context, body []byte) ([]byte, error) {
	var req addAssociationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.SourceArn == "" {
		return nil, fmt.Errorf("%w: SourceArn is required", errInvalidRequest)
	}

	if req.DestinationArn == "" {
		return nil, fmt.Errorf("%w: DestinationArn is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)

	assoc, err := h.Backend.AddAssociation(
		ctx,
		req.SourceArn,
		req.DestinationArn,
		req.AssociationType,
		tags,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: added association", "arn", assoc.AssociationArn)

	return json.Marshal(map[string]string{"AssociationArn": assoc.AssociationArn})
}

// associateTrialComponentRequest is the request body for AssociateTrialComponent.
type associateTrialComponentRequest struct {
	TrialName          string `json:"TrialName"`
	TrialComponentName string `json:"TrialComponentName"`
}

func (h *Handler) handleAssociateTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req associateTrialComponentRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialName == "" {
		return nil, fmt.Errorf("%w: TrialName is required", errInvalidRequest)
	}

	if req.TrialComponentName == "" {
		return nil, fmt.Errorf("%w: TrialComponentName is required", errInvalidRequest)
	}

	assoc, err := h.Backend.AssociateTrialComponent(ctx, req.TrialName, req.TrialComponentName)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: associated trial component",
		"trial", assoc.TrialArn, "component", assoc.TrialComponentArn)

	return json.Marshal(map[string]string{
		"TrialArn":          assoc.TrialArn,
		"TrialComponentArn": assoc.TrialComponentArn,
	})
}

// actionSourceRequest is the source for a CreateAction request.
type actionSourceRequest struct {
	SourceURI  string `json:"SourceUri"`
	SourceType string `json:"SourceType,omitempty"`
}

// createActionRequest is the request body for CreateAction.
type createActionRequest struct {
	Properties  map[string]string   `json:"Properties"`
	Source      actionSourceRequest `json:"Source"`
	ActionName  string              `json:"ActionName"`
	ActionType  string              `json:"ActionType"`
	Description string              `json:"Description,omitempty"`
	Status      string              `json:"Status,omitempty"`
	Tags        []tagObject         `json:"Tags"`
}

func (h *Handler) handleCreateAction(ctx context.Context, body []byte) ([]byte, error) {
	var req createActionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ActionName == "" {
		return nil, fmt.Errorf("%w: ActionName is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)
	source := ActionSource{
		SourceURI:  req.Source.SourceURI,
		SourceType: req.Source.SourceType,
	}

	a, err := h.Backend.CreateAction(
		ctx,
		req.ActionName,
		req.ActionType,
		req.Description,
		req.Status,
		source,
		req.Properties,
		tags,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: created action", "name", a.ActionName, "arn", a.ActionArn)

	return json.Marshal(map[string]string{"ActionArn": a.ActionArn})
}
