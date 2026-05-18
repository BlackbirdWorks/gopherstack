package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// Resource field key constants used in JSON responses.
const (
	keyDomainID        = "DomainId"
	keyURL             = "Url"
	keyUserProfileName = "UserProfileName"
	keyRoleArn         = "RoleArn"
	keyExperimentName  = "ExperimentName"
	keyStatusField     = keyStatus // alias of keyStatus from handler_stubs.go
)

// Operation name constants for the stateful resource groups.
// Defining them here eliminates goconst warnings when the same string
// appears in switch statements across multiple files.
const (
	opCreateApp                     = "CreateApp"
	opDescribeApp                   = "DescribeApp"
	opListApps                      = "ListApps"
	opDeleteApp                     = "DeleteApp"
	opCreateDomain                  = "CreateDomain"
	opDescribeDomain                = "DescribeDomain"
	opListDomains                   = "ListDomains"
	opDeleteDomain                  = "DeleteDomain"
	opUpdateDomain                  = "UpdateDomain"
	opCreateUserProfile             = "CreateUserProfile"
	opDescribeUserProfile           = "DescribeUserProfile"
	opListUserProfiles              = "ListUserProfiles"
	opDeleteUserProfile             = "DeleteUserProfile"
	opCreateFeatureGroup            = "CreateFeatureGroup"
	opDescribeFeatureGroup          = "DescribeFeatureGroup"
	opListFeatureGroups             = "ListFeatureGroups"
	opDeleteFeatureGroup            = "DeleteFeatureGroup"
	opCreatePipeline                = "CreatePipeline"
	opDescribePipeline              = "DescribePipeline"
	opListPipelines                 = "ListPipelines"
	opUpdatePipeline                = "UpdatePipeline"
	opDeletePipeline                = "DeletePipeline"
	opStartPipelineExecution        = "StartPipelineExecution"
	opDescribePipelineExec          = "DescribePipelineExecution"
	opListPipelineExecutions        = "ListPipelineExecutions"
	opListPipelineParametersForExec = "ListPipelineParametersForExecution"
	opCreateExperiment              = "CreateExperiment"
	opDescribeExperiment            = "DescribeExperiment"
	opListExperiments               = "ListExperiments"
	opDeleteExperiment              = "DeleteExperiment"
	opCreateTrial                   = "CreateTrial"
	opDescribeTrial                 = "DescribeTrial"
	opListTrials                    = "ListTrials"
	opDeleteTrial                   = "DeleteTrial"
	opCreateTrialComponent          = "CreateTrialComponent"
	opDescribeTrialComponent        = "DescribeTrialComponent"
	opDeleteTrialComponent          = "DeleteTrialComponent"
)

func (h *Handler) dispatchStatefulOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	if r, ok, err := h.dispatchDomainOps(ctx, op, body); ok {
		return r, ok, err
	}

	if r, ok, err := h.dispatchFeatureGroupAndPipelineOps(ctx, op, body); ok {
		return r, ok, err
	}

	if r, ok, err := h.dispatchExperimentAndTrialOps(ctx, op, body); ok {
		return r, ok, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchDomainOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case opCreateDomain:
		r, err := h.handleCreateDomain(ctx, body)

		return r, true, err
	case opDescribeDomain:
		r, err := h.handleDescribeDomain(ctx, body)

		return r, true, err
	case opListDomains:
		r, err := h.handleListDomains(body)

		return r, true, err
	case opDeleteDomain:
		return nil, true, h.handleDeleteDomain(ctx, body)
	case opUpdateDomain:
		r, err := h.handleUpdateDomain(ctx, body)

		return r, true, err
	case opCreateUserProfile:
		r, err := h.handleCreateUserProfile(ctx, body)

		return r, true, err
	case opDescribeUserProfile:
		r, err := h.handleDescribeUserProfile(ctx, body)

		return r, true, err
	case opListUserProfiles:
		r, err := h.handleListUserProfiles(body)

		return r, true, err
	case opDeleteUserProfile:
		return nil, true, h.handleDeleteUserProfile(ctx, body)
	case opCreateApp:
		r, err := h.handleCreateApp(ctx, body)

		return r, true, err
	case opDescribeApp:
		r, err := h.handleDescribeApp(ctx, body)

		return r, true, err
	case opListApps:
		r, err := h.handleListApps(body)

		return r, true, err
	case opDeleteApp:
		return nil, true, h.handleDeleteApp(ctx, body)
	}

	return nil, false, nil
}

func (h *Handler) dispatchFeatureGroupAndPipelineOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case opCreateFeatureGroup:
		r, err := h.handleCreateFeatureGroup(ctx, body)

		return r, true, err
	case opDescribeFeatureGroup:
		r, err := h.handleDescribeFeatureGroup(ctx, body)

		return r, true, err
	case opListFeatureGroups:
		r, err := h.handleListFeatureGroups(body)

		return r, true, err
	case opDeleteFeatureGroup:
		return nil, true, h.handleDeleteFeatureGroup(ctx, body)
	case "UpdateFeatureGroup":
		r, err := h.handleUpdateFeatureGroup(ctx, body)

		return r, true, err
	case opCreatePipeline:
		r, err := h.handleCreatePipelineFull(ctx, body)

		return r, true, err
	case opDescribePipeline:
		r, err := h.handleDescribePipeline(ctx, body)

		return r, true, err
	case opListPipelines:
		r, err := h.handleListPipelines(body)

		return r, true, err
	case opUpdatePipeline:
		r, err := h.handleUpdatePipelineFull(ctx, body)

		return r, true, err
	case opDeletePipeline:
		r, err := h.handleDeletePipeline(ctx, body)

		return r, true, err
	case opStartPipelineExecution:
		r, err := h.handleStartPipelineExecutionFull(ctx, body)

		return r, true, err
	case opDescribePipelineExec:
		r, err := h.handleDescribePipelineExecution(ctx, body)

		return r, true, err
	case opListPipelineExecutions:
		r, err := h.handleListPipelineExecutions(body)

		return r, true, err
	case opListPipelineParametersForExec:
		r, err := h.handleListPipelineParametersForExecution(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchExperimentAndTrialOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case opCreateExperiment:
		r, err := h.handleCreateExperiment(ctx, body)

		return r, true, err
	case opDescribeExperiment:
		r, err := h.handleDescribeExperiment(ctx, body)

		return r, true, err
	case opListExperiments:
		r, err := h.handleListExperiments(body)

		return r, true, err
	case opDeleteExperiment:
		r, err := h.handleDeleteExperiment(ctx, body)

		return r, true, err
	case opCreateTrial:
		r, err := h.handleCreateTrial(ctx, body)

		return r, true, err
	case opDescribeTrial:
		r, err := h.handleDescribeTrial(ctx, body)

		return r, true, err
	case opListTrials:
		r, err := h.handleListTrials(body)

		return r, true, err
	case opDeleteTrial:
		r, err := h.handleDeleteTrial(ctx, body)

		return r, true, err
	case opCreateTrialComponent:
		r, err := h.handleCreateTrialComponent(ctx, body)

		return r, true, err
	case opDescribeTrialComponent:
		r, err := h.handleDescribeTrialComponent(ctx, body)

		return r, true, err
	case opDeleteTrialComponent:
		r, err := h.handleDeleteTrialComponent(ctx, body)

		return r, true, err
	case "UpdateExperiment":
		r, err := h.handleUpdateExperiment(ctx, body)

		return r, true, err
	case "UpdateTrial":
		r, err := h.handleUpdateTrial(ctx, body)

		return r, true, err
	case "UpdateTrialComponent":
		r, err := h.handleUpdateTrialComponent(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

// ---------------------------------------------------------------------------
// Domain handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateDomain(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainName string      `json:"DomainName"`
		AuthMode   string      `json:"AuthMode"`
		Tags       []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", errInvalidRequest)
	}

	d, err := h.Backend.CreateDomain(req.DomainName, req.AuthMode, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: created domain", "name", d.DomainName, "id", d.DomainID)

	return json.Marshal(
		map[string]string{keyDomainArn: d.DomainArn, keyDomainID: d.DomainID, keyURL: d.URL},
	)
}

func (h *Handler) handleDescribeDomain(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID string `json:"DomainId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	d, err := h.Backend.DescribeDomain(req.DomainID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyDomainID:         d.DomainID,
		keyDomainArn:        d.DomainArn,
		"DomainName":        d.DomainName,
		keyStatus:           d.Status,
		"AuthMode":          d.AuthMode,
		"Url":               d.URL,
		keyCreationTime:     epochSeconds(d.CreationTime),
		keyLastModifiedTime: epochSeconds(d.LastModifiedTime),
	})
}

type domainSummary struct {
	DomainID     string  `json:"DomainId"`
	DomainArn    string  `json:"DomainArn"`
	DomainName   string  `json:"DomainName"`
	Status       string  `json:"Status"`
	CreationTime float64 `json:"CreationTime"`
}

func (h *Handler) handleListDomains(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	domains, nextToken := h.Backend.ListDomains(req.NextToken)
	summaries := make([]domainSummary, 0, len(domains))

	for _, d := range domains {
		summaries = append(summaries, domainSummary{
			DomainID:     d.DomainID,
			DomainArn:    d.DomainArn,
			DomainName:   d.DomainName,
			Status:       d.Status,
			CreationTime: epochSeconds(d.CreationTime),
		})
	}

	resp := map[string]any{"Domains": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteDomain(ctx context.Context, body []byte) error {
	var req struct {
		DomainID string `json:"DomainId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteDomain(req.DomainID); err != nil {
		return err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted domain", "id", req.DomainID)

	return nil
}

func (h *Handler) handleUpdateDomain(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID string `json:"DomainId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	d, err := h.Backend.UpdateDomain(req.DomainID)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated domain", "id", req.DomainID)

	return json.Marshal(map[string]string{keyDomainArn: d.DomainArn})
}

// ---------------------------------------------------------------------------
// UserProfile handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateUserProfile(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID        string      `json:"DomainId"`
		UserProfileName string      `json:"UserProfileName"`
		Tags            []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.UserProfileName == "" {
		return nil, fmt.Errorf("%w: UserProfileName is required", errInvalidRequest)
	}

	up, err := h.Backend.CreateUserProfile(
		req.DomainID,
		req.UserProfileName,
		fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created user profile", "name", up.UserProfileName)

	return json.Marshal(map[string]string{keyUserProfileArn: up.UserProfileArn})
}

func (h *Handler) handleDescribeUserProfile(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID        string `json:"DomainId"`
		UserProfileName string `json:"UserProfileName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.UserProfileName == "" {
		return nil, fmt.Errorf("%w: UserProfileName is required", errInvalidRequest)
	}

	up, err := h.Backend.DescribeUserProfile(req.DomainID, req.UserProfileName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"DomainId":          up.DomainID,
		"UserProfileName":   up.UserProfileName,
		keyUserProfileArn:   up.UserProfileArn,
		keyStatus:           up.Status,
		keyCreationTime:     epochSeconds(up.CreationTime),
		keyLastModifiedTime: epochSeconds(up.LastModifiedTime),
	})
}

type userProfileSummary struct {
	DomainID        string  `json:"DomainId"`
	UserProfileName string  `json:"UserProfileName"`
	UserProfileArn  string  `json:"UserProfileArn"`
	Status          string  `json:"Status"`
	CreationTime    float64 `json:"CreationTime"`
}

func (h *Handler) handleListUserProfiles(body []byte) ([]byte, error) {
	var req struct {
		DomainIDEquals string `json:"DomainIDEquals"`
		NextToken      string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	ups, nextToken := h.Backend.ListUserProfiles(req.DomainIDEquals, req.NextToken)
	summaries := make([]userProfileSummary, 0, len(ups))

	for _, up := range ups {
		summaries = append(summaries, userProfileSummary{
			DomainID:        up.DomainID,
			UserProfileName: up.UserProfileName,
			UserProfileArn:  up.UserProfileArn,
			Status:          up.Status,
			CreationTime:    epochSeconds(up.CreationTime),
		})
	}

	resp := map[string]any{"UserProfiles": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteUserProfile(ctx context.Context, body []byte) error {
	var req struct {
		DomainID        string `json:"DomainId"`
		UserProfileName string `json:"UserProfileName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.UserProfileName == "" {
		return fmt.Errorf("%w: UserProfileName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteUserProfile(req.DomainID, req.UserProfileName); err != nil {
		return err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: deleted user profile", "name", req.UserProfileName)

	return nil
}

// ---------------------------------------------------------------------------
// App handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateApp(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID        string      `json:"DomainId"`
		UserProfileName string      `json:"UserProfileName"`
		AppType         string      `json:"AppType"`
		AppName         string      `json:"AppName"`
		Tags            []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.AppType == "" {
		return nil, fmt.Errorf("%w: AppType is required", errInvalidRequest)
	}

	if req.AppName == "" {
		return nil, fmt.Errorf("%w: AppName is required", errInvalidRequest)
	}

	a, err := h.Backend.CreateApp(
		req.DomainID,
		req.UserProfileName,
		req.AppType,
		req.AppName,
		fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created app", "name", a.AppName, "arn", a.AppArn)

	return json.Marshal(map[string]string{keyAppArn: a.AppArn})
}

func (h *Handler) handleDescribeApp(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID        string `json:"DomainId"`
		UserProfileName string `json:"UserProfileName"`
		AppType         string `json:"AppType"`
		AppName         string `json:"AppName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.AppType == "" {
		return nil, fmt.Errorf("%w: AppType is required", errInvalidRequest)
	}

	if req.AppName == "" {
		return nil, fmt.Errorf("%w: AppName is required", errInvalidRequest)
	}

	a, err := h.Backend.DescribeApp(req.DomainID, req.UserProfileName, req.AppType, req.AppName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"DomainId":         a.DomainID,
		keyUserProfileName: a.UserProfileName,
		"AppType":          a.AppType,
		"AppName":          a.AppName,
		keyAppArn:          a.AppArn,
		keyStatus:          a.Status,
		keyCreationTime:    epochSeconds(a.CreationTime),
	})
}

type appSummary struct {
	DomainID        string  `json:"DomainId"`
	UserProfileName string  `json:"UserProfileName"`
	AppType         string  `json:"AppType"`
	AppName         string  `json:"AppName"`
	AppArn          string  `json:"AppArn"`
	Status          string  `json:"Status"`
	CreationTime    float64 `json:"CreationTime"`
}

func (h *Handler) handleListApps(body []byte) ([]byte, error) {
	var req struct {
		DomainIDEquals string `json:"DomainIDEquals"`
		NextToken      string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	apps, nextToken := h.Backend.ListApps(req.DomainIDEquals, req.NextToken)
	summaries := make([]appSummary, 0, len(apps))

	for _, a := range apps {
		summaries = append(summaries, appSummary{
			DomainID:        a.DomainID,
			UserProfileName: a.UserProfileName,
			AppType:         a.AppType,
			AppName:         a.AppName,
			AppArn:          a.AppArn,
			Status:          a.Status,
			CreationTime:    epochSeconds(a.CreationTime),
		})
	}

	resp := map[string]any{"Apps": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteApp(ctx context.Context, body []byte) error {
	var req struct {
		DomainID        string `json:"DomainId"`
		UserProfileName string `json:"UserProfileName"`
		AppType         string `json:"AppType"`
		AppName         string `json:"AppName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.AppType == "" {
		return fmt.Errorf("%w: AppType is required", errInvalidRequest)
	}

	if req.AppName == "" {
		return fmt.Errorf("%w: AppName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteApp(req.DomainID, req.UserProfileName, req.AppType, req.AppName); err != nil {
		return err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted app", "name", req.AppName)

	return nil
}

// ---------------------------------------------------------------------------
// FeatureGroup handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateFeatureGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		FeatureGroupName            string              `json:"FeatureGroupName"`
		RecordIdentifierFeatureName string              `json:"RecordIdentifierFeatureName"`
		EventTimeFeatureName        string              `json:"EventTimeFeatureName"`
		FeatureDefinitions          []FeatureDefinition `json:"FeatureDefinitions"`
		Tags                        []tagObject         `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FeatureGroupName == "" {
		return nil, fmt.Errorf("%w: FeatureGroupName is required", errInvalidRequest)
	}

	fg, err := h.Backend.CreateFeatureGroup(
		req.FeatureGroupName,
		req.RecordIdentifierFeatureName,
		req.EventTimeFeatureName,
		req.FeatureDefinitions,
		fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: created feature group", "name", fg.FeatureGroupName)

	return json.Marshal(map[string]string{keyFeatureGroupArn: fg.FeatureGroupArn})
}

func (h *Handler) handleDescribeFeatureGroup(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		FeatureGroupName string `json:"FeatureGroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FeatureGroupName == "" {
		return nil, fmt.Errorf("%w: FeatureGroupName is required", errInvalidRequest)
	}

	fg, err := h.Backend.DescribeFeatureGroup(req.FeatureGroupName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyFeatureGroupName:            fg.FeatureGroupName,
		keyFeatureGroupArn:             fg.FeatureGroupArn,
		keyFeatureGroupStatus:          fg.FeatureGroupStatus,
		keyRecordIdentifierFeatureName: fg.RecordIdentifierFeatureName,
		keyEventTimeFeatureName:        fg.EventTimeFeatureName,
		keyFeatureDefinitions:          fg.FeatureDefinitions,
		keyCreationTime:                epochSeconds(fg.CreationTime),
	})
}

type featureGroupSummary struct {
	FeatureGroupName   string  `json:"FeatureGroupName"`
	FeatureGroupArn    string  `json:"FeatureGroupArn"`
	FeatureGroupStatus string  `json:"FeatureGroupStatus"`
	CreationTime       float64 `json:"CreationTime"`
}

func (h *Handler) handleListFeatureGroups(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	fgs, nextToken := h.Backend.ListFeatureGroups(req.NextToken)
	summaries := make([]featureGroupSummary, 0, len(fgs))

	for _, fg := range fgs {
		summaries = append(summaries, featureGroupSummary{
			FeatureGroupName:   fg.FeatureGroupName,
			FeatureGroupArn:    fg.FeatureGroupArn,
			FeatureGroupStatus: fg.FeatureGroupStatus,
			CreationTime:       epochSeconds(fg.CreationTime),
		})
	}

	resp := map[string]any{"FeatureGroupSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteFeatureGroup(ctx context.Context, body []byte) error {
	var req struct {
		FeatureGroupName string `json:"FeatureGroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FeatureGroupName == "" {
		return fmt.Errorf("%w: FeatureGroupName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteFeatureGroup(req.FeatureGroupName); err != nil {
		return err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: deleted feature group", "name", req.FeatureGroupName)

	return nil
}

// ---------------------------------------------------------------------------
// Pipeline handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleDescribePipeline(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineName string `json:"PipelineName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	p, err := h.Backend.DescribePipeline(req.PipelineName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"PipelineName":       p.PipelineName,
		keyPipelineArn:       p.PipelineArn,
		"PipelineStatus":     p.PipelineStatus,
		"PipelineDefinition": p.PipelineDefinition,
		keyRoleArn:           p.RoleArn,
		keyCreationTime:      epochSeconds(p.CreationTime),
		keyLastModifiedTime:  epochSeconds(p.LastModifiedTime),
	}
	if p.PipelineDisplayName != "" {
		resp["PipelineDisplayName"] = p.PipelineDisplayName
	}
	if p.PipelineDescription != "" {
		resp["PipelineDescription"] = p.PipelineDescription
	}
	if p.ParallelismConfiguration != nil {
		resp["ParallelismConfiguration"] = p.ParallelismConfiguration
	}

	return json.Marshal(resp)
}

type pipelineSummary struct {
	PipelineName     string  `json:"PipelineName"`
	PipelineArn      string  `json:"PipelineArn"`
	PipelineStatus   string  `json:"PipelineStatus"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListPipelines(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	ps, nextToken := h.Backend.ListPipelines(req.NextToken)
	summaries := make([]pipelineSummary, 0, len(ps))

	for _, p := range ps {
		summaries = append(summaries, pipelineSummary{
			PipelineName:     p.PipelineName,
			PipelineArn:      p.PipelineArn,
			PipelineStatus:   p.PipelineStatus,
			CreationTime:     epochSeconds(p.CreationTime),
			LastModifiedTime: epochSeconds(p.LastModifiedTime),
		})
	}

	resp := map[string]any{"PipelineSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeletePipeline(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineName string `json:"PipelineName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	p, err := h.Backend.DeletePipeline(req.PipelineName)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted pipeline", "name", req.PipelineName)

	return json.Marshal(map[string]string{keyPipelineArn: p.PipelineArn})
}

func (h *Handler) handleDescribePipelineExecution(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineExecutionArn string `json:"PipelineExecutionArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	pe, err := h.Backend.DescribePipelineExecution(req.PipelineExecutionArn)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		keyPipelineArn:             pe.PipelineArn,
		keyPipelineExecutionArn:    pe.PipelineExecutionArn,
		keyPipelineExecutionStatus: pe.PipelineExecutionStatus,
		"StartTime":                epochSeconds(pe.StartTime),
	}
	if pe.PipelineExecutionDisplayName != "" {
		resp["PipelineExecutionDisplayName"] = pe.PipelineExecutionDisplayName
	}
	if pe.PipelineExecutionDescription != "" {
		resp["PipelineExecutionDescription"] = pe.PipelineExecutionDescription
	}
	if len(pe.PipelineParameters) > 0 {
		resp["PipelineParameters"] = pe.PipelineParameters
	}
	if pe.FailureReason != "" {
		resp["FailureReason"] = pe.FailureReason
	}

	return json.Marshal(resp)
}

type pipelineExecutionSummary struct {
	PipelineExecutionArn    string  `json:"PipelineExecutionArn"`
	PipelineExecutionStatus string  `json:"PipelineExecutionStatus"`
	StartTime               float64 `json:"StartTime"`
}

func (h *Handler) handleListPipelineExecutions(body []byte) ([]byte, error) {
	var req struct {
		PipelineName string `json:"PipelineName"`
		NextToken    string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	pes, nextToken := h.Backend.ListPipelineExecutions(req.PipelineName, req.NextToken)
	summaries := make([]pipelineExecutionSummary, 0, len(pes))

	for _, pe := range pes {
		summaries = append(summaries, pipelineExecutionSummary{
			PipelineExecutionArn:    pe.PipelineExecutionArn,
			PipelineExecutionStatus: pe.PipelineExecutionStatus,
			StartTime:               epochSeconds(pe.StartTime),
		})
	}

	resp := map[string]any{"PipelineExecutionSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// ---------------------------------------------------------------------------
// Experiment handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ExperimentName string      `json:"ExperimentName"`
		Tags           []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ExperimentName == "" {
		return nil, fmt.Errorf("%w: ExperimentName is required", errInvalidRequest)
	}

	e, err := h.Backend.CreateExperiment(req.ExperimentName, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created experiment", "name", e.ExperimentName)

	return json.Marshal(map[string]string{keyExperimentArn: e.ExperimentArn})
}

func (h *Handler) handleDescribeExperiment(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ExperimentName string `json:"ExperimentName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ExperimentName == "" {
		return nil, fmt.Errorf("%w: ExperimentName is required", errInvalidRequest)
	}

	e, err := h.Backend.DescribeExperiment(req.ExperimentName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		keyExperimentName:   e.ExperimentName,
		keyExperimentArn:    e.ExperimentArn,
		keyCreationTime:     epochSeconds(e.CreationTime),
		keyLastModifiedTime: epochSeconds(e.LastModifiedTime),
	}
	if e.DisplayName != "" {
		resp["DisplayName"] = e.DisplayName
	}
	if e.Description != "" {
		resp["Description"] = e.Description
	}

	return json.Marshal(resp)
}

type experimentSummary struct {
	ExperimentName string  `json:"ExperimentName"`
	ExperimentArn  string  `json:"ExperimentArn"`
	CreationTime   float64 `json:"CreationTime"`
}

func (h *Handler) handleListExperiments(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	exps, nextToken := h.Backend.ListExperiments(req.NextToken)
	summaries := make([]experimentSummary, 0, len(exps))

	for _, e := range exps {
		summaries = append(summaries, experimentSummary{
			ExperimentName: e.ExperimentName,
			ExperimentArn:  e.ExperimentArn,
			CreationTime:   epochSeconds(e.CreationTime),
		})
	}

	resp := map[string]any{"ExperimentSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ExperimentName string `json:"ExperimentName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ExperimentName == "" {
		return nil, fmt.Errorf("%w: ExperimentName is required", errInvalidRequest)
	}

	e, err := h.Backend.DeleteExperiment(req.ExperimentName)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted experiment", "name", req.ExperimentName)

	return json.Marshal(map[string]string{keyExperimentArn: e.ExperimentArn})
}

// ---------------------------------------------------------------------------
// Trial handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateTrial(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrialName      string      `json:"TrialName"`
		ExperimentName string      `json:"ExperimentName"`
		Tags           []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialName == "" {
		return nil, fmt.Errorf("%w: TrialName is required", errInvalidRequest)
	}

	t, err := h.Backend.CreateTrial(req.TrialName, req.ExperimentName, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created trial", "name", t.TrialName)

	return json.Marshal(map[string]string{keyTrialArn: t.TrialArn})
}

func (h *Handler) handleDescribeTrial(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrialName string `json:"TrialName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialName == "" {
		return nil, fmt.Errorf("%w: TrialName is required", errInvalidRequest)
	}

	t, err := h.Backend.DescribeTrial(req.TrialName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"TrialName":         t.TrialName,
		keyTrialArn:         t.TrialArn,
		keyExperimentName:   t.ExperimentName,
		keyCreationTime:     epochSeconds(t.CreationTime),
		keyLastModifiedTime: epochSeconds(t.LastModifiedTime),
	}
	if t.DisplayName != "" {
		resp["DisplayName"] = t.DisplayName
	}

	return json.Marshal(resp)
}

type trialSummary struct {
	TrialName    string  `json:"TrialName"`
	TrialArn     string  `json:"TrialArn"`
	CreationTime float64 `json:"CreationTime"`
}

func (h *Handler) handleListTrials(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	ts, nextToken := h.Backend.ListTrials(req.NextToken)
	summaries := make([]trialSummary, 0, len(ts))

	for _, t := range ts {
		summaries = append(summaries, trialSummary{
			TrialName:    t.TrialName,
			TrialArn:     t.TrialArn,
			CreationTime: epochSeconds(t.CreationTime),
		})
	}

	resp := map[string]any{"TrialSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteTrial(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrialName string `json:"TrialName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialName == "" {
		return nil, fmt.Errorf("%w: TrialName is required", errInvalidRequest)
	}

	t, err := h.Backend.DeleteTrial(req.TrialName)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted trial", "name", req.TrialName)

	return json.Marshal(map[string]string{keyTrialArn: t.TrialArn})
}

// ---------------------------------------------------------------------------
// TrialComponent handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrialComponentName string      `json:"TrialComponentName"`
		Tags               []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialComponentName == "" {
		return nil, fmt.Errorf("%w: TrialComponentName is required", errInvalidRequest)
	}

	tc, err := h.Backend.CreateTrialComponent(req.TrialComponentName, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: created trial component", "name", tc.TrialComponentName)

	return json.Marshal(map[string]string{keyTrialComponentArn: tc.TrialComponentArn})
}

func (h *Handler) handleDescribeTrialComponent(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrialComponentName string `json:"TrialComponentName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialComponentName == "" {
		return nil, fmt.Errorf("%w: TrialComponentName is required", errInvalidRequest)
	}

	tc, err := h.Backend.DescribeTrialComponent(req.TrialComponentName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"TrialComponentName": tc.TrialComponentName,
		keyTrialComponentArn: tc.TrialComponentArn,
		keyCreationTime:      epochSeconds(tc.CreationTime),
		keyLastModifiedTime:  epochSeconds(tc.LastModifiedTime),
	}
	if tc.DisplayName != "" {
		resp["DisplayName"] = tc.DisplayName
	}
	if tc.Status != "" {
		resp["Status"] = tc.Status
	}
	if len(tc.Parameters) > 0 {
		resp["Parameters"] = tc.Parameters
	}
	if len(tc.InputArtifacts) > 0 {
		resp["InputArtifacts"] = tc.InputArtifacts
	}
	if len(tc.OutputArtifacts) > 0 {
		resp["OutputArtifacts"] = tc.OutputArtifacts
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrialComponentName string `json:"TrialComponentName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialComponentName == "" {
		return nil, fmt.Errorf("%w: TrialComponentName is required", errInvalidRequest)
	}

	tc, err := h.Backend.DeleteTrialComponent(req.TrialComponentName)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: deleted trial component", "name", req.TrialComponentName)

	return json.Marshal(map[string]string{keyTrialComponentArn: tc.TrialComponentArn})
}
