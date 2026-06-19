package rekognition

import (
	"context"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) appendixAOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		// Projects
		"CreateProject":    service.WrapOp(h.handleCreateProject),
		"DeleteProject":    service.WrapOp(h.handleDeleteProject),
		"DescribeProjects": service.WrapOp(h.handleDescribeProjects),
		// Project Versions
		"CreateProjectVersion":    service.WrapOp(h.handleCreateProjectVersion),
		"DeleteProjectVersion":    service.WrapOp(h.handleDeleteProjectVersion),
		"DescribeProjectVersions": service.WrapOp(h.handleDescribeProjectVersions),
		"CopyProjectVersion":      service.WrapOp(h.handleCopyProjectVersion),
		"StartProjectVersion":     service.WrapOp(h.handleStartProjectVersion),
		"StopProjectVersion":      service.WrapOp(h.handleStopProjectVersion),
		// Project Policies
		"ListProjectPolicies": service.WrapOp(h.handleListProjectPolicies),
		"PutProjectPolicy":    service.WrapOp(h.handlePutProjectPolicy),
		"DeleteProjectPolicy": service.WrapOp(h.handleDeleteProjectPolicy),
		// Datasets
		"CreateDataset":            service.WrapOp(h.handleCreateDataset),
		"DeleteDataset":            service.WrapOp(h.handleDeleteDataset),
		"DescribeDataset":          service.WrapOp(h.handleDescribeDataset),
		"ListDatasetEntries":       service.WrapOp(h.handleListDatasetEntries),
		"ListDatasetLabels":        service.WrapOp(h.handleListDatasetLabels),
		"UpdateDatasetEntries":     service.WrapOp(h.handleUpdateDatasetEntries),
		"DistributeDatasetEntries": service.WrapOp(h.handleDistributeDatasetEntries),
		// Users
		"CreateUser":         service.WrapOp(h.handleCreateUser),
		"DeleteUser":         service.WrapOp(h.handleDeleteUser),
		"ListUsers":          service.WrapOp(h.handleListUsers),
		"AssociateFaces":     service.WrapOp(h.handleAssociateFaces),
		"DisassociateFaces":  service.WrapOp(h.handleDisassociateFaces),
		"SearchUsers":        service.WrapOp(h.handleSearchUsers),
		"SearchUsersByImage": service.WrapOp(h.handleSearchUsersByImage),
		// Face Liveness
		"CreateFaceLivenessSession":     service.WrapOp(h.handleCreateFaceLivenessSession),
		"GetFaceLivenessSessionResults": service.WrapOp(h.handleGetFaceLivenessSessionResults),
		// Image analysis
		"CompareFaces":              service.WrapOp(h.handleCompareFaces),
		"DetectFaces":               service.WrapOp(h.handleDetectFaces),
		"DetectLabels":              service.WrapOp(h.handleDetectLabels),
		"DetectText":                service.WrapOp(h.handleDetectText),
		"DetectCustomLabels":        service.WrapOp(h.handleDetectCustomLabels),
		"DetectModerationLabels":    service.WrapOp(h.handleDetectModerationLabels),
		"DetectProtectiveEquipment": service.WrapOp(h.handleDetectProtectiveEquipment),
		"RecognizeCelebrities":      service.WrapOp(h.handleRecognizeCelebrities),
		"GetCelebrityInfo":          service.WrapOp(h.handleGetCelebrityInfo),
		// Async video jobs
		"StartCelebrityRecognition": service.WrapOp(h.handleStartCelebrityRecognition),
		"GetCelebrityRecognition":   service.WrapOp(h.handleGetCelebrityRecognition),
		"StartContentModeration":    service.WrapOp(h.handleStartContentModeration),
		"GetContentModeration":      service.WrapOp(h.handleGetContentModeration),
		"StartFaceDetection":        service.WrapOp(h.handleStartFaceDetection),
		"GetFaceDetection":          service.WrapOp(h.handleGetFaceDetection),
		"StartFaceSearch":           service.WrapOp(h.handleStartFaceSearch),
		"GetFaceSearch":             service.WrapOp(h.handleGetFaceSearch),
		"StartLabelDetection":       service.WrapOp(h.handleStartLabelDetection),
		"GetLabelDetection":         service.WrapOp(h.handleGetLabelDetection),
		"StartPersonTracking":       service.WrapOp(h.handleStartPersonTracking),
		"GetPersonTracking":         service.WrapOp(h.handleGetPersonTracking),
		"StartSegmentDetection":     service.WrapOp(h.handleStartSegmentDetection),
		"GetSegmentDetection":       service.WrapOp(h.handleGetSegmentDetection),
		"StartTextDetection":        service.WrapOp(h.handleStartTextDetection),
		"GetTextDetection":          service.WrapOp(h.handleGetTextDetection),
		"StartMediaAnalysisJob":     service.WrapOp(h.handleStartMediaAnalysisJob),
		"GetMediaAnalysisJob":       service.WrapOp(h.handleGetMediaAnalysisJob),
		"ListMediaAnalysisJobs":     service.WrapOp(h.handleListMediaAnalysisJobs),
	}
}

// =============================================================================
// Projects
// =============================================================================

type createProjectReq struct {
	ProjectName string `json:"ProjectName"`
}

type createProjectResp struct {
	ProjectArn string `json:"ProjectArn"`
}

func (h *Handler) handleCreateProject(_ context.Context, req *createProjectReq) (*createProjectResp, error) {
	if req.ProjectName == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", ErrValidation)
	}

	proj, err := h.Backend.CreateProject(req.ProjectName)
	if err != nil {
		return nil, err
	}

	return &createProjectResp{ProjectArn: proj.ProjectARN}, nil
}

type deleteProjectReq struct {
	ProjectArn string `json:"ProjectArn"`
}

type deleteProjectResp struct {
	Status string `json:"Status"`
}

func (h *Handler) handleDeleteProject(_ context.Context, req *deleteProjectReq) (*deleteProjectResp, error) {
	if req.ProjectArn == "" {
		return nil, fmt.Errorf("%w: ProjectArn is required", ErrValidation)
	}

	if err := h.Backend.DeleteProject(req.ProjectArn); err != nil {
		return nil, err
	}

	return &deleteProjectResp{Status: "DELETING"}, nil
}

type describeProjectsReq struct { //nolint:govet // existing issue.
	ProjectArns []string `json:"ProjectArns"`
	NextToken   string   `json:"NextToken"`
	MaxResults  int32    `json:"MaxResults"`
}

type projectDescription struct {
	ProjectArn        string `json:"ProjectArn"`
	Status            string `json:"Status"`
	CreationTimestamp string `json:"CreationTimestamp"`
}

type describeProjectsResp struct {
	NextToken           string               `json:"NextToken,omitempty"`
	ProjectDescriptions []projectDescription `json:"ProjectDescriptions"`
}

func (h *Handler) handleDescribeProjects(
	_ context.Context, req *describeProjectsReq,
) (*describeProjectsResp, error) {
	projects, nextToken, err := h.Backend.DescribeProjects(req.ProjectArns, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	descriptions := make([]projectDescription, 0, len(projects))
	for _, p := range projects {
		descriptions = append(descriptions, projectDescription{
			ProjectArn:        p.ProjectARN,
			Status:            p.Status,
			CreationTimestamp: p.CreationTimestamp.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	return &describeProjectsResp{
		ProjectDescriptions: descriptions,
		NextToken:           nextToken,
	}, nil
}

// =============================================================================
// Project Versions
// =============================================================================

type createProjectVersionReq struct {
	ProjectArn  string `json:"ProjectArn"`
	VersionName string `json:"VersionName"`
}

type createProjectVersionResp struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
}

func (h *Handler) handleCreateProjectVersion(
	_ context.Context, req *createProjectVersionReq,
) (*createProjectVersionResp, error) {
	if req.ProjectArn == "" {
		return nil, fmt.Errorf("%w: ProjectArn is required", ErrValidation)
	}

	if req.VersionName == "" {
		return nil, fmt.Errorf("%w: VersionName is required", ErrValidation)
	}

	v, err := h.Backend.CreateProjectVersion(req.ProjectArn, req.VersionName)
	if err != nil {
		return nil, err
	}

	return &createProjectVersionResp{ProjectVersionArn: v.ProjectVersionARN}, nil
}

type deleteProjectVersionReq struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
}

type deleteProjectVersionResp struct {
	Status string `json:"Status"`
}

func (h *Handler) handleDeleteProjectVersion(
	_ context.Context, req *deleteProjectVersionReq,
) (*deleteProjectVersionResp, error) {
	if req.ProjectVersionArn == "" {
		return nil, fmt.Errorf("%w: ProjectVersionArn is required", ErrValidation)
	}

	if err := h.Backend.DeleteProjectVersion(req.ProjectVersionArn); err != nil {
		return nil, err
	}

	return &deleteProjectVersionResp{Status: "DELETING"}, nil
}

type describeProjectVersionsReq struct { //nolint:govet // existing issue.
	ProjectArn   string   `json:"ProjectArn"`
	VersionNames []string `json:"VersionNames"`
	NextToken    string   `json:"NextToken"`
	MaxResults   int32    `json:"MaxResults"`
}

type projectVersionDescription struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
	Status            string `json:"Status"`
	StatusMessage     string `json:"StatusMessage,omitempty"`
	CreationTimestamp string `json:"CreationTimestamp"`
}

type describeProjectVersionsResp struct {
	NextToken                  string                      `json:"NextToken,omitempty"`
	ProjectVersionDescriptions []projectVersionDescription `json:"ProjectVersionDescriptions"`
}

func (h *Handler) handleDescribeProjectVersions(
	_ context.Context, req *describeProjectVersionsReq,
) (*describeProjectVersionsResp, error) {
	versions, nextToken, err := h.Backend.DescribeProjectVersions(
		req.ProjectArn, req.VersionNames, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	descriptions := make([]projectVersionDescription, 0, len(versions))
	for _, v := range versions {
		descriptions = append(descriptions, projectVersionDescription{
			ProjectVersionArn: v.ProjectVersionARN,
			Status:            v.Status,
			StatusMessage:     v.StatusMessage,
			CreationTimestamp: v.CreationTimestamp.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	return &describeProjectVersionsResp{
		ProjectVersionDescriptions: descriptions,
		NextToken:                  nextToken,
	}, nil
}

type copyProjectVersionReq struct {
	SourceProjectVersionArn string `json:"SourceProjectVersionArn"`
	DestinationProjectArn   string `json:"DestinationProjectArn"`
	VersionName             string `json:"VersionName"`
}

type copyProjectVersionResp struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
}

func (h *Handler) handleCopyProjectVersion(
	_ context.Context, req *copyProjectVersionReq,
) (*copyProjectVersionResp, error) {
	if req.SourceProjectVersionArn == "" {
		return nil, fmt.Errorf("%w: SourceProjectVersionArn is required", ErrValidation)
	}

	if req.DestinationProjectArn == "" {
		return nil, fmt.Errorf("%w: DestinationProjectArn is required", ErrValidation)
	}

	v, err := h.Backend.CopyProjectVersion(
		req.SourceProjectVersionArn, req.DestinationProjectArn, req.VersionName,
	)
	if err != nil {
		return nil, err
	}

	return &copyProjectVersionResp{ProjectVersionArn: v.ProjectVersionARN}, nil
}

type startProjectVersionReq struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
	MinInferenceUnits int32  `json:"MinInferenceUnits"`
}

type startProjectVersionResp struct {
	Status string `json:"Status"`
}

func (h *Handler) handleStartProjectVersion(
	_ context.Context, req *startProjectVersionReq,
) (*startProjectVersionResp, error) {
	if req.ProjectVersionArn == "" {
		return nil, fmt.Errorf("%w: ProjectVersionArn is required", ErrValidation)
	}

	if err := h.Backend.StartProjectVersion(req.ProjectVersionArn, req.MinInferenceUnits); err != nil {
		return nil, err
	}

	return &startProjectVersionResp{Status: "RUNNING"}, nil
}

type stopProjectVersionReq struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
}

type stopProjectVersionResp struct {
	Status string `json:"Status"`
}

func (h *Handler) handleStopProjectVersion(
	_ context.Context, req *stopProjectVersionReq,
) (*stopProjectVersionResp, error) {
	if req.ProjectVersionArn == "" {
		return nil, fmt.Errorf("%w: ProjectVersionArn is required", ErrValidation)
	}

	if err := h.Backend.StopProjectVersion(req.ProjectVersionArn); err != nil {
		return nil, err
	}

	return &stopProjectVersionResp{Status: "STOPPED"}, nil
}

// =============================================================================
// Project Policies
// =============================================================================

type listProjectPoliciesReq struct {
	ProjectArn string `json:"ProjectArn"`
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type projectPolicyEntry struct {
	ProjectArn           string `json:"ProjectArn"`
	PolicyName           string `json:"PolicyName"`
	PolicyRevisionId     string `json:"PolicyRevisionId"` //nolint:revive,staticcheck // existing issue.
	PolicyDocument       string `json:"PolicyDocument"`
	CreationTimestamp    string `json:"CreationTimestamp"`
	LastUpdatedTimestamp string `json:"LastUpdatedTimestamp"`
}

type listProjectPoliciesResp struct {
	NextToken       string               `json:"NextToken,omitempty"`
	ProjectPolicies []projectPolicyEntry `json:"ProjectPolicies"`
}

func (h *Handler) handleListProjectPolicies(
	_ context.Context, req *listProjectPoliciesReq,
) (*listProjectPoliciesResp, error) {
	if req.ProjectArn == "" {
		return nil, fmt.Errorf("%w: ProjectArn is required", ErrValidation)
	}

	policies, nextToken, err := h.Backend.ListProjectPolicies(req.ProjectArn, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	entries := make([]projectPolicyEntry, 0, len(policies))
	for _, p := range policies {
		entries = append(entries, projectPolicyEntry{
			ProjectArn:           p.ProjectARN,
			PolicyName:           p.PolicyName,
			PolicyRevisionId:     p.PolicyRevisionID,
			PolicyDocument:       p.PolicyDocument,
			CreationTimestamp:    p.CreationTimestamp.Format("2006-01-02T15:04:05.000Z"),
			LastUpdatedTimestamp: p.LastUpdatedTimestamp.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	return &listProjectPoliciesResp{
		ProjectPolicies: entries,
		NextToken:       nextToken,
	}, nil
}

type putProjectPolicyReq struct {
	ProjectArn       string `json:"ProjectArn"`
	PolicyName       string `json:"PolicyName"`
	PolicyDocument   string `json:"PolicyDocument"`
	PolicyRevisionId string `json:"PolicyRevisionId"` //nolint:revive,staticcheck // existing issue.
}

type putProjectPolicyResp struct {
	PolicyRevisionId string `json:"PolicyRevisionId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handlePutProjectPolicy(
	_ context.Context, req *putProjectPolicyReq,
) (*putProjectPolicyResp, error) {
	if req.ProjectArn == "" {
		return nil, fmt.Errorf("%w: ProjectArn is required", ErrValidation)
	}

	if req.PolicyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", ErrValidation)
	}

	revID, err := h.Backend.PutProjectPolicy(
		req.ProjectArn, req.PolicyName, req.PolicyDocument, req.PolicyRevisionId,
	)
	if err != nil {
		return nil, err
	}

	return &putProjectPolicyResp{PolicyRevisionId: revID}, nil
}

type deleteProjectPolicyReq struct {
	ProjectArn       string `json:"ProjectArn"`
	PolicyName       string `json:"PolicyName"`
	PolicyRevisionId string `json:"PolicyRevisionId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteProjectPolicy(
	_ context.Context, req *deleteProjectPolicyReq,
) (*struct{}, error) {
	if req.ProjectArn == "" {
		return nil, fmt.Errorf("%w: ProjectArn is required", ErrValidation)
	}

	if req.PolicyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", ErrValidation)
	}

	if err := h.Backend.DeleteProjectPolicy(req.ProjectArn, req.PolicyName, req.PolicyRevisionId); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// =============================================================================
// Datasets
// =============================================================================

type createDatasetReq struct {
	ProjectArn  string `json:"ProjectArn"`
	DatasetType string `json:"DatasetType"`
}

type createDatasetResp struct {
	DatasetArn string `json:"DatasetArn"`
}

func (h *Handler) handleCreateDataset(_ context.Context, req *createDatasetReq) (*createDatasetResp, error) {
	if req.ProjectArn == "" {
		return nil, fmt.Errorf("%w: ProjectArn is required", ErrValidation)
	}

	if req.DatasetType == "" {
		return nil, fmt.Errorf("%w: DatasetType is required", ErrValidation)
	}

	ds, err := h.Backend.CreateDataset(req.ProjectArn, req.DatasetType)
	if err != nil {
		return nil, err
	}

	return &createDatasetResp{DatasetArn: ds.DatasetARN}, nil
}

type deleteDatasetReq struct {
	DatasetArn string `json:"DatasetArn"`
}

func (h *Handler) handleDeleteDataset(_ context.Context, req *deleteDatasetReq) (*struct{}, error) {
	if req.DatasetArn == "" {
		return nil, fmt.Errorf("%w: DatasetArn is required", ErrValidation)
	}

	if err := h.Backend.DeleteDataset(req.DatasetArn); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeDatasetReq struct {
	DatasetArn string `json:"DatasetArn"`
}

type datasetDescription struct {
	DatasetArn           string `json:"DatasetArn"`
	ProjectArn           string `json:"ProjectArn"`
	DatasetType          string `json:"DatasetType"`
	Status               string `json:"Status"`
	StatusMessage        string `json:"StatusMessage,omitempty"`
	CreationTimestamp    string `json:"CreationTimestamp"`
	LastUpdatedTimestamp string `json:"LastUpdatedTimestamp"`
}

type describeDatasetResp struct {
	DatasetDescription datasetDescription `json:"DatasetDescription"`
}

func (h *Handler) handleDescribeDataset(
	_ context.Context, req *describeDatasetReq,
) (*describeDatasetResp, error) {
	if req.DatasetArn == "" {
		return nil, fmt.Errorf("%w: DatasetArn is required", ErrValidation)
	}

	ds, err := h.Backend.DescribeDataset(req.DatasetArn)
	if err != nil {
		return nil, err
	}

	return &describeDatasetResp{
		DatasetDescription: datasetDescription{
			DatasetArn:           ds.DatasetARN,
			ProjectArn:           ds.ProjectARN,
			DatasetType:          ds.DatasetType,
			Status:               ds.Status,
			StatusMessage:        ds.StatusMessage,
			CreationTimestamp:    ds.CreationTimestamp.Format("2006-01-02T15:04:05.000Z"),
			LastUpdatedTimestamp: ds.LastUpdatedTimestamp.Format("2006-01-02T15:04:05.000Z"),
		},
	}, nil
}

type listDatasetEntriesReq struct {
	DatasetArn string `json:"DatasetArn"`
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listDatasetEntriesResp struct {
	NextToken      string   `json:"NextToken,omitempty"`
	DatasetEntries []string `json:"DatasetEntries"`
}

func (h *Handler) handleListDatasetEntries(
	_ context.Context, req *listDatasetEntriesReq,
) (*listDatasetEntriesResp, error) {
	if req.DatasetArn == "" {
		return nil, fmt.Errorf("%w: DatasetArn is required", ErrValidation)
	}

	entries, nextToken, err := h.Backend.ListDatasetEntries(req.DatasetArn, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	if entries == nil {
		entries = []string{}
	}

	return &listDatasetEntriesResp{
		DatasetEntries: entries,
		NextToken:      nextToken,
	}, nil
}

type listDatasetLabelsReq struct {
	DatasetArn string `json:"DatasetArn"`
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type datasetLabelEntry struct {
	LabelName  string `json:"LabelName"`
	EntryCount int64  `json:"EntryCount"`
}

type listDatasetLabelsResp struct {
	NextToken         string              `json:"NextToken,omitempty"`
	DatasetLabelStats []datasetLabelEntry `json:"DatasetLabelStats"`
}

func (h *Handler) handleListDatasetLabels(
	_ context.Context, req *listDatasetLabelsReq,
) (*listDatasetLabelsResp, error) {
	if req.DatasetArn == "" {
		return nil, fmt.Errorf("%w: DatasetArn is required", ErrValidation)
	}

	labels, nextToken, err := h.Backend.ListDatasetLabels(req.DatasetArn, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	entries := make([]datasetLabelEntry, 0, len(labels))
	for _, l := range labels {
		entries = append(entries, datasetLabelEntry{
			LabelName:  l.LabelName,
			EntryCount: l.EntryCount,
		})
	}

	return &listDatasetLabelsResp{
		DatasetLabelStats: entries,
		NextToken:         nextToken,
	}, nil
}

type updateDatasetEntriesReq struct {
	DatasetArn string `json:"DatasetArn"`
	Changes    []byte `json:"Changes"`
}

func (h *Handler) handleUpdateDatasetEntries(
	_ context.Context, req *updateDatasetEntriesReq,
) (*struct{}, error) {
	if req.DatasetArn == "" {
		return nil, fmt.Errorf("%w: DatasetArn is required", ErrValidation)
	}

	if err := h.Backend.UpdateDatasetEntries(req.DatasetArn, req.Changes); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type distributeDatasetEntriesReq struct {
	Datasets []struct {
		DatasetArn string `json:"DatasetArn"`
	} `json:"Datasets"`
}

func (h *Handler) handleDistributeDatasetEntries(
	_ context.Context, req *distributeDatasetEntriesReq,
) (*struct{}, error) {
	datasets := make([]DatasetDistribution, 0, len(req.Datasets))
	for _, d := range req.Datasets {
		datasets = append(datasets, DatasetDistribution{DatasetARN: d.DatasetArn})
	}

	if err := h.Backend.DistributeDatasetEntries(datasets); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// =============================================================================
// Users
// =============================================================================

type createUserReq struct {
	CollectionId string `json:"CollectionId"` //nolint:revive,staticcheck // existing issue.
	UserId       string `json:"UserId"`       //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCreateUser(_ context.Context, req *createUserReq) (*struct{}, error) {
	if req.CollectionId == "" {
		return nil, fmt.Errorf("%w: CollectionId is required", ErrValidation)
	}

	if req.UserId == "" {
		return nil, fmt.Errorf("%w: UserId is required", ErrValidation)
	}

	if err := h.Backend.CreateUser(req.CollectionId, req.UserId); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type deleteUserReq struct {
	CollectionId string `json:"CollectionId"` //nolint:revive,staticcheck // existing issue.
	UserId       string `json:"UserId"`       //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteUser(_ context.Context, req *deleteUserReq) (*struct{}, error) {
	if req.CollectionId == "" {
		return nil, fmt.Errorf("%w: CollectionId is required", ErrValidation)
	}

	if req.UserId == "" {
		return nil, fmt.Errorf("%w: UserId is required", ErrValidation)
	}

	if err := h.Backend.DeleteUser(req.CollectionId, req.UserId); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type listUsersReq struct {
	CollectionId string `json:"CollectionId"` //nolint:revive,staticcheck // existing issue.
	NextToken    string `json:"NextToken"`
	MaxResults   int32  `json:"MaxResults"`
}

type userEntry struct {
	UserId     string `json:"UserId"` //nolint:revive,staticcheck // existing issue.
	UserStatus string `json:"UserStatus"`
}

type listUsersResp struct {
	NextToken string      `json:"NextToken,omitempty"`
	Users     []userEntry `json:"Users"`
}

func (h *Handler) handleListUsers(_ context.Context, req *listUsersReq) (*listUsersResp, error) {
	if req.CollectionId == "" {
		return nil, fmt.Errorf("%w: CollectionId is required", ErrValidation)
	}

	users, nextToken, err := h.Backend.ListUsers(req.CollectionId, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	entries := make([]userEntry, 0, len(users))
	for _, u := range users {
		entries = append(entries, userEntry{
			UserId:     u.UserID,
			UserStatus: u.UserStatus,
		})
	}

	return &listUsersResp{
		Users:     entries,
		NextToken: nextToken,
	}, nil
}

type associateFacesReq struct {
	CollectionId string   `json:"CollectionId"` //nolint:revive,staticcheck // existing issue.
	UserId       string   `json:"UserId"`       //nolint:revive,staticcheck // existing issue.
	FaceIds      []string `json:"FaceIds"`      //nolint:revive // existing issue.
}

type associatedFaceEntry struct {
	FaceId string `json:"FaceId"` //nolint:revive,staticcheck // existing issue.
}

type unsuccessfulFaceAssociationEntry struct {
	FaceId  string   `json:"FaceId"` //nolint:revive,staticcheck // existing issue.
	Reasons []string `json:"Reasons"`
}

type associateFacesResp struct {
	AssociatedFaces              []associatedFaceEntry              `json:"AssociatedFaces"`
	UnsuccessfulFaceAssociations []unsuccessfulFaceAssociationEntry `json:"UnsuccessfulFaceAssociations"`
}

func (h *Handler) handleAssociateFaces( //nolint:dupl // existing issue.
	_ context.Context,
	req *associateFacesReq,
) (*associateFacesResp, error) {
	if req.CollectionId == "" {
		return nil, fmt.Errorf("%w: CollectionId is required", ErrValidation)
	}

	if req.UserId == "" {
		return nil, fmt.Errorf("%w: UserId is required", ErrValidation)
	}

	associated, unsuccessful, err := h.Backend.AssociateFaces(req.CollectionId, req.UserId, req.FaceIds)
	if err != nil {
		return nil, err
	}

	assocEntries := make([]associatedFaceEntry, 0, len(associated))
	for _, a := range associated {
		assocEntries = append(assocEntries, associatedFaceEntry{FaceId: a.FaceID})
	}

	unsuccessfulEntries := make([]unsuccessfulFaceAssociationEntry, 0, len(unsuccessful))
	for _, u := range unsuccessful {
		unsuccessfulEntries = append(unsuccessfulEntries, unsuccessfulFaceAssociationEntry{
			FaceId:  u.FaceID,
			Reasons: u.Reasons,
		})
	}

	return &associateFacesResp{
		AssociatedFaces:              assocEntries,
		UnsuccessfulFaceAssociations: unsuccessfulEntries,
	}, nil
}

type disassociateFacesReq struct {
	CollectionId string   `json:"CollectionId"` //nolint:revive,staticcheck // existing issue.
	UserId       string   `json:"UserId"`       //nolint:revive,staticcheck // existing issue.
	FaceIds      []string `json:"FaceIds"`      //nolint:revive // existing issue.
}

type disassociatedFaceEntry struct {
	FaceId string `json:"FaceId"` //nolint:revive,staticcheck // existing issue.
}

type unsuccessfulFaceDisassociationEntry struct {
	FaceId  string   `json:"FaceId"` //nolint:revive,staticcheck // existing issue.
	Reasons []string `json:"Reasons"`
}

type disassociateFacesResp struct {
	DisassociatedFaces              []disassociatedFaceEntry              `json:"DisassociatedFaces"`
	UnsuccessfulFaceDisassociations []unsuccessfulFaceDisassociationEntry `json:"UnsuccessfulFaceDisassociations"`
}

func (h *Handler) handleDisassociateFaces( //nolint:dupl // existing issue.
	_ context.Context, req *disassociateFacesReq,
) (*disassociateFacesResp, error) {
	if req.CollectionId == "" {
		return nil, fmt.Errorf("%w: CollectionId is required", ErrValidation)
	}

	if req.UserId == "" {
		return nil, fmt.Errorf("%w: UserId is required", ErrValidation)
	}

	disassociated, unsuccessful, err := h.Backend.DisassociateFaces(
		req.CollectionId, req.UserId, req.FaceIds,
	)
	if err != nil {
		return nil, err
	}

	disassocEntries := make([]disassociatedFaceEntry, 0, len(disassociated))
	for _, d := range disassociated {
		disassocEntries = append(disassocEntries, disassociatedFaceEntry{FaceId: d.FaceID})
	}

	unsuccessfulEntries := make([]unsuccessfulFaceDisassociationEntry, 0, len(unsuccessful))
	for _, u := range unsuccessful {
		unsuccessfulEntries = append(unsuccessfulEntries, unsuccessfulFaceDisassociationEntry{
			FaceId:  u.FaceID,
			Reasons: u.Reasons,
		})
	}

	return &disassociateFacesResp{
		DisassociatedFaces:              disassocEntries,
		UnsuccessfulFaceDisassociations: unsuccessfulEntries,
	}, nil
}

type searchUsersReq struct {
	CollectionId string `json:"CollectionId"` //nolint:revive,staticcheck // existing issue.
	UserId       string `json:"UserId"`       //nolint:revive,staticcheck // existing issue.
	MaxUsers     int32  `json:"MaxUsers"`
}

type userMatchEntry struct {
	User       userEntry `json:"User"`
	Similarity float64   `json:"Similarity"`
}

type searchUsersResp struct {
	FaceModelVersion string           `json:"FaceModelVersion"`
	SearchedUser     userEntry        `json:"SearchedUser"`
	UserMatches      []userMatchEntry `json:"UserMatches"`
}

func (h *Handler) handleSearchUsers(_ context.Context, req *searchUsersReq) (*searchUsersResp, error) {
	if req.CollectionId == "" {
		return nil, fmt.Errorf("%w: CollectionId is required", ErrValidation)
	}

	if req.UserId == "" {
		return nil, fmt.Errorf("%w: UserId is required", ErrValidation)
	}

	matches, err := h.Backend.SearchUsers(req.CollectionId, req.UserId, req.MaxUsers)
	if err != nil {
		return nil, err
	}

	entries := make([]userMatchEntry, 0, len(matches))
	for _, m := range matches {
		entries = append(entries, userMatchEntry{
			Similarity: m.Similarity,
			User: userEntry{
				UserId:     m.User.UserID,
				UserStatus: m.User.UserStatus,
			},
		})
	}

	return &searchUsersResp{
		FaceModelVersion: faceModelVersion,
		SearchedUser:     userEntry{UserId: req.UserId, UserStatus: "ACTIVE"},
		UserMatches:      entries,
	}, nil
}

type searchUsersByImageReq struct {
	CollectionId string `json:"CollectionId"` //nolint:revive,staticcheck // existing issue.
	MaxUsers     int32  `json:"MaxUsers"`
}

type searchUsersByImageResp struct {
	FaceModelVersion string           `json:"FaceModelVersion"`
	UserMatches      []userMatchEntry `json:"UserMatches"`
}

func (h *Handler) handleSearchUsersByImage(
	_ context.Context, req *searchUsersByImageReq,
) (*searchUsersByImageResp, error) {
	if req.CollectionId == "" {
		return nil, fmt.Errorf("%w: CollectionId is required", ErrValidation)
	}

	matches, err := h.Backend.SearchUsersByImage(req.CollectionId, req.MaxUsers)
	if err != nil {
		return nil, err
	}

	entries := make([]userMatchEntry, 0, len(matches))
	for _, m := range matches {
		entries = append(entries, userMatchEntry{
			Similarity: m.Similarity,
			User: userEntry{
				UserId:     m.User.UserID,
				UserStatus: m.User.UserStatus,
			},
		})
	}

	return &searchUsersByImageResp{
		FaceModelVersion: faceModelVersion,
		UserMatches:      entries,
	}, nil
}

// =============================================================================
// Face Liveness
// =============================================================================

type createFaceLivenessSessionReq struct {
	ClientRequestToken string `json:"ClientRequestToken"`
}

type createFaceLivenessSessionResp struct {
	SessionId string `json:"SessionId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCreateFaceLivenessSession(
	_ context.Context, _ *createFaceLivenessSessionReq,
) (*createFaceLivenessSessionResp, error) {
	sessionID, err := h.Backend.CreateFaceLivenessSession()
	if err != nil {
		return nil, err
	}

	return &createFaceLivenessSessionResp{SessionId: sessionID}, nil
}

type getFaceLivenessSessionResultsReq struct {
	SessionId string `json:"SessionId"` //nolint:revive,staticcheck // existing issue.
}

type getFaceLivenessSessionResultsResp struct {
	SessionId  string  `json:"SessionId"` //nolint:revive,staticcheck // existing issue.
	Status     string  `json:"Status"`
	Confidence float32 `json:"Confidence"`
}

func (h *Handler) handleGetFaceLivenessSessionResults(
	_ context.Context, req *getFaceLivenessSessionResultsReq,
) (*getFaceLivenessSessionResultsResp, error) {
	if req.SessionId == "" {
		return nil, fmt.Errorf("%w: SessionId is required", ErrValidation)
	}

	result, err := h.Backend.GetFaceLivenessSessionResults(req.SessionId)
	if err != nil {
		return nil, err
	}

	return &getFaceLivenessSessionResultsResp{
		SessionId:  result.SessionID,
		Status:     result.Status,
		Confidence: result.Confidence,
	}, nil
}

// =============================================================================
// Image Analysis (stateless mock results)
// =============================================================================

type imageRef struct {
	S3Object *struct {
		Bucket  string `json:"Bucket"`
		Name    string `json:"Name"`
		Version string `json:"Version"`
	} `json:"S3Object"`
	Bytes []byte `json:"Bytes"`
}

type compareFacesReq struct {
	SourceImage         imageRef `json:"SourceImage"`
	TargetImage         imageRef `json:"TargetImage"`
	SimilarityThreshold float64  `json:"SimilarityThreshold"`
}

type faceMatchResult struct {
	Similarity float64 `json:"Similarity"`
	Face       struct {
		BoundingBox struct {
			Height float32 `json:"Height"`
			Left   float32 `json:"Left"`
			Top    float32 `json:"Top"`
			Width  float32 `json:"Width"`
		} `json:"BoundingBox"`
		Confidence float64 `json:"Confidence"`
	} `json:"Face"`
}

type compareFacesResp struct {
	FaceMatches     []faceMatchResult `json:"FaceMatches"`
	UnmatchedFaces  []struct{}        `json:"UnmatchedFaces"`
	SourceImageFace struct {
		BoundingBox struct {
			Height float32 `json:"Height"`
			Left   float32 `json:"Left"`
			Top    float32 `json:"Top"`
			Width  float32 `json:"Width"`
		} `json:"BoundingBox"`
		Confidence float64 `json:"Confidence"`
	} `json:"SourceImageFace"`
}

func (h *Handler) handleCompareFaces(_ context.Context, _ *compareFacesReq) (*compareFacesResp, error) {
	resp := &compareFacesResp{}
	resp.SourceImageFace.Confidence = 99.9
	resp.SourceImageFace.BoundingBox.Height = 0.5
	resp.SourceImageFace.BoundingBox.Width = 0.3
	resp.FaceMatches = []faceMatchResult{}
	resp.UnmatchedFaces = []struct{}{}

	return resp, nil
}

type detectFacesReq struct {
	Image      imageRef `json:"Image"`
	Attributes []string `json:"Attributes"`
}

type faceDetailEntry struct {
	BoundingBox struct {
		Height float32 `json:"Height"`
		Left   float32 `json:"Left"`
		Top    float32 `json:"Top"`
		Width  float32 `json:"Width"`
	} `json:"BoundingBox"`
	Confidence float64 `json:"Confidence"`
}

type detectFacesResp struct { //nolint:govet // existing issue.
	FaceDetails           []faceDetailEntry `json:"FaceDetails"`
	OrientationCorrection string            `json:"OrientationCorrection"`
}

func (h *Handler) handleDetectFaces(_ context.Context, _ *detectFacesReq) (*detectFacesResp, error) {
	return &detectFacesResp{
		FaceDetails:           []faceDetailEntry{},
		OrientationCorrection: "ROTATE_0", //nolint:goconst // existing issue.
	}, nil
}

type detectLabelsReq struct {
	Image         imageRef `json:"Image"`
	MaxLabels     int32    `json:"MaxLabels"`
	MinConfidence float64  `json:"MinConfidence"`
}

type labelEntry struct {
	Name       string  `json:"Name"`
	Confidence float64 `json:"Confidence"`
}

type detectLabelsResp struct { //nolint:govet // existing issue.
	Labels                []labelEntry `json:"Labels"`
	OrientationCorrection string       `json:"OrientationCorrection"`
}

func (h *Handler) handleDetectLabels(_ context.Context, req *detectLabelsReq) (*detectLabelsResp, error) {
	minConf := req.MinConfidence
	if minConf <= 0 {
		minConf = 50.0
	}
	labels := plausibleLabels(minConf, req.MaxLabels)

	return &detectLabelsResp{
		Labels:                labels,
		OrientationCorrection: "ROTATE_0",
	}, nil
}

// plausibleLabels returns a set of generic scene labels above the confidence threshold.
func plausibleLabels(minConfidence float64, maxLabels int32) []labelEntry {
	all := []labelEntry{
		{Name: "Person", Confidence: 95.5},
		{Name: "Human", Confidence: 94.8},
		{Name: "Outdoor", Confidence: 87.3},
		{Name: "Nature", Confidence: 73.2},
		{Name: "Sky", Confidence: 68.9},
		{Name: "Vegetation", Confidence: 62.1},
		{Name: "Animal", Confidence: 55.4},
	}
	out := make([]labelEntry, 0, len(all))
	for _, l := range all {
		if l.Confidence >= minConfidence {
			out = append(out, l)
		}
		if maxLabels > 0 && int32(len(out)) >= maxLabels {
			break
		}
	}

	return out
}

type detectTextReq struct { //nolint:govet // existing issue.
	Image   imageRef  `json:"Image"`
	Filters *struct{} `json:"Filters"`
}

type textDetectionEntry struct { //nolint:govet // existing issue.
	DetectedText string  `json:"DetectedText"`
	Confidence   float64 `json:"Confidence"`
	Type         string  `json:"Type"`
	Id           int32   `json:"Id"` //nolint:revive,staticcheck // existing issue.
}

type detectTextResp struct { //nolint:govet // existing issue.
	TextDetections   []textDetectionEntry `json:"TextDetections"`
	TextModelVersion string               `json:"TextModelVersion"`
}

func (h *Handler) handleDetectText(_ context.Context, req *detectTextReq) (*detectTextResp, error) {
	detections := plausibleTextDetections(req)

	return &detectTextResp{
		TextDetections:   detections,
		TextModelVersion: "3.1",
	}, nil
}

// plausibleTextDetections returns minimal text detection results derived from the image reference.
func plausibleTextDetections(req *detectTextReq) []textDetectionEntry {
	// Derive a plausible text value from the image S3 key when available.
	label := "SAMPLE TEXT"
	if req.Image.S3Object != nil && req.Image.S3Object.Name != "" {
		label = req.Image.S3Object.Name
	}

	return []textDetectionEntry{
		{Id: 0, DetectedText: label, Type: "LINE", Confidence: 97.2},
		{Id: 1, DetectedText: label, Type: "WORD", Confidence: 97.2},
	}
}

type detectCustomLabelsReq struct {
	ProjectVersionArn string   `json:"ProjectVersionArn"`
	Image             imageRef `json:"Image"`
	MaxResults        int32    `json:"MaxResults"`
	MinConfidence     float32  `json:"MinConfidence"`
}

type customLabelEntry struct {
	Name       string  `json:"Name"`
	Confidence float32 `json:"Confidence"`
}

type detectCustomLabelsResp struct {
	CustomLabels []customLabelEntry `json:"CustomLabels"`
}

func (h *Handler) handleDetectCustomLabels(
	_ context.Context, _ *detectCustomLabelsReq,
) (*detectCustomLabelsResp, error) {
	return &detectCustomLabelsResp{CustomLabels: []customLabelEntry{}}, nil
}

type detectModerationLabelsReq struct {
	Image         imageRef `json:"Image"`
	MinConfidence float32  `json:"MinConfidence"`
}

type moderationLabelEntry struct {
	Name       string  `json:"Name"`
	ParentName string  `json:"ParentName"`
	Confidence float32 `json:"Confidence"`
}

type detectModerationLabelsResp struct { //nolint:govet // existing issue.
	ModerationLabels       []moderationLabelEntry `json:"ModerationLabels"`
	ModerationModelVersion string                 `json:"ModerationModelVersion"`
}

func (h *Handler) handleDetectModerationLabels(
	_ context.Context, req *detectModerationLabelsReq,
) (*detectModerationLabelsResp, error) {
	// Default: content is clean. Only return labels if MinConfidence is very low,
	// which indicates the caller wants to see all possible labels.
	labels := []moderationLabelEntry{}
	if req.MinConfidence > 0 && req.MinConfidence <= 10.0 {
		labels = []moderationLabelEntry{
			{Name: "Suggestive", ParentName: "", Confidence: 7.5},
		}
	}

	return &detectModerationLabelsResp{
		ModerationLabels:       labels,
		ModerationModelVersion: "4.0",
	}, nil
}

type detectProtectiveEquipmentReq struct { //nolint:govet // existing issue.
	Image                   imageRef  `json:"Image"`
	SummarizationAttributes *struct { //nolint:govet // existing issue.
		MinConfidence          float32  `json:"MinConfidence"`
		RequiredEquipmentTypes []string `json:"RequiredEquipmentTypes"`
	} `json:"SummarizationAttributes"`
}

type protectiveEquipmentPersonEntry struct {
	Id         int32   `json:"Id"` //nolint:revive,staticcheck // existing issue.
	Confidence float32 `json:"Confidence"`
}

type detectProtectiveEquipmentResp struct { //nolint:govet // existing issue.
	Persons                         []protectiveEquipmentPersonEntry `json:"Persons"`
	ProtectiveEquipmentModelVersion string                           `json:"ProtectiveEquipmentModelVersion"`
}

func (h *Handler) handleDetectProtectiveEquipment(
	_ context.Context, _ *detectProtectiveEquipmentReq,
) (*detectProtectiveEquipmentResp, error) {
	return &detectProtectiveEquipmentResp{
		Persons:                         []protectiveEquipmentPersonEntry{},
		ProtectiveEquipmentModelVersion: "1.0",
	}, nil
}

type recognizeCelebritiesReq struct {
	Image imageRef `json:"Image"`
}

type celebrityEntry struct { //nolint:govet // existing issue.
	Id              string   `json:"Id"` //nolint:revive,staticcheck // existing issue.
	Name            string   `json:"Name"`
	MatchConfidence float32  `json:"MatchConfidence"`
	Urls            []string `json:"Urls"`
}

type recognizeCelebritiesResp struct { //nolint:govet // existing issue.
	CelebrityFaces        []celebrityEntry `json:"CelebrityFaces"`
	UnrecognizedFaces     []struct{}       `json:"UnrecognizedFaces"`
	OrientationCorrection string           `json:"OrientationCorrection"`
}

func (h *Handler) handleRecognizeCelebrities(
	_ context.Context, _ *recognizeCelebritiesReq,
) (*recognizeCelebritiesResp, error) {
	return &recognizeCelebritiesResp{
		CelebrityFaces:        []celebrityEntry{},
		UnrecognizedFaces:     []struct{}{},
		OrientationCorrection: "ROTATE_0",
	}, nil
}

type getCelebrityInfoReq struct {
	Id string `json:"Id"` //nolint:revive,staticcheck // existing issue.
}

type knownGender struct {
	Type string `json:"Type"`
}

type getCelebrityInfoResp struct {
	KnownGender *knownGender `json:"KnownGender"`
	Name        string       `json:"Name"`
	Urls        []string     `json:"Urls"`
}

func (h *Handler) handleGetCelebrityInfo(
	_ context.Context, req *getCelebrityInfoReq,
) (*getCelebrityInfoResp, error) {
	if req.Id == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrValidation)
	}

	// Mock celebrity info — return a plausible response for any ID.
	return &getCelebrityInfoResp{
		Name:        "Celebrity " + req.Id,
		Urls:        []string{},
		KnownGender: &knownGender{Type: "Unknown"},
	}, nil
}

// =============================================================================
// Async Video Jobs — Start* handlers
// =============================================================================

type videoRef struct {
	S3Object *struct {
		Bucket  string `json:"Bucket"`
		Name    string `json:"Name"`
		Version string `json:"Version"`
	} `json:"S3Object"`
}

type startJobResp struct {
	JobId string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
}

type startCelebrityRecognitionReq struct {
	Video              videoRef `json:"Video"`
	ClientRequestToken string   `json:"ClientRequestToken"`
	JobTag             string   `json:"JobTag"`
}

func (h *Handler) handleStartCelebrityRecognition(
	_ context.Context, _ *startCelebrityRecognitionReq,
) (*startJobResp, error) {
	jobID, err := h.Backend.StartAsyncJob("celebrity_recognition", "")
	if err != nil {
		return nil, err
	}

	return &startJobResp{JobId: jobID}, nil
}

type startContentModerationReq struct {
	Video              videoRef `json:"Video"`
	ClientRequestToken string   `json:"ClientRequestToken"`
	JobTag             string   `json:"JobTag"`
	MinConfidence      float32  `json:"MinConfidence"`
}

func (h *Handler) handleStartContentModeration(
	_ context.Context, _ *startContentModerationReq,
) (*startJobResp, error) {
	jobID, err := h.Backend.StartAsyncJob("content_moderation", "")
	if err != nil {
		return nil, err
	}

	return &startJobResp{JobId: jobID}, nil
}

type startFaceDetectionReq struct {
	Video              videoRef `json:"Video"`
	ClientRequestToken string   `json:"ClientRequestToken"`
	JobTag             string   `json:"JobTag"`
	FaceAttributes     string   `json:"FaceAttributes"`
}

func (h *Handler) handleStartFaceDetection(
	_ context.Context, _ *startFaceDetectionReq,
) (*startJobResp, error) {
	jobID, err := h.Backend.StartAsyncJob("face_detection", "")
	if err != nil {
		return nil, err
	}

	return &startJobResp{JobId: jobID}, nil
}

type startFaceSearchReq struct {
	Video              videoRef `json:"Video"`
	CollectionId       string   `json:"CollectionId"` //nolint:revive,staticcheck // existing issue.
	ClientRequestToken string   `json:"ClientRequestToken"`
	JobTag             string   `json:"JobTag"`
	FaceMatchThreshold float32  `json:"FaceMatchThreshold"`
}

func (h *Handler) handleStartFaceSearch(
	_ context.Context, req *startFaceSearchReq,
) (*startJobResp, error) {
	jobID, err := h.Backend.StartAsyncJob("face_search", req.CollectionId)
	if err != nil {
		return nil, err
	}

	return &startJobResp{JobId: jobID}, nil
}

type startLabelDetectionReq struct {
	Video              videoRef `json:"Video"`
	ClientRequestToken string   `json:"ClientRequestToken"`
	JobTag             string   `json:"JobTag"`
	MinConfidence      float32  `json:"MinConfidence"`
}

func (h *Handler) handleStartLabelDetection(
	_ context.Context, _ *startLabelDetectionReq,
) (*startJobResp, error) {
	jobID, err := h.Backend.StartAsyncJob("label_detection", "")
	if err != nil {
		return nil, err
	}

	return &startJobResp{JobId: jobID}, nil
}

type startPersonTrackingReq struct {
	Video              videoRef `json:"Video"`
	ClientRequestToken string   `json:"ClientRequestToken"`
	JobTag             string   `json:"JobTag"`
}

func (h *Handler) handleStartPersonTracking(
	_ context.Context, _ *startPersonTrackingReq,
) (*startJobResp, error) {
	jobID, err := h.Backend.StartAsyncJob("person_tracking", "")
	if err != nil {
		return nil, err
	}

	return &startJobResp{JobId: jobID}, nil
}

type startSegmentDetectionReq struct {
	Video              videoRef `json:"Video"`
	ClientRequestToken string   `json:"ClientRequestToken"`
	JobTag             string   `json:"JobTag"`
	SegmentTypes       []string `json:"SegmentTypes"`
}

func (h *Handler) handleStartSegmentDetection(
	_ context.Context, _ *startSegmentDetectionReq,
) (*startJobResp, error) {
	jobID, err := h.Backend.StartAsyncJob("segment_detection", "")
	if err != nil {
		return nil, err
	}

	return &startJobResp{JobId: jobID}, nil
}

type startTextDetectionReq struct {
	Video              videoRef `json:"Video"`
	ClientRequestToken string   `json:"ClientRequestToken"`
	JobTag             string   `json:"JobTag"`
}

func (h *Handler) handleStartTextDetection(
	_ context.Context, _ *startTextDetectionReq,
) (*startJobResp, error) {
	jobID, err := h.Backend.StartAsyncJob("text_detection", "")
	if err != nil {
		return nil, err
	}

	return &startJobResp{JobId: jobID}, nil
}

// =============================================================================
// Async Video Jobs — Get* handlers
// =============================================================================

type getJobReq struct { //nolint:govet // existing issue.
	JobId      string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
	MaxResults int32  `json:"MaxResults"`
	NextToken  string `json:"NextToken"`
}

type videoMetadata struct { //nolint:govet // existing issue.
	Codec          string  `json:"Codec"`
	DurationMillis int64   `json:"DurationMillis"`
	Format         string  `json:"Format"`
	FrameRate      float32 `json:"FrameRate"`
}

type getJobBaseResp struct { //nolint:govet // existing issue.
	JobId         string         `json:"JobId"` //nolint:revive,staticcheck // existing issue.
	JobStatus     string         `json:"JobStatus"`
	NextToken     string         `json:"NextToken,omitempty"`
	StatusMessage string         `json:"StatusMessage,omitempty"`
	VideoMetadata *videoMetadata `json:"VideoMetadata"`
}

func (h *Handler) getJobBase(jobID string) (*getJobBaseResp, error) {
	if jobID == "" {
		return nil, fmt.Errorf("%w: JobId is required", ErrValidation)
	}

	job, err := h.Backend.GetAsyncJob(jobID)
	if err != nil {
		return nil, err
	}

	return &getJobBaseResp{
		JobId:     job.JobID,
		JobStatus: job.JobStatus,
		VideoMetadata: &videoMetadata{
			Codec:          "H264",
			DurationMillis: 0,
			Format:         "QuickTime / MOV",
			FrameRate:      30, //nolint:mnd // existing issue.
		},
	}, nil
}

type getCelebrityRecognitionResp struct {
	getJobBaseResp
	Celebrities []struct{} `json:"Celebrities"`
}

func (h *Handler) handleGetCelebrityRecognition(
	_ context.Context, req *getJobReq,
) (*getCelebrityRecognitionResp, error) {
	base, err := h.getJobBase(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getCelebrityRecognitionResp{
		getJobBaseResp: *base,
		Celebrities:    []struct{}{},
	}, nil
}

type getContentModerationResp struct {
	getJobBaseResp
	ModerationLabels []struct{} `json:"ModerationLabels"`
}

func (h *Handler) handleGetContentModeration(
	_ context.Context, req *getJobReq,
) (*getContentModerationResp, error) {
	base, err := h.getJobBase(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getContentModerationResp{
		getJobBaseResp:   *base,
		ModerationLabels: []struct{}{},
	}, nil
}

type getFaceDetectionResp struct {
	getJobBaseResp
	Faces []struct{} `json:"Faces"`
}

func (h *Handler) handleGetFaceDetection(
	_ context.Context, req *getJobReq,
) (*getFaceDetectionResp, error) {
	base, err := h.getJobBase(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getFaceDetectionResp{
		getJobBaseResp: *base,
		Faces:          []struct{}{},
	}, nil
}

type getFaceSearchResp struct {
	getJobBaseResp
	Persons []struct{} `json:"Persons"`
}

func (h *Handler) handleGetFaceSearch(
	_ context.Context, req *getJobReq,
) (*getFaceSearchResp, error) {
	base, err := h.getJobBase(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getFaceSearchResp{
		getJobBaseResp: *base,
		Persons:        []struct{}{},
	}, nil
}

type getLabelDetectionResp struct {
	getJobBaseResp
	Labels []struct{} `json:"Labels"`
}

func (h *Handler) handleGetLabelDetection(
	_ context.Context, req *getJobReq,
) (*getLabelDetectionResp, error) {
	base, err := h.getJobBase(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getLabelDetectionResp{
		getJobBaseResp: *base,
		Labels:         []struct{}{},
	}, nil
}

type getPersonTrackingResp struct {
	getJobBaseResp
	Persons []struct{} `json:"Persons"`
}

func (h *Handler) handleGetPersonTracking(
	_ context.Context, req *getJobReq,
) (*getPersonTrackingResp, error) {
	base, err := h.getJobBase(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getPersonTrackingResp{
		getJobBaseResp: *base,
		Persons:        []struct{}{},
	}, nil
}

type getSegmentDetectionResp struct {
	getJobBaseResp
	Segments             []struct{} `json:"Segments"`
	SelectedSegmentTypes []struct{} `json:"SelectedSegmentTypes"`
}

func (h *Handler) handleGetSegmentDetection(
	_ context.Context, req *getJobReq,
) (*getSegmentDetectionResp, error) {
	base, err := h.getJobBase(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getSegmentDetectionResp{
		getJobBaseResp:       *base,
		Segments:             []struct{}{},
		SelectedSegmentTypes: []struct{}{},
	}, nil
}

type getTextDetectionResp struct {
	getJobBaseResp
	TextDetections []struct{} `json:"TextDetections"`
}

func (h *Handler) handleGetTextDetection(
	_ context.Context, req *getJobReq,
) (*getTextDetectionResp, error) {
	base, err := h.getJobBase(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getTextDetectionResp{
		getJobBaseResp: *base,
		TextDetections: []struct{}{},
	}, nil
}

// =============================================================================
// MediaAnalysis Jobs
// =============================================================================

type startMediaAnalysisJobReq struct {
	JobName            string `json:"JobName"`
	ClientRequestToken string `json:"ClientRequestToken"`
}

type startMediaAnalysisJobResp struct {
	JobId string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleStartMediaAnalysisJob(
	_ context.Context, req *startMediaAnalysisJobReq,
) (*startMediaAnalysisJobResp, error) {
	jobName := req.JobName
	if jobName == "" {
		jobName = "media-analysis-job"
	}

	jobID, err := h.Backend.StartMediaAnalysisJob(jobName)
	if err != nil {
		return nil, err
	}

	return &startMediaAnalysisJobResp{JobId: jobID}, nil
}

type getMediaAnalysisJobReq struct {
	JobId string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
}

type mediaAnalysisJobDescription struct {
	JobId             string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
	JobName           string `json:"JobName"`
	Status            string `json:"Status"`
	CreationTimestamp string `json:"CreationTimestamp"`
}

type getMediaAnalysisJobResp struct {
	JobId             string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
	JobName           string `json:"JobName"`
	Status            string `json:"Status"`
	CreationTimestamp string `json:"CreationTimestamp"`
}

func (h *Handler) handleGetMediaAnalysisJob(
	_ context.Context, req *getMediaAnalysisJobReq,
) (*getMediaAnalysisJobResp, error) {
	if req.JobId == "" {
		return nil, fmt.Errorf("%w: JobId is required", ErrValidation)
	}

	job, err := h.Backend.GetMediaAnalysisJob(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getMediaAnalysisJobResp{
		JobId:             job.JobID,
		JobName:           job.JobName,
		Status:            job.Status,
		CreationTimestamp: job.CreationTimestamp.Format("2006-01-02T15:04:05.000Z"),
	}, nil
}

type listMediaAnalysisJobsReq struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listMediaAnalysisJobsResp struct {
	NextToken         string                        `json:"NextToken,omitempty"`
	MediaAnalysisJobs []mediaAnalysisJobDescription `json:"MediaAnalysisJobs"`
}

func (h *Handler) handleListMediaAnalysisJobs(
	_ context.Context, req *listMediaAnalysisJobsReq,
) (*listMediaAnalysisJobsResp, error) {
	jobs, nextToken, err := h.Backend.ListMediaAnalysisJobs(req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	descriptions := make([]mediaAnalysisJobDescription, 0, len(jobs))
	for _, j := range jobs {
		descriptions = append(descriptions, mediaAnalysisJobDescription{
			JobId:             j.JobID,
			JobName:           j.JobName,
			Status:            j.Status,
			CreationTimestamp: j.CreationTimestamp.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	return &listMediaAnalysisJobsResp{
		MediaAnalysisJobs: descriptions,
		NextToken:         nextToken,
	}, nil
}

// httpStatusOK is used to reference net/http and avoid the unused import error.
var _ = http.StatusOK
