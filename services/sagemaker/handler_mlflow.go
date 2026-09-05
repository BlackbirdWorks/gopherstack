package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// MlflowTrackingServer handlers
// ---------------------------------------------------------------------------

// createMlflowTrackingServerInput is CreateMlflowTrackingServer's request
// shape (api_op_CreateMlflowTrackingServer.go:31-96, sagemaker@v1.263.2).
type createMlflowTrackingServerInput struct {
	AutomaticModelRegistration   *bool       `json:"AutomaticModelRegistration"`
	S3BucketOwnerVerification    *bool       `json:"S3BucketOwnerVerification"`
	TrackingServerName           string      `json:"TrackingServerName"`
	ArtifactStoreURI             string      `json:"ArtifactStoreUri"`
	RoleArn                      string      `json:"RoleArn"`
	MlflowVersion                string      `json:"MlflowVersion,omitempty"`
	S3BucketOwnerAccountID       string      `json:"S3BucketOwnerAccountId,omitempty"`
	TrackingServerSize           string      `json:"TrackingServerSize,omitempty"`
	WeeklyMaintenanceWindowStart string      `json:"WeeklyMaintenanceWindowStart,omitempty"`
	Tags                         []tagObject `json:"Tags"`
}

func (h *Handler) handleCreateMlflowTrackingServer(ctx context.Context, body []byte) ([]byte, error) {
	var req createMlflowTrackingServerInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	trackingServerSize := req.TrackingServerSize
	if trackingServerSize == "" {
		trackingServerSize = "Small" // documented default, api_op_CreateMlflowTrackingServer.go:79-87
	}

	automaticModelRegistration := false // documented default, api_op_CreateMlflowTrackingServer.go:55-58
	if req.AutomaticModelRegistration != nil {
		automaticModelRegistration = *req.AutomaticModelRegistration
	}

	s3BucketOwnerVerification := true // documented default, api_op_CreateMlflowTrackingServer.go:71-73
	if req.S3BucketOwnerVerification != nil {
		s3BucketOwnerVerification = *req.S3BucketOwnerVerification
	}

	result, err := h.Backend.CreateMlflowTrackingServer(ctx, CreateMlflowTrackingServerOptions{
		TrackingServerName:           req.TrackingServerName,
		ArtifactStoreURI:             req.ArtifactStoreURI,
		RoleArn:                      req.RoleArn,
		MlflowVersion:                req.MlflowVersion,
		S3BucketOwnerAccountID:       req.S3BucketOwnerAccountID,
		TrackingServerSize:           trackingServerSize,
		WeeklyMaintenanceWindowStart: req.WeeklyMaintenanceWindowStart,
		AutomaticModelRegistration:   automaticModelRegistration,
		S3BucketOwnerVerification:    s3BucketOwnerVerification,
		Tags:                         fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyTrackingServerArn: result.TrackingServerArn})
}

// describeMlflowTrackingServerInput is DescribeMlflowTrackingServer's request
// shape (api_op_DescribeMlflowTrackingServer.go:29-37).
type describeMlflowTrackingServerInput struct {
	TrackingServerName string `json:"TrackingServerName"`
}

// describeMlflowTrackingServerResponse is DescribeMlflowTrackingServer's
// response shape (api_op_DescribeMlflowTrackingServer.go:39-106).
// CreatedBy/LastModifiedBy (types.UserContext) and
// TrackingServerMaintenanceStatus are disclosed absent: this service models
// no caller-identity concept anywhere (grepped repo-wide, none found) and no
// maintenance subsystem exists to report a real status from, so fabricating
// either would be a guess rather than a fact.
type describeMlflowTrackingServerResponse struct {
	TrackingServerName           string  `json:"TrackingServerName"`
	TrackingServerArn            string  `json:"TrackingServerArn"`
	TrackingServerStatus         string  `json:"TrackingServerStatus"`
	IsActive                     string  `json:"IsActive"`
	RoleArn                      string  `json:"RoleArn,omitempty"`
	MlflowVersion                string  `json:"MlflowVersion,omitempty"`
	ArtifactStoreURI             string  `json:"ArtifactStoreUri,omitempty"`
	S3BucketOwnerAccountID       string  `json:"S3BucketOwnerAccountId,omitempty"`
	TrackingServerSize           string  `json:"TrackingServerSize,omitempty"`
	TrackingServerURL            string  `json:"TrackingServerUrl,omitempty"`
	WeeklyMaintenanceWindowStart string  `json:"WeeklyMaintenanceWindowStart,omitempty"`
	AutomaticModelRegistration   bool    `json:"AutomaticModelRegistration"`
	S3BucketOwnerVerification    bool    `json:"S3BucketOwnerVerification"`
	CreationTime                 float64 `json:"CreationTime"`
	LastModifiedTime             float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleDescribeMlflowTrackingServer(ctx context.Context, body []byte) ([]byte, error) {
	var req describeMlflowTrackingServerInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeMlflowTrackingServer(ctx, req.TrackingServerName)
	if err != nil {
		return nil, err
	}

	region := getRegion(ctx, h.Backend.Region())

	return json.Marshal(describeMlflowTrackingServerResponse{
		TrackingServerName:           result.TrackingServerName,
		TrackingServerArn:            result.TrackingServerArn,
		TrackingServerStatus:         result.TrackingServerStatus,
		IsActive:                     mlflowTrackingServerIsActive(result.TrackingServerStatus),
		RoleArn:                      result.RoleArn,
		MlflowVersion:                result.MlflowVersion,
		ArtifactStoreURI:             result.ArtifactStoreURI,
		S3BucketOwnerAccountID:       result.S3BucketOwnerAccountID,
		TrackingServerSize:           result.TrackingServerSize,
		TrackingServerURL:            mlflowTrackingServerURL(result.TrackingServerName, region),
		WeeklyMaintenanceWindowStart: result.WeeklyMaintenanceWindowStart,
		AutomaticModelRegistration:   result.AutomaticModelRegistration,
		S3BucketOwnerVerification:    result.S3BucketOwnerVerification,
		CreationTime:                 epochSeconds(result.CreationTime),
		LastModifiedTime:             epochSeconds(result.LastModifiedTime),
	})
}

// deleteMlflowTrackingServerInput is DeleteMlflowTrackingServer's request
// shape (api_op_DeleteMlflowTrackingServer.go:29-37).
type deleteMlflowTrackingServerInput struct {
	TrackingServerName string `json:"TrackingServerName"`
}

func (h *Handler) handleDeleteMlflowTrackingServer(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteMlflowTrackingServerInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	arnStr, err := h.Backend.DeleteMlflowTrackingServer(ctx, req.TrackingServerName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyTrackingServerArn: arnStr})
}

// startMlflowTrackingServerInput is StartMlflowTrackingServer's request shape
// (api_op_StartMlflowTrackingServer.go:27-35).
type startMlflowTrackingServerInput struct {
	TrackingServerName string `json:"TrackingServerName"`
}

func (h *Handler) handleStartMlflowTrackingServer(ctx context.Context, body []byte) ([]byte, error) {
	var req startMlflowTrackingServerInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	arnStr, err := h.Backend.StartMlflowTrackingServer(ctx, req.TrackingServerName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyTrackingServerArn: arnStr})
}

// stopMlflowTrackingServerInput is StopMlflowTrackingServer's request shape
// (api_op_StopMlflowTrackingServer.go:27-35).
type stopMlflowTrackingServerInput struct {
	TrackingServerName string `json:"TrackingServerName"`
}

func (h *Handler) handleStopMlflowTrackingServer(ctx context.Context, body []byte) ([]byte, error) {
	var req stopMlflowTrackingServerInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	arnStr, err := h.Backend.StopMlflowTrackingServer(ctx, req.TrackingServerName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyTrackingServerArn: arnStr})
}

// createPresignedMlflowTrackingServerURLInput is
// CreatePresignedMlflowTrackingServerUrl's request shape
// (api_op_CreatePresignedMlflowTrackingServerUrl.go:30-46).
// ExpiresInSeconds/SessionExpirationDurationInSeconds are modeled for wire
// visibility but disclosed no-op: this backend generates presigned URLs with
// no TTL enforcement mechanism anywhere in the service (grepped repo-wide),
// the same structural gap as hub.go's PresignedUrlAccessConfig.
type createPresignedMlflowTrackingServerURLInput struct {
	ExpiresInSeconds                   *int32 `json:"ExpiresInSeconds,omitempty"`
	SessionExpirationDurationInSeconds *int32 `json:"SessionExpirationDurationInSeconds,omitempty"`
	TrackingServerName                 string `json:"TrackingServerName"`
}

func (h *Handler) handleCreatePresignedMlflowTrackingServerURL(ctx context.Context, body []byte) ([]byte, error) {
	var req createPresignedMlflowTrackingServerURLInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	url, err := h.Backend.CreatePresignedMlflowTrackingServerURL(ctx, req.TrackingServerName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyAuthorizedURL: url})
}

// ---------------------------------------------------------------------------
// MlflowApp handlers
// ---------------------------------------------------------------------------

// createMlflowAppInput is CreateMlflowApp's request shape
// (api_op_CreateMlflowApp.go:29-71).
type createMlflowAppInput struct {
	Name                         string      `json:"Name"`
	ArtifactStoreURI             string      `json:"ArtifactStoreUri"`
	RoleArn                      string      `json:"RoleArn"`
	AccountDefaultStatus         string      `json:"AccountDefaultStatus,omitempty"`
	ModelRegistrationMode        string      `json:"ModelRegistrationMode,omitempty"`
	WeeklyMaintenanceWindowStart string      `json:"WeeklyMaintenanceWindowStart,omitempty"`
	DefaultDomainIDList          []string    `json:"DefaultDomainIdList,omitempty"`
	Tags                         []tagObject `json:"Tags"`
}

func (h *Handler) handleCreateMlflowApp(ctx context.Context, body []byte) ([]byte, error) {
	var req createMlflowAppInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateMlflowApp(ctx, CreateMlflowAppOptions{
		Name:                         req.Name,
		ArtifactStoreURI:             req.ArtifactStoreURI,
		RoleArn:                      req.RoleArn,
		AccountDefaultStatus:         req.AccountDefaultStatus,
		ModelRegistrationMode:        req.ModelRegistrationMode,
		WeeklyMaintenanceWindowStart: req.WeeklyMaintenanceWindowStart,
		DefaultDomainIDList:          req.DefaultDomainIDList,
		Tags:                         fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyGenericArn: result.Arn})
}

// describeMlflowAppResponse is the response body for DescribeMlflowApp.
// MaintenanceStatus (types.MaintenanceStatus) and CreatedBy/LastModifiedBy
// (types.UserContext) are disclosed absent, same reasoning as
// describeMlflowTrackingServerResponse above.
type describeMlflowAppResponse struct {
	Arn                          string   `json:"Arn"`
	Name                         string   `json:"Name"`
	Status                       string   `json:"Status"`
	ArtifactStoreURI             string   `json:"ArtifactStoreUri,omitempty"`
	RoleArn                      string   `json:"RoleArn,omitempty"`
	MlflowVersion                string   `json:"MlflowVersion,omitempty"`
	AccountDefaultStatus         string   `json:"AccountDefaultStatus,omitempty"`
	ModelRegistrationMode        string   `json:"ModelRegistrationMode,omitempty"`
	WeeklyMaintenanceWindowStart string   `json:"WeeklyMaintenanceWindowStart,omitempty"`
	DefaultDomainIDList          []string `json:"DefaultDomainIdList,omitempty"`
	CreationTime                 float64  `json:"CreationTime"`
	LastModifiedTime             float64  `json:"LastModifiedTime"`
}

// describeMlflowAppInput is DescribeMlflowApp's request shape
// (api_op_DescribeMlflowApp.go:29-37).
type describeMlflowAppInput struct {
	Arn string `json:"Arn"`
}

func (h *Handler) handleDescribeMlflowApp(ctx context.Context, body []byte) ([]byte, error) {
	var req describeMlflowAppInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeMlflowApp(ctx, req.Arn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(describeMlflowAppResponse{
		Arn:                          result.Arn,
		Name:                         result.Name,
		Status:                       result.Status,
		ArtifactStoreURI:             result.ArtifactStoreURI,
		RoleArn:                      result.RoleArn,
		MlflowVersion:                result.MlflowVersion,
		AccountDefaultStatus:         result.AccountDefaultStatus,
		ModelRegistrationMode:        result.ModelRegistrationMode,
		WeeklyMaintenanceWindowStart: result.WeeklyMaintenanceWindowStart,
		DefaultDomainIDList:          result.DefaultDomainIDList,
		CreationTime:                 epochSeconds(result.CreationTime),
		LastModifiedTime:             epochSeconds(result.LastModifiedTime),
	})
}

// deleteMlflowAppInput is DeleteMlflowApp's request shape
// (api_op_DeleteMlflowApp.go:27-35).
type deleteMlflowAppInput struct {
	Arn string `json:"Arn"`
}

func (h *Handler) handleDeleteMlflowApp(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteMlflowAppInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteMlflowApp(ctx, req.Arn); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyGenericArn: req.Arn})
}

// updateMlflowAppInput is UpdateMlflowApp's request shape
// (api_op_UpdateMlflowApp.go:28-72). Name is decoded but intentionally not
// threaded through — see UpdateMlflowAppOptions' doc comment (mlflow.go) for
// why: disclosed rather than silently dropped.
type updateMlflowAppInput struct {
	Arn                          string   `json:"Arn"`
	Name                         string   `json:"Name,omitempty"`
	ArtifactStoreURI             string   `json:"ArtifactStoreUri,omitempty"`
	AccountDefaultStatus         string   `json:"AccountDefaultStatus,omitempty"`
	ModelRegistrationMode        string   `json:"ModelRegistrationMode,omitempty"`
	WeeklyMaintenanceWindowStart string   `json:"WeeklyMaintenanceWindowStart,omitempty"`
	DefaultDomainIDList          []string `json:"DefaultDomainIdList,omitempty"`
}

func (h *Handler) handleUpdateMlflowApp(ctx context.Context, body []byte) ([]byte, error) {
	var req updateMlflowAppInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateMlflowApp(ctx, UpdateMlflowAppOptions{
		Arn:                          req.Arn,
		ArtifactStoreURI:             req.ArtifactStoreURI,
		AccountDefaultStatus:         req.AccountDefaultStatus,
		ModelRegistrationMode:        req.ModelRegistrationMode,
		WeeklyMaintenanceWindowStart: req.WeeklyMaintenanceWindowStart,
		DefaultDomainIDList:          req.DefaultDomainIDList,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyGenericArn: result.Arn})
}

// listMlflowAppsInput is ListMlflowApps' request shape
// (api_op_ListMlflowApps.go:30-73).
type listMlflowAppsInput struct {
	AccountDefaultStatus string   `json:"AccountDefaultStatus,omitempty"`
	CreatedAfter         *float64 `json:"CreatedAfter"`
	CreatedBefore        *float64 `json:"CreatedBefore"`
	DefaultForDomainID   string   `json:"DefaultForDomainId,omitempty"`
	MlflowVersion        string   `json:"MlflowVersion,omitempty"`
	NextToken            string   `json:"NextToken"`
	SortBy               string   `json:"SortBy,omitempty"`
	SortOrder            string   `json:"SortOrder,omitempty"`
	Status               string   `json:"Status,omitempty"`
	MaxResults           int32    `json:"MaxResults"`
}

func (h *Handler) handleListMlflowApps(ctx context.Context, body []byte) ([]byte, error) {
	var req listMlflowAppsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	apps, nextToken := h.Backend.ListMlflowApps(ctx, ListMlflowAppsParams{
		AccountDefaultStatus: req.AccountDefaultStatus,
		CreatedAfter:         timeFromEpochSecondsPtr(req.CreatedAfter),
		CreatedBefore:        timeFromEpochSecondsPtr(req.CreatedBefore),
		DefaultForDomainID:   req.DefaultForDomainID,
		MlflowVersion:        req.MlflowVersion,
		NextToken:            req.NextToken,
		SortBy:               req.SortBy,
		SortOrder:            req.SortOrder,
		Status:               req.Status,
		MaxResults:           req.MaxResults,
	})

	items := make([]map[string]any, 0, len(apps))
	for _, a := range apps {
		items = append(items, map[string]any{
			keyGenericArn:       a.Arn,
			keyGenericName:      a.Name,
			keyStatus:           a.Status,
			"MlflowVersion":     a.MlflowVersion,
			keyCreationTime:     epochSeconds(a.CreationTime),
			keyLastModifiedTime: epochSeconds(a.LastModifiedTime),
		})
	}

	return listResp("Summaries", items, nextToken)
}

// createPresignedMlflowAppURLInput is CreatePresignedMlflowAppUrl's request
// shape (api_op_CreatePresignedMlflowAppUrl.go:30-47). ExpiresInSeconds/
// SessionExpirationDurationInSeconds are modeled for wire visibility but
// disclosed no-op — same reasoning as
// createPresignedMlflowTrackingServerURLInput above.
type createPresignedMlflowAppURLInput struct {
	ExpiresInSeconds                   *int32 `json:"ExpiresInSeconds,omitempty"`
	SessionExpirationDurationInSeconds *int32 `json:"SessionExpirationDurationInSeconds,omitempty"`
	Arn                                string `json:"Arn"`
}

func (h *Handler) handleCreatePresignedMlflowAppURL(ctx context.Context, body []byte) ([]byte, error) {
	var req createPresignedMlflowAppURLInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	url, err := h.Backend.CreatePresignedMlflowAppURL(ctx, req.Arn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyAuthorizedURL: url})
}

// ---------------------------------------------------------------------------
// MLflow tracking server handlers (list + update)
// ---------------------------------------------------------------------------

// listMlflowTrackingServersInput is ListMlflowTrackingServers' request shape
// (api_op_ListMlflowTrackingServers.go:30-72).
type listMlflowTrackingServersInput struct {
	CreatedAfter         *float64 `json:"CreatedAfter"`
	CreatedBefore        *float64 `json:"CreatedBefore"`
	MlflowVersion        string   `json:"MlflowVersion,omitempty"`
	NextToken            string   `json:"NextToken"`
	SortBy               string   `json:"SortBy,omitempty"`
	SortOrder            string   `json:"SortOrder,omitempty"`
	TrackingServerStatus string   `json:"TrackingServerStatus,omitempty"`
	MaxResults           int32    `json:"MaxResults"`
}

func (h *Handler) handleListMlflowTrackingServers(ctx context.Context, body []byte) ([]byte, error) {
	var req listMlflowTrackingServersInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	servers, nextToken := h.Backend.ListMlflowTrackingServers(ctx, ListMlflowTrackingServersParams{
		CreatedAfter:         timeFromEpochSecondsPtr(req.CreatedAfter),
		CreatedBefore:        timeFromEpochSecondsPtr(req.CreatedBefore),
		MlflowVersion:        req.MlflowVersion,
		NextToken:            req.NextToken,
		SortBy:               req.SortBy,
		SortOrder:            req.SortOrder,
		TrackingServerStatus: req.TrackingServerStatus,
		MaxResults:           req.MaxResults,
	})

	items := make([]map[string]any, 0, len(servers))
	for _, s := range servers {
		entry := map[string]any{
			"TrackingServerName":   s.TrackingServerName,
			"TrackingServerArn":    s.TrackingServerArn,
			"TrackingServerStatus": s.TrackingServerStatus,
			"IsActive":             mlflowTrackingServerIsActive(s.TrackingServerStatus),
			keyCreationTime:        epochSeconds(s.CreationTime),
			keyLastModifiedTime:    epochSeconds(s.LastModifiedTime),
		}
		if s.MlflowVersion != "" {
			entry["MlflowVersion"] = s.MlflowVersion
		}

		items = append(items, entry)
	}

	return listResp("TrackingServerSummaries", items, nextToken)
}

// updateMlflowTrackingServerInput is UpdateMlflowTrackingServer's request
// shape (api_op_UpdateMlflowTrackingServer.go:28-63). MlflowVersion is
// deliberately absent — see UpdateMlflowTrackingServerOptions' doc comment
// (mlflow.go) for why.
type updateMlflowTrackingServerInput struct {
	AutomaticModelRegistration   *bool  `json:"AutomaticModelRegistration"`
	S3BucketOwnerVerification    *bool  `json:"S3BucketOwnerVerification"`
	TrackingServerName           string `json:"TrackingServerName"`
	ArtifactStoreURI             string `json:"ArtifactStoreUri,omitempty"`
	S3BucketOwnerAccountID       string `json:"S3BucketOwnerAccountId,omitempty"`
	TrackingServerSize           string `json:"TrackingServerSize,omitempty"`
	WeeklyMaintenanceWindowStart string `json:"WeeklyMaintenanceWindowStart,omitempty"`
}

func (h *Handler) handleUpdateMlflowTrackingServer(ctx context.Context, body []byte) ([]byte, error) {
	var req updateMlflowTrackingServerInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	s, err := h.Backend.UpdateMlflowTrackingServer(ctx, UpdateMlflowTrackingServerOptions(req))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyTrackingServerArn: s.TrackingServerArn})
}
