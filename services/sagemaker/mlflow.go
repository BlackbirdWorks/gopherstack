package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrMlflowTrackingServerNotFound is returned when an MLflow tracking server does not exist.
	ErrMlflowTrackingServerNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrMlflowAppNotFound is returned when an MLflow App does not exist.
	ErrMlflowAppNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
)

// ---------------------------------------------------------------------------
// MlflowTrackingServer
// ---------------------------------------------------------------------------

// MlflowTrackingServer represents a SageMaker MLflow tracking server.
type MlflowTrackingServer struct {
	CreationTime                 time.Time         `json:"CreationTime"`
	LastModifiedTime             time.Time         `json:"LastModifiedTime"`
	Tags                         map[string]string `json:"Tags,omitempty"`
	TrackingServerName           string            `json:"TrackingServerName"`
	TrackingServerArn            string            `json:"TrackingServerArn"`
	TrackingServerStatus         string            `json:"TrackingServerStatus"`
	RoleArn                      string            `json:"RoleArn,omitempty"`
	MlflowVersion                string            `json:"MlflowVersion,omitempty"`
	ArtifactStoreURI             string            `json:"ArtifactStoreUri,omitempty"`
	S3BucketOwnerAccountID       string            `json:"S3BucketOwnerAccountId,omitempty"`
	TrackingServerSize           string            `json:"TrackingServerSize,omitempty"`
	WeeklyMaintenanceWindowStart string            `json:"WeeklyMaintenanceWindowStart,omitempty"`
	AutomaticModelRegistration   bool              `json:"AutomaticModelRegistration"`
	S3BucketOwnerVerification    bool              `json:"S3BucketOwnerVerification"`
}

func cloneMlflowTrackingServer(s *MlflowTrackingServer) *MlflowTrackingServer {
	cp := *s
	cp.Tags = maps.Clone(s.Tags)

	return &cp
}

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeMlflowTrackingServer.
func (s *MlflowTrackingServer) MarshalJSON() ([]byte, error) {
	type alias MlflowTrackingServer

	return json.Marshal(struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(s),
		CreationTime:     epochSeconds(s.CreationTime),
		LastModifiedTime: epochSeconds(s.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [MlflowTrackingServer.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (s *MlflowTrackingServer) UnmarshalJSON(data []byte) error {
	type alias MlflowTrackingServer

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(s)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	s.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	s.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// CreateMlflowTrackingServerOptions holds the fields accepted by
// CreateMlflowTrackingServer (api_op_CreateMlflowTrackingServer.go:31-96,
// sagemaker@v1.263.2). Callers resolve the documented AutomaticModelRegistration
// (default false), S3BucketOwnerVerification (default true) and TrackingServerSize
// (default "Small") defaults before calling, since the wire request must
// distinguish "not sent" from "explicitly false" for the two booleans.
type CreateMlflowTrackingServerOptions struct {
	Tags                         map[string]string
	TrackingServerName           string
	ArtifactStoreURI             string
	RoleArn                      string
	MlflowVersion                string
	S3BucketOwnerAccountID       string
	TrackingServerSize           string
	WeeklyMaintenanceWindowStart string
	AutomaticModelRegistration   bool
	S3BucketOwnerVerification    bool
}

// CreateMlflowTrackingServer creates an MLflow tracking server.
func (b *InMemoryBackend) CreateMlflowTrackingServer(
	ctx context.Context,
	opts CreateMlflowTrackingServerOptions,
) (*MlflowTrackingServer, error) {
	if opts.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", ErrValidation)
	}

	return sagemakerCreate(ctx, b,
		"CreateMlflowTrackingServer", opts.TrackingServerName, "mlflow-tracking-server",
		b.mlflowTrackingServersStore,
		func(n string) error { return sagemakerDupErr("MLflow tracking server", n) },
		func(arnStr string, now time.Time) *MlflowTrackingServer {
			return &MlflowTrackingServer{
				TrackingServerName:           opts.TrackingServerName,
				TrackingServerArn:            arnStr,
				TrackingServerStatus:         statusCreated,
				RoleArn:                      opts.RoleArn,
				MlflowVersion:                opts.MlflowVersion,
				ArtifactStoreURI:             opts.ArtifactStoreURI,
				AutomaticModelRegistration:   opts.AutomaticModelRegistration,
				S3BucketOwnerAccountID:       opts.S3BucketOwnerAccountID,
				S3BucketOwnerVerification:    opts.S3BucketOwnerVerification,
				TrackingServerSize:           opts.TrackingServerSize,
				WeeklyMaintenanceWindowStart: opts.WeeklyMaintenanceWindowStart,
				Tags:                         mergeTags(nil, opts.Tags),
				CreationTime:                 now,
				LastModifiedTime:             now,
			}
		},
		cloneMlflowTrackingServer,
	)
}

// DescribeMlflowTrackingServer returns an MLflow tracking server by name.
func (b *InMemoryBackend) DescribeMlflowTrackingServer(
	ctx context.Context,
	name string,
) (*MlflowTrackingServer, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeMlflowTrackingServer")
	defer b.mu.RUnlock()

	s, ok := b.mlflowTrackingServersStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	return cloneMlflowTrackingServer(s), nil
}

// DeleteMlflowTrackingServer removes an MLflow tracking server by name and
// returns its ARN (DeleteMlflowTrackingServerOutput.TrackingServerArn,
// api_op_DeleteMlflowTrackingServer.go:39-45).
func (b *InMemoryBackend) DeleteMlflowTrackingServer(ctx context.Context, name string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteMlflowTrackingServer")
	defer b.mu.Unlock()

	store := b.mlflowTrackingServersStore(region)

	s, ok := store.Get(name)
	if !ok {
		return "", fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	arnStr := s.TrackingServerArn
	store.Delete(name)

	return arnStr, nil
}

// StartMlflowTrackingServer sets an MLflow tracking server status to "Running"
// and returns its ARN (StartMlflowTrackingServerOutput.TrackingServerArn,
// api_op_StartMlflowTrackingServer.go:37-43).
func (b *InMemoryBackend) StartMlflowTrackingServer(ctx context.Context, name string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StartMlflowTrackingServer")
	defer b.mu.Unlock()

	s, ok := b.mlflowTrackingServersStore(region).Get(name)
	if !ok {
		return "", fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	s.TrackingServerStatus = statusRunning
	s.LastModifiedTime = time.Now()

	return s.TrackingServerArn, nil
}

// StopMlflowTrackingServer sets an MLflow tracking server status to "Stopped"
// and returns its ARN (StopMlflowTrackingServerOutput.TrackingServerArn,
// api_op_StopMlflowTrackingServer.go:37-43).
func (b *InMemoryBackend) StopMlflowTrackingServer(ctx context.Context, name string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StopMlflowTrackingServer")
	defer b.mu.Unlock()

	s, ok := b.mlflowTrackingServersStore(region).Get(name)
	if !ok {
		return "", fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	s.TrackingServerStatus = pipelineStatusStopped
	s.LastModifiedTime = time.Now()

	return s.TrackingServerArn, nil
}

// mlflowTrackingServerURL builds the (unsigned, real-shaped) MLflow UI base
// URL for a tracking server, shared by DescribeMlflowTrackingServer's
// TrackingServerUrl and CreatePresignedMlflowTrackingServerURL's AuthorizedUrl.
func mlflowTrackingServerURL(name, region string) string {
	return "https://" + name + ".mlflow-tracking-server.sagemaker." + region + ".amazonaws.com"
}

// mlflowTrackingServerIsActive derives IsTrackingServerActive
// (types/enums.go:4962-4969) from status: real AWS's own description of the
// field ("whether the tracking server is currently active") means Active
// exactly when the tracking server is Running, matching this backend's own
// Start/Stop-driven TrackingServerStatus values.
func mlflowTrackingServerIsActive(status string) string {
	if status == statusRunning {
		return statusActive
	}

	return "Inactive"
}

// CreatePresignedMlflowTrackingServerURL returns a one-time presigned URL for
// accessing the MLflow UI of an existing tracking server.
func (b *InMemoryBackend) CreatePresignedMlflowTrackingServerURL(ctx context.Context, name string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("CreatePresignedMlflowTrackingServerURL")
	defer b.mu.RUnlock()

	if _, ok := b.mlflowTrackingServersStoreRO(region).Get(name); !ok {
		return "", fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	return mlflowTrackingServerURL(name, region) + "/auth?authToken=" + generateID(), nil
}

// ---------------------------------------------------------------------------
// MlflowApp
// ---------------------------------------------------------------------------

// MlflowApp represents a SageMaker MLflow App.
type MlflowApp struct {
	CreationTime                 time.Time         `json:"CreationTime"`
	LastModifiedTime             time.Time         `json:"LastModifiedTime"`
	Tags                         map[string]string `json:"Tags,omitempty"`
	Name                         string            `json:"Name"`
	Arn                          string            `json:"Arn"`
	Status                       string            `json:"Status"`
	ArtifactStoreURI             string            `json:"ArtifactStoreUri,omitempty"`
	RoleArn                      string            `json:"RoleArn,omitempty"`
	MlflowVersion                string            `json:"MlflowVersion,omitempty"`
	AccountDefaultStatus         string            `json:"AccountDefaultStatus,omitempty"`
	ModelRegistrationMode        string            `json:"ModelRegistrationMode,omitempty"`
	WeeklyMaintenanceWindowStart string            `json:"WeeklyMaintenanceWindowStart,omitempty"`
	DefaultDomainIDList          []string          `json:"DefaultDomainIdList,omitempty"`
}

func cloneMlflowApp(m *MlflowApp) *MlflowApp {
	cp := *m
	cp.Tags = maps.Clone(m.Tags)
	cp.DefaultDomainIDList = append([]string(nil), m.DefaultDomainIDList...)

	return &cp
}

// CreateMlflowAppOptions holds the fields accepted by CreateMlflowApp.
type CreateMlflowAppOptions struct {
	Tags                         map[string]string
	Name                         string
	ArtifactStoreURI             string
	RoleArn                      string
	AccountDefaultStatus         string
	ModelRegistrationMode        string
	WeeklyMaintenanceWindowStart string
	DefaultDomainIDList          []string
}

// CreateMlflowApp creates an MLflow App. Stores by ARN; Name is used only to build the ARN.
func (b *InMemoryBackend) CreateMlflowApp(ctx context.Context, opts CreateMlflowAppOptions) (*MlflowApp, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateMlflowApp")
	defer b.mu.Unlock()

	appARN := arn.Build("sagemaker", region, b.accountID, "mlflow-app/"+opts.Name)

	store := b.mlflowAppsStore(region)
	if _, ok := store.Get(appARN); ok {
		return nil, sagemakerDupErr("MLflow App", opts.Name)
	}

	now := time.Now()
	m := &MlflowApp{
		Name:                         opts.Name,
		Arn:                          appARN,
		Status:                       statusCreated,
		ArtifactStoreURI:             opts.ArtifactStoreURI,
		RoleArn:                      opts.RoleArn,
		AccountDefaultStatus:         opts.AccountDefaultStatus,
		ModelRegistrationMode:        opts.ModelRegistrationMode,
		WeeklyMaintenanceWindowStart: opts.WeeklyMaintenanceWindowStart,
		DefaultDomainIDList:          opts.DefaultDomainIDList,
		Tags:                         mergeTags(nil, opts.Tags),
		CreationTime:                 now,
		LastModifiedTime:             now,
	}
	store.Put(m)

	return cloneMlflowApp(m), nil
}

// DescribeMlflowApp returns an MLflow App by ARN.
func (b *InMemoryBackend) DescribeMlflowApp(ctx context.Context, arnStr string) (*MlflowApp, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeMlflowApp")
	defer b.mu.RUnlock()

	m, ok := b.mlflowAppsStoreRO(region).Get(arnStr)
	if !ok {
		return nil, fmt.Errorf("%w: MLflow App %q not found", ErrMlflowAppNotFound, arnStr)
	}

	return cloneMlflowApp(m), nil
}

// DeleteMlflowApp removes an MLflow App by ARN.
func (b *InMemoryBackend) DeleteMlflowApp(ctx context.Context, arnStr string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteMlflowApp")
	defer b.mu.Unlock()

	store := b.mlflowAppsStore(region)

	if _, ok := store.Get(arnStr); !ok {
		return fmt.Errorf("%w: MLflow App %q not found", ErrMlflowAppNotFound, arnStr)
	}

	store.Delete(arnStr)

	return nil
}

// UpdateMlflowAppOptions holds the mutable fields accepted by UpdateMlflowApp.
// Name (api_op_UpdateMlflowApp.go:29-72) is deliberately not modeled here:
// AWS's own doc page (API_UpdateMlflowApp.html) gives Name no description
// beyond "The name of the MLflow App to update", and this backend's MlflowApp
// is stored by an Arn built from Name at creation time (mlflow-app/<name>),
// so treating Name as a rename would require rekeying the store — a
// consistency-vs-fabrication tradeoff the docs don't resolve either way.
// Disclosed rather than guessed.
type UpdateMlflowAppOptions struct {
	Arn                          string
	ArtifactStoreURI             string
	AccountDefaultStatus         string
	ModelRegistrationMode        string
	WeeklyMaintenanceWindowStart string
	DefaultDomainIDList          []string
}

// UpdateMlflowApp updates an MLflow App's mutable fields.
func (b *InMemoryBackend) UpdateMlflowApp(ctx context.Context, opts UpdateMlflowAppOptions) (*MlflowApp, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateMlflowApp")
	defer b.mu.Unlock()

	m, ok := b.mlflowAppsStore(region).Get(opts.Arn)
	if !ok {
		return nil, fmt.Errorf("%w: MLflow App %q not found", ErrMlflowAppNotFound, opts.Arn)
	}

	if opts.ArtifactStoreURI != "" {
		m.ArtifactStoreURI = opts.ArtifactStoreURI
	}

	if opts.AccountDefaultStatus != "" {
		m.AccountDefaultStatus = opts.AccountDefaultStatus
	}

	if opts.ModelRegistrationMode != "" {
		m.ModelRegistrationMode = opts.ModelRegistrationMode
	}

	if opts.WeeklyMaintenanceWindowStart != "" {
		m.WeeklyMaintenanceWindowStart = opts.WeeklyMaintenanceWindowStart
	}

	if opts.DefaultDomainIDList != nil {
		m.DefaultDomainIDList = opts.DefaultDomainIDList
	}

	m.LastModifiedTime = time.Now()

	return cloneMlflowApp(m), nil
}

// ListMlflowAppsParams bundles ListMlflowApps' filter/sort/pagination input
// (api_op_ListMlflowApps.go:30-73, sagemaker@v1.263.2).
type ListMlflowAppsParams struct {
	CreatedAfter         *time.Time
	CreatedBefore        *time.Time
	AccountDefaultStatus string
	DefaultForDomainID   string
	MlflowVersion        string
	NextToken            string
	SortBy               string
	SortOrder            string
	Status               string
	MaxResults           int32
}

// ListMlflowApps returns MLflow Apps matching params, sorted per params.SortBy
// (one of Name/CreationTime/Status) / params.SortOrder — both defaulted to
// CreationTime/Descending per the real op's documented default ("By default,
// MLflow Apps are listed in Descending order by creation time",
// api_op_ListMlflowApps.go:22-24) — capped at params.MaxResults.
func (b *InMemoryBackend) ListMlflowApps(ctx context.Context, params ListMlflowAppsParams) ([]*MlflowApp, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListMlflowApps")
	defer b.mu.RUnlock()

	tbl := b.mlflowAppsStoreRO(region)
	list := make([]*MlflowApp, 0, tbl.Len())

	for _, m := range tbl.All() {
		if !mlflowAppMatchesListParams(m, params) {
			continue
		}

		list = append(list, cloneMlflowApp(m))
	}

	desc := !strings.EqualFold(params.SortOrder, "Ascending")
	sort.Slice(list, func(i, j int) bool {
		less := mlflowAppSortLess(list[i], list[j], params.SortBy)
		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// mlflowAppMatchesListParams reports whether m passes every filter in params.
func mlflowAppMatchesListParams(m *MlflowApp, params ListMlflowAppsParams) bool {
	if params.AccountDefaultStatus != "" && m.AccountDefaultStatus != params.AccountDefaultStatus {
		return false
	}

	if params.Status != "" && m.Status != params.Status {
		return false
	}

	if params.MlflowVersion != "" && m.MlflowVersion != params.MlflowVersion {
		return false
	}

	if params.DefaultForDomainID != "" && !slices.Contains(m.DefaultDomainIDList, params.DefaultForDomainID) {
		return false
	}

	if params.CreatedAfter != nil && !m.CreationTime.After(*params.CreatedAfter) {
		return false
	}

	if params.CreatedBefore != nil && !m.CreationTime.Before(*params.CreatedBefore) {
		return false
	}

	return true
}

// mlflowAppSortLess orders two MLflow Apps by sortBy — one of SortMlflowAppBy's
// real values (Name, CreationTime, Status; types/enums.go:9219-9233) — falling
// through to the Arn tiebreak for a stable order.
func mlflowAppSortLess(a, b *MlflowApp, sortBy string) bool {
	switch sortBy {
	case keyGenericName:
		if a.Name != b.Name {
			return a.Name < b.Name
		}
	case keyStatus:
		if a.Status != b.Status {
			return a.Status < b.Status
		}
	default:
		if !a.CreationTime.Equal(b.CreationTime) {
			return a.CreationTime.Before(b.CreationTime)
		}
	}

	return a.Arn < b.Arn
}

// CreatePresignedMlflowAppURL returns a one-time presigned URL for accessing
// the MLflow UI of an existing MLflow App.
func (b *InMemoryBackend) CreatePresignedMlflowAppURL(ctx context.Context, arnStr string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("CreatePresignedMlflowAppURL")
	defer b.mu.RUnlock()

	m, ok := b.mlflowAppsStoreRO(region).Get(arnStr)
	if !ok {
		return "", fmt.Errorf("%w: MLflow App %q not found", ErrMlflowAppNotFound, arnStr)
	}

	return "https://" + m.Name + ".mlflow-app.sagemaker." + region +
		".amazonaws.com/auth?authToken=" + generateID(), nil
}

// UpdateMlflowTrackingServerOptions holds the mutable fields accepted by
// UpdateMlflowTrackingServer (api_op_UpdateMlflowTrackingServer.go:28-63,
// sagemaker@v1.263.2). AutomaticModelRegistration/S3BucketOwnerVerification
// are *bool: AWS's own doc page restates Create's "defaults to
// False"/"defaults to True if not provided" language verbatim for this
// partial-update op, but every one of this op's other five optional fields
// behaves as leave-unchanged-if-omitted, and no other Update op in this
// service resets a value to a constant on omission — so nil (not sent) means
// leave-unchanged here too, disclosed rather than silently reset.
//
// MlflowVersion is deliberately absent: it's a real CreateMlflowTrackingServerInput/
// DescribeMlflowTrackingServerOutput field but has no UpdateMlflowTrackingServerInput
// counterpart at all — no real client can ever change it after creation.
type UpdateMlflowTrackingServerOptions struct {
	AutomaticModelRegistration   *bool
	S3BucketOwnerVerification    *bool
	TrackingServerName           string
	ArtifactStoreURI             string
	S3BucketOwnerAccountID       string
	TrackingServerSize           string
	WeeklyMaintenanceWindowStart string
}

// UpdateMlflowTrackingServer updates an MLflow tracking server.
func (b *InMemoryBackend) UpdateMlflowTrackingServer(
	ctx context.Context,
	opts UpdateMlflowTrackingServerOptions,
) (*MlflowTrackingServer, error) {
	b.mu.Lock("UpdateMlflowTrackingServer")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	s, ok := b.mlflowTrackingServersStore(region).Get(opts.TrackingServerName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, opts.TrackingServerName,
		)
	}

	if opts.ArtifactStoreURI != "" {
		s.ArtifactStoreURI = opts.ArtifactStoreURI
	}

	if opts.S3BucketOwnerAccountID != "" {
		s.S3BucketOwnerAccountID = opts.S3BucketOwnerAccountID
	}

	if opts.TrackingServerSize != "" {
		s.TrackingServerSize = opts.TrackingServerSize
	}

	if opts.WeeklyMaintenanceWindowStart != "" {
		s.WeeklyMaintenanceWindowStart = opts.WeeklyMaintenanceWindowStart
	}

	if opts.AutomaticModelRegistration != nil {
		s.AutomaticModelRegistration = *opts.AutomaticModelRegistration
	}

	if opts.S3BucketOwnerVerification != nil {
		s.S3BucketOwnerVerification = *opts.S3BucketOwnerVerification
	}

	s.LastModifiedTime = time.Now()

	return cloneMlflowTrackingServer(s), nil
}

// ListMlflowTrackingServersParams bundles ListMlflowTrackingServers'
// filter/sort/pagination input (api_op_ListMlflowTrackingServers.go:30-72,
// sagemaker@v1.263.2).
type ListMlflowTrackingServersParams struct {
	CreatedAfter         *time.Time
	CreatedBefore        *time.Time
	MlflowVersion        string
	NextToken            string
	SortBy               string
	SortOrder            string
	TrackingServerStatus string
	MaxResults           int32
}

// ListMlflowTrackingServers returns tracking servers matching params, sorted
// per params.SortBy (one of Name/CreationTime/Status) / params.SortOrder —
// both defaulted to CreationTime/Descending per the real op's documented
// default ("By default, tracking servers are listed in Descending order by
// creation time", api_op_ListMlflowTrackingServers.go:20-22) — capped at
// params.MaxResults.
func (b *InMemoryBackend) ListMlflowTrackingServers(
	ctx context.Context,
	params ListMlflowTrackingServersParams,
) ([]*MlflowTrackingServer, string) {
	b.mu.RLock("ListMlflowTrackingServers")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	tbl := b.mlflowTrackingServersStoreRO(region)
	list := make([]*MlflowTrackingServer, 0, tbl.Len())

	for _, s := range tbl.All() {
		if !mlflowTrackingServerMatchesListParams(s, params) {
			continue
		}

		list = append(list, cloneMlflowTrackingServer(s))
	}

	desc := !strings.EqualFold(params.SortOrder, "Ascending")
	sort.Slice(list, func(i, j int) bool {
		less := mlflowTrackingServerSortLess(list[i], list[j], params.SortBy)
		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// mlflowTrackingServerMatchesListParams reports whether s passes every filter
// in params.
func mlflowTrackingServerMatchesListParams(s *MlflowTrackingServer, params ListMlflowTrackingServersParams) bool {
	if params.TrackingServerStatus != "" && s.TrackingServerStatus != params.TrackingServerStatus {
		return false
	}

	if params.MlflowVersion != "" && s.MlflowVersion != params.MlflowVersion {
		return false
	}

	if params.CreatedAfter != nil && !s.CreationTime.After(*params.CreatedAfter) {
		return false
	}

	if params.CreatedBefore != nil && !s.CreationTime.Before(*params.CreatedBefore) {
		return false
	}

	return true
}

// mlflowTrackingServerSortLess orders two tracking servers by sortBy — one of
// SortTrackingServerBy's real values (Name, CreationTime, Status;
// types/enums.go:9320-9334) — falling through to the TrackingServerArn
// tiebreak for a stable order.
func mlflowTrackingServerSortLess(a, b *MlflowTrackingServer, sortBy string) bool {
	switch sortBy {
	case keyGenericName:
		if a.TrackingServerName != b.TrackingServerName {
			return a.TrackingServerName < b.TrackingServerName
		}
	case keyStatus:
		if a.TrackingServerStatus != b.TrackingServerStatus {
			return a.TrackingServerStatus < b.TrackingServerStatus
		}
	default:
		if !a.CreationTime.Equal(b.CreationTime) {
			return a.CreationTime.Before(b.CreationTime)
		}
	}

	return a.TrackingServerArn < b.TrackingServerArn
}
