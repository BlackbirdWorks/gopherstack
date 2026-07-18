package sagemaker

import (
	"context"
	"fmt"
	"maps"
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
	CreationTime         time.Time         `json:"CreationTime"`
	LastModifiedTime     time.Time         `json:"LastModifiedTime"`
	Tags                 map[string]string `json:"Tags,omitempty"`
	TrackingServerName   string            `json:"TrackingServerName"`
	TrackingServerArn    string            `json:"TrackingServerArn"`
	TrackingServerStatus string            `json:"TrackingServerStatus"`
	RoleArn              string            `json:"RoleArn,omitempty"`
	MlflowVersion        string            `json:"MlflowVersion,omitempty"`
}

func cloneMlflowTrackingServer(s *MlflowTrackingServer) *MlflowTrackingServer {
	cp := *s
	cp.Tags = maps.Clone(s.Tags)

	return &cp
}

// CreateMlflowTrackingServer creates an MLflow tracking server.
func (b *InMemoryBackend) CreateMlflowTrackingServer(
	ctx context.Context,
	name, roleArn, mlflowVersion string,
	tags map[string]string,
) (*MlflowTrackingServer, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", ErrValidation)
	}

	return sagemakerCreate(ctx, b,
		"CreateMlflowTrackingServer", name, "mlflow-tracking-server",
		b.mlflowTrackingServersStore,
		func(n string) error { return sagemakerDupErr("MLflow tracking server", n) },
		func(arnStr string, now time.Time) *MlflowTrackingServer {
			return &MlflowTrackingServer{
				TrackingServerName:   name,
				TrackingServerArn:    arnStr,
				TrackingServerStatus: "Created",
				RoleArn:              roleArn,
				MlflowVersion:        mlflowVersion,
				Tags:                 mergeTags(nil, tags),
				CreationTime:         now,
				LastModifiedTime:     now,
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

// DeleteMlflowTrackingServer removes an MLflow tracking server by name.
func (b *InMemoryBackend) DeleteMlflowTrackingServer(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteMlflowTrackingServer")
	defer b.mu.Unlock()

	store := b.mlflowTrackingServersStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	store.Delete(name)

	return nil
}

// StartMlflowTrackingServer sets an MLflow tracking server status to "Running".
func (b *InMemoryBackend) StartMlflowTrackingServer(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StartMlflowTrackingServer")
	defer b.mu.Unlock()

	s, ok := b.mlflowTrackingServersStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	s.TrackingServerStatus = "Running"
	s.LastModifiedTime = time.Now()

	return nil
}

// StopMlflowTrackingServer sets an MLflow tracking server status to "Stopped".
func (b *InMemoryBackend) StopMlflowTrackingServer(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StopMlflowTrackingServer")
	defer b.mu.Unlock()

	s, ok := b.mlflowTrackingServersStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	s.TrackingServerStatus = pipelineStatusStopped
	s.LastModifiedTime = time.Now()

	return nil
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

	return "https://" + name + ".mlflow-tracking-server.sagemaker." + region +
		".amazonaws.com/auth?authToken=" + generateID(), nil
}

// ---------------------------------------------------------------------------
// MlflowApp
// ---------------------------------------------------------------------------

// MlflowApp represents a SageMaker MLflow App.
type MlflowApp struct {
	CreationTime          time.Time         `json:"CreationTime"`
	LastModifiedTime      time.Time         `json:"LastModifiedTime"`
	Tags                  map[string]string `json:"Tags,omitempty"`
	Name                  string            `json:"Name"`
	Arn                   string            `json:"Arn"`
	Status                string            `json:"Status"`
	ArtifactStoreURI      string            `json:"ArtifactStoreUri,omitempty"`
	RoleArn               string            `json:"RoleArn,omitempty"`
	MlflowVersion         string            `json:"MlflowVersion,omitempty"`
	AccountDefaultStatus  string            `json:"AccountDefaultStatus,omitempty"`
	ModelRegistrationMode string            `json:"ModelRegistrationMode,omitempty"`
	DefaultDomainIDList   []string          `json:"DefaultDomainIdList,omitempty"`
}

func cloneMlflowApp(m *MlflowApp) *MlflowApp {
	cp := *m
	cp.Tags = maps.Clone(m.Tags)
	cp.DefaultDomainIDList = append([]string(nil), m.DefaultDomainIDList...)

	return &cp
}

// CreateMlflowAppOptions holds the fields accepted by CreateMlflowApp.
type CreateMlflowAppOptions struct {
	Tags                  map[string]string
	Name                  string
	ArtifactStoreURI      string
	RoleArn               string
	AccountDefaultStatus  string
	ModelRegistrationMode string
	DefaultDomainIDList   []string
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
		Name:                  opts.Name,
		Arn:                   appARN,
		Status:                statusCreated,
		ArtifactStoreURI:      opts.ArtifactStoreURI,
		RoleArn:               opts.RoleArn,
		AccountDefaultStatus:  opts.AccountDefaultStatus,
		ModelRegistrationMode: opts.ModelRegistrationMode,
		DefaultDomainIDList:   opts.DefaultDomainIDList,
		Tags:                  mergeTags(nil, opts.Tags),
		CreationTime:          now,
		LastModifiedTime:      now,
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
type UpdateMlflowAppOptions struct {
	Arn                   string
	ArtifactStoreURI      string
	AccountDefaultStatus  string
	ModelRegistrationMode string
	DefaultDomainIDList   []string
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

	if opts.DefaultDomainIDList != nil {
		m.DefaultDomainIDList = opts.DefaultDomainIDList
	}

	m.LastModifiedTime = time.Now()

	return cloneMlflowApp(m), nil
}

// ListMlflowApps returns a page of MLflow Apps.
func (b *InMemoryBackend) ListMlflowApps(ctx context.Context, nextToken string) ([]*MlflowApp, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListMlflowApps")
	defer b.mu.RUnlock()

	return sagemakerListKeyPaged(
		b.mlflowAppsStoreRO(region),
		nextToken,
		cloneMlflowApp,
		func(v *MlflowApp) string { return v.Arn },
	)
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

// UpdateMlflowTrackingServer updates an MLflow tracking server.
func (b *InMemoryBackend) UpdateMlflowTrackingServer(
	ctx context.Context,
	name, mlflowVersion string,
) (*MlflowTrackingServer, error) {
	b.mu.Lock("UpdateMlflowTrackingServer")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	s, ok := b.mlflowTrackingServersStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	if mlflowVersion != "" {
		s.MlflowVersion = mlflowVersion
	}

	s.LastModifiedTime = time.Now()

	return cloneMlflowTrackingServer(s), nil
}

// ListMlflowTrackingServers returns all MLflow tracking servers.
func (b *InMemoryBackend) ListMlflowTrackingServers(
	ctx context.Context,
	nextToken string,
) ([]*MlflowTrackingServer, string) {
	b.mu.RLock("ListMlflowTrackingServers")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.mlflowTrackingServersStoreRO(region),
		nextToken,
		cloneMlflowTrackingServer,
		func(v *MlflowTrackingServer) string { return v.TrackingServerName },
	)
}
