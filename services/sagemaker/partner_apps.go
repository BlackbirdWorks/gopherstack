package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrPartnerAppNotFound is returned when a partner app does not exist.
var ErrPartnerAppNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// PartnerApp
// ---------------------------------------------------------------------------

// PartnerAppMaintenanceConfig mirrors types.PartnerAppMaintenanceConfig
// (types/types.go:17113-17122), a single field flat enough not to need
// json.RawMessage passthrough.
type PartnerAppMaintenanceConfig struct {
	MaintenanceWindowStart string `json:"MaintenanceWindowStart,omitempty"`
}

// PartnerApp represents a SageMaker partner app.
type PartnerApp struct {
	CreationTime                  time.Time                    `json:"CreationTime"`
	LastModifiedTime              time.Time                    `json:"LastModifiedTime"`
	Tags                          map[string]string            `json:"Tags,omitempty"`
	MaintenanceConfig             *PartnerAppMaintenanceConfig `json:"MaintenanceConfig,omitempty"`
	ExecutionRoleArn              string                       `json:"ExecutionRoleArn,omitempty"`
	Arn                           string                       `json:"Arn"`
	Status                        string                       `json:"Status"`
	Type                          string                       `json:"Type,omitempty"`
	Name                          string                       `json:"Name"`
	AuthType                      string                       `json:"AuthType,omitempty"`
	Tier                          string                       `json:"Tier,omitempty"`
	KmsKeyID                      string                       `json:"KmsKeyId,omitempty"`
	BaseURL                       string                       `json:"-"`
	ApplicationConfig             json.RawMessage              `json:"ApplicationConfig,omitempty"`
	EnableAutoMinorVersionUpgrade bool                         `json:"EnableAutoMinorVersionUpgrade,omitempty"`
	EnableIamSessionBasedIdentity bool                         `json:"EnableIamSessionBasedIdentity,omitempty"`
}

func clonePartnerApp(p *PartnerApp) *PartnerApp {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)
	cp.ApplicationConfig = append(json.RawMessage(nil), p.ApplicationConfig...)

	if p.MaintenanceConfig != nil {
		mc := *p.MaintenanceConfig
		cp.MaintenanceConfig = &mc
	}

	return &cp
}

// partnerAppBaseURL synthesizes the URL DescribePartnerAppOutput.BaseUrl
// documents ("the URL... that the Application SDK uses to support in-app
// calls for the user"), mirroring CreatePartnerAppPresignedURL's own
// synthesized host — this backend has no real partner-app-hosting
// infrastructure to derive one from.
func partnerAppBaseURL(name, region string) string {
	return "https://" + name + ".partner-app.sagemaker." + region + ".amazonaws.com"
}

// CreatePartnerAppOptions holds the fields accepted by CreatePartnerApp
// (api_op_CreatePartnerApp.go:36-93). ClientToken (a pure client-side
// idempotency token with no server-observable effect, per this service's
// repo-wide convention — see CreateModelPackageOptions) is deliberately
// omitted.
type CreatePartnerAppOptions struct {
	Tags                          map[string]string
	MaintenanceConfig             *PartnerAppMaintenanceConfig
	Name                          string
	Type                          string
	ExecutionRoleArn              string
	AuthType                      string
	Tier                          string
	KmsKeyID                      string
	ApplicationConfig             json.RawMessage
	EnableAutoMinorVersionUpgrade bool
	EnableIamSessionBasedIdentity bool
}

// CreatePartnerApp creates a partner app. Stores by ARN; returns both name and ARN.
func (b *InMemoryBackend) CreatePartnerApp(ctx context.Context, opts CreatePartnerAppOptions) (*PartnerApp, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreatePartnerApp")
	defer b.mu.Unlock()

	if opts.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	appARN := arn.Build("sagemaker", region, b.accountID, "partner-app/"+opts.Name)

	store := b.partnerAppsStore(region)

	if _, ok := store.Get(appARN); ok {
		return nil, fmt.Errorf("%w: partner app %q already exists", ErrValidation, opts.Name)
	}

	now := time.Now()
	p := &PartnerApp{
		Name:                          opts.Name,
		Arn:                           appARN,
		Status:                        "Available",
		Type:                          opts.Type,
		ExecutionRoleArn:              opts.ExecutionRoleArn,
		AuthType:                      opts.AuthType,
		Tier:                          opts.Tier,
		KmsKeyID:                      opts.KmsKeyID,
		ApplicationConfig:             opts.ApplicationConfig,
		MaintenanceConfig:             opts.MaintenanceConfig,
		EnableAutoMinorVersionUpgrade: opts.EnableAutoMinorVersionUpgrade,
		EnableIamSessionBasedIdentity: opts.EnableIamSessionBasedIdentity,
		Tags:                          mergeTags(nil, opts.Tags),
		CreationTime:                  now,
		LastModifiedTime:              now,
	}
	store.Put(p)

	return clonePartnerApp(p), nil
}

// DescribePartnerApp returns a partner app by ARN.
func (b *InMemoryBackend) DescribePartnerApp(ctx context.Context, arnStr string) (*PartnerApp, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribePartnerApp")
	defer b.mu.RUnlock()

	p, ok := b.partnerAppsStoreRO(region).Get(arnStr)
	if !ok {
		return nil, fmt.Errorf("%w: partner app %q not found", ErrPartnerAppNotFound, arnStr)
	}

	cp := clonePartnerApp(p)
	cp.BaseURL = partnerAppBaseURL(cp.Name, region)

	return cp, nil
}

// DeletePartnerApp removes a partner app by ARN.
func (b *InMemoryBackend) DeletePartnerApp(ctx context.Context, arnStr string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeletePartnerApp")
	defer b.mu.Unlock()

	store := b.partnerAppsStore(region)

	if _, ok := store.Get(arnStr); !ok {
		return fmt.Errorf("%w: partner app %q not found", ErrPartnerAppNotFound, arnStr)
	}

	store.Delete(arnStr)

	return nil
}

// UpdatePartnerAppOptions holds the mutable fields accepted by
// UpdatePartnerApp (api_op_UpdatePartnerApp.go:24-71). AppVersion is decoded
// by the handler for wire-shape fidelity but is a disclosed no-op: this
// backend tracks no minor-version-upgrade catalog (DescribePartnerAppOutput's
// AvailableUpgrade/Version are likewise never populated), so there is no
// version state for it to advance. ClientToken is omitted for the same
// reason as CreatePartnerApp's.
type UpdatePartnerAppOptions struct {
	MaintenanceConfig             *PartnerAppMaintenanceConfig
	Tags                          map[string]string
	EnableAutoMinorVersionUpgrade *bool
	EnableIamSessionBasedIdentity *bool
	Arn                           string
	Tier                          string
	ApplicationConfig             json.RawMessage
}

// UpdatePartnerApp updates a partner app's mutable fields.
func (b *InMemoryBackend) UpdatePartnerApp(ctx context.Context, opts UpdatePartnerAppOptions) (*PartnerApp, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdatePartnerApp")
	defer b.mu.Unlock()

	p, ok := b.partnerAppsStore(region).Get(opts.Arn)
	if !ok {
		return nil, fmt.Errorf("%w: partner app %q not found", ErrPartnerAppNotFound, opts.Arn)
	}

	if opts.Tier != "" {
		p.Tier = opts.Tier
	}

	if opts.ApplicationConfig != nil {
		p.ApplicationConfig = opts.ApplicationConfig
	}

	if opts.MaintenanceConfig != nil {
		p.MaintenanceConfig = opts.MaintenanceConfig
	}

	if opts.EnableAutoMinorVersionUpgrade != nil {
		p.EnableAutoMinorVersionUpgrade = *opts.EnableAutoMinorVersionUpgrade
	}

	if opts.EnableIamSessionBasedIdentity != nil {
		p.EnableIamSessionBasedIdentity = *opts.EnableIamSessionBasedIdentity
	}

	if len(opts.Tags) > 0 {
		p.Tags = mergeTags(p.Tags, opts.Tags)
	}

	p.LastModifiedTime = time.Now()

	return clonePartnerApp(p), nil
}

// ListPartnerApps returns a page of partner apps, capped at maxResults (<= 0
// falls back to the service default page size).
func (b *InMemoryBackend) ListPartnerApps(
	ctx context.Context, nextToken string, maxResults int32,
) ([]*PartnerApp, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListPartnerApps")
	defer b.mu.RUnlock()

	return sagemakerListKeyPagedN(
		b.partnerAppsStoreRO(region),
		nextToken,
		maxResults,
		clonePartnerApp,
		func(v *PartnerApp) string { return v.Arn },
	)
}

// CreatePartnerAppPresignedURL returns a one-time presigned URL for accessing
// an existing partner app.
func (b *InMemoryBackend) CreatePartnerAppPresignedURL(ctx context.Context, arnStr string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("CreatePartnerAppPresignedURL")
	defer b.mu.RUnlock()

	p, ok := b.partnerAppsStoreRO(region).Get(arnStr)
	if !ok {
		return "", fmt.Errorf("%w: partner app %q not found", ErrPartnerAppNotFound, arnStr)
	}

	return "https://" + p.Name + ".partner-app.sagemaker." + region +
		".amazonaws.com/auth?authToken=" + generateID(), nil
}
