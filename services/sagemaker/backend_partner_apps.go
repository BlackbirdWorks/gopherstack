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

// PartnerApp represents a SageMaker partner app.
type PartnerApp struct {
	CreationTime      time.Time         `json:"CreationTime"`
	LastModifiedTime  time.Time         `json:"LastModifiedTime"`
	Tags              map[string]string `json:"Tags,omitempty"`
	Name              string            `json:"Name"`
	Arn               string            `json:"Arn"`
	Status            string            `json:"Status"`
	Type              string            `json:"Type,omitempty"`
	ExecutionRoleArn  string            `json:"ExecutionRoleArn,omitempty"`
	AuthType          string            `json:"AuthType,omitempty"`
	Tier              string            `json:"Tier,omitempty"`
	ApplicationConfig json.RawMessage   `json:"ApplicationConfig,omitempty"`
}

func clonePartnerApp(p *PartnerApp) *PartnerApp {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)
	cp.ApplicationConfig = append(json.RawMessage(nil), p.ApplicationConfig...)

	return &cp
}

// CreatePartnerAppOptions holds the fields accepted by CreatePartnerApp.
type CreatePartnerAppOptions struct {
	Tags              map[string]string
	Name              string
	Type              string
	ExecutionRoleArn  string
	AuthType          string
	Tier              string
	ApplicationConfig json.RawMessage
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
		Name:              opts.Name,
		Arn:               appARN,
		Status:            "Available",
		Type:              opts.Type,
		ExecutionRoleArn:  opts.ExecutionRoleArn,
		AuthType:          opts.AuthType,
		Tier:              opts.Tier,
		ApplicationConfig: opts.ApplicationConfig,
		Tags:              mergeTags(nil, opts.Tags),
		CreationTime:      now,
		LastModifiedTime:  now,
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

	return clonePartnerApp(p), nil
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

// UpdatePartnerAppOptions holds the mutable fields accepted by UpdatePartnerApp.
type UpdatePartnerAppOptions struct {
	Arn               string
	Tier              string
	ApplicationConfig json.RawMessage
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

	p.LastModifiedTime = time.Now()

	return clonePartnerApp(p), nil
}

// ListPartnerApps returns a page of partner apps.
func (b *InMemoryBackend) ListPartnerApps(ctx context.Context, nextToken string) ([]*PartnerApp, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListPartnerApps")
	defer b.mu.RUnlock()

	return sagemakerListKeyPaged(
		b.partnerAppsStoreRO(region),
		nextToken,
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
