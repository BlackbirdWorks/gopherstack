package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrDomainNotFound is returned when a domain does not exist.
	ErrDomainNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrDomainAlreadyExists is returned when a domain already exists.
	ErrDomainAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// Domain represents a SageMaker Studio domain. DefaultUserSettings/
// DefaultSpaceSettings/DomainSettings are stored as opaque JSON (the
// json.RawMessage passthrough convention already used by algorithms.go and
// the parity-4 AI-job families for deeply-nested union/config shapes) — the
// client's Create payload is echoed back verbatim on Describe, wire-accurate
// for every field the client actually sent.
type Domain struct {
	CreationTime               time.Time         `json:"CreationTime"`
	LastModifiedTime           time.Time         `json:"LastModifiedTime"`
	Tags                       map[string]string `json:"Tags,omitempty"`
	DomainID                   string            `json:"DomainId"`
	DomainArn                  string            `json:"DomainArn"`
	DomainName                 string            `json:"DomainName"`
	Status                     string            `json:"Status"`
	URL                        string            `json:"Url,omitempty"`
	AuthMode                   string            `json:"AuthMode,omitempty"`
	AppNetworkAccessType       string            `json:"AppNetworkAccessType,omitempty"`
	AppSecurityGroupManagement string            `json:"AppSecurityGroupManagement,omitempty"`
	HomeEfsFileSystemCreation  string            `json:"HomeEfsFileSystemCreation,omitempty"`
	KmsKeyID                   string            `json:"KmsKeyId,omitempty"`
	VpcID                      string            `json:"VpcId,omitempty"`
	TagPropagation             string            `json:"TagPropagation,omitempty"`
	SubnetIDs                  []string          `json:"SubnetIds,omitempty"`
	DefaultUserSettings        json.RawMessage   `json:"DefaultUserSettings,omitempty"`
	DefaultSpaceSettings       json.RawMessage   `json:"DefaultSpaceSettings,omitempty"`
	DomainSettings             json.RawMessage   `json:"DomainSettings,omitempty"`
}

func cloneDomain(d *Domain) *Domain {
	cp := *d
	cp.Tags = maps.Clone(d.Tags)
	cp.SubnetIDs = append([]string(nil), d.SubnetIDs...)
	cp.DefaultUserSettings = append(json.RawMessage(nil), d.DefaultUserSettings...)
	cp.DefaultSpaceSettings = append(json.RawMessage(nil), d.DefaultSpaceSettings...)
	cp.DomainSettings = append(json.RawMessage(nil), d.DomainSettings...)

	return &cp
}

// CreateDomainOptions bundles CreateDomain's fields beyond the always-required
// DomainName/AuthMode/Tags trio the backend already took as positional
// params — named per this file's own precedent (CreateDeviceFleetOptions,
// ListAssociationsParams) rather than growing the positional signature.
type CreateDomainOptions struct {
	AppNetworkAccessType       string
	AppSecurityGroupManagement string
	HomeEfsFileSystemCreation  string
	KmsKeyID                   string
	VpcID                      string
	TagPropagation             string
	SubnetIDs                  []string
	DefaultUserSettings        json.RawMessage
	DefaultSpaceSettings       json.RawMessage
	DomainSettings             json.RawMessage
}

// CreateDomain creates a new SageMaker Studio domain.
func (b *InMemoryBackend) CreateDomain(
	ctx context.Context,
	name, authMode string,
	tags map[string]string,
	opts CreateDomainOptions,
) (*Domain, error) {
	b.mu.Lock("CreateDomain")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	for _, d := range b.domainsStore(region).All() {
		if d.DomainName == name {
			return nil, fmt.Errorf("%w: domain %s already exists", ErrDomainAlreadyExists, name)
		}
	}

	id := fmt.Sprintf("d-%s", generateID())
	domainArn := arn.Build("sagemaker", region, b.accountID, "domain/"+id)
	now := time.Now()

	d := &Domain{
		DomainID:                   id,
		DomainArn:                  domainArn,
		DomainName:                 name,
		AuthMode:                   authMode,
		Status:                     statusInService,
		URL:                        fmt.Sprintf("https://%s.studio.%s.sagemaker.aws", id, region),
		CreationTime:               now,
		LastModifiedTime:           now,
		Tags:                       mergeTags(nil, tags),
		AppNetworkAccessType:       opts.AppNetworkAccessType,
		AppSecurityGroupManagement: opts.AppSecurityGroupManagement,
		HomeEfsFileSystemCreation:  opts.HomeEfsFileSystemCreation,
		KmsKeyID:                   opts.KmsKeyID,
		VpcID:                      opts.VpcID,
		TagPropagation:             opts.TagPropagation,
		SubnetIDs:                  opts.SubnetIDs,
		DefaultUserSettings:        opts.DefaultUserSettings,
		DefaultSpaceSettings:       opts.DefaultSpaceSettings,
		DomainSettings:             opts.DomainSettings,
	}
	b.domainsStore(region).Put(d)

	return cloneDomain(d), nil
}

// DescribeDomain returns a domain by ID or name.
func (b *InMemoryBackend) DescribeDomain(ctx context.Context, idOrName string) (*Domain, error) {
	b.mu.RLock("DescribeDomain")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if d, ok := b.domainsStoreRO(region).Get(idOrName); ok {
		return cloneDomain(d), nil
	}

	for _, d := range b.domainsStoreRO(region).All() {
		if d.DomainName == idOrName {
			return cloneDomain(d), nil
		}
	}

	return nil, fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, idOrName)
}

// ListDomains returns all domains sorted by name, capped at maxResults (if
// positive) per page — ListDomainsInput.MaxResults is a real client-facing
// field (default 10 in the real API); previously always ignored in favor of
// the fixed sagemakerDefaultPageSize.
func (b *InMemoryBackend) ListDomains(ctx context.Context, nextToken string, maxResults int32) ([]*Domain, string) {
	b.mu.RLock("ListDomains")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	all := b.domainsStoreRO(region).All()
	list := make([]*Domain, 0, len(all))

	for _, d := range all {
		list = append(list, cloneDomain(d))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].DomainName < list[j].DomainName })

	return paginateSlice(list, nextToken, maxResults)
}

// DeleteDomain deletes a domain by ID or name.
func (b *InMemoryBackend) DeleteDomain(ctx context.Context, idOrName string) error {
	b.mu.Lock("DeleteDomain")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.domainsStore(region)

	for _, d := range store.All() {
		if d.DomainID == idOrName || d.DomainName == idOrName {
			store.Delete(d.DomainID)

			return nil
		}
	}

	return fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, idOrName)
}

// UpdateDomainOptions bundles UpdateDomain's optional fields. Every field is
// applied only when non-zero/non-nil — UpdateDomainInput is a partial update,
// not a full replace, so an unset field must leave the existing value alone
// rather than being zeroed.
type UpdateDomainOptions struct {
	AppNetworkAccessType       string
	AppSecurityGroupManagement string
	HomeEfsFileSystemCreation  string
	TagPropagation             string
	VpcID                      string
	SubnetIDs                  []string
	DefaultUserSettings        json.RawMessage
	DefaultSpaceSettings       json.RawMessage
	DomainSettingsForUpdate    json.RawMessage
}

// applyUpdateDomainOptions overwrites d's overridable fields with any
// non-zero/non-nil value in opts, leaving the rest untouched — split out of
// UpdateDomain to keep that function's cognitive complexity down.
func applyUpdateDomainOptions(d *Domain, opts UpdateDomainOptions) {
	if opts.AppNetworkAccessType != "" {
		d.AppNetworkAccessType = opts.AppNetworkAccessType
	}

	if opts.AppSecurityGroupManagement != "" {
		d.AppSecurityGroupManagement = opts.AppSecurityGroupManagement
	}

	if opts.HomeEfsFileSystemCreation != "" {
		d.HomeEfsFileSystemCreation = opts.HomeEfsFileSystemCreation
	}

	if opts.TagPropagation != "" {
		d.TagPropagation = opts.TagPropagation
	}

	if opts.VpcID != "" {
		d.VpcID = opts.VpcID
	}

	if opts.SubnetIDs != nil {
		d.SubnetIDs = opts.SubnetIDs
	}

	if opts.DefaultUserSettings != nil {
		d.DefaultUserSettings = opts.DefaultUserSettings
	}

	if opts.DefaultSpaceSettings != nil {
		d.DefaultSpaceSettings = opts.DefaultSpaceSettings
	}

	if opts.DomainSettingsForUpdate != nil {
		d.DomainSettings = opts.DomainSettingsForUpdate
	}
}

// UpdateDomain updates a domain's overridable settings.
func (b *InMemoryBackend) UpdateDomain(
	ctx context.Context, idOrName string, opts UpdateDomainOptions,
) (*Domain, error) {
	b.mu.Lock("UpdateDomain")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	for _, d := range b.domainsStore(region).All() {
		if d.DomainID != idOrName && d.DomainName != idOrName {
			continue
		}

		applyUpdateDomainOptions(d, opts)
		d.LastModifiedTime = time.Now()

		return cloneDomain(d), nil
	}

	return nil, fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, idOrName)
}
