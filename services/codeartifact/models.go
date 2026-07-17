package codeartifact

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Domain represents an AWS CodeArtifact domain.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateDomain.
type Domain struct {
	CreatedTime    time.Time  `json:"createdTime"`
	Tags           *tags.Tags `json:"tags,omitempty"`
	Name           string     `json:"name"`
	ARN            string     `json:"arn"`
	EncryptionKey  string     `json:"encryptionKey,omitempty"`
	Owner          string     `json:"owner"`
	Region         string     `json:"region"`
	Status         string     `json:"status"`
	S3BucketARN    string     `json:"s3BucketArn,omitempty"`
	AssetSizeBytes int64      `json:"assetSizeBytes"`
}

// Repository represents an AWS CodeArtifact repository.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateRepository.
type Repository struct {
	CreatedTime          time.Time  `json:"createdTime"`
	Tags                 *tags.Tags `json:"tags,omitempty"`
	Name                 string     `json:"name"`
	ARN                  string     `json:"arn"`
	DomainName           string     `json:"domainName"`
	DomainOwner          string     `json:"domainOwner"`
	Description          string     `json:"description,omitempty"`
	AdministratorAccount string     `json:"administratorAccount"`
	Region               string     `json:"region"`
	UpstreamRepositories []string   `json:"upstreamRepositories,omitempty"`
}

// PackageGroup represents an AWS CodeArtifact package group.
type PackageGroup struct {
	CreatedTime time.Time  `json:"createdTime"`
	Tags        *tags.Tags `json:"tags,omitempty"`
	ARN         string     `json:"arn"`
	DomainName  string     `json:"domainName"`
	DomainOwner string     `json:"domainOwner"`
	Pattern     string     `json:"pattern"`
	Description string     `json:"description,omitempty"`
	ContactInfo string     `json:"contactInfo,omitempty"`
	// region is the store.Table composite-key qualifier (see regionKey); it
	// is never part of the wire API, so it carries no json tag and is
	// round-tripped separately through a DTO in persistence.go.
	region string
}

// Package represents an AWS CodeArtifact package (without versions).
type Package struct {
	DomainName  string `json:"domainName"`
	DomainOwner string `json:"domainOwner"`
	Repository  string `json:"repository"`
	Format      string `json:"format"`
	Namespace   string `json:"namespace,omitempty"`
	Name        string `json:"name"`
	// OriginConfigPublish and OriginConfigUpstream mirror
	// PackageOriginRestrictions.Publish/Upstream ("ALLOW" or "BLOCK"), set via
	// PutPackageOriginConfiguration. Both default to "ALLOW", matching a package
	// that has never had its origin configuration explicitly set.
	OriginConfigPublish  string `json:"originConfigPublish,omitempty"`
	OriginConfigUpstream string `json:"originConfigUpstream,omitempty"`
	// region is the store.Table composite-key qualifier (see regionKey).
	region string
}

// PackageVersion represents a single version of an AWS CodeArtifact package.
type PackageVersion struct {
	PublishedAt time.Time `json:"publishedAt"`
	DomainName  string    `json:"domainName"`
	Repository  string    `json:"repository"`
	Format      string    `json:"format"`
	Namespace   string    `json:"namespace,omitempty"`
	PackageName string    `json:"packageName"`
	Version     string    `json:"version"`
	Status      string    `json:"status"`
	Revision    string    `json:"revision"`
	region      string
	Assets      []AssetInfo `json:"assets,omitempty"`
}

// AssetInfo represents an asset (file) uploaded to a package version via
// PublishPackageVersion. Content holds the raw bytes so GetPackageVersionAsset can
// serve back exactly what was published, instead of always returning an empty stub.
type AssetInfo struct {
	Name    string `json:"name"`
	SHA256  string `json:"sha256"`
	Content []byte `json:"content,omitempty"`
	Size    int64  `json:"size"`
}

// ExternalConnection represents a connection of a repository to an external package source.
type ExternalConnection struct {
	ExternalConnectionName string `json:"externalConnectionName"`
	PackageFormat          string `json:"packageFormat"`
	Status                 string `json:"status"`
}

// RepositoryPermissionsPolicy represents a permissions policy attached to a repository.
type RepositoryPermissionsPolicy struct {
	Document    string `json:"document"`
	Revision    string `json:"revision"`
	ResourceARN string `json:"resourceArn"`
	// region, domainName, and repoName are the store.Table composite-key
	// qualifiers (see regionKey/repoKey); none is part of the wire API, so
	// each carries no json tag and is round-tripped separately through a DTO
	// in persistence.go.
	region     string
	domainName string
	repoName   string
}

// DomainPermissionsPolicy represents a permissions policy attached to a domain.
type DomainPermissionsPolicy struct {
	Document    string `json:"document"`
	Revision    string `json:"revision"`
	ResourceARN string `json:"resourceArn"`
	// region and domainName are the store.Table composite-key qualifiers
	// (see regionKey); see RepositoryPermissionsPolicy's comment above.
	region     string
	domainName string
}
