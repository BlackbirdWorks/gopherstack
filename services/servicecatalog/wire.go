package servicecatalog

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// Paging carries the PageSize/PageToken request members shared by list operations.
type Paging struct {
	PageToken string `json:"PageToken,omitempty"`
	PageSize  int32  `json:"PageSize,omitempty"`
}

// Tag is a key-value pair.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// PortfolioDetail is the PortfolioDetail shape.
type PortfolioDetail struct {
	CreatedTime  *float64 `json:"CreatedTime,omitempty"`
	ARN          string   `json:"ARN"`
	Description  string   `json:"Description,omitempty"`
	DisplayName  string   `json:"DisplayName"`
	ID           string   `json:"Id"`
	ProviderName string   `json:"ProviderName"`
}

// ProductViewSummary is the ProductViewSummary shape.
type ProductViewSummary struct {
	Distributor        string `json:"Distributor,omitempty"`
	ID                 string `json:"Id"`
	Name               string `json:"Name"`
	Owner              string `json:"Owner,omitempty"`
	ProductID          string `json:"ProductId"`
	ShortDescription   string `json:"ShortDescription,omitempty"`
	SupportDescription string `json:"SupportDescription,omitempty"`
	SupportEmail       string `json:"SupportEmail,omitempty"`
	SupportURL         string `json:"SupportUrl,omitempty"`
	Type               string `json:"Type"`
	HasDefaultPath     bool   `json:"HasDefaultPath"`
}

// CodeStarParameters is the CodeStarParameters shape.
type CodeStarParameters struct {
	ArtifactPath  string `json:"ArtifactPath"`
	Branch        string `json:"Branch"`
	ConnectionArn string `json:"ConnectionArn"`
	Repository    string `json:"Repository"`
}

// SourceConnectionParameters is the SourceConnectionParameters shape.
type SourceConnectionParameters struct {
	CodeStar *CodeStarParameters `json:"CodeStar,omitempty"`
}

// SourceConnectionRequest is the SourceConnection request shape.
type SourceConnectionRequest struct {
	ConnectionParameters *SourceConnectionParameters `json:"ConnectionParameters,omitempty"`
	Type                 string                      `json:"Type,omitempty"`
}

// LastSync is the LastSync shape.
type LastSync struct {
	LastSuccessfulSyncTime *float64 `json:"LastSuccessfulSyncTime,omitempty"`
	LastSyncTime           *float64 `json:"LastSyncTime,omitempty"`
	LastSyncStatus         string   `json:"LastSyncStatus,omitempty"`
}

// SourceConnectionDetail is the SourceConnectionDetail shape.
type SourceConnectionDetail struct {
	ConnectionParameters *SourceConnectionParameters `json:"ConnectionParameters,omitempty"`
	LastSync             *LastSync                   `json:"LastSync,omitempty"`
	Type                 string                      `json:"Type,omitempty"`
}

// ProductViewDetail is the ProductViewDetail shape.
type ProductViewDetail struct {
	CreatedTime        *float64                `json:"CreatedTime,omitempty"`
	ProductViewSummary *ProductViewSummary     `json:"ProductViewSummary,omitempty"`
	SourceConnection   *SourceConnectionDetail `json:"SourceConnection,omitempty"`
	ProductARN         string                  `json:"ProductARN"`
	Status             string                  `json:"Status"`
}

// ProvisioningArtifactProperties is the ProvisioningArtifactProperties request shape.
type ProvisioningArtifactProperties struct {
	Info                      map[string]string `json:"Info,omitempty"`
	Description               string            `json:"Description,omitempty"`
	Name                      string            `json:"Name,omitempty"`
	Type                      string            `json:"Type,omitempty"`
	DisableTemplateValidation bool              `json:"DisableTemplateValidation,omitempty"`
}

// ProvisioningArtifactDetail is the ProvisioningArtifactDetail shape.
type ProvisioningArtifactDetail struct {
	Active         *bool    `json:"Active,omitempty"`
	CreatedTime    *float64 `json:"CreatedTime,omitempty"`
	Description    string   `json:"Description,omitempty"`
	Guidance       string   `json:"Guidance,omitempty"`
	ID             string   `json:"Id"`
	Name           string   `json:"Name,omitempty"`
	SourceRevision string   `json:"SourceRevision,omitempty"`
	Type           string   `json:"Type,omitempty"`
}

// ProvisioningArtifact is the end-user ProvisioningArtifact shape.
type ProvisioningArtifact struct {
	CreatedTime *float64 `json:"CreatedTime,omitempty"`
	Description string   `json:"Description,omitempty"`
	Guidance    string   `json:"Guidance,omitempty"`
	ID          string   `json:"Id"`
	Name        string   `json:"Name,omitempty"`
}

// ProvisioningArtifactSummary is the admin ProvisioningArtifactSummary shape.
type ProvisioningArtifactSummary struct {
	CreatedTime                  *float64          `json:"CreatedTime,omitempty"`
	ProvisioningArtifactMetadata map[string]string `json:"ProvisioningArtifactMetadata,omitempty"`
	Description                  string            `json:"Description,omitempty"`
	ID                           string            `json:"Id"`
	Name                         string            `json:"Name,omitempty"`
}

// ConstraintDetail is the ConstraintDetail shape.
type ConstraintDetail struct {
	ConstraintID string `json:"ConstraintId"`
	Description  string `json:"Description,omitempty"`
	Owner        string `json:"Owner,omitempty"`
	PortfolioID  string `json:"PortfolioId,omitempty"`
	ProductID    string `json:"ProductId,omitempty"`
	Type         string `json:"Type"`
}

// ConstraintSummary is the ConstraintSummary shape.
type ConstraintSummary struct {
	Description string `json:"Description,omitempty"`
	Type        string `json:"Type"`
}

// TagOptionDetail is the TagOptionDetail shape.
type TagOptionDetail struct {
	Active *bool  `json:"Active,omitempty"`
	ID     string `json:"Id"`
	Key    string `json:"Key"`
	Owner  string `json:"Owner,omitempty"`
	Value  string `json:"Value"`
}

// BudgetDetail is the BudgetDetail shape.
type BudgetDetail struct {
	BudgetName string `json:"BudgetName"`
}

// OrganizationNode is the OrganizationNode shape.
type OrganizationNode struct {
	Type  string `json:"Type,omitempty"`
	Value string `json:"Value,omitempty"`
}

// ServiceActionSummary is the ServiceActionSummary shape.
type ServiceActionSummary struct {
	DefinitionType string `json:"DefinitionType,omitempty"`
	Description    string `json:"Description,omitempty"`
	ID             string `json:"Id"`
	Name           string `json:"Name"`
}

// ServiceActionDetail is the ServiceActionDetail shape.
type ServiceActionDetail struct {
	Definition           map[string]string     `json:"Definition,omitempty"`
	ServiceActionSummary *ServiceActionSummary `json:"ServiceActionSummary,omitempty"`
}

// RecordTag is the RecordTag shape.
type RecordTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// RecordErrorWire is the RecordError shape.
type RecordErrorWire struct {
	Code        string `json:"Code"`
	Description string `json:"Description"`
}

// RecordDetail is the RecordDetail shape.
type RecordDetail struct {
	CreatedTime            *float64          `json:"CreatedTime,omitempty"`
	UpdatedTime            *float64          `json:"UpdatedTime,omitempty"`
	ProvisionedProductName string            `json:"ProvisionedProductName,omitempty"`
	PathID                 string            `json:"PathId,omitempty"`
	ProductID              string            `json:"ProductId,omitempty"`
	ProvisionedProductID   string            `json:"ProvisionedProductId,omitempty"`
	LaunchRoleArn          string            `json:"LaunchRoleArn,omitempty"`
	ProvisionedProductType string            `json:"ProvisionedProductType,omitempty"`
	ProvisioningArtifactID string            `json:"ProvisioningArtifactId,omitempty"`
	RecordID               string            `json:"RecordId"`
	RecordType             string            `json:"RecordType"`
	Status                 string            `json:"Status"`
	RecordErrors           []RecordErrorWire `json:"RecordErrors,omitempty"`
	RecordTags             []RecordTag       `json:"RecordTags,omitempty"`
}

// RecordOutput is the RecordOutput shape.
type RecordOutput struct {
	Description string `json:"Description,omitempty"`
	OutputKey   string `json:"OutputKey"`
	OutputValue string `json:"OutputValue"`
}

// AccessLevelFilter is the AccessLevelFilter shape.
type AccessLevelFilter struct {
	Key   string `json:"Key,omitempty"`
	Value string `json:"Value,omitempty"`
}

func ts(t time.Time) *float64 {
	if t.IsZero() {
		return nil
	}

	v := awstime.Epoch(t)

	return &v
}

//go:fix inline
func tagsToMap(tags []Tag) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

func mapToTags(m map[string]string) []Tag {
	if len(m) == 0 {
		return nil
	}

	out := make([]Tag, 0, len(m))
	for _, k := range sortedKeys(m) {
		out = append(out, Tag{Key: k, Value: m[k]})
	}

	return out
}

// pageOf slices all by PageSize/PageToken, rejecting out-of-range sizes and bad tokens.
func pageOf[T any](all []T, p Paging) (page.Page[T], error) {
	if p.PageSize < 0 || p.PageSize > maxPageSize {
		return page.Page[T]{}, invalid("PageSize must be between 1 and %d", maxPageSize)
	}

	if err := page.ValidateToken(p.PageToken); err != nil {
		return page.Page[T]{}, invalid("PageToken is not valid")
	}

	return page.New(all, p.PageToken, int(p.PageSize), maxPageSize), nil
}
