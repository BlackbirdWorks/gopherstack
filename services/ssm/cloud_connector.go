package ssm

// Cloud connector operations: Systems Manager Cloud Connectors establish a
// connection between Systems Manager and a third-party cloud environment
// (currently Microsoft Azure only). Wire shapes verified against
// aws-sdk-go-v2/service/ssm@v1.71.0's serializers.go/deserializers.go:
//   - CreatedAt/UpdatedAt are epoch-seconds JSON numbers (DateTime shape),
//     NOT ISO8601 strings -- matches the rest of this package's UnixTimeFloat
//     convention (see Activation).
//   - Configuration is a one-member (Azure-only) union, wire-wrapped by its
//     member's own field name "AzureConfiguration" -- modeled here as a
//     plain struct with an optional field rather than an interface-based
//     union, since this package only needs to (de)serialize JSON, not
//     replicate the SDK client's typed-union ergonomics.
//   - Errors: the SDK vends a generic types.ResourceNotFoundException (no
//     CloudConnector-specific error type exists), so Get/Delete/Update/
//     Validate on an unknown ID return "ResourceNotFoundException".
import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// AzureSubscription identifies one Azure subscription targeted by a cloud connector.
type AzureSubscription struct {
	ID          string `json:"Id"`
	DisplayName string `json:"DisplayName,omitempty"`
}

// ConfigurationTargets lists the Azure subscriptions targeted by a cloud connector.
type ConfigurationTargets struct {
	Subscriptions []AzureSubscription `json:"Subscriptions,omitempty"`
}

// AzureConfiguration holds the access details for connecting to a Microsoft
// Azure environment: the application registration used for authentication
// and the subscriptions to target.
type AzureConfiguration struct {
	Targets                *ConfigurationTargets `json:"Targets,omitempty"`
	ApplicationID          string                `json:"ApplicationId"`
	TenantID               string                `json:"TenantId"`
	ApplicationDisplayName string                `json:"ApplicationDisplayName,omitempty"`
	TenantDisplayName      string                `json:"TenantDisplayName,omitempty"`
}

// CloudConnectorConfiguration is the (currently Azure-only) configuration
// union for a cloud connector, wire-wrapped by member name.
type CloudConnectorConfiguration struct {
	AzureConfiguration *AzureConfiguration `json:"AzureConfiguration,omitempty"`
}

// CloudConnector is the internal record for a Systems Manager cloud connector.
type CloudConnector struct {
	Configuration      CloudConnectorConfiguration `json:"Configuration"`
	CloudConnectorID   string                      `json:"CloudConnectorId"`
	CloudConnectorArn  string                      `json:"CloudConnectorArn"`
	ConfigConnectorArn string                      `json:"ConfigConnectorArn"`
	Description        string                      `json:"Description,omitempty"`
	DisplayName        string                      `json:"DisplayName"`
	RoleArn            string                      `json:"RoleArn"`
	CreatedAt          float64                     `json:"CreatedAt"`
	UpdatedAt          float64                     `json:"UpdatedAt"`
}

// CloudConnectorSummary is the list-view projection of CloudConnector -- no
// ARNs, no Configuration (matches ListCloudConnectors' real response shape).
type CloudConnectorSummary struct {
	CloudConnectorID string  `json:"CloudConnectorId"`
	Description      string  `json:"Description,omitempty"`
	DisplayName      string  `json:"DisplayName"`
	RoleArn          string  `json:"RoleArn"`
	CreatedAt        float64 `json:"CreatedAt"`
	UpdatedAt        float64 `json:"UpdatedAt"`
}

func (c *CloudConnector) toSummary() CloudConnectorSummary {
	return CloudConnectorSummary{
		CloudConnectorID: c.CloudConnectorID,
		CreatedAt:        c.CreatedAt,
		Description:      c.Description,
		DisplayName:      c.DisplayName,
		RoleArn:          c.RoleArn,
		UpdatedAt:        c.UpdatedAt,
	}
}

// CreateCloudConnectorInput is the request payload for CreateCloudConnector.
type CreateCloudConnectorInput struct {
	ConfigConnectorArn string                      `json:"ConfigConnectorArn"`
	Configuration      CloudConnectorConfiguration `json:"Configuration"`
	Description        string                      `json:"Description,omitempty"`
	DisplayName        string                      `json:"DisplayName"`
	RoleArn            string                      `json:"RoleArn"`
	Tags               []Tag                       `json:"Tags,omitempty"`
}

// CreateCloudConnectorOutput is the response payload for CreateCloudConnector.
type CreateCloudConnectorOutput struct {
	CloudConnectorID string `json:"CloudConnectorId"`
}

// DeleteCloudConnectorInput is the request payload for DeleteCloudConnector.
type DeleteCloudConnectorInput struct {
	CloudConnectorID string `json:"CloudConnectorId"`
}

// DeleteCloudConnectorOutput is the response payload for DeleteCloudConnector.
type DeleteCloudConnectorOutput struct {
	CloudConnectorID string `json:"CloudConnectorId"`
}

// GetCloudConnectorInput is the request payload for GetCloudConnector.
type GetCloudConnectorInput struct {
	CloudConnectorID string `json:"CloudConnectorId"`
}

// GetCloudConnectorOutput is the response payload for GetCloudConnector.
type GetCloudConnectorOutput struct {
	Configuration      CloudConnectorConfiguration `json:"Configuration"`
	CloudConnectorArn  string                      `json:"CloudConnectorArn"`
	ConfigConnectorArn string                      `json:"ConfigConnectorArn"`
	Description        string                      `json:"Description,omitempty"`
	DisplayName        string                      `json:"DisplayName"`
	RoleArn            string                      `json:"RoleArn"`
	CreatedAt          float64                     `json:"CreatedAt"`
	UpdatedAt          float64                     `json:"UpdatedAt"`
}

// CloudConnectorFilter is a ListCloudConnectors filter (FilterKey: SubscriptionId | TenantId).
type CloudConnectorFilter struct {
	FilterKey    string   `json:"FilterKey"`
	FilterValues []string `json:"FilterValues,omitempty"`
}

// ListCloudConnectorsInput is the request payload for ListCloudConnectors.
type ListCloudConnectorsInput struct {
	MaxResults *int64                 `json:"MaxResults,omitempty"`
	NextToken  string                 `json:"NextToken,omitempty"`
	Filters    []CloudConnectorFilter `json:"Filters,omitempty"`
}

// ListCloudConnectorsOutput is the response payload for ListCloudConnectors.
type ListCloudConnectorsOutput struct {
	NextToken       string                  `json:"NextToken,omitempty"`
	CloudConnectors []CloudConnectorSummary `json:"CloudConnectors"`
}

// UpdateCloudConnectorInput is the request payload for UpdateCloudConnector.
type UpdateCloudConnectorInput struct {
	CloudConnectorID string                       `json:"CloudConnectorId"`
	Configuration    *CloudConnectorConfiguration `json:"Configuration,omitempty"`
	Description      string                       `json:"Description,omitempty"`
	DisplayName      string                       `json:"DisplayName,omitempty"`
}

// UpdateCloudConnectorOutput is the response payload for UpdateCloudConnector.
type UpdateCloudConnectorOutput struct {
	CloudConnectorID string `json:"CloudConnectorId"`
}

// ValidationFindingScope identifies the specific resource scope of a validation finding.
type ValidationFindingScope struct {
	ID   string `json:"Id,omitempty"`
	Type string `json:"Type,omitempty"`
}

// ValidationFinding describes one finding from ValidateCloudConnector.
type ValidationFinding struct {
	Code            string                  `json:"Code,omitempty"`
	Message         string                  `json:"Message,omitempty"`
	ProviderMessage string                  `json:"ProviderMessage,omitempty"`
	Scope           *ValidationFindingScope `json:"Scope,omitempty"`
	Type            string                  `json:"Type,omitempty"`
}

// ValidateCloudConnectorInput is the request payload for ValidateCloudConnector.
type ValidateCloudConnectorInput struct {
	CloudConnectorID string `json:"CloudConnectorId"`
	MaxResults       *int64 `json:"MaxResults,omitempty"`
	NextToken        string `json:"NextToken,omitempty"`
}

// ValidateCloudConnectorOutput is the response payload for ValidateCloudConnector.
type ValidateCloudConnectorOutput struct {
	NextToken          string              `json:"NextToken,omitempty"`
	ValidationFindings []ValidationFinding `json:"ValidationFindings"`
}

const (
	cloudConnectorIDPrefix = "cc-"
	// defaultCloudConnectorMaxResults bounds ListCloudConnectors/
	// ValidateCloudConnector pagination page size. AWS does not document an
	// exact bound for this newly-added API family; 50 matches the default
	// this package already uses for DescribeParameters/DescribeDocuments
	// (defaultDescribeMaxResults) rather than inventing an unverified number.
	defaultCloudConnectorMaxResults    = defaultDescribeMaxResults
	cloudConnectorFilterSubscriptionID = "SubscriptionId"
	cloudConnectorFilterTenantID       = "TenantId"
	// cloudConnectorFilterNone is the documented SubscriptionId filter value
	// ("NONE") used to return only tenant-level connectors (no subscription targets).
	cloudConnectorFilterNone = "NONE"
)

// ErrCloudConnectorNotFound is returned when a CloudConnectorId does not
// match any stored cloud connector. It maps to the SDK's generic
// ResourceNotFoundException (no CloudConnector-specific error type exists).
var ErrCloudConnectorNotFound = errors.New("ResourceNotFoundException")

// CreateCloudConnector creates a new cloud connector.
func (b *InMemoryBackend) CreateCloudConnector(
	ctx context.Context,
	input *CreateCloudConnectorInput,
) (*CreateCloudConnectorOutput, error) {
	if err := validateCreateCloudConnectorInput(input); err != nil {
		return nil, err
	}

	region := getRegion(ctx)
	b.mu.Lock("CreateCloudConnector")
	defer b.mu.Unlock()

	id := cloudConnectorIDPrefix + uuid.NewString()
	now := UnixTimeFloat(time.Now())

	cc := CloudConnector{
		CloudConnectorID:   id,
		CloudConnectorArn:  arn.Build("ssm", region, defaultAccountID, "cloudconnector/"+id),
		ConfigConnectorArn: input.ConfigConnectorArn,
		Configuration:      input.Configuration,
		CreatedAt:          now,
		Description:        input.Description,
		DisplayName:        input.DisplayName,
		RoleArn:            input.RoleArn,
		UpdatedAt:          now,
	}

	b.cloudConnectorsStore(region).Put(&cc)

	if len(input.Tags) > 0 {
		if b.miscResourceTags[region] == nil {
			b.miscResourceTags[region] = make(map[string]map[string]string)
		}

		miscTags := b.miscResourceTagsStore(region)
		if miscTags[id] == nil {
			miscTags[id] = make(map[string]string)
		}

		for _, t := range input.Tags {
			miscTags[id][t.Key] = t.Value
		}
	}

	return &CreateCloudConnectorOutput{CloudConnectorID: id}, nil
}

// validateCreateCloudConnectorInput enforces CreateCloudConnector's required fields.
func validateCreateCloudConnectorInput(input *CreateCloudConnectorInput) error {
	if input.ConfigConnectorArn == "" {
		return fmt.Errorf("%w: ConfigConnectorArn is required", ErrValidationException)
	}

	if input.DisplayName == "" {
		return fmt.Errorf("%w: DisplayName is required", ErrValidationException)
	}

	if input.RoleArn == "" {
		return fmt.Errorf("%w: RoleArn is required", ErrValidationException)
	}

	azure := input.Configuration.AzureConfiguration
	if azure == nil {
		return fmt.Errorf("%w: Configuration.AzureConfiguration is required", ErrValidationException)
	}

	if azure.ApplicationID == "" {
		return fmt.Errorf(
			"%w: Configuration.AzureConfiguration.ApplicationId is required", ErrValidationException,
		)
	}

	if azure.TenantID == "" {
		return fmt.Errorf("%w: Configuration.AzureConfiguration.TenantId is required", ErrValidationException)
	}

	return nil
}

// DeleteCloudConnector deletes a cloud connector.
func (b *InMemoryBackend) DeleteCloudConnector(
	ctx context.Context,
	input *DeleteCloudConnectorInput,
) (*DeleteCloudConnectorOutput, error) {
	if input.CloudConnectorID == "" {
		return nil, fmt.Errorf("%w: CloudConnectorId is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.Lock("DeleteCloudConnector")
	defer b.mu.Unlock()

	table := b.cloudConnectorsStore(region)
	if !table.Has(input.CloudConnectorID) {
		return nil, ErrCloudConnectorNotFound
	}

	table.Delete(input.CloudConnectorID)
	delete(b.miscResourceTagsStore(region), input.CloudConnectorID)

	return &DeleteCloudConnectorOutput{CloudConnectorID: input.CloudConnectorID}, nil
}

// GetCloudConnector returns detailed information about a cloud connector.
func (b *InMemoryBackend) GetCloudConnector(
	ctx context.Context,
	input *GetCloudConnectorInput,
) (*GetCloudConnectorOutput, error) {
	if input.CloudConnectorID == "" {
		return nil, fmt.Errorf("%w: CloudConnectorId is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.RLock("GetCloudConnector")
	defer b.mu.RUnlock()

	cc, ok := b.cloudConnectorsStore(region).Get(input.CloudConnectorID)
	if !ok {
		return nil, ErrCloudConnectorNotFound
	}

	return &GetCloudConnectorOutput{
		CloudConnectorArn:  cc.CloudConnectorArn,
		ConfigConnectorArn: cc.ConfigConnectorArn,
		Configuration:      cc.Configuration,
		CreatedAt:          cc.CreatedAt,
		Description:        cc.Description,
		DisplayName:        cc.DisplayName,
		RoleArn:            cc.RoleArn,
		UpdatedAt:          cc.UpdatedAt,
	}, nil
}

// ListCloudConnectors returns cloud connectors in the current account/region,
// optionally filtered by SubscriptionId/TenantId, paginated via the same
// opaque index-token scheme DescribeParameters/DescribeDocuments use.
func (b *InMemoryBackend) ListCloudConnectors(
	ctx context.Context,
	input *ListCloudConnectorsInput,
) (*ListCloudConnectorsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListCloudConnectors")
	defer b.mu.RUnlock()

	var filtered []CloudConnectorSummary

	for _, cc := range b.cloudConnectorsStore(region).Snapshot() {
		if cloudConnectorMatchesFilters(cc, input.Filters) {
			filtered = append(filtered, cc.toSummary())
		}
	}

	startIdx := parseNextToken(input.NextToken)

	maxResults := int64(defaultCloudConnectorMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(filtered) {
		return &ListCloudConnectorsOutput{CloudConnectors: []CloudConnectorSummary{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(filtered) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(filtered)
	}

	return &ListCloudConnectorsOutput{
		CloudConnectors: filtered[startIdx:end],
		NextToken:       nextToken,
	}, nil
}

// cloudConnectorMatchesFilters reports whether cc satisfies every filter (AND semantics).
func cloudConnectorMatchesFilters(cc *CloudConnector, filters []CloudConnectorFilter) bool {
	for _, f := range filters {
		if !cloudConnectorMatchesFilter(cc, f) {
			return false
		}
	}

	return true
}

// cloudConnectorMatchesFilter implements one ListCloudConnectors filter. Per
// the SDK's documented semantics: TenantId matches connectors targeting that
// tenant; SubscriptionId matches connectors with a targeted subscription in
// FilterValues, or (when FilterValues contains the literal "NONE") connectors
// with no subscription targets at all (tenant-level connectors).
func cloudConnectorMatchesFilter(cc *CloudConnector, f CloudConnectorFilter) bool {
	azure := cc.Configuration.AzureConfiguration

	switch f.FilterKey {
	case cloudConnectorFilterTenantID:
		return azure != nil && slices.Contains(f.FilterValues, azure.TenantID)
	case cloudConnectorFilterSubscriptionID:
		subs := cloudConnectorSubscriptionIDs(azure)
		for _, v := range f.FilterValues {
			if v == cloudConnectorFilterNone && len(subs) == 0 {
				return true
			}

			if slices.Contains(subs, v) {
				return true
			}
		}

		return false
	default:
		return true
	}
}

// cloudConnectorSubscriptionIDs returns the subscription IDs targeted by an
// Azure cloud connector configuration, or nil for a tenant-level connector.
func cloudConnectorSubscriptionIDs(azure *AzureConfiguration) []string {
	if azure == nil || azure.Targets == nil {
		return nil
	}

	ids := make([]string, 0, len(azure.Targets.Subscriptions))
	for _, s := range azure.Targets.Subscriptions {
		ids = append(ids, s.ID)
	}

	return ids
}

// UpdateCloudConnector updates an existing cloud connector's configuration/description/name.
func (b *InMemoryBackend) UpdateCloudConnector(
	ctx context.Context,
	input *UpdateCloudConnectorInput,
) (*UpdateCloudConnectorOutput, error) {
	if input.CloudConnectorID == "" {
		return nil, fmt.Errorf("%w: CloudConnectorId is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.Lock("UpdateCloudConnector")
	defer b.mu.Unlock()

	table := b.cloudConnectorsStore(region)

	existing, ok := table.Get(input.CloudConnectorID)
	if !ok {
		return nil, ErrCloudConnectorNotFound
	}

	updated := *existing
	if input.Configuration != nil {
		updated.Configuration = *input.Configuration
	}

	if input.Description != "" {
		updated.Description = input.Description
	}

	if input.DisplayName != "" {
		updated.DisplayName = input.DisplayName
	}

	updated.UpdatedAt = UnixTimeFloat(time.Now())

	table.Put(&updated)

	return &UpdateCloudConnectorOutput{CloudConnectorID: input.CloudConnectorID}, nil
}

// ValidateCloudConnector validates a cloud connector's configuration and
// connectivity. gopherstack has no real Azure tenant to call out to, so the
// returned findings are derived deterministically from the connector's own
// persisted configuration (tenant/subscription IDs) rather than a fabricated
// always-success response: a connector missing its Azure tenant configuration
// reports an ERROR finding, and a correctly configured one reports one INFO
// finding per targeted scope (tenant, then each subscription).
func (b *InMemoryBackend) ValidateCloudConnector(
	ctx context.Context,
	input *ValidateCloudConnectorInput,
) (*ValidateCloudConnectorOutput, error) {
	if input.CloudConnectorID == "" {
		return nil, fmt.Errorf("%w: CloudConnectorId is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.RLock("ValidateCloudConnector")
	defer b.mu.RUnlock()

	cc, ok := b.cloudConnectorsStore(region).Get(input.CloudConnectorID)
	if !ok {
		return nil, ErrCloudConnectorNotFound
	}

	findings := cloudConnectorValidationFindings(cc)

	startIdx := parseNextToken(input.NextToken)

	maxResults := int64(defaultCloudConnectorMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(findings) {
		return &ValidateCloudConnectorOutput{ValidationFindings: []ValidationFinding{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(findings) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(findings)
	}

	return &ValidateCloudConnectorOutput{
		ValidationFindings: findings[startIdx:end],
		NextToken:          nextToken,
	}, nil
}

// cloudConnectorValidationFindings derives a deterministic set of validation
// findings from cc's own stored configuration.
func cloudConnectorValidationFindings(cc *CloudConnector) []ValidationFinding {
	azure := cc.Configuration.AzureConfiguration
	if azure == nil || azure.TenantID == "" {
		return []ValidationFinding{{
			Code:    "TargetInaccessible",
			Message: "cloud connector has no configured Azure tenant",
			Type:    "ERROR",
		}}
	}

	findings := []ValidationFinding{{
		Code:    "TenantSummary",
		Message: fmt.Sprintf("tenant %s is reachable", azure.TenantID),
		Type:    "INFO",
		Scope:   &ValidationFindingScope{ID: azure.TenantID, Type: "azure:tenant"},
	}}

	for _, id := range cloudConnectorSubscriptionIDs(azure) {
		findings = append(findings, ValidationFinding{
			Code:    "SubscriptionAccessible",
			Message: fmt.Sprintf("subscription %s is accessible", id),
			Type:    "INFO",
			Scope:   &ValidationFindingScope{ID: id, Type: "azure:subscription"},
		})
	}

	return findings
}

func (b *InMemoryBackend) cloudConnectorsStore(region string) *store.Table[CloudConnector] {
	return getOrCreateTable(b, b.cloudConnectors, "cloudConnectors", region, cloudConnectorKeyFn)
}
