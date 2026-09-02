package glue

import (
	"sort"
	"strings"

	sdktypes "github.com/aws/aws-sdk-go-v2/service/glue/types"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Connection-type categories and capabilities, factored into constants so the
// built-in table below does not repeat string literals.

const categoryDatabase = "DATABASE"

const categorySaaS = "SAAS"

const categoryStreaming = "STREAMING"

const categoryNetwork = "NETWORK"

const categoryMarketplace = "MARKETPLACE"

const categoryCustom = "CUSTOM"

const capRead = "READ"

const capWrite = "WRITE"

// ErrConnectionTypeBuiltIn is returned when a caller attempts to delete a built-in
// (AWS-managed) connection type. Built-in types are undeletable, mirroring AWS, which
// rejects mutation of managed connector types. It carries a distinct AccessDenied
// shape (see handleError) so it is not conflated with EntityNotFound/Validation.
var ErrConnectionTypeBuiltIn = awserr.New(
	"connection type is a built-in AWS-managed type and cannot be deleted",
	awserr.ErrConflict,
)

// rwCaps returns the READ/WRITE capability set shared by most connectors.
func rwCaps() []string { return []string{capRead, capWrite} }

// readCaps returns the read-only capability set used by source-only SaaS connectors.
func readCaps() []string { return []string{capRead} }

// databaseConnectors are the read/write built-in database/warehouse connectors.
func databaseConnectors() []string {
	return []string{
		"JDBC", "MONGODB", "SNOWFLAKE", "BIGQUERY", "DYNAMODB", "OPENSEARCH",
		"AZURECOSMOS", "AZURESQL", "VERTICA", "SAPHANA", "TERADATA", "REDSHIFT",
	}
}

// saasConnectors are the read-only built-in SaaS source connectors.
func saasConnectors() []string {
	return []string{
		"SALESFORCE", "GOOGLEADS", "SERVICENOW", "ZENDESK", "HUBSPOT", "FACEBOOKADS",
		"INSTAGRAMADS", "MARKETO", "SAPODATA", "SLACK", "JIRACLOUD", "STRIPE",
		"INTERCOM", "PIPEDRIVE", "SALESFORCEPARDOT", "SALESFORCEMARKETING",
	}
}

// builtInConnectionTypes returns the set of AWS-managed Glue connection types keyed by
// canonical name. Callers may not delete these; they always appear in
// ListConnectionTypes. The list mirrors the connectors AWS Glue exposes through its
// connection framework. It is a function (not a package var) to stay lint-clean
// without a global.
func builtInConnectionTypes() map[string]ConnectionTypeInfo {
	out := make(map[string]ConnectionTypeInfo)

	add := func(name, category string, caps []string) {
		out[name] = ConnectionTypeInfo{
			ConnectionType: name,
			Description:    name + " connector",
			Category:       category,
			Capabilities:   caps,
			BuiltIn:        true,
		}
	}

	for _, name := range databaseConnectors() {
		add(name, categoryDatabase, rwCaps())
	}

	for _, name := range saasConnectors() {
		add(name, categorySaaS, readCaps())
	}

	add("KAFKA", categoryStreaming, readCaps())
	add(categoryNetwork, categoryNetwork, nil)
	add(categoryMarketplace, categoryMarketplace, nil)
	add(categoryCustom, categoryCustom, nil)

	return out
}

// normalizeConnectionType upper-cases a connection type so lookups are
// case-insensitive, matching AWS's treatment of connector-type identifiers.
func normalizeConnectionType(name string) string {
	return strings.ToUpper(strings.TrimSpace(name))
}

// builtInConnectionType returns the built-in definition for name (case-insensitive)
// and whether it exists.
func builtInConnectionType(name string) (ConnectionTypeInfo, bool) {
	info, ok := builtInConnectionTypes()[normalizeConnectionType(name)]

	return info, ok
}

// RegisterConnectionTypeSpec carries RegisterConnectionType's required
// members beyond name/description (glue@v1.152.0
// api_op_RegisterConnectionType.go:38-70: ConnectionProperties,
// ConnectorAuthenticationConfiguration, IntegrationType and RestConfiguration
// are all "This member is required").
type RegisterConnectionTypeSpec struct {
	ConnectionProperties                 map[string]any
	ConnectorAuthenticationConfiguration map[string]any
	RestConfiguration                    map[string]any
	IntegrationType                      string
}

// RegisterConnectionType registers a custom connection type, returning the stored
// info. Registering a name that collides with a built-in type is rejected (AWS
// reserves managed connector names); re-registering an existing custom type updates
// its description, matching AWS's idempotent register semantics.
func (b *InMemoryBackend) RegisterConnectionType(
	name, description string, spec RegisterConnectionTypeSpec,
) (*ConnectionTypeInfo, error) {
	norm := normalizeConnectionType(name)
	if norm == "" {
		return nil, awserr.New("ConnectionType is required", awserr.ErrInvalidParameter)
	}

	if spec.IntegrationType == "" {
		return nil, awserr.New("IntegrationType is required", awserr.ErrInvalidParameter)
	}

	if spec.IntegrationType != string(sdktypes.IntegrationTypeRest) {
		return nil, awserr.Newf("invalid IntegrationType: %q", awserr.ErrInvalidParameter, spec.IntegrationType)
	}

	if spec.ConnectionProperties == nil {
		return nil, awserr.New("ConnectionProperties is required", awserr.ErrInvalidParameter)
	}

	if spec.ConnectorAuthenticationConfiguration == nil {
		return nil, awserr.New("ConnectorAuthenticationConfiguration is required", awserr.ErrInvalidParameter)
	}

	if spec.ConnectorAuthenticationConfiguration["AuthenticationTypes"] == nil {
		return nil, awserr.New(
			"ConnectorAuthenticationConfiguration.AuthenticationTypes is required", awserr.ErrInvalidParameter,
		)
	}

	if spec.RestConfiguration == nil {
		return nil, awserr.New("RestConfiguration is required", awserr.ErrInvalidParameter)
	}

	if _, ok := builtInConnectionType(norm); ok {
		return nil, awserr.New(
			"connection type "+norm+" is a reserved built-in type",
			awserr.ErrAlreadyExists,
		)
	}

	b.mu.Lock("RegisterConnectionType")
	defer b.mu.Unlock()

	info := &ConnectionTypeInfo{
		ConnectionType:                       norm,
		Description:                          description,
		Category:                             categoryCustom,
		Capabilities:                         rwCaps(),
		BuiltIn:                              false,
		ConnectionTypeArn:                    arn.Build("glue", b.region, b.accountID, "connectionType/"+norm),
		IntegrationType:                      spec.IntegrationType,
		ConnectionProperties:                 spec.ConnectionProperties,
		ConnectorAuthenticationConfiguration: spec.ConnectorAuthenticationConfiguration,
		RestConfiguration:                    spec.RestConfiguration,
	}
	b.customConnectionTypes.Put(info)

	clone := *info

	return &clone, nil
}

// DeleteConnectionType removes a custom connection type. Deleting a built-in type
// returns ErrConnectionTypeBuiltIn (undeletable); deleting an unknown type returns
// ErrNotFound. This replaces the previous no-op that always reported success.
func (b *InMemoryBackend) DeleteConnectionType(name string) error {
	norm := normalizeConnectionType(name)
	if norm == "" {
		return awserr.New("ConnectionType is required", awserr.ErrInvalidParameter)
	}

	if _, ok := builtInConnectionType(norm); ok {
		return ErrConnectionTypeBuiltIn
	}

	b.mu.Lock("DeleteConnectionType")
	defer b.mu.Unlock()

	if !b.customConnectionTypes.Has(norm) {
		return awserr.New("connection type "+norm+" not found", awserr.ErrNotFound)
	}

	b.customConnectionTypes.Delete(norm)

	return nil
}

// DescribeConnectionType returns the info for a built-in or registered custom
// type. Its error switch has no EntityNotFoundException case, unlike
// DeleteConnectionType's, so an unknown ConnectionType surfaces as
// InvalidInputException.
func (b *InMemoryBackend) DescribeConnectionType(name string) (*ConnectionTypeInfo, error) {
	norm := normalizeConnectionType(name)
	if norm == "" {
		return nil, awserr.New("ConnectionType is required", awserr.ErrInvalidParameter)
	}

	if info, ok := builtInConnectionType(norm); ok {
		return &info, nil
	}

	b.mu.RLock("DescribeConnectionType")
	defer b.mu.RUnlock()

	if info, ok := b.customConnectionTypes.Get(norm); ok {
		clone := *info

		return &clone, nil
	}

	return nil, awserr.New("connection type "+norm+" not found", awserr.ErrInvalidParameter)
}

// ListConnectionTypes returns all built-in and registered custom connection types
// sorted by name.
func (b *InMemoryBackend) ListConnectionTypes() []*ConnectionTypeInfo {
	byName := builtInConnectionTypes()

	b.mu.RLock("ListConnectionTypes")
	for _, info := range b.customConnectionTypes.All() {
		byName[info.ConnectionType] = *info
	}
	b.mu.RUnlock()

	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}

	sort.Strings(names)

	out := make([]*ConnectionTypeInfo, 0, len(names))
	for _, name := range names {
		info := byName[name]
		out = append(out, &info)
	}

	return out
}
