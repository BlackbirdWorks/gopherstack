package glue

import (
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Connection-type categories and capabilities, factored into constants so the
// built-in table below does not repeat string literals.
const (
	categoryDatabase    = "DATABASE"
	categorySaaS        = "SAAS"
	categoryStreaming   = "STREAMING"
	categoryNetwork     = "NETWORK"
	categoryMarketplace = "MARKETPLACE"
	categoryCustom      = "CUSTOM"

	capRead  = "READ"
	capWrite = "WRITE"
)

// ErrConnectionTypeBuiltIn is returned when a caller attempts to delete a built-in
// (AWS-managed) connection type. Built-in types are undeletable, mirroring AWS, which
// rejects mutation of managed connector types. It carries a distinct AccessDenied
// shape (see handleError) so it is not conflated with EntityNotFound/Validation.
var ErrConnectionTypeBuiltIn = awserr.New(
	"connection type is a built-in AWS-managed type and cannot be deleted",
	awserr.ErrConflict,
)

// ConnectionTypeInfo describes a Glue connection type (connector). BuiltIn types are
// AWS-managed and undeletable; custom types are registered via RegisterConnectionType.
type ConnectionTypeInfo struct {
	// ConnectionType is the canonical connector name (e.g. "JDBC", "SALESFORCE").
	ConnectionType string `json:"ConnectionType"`
	// Description is a human-readable description of the connector.
	Description string `json:"Description,omitempty"`
	// Category groups connectors (e.g. "DATABASE", "SAAS", "STREAMING").
	Category string `json:"Category,omitempty"`
	// Capabilities lists supported connector capabilities.
	Capabilities []string `json:"Capabilities,omitempty"`
	// BuiltIn reports whether this is an AWS-managed (undeletable) type.
	BuiltIn bool `json:"BuiltIn"`
}

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

// RegisterConnectionType registers a custom connection type, returning the stored
// info. Registering a name that collides with a built-in type is rejected (AWS
// reserves managed connector names); re-registering an existing custom type updates
// its description, matching AWS's idempotent register semantics.
func (b *InMemoryBackend) RegisterConnectionType(name, description string) (*ConnectionTypeInfo, error) {
	norm := normalizeConnectionType(name)
	if norm == "" {
		return nil, awserr.New("ConnectionType is required", awserr.ErrInvalidParameter)
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
		ConnectionType: norm,
		Description:    description,
		Category:       categoryCustom,
		Capabilities:   rwCaps(),
		BuiltIn:        false,
	}
	b.customConnectionTypes[norm] = info

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

	if _, ok := b.customConnectionTypes[norm]; !ok {
		return awserr.New("connection type "+norm+" not found", awserr.ErrNotFound)
	}

	delete(b.customConnectionTypes, norm)

	return nil
}

// DescribeConnectionType returns the info for a built-in or registered custom type,
// or ErrNotFound when the type is unknown.
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

	if info, ok := b.customConnectionTypes[norm]; ok {
		clone := *info

		return &clone, nil
	}

	return nil, awserr.New("connection type "+norm+" not found", awserr.ErrNotFound)
}

// ListConnectionTypes returns all built-in and registered custom connection types
// sorted by name.
func (b *InMemoryBackend) ListConnectionTypes() []*ConnectionTypeInfo {
	byName := builtInConnectionTypes()

	b.mu.RLock("ListConnectionTypes")
	for name, info := range b.customConnectionTypes {
		byName[name] = *info
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
