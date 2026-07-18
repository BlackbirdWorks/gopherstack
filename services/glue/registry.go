package glue

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// PutSchemaVersionMetadata stores a key-value metadata pair for a schema
// version. schemaVersionID must be a valid version ID registered via
// RegisterSchemaVersion.
func (b *InMemoryBackend) PutSchemaVersionMetadata(schemaVersionID, key, value string) error {
	if schemaVersionID == "" || key == "" {
		return fmt.Errorf("schemaVersionID and key are required: %w", ErrValidation)
	}

	b.mu.Lock("PutSchemaVersionMetadata")
	defer b.mu.Unlock()

	if _, ok := b.schemaVersionMetadata[schemaVersionID]; !ok {
		b.schemaVersionMetadata[schemaVersionID] = make(map[string]string)
	}

	b.schemaVersionMetadata[schemaVersionID][key] = value

	return nil
}

// QuerySchemaVersionMetadata returns all metadata key-value pairs for the
// given schema version ID.  Returns an empty map when none have been stored.
func (b *InMemoryBackend) QuerySchemaVersionMetadata(schemaVersionID string) map[string]string {
	b.mu.RLock("QuerySchemaVersionMetadata")
	defer b.mu.RUnlock()

	out := make(map[string]string)

	if m, ok := b.schemaVersionMetadata[schemaVersionID]; ok {
		maps.Copy(out, m)
	}

	return out
}

// RemoveSchemaVersionMetadata deletes a metadata key from a schema version.
func (b *InMemoryBackend) RemoveSchemaVersionMetadata(schemaVersionID, key string) error {
	if schemaVersionID == "" || key == "" {
		return fmt.Errorf("schemaVersionID and key are required: %w", ErrValidation)
	}

	b.mu.Lock("RemoveSchemaVersionMetadata")
	defer b.mu.Unlock()

	if m, ok := b.schemaVersionMetadata[schemaVersionID]; ok {
		delete(m, key)
	}

	return nil
}

// GetSchemaByDefinition searches all versions of the named schema for one
// whose SchemaDefinition exactly matches definition.
func (b *InMemoryBackend) GetSchemaByDefinition(
	registryName, schemaName, definition string,
) (*SchemaVersion, error) {
	b.mu.RLock("GetSchemaByDefinition")
	defer b.mu.RUnlock()

	s, ok := b.schemas.Get(schemaKey(registryName, schemaName))
	if !ok {
		return nil, fmt.Errorf("schema %q/%q not found: %w", registryName, schemaName, ErrNotFound)
	}

	for _, sv := range b.schemaVersions[schemaVersionListKey(s.SchemaARN)] {
		if sv.SchemaDefinition == definition {
			cp := *sv

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("no schema version with matching definition: %w", ErrNotFound)
}

// GetSchemaVersionsDiff compares the definitions of two schema versions and
// returns a human-readable diff string (lines added in v2 prefixed with "+",
// lines removed prefixed with "-"). An empty string means the versions are
// identical.
func (b *InMemoryBackend) GetSchemaVersionsDiff(
	registryName, schemaName string,
	v1, v2 int64,
) (string, error) {
	b.mu.RLock("GetSchemaVersionsDiff")
	defer b.mu.RUnlock()

	s, ok := b.schemas.Get(schemaKey(registryName, schemaName))
	if !ok {
		return "", fmt.Errorf("schema %q/%q not found: %w", registryName, schemaName, ErrNotFound)
	}

	defs := make(map[int64]string)

	for _, sv := range b.schemaVersions[schemaVersionListKey(s.SchemaARN)] {
		if sv.VersionNumber == v1 || sv.VersionNumber == v2 {
			defs[sv.VersionNumber] = sv.SchemaDefinition
		}
	}

	def1 := defs[v1]
	def2 := defs[v2]

	if def1 == def2 {
		return "", nil
	}

	return buildLineDiff(def1, def2), nil
}

// buildLineDiff produces a simple unified-style line diff.
func buildLineDiff(old, newDef string) string {
	oldLines := splitLines(old)
	newLines := splitLines(newDef)

	oldSet := make(map[string]struct{}, len(oldLines))
	for _, l := range oldLines {
		oldSet[l] = struct{}{}
	}

	newSet := make(map[string]struct{}, len(newLines))
	for _, l := range newLines {
		newSet[l] = struct{}{}
	}

	var removed, added []string

	for _, l := range oldLines {
		if _, ok := newSet[l]; !ok {
			removed = append(removed, "-"+l)
		}
	}

	for _, l := range newLines {
		if _, ok := oldSet[l]; !ok {
			added = append(added, "+"+l)
		}
	}

	sort.Strings(removed)
	sort.Strings(added)

	removed = append(removed, added...)

	return strings.Join(removed, "\n")
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}

	return strings.Split(strings.TrimSpace(s), "\n")
}

// DeleteSchemaVersion removes a single schema version from the store.
// Returns ErrNotFound if the schema or version does not exist.
func (b *InMemoryBackend) DeleteSchemaVersion(
	registryName, schemaName string,
	versionNumber int64,
) error {
	b.mu.Lock("DeleteSchemaVersion")
	defer b.mu.Unlock()

	s, ok := b.schemas.Get(schemaKey(registryName, schemaName))
	if !ok {
		return fmt.Errorf("schema %q/%q not found: %w", registryName, schemaName, ErrNotFound)
	}

	listKey := schemaVersionListKey(s.SchemaARN)
	versions := b.schemaVersions[listKey]

	for i, sv := range versions {
		if sv.VersionNumber == versionNumber {
			b.schemaVersions[listKey] = append(versions[:i], versions[i+1:]...)

			return nil
		}
	}

	return fmt.Errorf("schema version %d not found: %w", versionNumber, ErrNotFound)
}

func (b *InMemoryBackend) registryARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "registry/"+name)
}

func (b *InMemoryBackend) schemaARN(registryName, schemaName string) string {
	return arn.Build(
		"glue",
		b.region,
		b.accountID,
		fmt.Sprintf("schema/%s/%s", registryName, schemaName),
	)
}

// schemaKey returns a composite key for a schema.
func schemaKey(registryName, schemaName string) string {
	return registryName + "|" + schemaName
}

// schemaVersionListKey returns the key for a schema's version list.
func schemaVersionListKey(schemaARN string) string {
	return schemaARN
}

// CreateRegistry creates a new Glue Schema Registry.
func (b *InMemoryBackend) CreateRegistry(
	name, description string,
	tags map[string]string,
) (*Registry, error) {
	b.mu.Lock("CreateRegistry")
	defer b.mu.Unlock()

	if name == "" {
		return nil, ErrValidation
	}

	if b.registries.Has(name) {
		return nil, ErrAlreadyExists
	}

	reg := &Registry{
		Name:        name,
		ARN:         b.registryARN(name),
		Description: description,
		Status:      stateAvailable,
		Tags:        maps.Clone(tags),
		CreatedTime: float64(time.Now().Unix()),
		UpdatedTime: float64(time.Now().Unix()),
	}

	b.registries.Put(reg)

	return reg, nil
}

// DescribeRegistry retrieves a registry by name.
func (b *InMemoryBackend) DescribeRegistry(name string) (*Registry, error) {
	b.mu.RLock("DescribeRegistry")
	defer b.mu.RUnlock()

	reg, ok := b.registries.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *reg
	cp.Tags = maps.Clone(reg.Tags)

	return &cp, nil
}

// ListRegistries returns all registries sorted by name.
func (b *InMemoryBackend) ListRegistries() []*Registry {
	b.mu.RLock("ListRegistries")
	defer b.mu.RUnlock()

	src := b.registries.Snapshot()

	out := make([]*Registry, 0, len(src))
	for _, reg := range src {
		cp := *reg
		cp.Tags = maps.Clone(reg.Tags)
		out = append(out, &cp)
	}

	return out
}

// UpdateRegistry updates a registry's description.
func (b *InMemoryBackend) UpdateRegistry(name, description string) error {
	b.mu.Lock("UpdateRegistry")
	defer b.mu.Unlock()

	reg, ok := b.registries.Get(name)
	if !ok {
		return ErrNotFound
	}

	reg.Description = description
	reg.UpdatedTime = float64(time.Now().Unix())

	return nil
}

// DeleteRegistry deletes a registry by name.
func (b *InMemoryBackend) DeleteRegistry(name string) error {
	b.mu.Lock("DeleteRegistry")
	defer b.mu.Unlock()

	if !b.registries.Has(name) {
		return ErrNotFound
	}

	b.registries.Delete(name)

	return nil
}

// CreateSchema creates a new schema in the given registry.
func (b *InMemoryBackend) CreateSchema(
	registryName, schemaName, dataFormat, compatibility, description string,
	tags map[string]string,
) (*Schema, error) {
	b.mu.Lock("CreateSchema")
	defer b.mu.Unlock()

	if schemaName == "" {
		return nil, ErrValidation
	}

	key := schemaKey(registryName, schemaName)
	if b.schemas.Has(key) {
		return nil, ErrAlreadyExists
	}

	schARN := b.schemaARN(registryName, schemaName)

	s := &Schema{
		RegistryName:        registryName,
		SchemaName:          schemaName,
		SchemaARN:           schARN,
		RegistryARN:         b.registryARN(registryName),
		DataFormat:          dataFormat,
		Compatibility:       compatibility,
		Description:         description,
		SchemaStatus:        stateAvailable,
		Tags:                maps.Clone(tags),
		CreatedTime:         float64(time.Now().Unix()),
		UpdatedTime:         float64(time.Now().Unix()),
		LatestSchemaVersion: 0,
		NextSchemaVersion:   1,
		CheckpointVersion:   1,
	}

	b.schemas.Put(s)
	b.schemaVersions[schemaVersionListKey(schARN)] = nil // init empty version list

	cp := *s
	cp.Tags = maps.Clone(s.Tags)

	return &cp, nil
}

// DescribeSchema retrieves a schema by registry and schema name.
func (b *InMemoryBackend) DescribeSchema(registryName, schemaName string) (*Schema, error) {
	b.mu.RLock("DescribeSchema")
	defer b.mu.RUnlock()

	s, ok := b.schemas.Get(schemaKey(registryName, schemaName))
	if !ok {
		return nil, ErrNotFound
	}

	cp := *s
	cp.Tags = maps.Clone(s.Tags)

	return &cp, nil
}

// ListSchemas returns all schemas for a registry.
func (b *InMemoryBackend) ListSchemas(registryName string) []*Schema {
	b.mu.RLock("ListSchemas")
	defer b.mu.RUnlock()

	src := b.schemas.All()
	out := make([]*Schema, 0, len(src))
	for _, s := range src {
		if registryName == "" || s.RegistryName == registryName {
			cp := *s
			cp.Tags = maps.Clone(s.Tags)
			out = append(out, &cp)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].SchemaName < out[j].SchemaName
	})

	return out
}

// UpdateSchema updates a schema's compatibility and description.
func (b *InMemoryBackend) UpdateSchema(
	registryName, schemaName, compatibility, description string,
) error {
	b.mu.Lock("UpdateSchema")
	defer b.mu.Unlock()

	s, ok := b.schemas.Get(schemaKey(registryName, schemaName))
	if !ok {
		return ErrNotFound
	}

	if compatibility != "" {
		s.Compatibility = compatibility
	}

	if description != "" {
		s.Description = description
	}

	s.UpdatedTime = float64(time.Now().Unix())

	return nil
}

// DeleteSchema deletes a schema by registry and schema name.
func (b *InMemoryBackend) DeleteSchema(registryName, schemaName string) error {
	b.mu.Lock("DeleteSchema")
	defer b.mu.Unlock()

	key := schemaKey(registryName, schemaName)

	s, ok := b.schemas.Get(key)
	if !ok {
		return ErrNotFound
	}

	delete(b.schemaVersions, schemaVersionListKey(s.SchemaARN))
	b.schemas.Delete(key)

	return nil
}

// RegisterSchemaVersion registers a new version of a schema.
func (b *InMemoryBackend) RegisterSchemaVersion(
	registryName, schemaName, schemaDefinition string,
) (*SchemaVersion, error) {
	b.mu.Lock("RegisterSchemaVersion")
	defer b.mu.Unlock()

	s, ok := b.schemas.Get(schemaKey(registryName, schemaName))
	if !ok {
		return nil, ErrNotFound
	}

	versionNumber := s.NextSchemaVersion
	s.NextSchemaVersion++
	s.LatestSchemaVersion = versionNumber
	s.UpdatedTime = float64(time.Now().Unix())

	sv := &SchemaVersion{
		SchemaVersionID:  uuid.New().String(),
		SchemaARN:        s.SchemaARN,
		SchemaDefinition: schemaDefinition,
		Status:           stateAvailable,
		DataFormat:       s.DataFormat,
		VersionNumber:    versionNumber,
		CreatedTime:      float64(time.Now().Unix()),
	}

	listKey := schemaVersionListKey(s.SchemaARN)
	b.schemaVersions[listKey] = append(b.schemaVersions[listKey], sv)

	return sv, nil
}

// GetSchemaVersion retrieves a specific schema version.
func (b *InMemoryBackend) GetSchemaVersion(
	registryName, schemaName string,
	versionNumber int64,
) (*SchemaVersion, error) {
	b.mu.RLock("GetSchemaVersion")
	defer b.mu.RUnlock()

	s, ok := b.schemas.Get(schemaKey(registryName, schemaName))
	if !ok {
		return nil, ErrNotFound
	}

	for _, sv := range b.schemaVersions[schemaVersionListKey(s.SchemaARN)] {
		if sv.VersionNumber == versionNumber {
			cp := *sv

			return &cp, nil
		}
	}

	return nil, ErrNotFound
}

// ListSchemaVersions returns all versions for a schema.
func (b *InMemoryBackend) ListSchemaVersions(registryName, schemaName string) []*SchemaVersion {
	b.mu.RLock("ListSchemaVersions")
	defer b.mu.RUnlock()

	s, ok := b.schemas.Get(schemaKey(registryName, schemaName))
	if !ok {
		return nil
	}

	src := b.schemaVersions[schemaVersionListKey(s.SchemaARN)]
	out := make([]*SchemaVersion, len(src))

	for i, sv := range src {
		cp := *sv
		out[i] = &cp
	}

	return out
}
