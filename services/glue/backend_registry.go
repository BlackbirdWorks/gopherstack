package glue

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// Registry represents a Glue Schema Registry.
type Registry struct {
	Tags        map[string]string `json:"Tags,omitempty"`
	Name        string            `json:"RegistryName"`
	ARN         string            `json:"RegistryArn"`
	Description string            `json:"Description,omitempty"`
	Status      string            `json:"Status"`
	CreatedTime float64           `json:"CreatedTime,omitempty"`
	UpdatedTime float64           `json:"UpdatedTime,omitempty"`
}

// Schema represents a Glue Schema Registry schema.
type Schema struct {
	Tags                map[string]string `json:"Tags,omitempty"`
	RegistryName        string            `json:"RegistryName"`
	SchemaName          string            `json:"SchemaName"`
	SchemaARN           string            `json:"SchemaArn"`
	RegistryARN         string            `json:"RegistryArn"`
	DataFormat          string            `json:"DataFormat"`
	Compatibility       string            `json:"Compatibility"`
	Description         string            `json:"Description,omitempty"`
	SchemaStatus        string            `json:"SchemaStatus"`
	CreatedTime         float64           `json:"CreatedTime,omitempty"`
	UpdatedTime         float64           `json:"UpdatedTime,omitempty"`
	LatestSchemaVersion int64             `json:"LatestSchemaVersion"`
	NextSchemaVersion   int64             `json:"NextSchemaVersion"`
	CheckpointVersion   int64             `json:"SchemaCheckpoint"`
}

// SchemaVersion represents a single version of a schema.
type SchemaVersion struct {
	SchemaVersionID  string  `json:"SchemaVersionId"`
	SchemaARN        string  `json:"SchemaArn"`
	SchemaDefinition string  `json:"SchemaDefinition,omitempty"`
	Status           string  `json:"Status"`
	DataFormat       string  `json:"DataFormat,omitempty"`
	VersionNumber    int64   `json:"VersionNumber"`
	CreatedTime      float64 `json:"CreatedTime,omitempty"`
}

// CrawlerMetrics holds runtime metrics for a crawler.
type CrawlerMetrics struct {
	CrawlerName          string  `json:"CrawlerName"`
	TimeLeftSeconds      float64 `json:"TimeLeftSeconds"`
	LastRuntimeSeconds   float64 `json:"LastRuntimeSeconds"`
	MedianRuntimeSeconds float64 `json:"MedianRuntimeSeconds"`
	TablesCreated        int     `json:"TablesCreated"`
	TablesUpdated        int     `json:"TablesUpdated"`
	TablesDeleted        int     `json:"TablesDeleted"`
	StillEstimating      bool    `json:"StillEstimating"`
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

// --- Registry operations ---

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

// --- Schema operations ---

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

	return s, nil
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

// --- Schema Version operations ---

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

// --- Crawler Metrics ---

// --- Table Version retrieval ---

// GetTableVersions returns all stored versions for a table, sorted by versionID.
func (b *InMemoryBackend) GetTableVersions(dbName, tableName string) []*TableVersion {
	b.mu.RLock("GetTableVersions")
	defer b.mu.RUnlock()

	prefix := tableVersionKey(dbName, tableName, "")
	src := b.tableVersions.Snapshot()
	out := make([]*TableVersion, 0, len(src))

	for _, tv := range src {
		if k := tableVersionEntryKeyFn(tv); len(k) > len(prefix) && k[:len(prefix)] == prefix {
			cp := *tv
			if tv.Table != nil {
				t := *tv.Table
				cp.Table = &t
			}

			out = append(out, &cp)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].VersionID < out[j].VersionID
	})

	return out
}

// GetTableVersion returns a specific version of a table.
func (b *InMemoryBackend) GetTableVersion(
	dbName, tableName, versionID string,
) (*TableVersion, error) {
	b.mu.RLock("GetTableVersion")
	defer b.mu.RUnlock()

	key := tableVersionKey(dbName, tableName, versionID)

	tv, ok := b.tableVersions.Get(key)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *tv
	if tv.Table != nil {
		t := *tv.Table
		cp.Table = &t
	}

	return &cp, nil
}

// crawlerDefaultRuntimeSeconds is the simulated last/median runtime returned for a crawler.
const crawlerDefaultRuntimeSeconds = 45.0

// GetCrawlerMetrics returns metrics for one or all crawlers.
// If crawlerNames is empty, metrics for all crawlers are returned.
func (b *InMemoryBackend) GetCrawlerMetrics(crawlerNames []string) []*CrawlerMetrics {
	b.mu.RLock("GetCrawlerMetrics")
	defer b.mu.RUnlock()

	if len(crawlerNames) == 0 {
		for _, c := range b.crawlers.All() {
			crawlerNames = append(crawlerNames, c.Name)
		}

		sort.Strings(crawlerNames)
	}

	out := make([]*CrawlerMetrics, 0, len(crawlerNames))

	for _, name := range crawlerNames {
		c, ok := b.crawlers.Get(name)
		if !ok {
			continue
		}

		metrics := &CrawlerMetrics{
			CrawlerName:          name,
			TimeLeftSeconds:      0,
			StillEstimating:      c.State == stateRunning,
			LastRuntimeSeconds:   crawlerDefaultRuntimeSeconds,
			MedianRuntimeSeconds: crawlerDefaultRuntimeSeconds,
		}

		out = append(out, metrics)
	}

	return out
}
