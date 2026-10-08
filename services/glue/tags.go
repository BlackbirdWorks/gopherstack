package glue

import (
	"maps"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// TagResource adds tags to a resource by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if err := validateTags(tags); err != nil {
		return err
	}

	return b.tagResource(resourceARN, tags)
}

func mergeTags(dst *map[string]string, src map[string]string) {
	if *dst == nil {
		*dst = make(map[string]string)
	}

	maps.Copy(*dst, src)
}

func (b *InMemoryBackend) tagResource(
	resourceARN string,
	tags map[string]string,
) error {
	field := b.tagsFieldByARN(resourceARN)
	if field == nil {
		return ErrNotFound
	}

	mergeTags(field, tags)

	return nil
}

// tagsFieldByARN resolves the Tags field of whichever resource resourceARN names.
func (b *InMemoryBackend) tagsFieldByARN(resourceARN string) *map[string]string {
	if field := b.coreTagsFieldByARN(resourceARN); field != nil {
		return field
	}

	return b.extraTagsFieldByARN(resourceARN)
}

func (b *InMemoryBackend) coreTagsFieldByARN(resourceARN string) *map[string]string {
	switch {
	case b.findDatabaseByARN(resourceARN) != nil:
		return &b.findDatabaseByARN(resourceARN).Tags
	case b.findCrawlerByARN(resourceARN) != nil:
		return &b.findCrawlerByARN(resourceARN).Tags
	case b.findJobByARN(resourceARN) != nil:
		return &b.findJobByARN(resourceARN).Tags
	case b.findDataQualityRulesetByARN(resourceARN) != nil:
		return &b.findDataQualityRulesetByARN(resourceARN).Tags
	case b.findConnectionByARN(resourceARN) != nil:
		return &b.findConnectionByARN(resourceARN).Tags
	case b.findTriggerByARN(resourceARN) != nil:
		return &b.findTriggerByARN(resourceARN).Tags
	case b.findWorkflowByARN(resourceARN) != nil:
		return &b.findWorkflowByARN(resourceARN).Tags
	case b.findBlueprintByARN(resourceARN) != nil:
		return &b.findBlueprintByARN(resourceARN).Tags
	default:
		return nil
	}
}

func (b *InMemoryBackend) extraTagsFieldByARN(resourceARN string) *map[string]string {
	switch {
	case b.findDevEndpointByARN(resourceARN) != nil:
		return &b.findDevEndpointByARN(resourceARN).Tags
	case b.findMLTransformByARN(resourceARN) != nil:
		return &b.findMLTransformByARN(resourceARN).Tags
	case b.findUDFByARN(resourceARN) != nil:
		return &b.findUDFByARN(resourceARN).Tags
	case b.findRegistryByARN(resourceARN) != nil:
		return &b.findRegistryByARN(resourceARN).Tags
	case b.findSchemaByARN(resourceARN) != nil:
		return &b.findSchemaByARN(resourceARN).Tags
	case b.findIntegrationByARN(resourceARN) != nil:
		return &b.findIntegrationByARN(resourceARN).Tags
	case b.findSessionByARN(resourceARN) != nil:
		return &b.findSessionByARN(resourceARN).Tags
	default:
		return nil
	}
}

func deleteTags(tags map[string]string, keys []string) {
	for _, k := range keys {
		delete(tags, k)
	}
}

// UntagResource removes tags from a resource by ARN.
func (b *InMemoryBackend) UntagResource(
	resourceARN string,
	tagKeys []string,
) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	field := b.tagsFieldByARN(resourceARN)
	if field == nil {
		return ErrNotFound
	}

	deleteTags(*field, tagKeys)

	return nil
}

// GetTags retrieves tags for a resource by ARN.
func (b *InMemoryBackend) GetTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("GetTags")
	defer b.mu.RUnlock()

	field := b.tagsFieldByARN(resourceARN)
	if field == nil {
		return nil, ErrNotFound
	}

	return maps.Clone(*field), nil
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's wireTaggingGlue).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every Glue resource ARN that currently has at least
// one tag, across every taggable Glue resource kind (databases, crawlers,
// jobs, data quality rulesets, connections, triggers, workflows, blueprints,
// dev endpoints, ML transforms, user-defined functions). Unlike ECS/Athena/ECR,
// Glue keeps tags inline on each typed resource (Database.Tags, Crawler.Tags,
// ...) rather than in a side map keyed by ARN, so this walks each store.Table
// directly instead of a single flat map.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, db := range b.databases.All() {
		out = appendTaggedEntry(out, db.ARN, db.Tags)
	}

	for _, c := range b.crawlers.All() {
		out = appendTaggedEntry(out, c.ARN, c.Tags)
	}

	for _, j := range b.jobs.All() {
		out = appendTaggedEntry(out, j.ARN, j.Tags)
	}

	for _, r := range b.dataQualityRulesets.All() {
		out = appendTaggedEntry(out, r.ARN, r.Tags)
	}

	for _, conn := range b.connections.All() {
		out = appendTaggedEntry(out, conn.ARN, conn.Tags)
	}

	for _, trig := range b.triggers.All() {
		out = appendTaggedEntry(out, trig.ARN, trig.Tags)
	}

	for _, w := range b.workflows.All() {
		out = appendTaggedEntry(out, w.ARN, w.Tags)
	}

	for _, bp := range b.blueprints.All() {
		out = appendTaggedEntry(out, b.blueprintARN(bp.Name), bp.Tags)
	}

	for _, dep := range b.devEndpoints.All() {
		out = appendTaggedEntry(out, dep.ARN, dep.Tags)
	}

	for _, m := range b.mlTransforms.All() {
		out = appendTaggedEntry(out, b.mlTransformARN(m.TransformID), m.Tags)
	}

	for _, u := range b.udfs.All() {
		out = appendTaggedEntry(out, u.FunctionARN, u.Tags)
	}

	for _, ig := range b.integrations.All() {
		out = appendTaggedEntry(out, ig.IntegrationArn, ig.Tags)
	}

	return out
}

// appendTaggedEntry appends a TaggedEntry for arn/tags to entries when tags is
// non-empty, cloning tags so callers cannot mutate the backend's copy.
func appendTaggedEntry(entries []TaggedEntry, arn string, tagMap map[string]string) []TaggedEntry {
	if len(tagMap) == 0 {
		return entries
	}

	return append(entries, TaggedEntry{ARN: arn, Tags: maps.Clone(tagMap)})
}

// resourceTagsSnapshot returns the ARN -> tags map across every taggable
// Glue resource kind. Tags is json:"-" on all eleven taggable structs (it is
// never part of a real Get*/Describe* wire shape -- tags come back only via
// GetTags), so store.Table's per-row JSON persistence silently drops it on
// restart. This is the side table Restore repopulates each struct's Tags
// field from (see restoreResourceTags), mirroring how services/mq's
// b.tags[arn] side-table survives a restore. Caller must hold at least
// b.mu's read lock.
func (b *InMemoryBackend) resourceTagsSnapshot() map[string]map[string]string {
	out := make(map[string]map[string]string)

	addTags := func(arn string, tags map[string]string) {
		if len(tags) == 0 {
			return
		}

		out[arn] = maps.Clone(tags)
	}

	for _, db := range b.databases.All() {
		addTags(db.ARN, db.Tags)
	}

	for _, c := range b.crawlers.All() {
		addTags(c.ARN, c.Tags)
	}

	for _, j := range b.jobs.All() {
		addTags(j.ARN, j.Tags)
	}

	for _, r := range b.dataQualityRulesets.All() {
		addTags(r.ARN, r.Tags)
	}

	for _, conn := range b.connections.All() {
		addTags(conn.ARN, conn.Tags)
	}

	for _, trig := range b.triggers.All() {
		addTags(trig.ARN, trig.Tags)
	}

	for _, w := range b.workflows.All() {
		addTags(w.ARN, w.Tags)
	}

	for _, bp := range b.blueprints.All() {
		addTags(b.blueprintARN(bp.Name), bp.Tags)
	}

	b.addExtraResourceTags(addTags)

	return out
}

func (b *InMemoryBackend) addExtraResourceTags(addTags func(string, map[string]string)) {
	for _, dep := range b.devEndpoints.All() {
		addTags(b.devEndpointARN(dep.EndpointName), dep.Tags)
	}

	for _, m := range b.mlTransforms.All() {
		addTags(b.mlTransformARN(m.TransformID), m.Tags)
	}

	for _, u := range b.udfs.All() {
		addTags(b.udfARN(u.DatabaseName, u.FunctionName), u.Tags)
	}

	for _, reg := range b.registries.All() {
		addTags(reg.ARN, reg.Tags)
	}

	for _, s := range b.schemas.All() {
		addTags(s.SchemaARN, s.Tags)
	}

	for _, sess := range b.sessions.All() {
		addTags(b.sessionARN(sess.SessionID), sess.Tags)
	}
}

// restoreResourceTags repopulates each taggable struct's Tags field from the
// resourceTags side table produced by resourceTagsSnapshot. DevEndpoint.ARN
// and UserDefinedFunction.FunctionARN are themselves json:"-" (recomputed on
// create, never round-tripped through a snapshot), so both sides key those
// two kinds by the deterministic devEndpointARN/udfARN helper rather than the
// struct field, which would read back empty here. Caller must hold b.mu's
// write lock.
func (b *InMemoryBackend) restoreResourceTags(resourceTags map[string]map[string]string) {
	for _, db := range b.databases.All() {
		db.Tags = resourceTags[db.ARN]
	}

	for _, c := range b.crawlers.All() {
		c.Tags = resourceTags[c.ARN]
	}

	for _, j := range b.jobs.All() {
		j.Tags = resourceTags[j.ARN]
	}

	for _, r := range b.dataQualityRulesets.All() {
		r.Tags = resourceTags[r.ARN]
	}

	for _, conn := range b.connections.All() {
		conn.Tags = resourceTags[conn.ARN]
	}

	for _, trig := range b.triggers.All() {
		trig.Tags = resourceTags[trig.ARN]
	}

	for _, w := range b.workflows.All() {
		w.Tags = resourceTags[w.ARN]
	}

	for _, bp := range b.blueprints.All() {
		bp.Tags = resourceTags[b.blueprintARN(bp.Name)]
	}

	b.restoreExtraResourceTags(resourceTags)
}

func (b *InMemoryBackend) restoreExtraResourceTags(resourceTags map[string]map[string]string) {
	for _, dep := range b.devEndpoints.All() {
		dep.Tags = resourceTags[b.devEndpointARN(dep.EndpointName)]
	}

	for _, m := range b.mlTransforms.All() {
		m.Tags = resourceTags[b.mlTransformARN(m.TransformID)]
	}

	for _, u := range b.udfs.All() {
		u.Tags = resourceTags[b.udfARN(u.DatabaseName, u.FunctionName)]
	}

	for _, reg := range b.registries.All() {
		reg.Tags = resourceTags[reg.ARN]
	}

	for _, s := range b.schemas.All() {
		s.Tags = resourceTags[s.SchemaARN]
	}

	for _, sess := range b.sessions.All() {
		sess.Tags = resourceTags[b.sessionARN(sess.SessionID)]
	}
}

func (b *InMemoryBackend) sessionARN(id string) string {
	return arn.Build("glue", b.region, b.accountID, "session/"+id)
}

func (b *InMemoryBackend) findSessionByARN(resourceARN string) *Session {
	id := glueResourceName(resourceARN, "session")
	if id == "" {
		return nil
	}

	s, ok := b.sessions.Get(id)
	if !ok {
		return nil
	}

	return s
}

func (b *InMemoryBackend) findDatabaseByARN(resourceARN string) *Database {
	name := glueResourceName(resourceARN, "database")
	if name == "" {
		return nil
	}

	db, ok := b.databases.Get(name)
	if !ok {
		return nil
	}

	return db
}

func (b *InMemoryBackend) findRegistryByARN(resourceARN string) *Registry {
	name := glueResourceName(resourceARN, "registry")
	if name == "" {
		return nil
	}

	reg, ok := b.registries.Get(name)
	if !ok {
		return nil
	}

	return reg
}

// findSchemaByARN looks up a schema by its ARN, whose resource segment has
// the form "schema/<registryName>/<schemaName>" (schemaARN, above).
func (b *InMemoryBackend) findSchemaByARN(resourceARN string) *Schema {
	rest := glueResourceName(resourceARN, "schema")
	if rest == "" {
		return nil
	}

	registryName, schemaName, ok := strings.Cut(rest, "/")
	if !ok {
		return nil
	}

	s, ok := b.schemas.Get(schemaKey(registryName, schemaName))
	if !ok {
		return nil
	}

	return s
}

func (b *InMemoryBackend) findCrawlerByARN(resourceARN string) *Crawler {
	name := glueResourceName(resourceARN, "crawler")
	if name == "" {
		return nil
	}

	c, ok := b.crawlers.Get(name)
	if !ok {
		return nil
	}

	return c
}

func (b *InMemoryBackend) findJobByARN(resourceARN string) *Job {
	name := glueResourceName(resourceARN, "job")
	if name == "" {
		return nil
	}

	j, ok := b.jobs.Get(name)
	if !ok {
		return nil
	}

	return j
}

func (b *InMemoryBackend) findDataQualityRulesetByARN(resourceARN string) *DataQualityRuleset {
	name := glueResourceName(resourceARN, "dataQualityRuleset")
	if name == "" {
		return nil
	}

	r, ok := b.dataQualityRulesets.Get(name)
	if !ok {
		return nil
	}

	return r
}

func (b *InMemoryBackend) findConnectionByARN(resourceARN string) *Connection {
	name := glueResourceName(resourceARN, "connection")
	if name == "" {
		return nil
	}

	c, ok := b.connections.Get(name)
	if !ok {
		return nil
	}

	return c
}

func (b *InMemoryBackend) findTriggerByARN(resourceARN string) *Trigger {
	name := glueResourceName(resourceARN, "trigger")
	if name == "" {
		return nil
	}

	t, ok := b.triggers.Get(name)
	if !ok {
		return nil
	}

	return t
}

func (b *InMemoryBackend) findWorkflowByARN(resourceARN string) *Workflow {
	name := glueResourceName(resourceARN, "workflow")
	if name == "" {
		return nil
	}

	w, ok := b.workflows.Get(name)
	if !ok {
		return nil
	}

	return w
}

func (b *InMemoryBackend) findBlueprintByARN(resourceARN string) *Blueprint {
	name := glueResourceName(resourceARN, "blueprint")
	if name == "" {
		return nil
	}

	bp, ok := b.blueprints.Get(name)
	if !ok {
		return nil
	}

	return bp
}

func (b *InMemoryBackend) findDevEndpointByARN(resourceARN string) *DevEndpoint {
	name := glueResourceName(resourceARN, "devEndpoint")
	if name == "" {
		return nil
	}

	dep, ok := b.devEndpoints.Get(name)
	if !ok {
		return nil
	}

	return dep
}

func (b *InMemoryBackend) findMLTransformByARN(resourceARN string) *MLTransform {
	id := glueResourceName(resourceARN, "mlTransform")
	if id == "" {
		return nil
	}

	m, ok := b.mlTransforms.Get(id)
	if !ok {
		return nil
	}

	return m
}

// findUDFByARN resolves a userDefinedFunction/<db>/<name> resource ARN. Unlike
// every other Glue taggable resource, a UDF's identity is two-level
// (database-scoped), so its ARN resource segment has an extra "/".
func (b *InMemoryBackend) findUDFByARN(resourceARN string) *UserDefinedFunction {
	rest := glueResourceName(resourceARN, "userDefinedFunction")
	if rest == "" {
		return nil
	}

	dbName, name, ok := strings.Cut(rest, "/")
	if !ok {
		return nil
	}

	u, ok := b.udfs.Get(b.udfKey(dbName, name))
	if !ok {
		return nil
	}

	return u
}

// findIntegrationByARN looks up a zero-ETL integration by its ARN.
func (b *InMemoryBackend) findIntegrationByARN(resourceARN string) *Integration {
	name := glueResourceName(resourceARN, "integration")
	if name == "" {
		return nil
	}

	ig, ok := b.integrations.Get(name)
	if !ok {
		return nil
	}

	return ig
}
