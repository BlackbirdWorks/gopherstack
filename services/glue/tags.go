package glue

import (
	"maps"
	"strings"
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
	if db := b.findDatabaseByARN(resourceARN); db != nil {
		mergeTags(&db.Tags, tags)

		return nil
	}

	if c := b.findCrawlerByARN(resourceARN); c != nil {
		mergeTags(&c.Tags, tags)

		return nil
	}

	if j := b.findJobByARN(resourceARN); j != nil {
		mergeTags(&j.Tags, tags)

		return nil
	}

	if r := b.findDataQualityRulesetByARN(resourceARN); r != nil {
		mergeTags(&r.Tags, tags)

		return nil
	}

	if conn := b.findConnectionByARN(resourceARN); conn != nil {
		mergeTags(&conn.Tags, tags)

		return nil
	}

	if trig := b.findTriggerByARN(resourceARN); trig != nil {
		mergeTags(&trig.Tags, tags)

		return nil
	}

	if w := b.findWorkflowByARN(resourceARN); w != nil {
		mergeTags(&w.Tags, tags)

		return nil
	}

	if bp := b.findBlueprintByARN(resourceARN); bp != nil {
		mergeTags(&bp.Tags, tags)

		return nil
	}

	if dep := b.findDevEndpointByARN(resourceARN); dep != nil {
		mergeTags(&dep.Tags, tags)

		return nil
	}

	if m := b.findMLTransformByARN(resourceARN); m != nil {
		mergeTags(&m.Tags, tags)

		return nil
	}

	if u := b.findUDFByARN(resourceARN); u != nil {
		mergeTags(&u.Tags, tags)

		return nil
	}

	return ErrNotFound
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

	if db := b.findDatabaseByARN(resourceARN); db != nil {
		deleteTags(db.Tags, tagKeys)

		return nil
	}

	if c := b.findCrawlerByARN(resourceARN); c != nil {
		deleteTags(c.Tags, tagKeys)

		return nil
	}

	if j := b.findJobByARN(resourceARN); j != nil {
		deleteTags(j.Tags, tagKeys)

		return nil
	}

	if r := b.findDataQualityRulesetByARN(resourceARN); r != nil {
		deleteTags(r.Tags, tagKeys)

		return nil
	}

	if conn := b.findConnectionByARN(resourceARN); conn != nil {
		deleteTags(conn.Tags, tagKeys)

		return nil
	}

	if trig := b.findTriggerByARN(resourceARN); trig != nil {
		deleteTags(trig.Tags, tagKeys)

		return nil
	}

	if w := b.findWorkflowByARN(resourceARN); w != nil {
		deleteTags(w.Tags, tagKeys)

		return nil
	}

	if bp := b.findBlueprintByARN(resourceARN); bp != nil {
		deleteTags(bp.Tags, tagKeys)

		return nil
	}

	if dep := b.findDevEndpointByARN(resourceARN); dep != nil {
		deleteTags(dep.Tags, tagKeys)

		return nil
	}

	if m := b.findMLTransformByARN(resourceARN); m != nil {
		deleteTags(m.Tags, tagKeys)

		return nil
	}

	if u := b.findUDFByARN(resourceARN); u != nil {
		deleteTags(u.Tags, tagKeys)

		return nil
	}

	return ErrNotFound
}

// GetTags retrieves tags for a resource by ARN.
func (b *InMemoryBackend) GetTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("GetTags")
	defer b.mu.RUnlock()

	if db := b.findDatabaseByARN(resourceARN); db != nil {
		return maps.Clone(db.Tags), nil
	}

	if c := b.findCrawlerByARN(resourceARN); c != nil {
		return maps.Clone(c.Tags), nil
	}

	if j := b.findJobByARN(resourceARN); j != nil {
		return maps.Clone(j.Tags), nil
	}

	if r := b.findDataQualityRulesetByARN(resourceARN); r != nil {
		return maps.Clone(r.Tags), nil
	}

	if conn := b.findConnectionByARN(resourceARN); conn != nil {
		return maps.Clone(conn.Tags), nil
	}

	if t := b.findTriggerByARN(resourceARN); t != nil {
		return maps.Clone(t.Tags), nil
	}

	if w := b.findWorkflowByARN(resourceARN); w != nil {
		return maps.Clone(w.Tags), nil
	}

	if bp := b.findBlueprintByARN(resourceARN); bp != nil {
		return maps.Clone(bp.Tags), nil
	}

	if dep := b.findDevEndpointByARN(resourceARN); dep != nil {
		return maps.Clone(dep.Tags), nil
	}

	if m := b.findMLTransformByARN(resourceARN); m != nil {
		return maps.Clone(m.Tags), nil
	}

	if u := b.findUDFByARN(resourceARN); u != nil {
		return maps.Clone(u.Tags), nil
	}

	return nil, ErrNotFound
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
