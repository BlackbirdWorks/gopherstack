package glue

import (
	"maps"
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

	return nil, ErrNotFound
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
