package glue

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrCrawlerRunning is returned when an operation requires the crawler to not be running.
var ErrCrawlerRunning = awserr.New("CrawlerRunningException", awserr.ErrInvalidParameter)

// ErrCrawlerNotRunning is returned when an operation requires the crawler to be running.
var ErrCrawlerNotRunning = awserr.New("CrawlerNotRunningException", awserr.ErrInvalidParameter)

const crawlerTransitionDelay = 200 * time.Millisecond // RUNNING→READY

// createCrawlerTablesLocked creates a Glue table per S3 prefix in the crawler's
// targets and returns how many tables were newly created (as opposed to already
// existing from a prior crawl), for the crawl-history summary. Must be called
// with b.mu held.
func (b *InMemoryBackend) createCrawlerTablesLocked(c *Crawler) int {
	created := 0

	for _, s3t := range c.Targets.S3Targets {
		path := strings.TrimPrefix(s3t.Path, "s3://")
		// Extract prefix after bucket name.
		var prefix string
		if _, after, ok := strings.Cut(path, "/"); ok {
			prefix = strings.Trim(after, "/")
		}

		if prefix == "" {
			prefix = "default"
		}

		// Sanitize prefix for use as table name.
		tableName := strings.NewReplacer("/", "_", "-", "_", ".", "_").Replace(prefix)
		if tableName == "" {
			tableName = "default"
		}

		key := c.DatabaseName + "|" + tableName
		if !b.tables.Has(key) {
			b.tables.Put(&Table{
				Name:         tableName,
				DatabaseName: c.DatabaseName,
				CreateTime:   float64(time.Now().Unix()),
			})
			created++
		}
	}

	return created
}

// cloneCrawler returns a deep copy of a Crawler.
func cloneCrawler(c *Crawler) *Crawler {
	cp := *c
	cp.Tags = maps.Clone(c.Tags)
	cp.Classifiers = append([]string(nil), c.Classifiers...)
	cp.Targets = cloneCrawlerTarget(c.Targets)

	return &cp
}

// cloneCrawlerTarget returns a deep copy of a CrawlerTarget, including the
// nested Exclusions/Tables slices on each individual target entry.
func cloneCrawlerTarget(t CrawlerTarget) CrawlerTarget {
	cp := CrawlerTarget{}

	if len(t.S3Targets) > 0 {
		cp.S3Targets = make([]S3Target, len(t.S3Targets))
		for i, s := range t.S3Targets {
			cp.S3Targets[i] = s
			cp.S3Targets[i].Exclusions = append([]string(nil), s.Exclusions...)
		}
	}

	if len(t.JdbcTargets) > 0 {
		cp.JdbcTargets = make([]JDBCTarget, len(t.JdbcTargets))
		for i, j := range t.JdbcTargets {
			cp.JdbcTargets[i] = j
			cp.JdbcTargets[i].Exclusions = append([]string(nil), j.Exclusions...)
		}
	}

	if len(t.CatalogTargets) > 0 {
		cp.CatalogTargets = make([]CatalogTarget, len(t.CatalogTargets))
		for i, ct := range t.CatalogTargets {
			cp.CatalogTargets[i] = ct
			cp.CatalogTargets[i].Tables = append([]string(nil), ct.Tables...)
		}
	}

	return cp
}

// crawlerARN returns the ARN for a Glue crawler.
func (b *InMemoryBackend) crawlerARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "crawler/"+name)
}

// CreateCrawler creates a new Glue crawler.
func (b *InMemoryBackend) CreateCrawler(
	name, role, dbName string,
	targets CrawlerTarget,
	tags map[string]string,
) (*Crawler, error) {
	return b.CreateCrawlerWithOptions(name, role, dbName, targets, tags, CrawlerOptions{})
}

// CreateCrawlerWithOptions is CreateCrawler plus the optional
// creation-time settings AWS's CreateCrawlerRequest supports
// (Schedule/Classifiers/Configuration/TablePrefix/Description) that the
// original positional-argument CreateCrawler predates.
func (b *InMemoryBackend) CreateCrawlerWithOptions(
	name, role, dbName string,
	targets CrawlerTarget,
	tags map[string]string,
	opts CrawlerOptions,
) (*Crawler, error) {
	b.mu.Lock("CreateCrawler")
	defer b.mu.Unlock()

	if name == "" || len(name) > maxNameLen || role == "" {
		return nil, ErrValidation
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	if dbName != "" {
		if !b.databases.Has(dbName) {
			return nil, ErrNotFound
		}
	}

	if b.crawlers.Has(name) {
		return nil, ErrAlreadyExists
	}

	now := float64(time.Now().Unix())
	c := &Crawler{
		Name:          name,
		Role:          role,
		DatabaseName:  dbName,
		Targets:       targets,
		State:         stateReady,
		ARN:           b.crawlerARN(name),
		Tags:          maps.Clone(tags),
		Description:   opts.Description,
		Configuration: opts.Configuration,
		TablePrefix:   opts.TablePrefix,
		Classifiers:   append([]string(nil), opts.Classifiers...),
		CreationTime:  now,
		LastUpdated:   now,
	}
	if opts.Schedule != "" {
		c.Schedule = CrawlerSchedule{ScheduleExpression: opts.Schedule, State: stateScheduled}
	}

	b.crawlers.Put(c)

	return c, nil
}

// GetCrawler retrieves a Glue crawler by name.
func (b *InMemoryBackend) GetCrawler(name string) (*Crawler, error) {
	b.advanceStates(time.Now())

	b.mu.RLock("GetCrawler")
	defer b.mu.RUnlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneCrawler(c), nil
}

// GetCrawlers returns all Glue crawlers sorted by name.
func (b *InMemoryBackend) GetCrawlers() []*Crawler {
	b.advanceStates(time.Now())

	b.mu.RLock("GetCrawlers")
	defer b.mu.RUnlock()

	src := b.crawlers.Snapshot()
	out := make([]*Crawler, 0, len(src))
	for _, c := range src {
		out = append(out, cloneCrawler(c))
	}

	return out
}

// ListCrawlers returns crawler names sorted alphabetically.
func (b *InMemoryBackend) ListCrawlers() []string {
	b.mu.RLock("ListCrawlers")
	defer b.mu.RUnlock()

	src := b.crawlers.Snapshot()
	out := make([]string, len(src))
	for i, c := range src {
		out[i] = c.Name
	}

	return out
}

// UpdateCrawler updates an existing Glue crawler.
func (b *InMemoryBackend) UpdateCrawler(name, role, dbName string, targets CrawlerTarget) error {
	return b.UpdateCrawlerWithOptions(name, role, dbName, targets, CrawlerOptions{})
}

// UpdateCrawlerWithOptions is UpdateCrawler plus the optional settings AWS's
// UpdateCrawlerRequest supports (Schedule/Classifiers/Configuration/
// TablePrefix/Description). Unset (zero-value) CrawlerOptions fields leave the
// corresponding crawler field unchanged, matching AWS's partial-update
// semantics for UpdateCrawler.
func (b *InMemoryBackend) UpdateCrawlerWithOptions(
	name, role, dbName string,
	targets CrawlerTarget,
	opts CrawlerOptions,
) error {
	b.mu.Lock("UpdateCrawler")
	defer b.mu.Unlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return ErrNotFound
	}

	// AWS rejects UpdateCrawler while the crawler is actively running, same as
	// DeleteCrawler.
	if c.State == stateRunning || c.State == stateStarting || c.State == stateStopping {
		return ErrCrawlerRunning
	}

	c.Role = role
	c.DatabaseName = dbName
	c.Targets = targets

	if opts.Description != "" {
		c.Description = opts.Description
	}

	if opts.Configuration != "" {
		c.Configuration = opts.Configuration
	}

	if opts.TablePrefix != "" {
		c.TablePrefix = opts.TablePrefix
	}

	if opts.Classifiers != nil {
		c.Classifiers = append([]string(nil), opts.Classifiers...)
	}

	if opts.Schedule != "" {
		c.Schedule = CrawlerSchedule{ScheduleExpression: opts.Schedule, State: stateScheduled}
	}

	c.LastUpdated = float64(time.Now().Unix())

	return nil
}

// DeleteCrawler deletes a Glue crawler by name.
func (b *InMemoryBackend) DeleteCrawler(name string) error {
	b.mu.Lock("DeleteCrawler")
	defer b.mu.Unlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return ErrNotFound
	}

	if c.State == stateRunning || c.State == stateStarting || c.State == stateStopping {
		return ErrCrawlerRunning
	}

	b.crawlers.Delete(name)

	return nil
}

// BatchGetCrawlers retrieves multiple crawlers by name.
func (b *InMemoryBackend) BatchGetCrawlers(names []string) ([]*Crawler, []string) {
	b.mu.RLock("BatchGetCrawlers")
	defer b.mu.RUnlock()

	found := make([]*Crawler, 0, len(names))
	missing := make([]string, 0, len(names))

	for _, name := range names {
		c, ok := b.crawlers.Get(name)
		if !ok {
			missing = append(missing, name)

			continue
		}

		found = append(found, cloneCrawler(c))
	}

	return found, missing
}

// StartCrawler sets a crawler's state to RUNNING (requires READY state).
// A background reconciler transitions the crawler to READY after crawlerTransitionDelay,
// creating Glue Catalog tables for each configured S3 prefix.
func (b *InMemoryBackend) StartCrawler(name string) error {
	b.mu.Lock("StartCrawler")
	defer b.mu.Unlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return ErrNotFound
	}

	if c.State == stateRunning || c.State == stateStopping {
		return ErrCrawlerRunning
	}

	now := time.Now()
	c.State = stateRunning
	c.LastUpdated = float64(now.Unix())

	b.crawlerReadyAt[name] = now.Add(crawlerTransitionDelay)
	b.crawlHistory[name] = append(b.crawlHistory[name], &CrawlHistoryEntry{
		CrawlID:   "cr-" + uuid.NewString()[:8],
		State:     "RUNNING",
		StartTime: float64(now.Unix()),
	})

	return nil
}

// StopCrawler sets a crawler's state to STOPPING (requires RUNNING state).
func (b *InMemoryBackend) StopCrawler(name string) error {
	b.mu.Lock("StopCrawler")
	defer b.mu.Unlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return ErrNotFound
	}
	if c.State != stateRunning {
		return ErrCrawlerNotRunning
	}

	now := time.Now()
	c.State = stateStopping
	c.LastUpdated = float64(now.Unix())

	// Schedule the STOPPING→READY transition so the crawler does not hang in
	// STOPPING forever. AWS returns the crawler to READY once it has stopped.
	b.crawlerReadyAt[name] = now.Add(crawlerTransitionDelay)

	return nil
}

// UpdateCrawlerSchedule updates the schedule expression on a crawler.
func (b *InMemoryBackend) UpdateCrawlerSchedule(name, scheduleExpression string) error {
	b.mu.Lock("UpdateCrawlerSchedule")
	defer b.mu.Unlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return ErrNotFound
	}
	c.Schedule.ScheduleExpression = scheduleExpression

	return nil
}

// StartCrawlerSchedule enables the crawler's schedule.
func (b *InMemoryBackend) StartCrawlerSchedule(name string) error {
	b.mu.Lock("StartCrawlerSchedule")
	defer b.mu.Unlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return ErrNotFound
	}
	if c.Schedule.ScheduleExpression == "" {
		return ErrValidation
	}
	if c.Schedule.State == stateScheduled {
		return ErrValidation
	}
	c.Schedule.State = stateScheduled

	return nil
}

// StopCrawlerSchedule disables the crawler's schedule.
func (b *InMemoryBackend) StopCrawlerSchedule(name string) error {
	b.mu.Lock("StopCrawlerSchedule")
	defer b.mu.Unlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return ErrNotFound
	}
	c.Schedule.State = stateNotScheduled

	return nil
}

// finishCrawlHistoryLocked marks the most recent in-flight crawl-history entry
// for name as finished with the given terminal state. A no-op if there is no
// open entry (e.g. crawl history predates this feature). Must be called with
// b.mu held.
func (b *InMemoryBackend) finishCrawlHistoryLocked(name, state string, tablesCreated int, at time.Time) {
	hist := b.crawlHistory[name]
	if len(hist) == 0 {
		return
	}

	last := hist[len(hist)-1]
	if last.EndTime != 0 {
		return
	}

	last.State = state
	last.EndTime = float64(at.Unix())

	if state == "COMPLETED" {
		last.Summary = fmt.Sprintf(
			`{"TABLES_ADDED":%d,"TABLES_UPDATED":0,"TABLES_DELETED":0,"PARTITIONS_ADDED":0}`,
			tablesCreated,
		)
	}
}

// ListCrawls returns the crawl history for a crawler, newest first.
func (b *InMemoryBackend) ListCrawls(crawlerName string) ([]*CrawlHistoryEntry, error) {
	b.mu.RLock("ListCrawls")
	defer b.mu.RUnlock()

	if !b.crawlers.Has(crawlerName) {
		return nil, ErrNotFound
	}

	hist := b.crawlHistory[crawlerName]
	out := make([]*CrawlHistoryEntry, len(hist))

	for i, e := range hist {
		cp := *e
		out[len(hist)-1-i] = &cp
	}

	return out, nil
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
