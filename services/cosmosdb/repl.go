package cosmosdb

import (
	"slices"
	"strings"
	"time"
)

// replEvent is one write on its way to one region.
type replEvent struct {
	Due    time.Time
	At     time.Time
	Doc    *storedDocument
	Key    documentCompositeKey
	Dest   string
	Origin string
	Op     string
	LSN    int64
	Base   int64
	Stale  bool
}

// tombstone remembers a delete applied in a region, so a concurrent replace arriving later is seen as conflicting
// with it.
type tombstone struct {
	lsn int64
}

// replState is the per-container replication state of a replicated account: a separate document store per
// non-home region, the writes in flight to each region, and what each region has applied from each origin. The
// home store is the container itself.
type replState struct {
	stores  map[string]*storedContainer
	applied map[string]map[string]int64
	tombs   map[string]map[documentCompositeKey]tombstone
	home    string
	events  []*replEvent
	lsn     int64
}

func newReplState(home string) *replState {
	return &replState{
		stores:  map[string]*storedContainer{},
		applied: map[string]map[string]int64{},
		tombs:   map[string]map[documentCompositeKey]tombstone{},
		home:    home,
	}
}

// copyConfig shares the configuration of src with dst; scripts, policies and the offer are read-only here.
func copyConfig(dst, src *storedContainer) {
	dst.Modified, dst.Offer = src.Modified, src.Offer
	dst.Triggers, dst.Sprocs, dst.UDFs = src.Triggers, src.Sprocs, src.UDFs
	dst.DefaultTTL, dst.ID, dst.PartitionKeyPath, dst.PartitionKeyPaths = src.DefaultTTL, src.ID, src.PartitionKeyPath,
		src.PartitionKeyPaths
	dst.ConflictResolutionPolicy, dst.IndexingPolicy, dst.UniqueKeyPolicy = src.ConflictResolutionPolicy,
		src.IndexingPolicy, src.UniqueKeyPolicy
	dst.GeospatialConfig, dst.VectorEmbeddingPolicy, dst.FullTextPolicy = src.GeospatialConfig,
		src.VectorEmbeddingPolicy, src.FullTextPolicy
	dst.ComputedProperties = src.ComputedProperties
	dst.repl = src.repl
	dst.keep = src.keep
}

// shadowStore is a region's copy of the container, seeded from the home store.
func shadowStore(c *storedContainer) *storedContainer {
	s := &storedContainer{Documents: make(map[documentCompositeKey]*storedDocument, len(c.Documents))}
	copyConfig(s, c)

	for k, d := range c.Documents {
		cp := *d
		s.Documents[k] = &cp
	}

	s.ChangeSeq = c.ChangeSeq

	return s
}

// container returns the store of region name.
func (r *replState) container(c *storedContainer, name string) *storedContainer {
	if strings.EqualFold(name, r.home) {
		return c
	}

	if s, ok := r.stores[name]; ok {
		return s
	}

	return c
}

func (r *replState) appliedFor(region string) map[string]int64 {
	m, ok := r.applied[region]
	if !ok {
		m = map[string]int64{}
		r.applied[region] = m
	}

	return m
}

// reconcileRepl brings every container's replication state in line with the topology; the write lock is held.
func (b *InMemoryBackend) reconcileRepl() {
	regs := b.regionsView()

	for _, d := range b.databases {
		for _, c := range d.Containers {
			b.reconcileContainer(d.ID, c, regs)
		}
	}
}

func (b *InMemoryBackend) reconcileContainer(dbID string, c *storedContainer, regs AccountRegions) {
	if !regs.replicated() {
		if c.repl != nil {
			b.deliver(dbID, c, regs, true)
			c.repl = nil
		}

		return
	}

	home := regs.home()

	if c.repl == nil {
		c.repl = newReplState(home)
		c.repl.lsn = maxGLSN(c)
	}

	r := c.repl
	if !strings.EqualFold(r.home, home) {
		b.rehome(c, r, regs, home)
	}

	live := map[string]bool{strings.ToLower(home): true}

	for _, reg := range regs.Regions {
		live[strings.ToLower(reg.Name)] = true

		if strings.EqualFold(reg.Name, home) {
			continue
		}

		if _, ok := r.stores[reg.Name]; !ok {
			r.stores[reg.Name] = shadowStore(c)
			r.applied[reg.Name] = map[string]int64{}
		}
	}

	for name := range r.stores {
		if !live[strings.ToLower(name)] {
			delete(r.stores, name)
			delete(r.applied, name)
		}
	}

	r.events = slices.DeleteFunc(r.events, func(e *replEvent) bool {
		return !live[strings.ToLower(e.Dest)] || !live[strings.ToLower(e.Origin)]
	})
}

func maxGLSN(c *storedContainer) int64 {
	var m int64

	for _, d := range c.Documents {
		m = max(m, d.GLSN)
	}

	return m
}

// rehome makes the store of region home the container's own: the document state, change log and sequence of the
// two swap, and writes still on their way from the former home are marked as coming from a former write region.
func (b *InMemoryBackend) rehome(c *storedContainer, r *replState, regs AccountRegions, home string) {
	old := r.home

	var target *storedContainer

	for name, s := range r.stores {
		if strings.EqualFold(name, home) {
			target = s

			delete(r.stores, name)

			break
		}
	}

	if target == nil {
		target = shadowStore(c)
	}

	c.Documents, target.Documents = target.Documents, c.Documents
	c.ChangeLog, target.ChangeLog = target.ChangeLog, c.ChangeLog
	c.ChangeSeq, target.ChangeSeq = target.ChangeSeq, c.ChangeSeq
	c.sorted.Store(nil)
	target.sorted.Store(nil)
	copyConfig(target, c)

	r.stores[old] = target
	r.home = home

	if _, ok := r.applied[old]; !ok {
		r.applied[old] = map[string]int64{}
	}

	if !regs.MultiWrite {
		for _, e := range r.events {
			if strings.EqualFold(e.Origin, old) {
				e.Stale = true
			}
		}
	}

	b.budgetEpoch++
}

func regionOnline(regs AccountRegions, name string) bool {
	reg, _, ok := regs.find(name)

	return ok && !reg.Offline
}

// deliver applies the writes that have reached their region. force applies everything deliverable regardless of
// its due time. Writes from or to an offline region wait for it.
func (b *InMemoryBackend) deliver(dbID string, c *storedContainer, regs AccountRegions, force bool) {
	r := c.repl
	if r == nil || len(r.events) == 0 {
		return
	}

	now := b.now()

	slices.SortStableFunc(r.events, func(x, y *replEvent) int {
		if !x.Due.Equal(y.Due) {
			return x.Due.Compare(y.Due)
		}

		return int(x.LSN - y.LSN)
	})

	pending := r.events
	r.events = nil

	var kept []*replEvent

	for _, ev := range pending {
		ready := force || !ev.Due.After(now)
		if ready && regionOnline(regs, ev.Dest) && regionOnline(regs, ev.Origin) {
			b.applyEvent(dbID, c, r, ev, regs, now)

			continue
		}

		kept = append(kept, ev)
	}

	r.events = b.forceStaleness(dbID, c, r, kept, regs, now)
}

// forceStaleness enforces the bounded-staleness version bound: a region never trails by more than K writes.
func (b *InMemoryBackend) forceStaleness(
	dbID string, c *storedContainer, r *replState, kept []*replEvent, regs AccountRegions, now time.Time,
) []*replEvent {
	pol := regs.consistency()
	if pol.Level != ConsistencyBoundedStaleness {
		return kept
	}

	limit := int(pol.MaxStalenessPrefix)
	if limit <= 0 {
		limit = defaultStalenessPrefix
	}

	for _, reg := range regs.Regions {
		if !reg.Offline {
			kept = b.trimRegion(dbID, c, r, kept, regs, now, reg.Name, limit)
		}
	}

	return kept
}

// trimRegion applies a region's oldest waiting writes until at most limit are left.
func (b *InMemoryBackend) trimRegion(
	dbID string, c *storedContainer, r *replState, kept []*replEvent, regs AccountRegions, now time.Time,
	region string, limit int,
) []*replEvent {
	for {
		idx, count := waitingFor(kept, regs, region)
		if count <= limit || idx < 0 {
			return kept
		}

		b.applyEvent(dbID, c, r, kept[idx], regs, now)
		kept = slices.Delete(kept, idx, idx+1)
	}
}

// waitingFor locates the oldest write waiting for region and counts the ones that could be delivered.
func waitingFor(events []*replEvent, regs AccountRegions, region string) (int, int) {
	first, count := -1, 0

	for i, ev := range events {
		if !strings.EqualFold(ev.Dest, region) || !regionOnline(regs, ev.Origin) {
			continue
		}

		count++

		if first < 0 {
			first = i
		}
	}

	return first, count
}

func (b *InMemoryBackend) replTx(dbID string, c, dest *storedContainer, r *replState, region string, now time.Time,
	regs AccountRegions, replicating bool,
) *containerTx {
	tx := &containerTx{
		b: b, c: dest, dbID: dbID, collID: c.ID, now: now, seq0: dest.ChangeSeq,
		region: region, repl: r, regs: regs, replicating: replicating,
	}
	tx.evLen, tx.lsn0 = len(r.events), r.lsn

	return tx
}

// applyEvent lands one write in its destination region, resolving it against a concurrent write there.
func (b *InMemoryBackend) applyEvent(
	dbID string, c *storedContainer, r *replState, ev *replEvent, regs AccountRegions, now time.Time,
) {
	dest := r.container(c, ev.Dest)
	if dest != c {
		copyConfig(dest, c)
	}

	tx := b.replTx(dbID, c, dest, r, ev.Dest, now, regs, true)

	r.appliedFor(ev.Dest)[ev.Origin] = max(r.appliedFor(ev.Dest)[ev.Origin], ev.LSN)

	cur, exists := dest.lookup(ev.Key, now)
	tomb, tombed := r.tombs[ev.Dest][ev.Key]

	switch {
	case exists && cur.GLSN == ev.LSN:
		return
	case ev.Doc == nil && !exists:
		b.noteTomb(r, ev)

		return
	case ev.Doc == nil && cur.GLSN <= ev.Base:
		tx.remove(ev.Key)
		b.noteTomb(r, ev)

		return
	case ev.Doc != nil && !exists && (!tombed || tomb.lsn <= ev.Base):
		b.putReplicated(tx, r, ev)

		return
	case ev.Doc != nil && exists && cur.GLSN <= ev.Base:
		b.putReplicated(tx, r, ev)

		return
	}

	b.resolveConflict(dbID, c, dest, r, ev, regs, now, cur, exists)
}

func (b *InMemoryBackend) noteTomb(r *replState, ev *replEvent) {
	m, ok := r.tombs[ev.Dest]
	if !ok {
		m = map[documentCompositeKey]tombstone{}
		r.tombs[ev.Dest] = m
	}

	m[ev.Key] = tombstone{lsn: ev.LSN}
}

func (b *InMemoryBackend) putReplicated(tx *containerTx, r *replState, ev *replEvent) {
	cp := *ev.Doc
	cp.GLSN, cp.Origin = ev.LSN, ev.Origin
	tx.put(ev.Key, &cp)
	delete(r.tombs[ev.Dest], ev.Key)
}

// attachRepl ties a write transaction to the replication state when the account is replicated.
func (b *InMemoryBackend) attachRepl(tx *containerTx) {
	rc := b.cur.Load()
	if rc == nil || tx.c.repl == nil {
		return
	}

	tx.repl, tx.region, tx.regs = tx.c.repl, rc.region, b.regionsView()
	tx.evLen, tx.lsn0 = len(tx.repl.events), tx.repl.lsn
}

// emit records a write for replication: it stamps the document with its global sequence number and queues the
// write for every other region, due after that region's replication lag.
func (tx *containerTx) emit(key documentCompositeKey, doc, prev *storedDocument, op string) {
	r := tx.repl
	if r == nil || tx.replicating {
		return
	}

	r.lsn++

	var base int64
	if prev != nil {
		base = prev.GLSN
	}

	if doc != nil {
		doc.GLSN, doc.Origin = r.lsn, tx.region
	} else {
		tx.b.noteTomb(r, &replEvent{Dest: tx.region, Key: key, LSN: r.lsn})
	}

	r.appliedFor(tx.region)[tx.region] = r.lsn
	pol := tx.regs.consistency()

	for _, reg := range tx.regs.Regions {
		if strings.EqualFold(reg.Name, tx.region) {
			continue
		}

		r.events = append(r.events, &replEvent{
			Due: tx.now.Add(effectiveLag(pol, tx.regs.lag(reg.Name))), At: tx.now, Doc: doc, Key: key,
			Dest: reg.Name, Origin: tx.region, Op: op, LSN: r.lsn, Base: base,
		})
	}
}

// effectiveLag is the replication delay the account consistency level allows: none for Strong, at most the
// staleness interval for Bounded Staleness.
func effectiveLag(pol ConsistencyPolicy, lag time.Duration) time.Duration {
	switch pol.Level {
	case ConsistencyStrong:
		return 0
	case ConsistencyBoundedStaleness:
		secs := pol.MaxIntervalInSeconds
		if secs <= 0 {
			secs = defaultStalenessSecs
		}

		return min(lag, time.Duration(secs)*time.Second)
	}

	return lag
}

func (tx *containerTx) rollbackRepl() {
	if tx.repl == nil {
		return
	}

	tx.repl.events = tx.repl.events[:min(tx.evLen, len(tx.repl.events))]
	tx.repl.lsn = tx.lsn0
}
