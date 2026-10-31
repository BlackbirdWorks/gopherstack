package cosmosdb

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Throughput limits. The 400/100 manual and 1000/1000 autoscale floors and steps are documented Cosmos DB
// limits; the exact error texts are not verified against the service.
const (
	manualThroughputMin  = 400
	manualThroughputStep = 100
	manualThroughputMax  = 1_000_000
	autoscaleMaxMin      = 1000
	autoscaleMaxStep     = 1000
	autoscaleMaxMax      = 1_000_000
	autoscaleMinDivisor  = 10
	serverlessBurstRUs   = 5000
	percentMax           = 100
	offerRIDHexLen       = 16
	offerVersionV2       = "V2"
	offerTypeInvalid     = "Invalid"
	offersSegment        = "offers"
	budgetKeySeparator   = "/"
	errServerlessMessage = "Setting offer throughput or autopilot on %s is not supported for serverless accounts."
	targetKindDatabase   = "database"
	targetKindContainer  = "container"
)

// Throughput errors.
var (
	ErrThroughputNotFound = errors.New("cosmosdb: no throughput is provisioned on the resource")
	ErrInvalidThroughput  = errors.New("cosmosdb: invalid throughput")
	ErrServerless         = errors.New("cosmosdb: throughput is not supported on serverless accounts")
)

// ThroughputSpec is a provisioned-throughput setting: Manual RU/s, or an autoscale ceiling (with an optional
// auto-upgrade increment percentage). Exactly one of Manual and AutoscaleMax is non-zero.
type ThroughputSpec struct {
	Manual           int `json:"Throughput,omitempty"`
	AutoscaleMax     int `json:"AutoscaleMax,omitempty"`
	IncrementPercent int `json:"IncrementPercent,omitempty"`
}

// storedOffer is the persisted throughput offer of a database (shared) or container (dedicated).
type storedOffer struct {
	Modified    time.Time `json:"Modified"`
	HighestSeen int       `json:"HighestSeen,omitempty"`
	ThroughputSpec
}

// ThroughputInfo is a read-only snapshot of an offer.
type ThroughputInfo struct {
	Modified     time.Time
	OfferRID     string
	ResourceRID  string
	ResourceLink string
	Pending      bool
	Minimum      int
	ThroughputSpec
}

// MinimumThroughput is the lowest value the offer can be lowered to.
func (s ThroughputSpec) MinimumThroughput() int {
	if s.AutoscaleMax > 0 {
		return s.AutoscaleMax / autoscaleMinDivisor
	}

	return manualThroughputMin
}

// level is the RU/s the spec provisions: the manual value, or the autoscale maximum.
func (s ThroughputSpec) level() int {
	if s.AutoscaleMax > 0 {
		return s.AutoscaleMax
	}

	return s.Manual
}

// Validate checks the spec against the documented limits.
func (s ThroughputSpec) Validate() error {
	switch {
	case s.Manual > 0 && s.AutoscaleMax > 0:
		return fmt.Errorf(
			"%w: specify either a manual throughput or autoscale settings, not both",
			ErrInvalidThroughput,
		)
	case s.AutoscaleMax > 0:
		return s.validateAutoscale()
	case s.Manual > 0:
		return s.validateManual()
	}

	return fmt.Errorf("%w: a manual throughput or autoscale settings is required", ErrInvalidThroughput)
}

func (s ThroughputSpec) validateManual() error {
	switch {
	case s.Manual < manualThroughputMin:
		return fmt.Errorf("%w: the offer throughput %d is below the minimum of %d RU/s",
			ErrInvalidThroughput, s.Manual, manualThroughputMin)
	case s.Manual%manualThroughputStep != 0:
		return fmt.Errorf("%w: the offer throughput %d must be a multiple of %d RU/s",
			ErrInvalidThroughput, s.Manual, manualThroughputStep)
	case s.Manual > manualThroughputMax:
		return fmt.Errorf("%w: the offer throughput %d exceeds the maximum of %d RU/s",
			ErrInvalidThroughput, s.Manual, manualThroughputMax)
	}

	return nil
}

func (s ThroughputSpec) validateAutoscale() error {
	switch {
	case s.AutoscaleMax < autoscaleMaxMin:
		return fmt.Errorf("%w: the autoscale maximum throughput %d is below the minimum of %d RU/s",
			ErrInvalidThroughput, s.AutoscaleMax, autoscaleMaxMin)
	case s.AutoscaleMax%autoscaleMaxStep != 0:
		return fmt.Errorf("%w: the autoscale maximum throughput %d must be a multiple of %d RU/s",
			ErrInvalidThroughput, s.AutoscaleMax, autoscaleMaxStep)
	case s.AutoscaleMax > autoscaleMaxMax:
		return fmt.Errorf("%w: the autoscale maximum throughput %d exceeds the maximum of %d RU/s",
			ErrInvalidThroughput, s.AutoscaleMax, autoscaleMaxMax)
	case s.IncrementPercent < 0 || s.IncrementPercent > percentMax:
		return fmt.Errorf("%w: the autoscale increment percentage %d must be between 0 and %d",
			ErrInvalidThroughput, s.IncrementPercent, percentMax)
	}

	return nil
}

// offerRIDFor derives the opaque, lowercase offer resource id for a database or container rid.
func offerRIDFor(resourceRID string) string {
	sum := sha256.Sum256([]byte(offersSegment + "/" + resourceRID))

	return hex.EncodeToString(sum[:])[:offerRIDHexLen]
}

// offerTarget addresses the offer slot of one database or container.
type offerTarget struct {
	slot *(*storedOffer)
	db   *storedDatabase
	coll *storedContainer
	kind string
	rid  string
	link string
	key  string
}

func (b *InMemoryBackend) databaseTarget(d *storedDatabase) offerTarget {
	rid := fakeRID("dbs/" + d.ID)

	return offerTarget{
		slot: &d.Offer, db: d, kind: targetKindDatabase, rid: rid, link: dbsSegment + "/" + rid + "/", key: d.ID,
	}
}

func (b *InMemoryBackend) containerTarget(d *storedDatabase, c *storedContainer) offerTarget {
	dbRID := fakeRID("dbs/" + d.ID)
	rid := fakeRID("dbs/" + d.ID + "/colls/" + c.ID)

	return offerTarget{
		slot: &c.Offer, db: d, coll: c, kind: targetKindContainer, rid: rid,
		link: dbsSegment + "/" + dbRID + "/" + collsSegment + "/" + rid + "/",
		key:  d.ID + budgetKeySeparator + c.ID,
	}
}

// resolveTarget finds the database (coll == "") or container offer slot; the lock must be held.
func (b *InMemoryBackend) resolveTarget(db, coll string) (offerTarget, error) {
	d, ok := b.databases[db]
	if !ok {
		return offerTarget{}, ErrDatabaseNotFound
	}

	if coll == "" {
		return b.databaseTarget(d), nil
	}

	c, ok := d.Containers[coll]
	if !ok {
		return offerTarget{}, ErrContainerNotFound
	}

	return b.containerTarget(d, c), nil
}

// allTargets lists every database and container slot in a stable order; the lock must be held.
func (b *InMemoryBackend) allTargets() []offerTarget {
	var out []offerTarget

	for _, d := range b.databases {
		out = append(out, b.databaseTarget(d))

		for _, c := range d.Containers {
			out = append(out, b.containerTarget(d, c))
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].rid < out[j].rid })

	return out
}

func (b *InMemoryBackend) info(t offerTarget) ThroughputInfo {
	o := *t.slot

	return ThroughputInfo{
		Modified: o.Modified, OfferRID: offerRIDFor(t.rid), ResourceRID: t.rid, ResourceLink: t.link,
		ThroughputSpec: o.ThroughputSpec, Pending: t.pending(), Minimum: b.headerMinimum(t, o.ThroughputSpec),
	}
}

// pending reports whether a scale-up of the offer is still waiting on partition splits.
func (t offerTarget) pending() bool {
	if t.coll != nil {
		return t.coll.PKLayout != nil && t.coll.PKLayout.pending()
	}

	for _, c := range t.db.Containers {
		if c.Offer == nil && c.PKLayout != nil && c.PKLayout.pending() {
			return true
		}
	}

	return false
}

// SetServerless marks the account serverless: throughput cannot be provisioned and every container is capped.
func (b *InMemoryBackend) SetServerless(v bool) {
	b.mu.Lock("SetServerless")
	defer b.mu.Unlock()

	b.serverless = v
}

// CheckThroughput validates spec for provisioning on a database or container.
func (b *InMemoryBackend) CheckThroughput(spec ThroughputSpec, containerLevel bool) error {
	b.mu.RLock("CheckThroughput")
	defer b.mu.RUnlock()

	return b.checkSpec(spec, containerLevel)
}

func (b *InMemoryBackend) checkSpec(spec ThroughputSpec, containerLevel bool) error {
	if b.serverless {
		kind := targetKindDatabase
		if containerLevel {
			kind = targetKindContainer
		}

		return fmt.Errorf("%w: %s", ErrServerless, fmt.Sprintf(errServerlessMessage, kind))
	}

	return spec.Validate()
}

// GetThroughput returns the offer of a database (container == "") or container.
func (b *InMemoryBackend) GetThroughput(db, container string) (ThroughputInfo, error) {
	b.mu.RLock("GetThroughput")
	defer b.mu.RUnlock()

	t, err := b.resolveTarget(db, container)
	if err != nil {
		return ThroughputInfo{}, err
	}

	if *t.slot == nil {
		return ThroughputInfo{}, ErrThroughputNotFound
	}

	return b.info(t), nil
}

// SetThroughput creates or replaces the offer of a database (container == "") or container.
func (b *InMemoryBackend) SetThroughput(db, container string, spec ThroughputSpec) (ThroughputInfo, error) {
	b.mu.Lock("SetThroughput")
	defer b.mu.Unlock()

	t, err := b.resolveTarget(db, container)
	if err != nil {
		return ThroughputInfo{}, err
	}

	return b.storeOffer(t, spec)
}

func (b *InMemoryBackend) storeOffer(t offerTarget, spec ThroughputSpec) (ThroughputInfo, error) {
	if err := b.checkSpec(spec, t.kind == targetKindContainer); err != nil {
		return ThroughputInfo{}, err
	}

	if minimum := b.minimumFor(t, spec); spec.level() < minimum {
		return ThroughputInfo{}, fmt.Errorf("%w: the requested throughput %d is below the minimum of %d RU/s "+
			"currently required by the resource", ErrInvalidThroughput, spec.level(), minimum)
	}

	b.eachGoverned(t, b.prepareLayout)

	highest := spec.level()
	if *t.slot != nil && (*t.slot).HighestSeen > highest {
		highest = (*t.slot).HighestSeen
	}

	*t.slot = &storedOffer{ThroughputSpec: spec, Modified: b.now(), HighestSeen: highest}
	b.budgetEpoch++

	b.eachGoverned(t, b.syncLayout)

	return b.info(t), nil
}

// ListThroughputs returns every provisioned offer.
func (b *InMemoryBackend) ListThroughputs() []ThroughputInfo {
	b.mu.RLock("ListThroughputs")
	defer b.mu.RUnlock()

	out := make([]ThroughputInfo, 0)

	for _, t := range b.allTargets() {
		if *t.slot != nil {
			out = append(out, b.info(t))
		}
	}

	return out
}

// findOffer locates the target whose offer has the given offer rid; the lock must be held.
func (b *InMemoryBackend) findOffer(offerRID string) (offerTarget, error) {
	for _, t := range b.allTargets() {
		if *t.slot != nil && offerRIDFor(t.rid) == offerRID {
			return t, nil
		}
	}

	return offerTarget{}, ErrThroughputNotFound
}

// GetThroughputByRID returns the offer with the given offer rid.
func (b *InMemoryBackend) GetThroughputByRID(offerRID string) (ThroughputInfo, error) {
	b.mu.RLock("GetThroughputByRID")
	defer b.mu.RUnlock()

	t, err := b.findOffer(offerRID)
	if err != nil {
		return ThroughputInfo{}, err
	}

	return b.info(t), nil
}

// ReplaceThroughputByRID replaces the offer with the given offer rid.
func (b *InMemoryBackend) ReplaceThroughputByRID(offerRID string, spec ThroughputSpec) (ThroughputInfo, error) {
	b.mu.Lock("ReplaceThroughputByRID")
	defer b.mu.Unlock()

	t, err := b.findOffer(offerRID)
	if err != nil {
		return ThroughputInfo{}, err
	}

	return b.storeOffer(t, spec)
}

// RUBudget is the RU/s budget governing a container: a bucket key shared by every container drawing on the same
// offer, the total rate (0 = unlimited), the physical partition count and an epoch that changes whenever an offer
// is set, so callers can reset their meters.
type RUBudget struct {
	Key    string
	Ranges []pkSpan
	Rate   float64
	Epoch  int64
}

// Partitions is the number of physical partitions.
func (r RUBudget) Partitions() int { return max(1, len(r.Ranges)) }

// ThroughputBudget reports the RU/s budget governing container coll of db.
func (b *InMemoryBackend) ThroughputBudget(db, coll string) RUBudget {
	b.settleDue()

	b.mu.RLock("ThroughputBudget")
	defer b.mu.RUnlock()

	var bud RUBudget

	if d, ok := b.databases[db]; ok {
		bud = b.budgetOf(d, coll)

		if c, found := d.Containers[coll]; found {
			l := b.layoutView(d, c)
			bud.Ranges = l.Spans

			if l.pending() {
				bud.Rate = min(bud.Rate, float64(len(l.Spans))*partitionMaxRUs)
			}
		}
	}

	bud.Epoch = b.budgetEpoch

	return bud
}

func (b *InMemoryBackend) budgetOf(d *storedDatabase, coll string) RUBudget {
	if c, found := d.Containers[coll]; found && c.Offer != nil {
		return offerBudget(d.ID+budgetKeySeparator+coll, c.Offer)
	}

	switch {
	case b.serverless:
		return RUBudget{Key: d.ID + budgetKeySeparator + coll, Rate: serverlessBurstRUs}
	case d.Offer != nil:
		return offerBudget(d.ID, d.Offer)
	}

	return RUBudget{}
}

func offerBudget(key string, o *storedOffer) RUBudget {
	return RUBudget{Key: key, Rate: o.rate()}
}

// rate is the enforcement ceiling: autoscale scales instantly within [10% of max, max], so a request can always
// draw up to the configured maximum.
func (o *storedOffer) rate() float64 {
	if o.AutoscaleMax > 0 {
		return float64(o.AutoscaleMax)
	}

	return float64(o.Manual)
}

// ThroughputBackend is implemented by backends that provision throughput offers.
type ThroughputBackend interface {
	CheckThroughput(spec ThroughputSpec, containerLevel bool) error
	GetThroughput(db, container string) (ThroughputInfo, error)
	SetThroughput(db, container string, spec ThroughputSpec) (ThroughputInfo, error)
	ListThroughputs() []ThroughputInfo
	GetThroughputByRID(offerRID string) (ThroughputInfo, error)
	ReplaceThroughputByRID(offerRID string, spec ThroughputSpec) (ThroughputInfo, error)
	ThroughputBudget(db, coll string) RUBudget
	RangeShares(db, coll string) []float64
}

var _ ThroughputBackend = (*InMemoryBackend)(nil)

// eachGoverned applies fn to every container whose layout the offer target governs; the lock must be held.
func (b *InMemoryBackend) eachGoverned(t offerTarget, fn func(*storedDatabase, *storedContainer)) {
	if t.coll != nil {
		fn(t.db, t.coll)

		return
	}

	for _, c := range t.db.Containers {
		if c.Offer == nil {
			fn(t.db, c)
		}
	}
}

// Minimum throughput terms (Learn, "Service quotas and default limits", Minimum throughput limits).
const (
	bytesPerGB             = 1 << 30
	sharedContainersFree   = 25
	manualPerGBRU          = 1
	autoscalePerGBRU       = 10
	manualHighestDivisor   = 100
	autoscaleHighestDiv    = 10
	manualPerExtraContainr = 100
	autoMinRoundUp         = 1000
)

func ceilDiv(a, b int) int { return (a + b - 1) / b }

// storageGB is the stored data of the target, rounded up to whole gigabytes; the lock must be held.
func (b *InMemoryBackend) storageGB(t offerTarget) int {
	var total int64

	count := func(c *storedContainer) {
		for _, d := range c.Documents {
			total += docBytes(d)
		}
	}

	if t.coll != nil {
		count(t.coll)
	} else {
		for _, c := range t.db.Containers {
			if c.Offer == nil {
				count(c)
			}
		}
	}

	return int(ceilDiv64(total, bytesPerGB))
}

func ceilDiv64(a, b int64) int64 { return (a + b - 1) / b }

// minimumFor is the lowest throughput the resource can be set to now: the larger of the flat floor, one RU/s (ten
// for autoscale) per stored GB, a hundredth (a tenth) of the highest throughput ever provisioned and, for a shared
// database, the per-container term past 25 containers.
func (b *InMemoryBackend) minimumFor(t offerTarget, spec ThroughputSpec) int {
	highest := 0
	if *t.slot != nil {
		highest = (*t.slot).HighestSeen
	}

	gb := b.storageGB(t)

	extra := 0
	if t.coll == nil {
		extra = max(len(t.db.Containers)-sharedContainersFree, 0)
	}

	if spec.AutoscaleMax > 0 {
		floor := max(autoscaleMaxMin, gb*autoscalePerGBRU, ceilDiv(highest, autoscaleHighestDiv),
			autoscaleMaxMin+extra*autoscaleMaxMin)

		return ceilDiv(floor, autoMinRoundUp) * autoMinRoundUp
	}

	return max(manualThroughputMin, gb*manualPerGBRU, ceilDiv(highest, manualHighestDivisor),
		manualThroughputMin+extra*manualPerExtraContainr)
}

// headerMinimum is x-ms-cosmos-min-throughput: the lowest manual throughput now allowed, or the floor an
// autoscale resource scales down to (a tenth of its maximum).
func (b *InMemoryBackend) headerMinimum(t offerTarget, spec ThroughputSpec) int {
	if spec.AutoscaleMax > 0 {
		return spec.MinimumThroughput()
	}

	return b.minimumFor(t, spec)
}
