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
	Modified time.Time `json:"Modified"`
	ThroughputSpec
}

// ThroughputInfo is a read-only snapshot of an offer.
type ThroughputInfo struct {
	Modified     time.Time
	OfferRID     string
	ResourceRID  string
	ResourceLink string
	ThroughputSpec
}

// MinimumThroughput is the lowest value the offer can be lowered to.
func (s ThroughputSpec) MinimumThroughput() int {
	if s.AutoscaleMax > 0 {
		return s.AutoscaleMax / autoscaleMinDivisor
	}

	return manualThroughputMin
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
	kind string
	rid  string
	link string
	key  string
}

func (b *InMemoryBackend) databaseTarget(d *storedDatabase) offerTarget {
	rid := fakeRID("dbs/" + d.ID)

	return offerTarget{
		slot: &d.Offer, kind: targetKindDatabase, rid: rid, link: dbsSegment + "/" + rid + "/", key: d.ID,
	}
}

func (b *InMemoryBackend) containerTarget(d *storedDatabase, c *storedContainer) offerTarget {
	dbRID := fakeRID("dbs/" + d.ID)
	rid := fakeRID("dbs/" + d.ID + "/colls/" + c.ID)

	return offerTarget{
		slot: &c.Offer, kind: targetKindContainer, rid: rid,
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

func (t offerTarget) info() ThroughputInfo {
	o := *t.slot

	return ThroughputInfo{
		Modified: o.Modified, OfferRID: offerRIDFor(t.rid), ResourceRID: t.rid, ResourceLink: t.link,
		ThroughputSpec: o.ThroughputSpec,
	}
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

	return t.info(), nil
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

	*t.slot = &storedOffer{ThroughputSpec: spec, Modified: b.now()}
	b.budgetEpoch++

	return t.info(), nil
}

// ListThroughputs returns every provisioned offer.
func (b *InMemoryBackend) ListThroughputs() []ThroughputInfo {
	b.mu.RLock("ListThroughputs")
	defer b.mu.RUnlock()

	out := make([]ThroughputInfo, 0)

	for _, t := range b.allTargets() {
		if *t.slot != nil {
			out = append(out, t.info())
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

	return t.info(), nil
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

// ThroughputBudget reports the RU/s budget governing container coll of db: a stable bucket key shared by every
// container drawing on the same offer, the per-second rate (0 = unlimited) and an epoch that changes whenever an
// offer is set, so callers can reset their meters.
func (b *InMemoryBackend) ThroughputBudget(db, coll string) (string, float64, int64) {
	b.mu.RLock("ThroughputBudget")
	defer b.mu.RUnlock()

	d, ok := b.databases[db]
	if !ok {
		return "", 0, b.budgetEpoch
	}

	if c, found := d.Containers[coll]; found && c.Offer != nil {
		return db + budgetKeySeparator + coll, c.Offer.rate(), b.budgetEpoch
	}

	switch {
	case b.serverless:
		return db + budgetKeySeparator + coll, serverlessBurstRUs, b.budgetEpoch
	case d.Offer != nil:
		return db, d.Offer.rate(), b.budgetEpoch
	}

	return "", 0, b.budgetEpoch
}

// rate is the enforcement ceiling: autoscale scales up instantly, so its ceiling is the configured maximum.
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
	ThroughputBudget(db, coll string) (key string, rate float64, epoch int64)
}

var _ ThroughputBackend = (*InMemoryBackend)(nil)
