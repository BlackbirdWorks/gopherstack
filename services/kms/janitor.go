package kms

import (
	"container/heap"
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultKMSJanitorInterval = time.Minute
	kmsJanitorServiceName     = "kms"
	kmsJanitorComponent       = "KeyDeletionSweeper"
)

// expiryEntry is a heap entry tracking when a key's deletion or material expiry fires.
type expiryEntry struct {
	region  string
	keyID   string
	fireAt  float64 // Unix seconds
	expKind expiryKind
	heapIdx int
}

type expiryKind int

const (
	expiryKindDeletion expiryKind = iota // key is pending hard deletion
	expiryKindMaterial                   // EXTERNAL key material ValidTo expiry
	expiryKindRotation                   // automatic key rotation (AWS_KMS rotation)
)

// expiryHeap implements heap.Interface for min-heap ordering by fireAt.
type expiryHeap []*expiryEntry

func (h *expiryHeap) Len() int           { return len(*h) }
func (h *expiryHeap) Less(i, j int) bool { return (*h)[i].fireAt < (*h)[j].fireAt }
func (h *expiryHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
	(*h)[i].heapIdx = i
	(*h)[j].heapIdx = j
}

func (h *expiryHeap) Push(x any) {
	e, ok := x.(*expiryEntry)
	if !ok {
		return
	}

	e.heapIdx = len(*h)
	*h = append(*h, e)
}

func (h *expiryHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	e.heapIdx = -1

	return e
}

// Janitor is the KMS background worker that permanently deletes keys past their
// scheduled deletion date and purges the associated key material.
type Janitor struct {
	Backend *InMemoryBackend
	// heap is the priority queue of pending expiry events.
	heap     expiryHeap
	Interval time.Duration
	// TaskTimeout bounds each individual janitor task. When non-zero, each task
	// runs with a child context that expires after this duration, preventing a
	// stalled operation from blocking the janitor loop indefinitely.
	TaskTimeout time.Duration
}

// NewJanitor creates a new KMS Janitor for the given backend.
// A zero interval falls back to defaultKMSJanitorInterval.
func NewJanitor(backend *InMemoryBackend, interval time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultKMSJanitorInterval
	}

	j := &Janitor{
		Backend:  backend,
		Interval: interval,
	}
	heap.Init(&j.heap)

	return j
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, kmsJanitorServiceName)
	g.Ticker(kmsJanitorComponent, j.Interval, j.TaskTimeout, j.sweepExpiredKeys)

	<-ctx.Done()
	g.Stop()
}

// SweepOnce executes a single deletion sweep. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepExpiredKeys(ctx)
}

// scheduleExpiry adds a key expiry event to the heap.
// Must be called with the backend write lock held OR before the janitor is started.
func (j *Janitor) scheduleExpiry(region, keyID string, fireAt float64, kind expiryKind) {
	e := &expiryEntry{region: region, keyID: keyID, fireAt: fireAt, expKind: kind}
	heap.Push(&j.heap, e)
}

// sweepExpiredKeys removes keys in PendingDeletion state whose deletion date has
// passed, permanently purging their key material and associated aliases and grants.
// It also expires imported key material (EXTERNAL-origin keys) whose ValidTo has passed,
// and performs automatic key rotations for keys whose NextRotationDate has elapsed.
// Uses a min-heap to avoid scanning all keys every tick — O(log n) per expiration.
func (j *Janitor) sweepExpiredKeys(ctx context.Context) {
	now := float64(time.Now().UnixNano()) / nanoToSeconds

	j.Backend.mu.Lock("sweepExpiredKeys")

	var purged, expired int

	if len(j.heap) > 0 {
		// Fast path: drain heap candidates that are ready.
		purged, expired = j.sweepFromHeap(now)
	}

	if purged == 0 && expired == 0 {
		// Fallback linear scan: catches keys scheduled before janitor started,
		// or after Reset(), ensuring correctness even with a cold or stale heap.
		p2, e2 := j.sweepKeys(now)
		purged += p2
		expired += e2
	}

	// Automatic key rotation always runs unconditionally (not dependent on heap state).
	rotated := j.sweepAutoRotations(now)

	j.Backend.mu.Unlock()

	j.logSweepResults(ctx, purged, expired, rotated)
}

// sweepFromHeap drains heap entries whose fireAt ≤ now.
// Must be called with the backend write lock held.
func (j *Janitor) sweepFromHeap(now float64) (int, int) {
	var purged, expired int

	for len(j.heap) > 0 && j.heap[0].fireAt <= now {
		raw := heap.Pop(&j.heap)
		e, ok := raw.(*expiryEntry)
		if !ok {
			continue
		}

		key, ok := j.Backend.keysStore(e.region)[e.keyID]
		if !ok {
			continue // already purged
		}

		switch e.expKind {
		case expiryKindDeletion:
			if key.KeyState == KeyStatePendingDeletion && key.DeletionDate != 0 &&
				now >= key.DeletionDate {
				j.purgeKey(e.region, e.keyID)
				purged++
			}
		case expiryKindMaterial:
			if j.shouldExpireMaterial(key, now) {
				j.expireMaterial(e.region, e.keyID, key)
				expired++
			}
		case expiryKindRotation:
			// Automatic rotation heap entries are handled by sweepAutoRotations.
			// This case intentionally does nothing; the rotation itself fires via
			// the sweepAutoRotations scan that always runs after sweepFromHeap.
		}
	}

	return purged, expired
}

// sweepKeys iterates over all keys and purges/expires them as needed.
// This is the fallback O(n) sweep used when the heap is cold or empty.
// Must be called with the backend write lock held.
func (j *Janitor) sweepKeys(now float64) (int, int) {
	var purged, expired int
	for region, regionKeys := range j.Backend.keys {
		for keyID, key := range regionKeys {
			if key.KeyState == KeyStatePendingDeletion {
				if key.DeletionDate != 0 && now >= key.DeletionDate {
					j.purgeKey(region, keyID)
					purged++
				}

				continue
			}

			if j.shouldExpireMaterial(key, now) {
				j.expireMaterial(region, keyID, key)
				expired++
			}
		}
	}

	return purged, expired
}

// purgeKey permanently removes a key and all associated resources.
// Must be called with the backend write lock held.
func (j *Janitor) purgeKey(region, keyID string) {
	delete(j.Backend.keyMaterialsStore(region), keyID)
	delete(j.Backend.keyMaterialHistoryStore(region), keyID)

	for aliasName, alias := range j.Backend.aliasesStore(region) {
		if alias.TargetKeyID == keyID {
			j.Backend.keyIDResolutionCache.Delete(aliasName)
			delete(j.Backend.aliasesStore(region), aliasName)
		}
	}

	for grantID, grant := range j.Backend.grantsStore(region) {
		if grant.KeyID == keyID {
			delete(j.Backend.grantsStore(region), grantID)
			delete(j.Backend.grantsByTokenStore(region), grant.GrantToken)
		}
	}

	delete(j.Backend.keysStore(region), keyID)
	delete(j.Backend.policiesStore(region), keyID)
	j.Backend.lastUsage.Delete(region + ":" + keyID)
}

// shouldExpireMaterial reports whether the key's imported material should be expired.
func (j *Janitor) shouldExpireMaterial(key *Key, now float64) bool {
	return key.Origin == KeyOriginExternal &&
		key.ExpirationModel == expirationModelExpires &&
		key.ValidTo > 0 &&
		now >= key.ValidTo &&
		key.KeyState == KeyStateEnabled
}

// expireMaterial revokes the imported key material and sets the key back to PendingImport.
// Must be called with the backend write lock held.
func (j *Janitor) expireMaterial(region, keyID string, key *Key) {
	delete(j.Backend.keyMaterialsStore(region), keyID)
	delete(j.Backend.keyMaterialHistoryStore(region), keyID)
	key.KeyState = KeyStatePendingImport
	key.Enabled = false
	key.ValidTo = 0
	key.ExpirationModel = ""
}

// sweepAutoRotations rotates all eligible keys whose rotation period has elapsed.
// Must be called with the backend write lock held.
func (j *Janitor) sweepAutoRotations(now float64) int {
	rotated := 0

	for region, regionKeys := range j.Backend.keys {
		for keyID, key := range regionKeys {
			if !key.RotationEnabled ||
				key.KeyState != KeyStateEnabled ||
				key.Origin == KeyOriginExternal ||
				key.KeySpec != keySpecSymmetric {
				continue
			}

			period := key.RotationPeriodInDays
			if period <= 0 {
				period = defaultRotationPeriodDays
			}

			last := j.Backend.lastScheduledRotationDate(key)
			nextAt := last + float64(period)*float64(24*time.Hour/time.Second)

			if now < nextAt {
				continue
			}

			if err := j.Backend.rotateKeyMaterialLocked(region, key, rotationTypeAWSKMS); err != nil {
				continue
			}

			rotated++

			// Schedule next rotation check.
			last2 := j.Backend.lastScheduledRotationDate(key)
			nextFire := last2 + float64(period)*float64(24*time.Hour/time.Second)
			j.scheduleExpiry(region, keyID, nextFire, expiryKindRotation)
		}
	}

	return rotated
}

// logSweepResults records telemetry and logs for the janitor sweep.
func (j *Janitor) logSweepResults(ctx context.Context, purged, expired, rotated int) {
	if purged > 0 {
		telemetry.RecordWorkerItems(kmsJanitorServiceName, kmsJanitorComponent, purged)
		logger.Load(ctx).InfoContext(ctx, "KMS janitor: expired keys purged", "count", purged)
	}

	if expired > 0 {
		telemetry.RecordWorkerItems(kmsJanitorServiceName, kmsJanitorComponent, expired)
		logger.Load(ctx).
			InfoContext(ctx, "KMS janitor: imported key material expired", "count", expired)
	}

	if rotated > 0 {
		telemetry.RecordWorkerItems(kmsJanitorServiceName, kmsJanitorComponent, rotated)
		logger.Load(ctx).InfoContext(ctx, "KMS janitor: keys auto-rotated", "count", rotated)
	}

	telemetry.RecordWorkerTask(kmsJanitorServiceName, kmsJanitorComponent, "success")
}
