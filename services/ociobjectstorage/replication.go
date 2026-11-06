package ociobjectstorage

import (
	"net/http"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	replActive      = "ACTIVE"
	replClientError = "CLIENT_ERROR"
)

// ReplicationPolicy is a bucket replication policy (wire form of ReplicationPolicy / ReplicationPolicySummary).
type ReplicationPolicy struct {
	TimeCreated           time.Time `json:"timeCreated"`
	TimeLastSync          time.Time `json:"timeLastSync"`
	ID                    string    `json:"id"`
	Name                  string    `json:"name"`
	DestinationRegionName string    `json:"destinationRegionName"`
	DestinationBucketName string    `json:"destinationBucketName"`
	Status                string    `json:"status"`
	StatusMessage         string    `json:"statusMessage"`
}

// ReplicationSource marks a bucket as the read-only destination of a policy.
type ReplicationSource struct {
	PolicyName       string `json:"policyName"`
	SourceRegionName string `json:"sourceRegionName"`
	SourceBucketName string `json:"sourceBucketName"`
}

// ReplTask is one pending replication of an object (or of its deletion).
type ReplTask struct {
	DueAt  time.Time `json:"dueAt"`
	Name   string    `json:"name"`
	Delete bool      `json:"delete,omitempty"`
}

func errReplReadOnly(name string) *APIError {
	return errConflict("BucketReplicationEnabled",
		"Bucket '"+name+"' is the destination of a replication policy and is read-only; make it writable first")
}

// writable rejects writes to a replication destination.
func writable(bk *Bucket) error {
	if bk.ReplicationSource != nil {
		return errReplReadOnly(bk.Name)
	}

	return nil
}

// replicate queues an object write or delete for the bucket's replication policy.
func (b *InMemoryBackend) replicate(bk *Bucket, name string, del bool) {
	if bk.Replication == nil || bk.Replication.Status != replActive {
		return
	}

	bk.ReplQueue = append(bk.ReplQueue, ReplTask{Name: name, Delete: del, DueAt: b.nowUTC().Add(asyncLag)})
}

// CreateReplicationInput is the CreateReplicationPolicyDetails payload.
type CreateReplicationInput struct {
	Name                  string `json:"name"`
	DestinationRegionName string `json:"destinationRegionName"`
	DestinationBucketName string `json:"destinationBucketName"`
}

// CreateReplicationPolicy starts replicating new objects of the bucket into a destination bucket.
func (b *InMemoryBackend) CreateReplicationPolicy(
	ns, bucket string, in CreateReplicationInput,
) (ReplicationPolicy, error) {
	if in.Name == "" || in.DestinationRegionName == "" || in.DestinationBucketName == "" {
		return ReplicationPolicy{}, errMissing("name, destinationRegionName and destinationBucketName are required")
	}

	b.mu.Lock("CreateReplicationPolicy")
	defer b.mu.Unlock()

	src, err := b.bucket(ns, bucket)
	if err != nil {
		return ReplicationPolicy{}, err
	}

	dst, err := b.bucket(ns, in.DestinationBucketName)
	if err != nil {
		return ReplicationPolicy{}, err
	}

	if err = checkReplicable(src, dst); err != nil {
		return ReplicationPolicy{}, err
	}

	now := b.nowUTC()
	p := &ReplicationPolicy{
		ID: uuid.NewString(), Name: in.Name, DestinationRegionName: in.DestinationRegionName,
		DestinationBucketName: in.DestinationBucketName, TimeCreated: now, TimeLastSync: now,
		Status: replActive, StatusMessage: "",
	}
	src.Replication = p
	dst.ReplicationSource = &ReplicationSource{PolicyName: in.Name, SourceRegionName: Region, SourceBucketName: bucket}
	src.ReplQueue = nil

	return *p, nil
}

func checkReplicable(src, dst *Bucket) error {
	switch {
	case src.Name == dst.Name:
		return errParam("The destination bucket must differ from the source bucket")
	case src.Replication != nil:
		return errConflict("ReplicationPolicyAlreadyExists", "A source bucket can only have one replication policy")
	case src.ReplicationSource != nil || dst.Replication != nil:
		return errConflict("InvalidReplicationConfiguration", "Chained replication is not supported")
	case dst.ReplicationSource != nil:
		return errConflict("InvalidReplicationConfiguration",
			"The destination bucket is already the destination of another replication policy")
	}

	return nil
}

func replicationNotFound(id string) *APIError {
	return newErr(http.StatusNotFound, "ReplicationPolicyNotFound",
		"The replication policy '"+id+"' does not exist or you are not authorized to access it")
}

func (b *InMemoryBackend) policy(ns, bucket, id string) (*Bucket, error) {
	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return nil, err
	}

	if bk.Replication == nil || bk.Replication.ID != id {
		return nil, replicationNotFound(id)
	}

	return bk, nil
}

// GetReplicationPolicy returns the bucket's policy.
func (b *InMemoryBackend) GetReplicationPolicy(ns, bucket, id string) (ReplicationPolicy, error) {
	b.mu.RLock("GetReplicationPolicy")
	defer b.mu.RUnlock()

	bk, err := b.policy(ns, bucket, id)
	if err != nil {
		return ReplicationPolicy{}, err
	}

	return *bk.Replication, nil
}

// ListReplicationPolicies lists the bucket's policies (at most one).
func (b *InMemoryBackend) ListReplicationPolicies(
	ns, bucket, token string, limit int,
) (page.Page[ReplicationPolicy], error) {
	b.mu.RLock("ListReplicationPolicies")
	defer b.mu.RUnlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return page.Page[ReplicationPolicy]{}, err
	}

	all := []ReplicationPolicy{}
	if bk.Replication != nil {
		all = append(all, *bk.Replication)
	}

	return page.New(all, token, limit, defaultListLimit), nil
}

// DeleteReplicationPolicy stops replication; the destination stays read-only until MakeBucketWritable.
func (b *InMemoryBackend) DeleteReplicationPolicy(ns, bucket, id string) error {
	b.mu.Lock("DeleteReplicationPolicy")
	defer b.mu.Unlock()

	bk, err := b.policy(ns, bucket, id)
	if err != nil {
		return err
	}

	bk.Replication, bk.ReplQueue = nil, nil

	return nil
}

// ListReplicationSources lists the policies replicating into the bucket.
func (b *InMemoryBackend) ListReplicationSources(
	ns, bucket, token string, limit int,
) (page.Page[ReplicationSource], error) {
	b.mu.RLock("ListReplicationSources")
	defer b.mu.RUnlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return page.Page[ReplicationSource]{}, err
	}

	all := []ReplicationSource{}
	if bk.ReplicationSource != nil {
		all = append(all, *bk.ReplicationSource)
	}

	return page.New(all, token, limit, defaultListLimit), nil
}

// MakeBucketWritable ends a destination's read-only state; the source policy turns CLIENT_ERROR.
func (b *InMemoryBackend) MakeBucketWritable(ns, bucket string) error {
	b.mu.Lock("MakeBucketWritable")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return err
	}

	if bk.ReplicationSource == nil {
		return nil
	}

	if src, ok := b.buckets[bk.ReplicationSource.SourceBucketName]; ok && src.Replication != nil &&
		src.Replication.DestinationBucketName == bucket {
		src.Replication.Status = replClientError
		src.Replication.StatusMessage = "The destination bucket was made writable and no longer accepts replication"
		src.ReplQueue = nil
	}

	bk.ReplicationSource = nil

	return nil
}

// SweepAsync applies due replication tasks and completes due cross-region copies on the backend clock.
func (b *InMemoryBackend) SweepAsync() int {
	defer b.flushEvents()

	b.mu.Lock("SweepAsync")
	defer b.mu.Unlock()

	now := b.nowUTC()
	done := b.runPendingCopies(now)

	names := make([]string, 0, len(b.buckets))
	for n, bk := range b.buckets {
		if bk.Replication != nil && len(bk.ReplQueue) > 0 {
			names = append(names, n)
		}
	}

	sort.Strings(names)

	for _, n := range names {
		done += b.drainReplication(b.buckets[n], now)
	}

	return done
}

func (b *InMemoryBackend) drainReplication(src *Bucket, now time.Time) int {
	var (
		keep []ReplTask
		done int
	)

	for _, t := range src.ReplQueue {
		if now.Before(t.DueAt) {
			keep = append(keep, t)

			continue
		}

		dst, ok := b.buckets[src.Replication.DestinationBucketName]
		if !ok || dst.ReplicationSource == nil {
			src.Replication.Status = replClientError
			src.Replication.StatusMessage = "The destination bucket is missing or no longer accepts replication"

			keep = nil

			break
		}

		b.applyReplication(src, dst, t)

		done++
		src.Replication.TimeLastSync = now
	}

	src.ReplQueue = keep

	return done
}

func (b *InMemoryBackend) applyReplication(src, dst *Bucket, t ReplTask) {
	cur := live(src, t.Name)

	switch {
	case cur == nil:
		switch {
		case live(dst, t.Name) == nil:
		case dst.Versioning == versioningDisabled:
			delete(dst.Objects, t.Name)
		default:
			b.addMarkerLocked(dst, t.Name)
		}
	case cur.SseKeySha256 != "":
		return
	default:
		meta := cur.Meta
		meta.Metadata = copyMap(cur.Meta.Metadata)

		o, err := b.storeLocked(dst, t.Name, append([]byte(nil), cur.Data...), meta, specOf(cur))
		if err != nil {
			src.Replication.Status = replClientError
			src.Replication.StatusMessage = err.Error()

			return
		}

		o.Etag = cur.Etag
	}
}
