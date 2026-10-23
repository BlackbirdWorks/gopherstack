package gcpredis

import (
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/redis/apiv1/redispb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// UpdateInstance applies the masked fields of the request's instance.
func (b *InMemoryBackend) UpdateInstance(in *redispb.UpdateInstanceRequest) (*longrunningpb.Operation, error) {
	paths := in.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		return nil, errInvalid("update_mask is required.")
	}

	if in.GetInstance() == nil {
		return nil, errInvalid("instance is required.")
	}

	src := cloneInstance(in.GetInstance())

	b.mu.Lock("UpdateInstance")
	rec, n, err := b.readyLocked(in.GetInstance().GetName())

	if err == nil {
		err = b.updateLocked(rec, n, src, paths)
	}
	b.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return b.begin(n.parent(), "update", n.full(), b.settle(n.full(), nil))
}

// updateLocked validates and installs the updated instance as UPDATING. Must hold b.mu.
func (b *InMemoryBackend) updateLocked(rec *instRec, n resName, src *redispb.Instance, paths []string) error {
	cand := cloneInstance(rec.pb)
	if err := applyMask(cand, src, paths); err != nil {
		return err
	}

	now := b.now()

	if maskHas(paths, "persistence_config") {
		if err := applyPersistenceDefaults(cand, now); err != nil {
			return err
		}
	}

	if err := validateSpec(n.location, cand); err != nil {
		return err
	}

	if maskHas(paths, "maintenance_policy") {
		stampPolicy(cand.GetMaintenancePolicy(), now)
	}

	b.applyAuthLocked(rec, cand)
	deriveTopology(cand)
	cand.State = redispb.Instance_UPDATING
	rec.pb = cand

	return nil
}

func stampPolicy(mp *redispb.MaintenancePolicy, now time.Time) {
	if mp == nil {
		return
	}

	if mp.GetCreateTime() == nil {
		mp.CreateTime = timestamppb.New(now)
	}

	mp.UpdateTime = timestamppb.New(now)
}

// applyAuthLocked reconciles the engine's AUTH requirement with cand.auth_enabled. Must hold b.mu.
func (b *InMemoryBackend) applyAuthLocked(rec *instRec, cand *redispb.Instance) {
	switch {
	case cand.GetAuthEnabled() && rec.authString == "":
		rec.authString = newAuthString()
	case !cand.GetAuthEnabled():
		rec.authString = ""
	}

	if rec.engine != nil {
		rec.engine.Mini.RequireAuth(rec.authString)
	}
}

// UpgradeInstance moves an instance to a newer Redis version.
func (b *InMemoryBackend) UpgradeInstance(in *redispb.UpgradeInstanceRequest) (*longrunningpb.Operation, error) {
	target := in.GetRedisVersion()
	if versionRank(target) < 0 {
		return nil, errInvalid("redis_version %q is not supported.", target)
	}

	b.mu.Lock("UpgradeInstance")
	rec, n, err := b.readyLocked(in.GetName())

	if err == nil && versionRank(target) <= versionRank(rec.pb.GetRedisVersion()) {
		err = errInvalid("Cannot upgrade %s to %s: the target must be newer.", rec.pb.GetRedisVersion(), target)
	}

	if err == nil {
		rec.pb.State = redispb.Instance_UPDATING
	}
	b.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return b.begin(n.parent(), "upgrade", n.full(), b.settle(n.full(), func(r *instRec) {
		r.pb.RedisVersion = target
	}))
}

// FailoverInstance swaps the primary and replica zones of a STANDARD_HA instance.
func (b *InMemoryBackend) FailoverInstance(in *redispb.FailoverInstanceRequest) (*longrunningpb.Operation, error) {
	b.mu.Lock("FailoverInstance")
	rec, n, err := b.readyLocked(in.GetName())

	if err == nil && rec.pb.GetTier() != redispb.Instance_STANDARD_HA {
		err = errPrecondition("Failover requires the STANDARD_HA tier.")
	}

	if err == nil {
		rec.pb.State = redispb.Instance_FAILING_OVER
	}
	b.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return b.begin(n.parent(), "failover", n.full(), b.settle(n.full(), func(r *instRec) {
		r.pb.CurrentLocationId = otherZone(r.pb)
		deriveTopology(r.pb)
	}))
}
