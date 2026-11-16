package lambdaweb

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

const (
	replicaStaggerDivisor = 4
	reasonReplicaCreating = "The Region's endpoint is being created."
)

// Replica is a MultiRegion or PerRegion endpoint's deployment in one non-home Region. The home Region's state is
// the endpoint's own.
type Replica struct {
	PendingUntil       time.Time
	UpdatePendingUntil time.Time
	State              string
	StateReason        string
	UpdateStatus       string
	UpdateReason       string
	Weights            []RevisionWeight
	NextWeights        []RevisionWeight
}

func (r *Replica) clone() *Replica {
	cp := *r
	cp.Weights = slices.Clone(r.Weights)
	cp.NextWeights = slices.Clone(r.NextWeights)

	return &cp
}

// keyARNIn returns the ARN of the multi-Region key's replica in region.
func keyARNIn(keyARN, region string) string {
	parts := strings.SplitN(keyARN, ":", 6) //nolint:mnd // ARN fields
	if len(parts) < 6 {                     //nolint:mnd // ARN fields
		return keyARN
	}

	parts[3] = region

	return strings.Join(parts, ":")
}

// replicationFailures reports, per non-home target Region, why replicating the revisions there would fail: the
// customer managed key needs a usable replica in every target Region.
func (b *InMemoryBackend) replicationFailures(
	ctx context.Context, regions []string, home string, revs []*Revision,
) map[string]string {
	b.mu.RLock("replicationFailures")
	keys := b.keys
	b.mu.RUnlock()

	out := map[string]string{}

	for _, region := range regions {
		if region == home {
			continue
		}

		for _, rev := range revs {
			if rev.KmsKeyArn == "" {
				continue
			}

			replica := keyARNIn(rev.KmsKeyArn, region)

			if keys != nil {
				if err := keys.CheckKey(ctx, region, replica); err != nil {
					out[region] = "The KMS key replica " + replica + " is not usable in " + region + ": " + err.Error()

					break
				}
			}

			if err := roleauth.AuthorizeResource(
				b.authorizer(), roleauth.PrincipalLambda, "kms:Decrypt", replica,
				revisionARN(region, b.accountID, rev.FunctionName, rev.ID),
			); err != nil {
				out[region] = "The key policy of " + replica + " does not allow " + ServicePrincipal + " to call kms:Decrypt."

				break
			}
		}
	}

	return out
}

// revisionsByID returns the named revisions of a function that exist.
func (b *InMemoryBackend) revisionsByID(region, fn string, ids []string) []*Revision {
	b.mu.RLock("revisionsByID")
	defer b.mu.RUnlock()

	var out []*Revision

	for _, id := range ids {
		if p, ok := parseWebARN(id); ok && p.kind == resRevision {
			id = p.child
		}

		if rev, ok := b.revisions.Get(revisionKey(region, fn, id)); ok {
			out = append(out, rev.clone())
		}
	}

	return out
}

func weightIDs(ws []RevisionWeight) []string {
	out := make([]string, 0, len(ws))
	for _, w := range ws {
		out = append(out, w.RevisionID)
	}

	return out
}

// seedReplicasLocked deploys a new endpoint to its non-home Regions; a Region that fails replication fails the
// endpoint, which then serves nothing anywhere.
func (b *InMemoryBackend) seedReplicasLocked(e *Endpoint, fails map[string]string, now time.Time) {
	if e.Type == endpointHomeRegion {
		return
	}

	e.Replicas = map[string]*Replica{}
	base := b.dwell(endpointActivationDelay)
	last := e.PendingUntil
	failed := []string{}

	for i, region := range e.Regions {
		if region == e.Region {
			continue
		}

		rep := &Replica{
			State: statePending, StateReason: reasonReplicaCreating, Weights: slices.Clone(e.Weights),
			PendingUntil: now.Add(base + time.Duration(i)*base/replicaStaggerDivisor),
		}

		if reason, bad := fails[region]; bad {
			rep.State, rep.StateReason, rep.PendingUntil, rep.Weights = stateFailed, reason, time.Time{}, nil
			failed = append(failed, region)
		}

		if rep.PendingUntil.After(last) {
			last = rep.PendingUntil
		}

		e.Replicas[region] = rep
	}

	e.PendingUntil = last

	if len(failed) > 0 {
		slices.Sort(failed)

		e.State = stateFailed
		e.StateReason = "Replication failed in " + strings.Join(failed, ", ") + ". See regionalEndpoints for details."
		e.PendingUntil = time.Time{}
	}
}

// redeployReplicasLocked starts a deployment of weights to every non-home Region of e. A Region that fails keeps
// serving what it last deployed and reports the failure in its own updateStatus.
func (b *InMemoryBackend) redeployReplicasLocked(
	e *Endpoint, weights []RevisionWeight, fails map[string]string, now time.Time,
) {
	if e.Type == endpointHomeRegion {
		return
	}

	base := b.dwell(endpointUpdateDelay)
	last := now.Add(base)

	for i, region := range e.Regions {
		rep := e.Replicas[region]
		if rep == nil || rep.State != stateActive {
			continue
		}

		if reason, bad := fails[region]; bad {
			rep.UpdateStatus, rep.UpdateReason, rep.NextWeights, rep.UpdatePendingUntil = updateFailed, reason, nil, time.Time{}

			continue
		}

		rep.UpdateStatus, rep.UpdateReason, rep.NextWeights = updateInProgress, "The Region's update is in progress.",
			slices.Clone(weights)
		rep.UpdatePendingUntil = now.Add(base + time.Duration(i)*base/replicaStaggerDivisor)

		if rep.UpdatePendingUntil.After(last) {
			last = rep.UpdatePendingUntil
		}
	}

	e.UpdatePendingUntil = last
}

// advanceReplicasLocked applies the replicas' due transitions.
func advanceReplicasLocked(e *Endpoint, now time.Time) {
	for _, rep := range e.Replicas {
		if rep.State == statePending && !rep.PendingUntil.IsZero() && !now.Before(rep.PendingUntil) {
			rep.State, rep.StateReason, rep.PendingUntil = stateActive, "", time.Time{}
		}

		if rep.UpdateStatus == updateInProgress && !now.Before(rep.UpdatePendingUntil) {
			rep.UpdateStatus, rep.UpdateReason, rep.Weights = updateSuccessful, "", rep.NextWeights
			rep.NextWeights, rep.UpdatePendingUntil = nil, time.Time{}
		}
	}
}

// updateOutcome folds the replicas' results into the endpoint's updateStatus once its update window has passed.
func finishEndpointUpdate(e *Endpoint) {
	var failed []string

	for region, rep := range e.Replicas {
		if rep.UpdateStatus == updateFailed {
			failed = append(failed, region)
		}
	}

	if len(failed) == 0 {
		e.UpdateStatus, e.UpdateReason, e.UpdatePendingUntil = updateSuccessful, "", time.Time{}

		return
	}

	slices.Sort(failed)

	e.UpdateStatus = updateFailed
	e.UpdateReason = "Update failed in " + strings.Join(failed, ", ") + ". See regionalEndpoints for details."
	e.UpdatePendingUntil = time.Time{}
}

// servingWeights returns the weights a Region serves: the endpoint's in the home Region, the last successful
// deployment elsewhere; ok is false while that Region cannot serve.
func servingWeights(e *Endpoint, region string, home []RevisionWeight) ([]RevisionWeight, bool) {
	if region == e.Region {
		return home, true
	}

	rep := e.Replicas[region]
	if rep == nil || rep.State != stateActive {
		return nil, false
	}

	return slices.Clone(rep.Weights), true
}

// updateReplicationFailures finds the Regions an update's revisions cannot replicate to.
func (b *InMemoryBackend) updateReplicationFailures(region, fn, epRef string, up EndpointUpdate) map[string]string {
	b.mu.RLock("updateReplicationFailures")

	e, err := b.endpointLocked(region, fn, epRef)
	if err != nil || e.Type == endpointHomeRegion {
		b.mu.RUnlock()

		return nil
	}

	regions, weights := slices.Clone(e.Regions), slices.Clone(e.Weights)
	b.mu.RUnlock()

	if up.WeightsSet {
		weights = up.Weights
	}

	return b.replicationFailures(context.Background(), regions, region, b.revisionsByID(region, fn, weightIDs(weights)))
}
