package ecs

import (
	"context"
	"time"
)

// purgePlan captures the deletions computed under the read lock so the write
// lock is held only long enough to apply them, rather than across the full
// nested scan of every task-definition family and revision.
type purgePlan struct {
	// revByFamily maps a task-definition family to the set of revision ARNs that
	// are older than the cutoff and eligible for removal.
	revByFamily map[string][]string
	// clusters is the list of cluster keys whose CreatedAt precedes the cutoff.
	clusters []string
}

// Purge removes all ECS resources created before the given cutoff time.
//
// The expensive scan over every task-definition family/revision is performed
// under a read lock (phase 1) to build a deletion plan; the write lock is then
// taken only to apply that plan (phase 2), so it is held O(deletions) rather
// than O(all revisions). Deletions are re-validated in phase 2 so a resource
// re-created between the phases is not purged.
func (b *InMemoryBackend) Purge(_ context.Context, cutoff time.Time) {
	b.mu.RLock("Purge-plan")
	plan := b.buildPurgePlanLocked(cutoff)
	b.mu.RUnlock()

	b.mu.Lock("Purge-apply")
	removed := b.applyPurgePlanLocked(plan, cutoff)
	b.mu.Unlock()

	// Notify hooks (e.g. reconciler semaphore eviction) after releasing the lock.
	b.fireClusterDeleteHooks(removed...)
}

// buildPurgePlanLocked scans state and returns the set of clusters and
// task-definition revisions eligible for purging. Must be called with at least
// the read lock held.
func (b *InMemoryBackend) buildPurgePlanLocked(cutoff time.Time) purgePlan {
	plan := purgePlan{revByFamily: make(map[string][]string)}

	for name, c := range b.clusters {
		if c.CreatedAt.Before(cutoff) {
			plan.clusters = append(plan.clusters, name)
		}
	}

	for family, revs := range b.taskDefinitions {
		for _, td := range revs {
			if td.RegisteredAt.Before(cutoff) {
				plan.revByFamily[family] = append(plan.revByFamily[family], td.TaskDefinitionArn)
			}
		}
	}

	return plan
}

// applyPurgePlanLocked applies a purge plan, re-validating each deletion against
// the cutoff, and returns the cluster keys actually removed. Must be called with
// the write lock held.
func (b *InMemoryBackend) applyPurgePlanLocked(plan purgePlan, cutoff time.Time) []string {
	removed := make([]string, 0, len(plan.clusters))

	for _, name := range plan.clusters {
		c, ok := b.clusters[name]
		if !ok || !c.CreatedAt.Before(cutoff) {
			// Cluster was removed or re-created between the plan and apply phases.
			continue
		}

		b.purgeClusterLocked(name)
		removed = append(removed, name)
	}

	for family, arns := range plan.revByFamily {
		b.purgeTaskDefRevisionsLocked(family, arns, cutoff)
	}

	return removed
}

// purgeClusterLocked removes a cluster and every resource scoped to it,
// including the service-deployment, task-set, service-revision, task-protection,
// lifecycle, attribute, reverse-index, and daemon entries that older code paths
// leaked. Must be called with the write lock held.
func (b *InMemoryBackend) purgeClusterLocked(name string) {
	if svcs, ok := b.services[name]; ok {
		for svcName, svc := range svcs {
			delete(b.serviceIndex, svcRef{cluster: name, name: svcName})
			b.deleteServiceDeploymentsForServiceLocked(svc.ServiceArn)
			delete(b.taskSets, svc.ServiceArn)

			for _, rev := range b.serviceRevisions[svc.ServiceArn] {
				delete(b.serviceRevisionsByArn, rev.ServiceRevisionArn)
			}

			delete(b.serviceRevisions, svc.ServiceArn)
		}
	}

	for taskArn := range b.tasks[name] {
		delete(b.taskProtections, taskArn)
		delete(b.lifecycle, taskArn)
	}

	b.purgeDaemonsLocked(name)

	delete(b.clusters, name)
	delete(b.services, name)
	delete(b.tasks, name)
	delete(b.containerInstances, name)
	delete(b.attributes, name)
	delete(b.tasksByInstance, name)
}

// purgeDaemonsLocked removes all daemons for a cluster along with their task
// definitions, revisions, and deployment records. Must be called with the write
// lock held.
func (b *InMemoryBackend) purgeDaemonsLocked(clusterName string) {
	daemons, ok := b.daemons[clusterName]
	if !ok {
		return
	}

	daemonArns := make(map[string]bool, len(daemons))

	for _, d := range daemons {
		daemonArns[d.DaemonArn] = true
		delete(b.daemonTaskDefs, d.DaemonArn)
		delete(b.daemonRevisions, d.DaemonArn)
	}

	for id, dep := range b.daemonDeployments {
		if daemonArns[dep.DaemonArn] {
			delete(b.daemonDeployments, id)
		}
	}

	delete(b.daemons, clusterName)
}

// purgeTaskDefRevisionsLocked removes the given (already-identified) revisions
// from a single family, re-checking the cutoff so a revision registered between
// the plan and apply phases is preserved. Only the affected family's slice is
// touched — O(revisions in family), not O(all revisions). Must be called with
// the write lock held.
func (b *InMemoryBackend) purgeTaskDefRevisionsLocked(family string, arns []string, cutoff time.Time) {
	revs, ok := b.taskDefinitions[family]
	if !ok {
		return
	}

	remove := make(map[string]bool, len(arns))
	for _, a := range arns {
		remove[a] = true
	}

	kept := make([]*TaskDefinition, 0, len(revs))

	for _, td := range revs {
		if remove[td.TaskDefinitionArn] && td.RegisteredAt.Before(cutoff) {
			delete(b.taskDefByArn, td.TaskDefinitionArn)

			continue
		}

		kept = append(kept, td)
	}

	if len(kept) == 0 {
		delete(b.taskDefinitions, family)
	} else {
		b.taskDefinitions[family] = kept
	}
}
