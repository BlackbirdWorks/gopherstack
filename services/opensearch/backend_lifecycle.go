package opensearch

import "time"

// clock returns the backend's current time, honouring an injected clock when set.
func (b *InMemoryBackend) clock() time.Time {
	if b.now != nil {
		return b.now()
	}

	return time.Now()
}

// SetProcessingDelay configures how long create/update/upgrade/delete lifecycle
// windows remain observable before settling to their terminal state. A delay of
// 0 (the default) keeps the historical fast behaviour: transitions still run
// through the real code path but settle immediately.
func (b *InMemoryBackend) SetProcessingDelay(d time.Duration) {
	b.mu.Lock("SetProcessingDelay")
	defer b.mu.Unlock()

	b.processingDelay = d
}

// SetClock installs a deterministic clock, primarily for tests. Passing nil
// restores the default time.Now clock.
func (b *InMemoryBackend) SetClock(fn func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = fn
}

// beginProcessing opens a processing window on the domain with the given
// DomainProcessingStatus. The caller must hold the write lock.
func (b *InMemoryBackend) beginProcessing(d *Domain, status string) {
	d.ProcessingStatus = status
	d.ProcessingUntil = b.clock().Add(b.processingDelay)
}

// domainProcessing reports the resolved processing / upgrade-processing flags
// and DomainProcessingStatus for a domain copy at the given instant. It never
// mutates stored state: the window naturally expires as the clock advances past
// ProcessingUntil.
func domainProcessing(d *Domain, now time.Time) (bool, bool, string) {
	if d.ProcessingUntil.IsZero() || !now.Before(d.ProcessingUntil) {
		return false, false, domainStatusActive
	}

	if d.ProcessingStatus == dpsUpgrading {
		return true, true, dpsUpgrading
	}

	return true, false, d.ProcessingStatus
}

// deleteWindowElapsed reports whether a domain marked for deletion has passed
// the end of its deleting window and should now be treated as gone.
func deleteWindowElapsed(d *Domain, now time.Time) bool {
	return d.Deleted && !now.Before(d.ProcessingUntil)
}

// statusWindowElapsed reports whether a sub-resource in a transient DELETING
// state has passed the end of its window and should now be treated as removed.
func statusWindowElapsed(status string, until, now time.Time) bool {
	return status == statusDeleting && !until.IsZero() && !now.Before(until)
}

// removeDomainLocked performs the full cascade removal of a domain and all its
// domain-scoped resources. The caller must hold the write lock.
func (b *InMemoryBackend) removeDomainLocked(name string) {
	d, ok := b.domains[name]
	if !ok {
		return
	}

	delete(b.domains, name)
	delete(b.arnIndex, d.ARN)

	if d.Tags != nil {
		d.Tags.Close()
	}

	// Cascade-clean all domain-scoped resources.
	delete(b.domainDataSources, name)
	delete(b.vpcAuthorizations, name)

	for pkgID := range b.domainPackages[name] {
		if domains, has := b.packageAssociations[pkgID]; has {
			delete(domains, name)
			if len(domains) == 0 {
				delete(b.packageAssociations, pkgID)
			}
		}
	}
	delete(b.domainPackages, name)

	delete(b.domainMaintenances, name)
	delete(b.upgradeHistory, upgradeHistoryKey(name))
	delete(b.autoTunes, autoTuneKey(name))
	delete(b.dryRuns, name)
	delete(b.domainIndexes, name)

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Deregister(d.Endpoint)
	}
}

// purgeExpiredDomainsLocked finalises any domains whose deleting window has
// elapsed, cascading their removal. The caller must hold the write lock.
func (b *InMemoryBackend) purgeExpiredDomainsLocked() {
	now := b.clock()

	for name, d := range b.domains {
		if deleteWindowElapsed(d, now) {
			b.removeDomainLocked(name)
		}
	}
}
