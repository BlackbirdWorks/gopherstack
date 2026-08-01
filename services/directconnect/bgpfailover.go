package directconnect

import (
	"slices"
	"time"
)

// vifTestStatusInProgress / vifTestStatusCompleted are this backend's
// values for VifTestHistory.Status -- a free-form *string on the wire (no
// typed enum exists, PARITY.md), so these are a defensible, documented
// choice rather than a verified AWS value.
const (
	vifTestStatusInProgress = "in progress"
	vifTestStatusCompleted  = "completed"
)

// defaultFailoverTestMinutes is used when the caller omits
// TestDurationInMinutes -- unspecified in this SDK (PARITY.md); 3 minutes
// is a defensible, documented default, not a verified AWS one.
const defaultFailoverTestMinutes = 3

// StartBgpFailoverTest puts the named BGP peers (or every peer on the VIF,
// if bgpPeers is empty) into BgpStatus "down" and the VIF itself into
// VirtualInterfaceState "testing" for durationMinutes, auto-reverting on
// expiry unless StopBgpFailoverTest ends it first.
func (b *InMemoryBackend) StartBgpFailoverTest(
	vifID string, bgpPeers []string, durationMinutes *int32,
) (*VifTestHistory, error) {
	b.mu.Lock("StartBgpFailoverTest")
	defer b.mu.Unlock()

	v, ok := b.virtualInterfaces.Get(vifID)
	if !ok {
		return nil, notFoundError(resourceVif, vifID)
	}

	targetIDs := bgpPeers
	if len(targetIDs) == 0 {
		targetIDs = make([]string, 0, len(v.BgpPeers))
		for _, p := range v.BgpPeers {
			targetIDs = append(targetIDs, p.BgpPeerID)
		}
	}

	for _, p := range v.BgpPeers {
		if slices.Contains(targetIDs, p.BgpPeerID) {
			p.BgpStatus = BGPStatusDown
		}
	}

	v.VirtualInterfaceState = VifStateTesting

	duration := int32(defaultFailoverTestMinutes)
	if durationMinutes != nil && *durationMinutes > 0 {
		duration = *durationMinutes
	}

	now := time.Now().UTC()
	test := &VifTestHistory{
		TestID:                newTestID(),
		VirtualInterfaceID:    vifID,
		Status:                vifTestStatusInProgress,
		OwnerAccount:          v.OwnerAccount,
		BgpPeers:              cloneStrs(targetIDs),
		StartTime:             &now,
		TestDurationInMinutes: duration,
	}
	b.vifTests.Put(test)

	b.work.After("vifTest:"+test.TestID, time.Duration(duration)*time.Minute, func() {
		b.mu.Lock("bgpFailoverTestExpiry")
		b.revertBgpTestLocked(test)
		b.mu.Unlock()
	})

	return test.clone(), nil
}

// revertBgpTestLocked ends test (if not already ended by a prior call --
// explicit StopBgpFailoverTest or a previous timer firing) and restores its
// peers' BgpStatus and the parent VIF's VirtualInterfaceState. Callers must
// hold b.mu.
func (b *InMemoryBackend) revertBgpTestLocked(test *VifTestHistory) {
	if test.EndTime != nil {
		return
	}

	now := time.Now().UTC()
	test.EndTime = &now
	test.Status = vifTestStatusCompleted

	v, ok := b.virtualInterfaces.Get(test.VirtualInterfaceID)
	if !ok {
		return
	}

	for _, p := range v.BgpPeers {
		if slices.Contains(test.BgpPeers, p.BgpPeerID) {
			p.BgpStatus = BGPStatusUp
		}
	}

	if v.VirtualInterfaceState == VifStateTesting {
		v.VirtualInterfaceState = VifStateAvailable
	}
}

// StopBgpFailoverTest ends the open failover test for vifID, if any.
func (b *InMemoryBackend) StopBgpFailoverTest(vifID string) (*VifTestHistory, error) {
	b.mu.Lock("StopBgpFailoverTest")
	defer b.mu.Unlock()

	test := b.openTestForVifLocked(vifID)
	if test == nil {
		return nil, notFoundError(resourceVifTest, vifID)
	}

	b.revertBgpTestLocked(test)

	return test.clone(), nil
}

// openTestForVifLocked returns the most recent test for vifID with no
// EndTime set, or nil. Callers must hold b.mu.
func (b *InMemoryBackend) openTestForVifLocked(vifID string) *VifTestHistory {
	var latest *VifTestHistory

	for _, t := range b.vifTestsByVif.Get(vifID) {
		if t.EndTime != nil {
			continue
		}

		if latest == nil || t.StartTime.After(*latest.StartTime) {
			latest = t
		}
	}

	return latest
}

// ListVirtualInterfaceTestHistory returns test-history records, optionally
// filtered by vifID/testID/status and any overlap with the bgpPeers filter.
func (b *InMemoryBackend) ListVirtualInterfaceTestHistory(
	vifID, testID, status string, bgpPeers []string,
) []*VifTestHistory {
	b.mu.RLock("ListVirtualInterfaceTestHistory")
	defer b.mu.RUnlock()

	var candidates []*VifTestHistory
	if vifID != "" {
		candidates = b.vifTestsByVif.Get(vifID)
	} else {
		candidates = b.vifTests.Snapshot()
	}

	out := make([]*VifTestHistory, 0, len(candidates))

	for _, t := range candidates {
		if testID != "" && t.TestID != testID {
			continue
		}

		if status != "" && t.Status != status {
			continue
		}

		if len(bgpPeers) > 0 && !overlaps(t.BgpPeers, bgpPeers) {
			continue
		}

		out = append(out, t.clone())
	}

	return out
}

// overlaps reports whether a and b share at least one element.
func overlaps(a, b []string) bool {
	for _, x := range a {
		if slices.Contains(b, x) {
			return true
		}
	}

	return false
}
