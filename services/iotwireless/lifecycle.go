package iotwireless

import "time"

// SetLifecycleDelay sets the dwell per FUOTA stage after StartFuotaTask
// (FuotaSession_Waiting, then In_FuotaSession, then FuotaDone). Zero (the default)
// leaves a started task in FuotaSession_Waiting.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

func observeFuotaStatus(ft *FuotaTask) string {
	if ft.Status != "FuotaSession_Waiting" || ft.stageDelay <= 0 || ft.startedAt.IsZero() {
		return ft.Status
	}

	switch elapsed := time.Since(ft.startedAt); {
	case elapsed >= 2*ft.stageDelay:
		return "FuotaDone"
	case elapsed >= ft.stageDelay:
		return "In_FuotaSession"
	default:
		return ft.Status
	}
}
