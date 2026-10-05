package kinesisvideo

import "time"

const transitionDelay = 500 * time.Millisecond

func (s *Stream) markUpdating(now time.Time) {
	if s.Status == statusActive {
		s.Status = statusUpdating
		s.PendingUntil = now.Add(transitionDelay)
	}
}

func (c *Channel) markUpdating(now time.Time) {
	if c.Status == statusActive {
		c.Status = statusUpdating
		c.PendingUntil = now.Add(transitionDelay)
	}
}

// sweepLocked advances every CREATING/UPDATING resource whose deadline passed
// to ACTIVE and removes DELETING ones. Caller must hold b.mu for writing.
func (b *InMemoryBackend) sweepLocked(now time.Time) {
	for _, s := range b.streams.All() {
		if now.Before(s.PendingUntil) {
			continue
		}

		switch s.Status {
		case statusCreating, statusUpdating:
			s.Status = statusActive
		case statusDeleting:
			b.streams.Delete(s.Name)
		}
	}

	for _, c := range b.channels.All() {
		if now.Before(c.PendingUntil) {
			continue
		}

		switch c.Status {
		case statusCreating, statusUpdating:
			c.Status = statusActive
		case statusDeleting:
			b.channels.Delete(c.Name)
		}
	}
}
