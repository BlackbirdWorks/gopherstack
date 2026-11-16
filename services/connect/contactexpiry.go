package connect

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	reasonExpired       = "EXPIRED"
	defaultTaskLifetime = 7 * 24 * time.Hour
)

type contactExpirySegment struct {
	ValueMap map[string]struct {
		ValueInteger *int64 `json:"ValueInteger"`
	} `json:"ValueMap"`
}

// expiryFromSegments reads connect:ContactExpiry.ExpiryDuration (minutes) from segment attributes.
func expiryFromSegments(raw jsontext.Value) (time.Duration, bool) {
	if len(raw) == 0 {
		return 0, false
	}

	segs := map[string]jsontext.Value{}
	if json.Unmarshal(raw, &segs, jsonx.V1()) != nil {
		return 0, false
	}

	var seg contactExpirySegment
	if json.Unmarshal(segs[expiryAttribute], &seg, jsonx.V1()) != nil {
		return 0, false
	}

	if d := seg.ValueMap["ExpiryDuration"]; d.ValueInteger != nil && *d.ValueInteger > 0 {
		return time.Duration(*d.ValueInteger) * time.Minute, true
	}

	return 0, false
}

// armExpiryLocked sets the deadline of a new contact: an explicit expiry, else seven days for tasks.
func (b *Backend) armExpiryLocked(c *contactRec) {
	d, ok := expiryFromSegments(c.Contact.SegmentAttributes)
	if !ok && c.Contact.Channel == channelTask {
		d, ok = defaultTaskLifetime, true
	}

	if !ok {
		return
	}

	t := awstime.NewEpochTime(c.Contact.InitiationTimestamp.Add(d))
	c.ExpiresAt, b.expiring = &t, true
}

// ExpireContacts disconnects active contacts whose expiry has passed with reason EXPIRED.
func (b *Backend) ExpireContacts() {
	b.mu.RLock("ExpireContacts")
	due := b.expiring && b.hasDueContactLocked()
	b.mu.RUnlock()

	if !due {
		return
	}

	b.mu.Lock("ExpireContacts")
	defer b.mu.Unlock()

	now, pending := b.now(), false

	for _, c := range b.contacts.table.All() {
		if c.ExpiresAt == nil || !c.active() {
			continue
		}

		if c.ExpiresAt.After(now) {
			pending = true

			continue
		}

		t := *c.ExpiresAt
		c.Contact.DisconnectTimestamp, c.Contact.DisconnectReason = &t, reasonExpired
		b.finishContactLocked(c, t)
		b.touch(c)
	}

	b.expiring = pending
}

func (b *Backend) hasDueContactLocked() bool {
	now := b.now()

	due := false

	b.contacts.table.Range(func(c *contactRec) bool {
		due = c.ExpiresAt != nil && c.active() && !c.ExpiresAt.After(now)

		return !due
	})

	return due
}
