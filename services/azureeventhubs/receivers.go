package azureeventhubs

import (
	"fmt"
	"strconv"
	"strings"
)

// ReceiverReg is a partition receiver attached to a consumer group; kicked closes when a newer epoch takes over.
type ReceiverReg struct {
	epoch  *int64
	kicked chan struct{}
	key    string
	name   string
	reason string
	done   bool
}

// Kicked is closed when a higher-epoch receiver supersedes this one.
func (r *ReceiverReg) Kicked() <-chan struct{} { return r.kicked }

// Reason describes why the receiver was disconnected; valid once Kicked is closed.
func (r *ReceiverReg) Reason() string { return r.reason }

func (r *ReceiverReg) kick(reason string) {
	if r.done {
		return
	}

	r.done = true
	r.reason = reason

	close(r.kicked)
}

func receiverKey(ref HubRef, cg string, pid int) string {
	return ref.Namespace + "/" + ref.Hub + "/" + cg + "/" + strconv.Itoa(pid)
}

func (b *InMemoryBackend) kickAllLocked(key string) {
	for _, r := range b.receivers[key] {
		r.kick("the receiver was disconnected because its entity was removed")
	}

	delete(b.receivers, key)
}

func (b *InMemoryBackend) dropHubReceiversLocked(ref HubRef) {
	prefix := ref.Namespace + "/" + ref.Hub + "/"

	for key := range b.receivers {
		if strings.HasPrefix(key, prefix) {
			b.kickAllLocked(key)
		}
	}

	ns, found := b.namespaces[ref.Namespace]
	if !found {
		return
	}

	if h, hubFound := ns.Hubs[ref.Hub]; hubFound {
		for _, p := range h.Partitions {
			p.wake()
		}
	}
}

// RegisterReceiver admits a receiver to a partition of a consumer group,
// applying epoch (owner level) arbitration.
func (b *InMemoryBackend) RegisterReceiver(
	ref HubRef,
	cg string,
	pid int,
	name string,
	epoch *int64,
) (*ReceiverReg, error) {
	b.mu.Lock("RegisterReceiver")
	defer b.mu.Unlock()

	h, _, err := b.partLocked(ref, pid)
	if err != nil {
		return nil, err
	}

	if _, ok := h.ConsumerGroups[cg]; !ok {
		return nil, ErrConsumerGroupNotFound
	}

	key := receiverKey(ref, cg, pid)
	reg := &ReceiverReg{key: key, name: name, epoch: epoch, kicked: make(chan struct{})}

	if admitErr := b.admitLocked(reg); admitErr != nil {
		return nil, admitErr
	}

	b.receivers[key] = append(b.receivers[key], reg)

	return reg, nil
}

func (b *InMemoryBackend) admitLocked(reg *ReceiverReg) error {
	cur := b.receivers[reg.key]

	if reg.epoch == nil {
		return admitPlain(cur)
	}

	for _, r := range cur {
		if r.epoch != nil && *r.epoch > *reg.epoch {
			return fmt.Errorf("%w: receiver %q with epoch %d already exists; %q with epoch %d cannot be created",
				ErrReceiverStolen, r.name, *r.epoch, reg.name, *reg.epoch)
		}
	}

	kept := cur[:0:0]

	for _, r := range cur {
		if r.epoch == nil || *r.epoch <= *reg.epoch {
			r.kick(
				fmt.Sprintf(
					"New receiver %q with higher epoch of '%d' is created hence current receiver %q is getting disconnected.",
					reg.name,
					*reg.epoch,
					r.name,
				),
			)

			continue
		}

		kept = append(kept, r)
	}

	b.receivers[reg.key] = kept

	return nil
}

func admitPlain(cur []*ReceiverReg) error {
	for _, r := range cur {
		if r.epoch != nil {
			return fmt.Errorf(
				"%w: an epoch receiver %q with epoch %d owns this partition",
				ErrReceiverStolen,
				r.name,
				*r.epoch,
			)
		}
	}

	if len(cur) >= maxReceiversPerGroup {
		return ErrTooManyReceivers
	}

	return nil
}

// UnregisterReceiver removes a receiver registration.
func (b *InMemoryBackend) UnregisterReceiver(reg *ReceiverReg) {
	b.mu.Lock("UnregisterReceiver")
	defer b.mu.Unlock()

	cur := b.receivers[reg.key]
	for i, r := range cur {
		if r == reg {
			b.receivers[reg.key] = append(cur[:i:i], cur[i+1:]...)

			break
		}
	}

	if len(b.receivers[reg.key]) == 0 {
		delete(b.receivers, reg.key)
	}
}
