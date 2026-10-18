package globalaccelerator

import (
	"cmp"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	listenerIDLen      = 8
	endpointGroupIDLen = 12
)

// ListenerInput holds create/update listener parameters; nil/empty fields are unchanged on update.
type ListenerInput struct {
	Protocol       string
	ClientAffinity string
	PortRanges     []PortRange
	Custom         bool
}

func (b *InMemoryBackend) listenerLocked(arn string, custom bool) (*Listener, error) {
	l, ok := b.listeners.Get(arn)
	if !ok || l.Custom != custom {
		return nil, fail(ErrListenerNotFound, "listener %s not found", arn)
	}

	return l, nil
}

func (b *InMemoryBackend) listenersOfLocked(acceleratorARN string) []*Listener {
	var out []*Listener

	b.listeners.Range(func(l *Listener) bool {
		if l.AcceleratorARN == acceleratorARN {
			out = append(out, l)
		}

		return true
	})

	slices.SortFunc(out, func(x, y *Listener) int { return cmp.Compare(x.Seq, y.Seq) })

	return out
}

// checkPortConflictsLocked rejects ranges that overlap a sibling listener with the same protocol.
func (b *InMemoryBackend) checkPortConflictsLocked(acceleratorARN, selfARN, protocol string, ranges []PortRange) error {
	for _, other := range b.listenersOfLocked(acceleratorARN) {
		if other.ARN == selfARN || other.Protocol != protocol {
			continue
		}

		for _, r := range ranges {
			for _, o := range other.PortRanges {
				if rangesOverlap(r, o) {
					return fail(
						ErrInvalidPortRange,
						"port range %d-%d overlaps listener %s",
						r.FromPort,
						r.ToPort,
						other.ARN,
					)
				}
			}
		}
	}

	return nil
}

func (in *ListenerInput) normalize() error {
	if err := validatePortRanges(in.PortRanges); err != nil {
		return err
	}

	if in.Custom {
		return nil
	}

	if err := validateProtocol(in.Protocol); err != nil {
		return err
	}

	switch in.ClientAffinity {
	case "":
		in.ClientAffinity = affinityNone
	case affinityNone, affinitySourceIP:
	default:
		return fail(ErrInvalidArgument, "clientAffinity %q must be NONE or SOURCE_IP", in.ClientAffinity)
	}

	return nil
}

// CreateListener adds a listener to an accelerator.
func (b *InMemoryBackend) CreateListener(acceleratorARN string, in ListenerInput) (*Listener, error) {
	if err := in.normalize(); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateListener")
	defer b.mu.Unlock()

	if _, err := b.acceleratorLocked(acceleratorARN, in.Custom); err != nil {
		return nil, err
	}

	if len(b.listenersOfLocked(acceleratorARN)) >= maxListeners {
		return nil, fail(ErrLimitExceeded, "an accelerator can have at most %d listeners", maxListeners)
	}

	if err := b.checkPortConflictsLocked(acceleratorARN, "", in.Protocol, in.PortRanges); err != nil {
		return nil, err
	}

	l := &Listener{
		ARN:            acceleratorARN + "/listener/" + newListenerID(),
		AcceleratorARN: acceleratorARN,
		PortRanges:     slices.Clone(in.PortRanges),
		Protocol:       in.Protocol,
		ClientAffinity: in.ClientAffinity,
		Custom:         in.Custom,
		Seq:            b.nextSeqLocked(),
	}
	b.listeners.Put(l)
	b.touchAcceleratorLocked(acceleratorARN)

	return cloneListener(l), nil
}

// GetListener returns one listener.
func (b *InMemoryBackend) GetListener(arn string, custom bool) (*Listener, error) {
	b.mu.RLock("GetListener")
	defer b.mu.RUnlock()

	l, err := b.listenerLocked(arn, custom)
	if err != nil {
		return nil, err
	}

	return cloneListener(l), nil
}

// ListListeners pages the listeners of an accelerator.
func (b *InMemoryBackend) ListListeners(
	acceleratorARN string, custom bool, token string, maxResults *int32,
) (page.Page[*Listener], error) {
	b.mu.RLock("ListListeners")
	defer b.mu.RUnlock()

	if _, err := b.acceleratorLocked(acceleratorARN, custom); err != nil {
		return page.Page[*Listener]{}, err
	}

	pg, err := paginate(b.listenersOfLocked(acceleratorARN), token, maxResults)
	if err != nil {
		return pg, err
	}

	for i, l := range pg.Data {
		pg.Data[i] = cloneListener(l)
	}

	return pg, nil
}

// UpdateListener changes the supplied listener fields.
func (b *InMemoryBackend) UpdateListener(arn string, in ListenerInput) (*Listener, error) {
	b.mu.Lock("UpdateListener")
	defer b.mu.Unlock()

	l, err := b.listenerLocked(arn, in.Custom)
	if err != nil {
		return nil, err
	}

	next := ListenerInput{
		Custom:         l.Custom,
		PortRanges:     l.PortRanges,
		Protocol:       l.Protocol,
		ClientAffinity: l.ClientAffinity,
	}

	if len(in.PortRanges) > 0 {
		next.PortRanges = in.PortRanges
	}

	if in.Protocol != "" {
		next.Protocol = in.Protocol
	}

	if in.ClientAffinity != "" {
		next.ClientAffinity = in.ClientAffinity
	}

	if err = next.normalize(); err != nil {
		return nil, err
	}

	if err = b.checkPortConflictsLocked(l.AcceleratorARN, arn, next.Protocol, next.PortRanges); err != nil {
		return nil, err
	}

	if l.Custom {
		if err = b.checkCapacityLocked(arn, next.PortRanges); err != nil {
			return nil, err
		}
	}

	l.PortRanges, l.Protocol, l.ClientAffinity = slices.Clone(next.PortRanges), next.Protocol, next.ClientAffinity
	b.touchAcceleratorLocked(l.AcceleratorARN)

	return cloneListener(l), nil
}

// DeleteListener removes a listener that has no endpoint groups.
func (b *InMemoryBackend) DeleteListener(arn string, custom bool) error {
	b.mu.Lock("DeleteListener")
	defer b.mu.Unlock()

	l, err := b.listenerLocked(arn, custom)
	if err != nil {
		return err
	}

	if len(b.groupsOfLocked(arn)) > 0 {
		return fail(ErrAssociatedEndpointGroupFound, "the listener %s still has endpoint groups", arn)
	}

	b.listeners.Delete(arn)
	b.touchAcceleratorLocked(l.AcceleratorARN)

	return nil
}

func newListenerID() string { return awsid.NewN("", listenerIDLen) }
