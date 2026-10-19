package globalaccelerator

import (
	"cmp"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	dnsSuffix          = ".awsglobalaccelerator.com"
	dualStackDNSSuffix = ".dualstack.awsglobalaccelerator.com"
	dnsHexLen          = 16

	// Static anycast-like pools: deterministic first-free allocation, two addresses per family.
	poolV4Prefix  = "75.2."
	poolV6Prefix  = "2600:9000:a000::"
	poolSize      = 65535
	addrsPerFamly = 2
	byteMask      = 0xff
	byteBits      = 8
)

// AcceleratorInput holds CreateAccelerator parameters.
type AcceleratorInput struct {
	Tags          map[string]string
	Name          string
	IPAddressType string
	IPAddresses   []string
	Enabled       bool
	Custom        bool
}

// AcceleratorPatch holds UpdateAccelerator parameters; nil fields are unchanged.
type AcceleratorPatch struct {
	Name          *string
	Enabled       *bool
	IPAddressType *string
	IPAddresses   []string
}

// AttributesPatch holds UpdateAcceleratorAttributes parameters.
type AttributesPatch struct {
	FlowLogsEnabled  *bool
	FlowLogsS3Bucket *string
	FlowLogsS3Prefix *string
}

// DualStackDNSName returns the dual-stack DNS name, or empty for an IPv4-only accelerator.
func (a *Accelerator) DualStackDNSName() string {
	if a.IPAddressType != ipTypeDualStack {
		return ""
	}

	return strings.TrimSuffix(a.DNSName, dnsSuffix) + dualStackDNSSuffix
}

func (b *InMemoryBackend) viewAcceleratorLocked(a *Accelerator) *Accelerator {
	cp := cloneAccelerator(a)
	cp.Status = statusDeployed

	if b.now().Before(a.PendingUntil) {
		cp.Status = statusInProgress
	}

	return cp
}

func (b *InMemoryBackend) acceleratorLocked(arn string, custom bool) (*Accelerator, error) {
	a, ok := b.accelerators.Get(arn)
	if !ok || a.Custom != custom {
		return nil, fail(ErrAcceleratorNotFound, "accelerator %s not found", arn)
	}

	return a, nil
}

func (b *InMemoryBackend) touchAcceleratorLocked(arn string) {
	if a, ok := b.accelerators.Get(arn); ok {
		now := b.now()
		a.LastModifiedTime = now
		a.PendingUntil = b.pendingUntil(now)
	}
}

func (b *InMemoryBackend) countAcceleratorsLocked(custom bool) int {
	n := 0

	b.accelerators.Range(func(a *Accelerator) bool {
		if a.Custom == custom {
			n++
		}

		return true
	})

	return n
}

// CreateAccelerator creates a standard or custom routing accelerator.
func (b *InMemoryBackend) CreateAccelerator(in AcceleratorInput) (*Accelerator, error) {
	if err := validateName(in.Name); err != nil {
		return nil, err
	}

	if in.IPAddressType == "" {
		in.IPAddressType = ipTypeV4
	}

	if err := validateIPAddressType(in.IPAddressType, in.Custom); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateAccelerator")
	defer b.mu.Unlock()

	if b.countAcceleratorsLocked(in.Custom) >= maxAccelerators {
		return nil, fail(ErrLimitExceeded, "the account already has %d accelerators", maxAccelerators)
	}

	v4, err := b.resolveV4Locked("", in.IPAddresses)
	if err != nil {
		return nil, err
	}

	var v6 []string

	if in.IPAddressType == ipTypeDualStack {
		if v6, err = b.allocateLocked(poolV6, addrsPerFamly); err != nil {
			return nil, err
		}
	}

	now := b.now()
	a := &Accelerator{
		ARN:              b.acceleratorARN(newID()),
		Name:             in.Name,
		IPAddressType:    in.IPAddressType,
		Enabled:          in.Enabled,
		DNSName:          awsid.NewN("a", dnsHexLen) + dnsSuffix,
		IPv4:             v4,
		IPv6:             v6,
		Tags:             in.Tags,
		Custom:           in.Custom,
		CreatedTime:      now,
		LastModifiedTime: now,
		PendingUntil:     b.pendingUntil(now),
		Seq:              b.nextSeqLocked(),
	}
	b.accelerators.Put(a)

	return b.viewAcceleratorLocked(a), nil
}

// GetAccelerator returns one accelerator.
func (b *InMemoryBackend) GetAccelerator(arn string, custom bool) (*Accelerator, error) {
	b.mu.RLock("GetAccelerator")
	defer b.mu.RUnlock()

	a, err := b.acceleratorLocked(arn, custom)
	if err != nil {
		return nil, err
	}

	return b.viewAcceleratorLocked(a), nil
}

// ListAccelerators pages accelerators in creation order.
func (b *InMemoryBackend) ListAccelerators(
	custom bool,
	token string,
	maxResults *int32,
) (page.Page[*Accelerator], error) {
	b.mu.RLock("ListAccelerators")
	defer b.mu.RUnlock()

	all := b.sortedAcceleratorsLocked(custom)

	pg, err := paginate(all, token, maxResults)
	if err != nil {
		return page.Page[*Accelerator]{}, err
	}

	out := page.Page[*Accelerator]{Next: pg.Next, Data: make([]*Accelerator, len(pg.Data))}
	for i, a := range pg.Data {
		out.Data[i] = b.viewAcceleratorLocked(a)
	}

	return out, nil
}

// UpdateAccelerator applies a partial update.
func (b *InMemoryBackend) UpdateAccelerator(arn string, custom bool, p AcceleratorPatch) (*Accelerator, error) {
	if p.Name != nil {
		if err := validateName(*p.Name); err != nil {
			return nil, err
		}
	}

	if p.IPAddressType != nil {
		if err := validateIPAddressType(*p.IPAddressType, custom); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("UpdateAccelerator")
	defer b.mu.Unlock()

	a, err := b.acceleratorLocked(arn, custom)
	if err != nil {
		return nil, err
	}

	if len(p.IPAddresses) > 0 {
		v4, rerr := b.resolveV4Locked(arn, p.IPAddresses)
		if rerr != nil {
			return nil, rerr
		}

		a.IPv4 = v4
	}

	if p.IPAddressType != nil && *p.IPAddressType != a.IPAddressType {
		if err = b.changeIPTypeLocked(a, *p.IPAddressType); err != nil {
			return nil, err
		}
	}

	if p.Name != nil {
		a.Name = *p.Name
	}

	if p.Enabled != nil {
		a.Enabled = *p.Enabled
	}

	b.touchAcceleratorLocked(arn)

	return b.viewAcceleratorLocked(a), nil
}

func (b *InMemoryBackend) changeIPTypeLocked(a *Accelerator, to string) error {
	msg := fmt.Sprintf("IP address type changed from %s to %s", a.IPAddressType, to)

	if to == ipTypeDualStack {
		v6, err := b.allocateLocked(poolV6, addrsPerFamly)
		if err != nil {
			return err
		}

		a.IPv6 = v6
	} else {
		a.IPv6 = nil
	}

	a.IPAddressType = to
	a.Events = append(a.Events, Event{Message: msg, Timestamp: b.now()})

	return nil
}

// DeleteAccelerator removes a disabled accelerator that has no listeners.
func (b *InMemoryBackend) DeleteAccelerator(arn string, custom bool) error {
	b.mu.Lock("DeleteAccelerator")
	defer b.mu.Unlock()

	a, err := b.acceleratorLocked(arn, custom)
	if err != nil {
		return err
	}

	if a.Enabled {
		return fail(ErrAcceleratorNotDisabled, "the accelerator %s must be disabled before it is deleted", arn)
	}

	if len(b.listenersOfLocked(arn)) > 0 {
		return fail(ErrAssociatedListenerFound, "the accelerator %s still has listeners", arn)
	}

	b.accelerators.Delete(arn)

	return nil
}

// GetAcceleratorAttributes returns flow log settings.
func (b *InMemoryBackend) GetAcceleratorAttributes(arn string, custom bool) (*Accelerator, error) {
	return b.GetAccelerator(arn, custom)
}

// UpdateAcceleratorAttributes updates flow log settings.
func (b *InMemoryBackend) UpdateAcceleratorAttributes(
	arn string,
	custom bool,
	p AttributesPatch,
) (*Accelerator, error) {
	b.mu.Lock("UpdateAcceleratorAttributes")
	defer b.mu.Unlock()

	a, err := b.acceleratorLocked(arn, custom)
	if err != nil {
		return nil, err
	}

	enabled, bucket, prefix := a.FlowLogsEnabled, a.FlowLogsS3Bucket, a.FlowLogsS3Prefix

	if p.FlowLogsEnabled != nil {
		enabled = *p.FlowLogsEnabled
	}

	if p.FlowLogsS3Bucket != nil {
		bucket = *p.FlowLogsS3Bucket
	}

	if p.FlowLogsS3Prefix != nil {
		prefix = *p.FlowLogsS3Prefix
	}

	if enabled && (bucket == "" || prefix == "") {
		return nil, fail(
			ErrInvalidArgument,
			"flowLogsS3Bucket and flowLogsS3Prefix are required when flow logs are enabled",
		)
	}

	a.FlowLogsEnabled, a.FlowLogsS3Bucket, a.FlowLogsS3Prefix = enabled, bucket, prefix
	b.touchAcceleratorLocked(arn)

	return b.viewAcceleratorLocked(a), nil
}

type addrPool int

const (
	poolV4 addrPool = iota
	poolV6
)

func poolAddr(pool addrPool, n int) string {
	if pool == poolV4 {
		return fmt.Sprintf("%s%d.%d", poolV4Prefix, n>>byteBits, n&byteMask)
	}

	addr, err := netip.ParseAddr(fmt.Sprintf("%s%x:%x", poolV6Prefix, n>>byteBits, n&byteMask))
	if err != nil {
		return ""
	}

	return addr.String()
}

func (b *InMemoryBackend) usedAddrsLocked(exceptARN string) map[string]bool {
	used := make(map[string]bool)

	b.accelerators.Range(func(a *Accelerator) bool {
		if a.ARN != exceptARN {
			for _, ip := range slices.Concat(a.IPv4, a.IPv6) {
				used[ip] = true
			}
		}

		return true
	})

	return used
}

// allocateLocked returns the first count free addresses of pool in ascending order.
func (b *InMemoryBackend) allocateLocked(pool addrPool, count int) ([]string, error) {
	used := b.usedAddrsLocked("")
	out := make([]string, 0, count)

	for n := 1; n < poolSize && len(out) < count; n++ {
		addr := poolAddr(pool, n)
		if pool == poolV4 && (n&byteMask == 0 || n&byteMask == byteMask) {
			continue
		}

		if !used[addr] {
			out = append(out, addr)
		}
	}

	if len(out) < count {
		return nil, fail(ErrLimitExceeded, "no static IP addresses are left in the pool")
	}

	return out, nil
}

// resolveV4Locked fills the IPv4 set: supplied BYOIP addresses first, then pool addresses.
func (b *InMemoryBackend) resolveV4Locked(exceptARN string, supplied []string) ([]string, error) {
	if len(supplied) > addrsPerFamly {
		return nil, fail(ErrInvalidArgument, "at most %d IP addresses can be specified", addrsPerFamly)
	}

	used := b.usedAddrsLocked(exceptARN)
	out := make([]string, 0, addrsPerFamly)

	for _, ip := range supplied {
		if err := b.checkBYOIPAddrLocked(ip, used, out); err != nil {
			return nil, err
		}

		out = append(out, ip)
	}

	if len(out) == addrsPerFamly {
		return out, nil
	}

	pool, err := b.allocateLocked(poolV4, addrsPerFamly-len(out))
	if err != nil {
		return nil, err
	}

	return append(out, pool...), nil
}

func (b *InMemoryBackend) checkBYOIPAddrLocked(ip string, used map[string]bool, picked []string) error {
	addr, err := netip.ParseAddr(ip)
	if err != nil || !addr.Is4() {
		return fail(ErrInvalidArgument, "%q is not an IPv4 address", ip)
	}

	if used[ip] || slices.Contains(picked, ip) {
		return fail(ErrInvalidArgument, "IP address %s is already in use", ip)
	}

	now := b.now()
	usable := false

	b.byoip.Range(func(c *ByoipCidr) bool {
		p, perr := netip.ParsePrefix(c.Cidr)
		st := byoipEffectiveState(c, now)

		if perr == nil && p.Contains(addr) && (st == byoipReady || st == byoipAdvertising) {
			usable = true

			return false
		}

		return true
	})

	if !usable {
		return fail(ErrInvalidArgument, "IP address %s is not in a provisioned BYOIP range", ip)
	}

	return nil
}

func (b *InMemoryBackend) sortedAcceleratorsLocked(custom bool) []*Accelerator {
	var all []*Accelerator

	b.accelerators.Range(func(a *Accelerator) bool {
		if a.Custom == custom {
			all = append(all, a)
		}

		return true
	})

	slices.SortFunc(all, func(x, y *Accelerator) int { return cmp.Compare(x.Seq, y.Seq) })

	return all
}
