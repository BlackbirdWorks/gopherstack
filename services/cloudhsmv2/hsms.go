package cloudhsmv2

import (
	"context"
	"time"
)

func (b *InMemoryBackend) wireHsmLocked(h *Hsm) wireHsm {
	state, msg := hsmState(h, b.now())

	return wireHsm{
		HsmID: h.ID, AZ: h.Zone, ClusterID: h.ClusterID, EniID: h.EniID, EniIP: h.EniIP,
		HsmType: h.HsmType, State: state, StateMessage: msg, SubnetID: h.SubnetID,
	}
}

func hsmState(h *Hsm, now time.Time) (string, string) {
	switch {
	case !h.DeletedAt.IsZero():
		return stateDeleteInProgress, "The HSM is being deleted."
	case now.Before(h.ReadyAt):
		return stateCreateInProgress, "The HSM is being created."
	default:
		return stateActive, "The HSM is active."
	}
}

// CreateHsm adds an HSM to a cluster in the given Availability Zone.
func (b *InMemoryBackend) CreateHsm(ctx context.Context, clusterID, zone, ip string) (wireHsm, error) {
	if clusterID == "" || zone == "" {
		return wireHsm{}, fail(ErrInvalidRequest, "ClusterId and AvailabilityZone are required")
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("CreateHsm")
	defer b.mu.Unlock()

	b.reapLocked()

	c, err := b.clusterLocked(sc, clusterID)
	if err != nil {
		return wireHsm{}, err
	}

	now := b.now()

	switch state, _ := b.clusterStateLocked(c, now); state {
	case stateCreateInProgress, stateDeleteInProgress, stateModifyInProgress:
		return wireHsm{}, fail(ErrInvalidRequest, "cluster %s is %s and cannot accept a new HSM", clusterID, state)
	}

	subnetID, ok := c.SubnetMapping[zone]
	if !ok {
		return wireHsm{}, fail(ErrInvalidRequest, "Availability Zone %s is not part of cluster %s", zone, clusterID)
	}

	live := b.liveHsmsLocked(clusterID, now)
	for _, h := range live {
		if h.Zone == zone {
			return wireHsm{}, fail(ErrInvalidRequest, "cluster %s already has an HSM in %s", clusterID, zone)
		}
	}

	if len(live) >= maxHsms {
		return wireHsm{}, fail(ErrResourceLimitExceeded, "a cluster can have at most %d HSMs", maxHsms)
	}

	if b.network == nil {
		return wireHsm{}, fail(ErrInvalidRequest, "network interfaces cannot be created because EC2 is not available")
	}

	eniID, eniIP, err := b.network.CreateENI(sc.region, subnetID, ip, c.SecurityGroup, "CloudHSM Managed Interface")
	if err != nil {
		return wireHsm{}, fail(ErrInvalidRequest, "cannot create the HSM network interface: %v", err)
	}

	if c.HsmCert == "" && c.HsmKey != "" {
		hw, hwErr := issueHardwareCerts(c.ID, c.HsmKey, now)
		if hwErr != nil {
			b.network.DeleteENI(sc.region, eniID)

			return wireHsm{}, fail(ErrInternalFailure, "%v", hwErr)
		}

		c.HsmCert, c.AwsHwCert, c.MfrHwCert = hw.HSM, hw.AWS, hw.Manufacturer
	}

	h := &Hsm{
		ID: newHsmID(), ClusterID: clusterID, Account: sc.account, Region: sc.region, Zone: zone, SubnetID: subnetID,
		EniID: eniID, EniIP: eniIP, HsmType: c.HsmType, Created: now, ReadyAt: now.Add(b.dwell()),
	}
	b.hsms.Put(h)

	if c.Initialized && len(live) == 0 {
		b.takeBackupLocked(c, now)
	}

	return b.wireHsmLocked(h), nil
}

// DeleteHsmInput identifies the HSM to delete by one of its ID, ENI ID or ENI IP.
type DeleteHsmInput struct {
	ClusterID, HsmID, EniID, EniIP string
}

func (in DeleteHsmInput) find(hsms []*Hsm) *Hsm {
	for _, h := range hsms {
		if (in.HsmID != "" && h.ID == in.HsmID) || (in.EniID != "" && h.EniID == in.EniID) ||
			(in.EniIP != "" && h.EniIP == in.EniIP) {
			return h
		}
	}

	return nil
}

// DeleteHsm starts deleting an HSM and returns its ID.
func (b *InMemoryBackend) DeleteHsm(ctx context.Context, in DeleteHsmInput) (string, error) {
	if in.ClusterID == "" || (in.HsmID == "" && in.EniID == "" && in.EniIP == "") {
		return "", fail(ErrInvalidRequest, "ClusterId and one of HsmId, EniId or EniIp are required")
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("DeleteHsm")
	defer b.mu.Unlock()

	b.reapLocked()

	c, err := b.clusterLocked(sc, in.ClusterID)
	if err != nil {
		return "", err
	}

	now := b.now()
	live := b.liveHsmsLocked(c.ID, now)

	target := in.find(live)
	if target == nil {
		return "", fail(ErrResourceNotFound, "HSM not found in cluster %s", in.ClusterID)
	}

	if target.DeletedAt.IsZero() {
		if c.Initialized && len(b.activeOrPendingLocked(live)) == 1 {
			b.takeBackupLocked(c, now)
		}

		target.DeletedAt = now
		target.DeleteUntil = now.Add(b.dwell())
	}

	return target.ID, nil
}

func (b *InMemoryBackend) activeOrPendingLocked(live []*Hsm) []*Hsm {
	var out []*Hsm

	for _, h := range live {
		if h.DeletedAt.IsZero() {
			out = append(out, h)
		}
	}

	return out
}
