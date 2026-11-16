package ocicore

import (
	"net/http"
	"slices"
	"time"
)

const (
	memberAddPending    = "ADD_PENDING"
	memberStable        = "STABLE"
	memberRemovePending = "REMOVE_PENDING"
)

// MemberReplica names one volume or boot volume replica of a volume group replica.
type MemberReplica struct {
	VolumeReplicaID string `json:"volumeReplicaId"`
	MembershipState string `json:"membershipState,omitempty"`
}

// VolumeGroupReplica is the asynchronous copy of a volume group in another availability domain.
type VolumeGroupReplica struct {
	TimeLastSynced time.Time `json:"timeLastSynced"`
	Base
	AvailabilityDomain string          `json:"availabilityDomain"`
	VolumeGroupID      string          `json:"volumeGroupId"`
	KmsKeyID           string          `json:"kmsKeyId,omitempty"`
	MemberReplicas     []MemberReplica `json:"memberReplicas"`
	SizeInGBs          int64           `json:"sizeInGBs"`
}

// VolumeGroupReplicaInfo is the replica summary embedded in a volume group.
type VolumeGroupReplicaInfo struct {
	DisplayName          string `json:"displayName"`
	VolumeGroupReplicaID string `json:"volumeGroupReplicaId"`
	AvailabilityDomain   string `json:"availabilityDomain"`
	KmsKeyID             string `json:"kmsKeyId,omitempty"`
}

func (v *VolumeGroupReplica) vcnRef() string { return "" }

func (v *VolumeGroupReplica) view() Resource {
	c := *v
	c.Internal, c.KmsKeyID = nil, ""
	c.MemberReplicas = slices.Clone(v.MemberReplicas)

	return &c
}

func (b *InMemoryBackend) checkGroupReplicas(g *VolumeGroup, want []ReplicaDetailsIn) *APIError {
	if aerr := b.checkReplicaDetails(g.AvailabilityDomain, want, "volume group"); aerr != nil {
		return aerr
	}

	for _, w := range want {
		if aerr := validName(w.DisplayName); aerr != nil {
			return aerr
		}

		if aerr := b.checkKmsKey(w.XrrKmsKeyID); aerr != nil {
			return aerr
		}
	}

	if len(want) == 0 {
		return nil
	}

	for _, id := range g.VolumeIDs {
		if aerr := b.ownReplicaBlock(id, g); aerr != nil {
			return aerr
		}
	}

	return nil
}

// ownReplicaBlock refuses group replication while a member already has a replica of its own.
func (b *InMemoryBackend) ownReplicaBlock(id string, g *VolumeGroup) *APIError {
	for _, info := range b.memberReplicaIDs(b.res[id]) {
		if r, ok := b.res[info].(groupReplicated); ok && live(r) && r.groupReplica() == "" {
			return errConflict("The volume " + id + " has its own replica " + info +
				"; remove it before replicating volume group " + g.ID)
		}
	}

	return nil
}

type groupReplicated interface {
	Resource
	groupReplica() string
}

func (v *BlockVolumeReplica) groupReplica() string { return v.VolumeGroupReplicaID }
func (v *BootVolumeReplica) groupReplica() string  { return v.VolumeGroupReplicaID }

func (b *InMemoryBackend) memberReplicaIDs(r Resource) []string {
	var out []string

	switch v := r.(type) {
	case *Volume:
		for _, i := range v.BlockVolumeReplicas {
			out = append(out, i.BlockVolumeReplicaID)
		}
	case *BootVolume:
		for _, i := range v.BootVolumeReplicas {
			out = append(out, i.BootVolumeReplicaID)
		}
	}

	return out
}

// applyGroupReplicas makes the replicas of a group match want; callers validated want with checkGroupReplicas.
func (b *InMemoryBackend) applyGroupReplicas(g *VolumeGroup, want []ReplicaDetailsIn) {
	var keep []VolumeGroupReplicaInfo

	for _, cur := range g.VolumeGroupReplicas {
		i := slices.IndexFunc(
			want,
			func(w ReplicaDetailsIn) bool { return w.AvailabilityDomain == cur.AvailabilityDomain },
		)
		if i < 0 {
			b.endGroupReplica(cur.VolumeGroupReplicaID)

			continue
		}

		if r, ok := b.res[cur.VolumeGroupReplicaID].(*VolumeGroupReplica); ok && want[i].DisplayName != "" {
			r.DisplayName, cur.DisplayName = want[i].DisplayName, want[i].DisplayName
			b.bump(r)
		}

		keep = append(keep, cur)
		want = slices.Delete(slices.Clone(want), i, i+1)
	}

	for _, w := range want {
		keep = append(keep, b.newGroupReplica(g, w))
	}

	g.VolumeGroupReplicas = keep
}

func (b *InMemoryBackend) newGroupReplica(g *VolumeGroup, w ReplicaDetailsIn) VolumeGroupReplicaInfo {
	name := cmpOrStr(w.DisplayName, g.DisplayName+" (Replica)")
	gr := &VolumeGroupReplica{
		Base:               b.newBase(kindGroupReplica, g.CompartmentID, name, TagsIn{}),
		AvailabilityDomain: w.AvailabilityDomain, VolumeGroupID: g.ID, KmsKeyID: w.XrrKmsKeyID,
		MemberReplicas: []MemberReplica{},
	}
	gr.TimeLastSynced = gr.TimeCreated
	b.add(gr)
	b.syncGroupReplica(g, gr)

	return VolumeGroupReplicaInfo{
		DisplayName:          name,
		VolumeGroupReplicaID: gr.ID,
		AvailabilityDomain:   w.AvailabilityDomain,
		KmsKeyID:             w.XrrKmsKeyID,
	}
}

// syncGroupReplica creates replicas for new members, ends those of removed members and refreshes the
// membership states and size of one group replica.
func (b *InMemoryBackend) syncGroupReplica(g *VolumeGroup, gr *VolumeGroupReplica) {
	if !live(gr) {
		return
	}

	have := map[string]string{}

	for _, m := range gr.MemberReplicas {
		have[m.VolumeReplicaID] = m.MembershipState
	}

	for _, id := range g.VolumeIDs {
		if !b.memberHasReplicaIn(id, gr) {
			b.replicateMember(id, gr)
		}
	}

	var out []MemberReplica

	var size int64

	for _, r := range b.res {
		x, ok := r.(groupReplicated)
		if !ok || x.groupReplica() != gr.ID {
			continue
		}

		state, sz := b.memberState(g, x)
		if state == "" {
			continue
		}

		size += sz
		out = append(out, MemberReplica{VolumeReplicaID: x.base().ID, MembershipState: state})
	}

	slices.SortFunc(out, func(a, c MemberReplica) int { return compareStr(a.VolumeReplicaID, c.VolumeReplicaID) })
	gr.MemberReplicas, gr.SizeInGBs = out, size
}

func compareStr(a, c string) int {
	switch {
	case a < c:
		return -1
	case a > c:
		return 1
	}

	return 0
}

// memberState is the membership state of a member replica, empty once it is gone.
func (b *InMemoryBackend) memberState(g *VolumeGroup, x groupReplicated) (string, int64) {
	var src string

	var size int64

	switch v := x.(type) {
	case *BlockVolumeReplica:
		src, size = v.BlockVolumeID, v.SizeInGBs
	case *BootVolumeReplica:
		src, size = v.BootVolumeID, v.SizeInGBs
	}

	switch {
	case !live(x):
		if x.base().LifecycleState == statTerminating {
			return memberRemovePending, 0
		}

		return "", 0
	case !slices.Contains(g.VolumeIDs, src):
		b.markTerminating(x)

		return memberRemovePending, 0
	case x.base().LifecycleState == statProvisioning:
		return memberAddPending, size
	}

	return memberStable, size
}

func (b *InMemoryBackend) memberHasReplicaIn(id string, gr *VolumeGroupReplica) bool {
	for _, rid := range b.memberReplicaIDs(b.res[id]) {
		if r, ok := b.res[rid].(groupReplicated); ok && live(r) && r.groupReplica() == gr.ID {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) replicateMember(id string, gr *VolumeGroupReplica) {
	w := ReplicaDetailsIn{AvailabilityDomain: gr.AvailabilityDomain, XrrKmsKeyID: gr.KmsKeyID}

	switch v := b.res[id].(type) {
	case *Volume:
		info := b.newReplica(v, w)
		if r, ok := b.res[info.BlockVolumeReplicaID].(*BlockVolumeReplica); ok {
			r.VolumeGroupReplicaID = gr.ID
		}

		v.BlockVolumeReplicas = append(v.BlockVolumeReplicas, info)
		b.bump(v)
	case *BootVolume:
		info := b.newBootReplica(v, w, gr.ID)
		v.BootVolumeReplicas = append(v.BootVolumeReplicas, info)
		b.bump(v)
	}
}

// sweepGroupReplicas keeps every live group replica in step with its group.
func (b *InMemoryBackend) sweepGroupReplicas() {
	for _, r := range b.res {
		gr, ok := r.(*VolumeGroupReplica)
		if !ok || !live(gr) {
			continue
		}

		if g, isGroup := b.res[gr.VolumeGroupID].(*VolumeGroup); isGroup && live(g) {
			b.syncGroupReplica(g, gr)
		}
	}
}

// endGroupReplica terminates a group replica and the member replicas it owns, and drops them from their volumes.
func (b *InMemoryBackend) endGroupReplica(id string) {
	gr, ok := b.res[id].(*VolumeGroupReplica)
	if !ok {
		return
	}

	for _, r := range b.res {
		x, owned := r.(groupReplicated)
		if !owned || x.groupReplica() != id {
			continue
		}

		if live(x) {
			b.markTerminating(x)
		}
	}

	if live(gr) {
		b.markTerminating(gr)
	}

	gr.MemberReplicas = []MemberReplica{}
}

func (b *InMemoryBackend) endGroupReplicas(g *VolumeGroup) {
	for _, info := range g.VolumeGroupReplicas {
		b.endGroupReplica(info.VolumeGroupReplicaID)
	}
}

// restoreFromGroupReplica creates a volume or boot volume from every live member replica.
func (b *InMemoryBackend) restoreFromGroupReplica(g *VolumeGroup, id string) ([]string, *APIError) {
	gr, aerr := getLive[*VolumeGroupReplica](b, id)
	if aerr != nil {
		return nil, aerr
	}

	if gr.LifecycleState != statAvailable {
		return nil, errState("The volume group replica " + gr.ID + " is " + gr.LifecycleState)
	}

	if gr.AvailabilityDomain != g.AvailabilityDomain {
		return nil, errParam("A volume group restored from a replica must be in the availability domain of the replica")
	}

	var ids []string

	for _, m := range gr.MemberReplicas {
		switch r := b.res[m.VolumeReplicaID].(type) {
		case *BlockVolumeReplica:
			ids = append(ids, b.restoreVolumeReplica(g, r).ID)
		case *BootVolumeReplica:
			ids = append(ids, b.restoreBootReplica(g, r).ID)
		}
	}

	slices.Sort(ids)

	return ids, nil
}

func (b *InMemoryBackend) restoreVolumeReplica(g *VolumeGroup, r *BlockVolumeReplica) *Volume {
	v := &Volume{
		Base:               b.newBase(kindVolume, g.CompartmentID, b.restoredName(r.BlockVolumeID), TagsIn{}),
		AvailabilityDomain: g.AvailabilityDomain, SizeInGBs: r.SizeInGBs, SizeInMBs: r.SizeInGBs * bytesGB,
		VpusPerGB: defaultVpus, SourceDetails: &VolumeSource{Type: srcBlockReplica, ID: r.ID}, VolumeGroupID: g.ID,
		KmsKeyID: r.KmsKeyID,
	}
	b.moveTo(v, statRestoring, statAvailable)
	b.add(v)

	return v
}

func (b *InMemoryBackend) restoreBootReplica(g *VolumeGroup, r *BootVolumeReplica) *BootVolume {
	bv := &BootVolume{
		Base:               b.newBase(kindBootVolume, g.CompartmentID, b.restoredName(r.BootVolumeID), TagsIn{}),
		AvailabilityDomain: g.AvailabilityDomain, ImageID: r.ImageID, SizeInGBs: r.SizeInGBs,
		SizeInMBs: r.SizeInGBs * bytesGB, VpusPerGB: defaultVpus, KmsKeyID: r.KmsKeyID, VolumeGroupID: g.ID,
		SourceDetails: &VolumeSource{Type: srcBootReplica, ID: r.ID},
	}
	b.moveTo(bv, statRestoring, statAvailable)
	b.add(bv)

	return bv
}

func groupReplicaEntries() []entry {
	return []entry{
		{groupReplicaList, http.MethodGet, "/volumeGroupReplicas", "ListVolumeGroupReplicas"},
		{
			getOp[*VolumeGroupReplica]("volumeGroupReplicaId"), http.MethodGet,
			idPath("/volumeGroupReplicas", "volumeGroupReplicaId"), "GetVolumeGroupReplica",
		},
	}
}

func groupReplicaList(h *Handler, q *req) error {
	if q.query("availabilityDomain") == "" {
		return errMissing("availabilityDomain is required")
	}

	o, err := q.listOpts()
	if err != nil {
		return err
	}

	o.Match = queryEq(q, "availabilityDomain", func(r Resource) string {
		x, _ := r.(*VolumeGroupReplica)

		return x.AvailabilityDomain
	})

	pg, aerr := list[*VolumeGroupReplica](h.Backend, o)
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}
