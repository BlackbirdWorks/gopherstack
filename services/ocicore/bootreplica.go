package ocicore

import (
	"net/http"
	"slices"
	"time"
)

// BootVolumeReplica is the asynchronous copy of a boot volume in another availability domain.
type BootVolumeReplica struct {
	TimeLastSynced time.Time `json:"timeLastSynced"`
	Base
	AvailabilityDomain        string `json:"availabilityDomain"`
	BootVolumeID              string `json:"bootVolumeId"`
	ImageID                   string `json:"imageId,omitempty"`
	KmsKeyID                  string `json:"kmsKeyId,omitempty"`
	VolumeGroupReplicaID      string `json:"volumeGroupReplicaId,omitempty"`
	SizeInGBs                 int64  `json:"sizeInGBs"`
	TotalDataTransferredInGBs int64  `json:"totalDataTransferredInGBs"`
}

// BootVolumeReplicaInfo is the replica summary embedded in a boot volume.
type BootVolumeReplicaInfo struct {
	DisplayName         string `json:"displayName"`
	BootVolumeReplicaID string `json:"bootVolumeReplicaId"`
	AvailabilityDomain  string `json:"availabilityDomain"`
	KmsKeyID            string `json:"kmsKeyId,omitempty"`
}

func (v *BootVolumeReplica) vcnRef() string { return "" }

func (v *BootVolumeReplica) view() Resource {
	c := *v
	c.Internal = nil

	return &c
}

func (b *InMemoryBackend) checkBootReplicas(bv *BootVolume, want []ReplicaDetailsIn) *APIError {
	if aerr := b.checkReplicaDetails(bv.AvailabilityDomain, want, "boot volume"); aerr != nil {
		return aerr
	}

	if len(want) > 0 || len(bv.BootVolumeReplicas) > 0 {
		for _, cur := range bv.BootVolumeReplicas {
			if r, ok := b.res[cur.BootVolumeReplicaID].(*BootVolumeReplica); ok && live(r) &&
				r.VolumeGroupReplicaID != "" {
				return errConflict("The boot volume replica " + r.ID + " is managed by volume group replica " +
					r.VolumeGroupReplicaID)
			}
		}
	}

	return nil
}

// applyBootReplicas makes the replicas of a boot volume match want; callers validated want.
func (b *InMemoryBackend) applyBootReplicas(bv *BootVolume, want []ReplicaDetailsIn) {
	var keep []BootVolumeReplicaInfo

	for _, cur := range bv.BootVolumeReplicas {
		i := slices.IndexFunc(
			want,
			func(w ReplicaDetailsIn) bool { return w.AvailabilityDomain == cur.AvailabilityDomain },
		)
		if i < 0 {
			if r, ok := b.res[cur.BootVolumeReplicaID].(*BootVolumeReplica); ok && live(r) {
				b.markTerminating(r)
			}

			continue
		}

		if r, ok := b.res[cur.BootVolumeReplicaID].(*BootVolumeReplica); ok && want[i].DisplayName != "" {
			r.DisplayName, cur.DisplayName = want[i].DisplayName, want[i].DisplayName
			b.bump(r)
		}

		keep = append(keep, cur)
		want = slices.Delete(slices.Clone(want), i, i+1)
	}

	for _, w := range want {
		keep = append(keep, b.newBootReplica(bv, w, ""))
	}

	bv.BootVolumeReplicas = keep
}

func (b *InMemoryBackend) newBootReplica(
	bv *BootVolume,
	w ReplicaDetailsIn,
	groupReplicaID string,
) BootVolumeReplicaInfo {
	name := cmpOrStr(w.DisplayName, bv.DisplayName+" (Replica)")
	r := &BootVolumeReplica{
		Base: b.newBase(kindBootReplica, bv.CompartmentID, name, TagsIn{}), AvailabilityDomain: w.AvailabilityDomain,
		BootVolumeID: bv.ID, ImageID: bv.ImageID, KmsKeyID: w.XrrKmsKeyID, SizeInGBs: bv.SizeInGBs,
		VolumeGroupReplicaID: groupReplicaID,
	}
	r.TimeLastSynced = r.TimeCreated
	b.add(r)

	return BootVolumeReplicaInfo{
		DisplayName:         name,
		BootVolumeReplicaID: r.ID,
		AvailabilityDomain:  w.AvailabilityDomain,
		KmsKeyID:            w.XrrKmsKeyID,
	}
}

func (b *InMemoryBackend) growBootReplicas(bv *BootVolume) {
	for _, info := range bv.BootVolumeReplicas {
		if r, ok := b.res[info.BootVolumeReplicaID].(*BootVolumeReplica); ok && live(r) {
			r.SizeInGBs = bv.SizeInGBs
			b.bump(r)
		}
	}
}

func (b *InMemoryBackend) endBootReplicas(bv *BootVolume) {
	for _, info := range bv.BootVolumeReplicas {
		if r, ok := b.res[info.BootVolumeReplicaID].(*BootVolumeReplica); ok && live(r) {
			b.markTerminating(r)
		}
	}
}

func (b *InMemoryBackend) bootReplicaSource(id, ad string) (bootSource, *APIError) {
	r, aerr := getLive[*BootVolumeReplica](b, id)
	if aerr != nil {
		return bootSource{}, aerr
	}

	if r.LifecycleState != statAvailable {
		return bootSource{}, errState("The boot volume replica " + r.ID + " is " + r.LifecycleState)
	}

	if ad != "" && ad != r.AvailabilityDomain {
		return bootSource{}, errParam(
			"A boot volume restored from a replica must be in the availability domain of the replica",
		)
	}

	return bootSource{imageID: r.ImageID, ad: r.AvailabilityDomain, state: statRestoring, sizeGBs: r.SizeInGBs}, nil
}

func bootReplicaEntries() []entry {
	return []entry{
		{bootReplicaList, http.MethodGet, "/bootVolumeReplicas", "ListBootVolumeReplicas"},
		{
			getOp[*BootVolumeReplica]("bootVolumeReplicaId"), http.MethodGet,
			idPath("/bootVolumeReplicas", "bootVolumeReplicaId"), "GetBootVolumeReplica",
		},
	}
}

func bootReplicaList(h *Handler, q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	o.AnyCompartment = o.Compartment == ""
	o.Match = and(
		queryEq(q, "availabilityDomain", func(r Resource) string {
			x, _ := r.(*BootVolumeReplica)

			return x.AvailabilityDomain
		}),
		queryEq(q, "volumeGroupReplicaId", func(r Resource) string {
			x, _ := r.(*BootVolumeReplica)

			return x.VolumeGroupReplicaID
		}),
	)

	pg, aerr := list[*BootVolumeReplica](h.Backend, o)
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}
