package ocicore

import (
	"net/http"
	"slices"
)

const maxReplicasPerVolume = 1

// ReplicaDetailsIn is a blockVolumeReplicas entry of CreateVolume and UpdateVolume.
type ReplicaDetailsIn struct {
	AvailabilityDomain string `json:"availabilityDomain"`
	DisplayName        string `json:"displayName"`
	XrrKmsKeyID        string `json:"xrrKmsKeyId"`
}

func (b *InMemoryBackend) checkReplicas(vol *Volume, want []ReplicaDetailsIn) *APIError {
	if len(want) > maxReplicasPerVolume {
		return errLimit("A block volume can have at most 1 replica")
	}

	for _, r := range want {
		switch {
		case !blockAdRe.MatchString(r.AvailabilityDomain):
			return errParam("availabilityDomain " + r.AvailabilityDomain + " is not a valid availability domain")
		case r.AvailabilityDomain == vol.AvailabilityDomain:
			return errParam("A replica must be in a different availability domain than its volume")
		}

		if aerr := validName(r.DisplayName); aerr != nil {
			return aerr
		}

		if aerr := b.checkKmsKey(r.XrrKmsKeyID); aerr != nil {
			return aerr
		}
	}

	return nil
}

// applyReplicas makes the replicas of a volume match want; callers validated want with checkReplicas.
func (b *InMemoryBackend) applyReplicas(vol *Volume, want []ReplicaDetailsIn) {
	var keep []BlockVolumeReplicaInfo

	for _, cur := range vol.BlockVolumeReplicas {
		i := slices.IndexFunc(
			want,
			func(w ReplicaDetailsIn) bool { return w.AvailabilityDomain == cur.AvailabilityDomain },
		)
		if i < 0 {
			if r, ok := b.res[cur.BlockVolumeReplicaID].(*BlockVolumeReplica); ok && live(r) {
				b.markTerminating(r)
			}

			continue
		}

		keep = append(keep, b.renameReplica(cur, want[i]))
		want = slices.Delete(slices.Clone(want), i, i+1)
	}

	for _, w := range want {
		keep = append(keep, b.newReplica(vol, w))
	}

	vol.BlockVolumeReplicas = keep
}

func (b *InMemoryBackend) renameReplica(cur BlockVolumeReplicaInfo, w ReplicaDetailsIn) BlockVolumeReplicaInfo {
	if r, ok := b.res[cur.BlockVolumeReplicaID].(*BlockVolumeReplica); ok && w.DisplayName != "" {
		r.DisplayName, cur.DisplayName = w.DisplayName, w.DisplayName
		b.bump(r)
	}

	return cur
}

func (b *InMemoryBackend) newReplica(vol *Volume, w ReplicaDetailsIn) BlockVolumeReplicaInfo {
	name := cmpOrStr(w.DisplayName, vol.DisplayName+" (Replica)")
	r := &BlockVolumeReplica{
		Base: b.newBase(kindReplica, vol.CompartmentID, name, TagsIn{}), AvailabilityDomain: w.AvailabilityDomain,
		BlockVolumeID: vol.ID, KmsKeyID: w.XrrKmsKeyID, SizeInGBs: vol.SizeInGBs,
	}
	r.TimeLastSynced = r.TimeCreated
	b.add(r)

	return BlockVolumeReplicaInfo{
		DisplayName:          name,
		BlockVolumeReplicaID: r.ID,
		AvailabilityDomain:   w.AvailabilityDomain,
		KmsKeyID:             w.XrrKmsKeyID,
	}
}

// growReplicas keeps replica sizes in step with their source volume.
func (b *InMemoryBackend) growReplicas(vol *Volume) {
	for _, info := range vol.BlockVolumeReplicas {
		if r, ok := b.res[info.BlockVolumeReplicaID].(*BlockVolumeReplica); ok && live(r) {
			r.SizeInGBs = vol.SizeInGBs
			b.bump(r)
		}
	}
}

// endReplicas terminates the replicas of a volume that is being deleted.
func (b *InMemoryBackend) endReplicas(vol *Volume) {
	for _, info := range vol.BlockVolumeReplicas {
		if r, ok := b.res[info.BlockVolumeReplicaID].(*BlockVolumeReplica); ok && live(r) {
			b.markTerminating(r)
		}
	}
}

func replicaFilter(q *req) func(resource) bool {
	return and(
		queryEq(q, "availabilityDomain", func(r resource) string {
			x, _ := r.(*BlockVolumeReplica)

			return x.AvailabilityDomain
		}),
		queryEq(q, "volumeGroupReplicaId", func(resource) string { return "" }),
	)
}

// replicaList is ListBlockVolumeReplicas: compartmentId is optional.
func replicaList(h *Handler, q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	o.AnyCompartment, o.Match = o.Compartment == "", replicaFilter(q)

	pg, aerr := list[*BlockVolumeReplica](h.Backend, o)
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func replicaEntries() []entry {
	return []entry{
		{replicaList, http.MethodGet, "/blockVolumeReplicas", "ListBlockVolumeReplicas"},
		{getOp[*BlockVolumeReplica]("blockVolumeReplicaId"), http.MethodGet,
			idPath("/blockVolumeReplicas", "blockVolumeReplicaId"), "GetBlockVolumeReplica"},
	}
}
