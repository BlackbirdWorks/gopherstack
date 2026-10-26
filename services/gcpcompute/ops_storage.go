package gcpcompute

import (
	computeapi "google.golang.org/api/compute/v1"
)

type resizeRequest struct {
	SizeGb int64 `json:"sizeGb,string"`
}

func (h *Handler) registerStorageOps() {
	h.registerDiskOps()
	h.registerSnapshotOps()
	h.registerImageOps()
}

func (h *Handler) registerDiskOps() {
	z := scopeZone
	h.scopedList("disks", z, "compute#diskList", "compute#diskAggregatedList", "disks", zoneScopes(),
		func(b *InMemoryBackend, p, s, l string) []entry { return b.listDisks(p, s, l) })
	h.reg("disks", z, verbGet, func(h *Handler, rq *request) (any, error) {
		return h.Backend.getDisk(rq.rt.project, rq.rt.loc, rq.rt.name)
	})
	h.reg("disks", z, verbInsert, func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.Disk](rq)
		if err != nil {
			return nil, err
		}

		if in.SourceImage == "" {
			in.SourceImage = rq.q.Get("sourceImage")
		}

		return opResult(h.Backend.insertDisk(rq.rt.project, rq.rt.loc, in))
	})
	h.reg("disks", z, verbDelete, func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteDisk(rq.rt.project, rq.rt.loc, rq.rt.name))
	})
	h.reg("disks", z, "resize", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[resizeRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.resizeDisk(rq.rt.project, rq.rt.loc, rq.rt.name, in.SizeGb))
	})
	h.reg("disks", z, "setLabels", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[labelsRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setDiskLabels(rq.rt.project, rq.rt.loc, rq.rt.name, *in))
	})
	h.reg("disks", z, "createSnapshot", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.Snapshot](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.createSnapshot(rq.rt.project, rq.rt.loc, rq.rt.name, in))
	})
}

func (h *Handler) registerSnapshotOps() {
	g := scopeGlobal
	h.scopedList("snapshots", g, "compute#snapshotList", "", "", nil,
		func(b *InMemoryBackend, p, _, _ string) []entry { return b.listSnapshots(p) })
	h.reg("snapshots", g, verbGet, func(h *Handler, rq *request) (any, error) {
		return h.Backend.getSnapshot(rq.rt.project, rq.rt.name)
	})
	h.reg("snapshots", g, verbDelete, func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteSnapshot(rq.rt.project, rq.rt.name))
	})
	h.reg("snapshots", g, "setLabels", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[labelsRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setSnapshotLabels(rq.rt.project, rq.rt.name, *in))
	})
}

func (h *Handler) registerImageOps() {
	g := scopeGlobal
	h.scopedList("images", g, "compute#imageList", "", "", nil,
		func(b *InMemoryBackend, p, _, _ string) []entry { return b.listImages(p) })
	h.reg("images", g, verbGet, func(h *Handler, rq *request) (any, error) {
		return h.Backend.getImage(rq.rt.project, rq.rt.name)
	})
	h.reg("images", g, "getFromFamily", func(h *Handler, rq *request) (any, error) {
		return h.Backend.getImageFromFamily(rq.rt.project, rq.rt.name)
	})
	h.reg("images", g, verbInsert, func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.Image](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.insertImage(rq.rt.project, in))
	})
	h.reg("images", g, verbDelete, func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteImage(rq.rt.project, rq.rt.name))
	})
	h.reg("images", g, "setLabels", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[labelsRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setImageLabels(rq.rt.project, rq.rt.name, *in))
	})
}
