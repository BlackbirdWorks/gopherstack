package gcpcompute

import (
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

type resizeRequest struct {
	SizeGb int64 `json:"sizeGb,string"`
}

func (h *Handler) registerStorageOps() {
	h.registerDiskOps()
	h.registerSnapshotOps()
	h.registerImageOps()
	h.registerInstantSnapshotOps()
	h.registerMachineImageOps()
	h.registerReservationOps()
}

func (h *Handler) registerDiskOps() {
	aggScopes := append(zoneScopes(), regionScopes()...)
	list := func(b *InMemoryBackend, p, s, l string) []entry { return b.listDisks(p, s, l) }
	h.scopedList("disks", scopeZone, "compute#diskList", "compute#diskAggregatedList", "disks", aggScopes, list)
	h.scopedList("disks", scopeRegion, "compute#diskList", "", "", nil, list)

	for _, scope := range []string{scopeZone, scopeRegion} {
		h.registerDiskScope(scope)
	}
}

func (h *Handler) registerDiskScope(z string) {
	h.reg("disks", z, verbGet, func(h *Handler, rq *request) (any, error) {
		return h.Backend.getDisk(rq.rt.project, z, rq.rt.loc, rq.rt.name)
	})
	h.reg("disks", z, verbInsert, func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.Disk](rq)
		if err != nil {
			return nil, err
		}

		if in.SourceImage == "" {
			in.SourceImage = rq.q.Get("sourceImage")
		}

		return opResult(h.Backend.insertDisk(rq.rt.project, z, rq.rt.loc, in))
	})
	h.reg("disks", z, verbDelete, func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteDisk(rq.rt.project, z, rq.rt.loc, rq.rt.name))
	})
	h.reg("disks", z, "resize", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[resizeRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.resizeDisk(rq.rt.project, z, rq.rt.loc, rq.rt.name, in.SizeGb))
	})
	h.reg("disks", z, "setLabels", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[labelsRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setDiskLabels(rq.rt.project, z, rq.rt.loc, rq.rt.name, *in))
	})
	h.reg("disks", z, "createSnapshot", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.Snapshot](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.createSnapshot(rq.rt.project, z, rq.rt.loc, rq.rt.name, in))
	})
}

func (h *Handler) registerInstantSnapshotOps() {
	list := func(b *InMemoryBackend, p, s, l string) []entry { return b.listInstantSnapshots(p, s, l) }
	h.scopedList(collInstantSnaps, scopeZone, "compute#instantSnapshotList", "compute#instantSnapshotAggregatedList",
		collInstantSnaps, append(zoneScopes(), regionScopes()...), list)
	h.scopedList(collInstantSnaps, scopeRegion, "compute#instantSnapshotList", "", "", nil, list)

	for _, scope := range []string{scopeZone, scopeRegion} {
		h.reg(collInstantSnaps, scope, verbGet, func(h *Handler, rq *request) (any, error) {
			return h.Backend.getInstantSnapshot(rq.rt.project, scope, rq.rt.loc, rq.rt.name)
		})
		h.reg(collInstantSnaps, scope, verbInsert, func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[computeapi.InstantSnapshot](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.insertInstantSnapshot(rq.rt.project, scope, rq.rt.loc, in))
		})
		h.reg(collInstantSnaps, scope, verbDelete, func(h *Handler, rq *request) (any, error) {
			return opResult(h.Backend.deleteInstantSnapshot(rq.rt.project, scope, rq.rt.loc, rq.rt.name))
		})
		h.reg(collInstantSnaps, scope, "setLabels", func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[labelsRequest](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.setInstantSnapshotLabels(rq.rt.project, scope, rq.rt.loc, rq.rt.name, *in))
		})
	}
}

func (h *Handler) registerMachineImageOps() {
	g := scopeGlobal
	h.scopedList(collMachineImages, g, "compute#machineImageList", "", "", nil,
		func(b *InMemoryBackend, p, _, _ string) []entry { return b.listMachineImages(p) })
	h.reg(collMachineImages, g, verbGet, func(h *Handler, rq *request) (any, error) {
		return h.Backend.getMachineImage(rq.rt.project, rq.rt.name)
	})
	h.reg(collMachineImages, g, verbInsert, func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.MachineImage](rq)
		if err != nil {
			return nil, err
		}

		if in.SourceInstance == "" {
			in.SourceInstance = rq.q.Get("sourceInstance")
		}

		return opResult(h.Backend.insertMachineImage(rq.rt.project, in))
	})
	h.reg(collMachineImages, g, verbDelete, func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteMachineImage(rq.rt.project, rq.rt.name))
	})
	h.reg(collMachineImages, g, "setLabels", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[labelsRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setMachineImageLabels(rq.rt.project, rq.rt.name, *in))
	})
}

func (h *Handler) registerReservationOps() {
	z := scopeZone
	h.scopedList(
		collReservations,
		z,
		"compute#reservationList",
		"compute#reservationAggregatedList",
		collReservations,
		zoneScopes(),
		func(b *InMemoryBackend, p, s, l string) []entry { return b.listReservations(p, s, l) },
	)
	h.reg(collReservations, z, verbGet, func(h *Handler, rq *request) (any, error) {
		return h.Backend.getReservation(rq.rt.project, rq.rt.loc, rq.rt.name)
	})
	h.reg(collReservations, z, verbInsert, func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.Reservation](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.insertReservation(rq.rt.project, rq.rt.loc, in))
	})
	h.reg(collReservations, z, verbDelete, func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteReservation(rq.rt.project, rq.rt.loc, rq.rt.name))
	})
	h.reg(collReservations, z, "resize", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[reservationResizeRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.resizeReservation(rq.rt.project, rq.rt.loc, rq.rt.name, *in))
	})
	h.reg(collReservations, z, verbPatch, func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.Reservation](rq)
		if err != nil {
			return nil, err
		}

		paths := strings.Split(rq.q.Get("paths"), ",")
		if mask := rq.q.Get("updateMask"); mask != "" {
			paths = strings.Split(mask, ",")
		}

		return opResult(h.Backend.updateReservation(rq.rt.project, rq.rt.loc, rq.rt.name, in, paths))
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
