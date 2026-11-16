package ocicontainerengine

import (
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// podShapeNames are the virtual node pod shapes published at contengimagesshapes.htm.
func podShapeNames() []string {
	return []string{"Pod.Standard.A1.Flex", "Pod.Standard.E3.Flex", "Pod.Standard.E4.Flex", "Pod.Standard.E5.Flex"}
}

// PodShapeWire is a PodShapeSummary body; the vendor publishes only the names.
type PodShapeWire struct {
	Name string `json:"name"`
}

// ListPodShapes returns the pod shapes virtual node pools accept.
func (b *InMemoryBackend) ListPodShapes(compartment, name string, o ListOpts) (page.Page[PodShapeWire], error) {
	if aerr := validListOpts(o, "ID", "NAME", "TIME_CREATED"); aerr != nil {
		return page.Page[PodShapeWire]{}, aerr
	}

	b.mu.RLock("ListPodShapes")
	aerr := b.checkCompartment(compartment)
	b.mu.RUnlock()

	if aerr != nil {
		return page.Page[PodShapeWire]{}, aerr
	}

	var all []PodShapeWire

	for _, n := range podShapeNames() {
		if name == "" || n == name {
			all = append(all, PodShapeWire{Name: n})
		}
	}

	if o.Order == "" {
		o.Order = "ASC"
	}

	return paginate(
		all, o,
		func(s PodShapeWire) string { return s.Name }, func(s PodShapeWire) string { return s.Name },
		func(PodShapeWire) (time.Time, int64) { return time.Time{}, 0 },
	), nil
}

func validPodShape(name string) bool { return slices.Contains(podShapeNames(), name) }

func (h *Handler) listPodShapes(q *req) error {
	pg, err := h.Backend.ListPodShapes(q.query("compartmentId"), q.query("name"), q.listOpts())

	return collectionReply(q, pg, err)
}
