package ocicore

import (
	"net/http"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

type entry struct {
	fn     opFunc
	method string
	path   string
	op     string
}

func queryEq(q *req, key string, get func(resource) string) func(resource) bool {
	want := q.query(key)

	return func(r resource) bool { return want == "" || get(r) == want }
}

func and(fs ...func(resource) bool) func(resource) bool {
	return func(r resource) bool {
		for _, f := range fs {
			if !f(r) {
				return false
			}
		}

		return true
	}
}

func instanceFilter(q *req) func(resource) bool {
	return queryEq(q, "availabilityDomain", func(r resource) string {
		i, _ := r.(*Instance)

		return i.AvailabilityDomain
	})
}

func volumeFilter(q *req) func(resource) bool {
	return queryEq(q, "availabilityDomain", func(r resource) string {
		switch v := r.(type) {
		case *Volume:
			return v.AvailabilityDomain
		case *BootVolume:
			return v.AvailabilityDomain
		}

		return ""
	})
}

func backupFilter(q *req) func(resource) bool {
	return and(
		queryEq(q, "volumeId", func(r resource) string {
			b, _ := r.(*VolumeBackup)

			return b.VolumeID
		}),
		queryEq(q, "sourceVolumeBackupId", func(resource) string { return "" }),
	)
}

func volumeAttachmentFilter(q *req) func(resource) bool {
	return and(
		queryEq(
			q,
			"availabilityDomain",
			func(r resource) string {
				a, _ := r.(*VolumeAttachment)

				return a.AvailabilityDomain
			},
		),
		queryEq(q, "instanceId", func(r resource) string {
			a, _ := r.(*VolumeAttachment)

			return a.InstanceID
		}),
		queryEq(q, "volumeId", func(r resource) string {
			a, _ := r.(*VolumeAttachment)

			return a.VolumeID
		}),
	)
}

func bootAttachmentFilter(q *req) func(resource) bool {
	return and(
		queryEq(
			q,
			"availabilityDomain",
			func(r resource) string {
				a, _ := r.(*BootVolumeAttachment)

				return a.AvailabilityDomain
			},
		),
		queryEq(q, "instanceId", func(r resource) string {
			a, _ := r.(*BootVolumeAttachment)

			return a.InstanceID
		}),
		queryEq(
			q,
			"bootVolumeId",
			func(r resource) string {
				a, _ := r.(*BootVolumeAttachment)

				return a.BootVolumeID
			},
		),
	)
}

func vnicAttachmentFilter(q *req) func(resource) bool {
	return and(
		queryEq(
			q,
			"availabilityDomain",
			func(r resource) string {
				a, _ := r.(*VnicAttachment)

				return a.AvailabilityDomain
			},
		),
		queryEq(q, "instanceId", func(r resource) string {
			a, _ := r.(*VnicAttachment)

			return a.InstanceID
		}),
		queryEq(q, "vnicId", func(r resource) string {
			a, _ := r.(*VnicAttachment)

			return a.VnicID
		}),
	)
}

func idPath(base, id string) string { return base + "/{" + id + "}" }

func computeEntries() []entry {
	return slices.Concat(instanceEntries(), networkEntries(), storageEntries())
}

func instanceEntries() []entry {
	inst := idPath("/instances", "instanceId")

	return []entry{
		{createOp((*InMemoryBackend).LaunchInstance), http.MethodPost, "/instances", "LaunchInstance"},
		{listOp[*Instance](instanceFilter), http.MethodGet, "/instances", "ListInstances"},
		{getOp[*Instance]("instanceId"), http.MethodGet, inst, "GetInstance"},
		{updateOp("instanceId", (*InMemoryBackend).UpdateInstance), http.MethodPut, inst, "UpdateInstance"},
		{(*Handler).terminateInstance, http.MethodDelete, inst, "TerminateInstance"},
		{(*Handler).instanceAction, http.MethodPost, inst, "InstanceAction"},
		{
			changeOp[*Instance]("instanceId"),
			http.MethodPost,
			inst + "/actions/changeCompartment",
			"ChangeInstanceCompartment",
		},
		{(*Handler).listShapes, http.MethodGet, "/shapes", "ListShapes"},
		{(*Handler).listImages, http.MethodGet, "/images", "ListImages"},
		{(*Handler).getImage, http.MethodGet, idPath("/images", "imageId"), "GetImage"},
		{createOp((*InMemoryBackend).CreateImage), http.MethodPost, "/images", "CreateImage"},
		{
			updateOp("imageId", (*InMemoryBackend).UpdateImage),
			http.MethodPut,
			idPath("/images", "imageId"),
			"UpdateImage",
		},
		{deleteImageOp, http.MethodDelete, idPath("/images", "imageId"), "DeleteImage"},
		{
			changeOp[*Image]("imageId"),
			http.MethodPost,
			idPath("/images", "imageId") + "/actions/changeCompartment",
			"ChangeImageCompartment",
		},
	}
}

func networkEntries() []entry {
	return []entry{
		{listOp[*VnicAttachment](vnicAttachmentFilter), http.MethodGet, "/vnicAttachments", "ListVnicAttachments"},
		{createOp((*InMemoryBackend).AttachVnic), http.MethodPost, "/vnicAttachments", "AttachVnic"},
		{
			getOp[*VnicAttachment]("vnicAttachmentId"),
			http.MethodGet,
			idPath("/vnicAttachments", "vnicAttachmentId"),
			"GetVnicAttachment",
		},
		{detachVnicOp, http.MethodDelete, idPath("/vnicAttachments", "vnicAttachmentId"), "DetachVnic"},
		{getOp[*Vnic]("vnicId"), http.MethodGet, idPath("/vnics", "vnicId"), "GetVnic"},
		{updateOp("vnicId", (*InMemoryBackend).UpdateVnic), http.MethodPut, idPath("/vnics", "vnicId"), "UpdateVnic"},
		{(*Handler).listPrivateIPs, http.MethodGet, "/privateIps", "ListPrivateIps"},
		{createOp((*InMemoryBackend).CreatePrivateIP), http.MethodPost, "/privateIps", "CreatePrivateIp"},
		{getOp[*PrivateIP]("privateIpId"), http.MethodGet, idPath("/privateIps", "privateIpId"), "GetPrivateIp"},
		{
			updateOp("privateIpId", (*InMemoryBackend).UpdatePrivateIP),
			http.MethodPut,
			idPath("/privateIps", "privateIpId"),
			"UpdatePrivateIp",
		},
		{deletePrivateIPOp, http.MethodDelete, idPath("/privateIps", "privateIpId"), "DeletePrivateIp"},
	}
}

func storageEntries() []entry {
	return []entry{
		{createOp((*InMemoryBackend).AttachVolume), http.MethodPost, "/volumeAttachments", "AttachVolume"},
		{
			listOp[*VolumeAttachment](volumeAttachmentFilter),
			http.MethodGet,
			"/volumeAttachments",
			"ListVolumeAttachments",
		},
		{
			getOp[*VolumeAttachment]("volumeAttachmentId"),
			http.MethodGet,
			idPath("/volumeAttachments", "volumeAttachmentId"),
			"GetVolumeAttachment",
		},
		{detachVolumeOp, http.MethodDelete, idPath("/volumeAttachments", "volumeAttachmentId"), "DetachVolume"},
		{
			listOp[*BootVolumeAttachment](bootAttachmentFilter),
			http.MethodGet,
			"/bootVolumeAttachments",
			"ListBootVolumeAttachments",
		},
		{
			getOp[*BootVolumeAttachment]("bootVolumeAttachmentId"),
			http.MethodGet,
			idPath("/bootVolumeAttachments", "bootVolumeAttachmentId"),
			"GetBootVolumeAttachment",
		},
		{
			createOp((*InMemoryBackend).CreateVolumeBackupPolicy),
			http.MethodPost,
			"/volumeBackupPolicies",
			"CreateVolumeBackupPolicy",
		},
		{(*Handler).listPolicies, http.MethodGet, "/volumeBackupPolicies", "ListVolumeBackupPolicies"},
		{(*Handler).getPolicy, http.MethodGet, idPath("/volumeBackupPolicies", "policyId"), "GetVolumeBackupPolicy"},
		{
			updateOp("policyId", (*InMemoryBackend).UpdateVolumeBackupPolicy),
			http.MethodPut,
			idPath("/volumeBackupPolicies", "policyId"),
			"UpdateVolumeBackupPolicy",
		},
		{deletePolicyOp, http.MethodDelete, idPath("/volumeBackupPolicies", "policyId"), "DeleteVolumeBackupPolicy"},
		{
			createOp((*InMemoryBackend).CreateVolumeBackupPolicyAssignment),
			http.MethodPost,
			"/volumeBackupPolicyAssignments",
			"CreateVolumeBackupPolicyAssignment",
		},
		{
			(*Handler).listAssignments,
			http.MethodGet,
			"/volumeBackupPolicyAssignments",
			"GetVolumeBackupPolicyAssetAssignment",
		},
		{
			getOp[*BackupPolicyAssignment]("policyAssignmentId"),
			http.MethodGet,
			idPath("/volumeBackupPolicyAssignments", "policyAssignmentId"),
			"GetVolumeBackupPolicyAssignment",
		},
		{
			deleteAssignmentOp,
			http.MethodDelete,
			idPath("/volumeBackupPolicyAssignments", "policyAssignmentId"),
			"DeleteVolumeBackupPolicyAssignment",
		},
	}
}

func computeRoutes() []restroute.Route {
	es := computeEntries()
	out := make([]restroute.Route, 0, len(es))

	for _, e := range es {
		out = append(out, rt(e.method, e.path, e.op))
	}

	return out
}

func computeOps() map[string]opFunc {
	es := computeEntries()
	out := make(map[string]opFunc, len(es))

	for _, e := range es {
		out[e.op] = e.fn
	}

	return out
}

func deleteImageOp(h *Handler, q *req) error {
	return noBody(q, h.Backend.DeleteImage(q.param("imageId"), q.ifMatch()))
}

func detachVnicOp(h *Handler, q *req) error {
	return noBody(q, h.Backend.DetachVnic(q.param("vnicAttachmentId"), q.ifMatch()))
}

func deletePrivateIPOp(h *Handler, q *req) error {
	return noBody(q, h.Backend.DeletePrivateIP(q.param("privateIpId"), q.ifMatch()))
}

func detachVolumeOp(h *Handler, q *req) error {
	return noBody(q, h.Backend.DetachVolume(q.param("volumeAttachmentId"), q.ifMatch()))
}

func deletePolicyOp(h *Handler, q *req) error {
	return noBody(q, h.Backend.DeleteVolumeBackupPolicy(q.param("policyId"), q.ifMatch()))
}

func deleteAssignmentOp(h *Handler, q *req) error {
	return noBody(q, h.Backend.DeleteVolumeBackupPolicyAssignment(q.param("policyAssignmentId"), q.ifMatch()))
}

func (h *Handler) terminateInstance(q *req) error {
	return noBody(
		q,
		h.Backend.TerminateInstance(q.param("instanceId"), q.ifMatch(), q.query("preserveBootVolume") == "true"),
	)
}

func (h *Handler) instanceAction(q *req) error {
	v, etag, aerr := h.Backend.InstanceAction(q.param("instanceId"), q.query("action"), q.ifMatch())

	return send(q, v, etag, aerr)
}

func (h *Handler) listShapes(q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	pg, aerr := h.Backend.ListShapes(o.Compartment, q.query("imageId"), q.query("shape"), o.Page, o.Limit)
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func (h *Handler) listImages(q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	pg, aerr := h.Backend.ListImages(ImageListOpts{
		Compartment: o.Compartment, Name: o.DisplayName, OS: q.query("operatingSystem"),
		OSVersion: q.query("operatingSystemVersion"), Shape: q.query("shape"), State: o.State,
		SortBy: o.SortBy, Order: o.Order, Page: o.Page, Limit: o.Limit,
	})
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func (h *Handler) getImage(q *req) error {
	v, etag, aerr := h.Backend.GetImage(q.param("imageId"))

	return send(q, v, etag, aerr)
}

func (h *Handler) listPrivateIPs(q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	pg, aerr := h.Backend.ListPrivateIPs(PrivateIPListOpts{
		IPAddress: q.query("ipAddress"), SubnetID: q.query("subnetId"), VnicID: q.query("vnicId"),
		IPState: q.query("ipState"), Lifetime: q.query("lifetime"), Page: o.Page, Limit: o.Limit,
	})
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func (h *Handler) listPolicies(q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	pg, aerr := h.Backend.ListVolumeBackupPolicies(o.Compartment, o.Page, o.Limit)
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func (h *Handler) getPolicy(q *req) error {
	v, etag, aerr := h.Backend.GetVolumeBackupPolicy(q.param("policyId"))

	return send(q, v, etag, aerr)
}

func (h *Handler) listAssignments(q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	pg, aerr := h.Backend.ListPolicyAssignments(q.query("assetId"), o.Page, o.Limit)
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}
