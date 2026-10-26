package ocicore

import (
	"maps"
	"net/http"
	"strconv"
)

func send(q *req, v resource, etag string, aerr *APIError) error {
	if aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, v, etag)
}

func noBody(q *req, aerr *APIError) error {
	if aerr != nil {
		return aerr
	}

	return q.noContent("")
}

func (q *req) listOpts() (ListOpts, error) {
	n := 0

	if s := q.query("limit"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v < 1 || v > maxLimit {
			return ListOpts{}, errParam("limit must be an integer between 1 and 1000")
		}

		n = v
	}

	return ListOpts{
		Compartment: q.query("compartmentId"),
		DisplayName: q.query("displayName"),
		State:       q.query("lifecycleState"),
		VcnID: q.query(
			"vcnId",
		),
		SortBy: q.query("sortBy"),
		Order:  q.query("sortOrder"),
		Page:   q.query("page"),
		Limit:  n,
	}, nil
}

func createOp[In any](fn func(*InMemoryBackend, In) (resource, string, *APIError)) opFunc {
	return func(h *Handler, q *req) error {
		var in In
		if err := q.decode(&in); err != nil {
			return err
		}

		v, etag, aerr := fn(h.Backend, in)

		return send(q, v, etag, aerr)
	}
}

func updateOp[In any](
	param string, fn func(*InMemoryBackend, string, string, In) (resource, string, *APIError),
) opFunc {
	return func(h *Handler, q *req) error {
		var in In
		if err := q.decode(&in); err != nil {
			return err
		}

		v, etag, aerr := fn(h.Backend, q.param(param), q.ifMatch(), in)

		return send(q, v, etag, aerr)
	}
}

func getOp[T resource](param string) opFunc {
	return func(h *Handler, q *req) error {
		v, etag, aerr := getView[T](h.Backend, q.param(param))

		return send(q, v, etag, aerr)
	}
}

func listOp[T resource](extra func(*req) func(resource) bool) opFunc {
	return func(h *Handler, q *req) error {
		o, err := q.listOpts()
		if err != nil {
			return err
		}

		if extra != nil {
			o.Match = extra(q)
		}

		pg, aerr := list[T](h.Backend, o)
		if aerr != nil {
			return aerr
		}

		q.nextPage(pg.Next)

		return q.json(http.StatusOK, pg.Data, "")
	}
}

func deleteOp[T resource](param string) opFunc {
	return func(h *Handler, q *req) error {
		return noBody(q, deleteRes[T](h.Backend, q.param(param), q.ifMatch()))
	}
}

func changeOp[T resource](param string) opFunc {
	return func(h *Handler, q *req) error {
		var in struct {
			CompartmentID string `json:"compartmentId"`
		}

		if err := q.decode(&in); err != nil {
			return err
		}

		return noBody(q, changeCompartment[T](h.Backend, q.param(param), in.CompartmentID, q.ifMatch()))
	}
}

// crud describes the six standard operations of one resource kind.
type crud struct {
	ops  map[string]opFunc
	noun string
	list string
	path string
	id   string
}

func newCrud[T resource, C, U any](
	noun, plural, path, param string,
	create func(*InMemoryBackend, C) (resource, string, *APIError),
	update func(*InMemoryBackend, string, string, U) (resource, string, *APIError),
	extra func(*req) func(resource) bool,
) crud {
	return crud{
		noun: noun, list: "List" + plural, path: path, id: param,
		ops: map[string]opFunc{
			"Create" + noun: createOp(create), "Get" + noun: getOp[T](param), "List" + plural: listOp[T](extra),
			"Update" + noun: updateOp(param, update), "Delete" + noun: deleteOp[T](param),
			"Change" + noun + "Compartment": changeOp[T](param),
		},
	}
}

func publicIPFilter(q *req) func(resource) bool {
	life := q.query("lifetime")

	return func(r resource) bool {
		p, ok := r.(*PublicIP)

		ad := q.query("availabilityDomain")

		return ok && p.Scope == q.query("scope") && (life == "" || p.Lifetime == life) &&
			(ad == "" || (p.AvailabilityDomain != nil && *p.AvailabilityDomain == ad))
	}
}

func cruds() []crud {
	return []crud{
		newCrud[*Vcn]("Vcn", "Vcns", "/vcns", "vcnId", (*InMemoryBackend).CreateVcn, (*InMemoryBackend).UpdateVcn, nil),
		newCrud[*Subnet]("Subnet", "Subnets", "/subnets", "subnetId",
			(*InMemoryBackend).CreateSubnet, (*InMemoryBackend).UpdateSubnet, nil),
		newCrud[*InternetGateway]("InternetGateway", "InternetGateways", "/internetGateways", "igId",
			(*InMemoryBackend).CreateInternetGateway, (*InMemoryBackend).UpdateInternetGateway, nil),
		newCrud[*NatGateway]("NatGateway", "NatGateways", "/natGateways", "natGatewayId",
			(*InMemoryBackend).CreateNatGateway, (*InMemoryBackend).UpdateNatGateway, nil),
		newCrud[*ServiceGateway]("ServiceGateway", "ServiceGateways", "/serviceGateways", "serviceGatewayId",
			(*InMemoryBackend).CreateServiceGateway, (*InMemoryBackend).UpdateServiceGateway, nil),
		newCrud[*RouteTable]("RouteTable", "RouteTables", "/routeTables", "rtId",
			(*InMemoryBackend).CreateRouteTable, (*InMemoryBackend).UpdateRouteTable, nil),
		newCrud[*SecurityList]("SecurityList", "SecurityLists", "/securityLists", "securityListId",
			(*InMemoryBackend).CreateSecurityList, (*InMemoryBackend).UpdateSecurityList, nil),
		newCrud[*NetworkSecurityGroup]("NetworkSecurityGroup", "NetworkSecurityGroups", "/networkSecurityGroups",
			"networkSecurityGroupId", (*InMemoryBackend).CreateNetworkSecurityGroup,
			(*InMemoryBackend).UpdateNetworkSecurityGroup, nil),
		newCrud[*DhcpOptions]("DhcpOptions", "DhcpOptions", "/dhcps", "dhcpId",
			(*InMemoryBackend).CreateDhcpOptions, (*InMemoryBackend).UpdateDhcpOptions, nil),
		newCrud[*LocalPeeringGateway]("LocalPeeringGateway", "LocalPeeringGateways", "/localPeeringGateways",
			"localPeeringGatewayId", (*InMemoryBackend).CreateLocalPeeringGateway,
			(*InMemoryBackend).UpdateLocalPeeringGateway, nil),
		newCrud[*Volume]("Volume", "Volumes", "/volumes", "volumeId",
			(*InMemoryBackend).CreateVolume, (*InMemoryBackend).UpdateVolume, volumeFilter),
		newCrud[*BootVolume]("BootVolume", "BootVolumes", "/bootVolumes", "bootVolumeId",
			(*InMemoryBackend).CreateBootVolume, (*InMemoryBackend).UpdateBootVolume, volumeFilter),
		newCrud[*VolumeBackup]("VolumeBackup", "VolumeBackups", "/volumeBackups", "volumeBackupId",
			(*InMemoryBackend).CreateVolumeBackup, (*InMemoryBackend).UpdateVolumeBackup, backupFilter),
		newCrud[*PublicIP]("PublicIp", "PublicIps", "/publicIps", "publicIpId",
			(*InMemoryBackend).CreatePublicIP, (*InMemoryBackend).UpdatePublicIP, publicIPFilter),
	}
}

func buildOps() map[string]opFunc {
	ops := map[string]opFunc{
		opListServices: (*Handler).listServices, opGetService: (*Handler).getService,
		opAttachService: (*Handler).attachService, opDetachService: (*Handler).detachService,
		opConnectLpg:     (*Handler).connectLpg,
		opAddNsgRules:    (*Handler).addNsgRules,
		opUpdateNsgRules: (*Handler).updateNsgRules,
		opRemoveNsgRules: (*Handler).removeNsgRules,
		opListNsgRules:   (*Handler).listNsgRules,
		opListNsgVnics:   (*Handler).listNsgVnics,
		opPublicIPByAddr: (*Handler).publicIPByAddress,
	}

	for _, c := range cruds() {
		maps.Copy(ops, c.ops)
	}

	maps.Copy(ops, computeOps())

	return ops
}

func (h *Handler) listServices(q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	pg, aerr := h.Backend.ListServices(o.Page, o.Limit)
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func (h *Handler) getService(q *req) error {
	s, aerr := h.Backend.GetService(q.param("serviceId"))
	if aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, s, "")
}

func (h *Handler) attachService(q *req) error {
	var in ServiceIDIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, aerr := h.Backend.AttachService(q.param("serviceGatewayId"), q.ifMatch(), in)

	return send(q, v, etag, aerr)
}

func (h *Handler) detachService(q *req) error {
	var in ServiceIDIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, aerr := h.Backend.DetachService(q.param("serviceGatewayId"), q.ifMatch(), in)

	return send(q, v, etag, aerr)
}

func (h *Handler) connectLpg(q *req) error {
	var in struct {
		PeerID string `json:"peerId"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	if aerr := h.Backend.ConnectLocalPeeringGateways(
		q.param("localPeeringGatewayId"),
		in.PeerID,
		q.ifMatch(),
	); aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, struct{}{}, "")
}

func (h *Handler) addNsgRules(q *req) error {
	var in SecurityRulesIn
	if err := q.decode(&in); err != nil {
		return err
	}

	out, aerr := h.Backend.AddSecurityRules(q.param("networkSecurityGroupId"), in)
	if aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, out, "")
}

func (h *Handler) updateNsgRules(q *req) error {
	var in SecurityRulesIn
	if err := q.decode(&in); err != nil {
		return err
	}

	out, aerr := h.Backend.UpdateSecurityRules(q.param("networkSecurityGroupId"), in)
	if aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, out, "")
}

func (h *Handler) removeNsgRules(q *req) error {
	var in struct {
		SecurityRuleIDs []string `json:"securityRuleIds"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	return noBody(q, h.Backend.RemoveSecurityRules(q.param("networkSecurityGroupId"), in.SecurityRuleIDs))
}

func (h *Handler) listNsgRules(q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	pg, aerr := h.Backend.ListSecurityRules(q.param("networkSecurityGroupId"), RuleListOpts{
		Direction: q.query("direction"), SortBy: o.SortBy, Order: o.Order, Page: o.Page, Limit: o.Limit,
	})
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func (h *Handler) listNsgVnics(q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	pg, aerr := h.Backend.ListNsgVnics(q.param("networkSecurityGroupId"), o)
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func (h *Handler) publicIPByAddress(q *req) error {
	var in struct {
		IPAddress string `json:"ipAddress"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, aerr := h.Backend.PublicIPByAddress(in.IPAddress)

	return send(q, v, etag, aerr)
}
