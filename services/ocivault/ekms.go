package ocivault

import (
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	limitEkmsEndpoints = 3
	defaultEkmsPort    = 443
	maxPort            = 65535
	addrBits           = 32
	reservedAddrs      = 3
	reservedTail       = 2
	opCreateEkms       = "CreateEkmsPrivateEndpoint"
	opGetEkms          = "GetEkmsPrivateEndpoint"
	opListEkms         = "ListEkmsPrivateEndpoints"
	opUpdateEkms       = "UpdateEkmsPrivateEndpoint"
	opDeleteEkms       = "DeleteEkmsPrivateEndpoint"
	ekmsPath           = kmsBase + "/ekmsPrivateEndpoints"
)

// SubnetResolver reports the CIDR of a live subnet in OCI Core.
type SubnetResolver func(id string) (cidr string, found bool)

// SetSubnetResolver wires subnet validation for EKMS private endpoints; nil only shape-checks subnet OCIDs.
func (b *InMemoryBackend) SetSubnetResolver(fn SubnetResolver) {
	b.mu.Lock("SetSubnetResolver")
	b.subnets = fn
	b.mu.Unlock()
}

// EkmsEndpoint is an EKMS private endpoint.
type EkmsEndpoint struct {
	transition
	TimeCreated   time.Time                 `json:"timeCreated"`
	TimeUpdated   *time.Time                `json:"timeUpdated,omitempty"`
	FreeformTags  map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags   map[string]map[string]any `json:"definedTags,omitempty"`
	ID            string                    `json:"id"`
	CompartmentID string                    `json:"compartmentId"`
	SubnetID      string                    `json:"subnetId"`
	DisplayName   string                    `json:"displayName"`
	State         string                    `json:"state"`
	ExternalIP    string                    `json:"externalKeyManagerIp"`
	CABundle      string                    `json:"caBundle"`
	PrivateIP     string                    `json:"privateEndpointIp,omitempty"`
	Etag          string                    `json:"etag"`
	Port          int                       `json:"port"`
	Seq           int64                     `json:"seq"`
}

// EkmsEndpointWire is the EkmsPrivateEndpoint body.
type EkmsEndpointWire struct {
	TimeCreated   time.Time                 `json:"timeCreated"`
	TimeUpdated   *time.Time                `json:"timeUpdated,omitempty"`
	FreeformTags  map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags   map[string]map[string]any `json:"definedTags,omitempty"`
	ID            string                    `json:"id"`
	CompartmentID string                    `json:"compartmentId"`
	SubnetID      string                    `json:"subnetId"`
	DisplayName   string                    `json:"displayName"`
	State         string                    `json:"lifecycleState"`
	ExternalIP    string                    `json:"externalKeyManagerIp"`
	CABundle      string                    `json:"caBundle,omitempty"`
	PrivateIP     string                    `json:"privateEndpointIp,omitempty"`
	Port          int                       `json:"port"`
}

type ekmsSummaryWire struct {
	TimeCreated   time.Time                 `json:"timeCreated"`
	TimeUpdated   *time.Time                `json:"timeUpdated,omitempty"`
	FreeformTags  map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags   map[string]map[string]any `json:"definedTags,omitempty"`
	ID            string                    `json:"id"`
	CompartmentID string                    `json:"compartmentId"`
	SubnetID      string                    `json:"subnetId"`
	DisplayName   string                    `json:"displayName"`
	State         string                    `json:"lifecycleState"`
}

func ekmsView(e *EkmsEndpoint) EkmsEndpointWire {
	return EkmsEndpointWire{
		TimeCreated: e.TimeCreated, TimeUpdated: e.TimeUpdated, FreeformTags: e.FreeformTags,
		DefinedTags: e.DefinedTags, ID: e.ID, CompartmentID: e.CompartmentID, SubnetID: e.SubnetID,
		DisplayName: e.DisplayName, State: e.State, ExternalIP: e.ExternalIP, CABundle: e.CABundle,
		PrivateIP: e.PrivateIP, Port: e.Port,
	}
}

// CreateEkmsIn is the CreateEkmsPrivateEndpoint request body.
type CreateEkmsIn struct {
	FreeformTags  map[string]string         `json:"freeformTags"`
	DefinedTags   map[string]map[string]any `json:"definedTags"`
	Port          *int                      `json:"port"`
	SubnetID      string                    `json:"subnetId"`
	CompartmentID string                    `json:"compartmentId"`
	DisplayName   string                    `json:"displayName"`
	ExternalIP    string                    `json:"externalKeyManagerIp"`
	CABundle      string                    `json:"caBundle"`
}

// UpdateEkmsIn is the UpdateEkmsPrivateEndpoint request body.
type UpdateEkmsIn struct {
	FreeformTags map[string]string         `json:"freeformTags"`
	DefinedTags  map[string]map[string]any `json:"definedTags"`
	DisplayName  *string                   `json:"displayName"`
}

func validCABundle(pemText string) bool {
	rest, certs := []byte(pemText), 0

	for {
		var blk *pem.Block

		blk, rest = pem.Decode(rest)
		if blk == nil {
			break
		}

		if blk.Type != "CERTIFICATE" {
			return false
		}

		if _, err := x509.ParseCertificate(blk.Bytes); err != nil {
			return false
		}

		certs++
	}

	return certs > 0
}

func (b *InMemoryBackend) checkEkmsCreate(in *CreateEkmsIn) *APIError {
	switch {
	case in.CompartmentID == "" || in.SubnetID == "" || in.ExternalIP == "" || in.CABundle == "":
		return errMissing("compartmentId, subnetId, externalKeyManagerIp and caBundle are required")
	case in.DisplayName == "":
		return errMissing("displayName is required")
	case net.ParseIP(in.ExternalIP) == nil:
		return errParam("externalKeyManagerIp must be an IP address")
	case !validCABundle(in.CABundle):
		return errParam("caBundle must hold PEM encoded certificates")
	case !strings.HasPrefix(in.SubnetID, "ocid1.subnet."):
		return errParam("subnetId must be the OCID of a subnet")
	case in.Port != nil && (*in.Port < 1 || *in.Port > maxPort):
		return errParam("port must be between 1 and 65535")
	}

	if aerr := validateDisplayName(in.DisplayName); aerr != nil {
		return aerr
	}

	return b.checkTags(in.FreeformTags, in.DefinedTags)
}

// subnetIP picks the highest free host address of the subnet, which OCI Core does not hand out first.
func (b *InMemoryBackend) subnetIP(subnetID string) (string, *APIError) {
	if b.subnets == nil {
		return "", nil
	}

	cidr, ok := b.subnets(subnetID)
	if !ok {
		return "", errParam("subnetId " + subnetID + " does not reference an existing subnet")
	}

	p, err := netip.ParsePrefix(cidr)
	if err != nil || !p.Addr().Is4() {
		return "", errParam("subnet " + subnetID + " has no usable IPv4 range")
	}

	used := map[string]bool{}

	for _, e := range b.ekms {
		if e.State != stateDeleted {
			used[e.PrivateIP] = true
		}
	}

	a4 := p.Masked().Addr().As4()
	size := uint32(1) << (addrBits - p.Bits())
	base := binary.BigEndian.Uint32(a4[:])

	for off := size - reservedTail; off > reservedAddrs; off-- {
		var raw [4]byte

		binary.BigEndian.PutUint32(raw[:], base+off)

		if cand := netip.AddrFrom4(raw).String(); !used[cand] {
			return cand, nil
		}
	}

	return "", errLimit("free addresses in subnet "+subnetID, 0)
}

// CreateEkmsPrivateEndpoint creates a private endpoint to an external key manager; it is CREATING until the
// transition delay elapses.
func (b *InMemoryBackend) CreateEkmsPrivateEndpoint(in CreateEkmsIn) (EkmsEndpointWire, string, error) {
	b.mu.Lock("CreateEkmsPrivateEndpoint")
	defer b.mu.Unlock()

	if aerr := b.checkEkmsCreate(&in); aerr != nil {
		return EkmsEndpointWire{}, "", aerr
	}

	n := 0

	for _, e := range b.ekms {
		if e.State != stateDeleted {
			n++
		}
	}

	if n >= limitEkmsEndpoints {
		return EkmsEndpointWire{}, "", errLimit("EKMS private endpoints per region", limitEkmsEndpoints)
	}

	ip, aerr := b.subnetIP(in.SubnetID)
	if aerr != nil {
		return EkmsEndpointWire{}, "", aerr
	}

	e := &EkmsEndpoint{
		ID: newOCID("ekmsprivateendpoint", ""), CompartmentID: in.CompartmentID, SubnetID: in.SubnetID,
		DisplayName: in.DisplayName, ExternalIP: in.ExternalIP, CABundle: in.CABundle, PrivateIP: ip,
		Port: defaultEkmsPort, FreeformTags: in.FreeformTags, DefinedTags: in.DefinedTags,
		TimeCreated: b.nowUTC(), Etag: newEtag(), Seq: b.nextSeq(),
	}
	if in.Port != nil {
		e.Port = *in.Port
	}

	b.begin(&e.State, &e.transition, stateCreating, stateActive)
	b.ekms[e.ID] = e

	return ekmsView(e), e.Etag, nil
}

func (b *InMemoryBackend) ekmsByID(id string) (*EkmsEndpoint, *APIError) {
	e, ok := b.ekms[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return e, nil
}

// GetEkmsPrivateEndpoint returns one private endpoint; DELETED endpoints stay readable.
func (b *InMemoryBackend) GetEkmsPrivateEndpoint(id string) (EkmsEndpointWire, string, error) {
	b.mu.RLock("GetEkmsPrivateEndpoint")
	defer b.mu.RUnlock()

	e, aerr := b.ekmsByID(id)
	if aerr != nil {
		return EkmsEndpointWire{}, "", aerr
	}

	return ekmsView(e), e.Etag, nil
}

// ListEkmsPrivateEndpoints lists the private endpoints of a compartment.
func (b *InMemoryBackend) ListEkmsPrivateEndpoints(compartment string, o ListOpts) (page.Page[ekmsSummaryWire], error) {
	if compartment == "" {
		return page.Page[ekmsSummaryWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListEkmsPrivateEndpoints")
	defer b.mu.RUnlock()

	var all []*EkmsEndpoint

	for _, e := range b.ekms {
		if e.CompartmentID == compartment {
			all = append(all, e)
		}
	}

	pg := paginate(all, o, func(e *EkmsEndpoint) string { return e.DisplayName },
		func(e *EkmsEndpoint) (time.Time, int64) { return e.TimeCreated, e.Seq })

	out := make([]ekmsSummaryWire, 0, len(pg.Data))

	for _, e := range pg.Data {
		out = append(out, ekmsSummaryWire{
			TimeCreated: e.TimeCreated, TimeUpdated: e.TimeUpdated, FreeformTags: e.FreeformTags,
			DefinedTags: e.DefinedTags, ID: e.ID, CompartmentID: e.CompartmentID, SubnetID: e.SubnetID,
			DisplayName: e.DisplayName, State: e.State,
		})
	}

	return page.Page[ekmsSummaryWire]{Data: out, Next: pg.Next}, nil
}

// UpdateEkmsPrivateEndpoint changes the display name and tags.
func (b *InMemoryBackend) UpdateEkmsPrivateEndpoint(
	id, ifMatch string,
	in UpdateEkmsIn,
) (EkmsEndpointWire, string, error) {
	b.mu.Lock("UpdateEkmsPrivateEndpoint")
	defer b.mu.Unlock()

	e, aerr := b.ekmsByID(id)
	if aerr != nil {
		return EkmsEndpointWire{}, "", aerr
	}

	if e.State != stateActive {
		return EkmsEndpointWire{}, "", errState("EKMS private endpoint " + id + " is in state " + e.State)
	}

	if aerr = checkEtag(e.Etag, ifMatch); aerr != nil {
		return EkmsEndpointWire{}, "", aerr
	}

	if in.DisplayName != nil {
		if aerr = validateDisplayName(*in.DisplayName); aerr != nil {
			return EkmsEndpointWire{}, "", aerr
		}
	}

	if aerr = b.checkTags(in.FreeformTags, in.DefinedTags); aerr != nil {
		return EkmsEndpointWire{}, "", aerr
	}

	if in.DisplayName != nil {
		e.DisplayName = *in.DisplayName
	}

	if in.FreeformTags != nil {
		e.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		e.DefinedTags = in.DefinedTags
	}

	now := b.nowUTC()
	e.TimeUpdated, e.Etag = &now, newEtag()

	return ekmsView(e), e.Etag, nil
}

// DeleteEkmsPrivateEndpoint deletes a private endpoint that no external vault uses.
func (b *InMemoryBackend) DeleteEkmsPrivateEndpoint(id, ifMatch string) error {
	b.mu.Lock("DeleteEkmsPrivateEndpoint")
	defer b.mu.Unlock()

	e, aerr := b.ekmsByID(id)
	if aerr != nil {
		return aerr
	}

	if e.State == stateDeleted || e.State == stateDeleting {
		return nil
	}

	if e.State != stateActive {
		return errState("EKMS private endpoint " + id + " is in state " + e.State)
	}

	if aerr = checkEtag(e.Etag, ifMatch); aerr != nil {
		return aerr
	}

	b.begin(&e.State, &e.transition, stateDeleting, stateDeleted)
	e.Etag = newEtag()

	return nil
}

func (b *InMemoryBackend) sweepEkms(now time.Time) {
	for _, e := range b.ekms {
		e.settle(&e.State, now)
	}
}

func (h *Handler) createEkms(q *req) error {
	var in CreateEkmsIn
	if err := q.decode(&in); err != nil {
		return err
	}

	w, etag, err := h.Backend.CreateEkmsPrivateEndpoint(in)

	return reply(q, w, etag, err)
}

func (h *Handler) getEkms(q *req) error {
	w, etag, err := h.Backend.GetEkmsPrivateEndpoint(q.param("ekmsPrivateEndpointId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listEkms(q *req) error {
	pg, err := h.Backend.ListEkmsPrivateEndpoints(q.query("compartmentId"), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateEkms(q *req) error {
	var in UpdateEkmsIn
	if err := q.decode(&in); err != nil {
		return err
	}

	w, etag, err := h.Backend.UpdateEkmsPrivateEndpoint(q.param("ekmsPrivateEndpointId"), q.ifMatch(), in)

	return reply(q, w, etag, err)
}

func (h *Handler) deleteEkms(q *req) error {
	err := h.Backend.DeleteEkmsPrivateEndpoint(q.param("ekmsPrivateEndpointId"), q.ifMatch())
	if err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusAccepted)

	return nil
}

func ekmsOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateEkms: (*Handler).createEkms, opGetEkms: (*Handler).getEkms, opListEkms: (*Handler).listEkms,
		opUpdateEkms: (*Handler).updateEkms, opDeleteEkms: (*Handler).deleteEkms,
	}
}
