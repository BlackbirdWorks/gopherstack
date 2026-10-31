package ociapigateway

import (
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

type createGatewayIn struct {
	Tags
	Locks         json.RawMessage `json:"locks"`
	CaBundles     json.RawMessage `json:"caBundles"`
	ResponseCache json.RawMessage `json:"responseCacheDetails"`
	DisplayName   string          `json:"displayName"`
	CompartmentID string          `json:"compartmentId"`
	EndpointType  string          `json:"endpointType"`
	SubnetID      string          `json:"subnetId"`
	CertificateID string          `json:"certificateId"`
	IPMode        string          `json:"ipMode"`
	NSGs          []string        `json:"networkSecurityGroupIds"`
}

type updateGatewayIn struct {
	Tags
	CaBundles     json.RawMessage `json:"caBundles"`
	ResponseCache json.RawMessage `json:"responseCacheDetails"`
	DisplayName   *string         `json:"displayName"`
	CertificateID *string         `json:"certificateId"`
	NSGs          []string        `json:"networkSecurityGroupIds"`
}

type changeCompartmentIn struct {
	CompartmentID string `json:"compartmentId"`
}

// GatewayWire is the wire form of a gateway.
type GatewayWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeUpdated    time.Time                 `json:"timeUpdated"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	Locks          json.RawMessage           `json:"locks,omitempty"`
	CaBundles      json.RawMessage           `json:"caBundles,omitempty"`
	ResponseCache  json.RawMessage           `json:"responseCacheDetails,omitempty"`
	ID             string                    `json:"id"`
	CompartmentID  string                    `json:"compartmentId"`
	EndpointType   string                    `json:"endpointType"`
	DisplayName    string                    `json:"displayName"`
	SubnetID       string                    `json:"subnetId"`
	Hostname       string                    `json:"hostname"`
	CertificateID  string                    `json:"certificateId,omitempty"`
	IPMode         string                    `json:"ipMode,omitempty"`
	LifecycleState string                    `json:"lifecycleState"`
	NSGs           []string                  `json:"networkSecurityGroupIds,omitempty"`
}

func gatewayView(g *Gateway) GatewayWire {
	return GatewayWire{
		TimeCreated: g.TimeCreated, TimeUpdated: g.TimeUpdated, DefinedTags: g.Tags.DefinedTags,
		FreeformTags: g.Tags.FreeformTags, Locks: g.Locks, CaBundles: g.CaBundles, ResponseCache: g.ResponseCache,
		ID: g.ID, CompartmentID: g.CompartmentID, EndpointType: g.EndpointType, DisplayName: g.DisplayName,
		SubnetID: g.SubnetID, Hostname: g.Hostname, CertificateID: g.CertificateID, IPMode: g.IPMode,
		LifecycleState: g.State, NSGs: g.NSGs,
	}
}

func (b *InMemoryBackend) liveGateway(id string) (*Gateway, *APIError) {
	g, ok := b.gateways[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return g, nil
}

func (b *InMemoryBackend) checkCertificateRef(id string) *APIError {
	if id == "" {
		return nil
	}

	if c, ok := b.certificates[id]; !ok || c.State == stateDeleted {
		return errNotFound(id)
	}

	return nil
}

func (b *InMemoryBackend) validateGateway(in *createGatewayIn) *APIError {
	if err := b.checkCompartment(in.CompartmentID); err != nil {
		return err
	}

	if in.EndpointType != endpointPublic && in.EndpointType != endpointPrivate {
		return errParam("endpointType must be PUBLIC or PRIVATE")
	}

	if in.SubnetID == "" {
		return errMissing("subnetId is required")
	}

	if b.subnet != nil && !b.subnet(in.SubnetID) {
		return errNotFound(in.SubnetID)
	}

	if in.IPMode != "" && !slices.Contains([]string{"IPV4", "IPV6", "DUAL_STACK"}, in.IPMode) {
		return errParam("ipMode must be IPV4, IPV6 or DUAL_STACK")
	}

	if len(in.ResponseCache) > 0 && string(in.ResponseCache) != jsonNull && !jsonIsNoCache(in.ResponseCache) {
		return errUnsupported("responseCacheDetails other than type NONE")
	}

	return b.checkCertificateRef(in.CertificateID)
}

func jsonIsNoCache(raw json.RawMessage) bool {
	var d struct {
		Type string `json:"type"`
	}

	return json.Unmarshal(raw, &d) == nil && d.Type == "NONE"
}

func (b *InMemoryBackend) gatewayNameTaken(compartment, name, except string) bool {
	for _, g := range b.gateways {
		if g.ID != except && g.CompartmentID == compartment && g.State != stateDeleted &&
			strs.Equal(g.DisplayName, name) {
			return true
		}
	}

	return false
}

// CreateGateway creates a gateway in CREATING state and returns it with its work request id.
func (b *InMemoryBackend) CreateGateway(in createGatewayIn) (GatewayWire, string, string, error) {
	b.mu.Lock("CreateGateway")
	defer b.mu.Unlock()

	if err := b.validateGateway(&in); err != nil {
		return GatewayWire{}, "", "", err
	}

	now := b.nowUTC()
	name := in.DisplayName

	if name == "" {
		name = "gateway" + now.Format("20060102150405")
	}

	if b.gatewayNameTaken(in.CompartmentID, name, "") {
		return GatewayWire{}, "", "", errConflict("a gateway named " + name + " already exists in this compartment")
	}

	g := &Gateway{
		ID: newOCID("apigateway"), CompartmentID: in.CompartmentID, EndpointType: in.EndpointType,
		DisplayName: name, SubnetID: in.SubnetID, Hostname: newHostname(), CertificateID: in.CertificateID,
		IPMode: in.IPMode, NSGs: in.NSGs, Locks: nullRaw(in.Locks), CaBundles: nullRaw(in.CaBundles),
		ResponseCache: nullRaw(in.ResponseCache), State: stateCreating, TransitionAt: b.transitionAt(),
		TimeCreated: now, TimeUpdated: now, Etag: newEtag(), Tags: in.Tags,
	}
	b.gateways[g.ID] = g
	wr := b.newWR(wrCreateGateway, g.CompartmentID, entityGateway, g.ID, actionCreated, *g.TransitionAt)

	return gatewayView(g), wr, g.Etag, nil
}

func nullRaw(r json.RawMessage) json.RawMessage {
	if string(r) == jsonNull {
		return nil
	}

	return r
}

// GetGateway returns one gateway and its etag.
func (b *InMemoryBackend) GetGateway(id string) (GatewayWire, string, error) {
	b.mu.RLock("GetGateway")
	defer b.mu.RUnlock()

	g, err := b.liveGateway(id)
	if err != nil {
		return GatewayWire{}, "", err
	}

	return gatewayView(g), g.Etag, nil
}

// ListGateways lists the gateways of a compartment.
func (b *InMemoryBackend) ListGateways(
	compartment, name, state, certificate string, o ListOpts,
) (page.Page[GatewayWire], error) {
	if compartment == "" {
		return page.Page[GatewayWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListGateways")
	defer b.mu.RUnlock()

	var all []*Gateway

	for _, g := range b.gateways {
		if g.CompartmentID != compartment || !stateMatches(g.State, state) ||
			(name != "" && !strs.Equal(g.DisplayName, name)) || (certificate != "" && g.CertificateID != certificate) {
			continue
		}

		all = append(all, g)
	}

	pg := paginate(all, o, func(g *Gateway) string { return g.DisplayName },
		func(g *Gateway) (time.Time, int64) { return g.TimeCreated, 0 })

	out := make([]GatewayWire, 0, len(pg.Data))
	for _, g := range pg.Data {
		out = append(out, gatewayView(g))
	}

	return page.Page[GatewayWire]{Data: out, Next: pg.Next}, nil
}

// stateMatches filters by lifecycleState; without a filter, DELETED resources are hidden.
func stateMatches(have, want string) bool {
	if want == "" {
		return have != stateDeleted
	}

	return strings.EqualFold(have, want)
}

// UpdateGateway updates mutable gateway attributes and returns the work request id.
func (b *InMemoryBackend) UpdateGateway(id, ifMatch string, in updateGatewayIn) (string, error) {
	b.mu.Lock("UpdateGateway")
	defer b.mu.Unlock()

	g, err := b.liveGateway(id)
	if err != nil {
		return "", err
	}

	if g.State != stateActive {
		return "", errState("gateway " + id + " is in state " + g.State)
	}

	if err = checkEtag(g.Etag, ifMatch); err != nil {
		return "", err
	}

	if in.CertificateID != nil {
		if cerr := b.checkCertificateRef(*in.CertificateID); cerr != nil {
			return "", cerr
		}

		g.CertificateID = *in.CertificateID
	}

	if in.DisplayName != nil && *in.DisplayName != "" {
		if b.gatewayNameTaken(g.CompartmentID, *in.DisplayName, g.ID) {
			return "", errConflict("a gateway named " + *in.DisplayName + " already exists in this compartment")
		}

		g.DisplayName = *in.DisplayName
	}

	if in.NSGs != nil {
		g.NSGs = in.NSGs
	}

	if len(in.CaBundles) > 0 {
		g.CaBundles = nullRaw(in.CaBundles)
	}

	if len(in.ResponseCache) > 0 && !jsonIsNoCache(in.ResponseCache) && string(in.ResponseCache) != jsonNull {
		return "", errUnsupported("responseCacheDetails other than type NONE")
	}

	g.Tags.merge(in.Tags)

	return b.touchGateway(g, wrUpdateGateway), nil
}

func (b *InMemoryBackend) touchGateway(g *Gateway, op string) string {
	g.State, g.TransitionAt = stateUpdating, b.transitionAt()
	g.TimeUpdated, g.Etag = b.nowUTC(), newEtag()

	return b.newWR(op, g.CompartmentID, entityGateway, g.ID, actionUpdated, *g.TransitionAt)
}

// DeleteGateway starts deleting a gateway that has no deployments.
func (b *InMemoryBackend) DeleteGateway(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteGateway")
	defer b.mu.Unlock()

	g, err := b.liveGateway(id)
	if err != nil {
		return "", err
	}

	if g.State != stateActive {
		return "", errState("gateway " + id + " is in state " + g.State)
	}

	if err = checkEtag(g.Etag, ifMatch); err != nil {
		return "", err
	}

	for _, d := range b.deployments {
		if d.GatewayID == id && d.State != stateDeleted {
			return "", errConflict("gateway " + id + " still has API deployments")
		}
	}

	g.State, g.TransitionAt = stateDeleting, b.transitionAt()
	g.TimeUpdated = b.nowUTC()

	return b.newWR(wrDeleteGateway, g.CompartmentID, entityGateway, id, actionDeleted, *g.TransitionAt), nil
}

// ChangeGatewayCompartment moves a gateway.
func (b *InMemoryBackend) ChangeGatewayCompartment(id, ifMatch string, in changeCompartmentIn) (string, error) {
	b.mu.Lock("ChangeGatewayCompartment")
	defer b.mu.Unlock()

	g, err := b.liveGateway(id)
	if err != nil {
		return "", err
	}

	if err = b.checkMove(g.Etag, ifMatch, in.CompartmentID); err != nil {
		return "", err
	}

	if b.gatewayNameTaken(in.CompartmentID, g.DisplayName, g.ID) {
		return "", errConflict("a gateway named " + g.DisplayName + " already exists in the target compartment")
	}

	g.CompartmentID, g.TimeUpdated, g.Etag = in.CompartmentID, b.nowUTC(), newEtag()

	return b.newWR(
		wrUpdateGateway,
		g.CompartmentID,
		entityGateway,
		id,
		actionUpdated,
		b.nowUTC().Add(b.transition),
	), nil
}

func (h *Handler) createGateway(q *req) error {
	return created(q, "/gateways/", func(w GatewayWire) string { return w.ID }, h.Backend.CreateGateway)
}

func (h *Handler) getGateway(q *req) error {
	w, etag, err := h.Backend.GetGateway(q.param("gatewayId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listGateways(q *req) error {
	pg, err := h.Backend.ListGateways(q.query("compartmentId"), q.query("displayName"),
		q.query("lifecycleState"), q.query("certificateId"), q.listOpts())

	return collectionReply(q, pg, err)
}

func (h *Handler) updateGateway(q *req) error {
	return withBody(q, func(in updateGatewayIn) (string, error) {
		return h.Backend.UpdateGateway(q.param("gatewayId"), q.ifMatch(), in)
	})
}

func (h *Handler) deleteGateway(q *req) error {
	wr, err := h.Backend.DeleteGateway(q.param("gatewayId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) changeGatewayCompartment(q *req) error {
	return withBody(q, func(in changeCompartmentIn) (string, error) {
		return h.Backend.ChangeGatewayCompartment(q.param("gatewayId"), q.ifMatch(), in)
	})
}
