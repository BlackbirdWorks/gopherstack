package ociapigateway

import (
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

type createDeploymentIn struct {
	Tags
	GatewayID     string          `json:"gatewayId"`
	CompartmentID string          `json:"compartmentId"`
	PathPrefix    string          `json:"pathPrefix"`
	DisplayName   string          `json:"displayName"`
	Locks         json.RawMessage `json:"locks"`
	Specification json.RawMessage `json:"specification"`
}

type updateDeploymentIn struct {
	Tags
	DisplayName   *string         `json:"displayName"`
	Specification json.RawMessage `json:"specification"`
}

// DeploymentWire is the wire form of a deployment.
type DeploymentWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeUpdated    time.Time                 `json:"timeUpdated"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	ID             string                    `json:"id"`
	GatewayID      string                    `json:"gatewayId"`
	CompartmentID  string                    `json:"compartmentId"`
	PathPrefix     string                    `json:"pathPrefix"`
	Endpoint       string                    `json:"endpoint"`
	DisplayName    string                    `json:"displayName"`
	LifecycleState string                    `json:"lifecycleState"`
	Locks          json.RawMessage           `json:"locks,omitempty"`
	Specification  json.RawMessage           `json:"specification,omitempty"`
}

func (b *InMemoryBackend) deploymentView(d *Deployment, full bool) DeploymentWire {
	w := DeploymentWire{
		TimeCreated: d.TimeCreated, TimeUpdated: d.TimeUpdated, DefinedTags: d.Tags.DefinedTags,
		FreeformTags: d.Tags.FreeformTags, Locks: d.Locks, ID: d.ID, GatewayID: d.GatewayID,
		CompartmentID: d.CompartmentID, PathPrefix: d.PathPrefix, DisplayName: d.DisplayName, LifecycleState: d.State,
	}
	if g, ok := b.gateways[d.GatewayID]; ok {
		w.Endpoint = "https://" + g.Hostname + d.PathPrefix
	}

	if full {
		w.Specification = d.Spec
	}

	return w
}

func (b *InMemoryBackend) prefixTaken(gatewayID, prefix, except string) bool {
	for _, d := range b.deployments {
		if d.ID == except || d.GatewayID != gatewayID || d.State == stateDeleted {
			continue
		}

		if d.PathPrefix == prefix || d.PathPrefix == "/" || prefix == "/" {
			return true
		}
	}

	return false
}

// CreateDeployment creates a deployment in CREATING state.
func (b *InMemoryBackend) CreateDeployment(in createDeploymentIn) (DeploymentWire, string, string, error) {
	b.mu.Lock("CreateDeployment")
	defer b.mu.Unlock()

	if err := b.checkCompartment(in.CompartmentID); err != nil {
		return DeploymentWire{}, "", "", err
	}

	if in.GatewayID == "" {
		return DeploymentWire{}, "", "", errMissing("gatewayId is required")
	}

	g, ok := b.gateways[in.GatewayID]
	if !ok || g.State == stateDeleted {
		return DeploymentWire{}, "", "", errNotFound(in.GatewayID)
	}

	if g.State == stateDeleting {
		return DeploymentWire{}, "", "", errState("gateway " + g.ID + " is being deleted")
	}

	if err := validatePathPrefix(in.PathPrefix); err != nil {
		return DeploymentWire{}, "", "", err
	}

	cs, err := compileSpec(in.Specification)
	if err != nil {
		return DeploymentWire{}, "", "", err
	}

	if b.prefixTaken(g.ID, in.PathPrefix, "") {
		return DeploymentWire{}, "", "", errConflict(
			"pathPrefix " + in.PathPrefix + " conflicts with another deployment on this gateway",
		)
	}

	now := b.nowUTC()
	name := in.DisplayName

	if name == "" {
		name = "deployment" + now.Format("20060102150405")
	}

	d := &Deployment{
		ID: newOCID("apideployment"), GatewayID: g.ID, CompartmentID: in.CompartmentID, PathPrefix: in.PathPrefix,
		DisplayName: name, Spec: in.Specification, Locks: nullRaw(in.Locks), State: stateCreating,
		TransitionAt: b.transitionAt(), TimeCreated: now, TimeUpdated: now, Etag: newEtag(), Tags: in.Tags,
	}
	b.deployments[d.ID] = d
	b.specs[d.ID] = cs
	wr := b.newWR(wrCreateDeploy, d.CompartmentID, entityDeploy, d.ID, actionCreated, *d.TransitionAt)

	return b.deploymentView(d, true), wr, d.Etag, nil
}

func (b *InMemoryBackend) liveDeployment(id string) (*Deployment, *APIError) {
	d, ok := b.deployments[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return d, nil
}

// GetDeployment returns one deployment and its etag.
func (b *InMemoryBackend) GetDeployment(id string) (DeploymentWire, string, error) {
	b.mu.RLock("GetDeployment")
	defer b.mu.RUnlock()

	d, err := b.liveDeployment(id)
	if err != nil {
		return DeploymentWire{}, "", err
	}

	return b.deploymentView(d, true), d.Etag, nil
}

// ListDeployments lists the deployments of a compartment, optionally of one gateway.
func (b *InMemoryBackend) ListDeployments(
	compartment, gatewayID, name, state string, o ListOpts,
) (page.Page[DeploymentWire], error) {
	if compartment == "" {
		return page.Page[DeploymentWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListDeployments")
	defer b.mu.RUnlock()

	var all []*Deployment

	for _, d := range b.deployments {
		if d.CompartmentID != compartment || !stateMatches(d.State, state) ||
			(gatewayID != "" && d.GatewayID != gatewayID) || (name != "" && !strs.Equal(d.DisplayName, name)) {
			continue
		}

		all = append(all, d)
	}

	pg := paginate(all, o, func(d *Deployment) string { return d.DisplayName },
		func(d *Deployment) (time.Time, int64) { return d.TimeCreated, 0 })

	out := make([]DeploymentWire, 0, len(pg.Data))
	for _, d := range pg.Data {
		out = append(out, b.deploymentView(d, false))
	}

	return page.Page[DeploymentWire]{Data: out, Next: pg.Next}, nil
}

// UpdateDeployment replaces the specification and/or display name and tags.
func (b *InMemoryBackend) UpdateDeployment(id, ifMatch string, in updateDeploymentIn) (string, error) {
	b.mu.Lock("UpdateDeployment")
	defer b.mu.Unlock()

	d, err := b.liveDeployment(id)
	if err != nil {
		return "", err
	}

	if d.State != stateActive {
		return "", errState("deployment " + id + " is in state " + d.State)
	}

	if err = checkEtag(d.Etag, ifMatch); err != nil {
		return "", err
	}

	if isPresent(in.Specification) {
		cs, cerr := compileSpec(in.Specification)
		if cerr != nil {
			return "", cerr
		}

		d.Spec, b.specs[id] = in.Specification, cs
	}

	if in.DisplayName != nil && *in.DisplayName != "" {
		d.DisplayName = *in.DisplayName
	}

	d.Tags.merge(in.Tags)
	d.State, d.TransitionAt = stateUpdating, b.transitionAt()
	d.TimeUpdated, d.Etag = b.nowUTC(), newEtag()

	return b.newWR(wrUpdateDeploy, d.CompartmentID, entityDeploy, id, actionUpdated, *d.TransitionAt), nil
}

// DeleteDeployment starts deleting a deployment.
func (b *InMemoryBackend) DeleteDeployment(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteDeployment")
	defer b.mu.Unlock()

	d, err := b.liveDeployment(id)
	if err != nil {
		return "", err
	}

	if d.State != stateActive {
		return "", errState("deployment " + id + " is in state " + d.State)
	}

	if err = checkEtag(d.Etag, ifMatch); err != nil {
		return "", err
	}

	d.State, d.TransitionAt = stateDeleting, b.transitionAt()
	d.TimeUpdated = b.nowUTC()

	return b.newWR(wrDeleteDeploy, d.CompartmentID, entityDeploy, id, actionDeleted, *d.TransitionAt), nil
}

// ChangeDeploymentCompartment moves a deployment.
func (b *InMemoryBackend) ChangeDeploymentCompartment(id, ifMatch string, in changeCompartmentIn) (string, error) {
	b.mu.Lock("ChangeDeploymentCompartment")
	defer b.mu.Unlock()

	d, err := b.liveDeployment(id)
	if err != nil {
		return "", err
	}

	if err = b.checkMove(d.Etag, ifMatch, in.CompartmentID); err != nil {
		return "", err
	}

	d.CompartmentID, d.TimeUpdated, d.Etag = in.CompartmentID, b.nowUTC(), newEtag()

	return b.newWR(wrUpdateDeploy, d.CompartmentID, entityDeploy, id, actionUpdated, b.nowUTC().Add(b.transition)), nil
}

func (h *Handler) createDeployment(q *req) error {
	return created(q, "/deployments/", func(w DeploymentWire) string { return w.ID }, h.Backend.CreateDeployment)
}

func (h *Handler) getDeployment(q *req) error {
	w, etag, err := h.Backend.GetDeployment(q.param("deploymentId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listDeployments(q *req) error {
	pg, err := h.Backend.ListDeployments(q.query("compartmentId"), q.query("gatewayId"),
		q.query("displayName"), q.query("lifecycleState"), q.listOpts())

	return collectionReply(q, pg, err)
}

func (h *Handler) updateDeployment(q *req) error {
	return withBody(q, func(in updateDeploymentIn) (string, error) {
		return h.Backend.UpdateDeployment(q.param("deploymentId"), q.ifMatch(), in)
	})
}

func (h *Handler) deleteDeployment(q *req) error {
	wr, err := h.Backend.DeleteDeployment(q.param("deploymentId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) changeDeploymentCompartment(q *req) error {
	return withBody(q, func(in changeCompartmentIn) (string, error) {
		return h.Backend.ChangeDeploymentCompartment(q.param("deploymentId"), q.ifMatch(), in)
	})
}
