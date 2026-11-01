package ocicontainerinstances

import (
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// Operation types recorded on work requests.
const (
	wrCreate  = "CREATE_CONTAINER_INSTANCE"
	wrUpdate  = "UPDATE_CONTAINER_INSTANCE"
	wrDelete  = "DELETE_CONTAINER_INSTANCE"
	wrMove    = "MOVE_CONTAINER_INSTANCE"
	wrStart   = "START_CONTAINER_INSTANCE"
	wrStop    = "STOP_CONTAINER_INSTANCE"
	wrRestart = "RESTART_CONTAINER_INSTANCE"
	wrUpdCtr  = "UPDATE_CONTAINER"
)

func (in *Instance) clone() *Instance {
	raw, err := json.Marshal(in)
	if err != nil {
		return in
	}

	out := &Instance{}
	if json.Unmarshal(raw, out) != nil {
		return in
	}

	return out
}

func (b *InMemoryBackend) liveInstance(id string) (*Instance, *APIError) {
	in, ok := b.instances[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return in, nil
}

// CreateInstance registers a container instance and starts its work request.
func (b *InMemoryBackend) CreateInstance(in createIn, token string) (instanceWire, string, string, error) {
	if aerr := b.checkVaultSecrets(in.Secrets); aerr != nil {
		return instanceWire{}, "", "", aerr
	}

	b.mu.Lock("CreateInstance")

	if token != "" {
		for _, ex := range b.instances {
			if ex.RetryToken == token {
				w, etag, wr := instanceView(ex), ex.Etag, b.wrFor(ex.ID, wrCreate)
				b.mu.Unlock()

				return w, wr, etag, nil
			}
		}
	}

	inst, aerr := b.buildInstance(in)
	if aerr != nil {
		b.mu.Unlock()

		return instanceWire{}, "", "", aerr
	}

	inst.RetryToken = token
	b.instances[inst.ID] = inst

	for _, c := range inst.Containers {
		b.containers[c.ID] = inst.ID
	}

	wr := b.newWR(inst, wrCreate, "CREATED", nil)
	eng := b.engine

	if eng == nil {
		inst.Target, inst.TransitionAt = stateActive, b.finishAt()
		wr.FinishAt = inst.TransitionAt
	}

	view, etag, snap := instanceView(inst), inst.Etag, inst.clone()
	b.mu.Unlock()

	if eng != nil {
		b.launch(snap, wr.ID)
	}

	return view, wr.ID, etag, nil
}

func (b *InMemoryBackend) wrFor(instID, typ string) string {
	var best *WorkRequest

	for _, wr := range b.workReqs {
		if wr.ResourceID == instID && wr.Type == typ && (best == nil || wr.Seq > best.Seq) {
			best = wr
		}
	}

	if best == nil {
		return ""
	}

	return best.ID
}

func (b *InMemoryBackend) checkVaultSecrets(secrets []createSecretIn) *APIError {
	b.mu.RLock("checkVaultSecrets")
	read := b.secrets
	b.mu.RUnlock()

	if read == nil {
		return nil
	}

	for _, s := range secrets {
		if s.SecretType != secretVault || s.SecretID == "" {
			continue
		}

		if _, err := read(s.SecretID); err != nil {
			return errNotFound(s.SecretID)
		}
	}

	return nil
}

// launch pulls and starts the instance's containers in the engine and completes the work request.
func (b *InMemoryBackend) launch(snap *Instance, wrID string) {
	b.mu.RLock("launch")
	eng := b.engine
	b.mu.RUnlock()

	eng.Launch(snap, b.resolveCreds, func(err error) { b.complete(snap.ID, wrID, stateActive, err) })
}

// resolveCreds turns an instance's pull secrets into registry credentials, reading VAULT secrets from OCI Vault.
func (b *InMemoryBackend) resolveCreds(secrets []PullSecret) ([]Credential, error) {
	b.mu.RLock("resolveCreds")
	read := b.secrets
	b.mu.RUnlock()

	out := make([]Credential, 0, len(secrets))

	for _, s := range secrets {
		if s.Type == secretBasic {
			out = append(out, Credential{Endpoint: s.Endpoint, Username: s.Username, Password: s.Password})

			continue
		}

		if read == nil {
			return nil, errVaultUnavailable
		}

		raw, err := read(s.SecretID)
		if err != nil {
			return nil, err
		}

		var auth struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}

		if json.Unmarshal([]byte(raw), &auth) != nil || auth.Username == "" || auth.Password == "" {
			return nil, errVaultSecretShape
		}

		out = append(out, Credential{Endpoint: s.Endpoint, Username: auth.Username, Password: auth.Password})
	}

	return out, nil
}

// complete finishes an engine-driven transition.
func (b *InMemoryBackend) complete(instID, wrID, target string, err error) {
	b.mu.Lock("complete")
	defer b.mu.Unlock()

	inst, ok := b.instances[instID]
	if !ok || inst.State == stateDeleted {
		return
	}

	now := b.nowUTC()
	wr := b.workReqs[wrID]

	if err != nil {
		b.setState(inst, stateFailed, err.Error())

		if wr != nil {
			wr.Status, wr.FinishedAt, wr.ErrCode, wr.ErrMsg = wrFailed, &now, "ContainerInstanceFailed", err.Error()
		}
	} else {
		b.setState(inst, target, "")

		if wr != nil {
			wr.Status, wr.FinishedAt = wrSucceeded, &now
		}
	}

	inst.Etag = newEtag()
}

// GetInstance returns one container instance with its etag.
func (b *InMemoryBackend) GetInstance(id string) (instanceWire, string, error) {
	b.mu.RLock("GetInstance")
	defer b.mu.RUnlock()

	in, aerr := b.liveInstance(id)
	if aerr != nil {
		return instanceWire{}, "", aerr
	}

	return instanceView(in), in.Etag, nil
}

// InstanceFilter selects container instances in a list.
type InstanceFilter struct {
	Compartment string
	State       string
	DisplayName string
	AD          string
}

// ListInstances lists container instances of a compartment.
func (b *InMemoryBackend) ListInstances(f InstanceFilter, o ListOpts) (page.Page[instanceSummaryWire], error) {
	b.mu.RLock("ListInstances")
	defer b.mu.RUnlock()

	if aerr := b.checkCompartment(f.Compartment); aerr != nil {
		return page.Page[instanceSummaryWire]{}, aerr
	}

	var all []*Instance

	for _, in := range b.instances {
		if in.Compartment == f.Compartment && (f.State == "" || in.State == f.State) &&
			(f.DisplayName == "" || in.DisplayName == f.DisplayName) && (f.AD == "" || in.AD == f.AD) {
			all = append(all, in)
		}
	}

	pg := paginate(all, o, func(i *Instance) string { return i.DisplayName },
		func(i *Instance) (time.Time, int64) { return i.Created, 0 })

	out := make([]instanceSummaryWire, 0, len(pg.Data))
	for _, in := range pg.Data {
		out = append(out, summaryOf(in))
	}

	return page.Page[instanceSummaryWire]{Data: out, Next: pg.Next}, nil
}

// UpdateInstance changes the display name and tags.
func (b *InMemoryBackend) UpdateInstance(id, ifMatch string, u updateIn) (string, error) {
	b.mu.Lock("UpdateInstance")
	defer b.mu.Unlock()

	in, aerr := b.liveInstance(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(in.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if in.State == stateDeleting || in.State == stateDeleted {
		return "", errState("container instance is " + in.State)
	}

	if u.DisplayName != nil {
		in.DisplayName = *u.DisplayName
	}

	applyTags(&in.Freeform, &in.Defined, u)

	now := b.nowUTC()
	in.Updated, in.Etag = &now, newEtag()

	return b.newWR(in, wrUpdate, actionUpdated, b.finishAt()).ID, nil
}

func applyTags(free *map[string]string, defined *map[string]any, u updateIn) {
	if u.Freeform != nil {
		*free = u.Freeform
	}

	if u.Defined != nil {
		*defined = u.Defined
	}
}

// ChangeCompartment moves the instance to another compartment.
func (b *InMemoryBackend) ChangeCompartment(id, ifMatch, compartment string) (string, error) {
	b.mu.Lock("ChangeCompartment")
	defer b.mu.Unlock()

	in, aerr := b.liveInstance(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(in.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if aerr = b.checkCompartment(compartment); aerr != nil {
		return "", aerr
	}

	if in.State == stateDeleting || in.State == stateDeleted {
		return "", errState("container instance is " + in.State)
	}

	in.Compartment, in.Etag = compartment, newEtag()
	now := b.nowUTC()
	in.Updated = &now

	return b.newWR(in, wrMove, actionUpdated, b.finishAt()).ID, nil
}

type transition struct {
	typ     string
	action  string
	mid     string
	target  string
	allowed []string
}

const actionUpdated = "UPDATED"

func transStop() transition {
	return transition{wrStop, actionUpdated, stateUpdating, stateInactive, []string{stateActive}}
}

func transStart() transition {
	return transition{wrStart, actionUpdated, stateUpdating, stateActive, []string{stateInactive}}
}

func transRestart() transition {
	return transition{wrRestart, actionUpdated, stateUpdating, stateActive, []string{stateActive}}
}

func transDelete() transition {
	return transition{
		wrDelete,
		"DELETED",
		stateDeleting,
		stateDeleted,
		[]string{stateActive, stateInactive, stateFailed},
	}
}

// Stop stops a running container instance.
func (b *InMemoryBackend) Stop(id, ifMatch string) (string, error) {
	return b.transitionInstance(id, ifMatch, transStop())
}

// Start starts a stopped container instance.
func (b *InMemoryBackend) Start(id, ifMatch string) (string, error) {
	return b.transitionInstance(id, ifMatch, transStart())
}

// Restart restarts a running container instance.
func (b *InMemoryBackend) Restart(id, ifMatch string) (string, error) {
	return b.transitionInstance(id, ifMatch, transRestart())
}

// DeleteInstance deletes a container instance.
func (b *InMemoryBackend) DeleteInstance(id, ifMatch string) (string, error) {
	return b.transitionInstance(id, ifMatch, transDelete())
}

func (b *InMemoryBackend) transitionInstance(id, ifMatch string, t transition) (string, error) {
	b.mu.Lock("transitionInstance")

	in, aerr := b.liveInstance(id)
	if aerr == nil {
		aerr = checkEtag(in.Etag, ifMatch)
	}

	if aerr == nil && !slices.Contains(t.allowed, in.State) {
		aerr = errState(
			"container instance " + id + " is " + in.State + "; it must be " + strings.Join(t.allowed, " or "),
		)
	}

	if aerr != nil {
		b.mu.Unlock()

		return "", aerr
	}

	wr := b.newWR(in, t.typ, t.action, nil)
	eng := b.engine
	b.setState(in, t.mid, "")
	in.Etag = newEtag()

	if eng == nil {
		in.Target, in.TransitionAt = t.target, b.finishAt()
		wr.FinishAt = in.TransitionAt
		b.mu.Unlock()

		return wr.ID, nil
	}

	snap := in.clone()
	b.mu.Unlock()

	b.runEngineTransition(snap, wr.ID, t)

	return wr.ID, nil
}

func (b *InMemoryBackend) runEngineTransition(snap *Instance, wrID string, t transition) {
	b.mu.RLock("runEngineTransition")
	eng := b.engine
	b.mu.RUnlock()

	switch t.typ {
	case wrStart:
		eng.Launch(snap, b.resolveCreds, func(err error) { b.complete(snap.ID, wrID, t.target, err) })
	case wrRestart:
		eng.Relaunch(snap, b.resolveCreds, func(err error) { b.complete(snap.ID, wrID, t.target, err) })
	default:
		eng.HaltAsync(snap.ID, func() { b.complete(snap.ID, wrID, t.target, nil) })
	}
}

// GetContainer returns one container.
func (b *InMemoryBackend) GetContainer(id string) (containerWire, error) {
	b.mu.RLock("GetContainer")
	defer b.mu.RUnlock()

	in, c := b.findContainer(id)
	if c == nil {
		return containerWire{}, errNotFound(id)
	}

	return containerView(in, c), nil
}

func (b *InMemoryBackend) findContainer(id string) (*Instance, *Container) {
	in, ok := b.instances[b.containers[id]]
	if !ok {
		return nil, nil
	}

	for _, c := range in.Containers {
		if c.ID == id {
			return in, c
		}
	}

	return nil, nil
}

// ContainerFilter selects containers in a list.
type ContainerFilter struct {
	Compartment string
	State       string
	DisplayName string
	InstanceID  string
	AD          string
}

// ListContainers lists containers of a compartment.
func (b *InMemoryBackend) ListContainers(f ContainerFilter, o ListOpts) (page.Page[containerSummaryWire], error) {
	b.mu.RLock("ListContainers")
	defer b.mu.RUnlock()

	if aerr := b.checkCompartment(f.Compartment); aerr != nil {
		return page.Page[containerSummaryWire]{}, aerr
	}

	type pair struct {
		in *Instance
		c  *Container
	}

	var all []pair

	for _, in := range b.instances {
		if in.Compartment != f.Compartment || (f.InstanceID != "" && in.ID != f.InstanceID) ||
			(f.AD != "" && in.AD != f.AD) {
			continue
		}

		for _, c := range in.Containers {
			if (f.State == "" || c.State == f.State) && (f.DisplayName == "" || c.DisplayName == f.DisplayName) {
				all = append(all, pair{in, c})
			}
		}
	}

	pg := paginate(all, o, func(p pair) string { return p.c.DisplayName },
		func(p pair) (time.Time, int64) { return p.c.Created, 0 })

	out := make([]containerSummaryWire, 0, len(pg.Data))
	for _, p := range pg.Data {
		out = append(out, containerSummaryOf(p.in, p.c))
	}

	return page.Page[containerSummaryWire]{Data: out, Next: pg.Next}, nil
}

// UpdateContainer changes a container's display name and tags.
func (b *InMemoryBackend) UpdateContainer(id, ifMatch string, u updateIn) (string, error) {
	b.mu.Lock("UpdateContainer")
	defer b.mu.Unlock()

	in, c := b.findContainer(id)
	if c == nil {
		return "", errNotFound(id)
	}

	if aerr := checkEtag(in.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if in.State == stateDeleting || in.State == stateDeleted {
		return "", errState("container instance is " + in.State)
	}

	if u.DisplayName != nil {
		if slices.ContainsFunc(in.Containers, func(o *Container) bool {
			return o.ID != c.ID && strs.Equal(o.DisplayName, *u.DisplayName)
		}) {
			return "", errParam("a container named " + *u.DisplayName + " already exists in this instance")
		}

		c.DisplayName = *u.DisplayName
	}

	applyTags(&c.Freeform, &c.Defined, u)

	now := b.nowUTC()
	c.Updated = &now

	wr := b.newWR(in, wrUpdCtr, actionUpdated, b.finishAt())
	wr.ResourceID, wr.ResourceType = c.ID, "container"

	return wr.ID, nil
}

// RetrieveLogs returns the container's stdout and stderr.
func (b *InMemoryBackend) RetrieveLogs(id string, previous bool) (string, error) {
	b.mu.RLock("RetrieveLogs")
	in, c := b.findContainer(id)
	eng := b.engine

	var instID, ctrID, state string
	if c != nil {
		instID, ctrID, state = in.ID, c.ID, in.State
	}
	b.mu.RUnlock()

	switch {
	case c == nil:
		return "", errNotFound(id)
	case eng == nil:
		return "", errState("container logs need the docker engine (--oci-container-instances-engine=docker)")
	case state == stateCreating:
		return "", errState("container instance is still being created")
	}

	out, err := eng.Logs(instID, ctrID, previous)
	if err != nil {
		return "", errState(err.Error())
	}

	return out, nil
}
