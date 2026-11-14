package ocimysql

import (
	"context"
	"encoding/json/jsontext"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/ociretry"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	entityReplica       = "replica"
	wrCreateReplica     = "CREATE_REPLICA"
	wrUpdateReplica     = "UPDATE_REPLICA"
	wrDeleteReplica     = "DELETE_REPLICA"
	maxReplicas         = 18
	minReplicaECPUs     = 8
	minReplicaOCPUs     = 4
	vmShapePrefix       = "MySQL.VM."
	paramReplica        = "replicaId"
	resourceReadReplica = "READ_REPLICA"
	resourceLB          = "LOAD_BALANCER"
)

// ReplicaOverrides are the properties a read replica sets itself instead of inheriting them.
type ReplicaOverrides struct {
	SecurityAttributes map[string]map[string]any `json:"securityAttributes,omitempty"`
	Telemetry          jsontext.Value            `json:"telemetryConfiguration,omitempty"`
	MysqlVersion       string                    `json:"mysqlVersion,omitempty"`
	ShapeName          string                    `json:"shapeName,omitempty"`
	ConfigurationID    string                    `json:"configurationId,omitempty"`
	NsgIDs             []string                  `json:"nsgIds,omitempty"`
}

// Replica is a read replica of a DB system.
type Replica struct {
	TimeCreated     time.Time        `json:"timeCreated"`
	TimeUpdated     time.Time        `json:"timeUpdated"`
	Tags            Tags             `json:"tags"`
	TransitionAt    *time.Time       `json:"transitionAt,omitempty"`
	State           string           `json:"state"`
	Etag            string           `json:"etag"`
	DBSystemID      string           `json:"dbSystemId"`
	CompartmentID   string           `json:"compartmentId"`
	Name            string           `json:"displayName"`
	Description     string           `json:"description,omitempty"`
	AppliedConfig   string           `json:"appliedConfig,omitempty"`
	After           string           `json:"after,omitempty"`
	Details         string           `json:"details,omitempty"`
	ID              string           `json:"id"`
	IP              string           `json:"ipAddress"`
	RootSecret      string           `json:"rootSecret,omitempty"`
	EngineHost      string           `json:"engineHost,omitempty"`
	Overrides       ReplicaOverrides `json:"overrides"`
	Seq             int64            `json:"seq"`
	EnginePort      int              `json:"enginePort,omitempty"`
	ServerID        uint32           `json:"serverID,omitempty"`
	DeleteProtected bool             `json:"isDeleteProtected"`
}

// ReplicaOverridesIn is the replicaOverrides request object.
type ReplicaOverridesIn struct {
	SecurityAttributes map[string]map[string]any `json:"securityAttributes"`
	Telemetry          jsontext.Value            `json:"telemetryConfiguration"`
	MysqlVersion       *string                   `json:"mysqlVersion"`
	ShapeName          *string                   `json:"shapeName"`
	ConfigurationID    *string                   `json:"configurationId"`
	NsgIDs             []string                  `json:"nsgIds"`
}

// CreateReplicaIn is the CreateReplica body.
type CreateReplicaIn struct {
	RetryToken string `json:"-"`
	Tags
	IsDeleteProtected *bool               `json:"isDeleteProtected"`
	ReplicaOverrides  *ReplicaOverridesIn `json:"replicaOverrides"`
	DBSystemID        string              `json:"dbSystemId"`
	DisplayName       string              `json:"displayName"`
	Description       string              `json:"description"`
}

// UpdateReplicaIn is the UpdateReplica body.
type UpdateReplicaIn struct {
	Tags
	IsDeleteProtected *bool               `json:"isDeleteProtected"`
	ReplicaOverrides  *ReplicaOverridesIn `json:"replicaOverrides"`
	DisplayName       *string             `json:"displayName"`
	Description       *string             `json:"description"`
}

func replicaShapeOK(shape string) bool {
	sh, ok := shapeByName(shape)
	if !ok || shape == shapeFree {
		return false
	}

	if strings.HasPrefix(shape, vmShapePrefix) {
		return sh.cpus >= minReplicaOCPUs
	}

	return sh.cpus >= minReplicaECPUs
}

func (r *Replica) shape(s *DBSystem) string { return orStr(r.Overrides.ShapeName, s.Shape) }

func (r *Replica) version(s *DBSystem) string { return orStr(r.Overrides.MysqlVersion, s.Version) }

func (r *Replica) config(s *DBSystem) string {
	switch {
	case r.Overrides.ConfigurationID != "":
		return r.Overrides.ConfigurationID
	case r.shape(s) == s.Shape:
		return s.ConfigID
	default:
		return defaultConfigID(r.shape(s), false)
	}
}

func (r *Replica) upToDate(s *DBSystem) bool {
	return r.shape(s) == s.Shape && r.version(s) == s.Version && r.config(s) == s.ConfigID
}

// mergeOverrides applies in over cur; an empty string clears the override so the value is inherited again.
func mergeOverrides(cur ReplicaOverrides, in *ReplicaOverridesIn) ReplicaOverrides {
	if in == nil {
		return cur
	}

	if in.MysqlVersion != nil {
		cur.MysqlVersion = *in.MysqlVersion
	}

	if in.ShapeName != nil {
		cur.ShapeName = *in.ShapeName
	}

	if in.ConfigurationID != nil {
		cur.ConfigurationID = *in.ConfigurationID
	}

	if in.NsgIDs != nil {
		cur.NsgIDs = in.NsgIDs
	}

	if in.SecurityAttributes != nil {
		cur.SecurityAttributes = in.SecurityAttributes
	}

	if in.Telemetry != nil {
		cur.Telemetry = in.Telemetry
	}

	return cur
}

func (b *InMemoryBackend) validateReplica(r *Replica, s *DBSystem) *APIError {
	shape, version := r.shape(s), r.version(s)

	switch {
	case !replicaShapeOK(shape):
		return errParam("shape " + shape + " does not support read replicas: Always Free and shapes below 8 ECPUs " +
			"or 4 OCPUs are not supported")
	case !isKnownVersion(version):
		return errParam("mysqlVersion " + version + " is not supported; see ListVersions")
	}

	if id := r.Overrides.ConfigurationID; id != "" {
		if _, aerr := b.resolveConfig(id, shape, false); aerr != nil {
			return aerr
		}
	}

	return nil
}

func (b *InMemoryBackend) liveReplicas(sysID string) []*Replica {
	var out []*Replica

	for _, r := range b.replicas {
		if r.DBSystemID == sysID && r.State != stateDeleted {
			out = append(out, r)
		}
	}

	return out
}

// allocFree picks the first free host of the DB system's subnet that is not in skip.
func (b *InMemoryBackend) allocFree(s *DBSystem, skip string) (string, *APIError) {
	sub, aerr := b.checkSubnet(s.SubnetID)
	if aerr != nil {
		return "", aerr
	}

	if sub.CIDR == "" {
		return documentationAddr(b.nextSeq()), nil
	}

	taken := b.takenAddrs(sub.ID, "")

	for off := firstHostOffset; ; off++ {
		ip := hostIn(sub.CIDR, off)
		if ip == "" {
			return "", errParam("the subnet has no free addresses")
		}

		if ip != skip && !taken[ip] {
			return ip, nil
		}
	}
}

func (b *InMemoryBackend) replicaEndpointStatus(r *Replica) string {
	switch r.State {
	case stateInactive, stateFailed, stateDeleted, stateNeedsAttention:
		return "INACTIVE"
	case stateUpdating, stateCreating:
		return "UPDATING"
	}

	return stateActive
}

func (b *InMemoryBackend) replicaAddr(r *Replica, s *DBSystem) (string, int) {
	if r.EngineHost != "" && r.EnginePort != 0 {
		return r.EngineHost, r.EnginePort
	}

	return r.IP, s.Port
}

func (b *InMemoryBackend) replicaDoc(r *Replica) Doc {
	s := b.systems[r.DBSystemID]
	ip, port := b.replicaAddr(r, s)

	d := Doc{
		"id": r.ID, paramDBSystem: r.DBSystemID, keyCompartment: r.CompartmentID, keyDisplayName: r.Name,
		keyLifecycle: r.State, keyTimeCreated: r.TimeCreated, keyTimeUpdated: r.TimeUpdated,
		"mysqlVersion": r.version(s), keyIPAddress: ip, keyPort: port, keyPortX: s.PortX,
		keyAD: s.AD, keyFD: s.FD, keyShapeName: r.shape(s), "configurationId": r.config(s),
		"isDeleteProtected": r.DeleteProtected,
	}
	putTags(d, r.Tags)

	if r.Description != "" {
		d["description"] = r.Description
	}

	if r.Details != "" {
		d["lifecycleDetails"] = r.Details
	}

	d["replicaOverrides"] = r.Overrides

	if len(r.Overrides.NsgIDs) > 0 {
		d["nsgIds"] = r.Overrides.NsgIDs
	}

	if r.Overrides.SecurityAttributes != nil {
		d["securityAttributes"] = r.Overrides.SecurityAttributes
	}

	if r.Overrides.Telemetry != nil {
		d["telemetryConfiguration"] = r.Overrides.Telemetry
	}

	return d
}

func (b *InMemoryBackend) replicaEndpoints(s *DBSystem) []Doc {
	var out []Doc

	for _, r := range b.liveReplicas(s.ID) {
		ip, port := b.replicaAddr(r, s)
		out = append(out, Doc{
			keyIPAddress: ip, keyPort: port, keyPortX: s.PortX, keyIPVersion: ipv4, keyModes: []string{modeRead},
			keyStatus: b.replicaEndpointStatus(r), keyResourceType: resourceReadReplica, "resourceId": r.ID,
		})
	}

	if s.ReadIP != "" && b.eng == nil && len(out) > 0 {
		out = append(out, Doc{
			keyIPAddress: s.ReadIP, keyPort: s.Port, keyPortX: s.PortX, keyIPVersion: ipv4,
			keyModes: []string{modeRead}, keyStatus: stateActive, keyResourceType: resourceLB,
		})
	}

	return out
}

// CreateReplica creates a read replica of an ACTIVE DB system.
func (b *InMemoryBackend) CreateReplica(in CreateReplicaIn) (Doc, string, string, error) {
	b.mu.Lock("CreateReplica")
	defer b.mu.Unlock()

	id, rwr, hit, rerr := b.replayCreate("CreateReplica", in.RetryToken, func(id string) (string, bool) {
		r, ok := b.replicas[id]
		if !ok {
			return "", false
		}

		return r.State, true
	})
	if hit {
		if rerr != nil {
			return nil, "", "", rerr
		}

		r := b.replicas[id]

		return b.replicaDoc(r), r.Etag, rwr, nil
	}

	r, aerr := b.buildReplica(in)
	if aerr != nil {
		return nil, "", "", aerr
	}

	r.TransitionAt = b.transitionAt()
	if b.eng != nil {
		r.TransitionAt = nil
	}

	wr := b.newWR(wrCreateReplica, r.CompartmentID, entityReplica, r.ID, actionCreated, r.TransitionAt)
	b.replicas[r.ID] = r
	b.retry.Remember("CreateReplica", in.RetryToken, ociretry.Join(r.ID, wr), b.nowUTC())

	if b.eng != nil {
		b.spawnReplica(r.ID, wr, b.replicaWork(r.ID, true), func(r *Replica) { r.State = stateActive })
	}

	return b.replicaDoc(r), r.Etag, wr, nil
}

func (b *InMemoryBackend) buildReplica(in CreateReplicaIn) (*Replica, *APIError) {
	if in.DBSystemID == "" {
		return nil, errMissing("dbSystemId is required")
	}

	b.settleID(in.DBSystemID)

	s, ok := b.systems[in.DBSystemID]
	if !ok || s.State == stateDeleted {
		return nil, errNotFound(in.DBSystemID)
	}

	if s.State != stateActive {
		return nil, errState("DB system " + s.ID + " is in state " + s.State + "; read replicas require ACTIVE")
	}

	if len(b.liveReplicas(s.ID)) >= maxReplicas {
		return nil, newErr(http.StatusBadRequest, "LimitExceeded", "A DB system can have at most 18 read replicas")
	}

	now := b.nowUTC()
	r := &Replica{
		ID: newOCID("mysqlreplica"), DBSystemID: s.ID, CompartmentID: s.CompartmentID, Description: in.Description,
		State: stateCreating, Etag: newEtag(), Tags: in.Tags, TimeCreated: now, TimeUpdated: now, Seq: b.nextSeq(),
		Overrides: mergeOverrides(ReplicaOverrides{}, in.ReplicaOverrides), RootSecret: newSecret(),
		ServerID: randomServerID(), DeleteProtected: in.IsDeleteProtected != nil && *in.IsDeleteProtected,
	}

	r.Name = orStr(in.DisplayName, "MySQLReplica"+now.Format("20060102150405"))

	if aerr := firstErr(validateName(r.Name), b.validateReplica(r, s)); aerr != nil {
		return nil, aerr
	}

	ip, aerr := b.allocFree(s, "")
	if aerr != nil {
		return nil, aerr
	}

	r.IP = ip

	if s.ReadIP == "" {
		if s.ReadIP, aerr = b.allocFree(s, ip); aerr != nil {
			return nil, aerr
		}
	}

	return r, nil
}

// GetReplica returns one read replica.
func (b *InMemoryBackend) GetReplica(id string) (Doc, string, error) {
	b.mu.Lock("GetReplica")
	defer b.mu.Unlock()

	b.settleID(id)

	r, ok := b.replicas[id]
	if !ok {
		return nil, "", errNotFound(id)
	}

	return b.replicaDoc(r), r.Etag, nil
}

// ReplicaListIn are the ListReplicas filters.
type ReplicaListIn struct {
	UpToDate        *bool
	Compartment     string
	ID              string
	DBSystemID      string
	Name            string
	State           string
	ConfigurationID string
	ListOpts
}

// ListReplicas lists the read replicas of a compartment.
func (b *InMemoryBackend) ListReplicas(in ReplicaListIn) (page.Page[Doc], error) {
	if aerr := firstErr(
		b.checkCompartmentLocked(in.Compartment),
		validListOpts(in.ListOpts, keyDisplayName, keyTimeCreated),
		validEnum(keyLifecycle, in.State, stateCreating, stateActive, stateInactive, stateUpdating,
			stateDeleting, stateDeleted, stateNeedsAttention, stateFailed),
	); aerr != nil {
		return page.Page[Doc]{}, aerr
	}

	b.mu.Lock("ListReplicas")
	defer b.mu.Unlock()

	b.settleAll()

	var all []*Replica

	for _, r := range b.replicas {
		if b.replicaMatches(r, in) {
			all = append(all, r)
		}
	}

	pg := paginate(all, in.ListOpts, func(r *Replica) sortKeys {
		return sortKeys{created: r.TimeCreated, updated: r.TimeUpdated, name: r.Name, seq: r.Seq}
	})

	out := make([]Doc, 0, len(pg.Data))
	for _, r := range pg.Data {
		out = append(out, b.replicaDoc(r))
	}

	return page.Page[Doc]{Data: out, Next: pg.Next}, nil
}

func (b *InMemoryBackend) replicaMatches(r *Replica, in ReplicaListIn) bool {
	s := b.systems[r.DBSystemID]

	if r.CompartmentID != in.Compartment || (in.ID != "" && r.ID != in.ID) ||
		(in.DBSystemID != "" && r.DBSystemID != in.DBSystemID) || (in.Name != "" && r.Name != in.Name) ||
		(in.ConfigurationID != "" && r.config(s) != in.ConfigurationID) ||
		(in.UpToDate != nil && *in.UpToDate != r.upToDate(s)) {
		return false
	}

	return stateMatches(r.State, in.State)
}

func (b *InMemoryBackend) mutableReplica(id, ifMatch string, states ...string) (*Replica, *APIError) {
	b.settleID(id)

	r, ok := b.replicas[id]
	if !ok || r.State == stateDeleted {
		return nil, errNotFound(id)
	}

	if aerr := checkEtag(r.Etag, ifMatch); aerr != nil {
		return nil, aerr
	}

	if slices.Contains(states, r.State) {
		return r, nil
	}

	return nil, errState("read replica " + id + " is in state " + r.State + "; operation requires " +
		strings.Join(states, " or "))
}

// UpdateReplica updates a read replica and returns its work request.
func (b *InMemoryBackend) UpdateReplica(id, ifMatch string, in UpdateReplicaIn) (string, error) {
	b.mu.Lock("UpdateReplica")
	defer b.mu.Unlock()

	r, aerr := b.mutableReplica(id, ifMatch, stateActive, stateInactive)
	if aerr != nil {
		return "", aerr
	}

	n := *r

	structural, aerr := b.applyReplicaUpdate(&n, in)
	if aerr != nil {
		return "", aerr
	}

	prev := r.State
	*r = n

	if !structural {
		r.TimeUpdated, r.Etag = b.nowUTC(), newEtag()
		wr := b.newWR(wrUpdateReplica, r.CompartmentID, entityReplica, r.ID, actionUpdated, nil)
		b.finishWR(wr, "")

		return wr, nil
	}

	r.State, r.After, r.TimeUpdated, r.Etag, r.TransitionAt = stateUpdating, prev, b.nowUTC(), newEtag(), nil
	if b.eng == nil {
		r.TransitionAt = b.transitionAt()
	}

	wr := b.newWR(wrUpdateReplica, r.CompartmentID, entityReplica, r.ID, actionUpdated, r.TransitionAt)

	if b.eng != nil {
		b.spawnReplica(r.ID, wr, b.replicaRelaunch(r.ID, prev), func(r *Replica) { r.State = prev })
	}

	return wr, nil
}

func (b *InMemoryBackend) applyReplicaUpdate(n *Replica, in UpdateReplicaIn) (bool, *APIError) {
	s := b.systems[n.DBSystemID]
	oldShape, oldVersion, oldConfig := n.shape(s), n.version(s), n.config(s)

	if in.DisplayName != nil {
		if aerr := validateName(*in.DisplayName); aerr != nil {
			return false, aerr
		}

		n.Name = *in.DisplayName
	}

	if in.Description != nil {
		n.Description = *in.Description
	}

	if in.IsDeleteProtected != nil {
		n.DeleteProtected = *in.IsDeleteProtected
	}

	n.Overrides = mergeOverrides(n.Overrides, in.ReplicaOverrides)
	n.Tags.merge(in.Tags)

	if aerr := b.validateReplica(n, s); aerr != nil {
		return false, aerr
	}

	return n.shape(s) != oldShape || n.version(s) != oldVersion || n.config(s) != oldConfig, nil
}

// DeleteReplica deletes a read replica and returns its work request.
func (b *InMemoryBackend) DeleteReplica(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteReplica")
	defer b.mu.Unlock()

	r, aerr := b.mutableReplica(id, ifMatch, stateActive, stateInactive, stateFailed, stateNeedsAttention)
	if aerr != nil {
		return "", aerr
	}

	if r.DeleteProtected {
		return "", newErr(http.StatusConflict, "Conflict",
			"read replica "+id+" is delete protected; set isDeleteProtected to false first")
	}

	r.State, r.After, r.TimeUpdated, r.Etag, r.TransitionAt = stateDeleting, "", b.nowUTC(), newEtag(), nil

	if b.eng == nil {
		r.TransitionAt = b.transitionAt()

		return b.newWR(wrDeleteReplica, r.CompartmentID, entityReplica, id, actionDeleted, r.TransitionAt), nil
	}

	inst := b.eng.live[id]
	delete(b.eng.live, id)

	wr := b.newWR(wrDeleteReplica, r.CompartmentID, entityReplica, id, actionDeleted, nil)
	b.spawnReplica(id, wr, func(ctx context.Context) error {
		b.closeReplica(ctx, id, inst)

		return nil
	}, func(r *Replica) { r.State = stateDeleted })

	return wr, nil
}

func settleReplica(r *Replica, now time.Time) {
	if !due(r.TransitionAt, now) {
		return
	}

	r.TransitionAt = nil

	switch {
	case r.State == stateDeleting:
		r.State = stateDeleted
	case r.After != "":
		r.State = r.After
	default:
		r.State = stateActive
	}

	r.After = ""
}

// cascadeReplicas moves the read replicas of s along with it and returns their ids. A replica in from heads for to
// (any live replica when to is DELETED); with the engine the transition is settled by the work that follows.
func (b *InMemoryBackend) cascadeReplicas(s *DBSystem, from, to string) []string {
	var ids []string

	for _, r := range b.liveReplicas(s.ID) {
		if to != stateDeleted && r.State != from || r.State == stateDeleting {
			continue
		}

		ids = append(ids, r.ID)
		r.State, r.After, r.TransitionAt, r.TimeUpdated, r.Etag = stateUpdating, to, s.TransitionAt, b.nowUTC(), newEtag()

		if to == stateDeleted {
			r.State, r.After = stateDeleting, ""
		}
	}

	return ids
}

func (h *Handler) createReplica(q *req) error {
	var in CreateReplicaIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.RetryToken = q.r.Header.Get(headerRetry)

	d, etag, wr, err := h.Backend.CreateReplica(in)
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)

	return q.json(http.StatusOK, d, etag)
}

func (h *Handler) getReplica(q *req) error {
	d, etag, err := h.Backend.GetReplica(q.param(paramReplica))

	return reply(q, d, etag, err)
}

func (h *Handler) listReplicas(q *req) error {
	in := ReplicaListIn{
		Compartment: q.query(keyCompartment), ID: q.query(paramReplica), DBSystemID: q.query(paramDBSystem),
		Name: q.query(keyDisplayName), State: q.query(keyLifecycle), ConfigurationID: q.query("configurationId"),
		ListOpts: q.listOpts(),
	}
	if v := q.query("isUpToDate"); v != "" {
		t := strings.EqualFold(v, "true")
		in.UpToDate = &t
	}

	pg, err := h.Backend.ListReplicas(in)

	return collectionReply(q, pg, err)
}

func (h *Handler) updateReplica(q *req) error {
	var in UpdateReplicaIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateReplica(q.param(paramReplica), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteReplica(q *req) error {
	wr, err := h.Backend.DeleteReplica(q.param(paramReplica), q.ifMatch())

	return acceptedWR(q, wr, err)
}

// inheritingReplicasOK rejects a DB system change that leaves a replica without a supported shape.
func (b *InMemoryBackend) inheritingReplicasOK(n *DBSystem) *APIError {
	for _, r := range b.liveReplicas(n.ID) {
		if r.Overrides.ShapeName == "" && !replicaShapeOK(n.Shape) {
			return errParam("shape " + n.Shape + " does not support read replicas; delete the replicas first")
		}
	}

	return nil
}

// markInheritingReplicas puts the ACTIVE replicas that follow the DB system's version into UPDATING.
func (b *InMemoryBackend) markInheritingReplicas(s *DBSystem, version bool) []string {
	if !version {
		return nil
	}

	var ids []string

	for _, r := range b.liveReplicas(s.ID) {
		if r.State == stateActive && r.Overrides.MysqlVersion == "" {
			ids = append(ids, r.ID)
			r.State, r.TimeUpdated, r.Etag = stateUpdating, b.nowUTC(), newEtag()
		}
	}

	return ids
}
