package ocicontainerengine

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/ociretry"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	rotWaiting      = "WAITING"
	rotInProgress   = "IN_PROGRESS"
	rotCompleted    = "COMPLETED"
	rotIssuing      = "ISSUING_NEW_CREDENTIALS"
	rotIssued       = "NEW_CREDENTIALS_ISSUED"
	rotRetiring     = "RETIRING_OLD_CREDENTIALS"
	maxRotationDays = 90
	decPending      = "PENDING"
	decInProgress   = "IN_PROGRESS"
	decRollingBack  = "ROLLING_BACK"
	decDone         = "DECOMMISSIONED"
	decFinalized    = "FINALIZED"
)

var (
	namespaceRe = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
	daysRe      = regexp.MustCompile(`^P(\d{1,3})D$`)
)

// Rotation is the credential rotation state of a cluster.
type Rotation struct {
	PhaseAt        *time.Time `json:"phaseAt,omitempty"`
	AutoCompleteAt *time.Time `json:"autoCompleteAt,omitempty"`
	Status         string     `json:"status"`
	Details        string     `json:"details"`
	DelayDays      int        `json:"delayDays"`
}

// Decommission is the public API endpoint decommission state of a cluster.
type Decommission struct {
	PhaseAt  *time.Time `json:"phaseAt,omitempty"`
	Deadline *time.Time `json:"deadline,omitempty"`
	Status   string     `json:"status"`
	Legacy   string     `json:"legacyEndpoint,omitempty"`
}

func parseDays(name string, s *string, lo, hi int) (int, *APIError) {
	if s == nil {
		return 0, errMissing(name + " is required")
	}

	m := daysRe.FindStringSubmatch(*s)
	if m == nil {
		return 0, errParam(name + " must be an ISO 8601 day duration such as P5D")
	}

	n := atoiSmall(m[1])
	if n < lo || n > hi {
		return 0, errParam(name + " must be between P" + strconv.Itoa(lo) + "D and P" + strconv.Itoa(hi) + "D")
	}

	return n, nil
}

func (b *InMemoryBackend) settleExtras(now time.Time) {
	for _, p := range b.vpools {
		settleVNP(p, now)
	}

	for _, m := range b.mappings {
		if due(m.TransitionAt, now) {
			m.TransitionAt = nil

			switch m.State {
			case stateCreating, stateUpdating:
				m.State = stateActive
			case stateDeleting:
				m.State = stateDeleted
			}
		}
	}
}

func (b *InMemoryBackend) settleClusterExtras(c *Cluster, now time.Time) {
	settleAddons(c, now)

	for range 3 {
		if !b.stepRotation(c, now) && !b.stepDecommission(c, now) {
			return
		}
	}
}

func (b *InMemoryBackend) stepRotation(c *Cluster, now time.Time) bool {
	r := c.Rotation
	if r == nil {
		return false
	}

	switch {
	case due(r.PhaseAt, now) && r.Details == rotIssuing:
		at := *r.PhaseAt
		auto := at.Add(time.Duration(r.DelayDays) * hoursPerDay * time.Hour)
		r.PhaseAt, r.Status, r.Details, r.AutoCompleteAt = nil, rotWaiting, rotIssued, &auto
	case due(r.PhaseAt, now) && r.Details == rotRetiring:
		r.PhaseAt, r.Status, r.Details, r.AutoCompleteAt = nil, rotCompleted, rotCompleted, nil
	case r.Status == rotWaiting && due(r.AutoCompleteAt, now):
		at := r.AutoCompleteAt.Add(b.transition)
		r.Status, r.Details, r.PhaseAt, r.AutoCompleteAt = rotInProgress, rotRetiring, &at, nil
	default:
		return false
	}

	return true
}

func (b *InMemoryBackend) stepDecommission(c *Cluster, now time.Time) bool {
	d := c.Decommission
	if d == nil {
		return false
	}

	switch {
	case d.Status == decInProgress && due(d.PhaseAt, now):
		dl := d.PhaseAt.Add(maxDecommission * hoursPerDay * time.Hour)
		d.Status, d.Deadline, d.PhaseAt = decDone, &dl, nil
		c.Endpoints.Kubernetes, c.MigrateState = "", migrateCompleted
	case d.Status == decRollingBack && due(d.PhaseAt, now):
		d.Status, d.Deadline, d.PhaseAt = decPending, nil, nil
		c.Endpoints.Kubernetes, c.MigrateState, c.MigrateScheduled = d.Legacy, migratePending, nil
	case d.Status == decDone && due(d.Deadline, now):
		d.Status = decFinalized
	default:
		return false
	}

	return true
}

func (b *InMemoryBackend) cascadeClusterDelete(id string, at *time.Time) {
	for _, p := range b.vpools {
		if p.ClusterID == id && p.State != stateDeleted {
			b.markVNPDeleting(p, at)
		}
	}

	for _, m := range b.mappings {
		if m.ClusterID == id && m.State != stateDeleted {
			m.State, m.TransitionAt = stateDeleting, at
		}
	}

	if c, ok := b.clusters[id]; ok {
		c.Addons = nil
	}
}

// WorkloadMapping maps a Kubernetes namespace of a cluster to a compartment.
type WorkloadMapping struct {
	TimeCreated  time.Time  `json:"timeCreated"`
	TransitionAt *time.Time `json:"transitionAt,omitempty"`
	Tags         Tags       `json:"tags"`
	ID           string     `json:"id"`
	ClusterID    string     `json:"clusterId"`
	Namespace    string     `json:"namespace"`
	Compartment  string     `json:"mappedCompartmentId"`
	State        string     `json:"state"`
	Etag         string     `json:"etag"`
	Seq          int64      `json:"seq"`
}

// MappingWire is the WorkloadMapping and WorkloadMappingSummary body.
type MappingWire struct {
	TimeCreated         time.Time                 `json:"timeCreated"`
	FreeformTags        map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags         map[string]map[string]any `json:"definedTags,omitempty"`
	ID                  string                    `json:"id"`
	ClusterID           string                    `json:"clusterId"`
	Namespace           string                    `json:"namespace"`
	MappedCompartmentID string                    `json:"mappedCompartmentId"`
	LifecycleState      string                    `json:"lifecycleState"`
}

func mappingView(m *WorkloadMapping) MappingWire {
	return MappingWire{
		ID: m.ID, ClusterID: m.ClusterID, Namespace: m.Namespace, MappedCompartmentID: m.Compartment,
		LifecycleState: m.State, TimeCreated: m.TimeCreated, FreeformTags: m.Tags.FreeformTags,
		DefinedTags: m.Tags.DefinedTags,
	}
}

// CreateMappingIn is the CreateWorkloadMapping request body.
type CreateMappingIn struct {
	Tags
	RetryToken  string `json:"-"`
	Namespace   string `json:"namespace"`
	Compartment string `json:"mappedCompartmentId"`
}

// UpdateMappingIn is the UpdateWorkloadMapping request body.
type UpdateMappingIn struct {
	Tags
	Compartment *string `json:"mappedCompartmentId"`
}

func (b *InMemoryBackend) mappingTaken(clusterID, ns, except string) bool {
	for _, m := range b.mappings {
		if m.ID != except && m.ClusterID == clusterID && m.State != stateDeleted && m.State != stateDeleting &&
			strs.Equal(m.Namespace, ns) {
			return true
		}
	}

	return false
}

// CreateWorkloadMapping maps a namespace of a cluster to a compartment; it starts CREATING.
func (b *InMemoryBackend) CreateWorkloadMapping(clusterID string, in CreateMappingIn) (MappingWire, string, error) {
	b.mu.Lock("CreateWorkloadMapping")
	defer b.mu.Unlock()

	b.settleAll()

	if v, ok := b.retry.Lookup("CreateWorkloadMapping", in.RetryToken, b.nowUTC()); ok {
		m, live := b.mappings[v]
		if !live || m.State == stateDeleting || m.State == stateDeleted {
			return MappingWire{}, "", errInvalidatedToken()
		}

		return mappingView(m), m.Etag, nil
	}

	if !namespaceRe.MatchString(in.Namespace) {
		return MappingWire{}, "", errParam("namespace must be a valid Kubernetes namespace name")
	}

	c, aerr := b.activeCluster(clusterID)
	if aerr != nil {
		return MappingWire{}, "", aerr
	}

	if aerr = firstErr(b.checkCompartment(in.Compartment), b.checkTags(in.Tags)); aerr != nil {
		return MappingWire{}, "", aerr
	}

	if b.mappingTaken(c.ID, in.Namespace, "") {
		return MappingWire{}, "", errExists(in.Namespace)
	}

	now := b.nowUTC()
	m := &WorkloadMapping{
		ID: newOCID("workloadmapping"), ClusterID: c.ID, Namespace: in.Namespace, Compartment: in.Compartment,
		State: stateCreating, Etag: newEtag(), Tags: in.Tags, TimeCreated: now, Seq: b.nextSeq(),
		TransitionAt: b.transitionAt(),
	}
	b.mappings[m.ID] = m
	b.retry.Remember("CreateWorkloadMapping", in.RetryToken, m.ID, now)

	return mappingView(m), m.Etag, nil
}

func (b *InMemoryBackend) mapping(clusterID, id string) (*WorkloadMapping, *APIError) {
	b.settleAll()

	m, ok := b.mappings[id]
	if !ok || m.ClusterID != clusterID {
		return nil, errNotFound(id)
	}

	return m, nil
}

// GetWorkloadMapping returns one workload mapping and its etag.
func (b *InMemoryBackend) GetWorkloadMapping(clusterID, id string) (MappingWire, string, error) {
	b.mu.Lock("GetWorkloadMapping")
	defer b.mu.Unlock()

	m, aerr := b.mapping(clusterID, id)
	if aerr != nil {
		return MappingWire{}, "", aerr
	}

	return mappingView(m), m.Etag, nil
}

// ListWorkloadMappings lists the workload mappings of a cluster.
func (b *InMemoryBackend) ListWorkloadMappings(clusterID string, o ListOpts) (page.Page[MappingWire], error) {
	if aerr := validListOpts(o, "NAMESPACE", "TIMECREATED"); aerr != nil {
		return page.Page[MappingWire]{}, aerr
	}

	b.mu.Lock("ListWorkloadMappings")
	defer b.mu.Unlock()

	b.settleAll()

	if _, aerr := b.liveCluster(clusterID); aerr != nil {
		return page.Page[MappingWire]{}, aerr
	}

	var all []*WorkloadMapping

	for _, m := range b.mappings {
		if m.ClusterID == clusterID && m.State != stateDeleted {
			all = append(all, m)
		}
	}

	if strings.EqualFold(o.SortBy, "NAMESPACE") {
		o.SortBy = "NAME"
	} else {
		o.SortBy = ""
	}

	pg := paginate(all, o, func(m *WorkloadMapping) string { return m.ID },
		func(m *WorkloadMapping) string { return m.Namespace },
		func(m *WorkloadMapping) (time.Time, int64) { return m.TimeCreated, m.Seq })

	out := make([]MappingWire, 0, len(pg.Data))
	for _, m := range pg.Data {
		out = append(out, mappingView(m))
	}

	return page.Page[MappingWire]{Data: out, Next: pg.Next}, nil
}

// UpdateWorkloadMapping changes the mapped compartment or the tags of a mapping.
func (b *InMemoryBackend) UpdateWorkloadMapping(
	clusterID, id, ifMatch string,
	in UpdateMappingIn,
) (MappingWire, string, error) {
	b.mu.Lock("UpdateWorkloadMapping")
	defer b.mu.Unlock()

	m, aerr := b.mapping(clusterID, id)
	if aerr != nil {
		return MappingWire{}, "", aerr
	}

	if aerr = firstErr(checkEtag(m.Etag, ifMatch), b.checkTags(in.Tags)); aerr != nil {
		return MappingWire{}, "", aerr
	}

	if m.State != stateActive {
		return MappingWire{}, "", errState(
			"Workload mapping " + id + " is in state " + m.State + "; the operation requires ACTIVE",
		)
	}

	if in.Compartment != nil {
		if aerr = b.checkCompartment(*in.Compartment); aerr != nil {
			return MappingWire{}, "", aerr
		}

		m.Compartment = *in.Compartment
	}

	m.Tags.merge(in.Tags)
	m.State, m.Etag, m.TransitionAt = stateUpdating, newEtag(), b.transitionAt()

	return mappingView(m), m.Etag, nil
}

// DeleteWorkloadMapping removes a mapping; it goes DELETING then DELETED.
func (b *InMemoryBackend) DeleteWorkloadMapping(clusterID, id, ifMatch string) error {
	b.mu.Lock("DeleteWorkloadMapping")
	defer b.mu.Unlock()

	m, aerr := b.mapping(clusterID, id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(m.Etag, ifMatch); aerr != nil {
		return aerr
	}

	if m.State != stateActive {
		return errState("Workload mapping " + id + " is in state " + m.State + "; the operation requires ACTIVE")
	}

	m.State, m.Etag, m.TransitionAt = stateDeleting, newEtag(), b.transitionAt()

	return nil
}

// StartCredentialRotation begins a credential rotation; old credentials keep working until it completes.
func (b *InMemoryBackend) StartCredentialRotation(id, ifMatch, token string, delay *string) (string, error) {
	b.mu.Lock("StartCredentialRotation")
	defer b.mu.Unlock()

	if wr, hit, aerr := b.replay("StartCredentialRotation", token, b.clusterAlive); hit {
		if aerr != nil {
			return "", aerr
		}

		return wr, nil
	}

	days, aerr := parseDays("autoCompletionDelayDuration", delay, 0, maxRotationDays)
	if aerr != nil {
		return "", aerr
	}

	c, aerr := b.mutableCluster(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if c.State != stateActive {
		return "", errState("Cluster " + id + " is in state " + c.State + "; the operation requires ACTIVE")
	}

	if c.Rotation != nil && c.Rotation.Status != rotCompleted {
		return "", errState("A credential rotation is already in progress for cluster " + id)
	}

	wr := b.beginTransition(c, wrClusterUpdate, stateUpdating, actionUpdated)
	c.Rotation = &Rotation{Status: rotInProgress, Details: rotIssuing, PhaseAt: c.TransitionAt, DelayDays: days}
	b.retry.Remember("StartCredentialRotation", token, ociretry.Join(c.ID, wr), b.nowUTC())

	return wr, nil
}

// CompleteCredentialRotation retires the old credentials once new ones have been issued.
func (b *InMemoryBackend) CompleteCredentialRotation(id, ifMatch, token string) (string, error) {
	b.mu.Lock("CompleteCredentialRotation")
	defer b.mu.Unlock()

	if wr, hit, aerr := b.replay("CompleteCredentialRotation", token, b.clusterAlive); hit {
		if aerr != nil {
			return "", aerr
		}

		return wr, nil
	}

	c, aerr := b.mutableCluster(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if c.State != stateActive {
		return "", errState("Cluster " + id + " is in state " + c.State + "; the operation requires ACTIVE")
	}

	if c.Rotation == nil || c.Rotation.Status != rotWaiting {
		return "", errState("Cluster " + id + " has no credential rotation waiting to be completed")
	}

	wr := b.beginTransition(c, wrClusterUpdate, stateUpdating, actionUpdated)
	c.Rotation.Status, c.Rotation.Details, c.Rotation.PhaseAt, c.Rotation.AutoCompleteAt = rotInProgress,
		rotRetiring, c.TransitionAt, nil
	b.retry.Remember("CompleteCredentialRotation", token, ociretry.Join(c.ID, wr), b.nowUTC())

	return wr, nil
}

// RotationStatusWire is the CredentialRotationStatus body.
type RotationStatusWire struct {
	TimeAutoCompletionScheduled *time.Time `json:"timeAutoCompletionScheduled,omitempty"`
	Status                      string     `json:"status"`
	StatusDetails               string     `json:"statusDetails"`
}

// CredentialRotationStatus reports the rotation state; a cluster that never rotated reports COMPLETED.
func (b *InMemoryBackend) CredentialRotationStatus(id string) (RotationStatusWire, string, error) {
	b.mu.Lock("CredentialRotationStatus")
	defer b.mu.Unlock()

	b.settleAll()

	c, aerr := b.liveCluster(id)
	if aerr != nil {
		return RotationStatusWire{}, "", aerr
	}

	if c.Rotation == nil {
		return RotationStatusWire{Status: rotCompleted, StatusDetails: rotCompleted}, c.Etag, nil
	}

	return RotationStatusWire{
		Status:                      c.Rotation.Status,
		StatusDetails:               c.Rotation.Details,
		TimeAutoCompletionScheduled: c.Rotation.AutoCompleteAt,
	}, c.Etag, nil
}

// DecommissionWire is the PublicApiEndpointDecommissionStatus body.
type DecommissionWire struct {
	Deadline *time.Time `json:"timeDecommissionRollbackDeadline,omitempty"`
	Status   string     `json:"status"`
}

func decommissionView(c *Cluster) DecommissionWire {
	if c.Decommission == nil {
		return DecommissionWire{Status: decPending}
	}

	return DecommissionWire{Status: c.Decommission.Status, Deadline: c.Decommission.Deadline}
}

func (b *InMemoryBackend) decommissionCluster(id, ifMatch string) (*Cluster, *APIError) {
	c, aerr := b.mutableCluster(id, ifMatch)
	if aerr != nil {
		return nil, aerr
	}

	if c.State != stateActive {
		return nil, errState("Cluster " + id + " is in state " + c.State + "; the operation requires ACTIVE")
	}

	return c, nil
}

// StartPublicAPIEndpointDecommission removes the legacy endpoint of a migrated cluster.
func (b *InMemoryBackend) StartPublicAPIEndpointDecommission(id, ifMatch, token string) (string, error) {
	b.mu.Lock("StartPublicAPIEndpointDecommission")
	defer b.mu.Unlock()

	if wr, hit, aerr := b.replay("StartPublicAPIEndpointDecommission", token, b.clusterAlive); hit {
		if aerr != nil {
			return "", aerr
		}

		return wr, nil
	}

	c, aerr := b.decommissionCluster(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if c.MigrateState != migratePending || (c.Decommission != nil && c.Decommission.Status != decPending) {
		return "", errState("Cluster " + id + " has no legacy public API endpoint waiting to be decommissioned")
	}

	wr := b.beginTransition(c, wrDecommission, stateUpdating, actionUpdated)
	c.Decommission = &Decommission{Status: decInProgress, PhaseAt: c.TransitionAt, Legacy: c.Endpoints.Kubernetes}
	b.retry.Remember("StartPublicAPIEndpointDecommission", token, ociretry.Join(c.ID, wr), b.nowUTC())

	return wr, nil
}

func (c *Cluster) decommissionOpen(now time.Time) bool {
	d := c.Decommission

	return d != nil && d.Status == decDone && d.Deadline != nil && !d.Deadline.Before(now)
}

// RollbackPublicAPIEndpointDecommission restores the legacy endpoint within the rollback deadline.
func (b *InMemoryBackend) RollbackPublicAPIEndpointDecommission(id, ifMatch, token string) (string, error) {
	b.mu.Lock("RollbackPublicAPIEndpointDecommission")
	defer b.mu.Unlock()

	if wr, hit, aerr := b.replay("RollbackPublicAPIEndpointDecommission", token, b.clusterAlive); hit {
		if aerr != nil {
			return "", aerr
		}

		return wr, nil
	}

	c, aerr := b.decommissionCluster(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if !c.decommissionOpen(b.nowUTC()) {
		return "", errState("Cluster " + id + " has no decommission that can still be rolled back")
	}

	wr := b.beginTransition(c, wrRollback, stateUpdating, actionUpdated)
	c.Decommission.Status, c.Decommission.PhaseAt = decRollingBack, c.TransitionAt
	b.retry.Remember("RollbackPublicAPIEndpointDecommission", token, ociretry.Join(c.ID, wr), b.nowUTC())

	return wr, nil
}

// ExtendEndpointDecommissionRollbackDeadline pushes the rollback deadline out by up to 30 days.
func (b *InMemoryBackend) ExtendEndpointDecommissionRollbackDeadline(
	id, ifMatch string,
	delay *string,
) (DecommissionWire, string, error) {
	b.mu.Lock("ExtendEndpointDecommissionRollbackDeadline")
	defer b.mu.Unlock()

	days, aerr := parseDays("rollbackDeadlineDelay", delay, 0, maxDecommission)
	if aerr != nil {
		return DecommissionWire{}, "", aerr
	}

	c, aerr := b.decommissionCluster(id, ifMatch)
	if aerr != nil {
		return DecommissionWire{}, "", aerr
	}

	if !c.decommissionOpen(b.nowUTC()) {
		return DecommissionWire{}, "", errState(
			"Cluster " + id + " has no decommission whose rollback deadline can be extended",
		)
	}

	dl := c.Decommission.Deadline.Add(time.Duration(days) * hoursPerDay * time.Hour)
	c.Decommission.Deadline, c.Etag = &dl, newEtag()

	return decommissionView(c), c.Etag, nil
}

// DecommissionStatus reports the public API endpoint decommission state.
func (b *InMemoryBackend) DecommissionStatus(id string) (DecommissionWire, string, error) {
	b.mu.Lock("DecommissionStatus")
	defer b.mu.Unlock()

	b.settleAll()

	c, aerr := b.liveCluster(id)
	if aerr != nil {
		return DecommissionWire{}, "", aerr
	}

	return decommissionView(c), c.Etag, nil
}

// UpdateEndpointConfigIn is the UpdateClusterEndpointConfig request body.
type UpdateEndpointConfigIn struct {
	IsPublicIPEnabled  *bool                     `json:"isPublicIpEnabled"`
	SecurityAttributes map[string]map[string]any `json:"securityAttributes"`
	NsgIDs             []string                  `json:"nsgIds"`
}

// UpdateClusterEndpointConfig changes the NSGs and public address of a VCN-native endpoint.
func (b *InMemoryBackend) UpdateClusterEndpointConfig(id, ifMatch string, in UpdateEndpointConfigIn) (string, error) {
	b.mu.Lock("UpdateClusterEndpointConfig")
	defer b.mu.Unlock()

	c, aerr := b.decommissionCluster(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if c.EndpointConfig == nil || c.EndpointConfig.SubnetID == "" {
		return "", errParam("Cluster " + id + " does not use a VCN-native endpoint")
	}

	cfg := *c.EndpointConfig
	cfg.NsgIDs = in.NsgIDs
	cfg.IsPublicIPEnabled = in.IsPublicIPEnabled
	cfg.SecurityAttributes = in.SecurityAttributes
	c.EndpointConfig = &cfg

	enabled := in.IsPublicIPEnabled != nil && *in.IsPublicIPEnabled
	switch {
	case enabled && c.Endpoints.PublicEndpoint == "":
		c.Endpoints.PublicEndpoint = documentationAddr(b.nextSeq()) + ":" + apiPort
	case !enabled:
		c.Endpoints.PublicEndpoint = ""
	}

	return b.beginTransition(c, wrClusterUpdate, stateUpdating, actionUpdated), nil
}

// NodeActionIn is the RebootClusterNode and ReplaceBootVolumeClusterNode request body.
type NodeActionIn struct {
	Eviction *struct {
		Grace *string `json:"evictionGraceDuration"`
		Force *bool   `json:"isForceActionAfterGraceDuration"`
	} `json:"nodeEvictionSettings"`
}

func (b *InMemoryBackend) nodeAction(op, clusterID, nodeID, token string, in NodeActionIn) (string, error) {
	b.mu.Lock(op)
	defer b.mu.Unlock()

	scope := "node:" + op
	if wr, hit, aerr := b.replay(scope, token, b.clusterAlive); hit {
		if aerr != nil {
			return "", aerr
		}

		return wr, nil
	}

	if in.Eviction != nil {
		if aerr := validEviction("evictionGraceDuration", in.Eviction.Grace); aerr != nil {
			return "", aerr
		}
	}

	b.settleAll()

	c, aerr := b.activeCluster(clusterID)
	if aerr != nil {
		return "", aerr
	}

	node := b.findClusterNode(c.ID, nodeID)
	if node == nil {
		return "", errNotFound(nodeID)
	}

	if node.State != stateActive {
		return "", errState("Node " + nodeID + " is in state " + node.State + "; the operation requires ACTIVE")
	}

	node.State, node.TransitionAt = stateUpdating, b.transitionAt()
	wrType := wrNodeReboot

	if op == opReplaceBootVolume {
		wrType = wrNodeReplace
	}

	wr := b.newWR(wrType, c.CompartmentID, entityCluster, c.ID, actionUpdated, node.TransitionAt)
	b.retry.Remember(scope, token, ociretry.Join(c.ID, wr), b.nowUTC())

	return wr, nil
}

func (b *InMemoryBackend) findClusterNode(clusterID, nodeID string) *Node {
	for _, np := range b.pools {
		if np.ClusterID != clusterID || np.State == stateDeleted {
			continue
		}

		for _, n := range np.Nodes {
			if n.ID == nodeID {
				return n
			}
		}
	}

	return nil
}

func (h *Handler) createMapping(q *req) error {
	var in CreateMappingIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.RetryToken = q.r.Header.Get(headerRetry)

	w, etag, err := h.Backend.CreateWorkloadMapping(q.param("clusterId"), in)

	return reply(q, w, etag, err)
}

func (h *Handler) getMapping(q *req) error {
	w, etag, err := h.Backend.GetWorkloadMapping(q.param("clusterId"), q.param("workloadMappingId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listMappings(q *req) error {
	pg, err := h.Backend.ListWorkloadMappings(q.param("clusterId"), q.listOpts())

	return collectionReply(q, pg, err)
}

func (h *Handler) updateMapping(q *req) error {
	var in UpdateMappingIn
	if err := q.decode(&in); err != nil {
		return err
	}

	w, etag, err := h.Backend.UpdateWorkloadMapping(q.param("clusterId"), q.param("workloadMappingId"), q.ifMatch(), in)

	return reply(q, w, etag, err)
}

func (h *Handler) deleteMapping(q *req) error {
	if err := h.Backend.DeleteWorkloadMapping(
		q.param("clusterId"),
		q.param("workloadMappingId"),
		q.ifMatch(),
	); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusAccepted)

	return nil
}

func (h *Handler) startRotation(q *req) error {
	var in struct {
		Delay *string `json:"autoCompletionDelayDuration"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.StartCredentialRotation(
		q.param("clusterId"),
		q.ifMatch(),
		q.r.Header.Get(headerRetry),
		in.Delay,
	)

	return acceptedWR(q, wr, err)
}

func (h *Handler) completeRotation(q *req) error {
	wr, err := h.Backend.CompleteCredentialRotation(q.param("clusterId"), q.ifMatch(), q.r.Header.Get(headerRetry))

	return acceptedWR(q, wr, err)
}

func (h *Handler) rotationStatus(q *req) error {
	w, etag, err := h.Backend.CredentialRotationStatus(q.param("clusterId"))

	return reply(q, w, etag, err)
}

func (h *Handler) startDecommission(q *req) error {
	wr, err := h.Backend.StartPublicAPIEndpointDecommission(
		q.param("clusterId"),
		q.ifMatch(),
		q.r.Header.Get(headerRetry),
	)

	return acceptedWR(q, wr, err)
}

func (h *Handler) rollbackDecommission(q *req) error {
	wr, err := h.Backend.RollbackPublicAPIEndpointDecommission(q.param("clusterId"), q.ifMatch(),
		q.r.Header.Get(headerRetry))

	return acceptedWR(q, wr, err)
}

func (h *Handler) extendDecommission(q *req) error {
	var in struct {
		Delay *string `json:"rollbackDeadlineDelay"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	w, etag, err := h.Backend.ExtendEndpointDecommissionRollbackDeadline(q.param("clusterId"), q.ifMatch(), in.Delay)

	return reply(q, w, etag, err)
}

func (h *Handler) decommissionStatus(q *req) error {
	w, etag, err := h.Backend.DecommissionStatus(q.param("clusterId"))

	return reply(q, w, etag, err)
}

func (h *Handler) updateEndpointConfig(q *req) error {
	var in UpdateEndpointConfigIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateClusterEndpointConfig(q.param("clusterId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) nodeActionHandler(q *req, op string) error {
	var in NodeActionIn
	if len(q.body) > 0 {
		if err := q.decode(&in); err != nil {
			return err
		}
	}

	wr, err := h.Backend.nodeAction(op, q.param("clusterId"), q.param("nodeId"), q.r.Header.Get(headerRetry), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) rebootNode(q *req) error { return h.nodeActionHandler(q, opRebootNode) }

func (h *Handler) replaceBootVolume(q *req) error { return h.nodeActionHandler(q, opReplaceBootVolume) }
