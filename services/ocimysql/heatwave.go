package ocimysql

import (
	"net/http"
	"slices"
	"time"
)

const (
	wrAddHeatWave     = "ADD_HEATWAVE_CLUSTER"
	wrUpdateHeatWave  = "UPDATE_HEATWAVE_CLUSTER"
	wrDeleteHeatWave  = "DELETE_HEATWAVE_CLUSTER"
	wrStartHeatWave   = "START_HEATWAVE_CLUSTER"
	wrStopHeatWave    = "STOP_HEATWAVE_CLUSTER"
	wrRestartHeatWave = "RESTART_HEATWAVE_CLUSTER"
)

// HeatWaveCluster is the analytics cluster attached to a DB system.
type HeatWaveCluster struct {
	TimeCreated  time.Time  `json:"timeCreated"`
	TimeUpdated  time.Time  `json:"timeUpdated"`
	TransitionAt *time.Time `json:"transitionAt,omitempty"`
	Shape        string     `json:"shapeName"`
	State        string     `json:"state"`
	After        string     `json:"after,omitempty"`
	Etag         string     `json:"etag"`
	NodeIDs      []string   `json:"nodeIds"`
	Size         int        `json:"clusterSize"`
	Lakehouse    bool       `json:"isLakehouseEnabled"`
}

// AddHeatWaveClusterIn is the AddHeatWaveCluster body.
type AddHeatWaveClusterIn struct {
	ClusterSize *int   `json:"clusterSize"`
	Lakehouse   *bool  `json:"isLakehouseEnabled"`
	ShapeName   string `json:"shapeName"`
	RetryToken  string `json:"-"`
	IfMatch     string `json:"-"`
	DBSystemID  string `json:"-"`
}

// UpdateHeatWaveClusterIn is the UpdateHeatWaveCluster body.
type UpdateHeatWaveClusterIn struct {
	ClusterSize *int    `json:"clusterSize"`
	Lakehouse   *bool   `json:"isLakehouseEnabled"`
	ShapeName   *string `json:"shapeName"`
}

func settleHeatWave(s *DBSystem, now time.Time) {
	c := s.HeatWave
	if c == nil || !due(c.TransitionAt, now) {
		return
	}

	c.TransitionAt = nil

	switch {
	case c.State == stateDeleting:
		s.HeatWave = nil
	case c.After != "":
		c.State, c.After = c.After, ""
	default:
		c.State = stateActive
	}
}

func (b *InMemoryBackend) heatWaveDoc(s *DBSystem) Doc {
	c := s.HeatWave
	nodeState := c.State
	if c.State == stateDeleting {
		nodeState = stateDeleting
	}

	nodes := make([]Doc, 0, len(c.NodeIDs))
	for _, id := range c.NodeIDs {
		nodes = append(nodes, Doc{
			"nodeId": id, keyLifecycle: nodeState, keyTimeCreated: c.TimeCreated, keyTimeUpdated: c.TimeUpdated,
		})
	}

	return Doc{
		"dbSystemId": s.ID, keyShapeName: c.Shape, "clusterSize": c.Size, "clusterNodes": nodes,
		keyLifecycle: c.State, keyTimeCreated: c.TimeCreated, keyTimeUpdated: c.TimeUpdated,
		"isLakehouseEnabled": c.Lakehouse,
	}
}

func (b *InMemoryBackend) heatWaveSummary(s *DBSystem) Doc {
	c := s.HeatWave

	return Doc{
		keyShapeName: c.Shape, "clusterSize": c.Size, keyLifecycle: c.State, keyTimeCreated: c.TimeCreated,
		keyTimeUpdated: c.TimeUpdated, "isLakehouseEnabled": c.Lakehouse,
	}
}

func heatWaveShape(name string) *APIError {
	if name == "" {
		return errMissing("shapeName is required")
	}

	sh, ok := shapeByName(name)
	if !ok || sh.support != supportHeatWave {
		return errParam("shapeName " + name + " is not a HeatWave cluster shape")
	}

	return nil
}

func validClusterSize(n int) *APIError {
	if n < 1 {
		return errParam("clusterSize must be at least 1")
	}

	return nil
}

func (b *InMemoryBackend) heatWaveOf(id, ifMatch string, states ...string) (*DBSystem, *HeatWaveCluster, *APIError) {
	b.settleID(id)

	s, ok := b.systems[id]
	if !ok || s.State == stateDeleted {
		return nil, nil, errNotFound(id)
	}

	c := s.HeatWave
	if c == nil {
		return nil, nil, errNotFound("HeatWave cluster of " + id)
	}

	if aerr := checkEtag(c.Etag, ifMatch); aerr != nil {
		return nil, nil, aerr
	}

	if slices.Contains(states, c.State) {
		return s, c, nil
	}

	return nil, nil, errState("HeatWave cluster of " + id + " is in state " + c.State)
}

func (b *InMemoryBackend) beginHeatWave(s *DBSystem, c *HeatWaveCluster, op, state, after string) string {
	now := b.nowUTC()
	c.State, c.After, c.TimeUpdated, c.Etag, c.TransitionAt = state, after, now, newEtag(), b.transitionAt()
	s.TimeUpdated = now

	return b.newWR(op, s.CompartmentID, entityDBSystem, s.ID, actionUpdated, c.TransitionAt)
}

// AddHeatWaveCluster attaches a HeatWave cluster to an ACTIVE DB system.
func (b *InMemoryBackend) AddHeatWaveCluster(in AddHeatWaveClusterIn) (Doc, string, string, error) {
	if aerr := heatWaveShape(in.ShapeName); aerr != nil {
		return nil, "", "", aerr
	}

	if in.ClusterSize == nil {
		return nil, "", "", errMissing("clusterSize is required")
	}

	if aerr := validClusterSize(*in.ClusterSize); aerr != nil {
		return nil, "", "", aerr
	}

	b.mu.Lock("AddHeatWaveCluster")
	defer b.mu.Unlock()

	scope := "AddHeatWaveCluster/" + in.DBSystemID

	if v, ok := b.retry.Lookup(scope, in.RetryToken, b.nowUTC()); ok {
		if s, live := b.systems[in.DBSystemID]; live && s.HeatWave != nil {
			return b.heatWaveDoc(s), s.HeatWave.Etag, v, nil
		}

		return nil, "", "", errInvalidatedToken()
	}

	s, aerr := b.mutable(in.DBSystemID, in.IfMatch, stateActive)
	if aerr != nil {
		return nil, "", "", aerr
	}

	if s.HeatWave != nil {
		return nil, "", "", newErr(http.StatusConflict, "Conflict", "DB system "+s.ID+" already has a HeatWave cluster")
	}

	now := b.nowUTC()
	c := &HeatWaveCluster{
		Shape: in.ShapeName, Size: *in.ClusterSize, State: stateCreating, TimeCreated: now, TimeUpdated: now,
		Etag: newEtag(), TransitionAt: b.transitionAt(),
	}
	if in.Lakehouse != nil {
		c.Lakehouse = *in.Lakehouse
	}

	for range c.Size {
		c.NodeIDs = append(c.NodeIDs, newOCID("mysqlheatwavenode"))
	}

	s.HeatWave, s.TimeUpdated = c, now

	wr := b.newWR(wrAddHeatWave, s.CompartmentID, entityDBSystem, s.ID, actionUpdated, c.TransitionAt)
	b.retry.Remember(scope, in.RetryToken, wr, now)

	return b.heatWaveDoc(s), c.Etag, wr, nil
}

// GetHeatWaveCluster returns the HeatWave cluster of a DB system and its etag.
func (b *InMemoryBackend) GetHeatWaveCluster(id string) (Doc, string, error) {
	b.mu.Lock("GetHeatWaveCluster")
	defer b.mu.Unlock()

	s, c, aerr := b.heatWaveOf(id, "", stateCreating, stateActive, stateInactive, stateUpdating, stateDeleting,
		stateFailed)
	if aerr != nil {
		return nil, "", aerr
	}

	return b.heatWaveDoc(s), c.Etag, nil
}

// UpdateHeatWaveCluster changes the shape, size or Lakehouse flag; shape and size rebuild the cluster.
func (b *InMemoryBackend) UpdateHeatWaveCluster(id, ifMatch string, in UpdateHeatWaveClusterIn) (string, error) {
	if in.ShapeName == nil && in.ClusterSize == nil && in.Lakehouse == nil {
		return "", errMissing("at least one of shapeName, clusterSize or isLakehouseEnabled is required")
	}

	if in.ShapeName != nil {
		if aerr := heatWaveShape(*in.ShapeName); aerr != nil {
			return "", aerr
		}
	}

	if in.ClusterSize != nil {
		if aerr := validClusterSize(*in.ClusterSize); aerr != nil {
			return "", aerr
		}
	}

	b.mu.Lock("UpdateHeatWaveCluster")
	defer b.mu.Unlock()

	s, c, aerr := b.heatWaveOf(id, ifMatch, stateActive, stateInactive)
	if aerr != nil {
		return "", aerr
	}

	if in.ShapeName != nil {
		c.Shape = *in.ShapeName
	}

	if in.Lakehouse != nil {
		c.Lakehouse = *in.Lakehouse
	}

	if in.ClusterSize != nil && *in.ClusterSize != c.Size {
		c.Size, c.NodeIDs = *in.ClusterSize, nil
		for range c.Size {
			c.NodeIDs = append(c.NodeIDs, newOCID("mysqlheatwavenode"))
		}
	}

	return b.beginHeatWave(s, c, wrUpdateHeatWave, stateUpdating, c.State), nil
}

// DeleteHeatWaveCluster detaches the cluster; it disappears when the work request finishes.
func (b *InMemoryBackend) DeleteHeatWaveCluster(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteHeatWaveCluster")
	defer b.mu.Unlock()

	s, c, aerr := b.heatWaveOf(id, ifMatch, stateActive, stateInactive, stateFailed)
	if aerr != nil {
		return "", aerr
	}

	return b.beginHeatWave(s, c, wrDeleteHeatWave, stateDeleting, ""), nil
}

func (b *InMemoryBackend) heatWaveAction(
	name, id, ifMatch, token, op, after string, from ...string,
) (string, error) {
	b.mu.Lock(name)
	defer b.mu.Unlock()

	scope := name + "/" + id
	if v, ok := b.retry.Lookup(scope, token, b.nowUTC()); ok {
		return v, nil
	}

	s, c, aerr := b.heatWaveOf(id, ifMatch, from...)
	if aerr != nil {
		return "", aerr
	}

	wr := b.beginHeatWave(s, c, op, stateUpdating, after)
	b.retry.Remember(scope, token, wr, b.nowUTC())

	return wr, nil
}

func (h *Handler) addHeatWaveCluster(q *req) error {
	var in AddHeatWaveClusterIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.DBSystemID, in.IfMatch, in.RetryToken = q.param(paramDBSystem), q.ifMatch(), q.r.Header.Get(headerRetry)

	d, etag, wr, err := h.Backend.AddHeatWaveCluster(in)
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)

	return q.json(http.StatusOK, d, etag)
}

func (h *Handler) getHeatWaveCluster(q *req) error {
	d, etag, err := h.Backend.GetHeatWaveCluster(q.param(paramDBSystem))

	return reply(q, d, etag, err)
}

func (h *Handler) updateHeatWaveCluster(q *req) error {
	var in UpdateHeatWaveClusterIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateHeatWaveCluster(q.param(paramDBSystem), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteHeatWaveCluster(q *req) error {
	wr, err := h.Backend.DeleteHeatWaveCluster(q.param(paramDBSystem), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) startHeatWaveCluster(q *req) error {
	wr, err := h.Backend.heatWaveAction("StartHeatWaveCluster", q.param(paramDBSystem), q.ifMatch(),
		q.r.Header.Get(headerRetry), wrStartHeatWave, stateActive, stateInactive)

	return acceptedWR(q, wr, err)
}

func (h *Handler) stopHeatWaveCluster(q *req) error {
	wr, err := h.Backend.heatWaveAction("StopHeatWaveCluster", q.param(paramDBSystem), q.ifMatch(),
		q.r.Header.Get(headerRetry), wrStopHeatWave, stateInactive, stateActive)

	return acceptedWR(q, wr, err)
}

func (h *Handler) restartHeatWaveCluster(q *req) error {
	wr, err := h.Backend.heatWaveAction("RestartHeatWaveCluster", q.param(paramDBSystem), q.ifMatch(),
		q.r.Header.Get(headerRetry), wrRestartHeatWave, stateActive, stateActive)

	return acceptedWR(q, wr, err)
}
