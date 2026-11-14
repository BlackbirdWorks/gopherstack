package ocinosql

import (
	"regexp"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/ociretry"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	percentDone  = 100
	regionMaxLen = 64
)

var regionRe = regexp.MustCompile(`^[a-z]+(-[a-z0-9]+)+-[0-9]+$`)

type replicaIn struct {
	MaxReadUnits  *int   `json:"maxReadUnits"`
	MaxWriteUnits *int   `json:"maxWriteUnits"`
	Region        string `json:"region"`
	CompartmentID string `json:"compartmentId"`
	RetryToken    string `json:"-"`
}

type replicaWire struct {
	Region         string `json:"region"`
	TableID        string `json:"tableId"`
	CapacityMode   string `json:"capacityMode"`
	LifecycleState string `json:"lifecycleState"`
	MaxWriteUnits  int    `json:"maxWriteUnits"`
}

func replicasView(t *Table) []replicaWire {
	out := make([]replicaWire, 0, len(t.Replicas))
	for _, r := range t.Replicas {
		out = append(out, replicaWire{
			Region: r.Region, TableID: r.TableID, CapacityMode: r.Mode, LifecycleState: r.State,
			MaxWriteUnits: r.WriteUnits,
		})
	}

	return out
}

func settleReplicas(t *Table, now time.Time) {
	kept := t.Replicas[:0]

	for _, r := range t.Replicas {
		if !due(r.TransitionAt, now) {
			kept = append(kept, r)

			continue
		}

		r.TransitionAt = nil

		if r.State == stateDeleting {
			continue
		}

		r.State = stateActive

		kept = append(kept, r)
	}

	t.Replicas = kept
}

func (b *InMemoryBackend) replicaLimits(t *Table, in replicaIn) (string, int, *APIError) {
	if t.Limits.Mode == modeOnDemand {
		if in.MaxReadUnits != nil || in.MaxWriteUnits != nil {
			return "", 0, errParam("maxReadUnits and maxWriteUnits cannot be set for an ON_DEMAND table")
		}

		return modeOnDemand, 0, nil
	}

	read, write := t.Limits.ReadUnits, t.Limits.WriteUnits
	if in.MaxReadUnits != nil {
		read = *in.MaxReadUnits
	}

	if in.MaxWriteUnits != nil {
		write = *in.MaxWriteUnits
	}

	if read < 1 || read > maxReadUnits || write < 1 || write > maxWriteUnits {
		return "", 0, errParam("maxReadUnits must be between 1 and 40000 and maxWriteUnits between 1 and 20000")
	}

	return modeProvisioned, write, nil
}

func (b *InMemoryBackend) checkReplicaCreate(t *Table, in replicaIn) *APIError {
	switch {
	case in.Region == "":
		return errMissing("region is required")
	case len(in.Region) > regionMaxLen || !regionRe.MatchString(in.Region):
		return errParam(in.Region + " isn't known to this service, and is not a valid region name")
	case in.Region == Region:
		return errParam("region must be a remote region, not the region of the table (" + Region + ")")
	case t.AutoReclaim:
		return errParam("An always-free table cannot be converted to a Global Active table.")
	case !t.SchemaFrozen:
		return errParam("The schema of table " + t.Name + " must be frozen before a replica can be created.")
	}

	if slices.ContainsFunc(t.Replicas, func(r *Replica) bool { return r.Region == in.Region }) {
		return errExists(in.Region)
	}

	return nil
}

// CreateReplica records a replica of a table in a remote region and returns its work request id.
func (b *InMemoryBackend) CreateReplica(ref, ifMatch string, in replicaIn) (string, error) {
	b.mu.Lock("CreateReplica")
	defer b.mu.Unlock()

	if v, ok := b.retry.Lookup("CreateReplica", in.RetryToken, b.nowUTC()); ok {
		id, wr := ociretry.Split(v)
		if b.tableGone(id) {
			return "", errInvalidatedToken()
		}

		return wr, nil
	}

	t, aerr := b.resolve(ref, in.CompartmentID)
	if aerr != nil {
		return "", aerr
	}

	if aerr = firstErr(b.modifiable(t), checkEtag(t.Etag, ifMatch), b.checkReplicaCreate(t, in)); aerr != nil {
		return "", aerr
	}

	mode, write, aerr := b.replicaLimits(t, in)
	if aerr != nil {
		return "", aerr
	}

	now := b.nowUTC()
	t.Replicas = append(t.Replicas, &Replica{
		Region: in.Region, TableID: newOCID("nosqltable"), Mode: mode, WriteUnits: write,
		State: stateCreating, TransitionAt: b.transitionAt(),
	})
	t.Etag, t.TimeUpdated = newEtag(), now

	wr := b.newWR(wrUpdate, t.CompartmentID, t.ID, "UPDATED", *t.Replicas[len(t.Replicas)-1].TransitionAt)
	b.retry.Remember("CreateReplica", in.RetryToken, ociretry.Join(t.ID, wr), now)

	return wr, nil
}

// DeleteReplica removes the replica in a region and returns its work request id.
func (b *InMemoryBackend) DeleteReplica(ref, region, compartment, ifMatch string) (string, error) {
	b.mu.Lock("DeleteReplica")
	defer b.mu.Unlock()

	t, aerr := b.resolve(ref, compartment)
	if aerr != nil {
		return "", aerr
	}

	if aerr = firstErr(b.modifiable(t), checkEtag(t.Etag, ifMatch)); aerr != nil {
		return "", aerr
	}

	i := slices.IndexFunc(t.Replicas, func(r *Replica) bool { return strs.Equal(r.Region, region) })
	if i < 0 || t.Replicas[i].State == stateDeleting {
		return "", errNotFound(region)
	}

	r := t.Replicas[i]
	r.State, r.TransitionAt = stateDeleting, b.transitionAt()
	t.Etag, t.TimeUpdated = newEtag(), b.nowUTC()

	return b.newWR(wrUpdate, t.CompartmentID, t.ID, "UPDATED", *r.TransitionAt), nil
}

func checkFrozenAlter(t *Table, spec *alterSpec) *APIError {
	switch {
	case t.SchemaFrozen && len(spec.add) > 0:
		return errParam("Columns can't be added, because the table's schema is frozen.")
	case t.SchemaFrozen && len(spec.drop) > 0:
		return errParam("The columns of a Global Active table cannot be deleted as the schema is frozen.")
	case spec.freeze != nil && !*spec.freeze && len(t.Replicas) > 0:
		return errState("The table's schema can't be unfrozen, because it is a global active table with replicas.")
	}

	return nil
}

func (h *Handler) createReplica(q *req) error {
	var in replicaIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.RetryToken = q.r.Header.Get(headerRetry)

	wr, err := h.Backend.CreateReplica(q.param("tableNameOrId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteReplica(q *req) error {
	wr, err := h.Backend.DeleteReplica(q.param("tableNameOrId"), q.param("region"), q.query("compartmentId"),
		q.ifMatch())

	return acceptedWR(q, wr, err)
}
