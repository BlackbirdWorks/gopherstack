package gcpdatastore

import (
	"time"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxTransforms = 500

// wstate is the working copy of one entity while a commit's mutations are applied in order.
type wstate struct {
	key     *datastorepb.Key
	ent     *datastorepb.Entity
	old     *datastorepb.Entity
	created time.Time
	updated time.Time
	version int64
	dirty   bool
}

type commitCtx struct {
	b       *InMemoryBackend
	req     partition
	at      time.Time
	work    map[string]*wstate
	order   []string
	version int64
	strict  bool
}

func (c *commitCtx) load(ks string, key *datastorepb.Key) *wstate {
	if w, ok := c.work[ks]; ok {
		return w
	}

	w := &wstate{key: key}

	if rec := c.b.ents[ks]; rec != nil {
		if ver := rec.cur(); ver != nil {
			w.ent, w.old = ver.ent, ver.ent
			w.created, w.updated, w.version = ver.created, ver.at, ver.version
		}
	}

	c.work[ks] = w
	c.order = append(c.order, ks)

	return w
}

// Commit applies mutations atomically, optionally inside a transaction.
func (b *InMemoryBackend) Commit(in *datastorepb.CommitRequest) (*datastorepb.CommitResponse, error) {
	req, err := b.resolve(in.GetProjectId(), in.GetDatabaseId())
	if err != nil {
		return nil, err
	}

	mode := in.GetMode()
	if mode == datastorepb.CommitRequest_MODE_UNSPECIFIED {
		mode = datastorepb.CommitRequest_TRANSACTIONAL
	}

	b.mu.Lock("Commit")
	defer b.mu.Unlock()

	b.sweepLocked(b.wallLocked())

	txn, err := b.commitTxnLocked(in, mode, req)
	if err != nil {
		return nil, err
	}

	if txn != nil && len(in.GetMutations()) > 0 {
		if err = b.checkConflictLocked(txn, in.GetMutations(), req); err != nil {
			b.endTxnLocked(txnKey(in))

			return nil, err
		}
	}

	c := &commitCtx{
		b: b, req: req, work: map[string]*wstate{},
		strict: mode == datastorepb.CommitRequest_NON_TRANSACTIONAL,
	}
	c.at = b.wallLocked()

	if !c.at.After(b.lastCommit) {
		c.at = b.lastCommit.Add(time.Microsecond)
	}

	c.version = c.at.UnixMicro()

	results := make([]*datastorepb.MutationResult, 0, len(in.GetMutations()))

	for _, m := range in.GetMutations() {
		r, merr := c.apply(m)
		if merr != nil {
			if txn != nil {
				b.endTxnLocked(txnKey(in))
			}

			return nil, merr
		}

		results = append(results, r)
	}

	if txn != nil {
		b.endTxnLocked(txnKey(in))
	}

	resp := &datastorepb.CommitResponse{MutationResults: results}

	if len(c.order) > 0 && c.anyDirty() {
		b.lastCommit = c.at
		resp.IndexUpdates = clampInt32(c.persist())
	}

	resp.CommitTime = timestamppb.New(c.at)

	return resp, nil
}

func txnKey(in *datastorepb.CommitRequest) string {
	if t := in.GetTransaction(); t != nil {
		return string(t)
	}

	return ""
}

func (b *InMemoryBackend) commitTxnLocked(
	in *datastorepb.CommitRequest, mode datastorepb.CommitRequest_Mode, req partition,
) (*txnRec, error) {
	if mode == datastorepb.CommitRequest_NON_TRANSACTIONAL {
		if in.GetTransactionSelector() != nil {
			return nil, errInvalid("A transaction must not be specified for a non-transactional commit.")
		}

		return nil, nil //nolint:nilnil // no transaction for non-transactional commits
	}

	switch sel := in.GetTransactionSelector().(type) {
	case *datastorepb.CommitRequest_Transaction:
		t, err := b.txnLocked(sel.Transaction, req)
		if err != nil {
			return nil, err
		}

		if t.readOnly && len(in.GetMutations()) > 0 {
			b.endTxnLocked(string(sel.Transaction))

			return nil, errInvalid("Cannot modify entities in a read-only transaction.")
		}

		return t, nil
	case *datastorepb.CommitRequest_SingleUseTransaction:
		if sel.SingleUseTransaction.GetReadOnly() != nil {
			return nil, errInvalid("A single-use transaction must be read-write.")
		}

		return nil, nil //nolint:nilnil // single-use transactions have no prior reads to validate
	default:
		return nil, errInvalid("A transaction must be specified for a transactional commit.")
	}
}

// checkConflictLocked aborts when anything the transaction read or writes changed after its snapshot.
func (b *InMemoryBackend) checkConflictLocked(
	t *txnRec, muts []*datastorepb.Mutation, req partition,
) error {
	writes := map[string]struct{}{}

	for _, m := range muts {
		var k *datastorepb.Key

		switch op := m.GetOperation().(type) {
		case *datastorepb.Mutation_Insert:
			k = op.Insert.GetKey()
		case *datastorepb.Mutation_Update:
			k = op.Update.GetKey()
		case *datastorepb.Mutation_Upsert:
			k = op.Upsert.GetKey()
		case *datastorepb.Mutation_Delete:
			k = op.Delete
		}

		if nk, _, err := normKey(k, req, true, false); err == nil && isComplete(nk) {
			writes[keyString(nk)] = struct{}{}
		}
	}

	for _, ch := range b.changes {
		if !ch.at.After(t.readTime) {
			continue
		}

		if _, ok := t.reads[ch.ks]; ok {
			return errContention()
		}

		if _, ok := writes[ch.ks]; ok {
			return errContention()
		}

		for _, q := range t.queries {
			if q.covers(ch.key) {
				return errContention()
			}
		}
	}

	return nil
}

func (c *commitCtx) anyDirty() bool {
	for _, w := range c.work {
		if w.dirty {
			return true
		}
	}

	return false
}

// persist writes the dirty working states as new versions and returns the index entry count.
func (c *commitCtx) persist() int {
	b := c.b
	idx := 0

	for _, ks := range c.order {
		w := c.work[ks]
		if !w.dirty {
			continue
		}

		rec := b.ents[ks]
		if rec == nil {
			rec = &entRec{key: w.key}
			b.ents[ks] = rec
		}

		if w.ent == nil {
			rec.vers = append(rec.vers, entVer{at: c.at, version: c.version})
		} else {
			rec.vers = append(rec.vers, entVer{at: c.at, created: w.created, ent: w.ent, version: c.version})
		}

		if e, ok := lastElem(w.key).GetIdType().(*datastorepb.Key_PathElement_Id); ok {
			b.reserveIDLocked(keyPartition(w.key), e.Id)
		}

		b.changes = append(b.changes, change{at: c.at, key: w.key, ks: ks})
		idx += countIndexed(w.ent) + countIndexed(w.old)
	}

	return idx
}

func (c *commitCtx) apply(m *datastorepb.Mutation) (*datastorepb.MutationResult, error) {
	if m.GetConflictResolutionStrategy() != datastorepb.Mutation_STRATEGY_UNSPECIFIED &&
		m.GetConflictDetectionStrategy() == nil {
		return nil, errInvalid("conflict_resolution_strategy requires a conflict detection strategy.")
	}

	var (
		ent *datastorepb.Entity
		del *datastorepb.Key
	)

	switch op := m.GetOperation().(type) {
	case *datastorepb.Mutation_Insert:
		ent = op.Insert
	case *datastorepb.Mutation_Update:
		ent = op.Update
	case *datastorepb.Mutation_Upsert:
		ent = op.Upsert
	case *datastorepb.Mutation_Delete:
		del = op.Delete
	default:
		return nil, errInvalid("Mutation has no operation set.")
	}

	if del != nil {
		if len(m.GetPropertyTransforms()) > 0 {
			return nil, errInvalid("Property transforms are not allowed on delete.")
		}

		return c.applyDelete(m, del)
	}

	return c.applyWrite(m, ent)
}

func (c *commitCtx) conflict(m *datastorepb.Mutation, w *wstate) (bool, error) {
	var hit bool

	switch d := m.GetConflictDetectionStrategy().(type) {
	case *datastorepb.Mutation_BaseVersion:
		hit = w.version != d.BaseVersion
	case *datastorepb.Mutation_UpdateTime:
		hit = w.ent == nil || !w.updated.Equal(d.UpdateTime.AsTime())
	default:
		return false, nil
	}

	if hit && m.GetConflictResolutionStrategy() == datastorepb.Mutation_FAIL {
		return true, errContention()
	}

	return hit, nil
}

func (c *commitCtx) skipped(w *wstate) *datastorepb.MutationResult {
	r := &datastorepb.MutationResult{ConflictDetected: true, Version: w.version}
	if w.ent != nil {
		r.CreateTime, r.UpdateTime = timestamppb.New(w.created), timestamppb.New(w.updated)
	}

	return r
}

func (c *commitCtx) applyDelete(m *datastorepb.Mutation, k *datastorepb.Key) (*datastorepb.MutationResult, error) {
	key, _, err := normKey(k, c.req, false, true)
	if err != nil {
		return nil, err
	}

	ks := keyString(key)
	if err = c.touch(ks); err != nil {
		return nil, err
	}

	w := c.load(ks, key)

	if hit, cerr := c.conflict(m, w); cerr != nil {
		return nil, cerr
	} else if hit {
		return c.skipped(w), nil
	}

	if w.ent != nil {
		w.ent, w.dirty = nil, true
	}

	w.version = c.version

	return &datastorepb.MutationResult{Version: c.version}, nil
}

func (c *commitCtx) touch(ks string) error {
	if _, seen := c.work[ks]; seen && c.strict {
		return errInvalid("A non-transactional commit may not contain two mutations of the same entity.")
	}

	return nil
}

// writeTarget validates a write mutation's entity and resolves its key, allocating an id for incomplete keys.
func (c *commitCtx) writeTarget(
	m *datastorepb.Mutation, e *datastorepb.Entity, update bool,
) (*datastorepb.Key, *datastorepb.Entity, *datastorepb.MutationResult, error) {
	key, part, err := normKey(e.GetKey(), c.req, !update, true)
	if err != nil {
		return nil, nil, nil, err
	}

	if len(m.GetPropertyTransforms()) > maxTransforms {
		return nil, nil, nil, errInvalid("A mutation may have at most %d property transforms.", maxTransforms)
	}

	prepared, err := prepareEntity(e, c.req)
	if err != nil {
		return nil, nil, nil, err
	}

	res := &datastorepb.MutationResult{}

	if !isComplete(key) {
		id, aerr := c.b.allocIDLocked(part)
		if aerr != nil {
			return nil, nil, nil, aerr
		}

		lastElem(key).IdType = &datastorepb.Key_PathElement_Id{Id: id}
		res.Key = key
	}

	return key, prepared, res, nil
}

func (c *commitCtx) applyWrite(m *datastorepb.Mutation, e *datastorepb.Entity) (*datastorepb.MutationResult, error) {
	_, isInsert := m.GetOperation().(*datastorepb.Mutation_Insert)
	_, isUpdate := m.GetOperation().(*datastorepb.Mutation_Update)

	key, prepared, res, err := c.writeTarget(m, e, isUpdate)
	if err != nil {
		return nil, err
	}

	ks := keyString(key)
	if err = c.touch(ks); err != nil {
		return nil, err
	}

	w := c.load(ks, key)

	if hit, cerr := c.conflict(m, w); cerr != nil {
		return nil, cerr
	} else if hit {
		return c.skipped(w), nil
	}

	if isInsert && w.ent != nil {
		return nil, errExists("entity already exists: %s", keyDescription(key))
	}

	if isUpdate && w.ent == nil {
		return nil, errNotFound("no entity to update: %s", keyDescription(key))
	}

	next := prepared
	if m.GetPropertyMask() != nil && w.ent != nil {
		next = applyMask(w.ent, prepared, m.GetPropertyMask().GetPaths())
	}

	next.Key = key

	if res.TransformResults, err = c.transform(next, m.GetPropertyTransforms()); err != nil {
		return nil, err
	}

	if w.ent == nil {
		w.created = c.at
	}

	w.ent, w.dirty, w.updated, w.version = next, true, c.at, c.version
	res.Version = c.version
	res.CreateTime, res.UpdateTime = timestamppb.New(w.created), timestamppb.New(c.at)

	return res, nil
}

func applyMask(existing, incoming *datastorepb.Entity, paths []string) *datastorepb.Entity {
	out, _ := proto.Clone(existing).(*datastorepb.Entity)
	if out.GetProperties() == nil {
		out.Properties = map[string]*datastorepb.Value{}
	}

	for _, p := range paths {
		setPath(out, incoming, splitPath(p))
	}

	return out
}

func setPath(dst, src *datastorepb.Entity, path []string) {
	head := path[0]

	if len(path) == 1 {
		if v, ok := src.GetProperties()[head]; ok {
			dst.Properties[head] = v
		} else {
			delete(dst.GetProperties(), head)
		}

		return
	}

	sv, ok := src.GetProperties()[head].GetValueType().(*datastorepb.Value_EntityValue)
	if !ok {
		delete(dst.GetProperties(), head)

		return
	}

	dv := dst.GetProperties()[head]
	if _, isEnt := dv.GetValueType().(*datastorepb.Value_EntityValue); !isEnt {
		dv = &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{
			EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{}},
		}}
		dst.Properties[head] = dv
	}

	sub := dv.GetEntityValue()
	if sub.GetProperties() == nil {
		sub.Properties = map[string]*datastorepb.Value{}
	}

	setPath(sub, sv.EntityValue, path[1:])
}
