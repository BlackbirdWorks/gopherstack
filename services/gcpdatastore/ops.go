package gcpdatastore

import (
	"strings"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (b *InMemoryBackend) resolve(project, database string) (partition, error) {
	return requestPartition(project, database)
}

// Lookup returns the entities for the requested keys, split into found and missing.
func (b *InMemoryBackend) Lookup(in *datastorepb.LookupRequest) (*datastorepb.LookupResponse, error) {
	req, err := b.resolve(in.GetProjectId(), in.GetDatabaseId())
	if err != nil {
		return nil, err
	}

	if len(in.GetKeys()) == 0 {
		return nil, errInvalid("Lookup requires at least one key.")
	}

	if len(in.GetKeys()) > maxKeysLookup {
		return nil, errInvalid("Cannot get more than %d keys in a single call.", maxKeysLookup)
	}

	keys := make([]*datastorepb.Key, len(in.GetKeys()))

	for i, k := range in.GetKeys() {
		if keys[i], _, err = normKey(k, req, false, false); err != nil {
			return nil, err
		}
	}

	resp := &datastorepb.LookupResponse{}

	err = b.read(in.GetReadOptions(), req, func(rv *readView) error {
		resp.Transaction = rv.newTxnID
		resp.ReadTime = timestamppb.New(rv.at)

		for _, k := range keys {
			ks := keyString(k)
			if rv.txn != nil && !rv.txn.readOnly {
				rv.txn.reads[ks] = struct{}{}
			}

			ver := b.entityAt(ks, rv)
			if ver == nil {
				resp.Missing = append(resp.Missing, &datastorepb.EntityResult{
					Entity:  &datastorepb.Entity{Key: k},
					Version: rv.at.UnixMicro(),
				})

				continue
			}

			resp.Found = append(resp.Found, entityResult(k, ver, in.GetPropertyMask()))
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func entityResult(k *datastorepb.Key, ver *entVer, mask *datastorepb.PropertyMask) *datastorepb.EntityResult {
	return &datastorepb.EntityResult{
		Entity:     maskEntity(k, ver.ent, mask),
		Version:    ver.version,
		CreateTime: timestamppb.New(ver.created),
		UpdateTime: timestamppb.New(ver.at),
	}
}

func maskEntity(k *datastorepb.Key, e *datastorepb.Entity, mask *datastorepb.PropertyMask) *datastorepb.Entity {
	out, _ := proto.Clone(e).(*datastorepb.Entity)
	out.Key = k

	if mask == nil {
		return out
	}

	keep := &datastorepb.Entity{Key: k, Properties: map[string]*datastorepb.Value{}}

	for _, p := range mask.GetPaths() {
		copyPath(keep, out, strings.Split(p, "."))
	}

	return keep
}

func copyPath(dst, src *datastorepb.Entity, path []string) {
	v, ok := src.GetProperties()[path[0]]
	if !ok {
		return
	}

	if len(path) == 1 {
		dst.Properties[path[0]] = v

		return
	}

	sub, ok := v.GetValueType().(*datastorepb.Value_EntityValue)
	if !ok {
		return
	}

	cur, exists := dst.GetProperties()[path[0]]
	if !exists {
		cur = &datastorepb.Value{
			ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{
				Key: sub.EntityValue.GetKey(), Properties: map[string]*datastorepb.Value{},
			}},
			ExcludeFromIndexes: v.GetExcludeFromIndexes(),
		}
		dst.Properties[path[0]] = cur
	}

	copyPath(cur.GetEntityValue(), sub.EntityValue, path[1:])
}

// BeginTransaction starts a read-write or read-only transaction.
func (b *InMemoryBackend) BeginTransaction(
	in *datastorepb.BeginTransactionRequest,
) (*datastorepb.BeginTransactionResponse, error) {
	req, err := b.resolve(in.GetProjectId(), in.GetDatabaseId())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("BeginTransaction")
	defer b.mu.Unlock()

	b.sweepLocked(b.wallLocked())

	id, _, err := b.beginLocked(in.GetTransactionOptions(), req)
	if err != nil {
		return nil, err
	}

	return &datastorepb.BeginTransactionResponse{Transaction: []byte(id)}, nil
}

// Rollback discards a transaction.
func (b *InMemoryBackend) Rollback(in *datastorepb.RollbackRequest) (*datastorepb.RollbackResponse, error) {
	req, err := b.resolve(in.GetProjectId(), in.GetDatabaseId())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("Rollback")
	defer b.mu.Unlock()

	if _, done := b.ended[string(in.GetTransaction())]; done {
		return &datastorepb.RollbackResponse{}, nil
	}

	if _, err = b.txnLocked(in.GetTransaction(), req); err != nil {
		return nil, err
	}

	b.endTxnLocked(string(in.GetTransaction()))

	return &datastorepb.RollbackResponse{}, nil
}

// AllocateIDs fills in the ids of incomplete keys.
func (b *InMemoryBackend) AllocateIDs(in *datastorepb.AllocateIdsRequest) (*datastorepb.AllocateIdsResponse, error) {
	req, err := b.resolve(in.GetProjectId(), in.GetDatabaseId())
	if err != nil {
		return nil, err
	}

	if len(in.GetKeys()) == 0 {
		return nil, errInvalid("AllocateIds requires at least one key.")
	}

	norm := make([]*datastorepb.Key, len(in.GetKeys()))
	parts := make([]partition, len(in.GetKeys()))

	for i, k := range in.GetKeys() {
		if norm[i], parts[i], err = normKey(k, req, true, true); err != nil {
			return nil, err
		}

		if isComplete(norm[i]) {
			return nil, errInvalid("Only incomplete keys may be passed to AllocateIds: %s.", keyDescription(norm[i]))
		}
	}

	b.mu.Lock("AllocateIds")
	defer b.mu.Unlock()

	for i, k := range norm {
		id, aerr := b.allocIDLocked(parts[i])
		if aerr != nil {
			return nil, aerr
		}

		lastElem(k).IdType = &datastorepb.Key_PathElement_Id{Id: id}
	}

	return &datastorepb.AllocateIdsResponse{Keys: norm}, nil
}

// ReserveIDs prevents the numeric ids of the given keys from being auto-allocated.
func (b *InMemoryBackend) ReserveIDs(in *datastorepb.ReserveIdsRequest) (*datastorepb.ReserveIdsResponse, error) {
	req, err := b.resolve(in.GetProjectId(), in.GetDatabaseId())
	if err != nil {
		return nil, err
	}

	if len(in.GetKeys()) == 0 {
		return nil, errInvalid("ReserveIds requires at least one key.")
	}

	norm := make([]*datastorepb.Key, len(in.GetKeys()))
	parts := make([]partition, len(in.GetKeys()))

	for i, k := range in.GetKeys() {
		if norm[i], parts[i], err = normKey(k, req, false, true); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("ReserveIds")
	defer b.mu.Unlock()

	for i, k := range norm {
		if _, isID := lastElem(k).GetIdType().(*datastorepb.Key_PathElement_Id); isID {
			b.reserveIDLocked(parts[i], lastElem(k).GetId())
		}
	}

	return &datastorepb.ReserveIdsResponse{}, nil
}
