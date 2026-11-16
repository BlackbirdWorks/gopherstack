package gcpfirestore

import (
	"encoding/json/jsontext"
	"fmt"

	"cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// adminSnapshot is the serialised Admin API state; protobuf messages are kept as protojson.
type adminSnapshot struct {
	Databases map[string]jsontext.Value   `json:"databases,omitempty"`
	Indexes   map[string]jsontext.Value   `json:"indexes,omitempty"`
	Fields    map[string]jsontext.Value   `json:"fields,omitempty"`
	Ops       map[string]jsontext.Value   `json:"operations,omitempty"`
	Exports   map[string][]jsontext.Value `json:"exports,omitempty"`
	OpOrder   []string                    `json:"operationOrder,omitempty"`
	ExpOrder  []string                    `json:"exportOrder,omitempty"`
}

func marshalMap[T proto.Message](in map[string]T) map[string]jsontext.Value {
	out := make(map[string]jsontext.Value, len(in))

	for k, v := range in {
		if raw, err := protojson.Marshal(v); err == nil {
			out[k] = raw
		}
	}

	return out
}

func unmarshalMap[T proto.Message](in map[string]jsontext.Value, mk func() T) (map[string]T, error) {
	out := make(map[string]T, len(in))

	for k, raw := range in {
		m := mk()
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, m); err != nil {
			return nil, fmt.Errorf("gcpfirestore: restore admin state %q: %w", k, err)
		}

		out[k] = m
	}

	return out, nil
}

func (a *adminState) snapshot() *adminSnapshot {
	snap := &adminSnapshot{
		Databases: marshalMap(a.dbs), Indexes: marshalMap(a.indexes), Fields: marshalMap(a.fields),
		Ops: marshalMap(a.ops), OpOrder: a.opOrder, ExpOrder: a.expOrder,
		Exports: make(map[string][]jsontext.Value, len(a.exports)),
	}

	for uri, e := range a.exports {
		for _, d := range e.docs {
			if raw, err := protojson.Marshal(d); err == nil {
				snap.Exports[uri] = append(snap.Exports[uri], raw)
			}
		}
	}

	return snap
}

func restoreAdmin(snap *adminSnapshot) (*adminState, error) {
	a := newAdminState()
	if snap == nil {
		return a, nil
	}

	var err error

	if a.dbs, err = unmarshalMap(snap.Databases, func() *adminpb.Database { return &adminpb.Database{} }); err != nil {
		return nil, err
	}

	if a.indexes, err = unmarshalMap(snap.Indexes, func() *adminpb.Index { return &adminpb.Index{} }); err != nil {
		return nil, err
	}

	if a.fields, err = unmarshalMap(snap.Fields, func() *adminpb.Field { return &adminpb.Field{} }); err != nil {
		return nil, err
	}

	if a.ops, err = unmarshalMap(
		snap.Ops,
		func() *longrunningpb.Operation { return &longrunningpb.Operation{} },
	); err != nil {
		return nil, err
	}

	a.opOrder, a.expOrder = snap.OpOrder, snap.ExpOrder

	for uri, raws := range snap.Exports {
		e := &exportDocs{}

		for _, raw := range raws {
			d := &firestorepb.Document{}
			if uerr := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, d); uerr != nil {
				return nil, fmt.Errorf("gcpfirestore: restore export %q: %w", uri, uerr)
			}

			e.docs = append(e.docs, d)
		}

		a.exports[uri] = e
	}

	return a, nil
}
