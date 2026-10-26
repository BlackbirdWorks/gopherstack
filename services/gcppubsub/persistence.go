package gcppubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

var errNullMessage = errors.New("gcppubsub: snapshot has a null message")

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type topicSnap struct {
	Topic    json.RawMessage `json:"topic"`
	Retained []*message      `json:"retained,omitempty"`
}

type subSnap struct {
	LastActive time.Time       `json:"lastActive"`
	Sub        json.RawMessage `json:"sub"`
	Msgs       []*subMsg       `json:"msgs,omitempty"`
}

type pbSnapSnap struct {
	Created  time.Time       `json:"created"`
	Snapshot json.RawMessage `json:"snapshot"`
	Unacked  []string        `json:"unacked,omitempty"`
}

type backendSnapshot struct {
	Topics    map[string]topicSnap       `json:"topics"`
	Subs      map[string]subSnap         `json:"subs"`
	Snapshots map[string]pbSnapSnap      `json:"snapshots"`
	Schemas   map[string]*schemaRec      `json:"schemas"`
	Policies  map[string]json.RawMessage `json:"policies,omitempty"`
	LastMsg   int64                      `json:"lastMsg"`
	EtagSeq   uint64                     `json:"etagSeq,omitempty"`
	Version   int                        `json:"version"`
}

func marshalPB(m proto.Message) json.RawMessage {
	out, err := protojson.Marshal(m)
	if err != nil {
		return nil
	}

	return out
}

func unmarshalPB(raw json.RawMessage, m proto.Message) error {
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, m)
}

// Snapshot serialises the backend state, including unacknowledged messages.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	iamPolicies, iamSeq := b.iam.Snapshot()

	snap := backendSnapshot{
		Version:   snapshotVersion,
		LastMsg:   b.lastMsg,
		EtagSeq:   iamSeq,
		Policies:  make(map[string]json.RawMessage, len(iamPolicies)),
		Schemas:   b.schemas,
		Topics:    make(map[string]topicSnap, len(b.topics)),
		Subs:      make(map[string]subSnap, len(b.subs)),
		Snapshots: make(map[string]pbSnapSnap, len(b.snaps)),
	}

	for n, p := range iamPolicies {
		snap.Policies[n] = marshalPB(p)
	}

	for n, t := range b.topics {
		snap.Topics[n] = topicSnap{Topic: marshalPB(t.pb), Retained: t.retained}
	}

	for n, s := range b.subs {
		snap.Subs[n] = subSnap{Sub: marshalPB(s.pb), Msgs: s.msgs, LastActive: s.lastActive}
	}

	for n, sn := range b.snaps {
		ids := make([]string, 0, len(sn.unacked))
		for id := range sn.unacked {
			ids = append(ids, id)
		}

		snap.Snapshots[n] = pbSnapSnap{Snapshot: marshalPB(sn.pb), Created: sn.created, Unacked: ids}
	}

	return persistence.MarshalSnapshot(ctx, "gcppubsub", snap)
}

// Restore loads backend state from a snapshot; message leases are not restored, so in-flight messages redeliver.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcppubsub", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcppubsub: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		b.resetMaps()

		return nil
	}

	topics, subs, snaps, err := decodeSnapshot(snap)
	if err != nil {
		return err
	}

	policies, err := decodePolicies(snap.Policies)
	if err != nil {
		return err
	}

	for _, s := range b.subs {
		b.notifyLocked(s)
	}

	_, curSeq := b.iam.Snapshot()
	b.iam.Restore(policies, max(curSeq, snap.EtagSeq))
	b.topics, b.subs, b.snaps = topics, subs, snaps
	b.schemas = snap.Schemas

	if b.schemas == nil {
		b.schemas = make(map[string]*schemaRec)
	}

	b.lastMsg = max(b.lastMsg, snap.LastMsg)
	b.signalPush()

	return nil
}

func decodePolicies(raw map[string]json.RawMessage) (map[string]*iampb.Policy, error) {
	out := make(map[string]*iampb.Policy, len(raw))

	for n, r := range raw {
		p := &iampb.Policy{}
		if err := unmarshalPB(r, p); err != nil {
			return nil, fmt.Errorf("gcppubsub: restore policy %q: %w", n, err)
		}

		out[n] = p
	}

	return out, nil
}

func decodeSnapshot(snap backendSnapshot) (map[string]*topic, map[string]*subscription, map[string]*snapshot, error) {
	topics := make(map[string]*topic, len(snap.Topics))
	subs := make(map[string]*subscription, len(snap.Subs))
	snaps := make(map[string]*snapshot, len(snap.Snapshots))

	for n, ts := range snap.Topics {
		pb := &pubsubpb.Topic{}
		if err := unmarshalPB(ts.Topic, pb); err != nil {
			return nil, nil, nil, fmt.Errorf("gcppubsub: restore topic %q: %w", n, err)
		}

		topics[n] = &topic{pb: pb, retained: ts.Retained}
	}

	for n, ss := range snap.Subs {
		s, err := decodeSub(n, ss)
		if err != nil {
			return nil, nil, nil, err
		}

		subs[n] = s
	}

	for n, ps := range snap.Snapshots {
		pb := &pubsubpb.Snapshot{}
		if err := unmarshalPB(ps.Snapshot, pb); err != nil {
			return nil, nil, nil, fmt.Errorf("gcppubsub: restore snapshot %q: %w", n, err)
		}

		unacked := make(map[string]struct{}, len(ps.Unacked))
		for _, id := range ps.Unacked {
			unacked[id] = struct{}{}
		}

		snaps[n] = &snapshot{pb: pb, created: ps.Created, unacked: unacked}
	}

	return topics, subs, snaps, nil
}

func decodeSub(name string, ss subSnap) (*subscription, error) {
	pb := &pubsubpb.Subscription{}
	if err := unmarshalPB(ss.Sub, pb); err != nil {
		return nil, fmt.Errorf("gcppubsub: restore subscription %q: %w", name, err)
	}

	flt, err := compileFilter(pb.GetFilter())
	if err != nil {
		return nil, fmt.Errorf("gcppubsub: restore subscription %q filter: %w", name, err)
	}

	s := &subscription{
		pb: pb, filter: flt, lastActive: ss.LastActive,
		acks: make(map[string]*subMsg), wake: make(chan struct{}),
	}

	for _, m := range ss.Msgs {
		if m == nil || m.Msg == nil {
			return nil, fmt.Errorf("%w: subscription %q", errNullMessage, name)
		}

		if m.Acked {
			s.acked++
		}

		s.msgs = append(s.msgs, m)
	}

	return s, nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcppubsub: restore snapshot: %w", err)
	}

	return nil
}
