package gcpeventarc

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type triggerSnap struct {
	Trigger    json.RawMessage `json:"trigger"`
	Policy     json.RawMessage `json:"policy,omitempty"`
	OwnedTopic bool            `json:"ownedTopic,omitempty"`
}

type channelSnap struct {
	Channel json.RawMessage `json:"channel"`
	Policy  json.RawMessage `json:"policy,omitempty"`
}

type backendSnapshot struct {
	Triggers    map[string]triggerSnap     `json:"triggers"`
	Channels    map[string]channelSnap     `json:"channels"`
	Connections map[string]json.RawMessage `json:"connections"`
	Configs     map[string]json.RawMessage `json:"configs"`
	Operations  map[string]json.RawMessage `json:"operations"`
	Version     int                        `json:"version"`
	Seq         uint64                     `json:"seq"`
}

func marshalPB(m proto.Message) json.RawMessage {
	if m == nil || !m.ProtoReflect().IsValid() {
		return nil
	}

	raw, err := protojson.Marshal(m)
	if err != nil {
		return nil
	}

	return raw
}

func unmarshalPB(raw []byte, m proto.Message) error {
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, m)
}

func restorePolicy(raw json.RawMessage) (*iampb.Policy, error) {
	if len(raw) == 0 {
		return nil, nil //nolint:nilnil // no policy stored
	}

	p := &iampb.Policy{}
	if err := unmarshalPB(raw, p); err != nil {
		return nil, err
	}

	return p, nil
}

// Snapshot serialises triggers, channels, connections, channel configs and operations.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version: snapshotVersion, Seq: b.seq,
		Triggers:    make(map[string]triggerSnap, len(b.triggers)),
		Channels:    make(map[string]channelSnap, len(b.channels)),
		Connections: make(map[string]json.RawMessage, len(b.conns)),
		Configs:     make(map[string]json.RawMessage, len(b.configs)),
		Operations:  b.ops.Snapshot(),
	}

	for n, t := range b.triggers {
		snap.Triggers[n] = triggerSnap{Trigger: marshalPB(t.pb), Policy: marshalPB(t.policy), OwnedTopic: t.ownedTopic}
	}

	for n, c := range b.channels {
		snap.Channels[n] = channelSnap{Channel: marshalPB(c.pb), Policy: marshalPB(c.policy)}
	}

	for n, c := range b.conns {
		snap.Connections[n] = marshalPB(c)
	}

	for n, c := range b.configs {
		snap.Configs[n] = marshalPB(c)
	}

	return persistence.MarshalSnapshot(ctx, "gcpeventarc", snap)
}

type restored struct {
	triggers map[string]*triggerRec
	channels map[string]*channelRec
	conns    map[string]*eventarcpb.ChannelConnection
	configs  map[string]*eventarcpb.GoogleChannelConfig
}

func restoreAll(snap *backendSnapshot) (*restored, error) {
	out := &restored{
		triggers: make(map[string]*triggerRec, len(snap.Triggers)),
		channels: make(map[string]*channelRec, len(snap.Channels)),
		conns:    make(map[string]*eventarcpb.ChannelConnection, len(snap.Connections)),
		configs:  make(map[string]*eventarcpb.GoogleChannelConfig, len(snap.Configs)),
	}

	for n, s := range snap.Triggers {
		t := &eventarcpb.Trigger{}
		if err := unmarshalPB(s.Trigger, t); err != nil {
			return nil, fmt.Errorf("trigger %q: %w", n, err)
		}

		pol, err := restorePolicy(s.Policy)
		if err != nil {
			return nil, fmt.Errorf("trigger %q policy: %w", n, err)
		}

		out.triggers[n] = &triggerRec{pb: t, policy: pol, ownedTopic: s.OwnedTopic}
	}

	for n, s := range snap.Channels {
		c := &eventarcpb.Channel{}
		if err := unmarshalPB(s.Channel, c); err != nil {
			return nil, fmt.Errorf("channel %q: %w", n, err)
		}

		pol, err := restorePolicy(s.Policy)
		if err != nil {
			return nil, fmt.Errorf("channel %q policy: %w", n, err)
		}

		out.channels[n] = &channelRec{pb: c, policy: pol}
	}

	for n, raw := range snap.Connections {
		c := &eventarcpb.ChannelConnection{}
		if err := unmarshalPB(raw, c); err != nil {
			return nil, fmt.Errorf("channel connection %q: %w", n, err)
		}

		out.conns[n] = c
	}

	for n, raw := range snap.Configs {
		c := &eventarcpb.GoogleChannelConfig{}
		if err := unmarshalPB(raw, c); err != nil {
			return nil, fmt.Errorf("channel config %q: %w", n, err)
		}

		out.configs[n] = c
	}

	return out, nil
}

// Restore loads a snapshot, replacing all state.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpeventarc", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpeventarc: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	r, err := restoreAll(&snap)
	if err != nil {
		return fmt.Errorf("gcpeventarc: restore: %w", err)
	}

	if err = b.ops.Restore(snap.Operations); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	b.triggers, b.channels, b.conns, b.configs, b.seq = r.triggers, r.channels, r.conns, r.configs, snap.Seq
	b.mu.Unlock()

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpeventarc: restore snapshot: %w", err)
	}

	return nil
}
