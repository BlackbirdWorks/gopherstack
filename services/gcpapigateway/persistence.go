package gcpapigateway

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/apigateway/apiv1/apigatewaypb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type recSnap struct {
	Resource json.RawMessage `json:"resource"`
	Policy   json.RawMessage `json:"policy,omitempty"`
	Life     life            `json:"life"`
}

type backendSnapshot struct {
	Apis     map[string]recSnap `json:"apis"`
	Configs  map[string]recSnap `json:"configs"`
	Gateways map[string]recSnap `json:"gateways"`
	Ops      map[string]*opRec  `json:"operations"`
	Version  int                `json:"version"`
	Seq      uint64             `json:"seq"`
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

// Snapshot serialises APIs, API configs, gateways, policies and operations. Quota counters and cached signing
// keys are not kept.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version: snapshotVersion, Seq: b.seq, Ops: b.ops,
		Apis:     make(map[string]recSnap, len(b.apis)),
		Configs:  make(map[string]recSnap, len(b.configs)),
		Gateways: make(map[string]recSnap, len(b.gateways)),
	}

	for n, r := range b.apis {
		snap.Apis[n] = recSnap{Resource: marshalPB(r.pb), Policy: marshalPB(r.policy), Life: r.life}
	}

	for n, r := range b.configs {
		snap.Configs[n] = recSnap{Resource: marshalPB(r.pb), Policy: marshalPB(r.policy), Life: r.life}
	}

	for n, r := range b.gateways {
		snap.Gateways[n] = recSnap{Resource: marshalPB(r.pb), Policy: marshalPB(r.policy), Life: r.life}
	}

	return persistence.MarshalSnapshot(ctx, "gcpapigateway", snap)
}

type restored struct {
	apis     map[string]*apiRec
	configs  map[string]*configRec
	gateways map[string]*gatewayRec
}

func restoreAll(snap *backendSnapshot) (*restored, error) {
	out := &restored{
		apis:     make(map[string]*apiRec, len(snap.Apis)),
		configs:  make(map[string]*configRec, len(snap.Configs)),
		gateways: make(map[string]*gatewayRec, len(snap.Gateways)),
	}

	for n, s := range snap.Apis {
		pb := &apigatewaypb.Api{}
		if err := unmarshalPB(s.Resource, pb); err != nil {
			return nil, fmt.Errorf("api %q: %w", n, err)
		}

		pol, err := restorePolicy(s.Policy)
		if err != nil {
			return nil, fmt.Errorf("api %q policy: %w", n, err)
		}

		out.apis[n] = &apiRec{pb: pb, policy: pol, life: s.Life}
	}

	for n, s := range snap.Configs {
		pb := &apigatewaypb.ApiConfig{}
		if err := unmarshalPB(s.Resource, pb); err != nil {
			return nil, fmt.Errorf("api config %q: %w", n, err)
		}

		pol, err := restorePolicy(s.Policy)
		if err != nil {
			return nil, fmt.Errorf("api config %q policy: %w", n, err)
		}

		rec := &configRec{pb: pb, policy: pol, life: s.Life}
		if pb.GetState() != apigatewaypb.ApiConfig_FAILED && len(pb.GetOpenapiDocuments()) > 0 {
			rec.spec, _ = compileSpec(pb.GetOpenapiDocuments()[0].GetDocument().GetContents())
		}

		out.configs[n] = rec
	}

	for n, s := range snap.Gateways {
		pb := &apigatewaypb.Gateway{}
		if err := unmarshalPB(s.Resource, pb); err != nil {
			return nil, fmt.Errorf("gateway %q: %w", n, err)
		}

		pol, err := restorePolicy(s.Policy)
		if err != nil {
			return nil, fmt.Errorf("gateway %q policy: %w", n, err)
		}

		out.gateways[n] = &gatewayRec{pb: pb, policy: pol, life: s.Life}
	}

	return out, nil
}

// Restore loads a snapshot, replacing all state.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpapigateway", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpapigateway: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	r, err := restoreAll(&snap)
	if err != nil {
		return fmt.Errorf("gcpapigateway: restore: %w", err)
	}

	if snap.Ops == nil {
		snap.Ops = map[string]*opRec{}
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.apis, b.configs, b.gateways, b.ops, b.seq = r.apis, r.configs, r.gateways, snap.Ops, snap.Seq
	b.quota = map[string]*quotaCounter{}
	b.jwks.reset()

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpapigateway: restore snapshot: %w", err)
	}

	return nil
}
