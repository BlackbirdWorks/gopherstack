package ocimysql

import (
	"context"
	"encoding/json/v2"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/tcpfront"
)

const readFrontPrefix = "read:"

// readSettings parses the readEndpoint object of a DB system: enabled unless isEnabled is false, minus the replica
// IP addresses listed in excludeIps (mysql/read_endpoint_details.go).
func readSettings(s *DBSystem) (bool, []string) {
	raw, ok := s.Extra["readEndpoint"]
	if !ok {
		return true, nil
	}

	var v struct {
		IsEnabled  *bool    `json:"isEnabled"`
		ExcludeIps []string `json:"excludeIps"`
	}

	if json.Unmarshal(raw, &v, jsonx.V1()) != nil {
		return true, nil
	}

	return v.IsEnabled == nil || *v.IsEnabled, v.ExcludeIps
}

// readTarget picks the next ACTIVE, non-excluded read replica of a DB system, round robin.
func (b *InMemoryBackend) readTarget(sysID string) (string, int) {
	b.mu.RLock("readTarget")
	defer b.mu.RUnlock()

	s, ok := b.systems[sysID]
	if !ok {
		return "", 0
	}

	_, excluded := readSettings(s)

	var live []Instance

	for _, r := range b.liveReplicas(sysID) {
		if r.State != stateActive || slices.Contains(excluded, r.IP) {
			continue
		}

		if inst := b.eng.live[r.ID]; inst != nil {
			live = append(live, inst)
		}
	}

	if len(live) == 0 {
		return "", 0
	}

	return live[b.eng.rr.Add(1)%uint64(len(live))].Endpoint()
}

// ensureReadFront starts the read endpoint of a DB system when its first replica comes up. It must be called with
// the lock held.
func (b *InMemoryBackend) ensureReadFront(ctx context.Context, sysID string) {
	key := readFrontPrefix + sysID
	if _, ok := b.eng.fronts[key]; ok {
		return
	}

	if s, ok := b.systems[sysID]; ok {
		if on, _ := readSettings(s); !on {
			return
		}
	}

	f, err := tcpfront.New(context.WithoutCancel(ctx), func() (string, int) { return b.readTarget(sysID) })
	if err == nil {
		b.eng.fronts[key] = f
	}
}

// readFrontAddr is the address of a DB system's read endpoint under the engine.
func (b *InMemoryBackend) readFrontAddr(sysID string) (string, int, bool) {
	if b.eng == nil {
		return "", 0, false
	}

	f, ok := b.eng.fronts[readFrontPrefix+sysID]
	if !ok {
		return "", 0, false
	}

	h, p := f.Addr()

	return h, p, true
}
