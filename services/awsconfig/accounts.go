package awsconfig

import (
	"encoding/json"
	"maps"
	"regexp"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	accountsSnapshotKey = "accounts"
	maxAccountPeers     = 64
)

var accountIDPattern = regexp.MustCompile(`^[0-9]{12}$`)

// accountSet holds the per-account sibling handlers of one home handler; each
// sibling owns its own account-scoped backend and region peers.
type accountSet struct {
	peers map[string]*Handler
	build func(account string) *Handler
	mu    *lockmetrics.RWMutex
	home  string
}

func newAccountSet(home string, build func(account string) *Handler) *accountSet {
	return &accountSet{
		home:  home,
		build: build,
		peers: make(map[string]*Handler),
		mu:    lockmetrics.New("awsconfig-accounts"),
	}
}

// get returns account's sibling, building it on first use; nil means the home
// handler serves (home account, empty/malformed account, or the cap is reached).
func (s *accountSet) get(account string) *Handler {
	if s == nil || account == "" || account == s.home || !accountIDPattern.MatchString(account) {
		return nil
	}

	s.mu.Lock("get")
	defer s.mu.Unlock()

	p, ok := s.peers[account]
	if !ok {
		if len(s.peers) >= maxAccountPeers {
			return nil
		}

		p = s.build(account)
		s.peers[account] = p
	}

	return p
}

func (s *accountSet) all() []*Handler {
	if s == nil {
		return nil
	}

	s.mu.RLock("all")
	defer s.mu.RUnlock()

	out := make([]*Handler, 0, len(s.peers))
	for _, a := range slices.Sorted(maps.Keys(s.peers)) {
		out = append(out, s.peers[a])
	}

	return out
}

func (s *accountSet) drain() []*Handler {
	if s == nil {
		return nil
	}

	s.mu.Lock("drain")
	defer s.mu.Unlock()

	out := make([]*Handler, 0, len(s.peers))
	for _, a := range slices.Sorted(maps.Keys(s.peers)) {
		out = append(out, s.peers[a])
	}

	s.peers = make(map[string]*Handler)

	return out
}

// snapshot adds an additive "accounts" object to base; with no siblings base is returned unchanged.
func (s *accountSet) snapshot(base []byte, snap func(*Handler) []byte) []byte {
	if s == nil || base == nil {
		return base
	}

	s.mu.RLock("snapshot")
	accounts := make(map[string]json.RawMessage, len(s.peers))

	for a, p := range s.peers {
		if data := snap(p); data != nil {
			accounts[a] = data
		}
	}
	s.mu.RUnlock()

	if len(accounts) == 0 {
		return base
	}

	doc := make(map[string]json.RawMessage)
	if err := json.Unmarshal(base, &doc); err != nil {
		return base
	}

	enc, err := json.Marshal(accounts)
	if err != nil {
		return base
	}

	doc[accountsSnapshotKey] = enc

	out, err := json.Marshal(doc)
	if err != nil {
		return base
	}

	return out
}

// restore rebuilds siblings from the "accounts" object in data; older snapshots without it just clear them.
func (s *accountSet) restore(data []byte, restore func(*Handler, []byte) error) error {
	if s == nil {
		return nil
	}

	for _, p := range s.drain() {
		p.Reset()
	}

	var doc struct {
		Accounts map[string]json.RawMessage `json:"accounts"`
	}

	if err := json.Unmarshal(data, &doc); err != nil {
		return err
	}

	for _, a := range slices.Sorted(maps.Keys(doc.Accounts)) {
		if p := s.get(a); p != nil {
			if err := restore(p, doc.Accounts[a]); err != nil {
				return err
			}
		}
	}

	return nil
}
