package azurearm

import "encoding/json"

// apimSnapshot is the persisted state of the API Management provider, subscription keys, secret named values and
// backend credentials included (as with the other azurearm credentials).
type apimSnapshot struct {
	Services   json.RawMessage `json:"services,omitempty"`
	Operations json.RawMessage `json:"operations,omitempty"`
}

func (p *APIMProvider) snapshot() *apimSnapshot {
	p.mu.RLock(apimCSnapshot)
	defer p.mu.RUnlock()

	raw, err := json.Marshal(p.svcs)
	if err != nil {
		return &apimSnapshot{}
	}

	ops, err := json.Marshal(p.ops)
	if err != nil {
		return &apimSnapshot{Services: raw}
	}

	return &apimSnapshot{Services: raw, Operations: ops}
}

// restore replaces the provider's state; nil clears it.
func (p *APIMProvider) restore(snap *apimSnapshot) {
	p.mu.Lock(apimCRestore)
	defer p.mu.Unlock()

	p.svcs = map[string]*apimService{}
	p.ops = map[string]*apimOp{}

	if snap == nil || len(snap.Services) == 0 {
		return
	}

	var ops map[string]*apimOp
	if err := json.Unmarshal(snap.Operations, &ops); err == nil {
		for id, o := range ops {
			if o != nil {
				p.ops[id] = o
			}
		}
	}

	var svcs map[string]*apimService
	if err := json.Unmarshal(snap.Services, &svcs); err != nil {
		return
	}

	for k, s := range svcs {
		if s == nil {
			continue
		}

		s.now = p.now
		ensureAPIMMaps(s)
		p.svcs[k] = s
	}
}

func ensureAPIMMaps(s *apimService) {
	all := []*map[string]*apimEntity{&s.APIs, &s.Ops, &s.Products, &s.Subs, &s.Named, &s.Backends, &s.Policies}

	for _, m := range all {
		if *m == nil {
			*m = map[string]*apimEntity{}
		}
	}

	if s.ProductAPIs == nil {
		s.ProductAPIs = map[string]bool{}
	}

	if s.Extra == nil {
		s.Extra = map[string]any{}
	}
}
