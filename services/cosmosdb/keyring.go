package cosmosdb

import "net/http"

// AcceptKey makes key a valid master key in addition to MasterKey, for keys the ARM control plane hands out.
func (h *Handler) AcceptKey(key string) {
	if key == "" {
		return
	}

	h.keysMu.Lock("AcceptKey")
	h.extraKeys[key] = struct{}{}
	h.keysMu.Unlock()
}

// RevokeKey stops accepting a key added by AcceptKey; the configured MasterKey is never revoked.
func (h *Handler) RevokeKey(key string) {
	h.keysMu.Lock("RevokeKey")
	delete(h.extraKeys, key)
	h.keysMu.Unlock()
}

func (h *Handler) candidateKeys() []string {
	h.keysMu.RLock("candidateKeys")
	defer h.keysMu.RUnlock()

	keys := make([]string, 0, len(h.extraKeys)+1)
	keys = append(keys, h.MasterKey)

	for k := range h.extraKeys {
		keys = append(keys, k)
	}

	return keys
}

// verifyAnyKey reports whether r verifies under any accepted key; the first verification error is returned only
// when no key matches.
func (h *Handler) verifyAnyKey(r *http.Request, verify func(string, *http.Request) (bool, error)) (bool, error) {
	var firstErr error

	for _, k := range h.candidateKeys() {
		ok, err := verify(k, r)
		if ok {
			return true, nil
		}

		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return false, firstErr
}
