package transfer

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// EnableRegions makes h serve every other region through lazily built per-region siblings whose timers run under ctx.
func (h *Handler) EnableRegions(ctx context.Context) {
	home, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}

	h.peers = regionpeers.New(home.region, func(region string) *Handler {
		bk := NewInMemoryBackend(ctx, home.accountID, region)
		if cfg := h.sftpConfig(); cfg != nil {
			bk.EnableSFTP(*cfg)
		}

		return NewHandler(bk)
	})
}

// EnableSFTP turns on real SFTP listeners for this region and every region built later.
func (h *Handler) EnableSFTP(cfg SFTPConfig) {
	h.sftpCfg = &cfg

	for _, bk := range h.RegionBackends() {
		if ib, ok := bk.(*InMemoryBackend); ok {
			ib.EnableSFTP(cfg)
		}
	}
}

func (h *Handler) sftpConfig() *SFTPConfig { return h.sftpCfg }

// RegionHandler returns the handler serving region: h itself for the home region, else its sibling.
func (h *Handler) RegionHandler(region string) *Handler {
	if p := h.peers.Get(region); p != nil {
		return p
	}

	return h
}

// BackendFor returns the backend serving region: the home backend, or the sibling built on first use.
func (h *Handler) BackendFor(region string) StorageBackend {
	return h.RegionHandler(region).Backend
}

// RegionBackends returns the home backend followed by every sibling built so far.
func (h *Handler) RegionBackends() []StorageBackend {
	peers := h.peers.All()
	out := make([]StorageBackend, 0, 1+len(peers))
	out = append(out, h.Backend)

	for _, p := range peers {
		out = append(out, p.Backend)
	}

	return out
}
