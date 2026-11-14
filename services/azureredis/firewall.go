package azureredis

import (
	"fmt"
	"net"
	"net/netip"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

// IPRange is one firewall rule: a contiguous, inclusive IPv4 range.
type IPRange struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

type addrRange struct{ lo, hi netip.Addr }

// firewall is a live-updatable client allow-list. An empty list admits every client.
type firewall struct{ rules atomic.Pointer[[]addrRange] }

func parseRanges(in []IPRange) ([]addrRange, error) {
	out := make([]addrRange, 0, len(in))

	for _, r := range in {
		lo, errLo := netip.ParseAddr(r.Start)
		hi, errHi := netip.ParseAddr(r.End)

		if errLo != nil || errHi != nil || !lo.Is4() || !hi.Is4() || lo.Compare(hi) > 0 {
			return nil, fmt.Errorf("%w: firewall rule %q-%q is not an IPv4 range", ErrInvalid, r.Start, r.End)
		}

		out = append(out, addrRange{lo: lo, hi: hi})
	}

	return out, nil
}

func (f *firewall) set(in []IPRange) error {
	rules, err := parseRanges(in)
	if err != nil {
		return err
	}

	f.rules.Store(&rules)

	return nil
}

// admit refuses a client whose address is not in any rule once rules exist; an unreadable address is refused.
func (f *firewall) admit(remote net.Addr) bool {
	p := f.rules.Load()
	if p == nil || len(*p) == 0 {
		return true
	}

	addr := sgfilter.AddrOf(remote)
	if !addr.IsValid() {
		return false
	}

	for _, r := range *p {
		if addr.Compare(r.lo) >= 0 && addr.Compare(r.hi) <= 0 {
			return true
		}
	}

	return false
}
