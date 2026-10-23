package dashboard

import (
	"net"
	"sort"
	"strconv"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

// AllocatedPort is one entry of the system/state ports list.
type AllocatedPort struct {
	Label     string `json:"label"`
	Port      int    `json:"port"`
	Fixed     bool   `json:"fixed"`
	Listening bool   `json:"listening"`
}

// CollectAllocatedPorts lists pool allocations plus fixed service
// reservations, probing the fixed ones for health.
func CollectAllocatedPorts(alloc *portalloc.Allocator) []AllocatedPort {
	out := make([]AllocatedPort, 0)
	if alloc == nil {
		return out
	}

	reserved := alloc.Reserved()
	for port, label := range alloc.Allocated() {
		_, fixed := reserved[port]
		out = append(out, AllocatedPort{Port: port, Label: label, Fixed: fixed})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Port < out[j].Port })
	probeFixedPorts(out)

	return out
}

func probeFixedPorts(ports []AllocatedPort) {
	var wg sync.WaitGroup

	for i := range ports {
		if !ports[i].Fixed {
			continue
		}

		wg.Go(func() {
			ports[i].Listening = portalloc.IsListening(net.JoinHostPort("127.0.0.1", strconv.Itoa(ports[i].Port)))
		})
	}

	wg.Wait()
}
