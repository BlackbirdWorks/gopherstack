package azurearm

import (
	"fmt"
	"hash/fnv"
	"runtime"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	aciAddrSpan   = 1 << 24
	aciAddrOctets = 8
	aciOctetMask  = 0xff
	aciAddrSecond = 16
)

// hostFor is the host address a published port of the group binds to: the group's own loopback address when it has
// one, otherwise 127.0.0.1.
func (r *aciRun) hostFor(port int) string {
	if h, ok := r.backHost[port]; ok && port != 0 {
		return h
	}

	if r.addr != "" {
		return r.addr
	}

	return container.LoopbackHost
}

// assignAddress gives a group with an ipAddress its own address in 127.0.0.0/8, so its ports are published under the
// declared numbers instead of pool ports. Linux binds all of 127/8 without configuration; other hosts, and Container
// Apps replicas (which are reached through the ARM listener), keep 127.0.0.1 and pool ports.
func (e *ACIEngine) assignAddress(run *aciRun, cfg aciConfig) {
	if runtime.GOOS != "linux" || cfg.Props.IPAddress == nil || strings.HasPrefix(cfg.Key, acaKeyPrefix) {
		return
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(e.nonce + cfg.Key))
	seed := h.Sum32()

	e.mu.Lock("assignAddress")
	defer e.mu.Unlock()

	for i := range uint32(aciAddrSpan) {
		n := (seed + i) % aciAddrSpan
		o2, o3, o4 := n>>aciAddrSecond&aciOctetMask, n>>aciAddrOctets&aciOctetMask, n&aciOctetMask
		addr := fmt.Sprintf("127.%d.%d.%d", o2, o3, o4)

		if n&aciOctetMask == 0 || n&aciOctetMask == aciOctetMask || n == 0 || e.addrs[addr] {
			continue
		}

		e.addrs[addr] = true
		run.addr = addr

		return
	}
}
