package ocidns

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/miekg/dns"

	pkgdns "github.com/blackbirdworks/gopherstack/pkgs/dns"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	eventQueue    = 256
	refreshTick   = 10 * time.Second
	initialRetry  = 30 * time.Second
	xferOpTimeout = 30 * time.Second
	soaFieldCount = 7
	evPull        = "pull"
	evNotify      = "notify"
	uint32Mask    = 0xFFFFFFFF
)

type xferEvent struct {
	kind   string
	zoneID string
}

// queue schedules background transfer work; a full queue drops the event, which the periodic refresh recovers.
func (b *InMemoryBackend) queue(ev xferEvent) {
	select {
	case b.events <- ev:
	default:
	}
}

func (b *InMemoryBackend) queueZoneChange(z *Zone) {
	if z.Scope != scopeGlobal {
		return
	}

	switch {
	case z.ZoneType == zoneSecondary:
		b.queue(xferEvent{kind: evPull, zoneID: z.ID})
	case len(z.ExternalDownstreams) > 0:
		b.queue(xferEvent{kind: evNotify, zoneID: z.ID})
	}
}

func transferredApex(z *Zone) []recordWire {
	out := []recordWire{{
		Domain: z.Name, Rtype: rtypeSOA, TTL: z.ApexSOA.TTL, Rdata: z.ApexSOA.Rdata[0], IsProtected: true,
		RrsetVersion: strconv.FormatInt(z.Version, 10),
	}}

	if z.ApexNS != nil {
		for _, rd := range z.ApexNS.Rdata {
			out = append(out, recordWire{
				Domain: z.Name, Rtype: rtypeNS, TTL: z.ApexNS.TTL, Rdata: rd, IsProtected: true, RrsetVersion: "1",
			})
		}
	}

	for i := range out {
		out[i].RecordHash = recordHash(out[i].Domain, out[i].Rtype, out[i].TTL, out[i].Rdata)
	}

	return out
}

// tsigLookup finds a TSIG key by canonical name for the DNS server's signature checks.
func (b *InMemoryBackend) tsigLookup(canonical string) (pkgdns.TSIGKey, bool) {
	b.mu.RLock("tsigLookup")
	defer b.mu.RUnlock()

	for _, k := range b.tsigs {
		if pkgdns.CanonicalKeyName(k.Name) == canonical {
			return pkgdns.TSIGKey{Name: k.Name, Algorithm: k.Algorithm, Secret: k.Secret}, true
		}
	}

	return pkgdns.TSIGKey{}, false
}

type peer struct {
	key  *pkgdns.TSIGKey
	addr string
}

func (b *InMemoryBackend) peers(servers []ExtServer) []peer {
	out := make([]peer, 0, len(servers))

	for _, s := range servers {
		port := dnsPort
		if s.Port != nil {
			port = *s.Port
		}

		p := peer{addr: net.JoinHostPort(s.Address, strconv.Itoa(port))}

		if k, ok := b.tsigs[s.TsigKeyID]; ok {
			p.key = &pkgdns.TSIGKey{Name: k.Name, Algorithm: k.Algorithm, Secret: k.Secret}
		}

		out = append(out, p)
	}

	return out
}

// RunTransfers performs zone transfers and NOTIFYs until ctx ends: it pulls SECONDARY zones from their external
// masters when they are created, changed, notified or due by their SOA timers, and notifies external downstreams of
// changes to PRIMARY zones.
func (b *InMemoryBackend) RunTransfers(ctx context.Context) {
	tick := time.NewTicker(refreshTick)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-b.events:
			b.handleEvent(ctx, ev)
		case <-tick.C:
			b.refreshDue(ctx)
		}
	}
}

func (b *InMemoryBackend) handleEvent(ctx context.Context, ev xferEvent) {
	ctx, cancel := context.WithTimeout(ctx, xferOpTimeout)
	defer cancel()

	switch ev.kind {
	case evPull:
		b.pull(ctx, ev.zoneID)
	case evNotify:
		b.notifyDownstreams(ctx, ev.zoneID)
	}
}

func (b *InMemoryBackend) refreshDue(ctx context.Context) {
	b.mu.RLock("refreshDue")

	var due []string

	now := b.nowUTC()

	for id, z := range b.zones {
		if z.Scope != scopeGlobal || z.ZoneType != zoneSecondary {
			continue
		}

		if b.pullDue(z, now) {
			due = append(due, id)
		}
	}

	b.mu.RUnlock()

	slices.Sort(due)

	for _, id := range due {
		b.handleEvent(ctx, xferEvent{kind: evPull, zoneID: id})
	}
}

func (b *InMemoryBackend) pullDue(z *Zone, now time.Time) bool {
	refresh, retry := soaTimers(z)

	if last := b.attempts[z.ID]; !last.IsZero() && now.Sub(last) < retry {
		return false
	}

	return z.LastTransfer.IsZero() || now.Sub(z.LastTransfer) >= refresh
}

// soaTimers returns the transferred SOA's refresh and retry intervals; a zone never transferred retries quickly.
func soaTimers(z *Zone) (time.Duration, time.Duration) {
	if z.ApexSOA == nil {
		return 0, initialRetry
	}

	rr, err := dns.NewRR(z.Name + ". 0 IN SOA " + z.ApexSOA.Rdata[0])
	if err != nil {
		return time.Duration(soaRefresh) * time.Second, time.Duration(soaRetry) * time.Second
	}

	soa, ok := rr.(*dns.SOA)
	if !ok {
		return time.Duration(soaRefresh) * time.Second, time.Duration(soaRetry) * time.Second
	}

	return time.Duration(soa.Refresh) * time.Second, max(time.Duration(soa.Retry)*time.Second, time.Second)
}

func (b *InMemoryBackend) pull(ctx context.Context, zoneID string) {
	if err := b.Pull(ctx, zoneID); err != nil {
		logger.Load(ctx).WarnContext(ctx, "ocidns: zone transfer failed", "zone", zoneID, "error", err)
	}
}

// Pull transfers a SECONDARY zone from its external masters now, trying them in order, and returns the joined
// errors when none of them delivered the zone.
func (b *InMemoryBackend) Pull(ctx context.Context, zoneID string) error {
	b.mu.Lock("Pull")

	z, ok := b.zones[zoneID]
	if !ok || z.ZoneType != zoneSecondary || z.Scope != scopeGlobal {
		b.mu.Unlock()

		return fmt.Errorf("%w: %s is not a SECONDARY GLOBAL zone", errTransfer, zoneID)
	}

	b.attempts[zoneID] = b.nowUTC()
	name, masters := z.Name, b.peers(z.ExternalMasters)
	b.mu.Unlock()

	var errs []error

	for _, m := range masters {
		rrs, err := pkgdns.FetchZone(ctx, m.addr, name, m.key)
		if err == nil {
			err = b.applyTransfer(zoneID, rrs)
		}

		if err == nil {
			return nil
		}

		errs = append(errs, fmt.Errorf("master %s: %w", m.addr, err))
	}

	return errors.Join(errs...)
}

func (b *InMemoryBackend) applyTransfer(zoneID string, rrs []dns.RR) error {
	b.mu.Lock("applyTransfer")
	defer b.mu.Unlock()

	z, ok := b.zones[zoneID]
	if !ok {
		return nil
	}

	soa, ok := rrs[0].(*dns.SOA)
	if !ok {
		return fmt.Errorf("%w: first record is not an SOA", errTransfer)
	}

	now := b.nowUTC()
	z.LastTransfer = now

	if z.ApexSOA != nil && serialOf(z.ApexSOA) == int64(soa.Serial) {
		return nil
	}

	records, apexNS, err := z.fromTransfer(rrs[1 : len(rrs)-1])
	if err != nil {
		return err
	}

	serial := int64(soa.Serial)
	z.ApexSOA = &RRSet{
		Domain: z.Name, Rtype: rtypeSOA, TTL: int(soa.Hdr.Ttl), Version: serial,
		Rdata: []string{strings.TrimPrefix(soa.String(), soa.Hdr.String())},
	}
	z.ApexNS, z.Records, z.Version = apexNS, records, serial

	for _, rs := range records {
		rs.Version = serial
	}

	z.touch(now)

	return nil
}

func serialOf(soa *RRSet) int64 {
	fields := strings.Fields(soa.Rdata[0])
	if len(fields) != soaFieldCount {
		return -1
	}

	n, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return -1
	}

	return n & uint32Mask
}

// fromTransfer converts transferred records into record sets, splitting the apex NS set from the user records.
// Records outside the zone and of types the service does not manage are dropped.
func (z *Zone) fromTransfer(rrs []dns.RR) (map[string]*RRSet, *RRSet, error) {
	records := map[string]*RRSet{}

	var apexNS *RRSet

	for _, rr := range rrs {
		hdr := rr.Header()
		rtype := dns.TypeToString[hdr.Rrtype]
		owner, ok := normName(hdr.Name, true)

		if !ok || !inZone(z.Name, owner) || rtype == rtypeSOA || !supportedRtypes[rtype] {
			continue
		}

		rdata, aerr := normRdata(owner, rtype, strings.TrimPrefix(rr.String(), hdr.String()), int(hdr.Ttl))
		if aerr != nil {
			return nil, nil, fmt.Errorf("%w: %s %s: %s", errTransfer, owner, rtype, aerr.Message)
		}

		if owner == z.Name && rtype == rtypeNS {
			apexNS = addRdata(apexNS, owner, rtype, int(hdr.Ttl), rdata)

			continue
		}

		k := rrKey(owner, rtype)
		records[k] = addRdata(records[k], owner, rtype, int(hdr.Ttl), rdata)
	}

	return records, apexNS, nil
}

func addRdata(rs *RRSet, owner, rtype string, ttl int, rdata string) *RRSet {
	if rs == nil {
		return &RRSet{Domain: owner, Rtype: rtype, TTL: ttl, Rdata: []string{rdata}}
	}

	rs.TTL = min(rs.TTL, ttl)

	if !slices.Contains(rs.Rdata, rdata) {
		rs.Rdata = append(rs.Rdata, rdata)
	}

	return rs
}

func (b *InMemoryBackend) notifyDownstreams(ctx context.Context, zoneID string) {
	b.mu.RLock("notifyDownstreams")

	z, ok := b.zones[zoneID]
	if !ok || z.Scope != scopeGlobal || len(z.ExternalDownstreams) == 0 {
		b.mu.RUnlock()

		return
	}

	name, downstreams := z.Name, b.peers(z.ExternalDownstreams)
	soa := b.soaRR(z)
	b.mu.RUnlock()

	log := logger.Load(ctx)

	for _, d := range downstreams {
		if err := pkgdns.SendNotify(ctx, d.addr, name, soa, d.key); err != nil {
			log.WarnContext(ctx, "ocidns: notify failed", "zone", name, "downstream", d.addr, "error", err)
		}
	}
}

func (b *InMemoryBackend) soaRR(z *Zone) dns.RR {
	for _, m := range b.managed(z) {
		if m.Rtype == rtypeSOA {
			if rrs := toRRs(
				z.Name,
				&RRSet{Domain: z.Name, Rtype: rtypeSOA, TTL: m.TTL, Rdata: []string{m.Rdata}},
			); len(
				rrs,
			) > 0 {
				return rrs[0]
			}
		}
	}

	return nil
}

func remoteIP(a net.Addr) (netip.Addr, bool) {
	if a == nil {
		return netip.Addr{}, false
	}

	ap, err := netip.ParseAddrPort(a.String())
	if err != nil {
		return netip.Addr{}, false
	}

	return ap.Addr().Unmap(), true
}

type signer struct {
	err  error
	name string
}

func signerOf(w dns.ResponseWriter, req *dns.Msg) signer {
	t := req.IsTsig()
	if t == nil {
		return signer{}
	}

	return signer{name: pkgdns.CanonicalKeyName(t.Hdr.Name), err: w.TsigStatus()}
}

// peerAllowed reports whether a request from src signed by who may act as one of servers.
func (b *InMemoryBackend) peerAllowed(servers []ExtServer, src netip.Addr, who signer) int {
	matched := false

	for _, s := range servers {
		addr, err := netip.ParseAddr(s.Address)
		if err != nil || addr.Unmap() != src {
			continue
		}

		matched = true

		if who.err != nil {
			continue
		}

		k, hasKey := b.tsigs[s.TsigKeyID]

		switch {
		case !hasKey:
			return dns.RcodeSuccess
		case who.name == pkgdns.CanonicalKeyName(k.Name):
			return dns.RcodeSuccess
		}
	}

	if matched || who.err != nil {
		return dns.RcodeNotAuth
	}

	return dns.RcodeRefused
}

// InterceptStream implements pkgs/dns.StreamInterceptor for AXFR, IXFR and NOTIFY.
func (a authority) InterceptStream(w dns.ResponseWriter, req *dns.Msg) bool {
	if len(req.Question) != 1 || req.Question[0].Qclass != dns.ClassINET {
		return false
	}

	q := req.Question[0]

	switch {
	case req.Opcode == dns.OpcodeNotify && q.Qtype == dns.TypeSOA:
		a.notify(w, req)

		return true
	case req.Opcode == dns.OpcodeQuery && (q.Qtype == dns.TypeAXFR || q.Qtype == dns.TypeIXFR) &&
		w.RemoteAddr() != nil && w.RemoteAddr().Network() == "tcp":
		a.transfer(w, req)

		return true
	}

	return false
}

func reply(w dns.ResponseWriter, req *dns.Msg, rcode int) {
	m := new(dns.Msg)
	m.SetRcode(req, rcode)
	m.Authoritative = true
	_ = w.WriteMsg(m)
}

func (a authority) notify(w dns.ResponseWriter, req *dns.Msg) {
	name, ok := normName(req.Question[0].Name, false)
	src, okIP := remoteIP(w.RemoteAddr())

	if !ok || !okIP {
		reply(w, req, dns.RcodeRefused)

		return
	}

	rcode := a.b.acceptNotify(name, src, signerOf(w, req))
	reply(w, req, rcode)
}

func (b *InMemoryBackend) acceptNotify(name string, src netip.Addr, who signer) int {
	b.mu.RLock("acceptNotify")
	defer b.mu.RUnlock()

	for _, z := range b.zones {
		if z.Scope != scopeGlobal || z.ZoneType != zoneSecondary || z.Name != name {
			continue
		}

		switch rcode := b.peerAllowed(z.ExternalMasters, src, who); rcode {
		case dns.RcodeSuccess:
			b.queue(xferEvent{kind: evPull, zoneID: z.ID})

			return rcode
		default:
			return dns.RcodeNotAuth
		}
	}

	return dns.RcodeNotAuth
}

func (a authority) transfer(w dns.ResponseWriter, req *dns.Msg) {
	q := req.Question[0]
	name, ok := normName(q.Name, false)
	src, okIP := remoteIP(w.RemoteAddr())

	if !ok || !okIP {
		reply(w, req, dns.RcodeRefused)

		return
	}

	rrs, rcode := a.b.transferRecords(name, src, signerOf(w, req))
	if rcode != dns.RcodeSuccess {
		reply(w, req, rcode)

		return
	}

	if q.Qtype == dns.TypeIXFR && len(req.Ns) > 0 {
		have, haveOK := rrs[0].(*dns.SOA)
		want, wantOK := req.Ns[0].(*dns.SOA)

		if haveOK && wantOK && have.Serial == want.Serial {
			rrs = rrs[:1]
		}
	}

	if err := pkgdns.ServeTransfer(w, req, rrs); err != nil {
		logger.Load(context.Background()).Warn("ocidns: zone transfer aborted", "zone", name, "error", err)
	}
}

// transferRecords lists the zone for an outbound transfer: the SOA, every other record, the SOA again.
func (b *InMemoryBackend) transferRecords(name string, src netip.Addr, who signer) ([]dns.RR, int) {
	b.mu.RLock("transferRecords")
	defer b.mu.RUnlock()

	var z *Zone

	for _, cand := range b.zones {
		if cand.Scope == scopeGlobal && cand.Name == name {
			z = cand

			break
		}
	}

	if z == nil {
		return nil, dns.RcodeRefused
	}

	if rcode := b.peerAllowed(z.ExternalDownstreams, src, who); rcode != dns.RcodeSuccess {
		return nil, rcode
	}

	soa := b.soaRR(z)
	if soa == nil {
		return nil, dns.RcodeServerFailure
	}

	nodes := b.nodes(z)
	out := []dns.RR{soa}

	for _, owner := range slices.Sorted(maps.Keys(nodes)) {
		for _, rtype := range slices.Sorted(maps.Keys(nodes[owner])) {
			if owner == z.Name && rtype == rtypeSOA {
				continue
			}

			out = append(out, toRRs(owner, nodes[owner][rtype])...)
		}
	}

	return append(out, dns.Copy(soa)), dns.RcodeSuccess
}
