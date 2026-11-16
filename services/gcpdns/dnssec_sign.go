package gcpdns

import (
	"crypto"
	"crypto/x509"
	"encoding/base64"
	"math"
	"time"

	"github.com/miekg/dns"
)

const (
	sigValidity = 7 * 24 * time.Hour
	sigSkew     = time.Hour
	nsecDefault = 300
)

type proofKind int

const (
	proofNone proofKind = iota
	proofNoData
	proofNameError
)

type zoneSigner struct {
	key crypto.Signer
	tag uint16
	alg uint8
	ksk bool
}

type setKey struct {
	name string
	typ  uint16
}

func unix32(t time.Time) uint32 {
	u := t.Unix()
	if u < 0 {
		return 0
	}

	if u > math.MaxUint32 {
		return math.MaxUint32
	}

	return uint32(u)
}

func (z *zoneAnswer) signers() []zoneSigner {
	var out []zoneSigner

	for _, k := range z.rec.Keys {
		raw, err := base64.StdEncoding.DecodeString(z.rec.Private[k.Id])
		if err != nil || len(raw) == 0 || !k.IsActive {
			continue
		}

		parsed, err := x509.ParsePKCS8PrivateKey(raw)
		if err != nil {
			continue
		}

		sk, ok := parsed.(crypto.Signer)
		if !ok {
			continue
		}

		out = append(out, zoneSigner{
			key: sk, tag: uint16(min(max(k.KeyTag, 0), math.MaxUint16)), alg: algorithms[k.Algorithm].num,
			ksk: k.Type == keySigning,
		})
	}

	return out
}

func (z *zoneAnswer) nsec3Enabled() bool {
	cfg := z.rec.Zone.DnssecConfig

	return len(z.rec.Keys) > 0 && (cfg == nil || cfg.NonExistence != "nsec")
}

func (z *zoneAnswer) nsec3Param() *dns.NSEC3PARAM {
	return &dns.NSEC3PARAM{
		Hdr:  dns.RR_Header{Name: z.apex(), Rrtype: dns.TypeNSEC3PARAM, Class: dns.ClassINET, Ttl: nsecDefault},
		Hash: dns.SHA1,
	}
}

func (z *zoneAnswer) negTTL() uint32 {
	if soa := z.soaRR(); soa != nil {
		return min(soa.Hdr.Ttl, soa.Minttl)
	}

	return nsecDefault
}

func (z *zoneAnswer) sign() {
	sg := z.signers()
	if len(sg) == 0 {
		return
	}

	z.addDenial()
	z.resp.Answer = z.signSets(z.resp.Answer, sg)
	z.resp.Ns = z.signSets(z.resp.Ns, sg)
}

func (z *zoneAnswer) signSets(rrs []dns.RR, sg []zoneSigner) []dns.RR {
	groups := map[setKey][]dns.RR{}

	var order []setKey

	for _, rr := range rrs {
		k := setKey{rr.Header().Name, rr.Header().Rrtype}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}

		groups[k] = append(groups[k], rr)
	}

	out := make([]dns.RR, 0, len(rrs)+len(sg))

	for _, k := range order {
		out = append(out, groups[k]...)

		if k.typ == dns.TypeRRSIG || k.typ == dns.TypeNS && k.name == z.cut {
			continue
		}

		out = append(out, z.signGroup(groups[k], sg)...)
	}

	return out
}

func (z *zoneAnswer) signGroup(group []dns.RR, sg []zoneSigner) []dns.RR {
	h := group[0].Header()
	wantKSK := h.Rrtype == dns.TypeDNSKEY

	use := make([]zoneSigner, 0, len(sg))

	for _, s := range sg {
		if s.ksk == wantKSK {
			use = append(use, s)
		}
	}

	if len(use) == 0 {
		use = sg
	}

	set := group
	if z.wildcard != "" && h.Name == z.qname {
		set = make([]dns.RR, len(group))

		for i, rr := range group {
			set[i] = dns.Copy(rr)
			set[i].Header().Name = z.wildcard
		}
	}

	var sigs []dns.RR

	for _, s := range use {
		sig := &dns.RRSIG{
			Hdr:        dns.RR_Header{Rrtype: dns.TypeRRSIG, Class: dns.ClassINET, Ttl: h.Ttl},
			Algorithm:  s.alg,
			KeyTag:     s.tag,
			SignerName: z.apex(),
			Inception:  unix32(z.now.Add(-sigSkew)),
			Expiration: unix32(z.now.Add(sigValidity)),
		}

		if err := sig.Sign(s.key, set); err != nil {
			continue
		}

		sig.Hdr.Name = h.Name
		sigs = append(sigs, sig)
	}

	return sigs
}
