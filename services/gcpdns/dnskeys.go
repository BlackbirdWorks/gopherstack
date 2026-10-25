package gcpdns

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/miekg/dns"
	dnsapi "google.golang.org/api/dns/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	keySigning  = "keySigning"
	zoneSigning = "zoneSigning"
	kskFlags    = 257
	zskFlags    = 256
)

type algorithm struct {
	num     uint8
	lengths []int64
}

var algorithms = map[string]algorithm{ //nolint:gochecknoglobals // immutable catalog
	"rsasha1":         {dns.RSASHA1, []int64{1024, 2048, 4096}},
	"rsasha256":       {dns.RSASHA256, []int64{1024, 2048, 4096}},
	"rsasha512":       {dns.RSASHA512, []int64{1024, 2048, 4096}},
	"ecdsap256sha256": {dns.ECDSAP256SHA256, []int64{256}},
	"ecdsap384sha384": {dns.ECDSAP384SHA384, []int64{384}},
}

var digestTypes = map[string]uint8{ //nolint:gochecknoglobals // immutable catalog
	"sha1": dns.SHA1, "sha256": dns.SHA256, "sha384": dns.SHA384,
}

func normalizeKeySpecs(specs []*dnsapi.DnsKeySpec) error {
	for i, s := range specs {
		path := fmt.Sprintf("%s.dnssecConfig.defaultKeySpecs[%d]", zonePath, i)
		s.Kind = "dns#dnsKeySpec"

		alg, ok := algorithms[s.Algorithm]
		if !ok {
			return errInvalidValue(path+".algorithm", s.Algorithm)
		}

		if s.KeyType != keySigning && s.KeyType != zoneSigning {
			return errInvalidValue(path+".keyType", s.KeyType)
		}

		if s.KeyLength == 0 {
			s.KeyLength = alg.lengths[0]

			if s.KeyType == keySigning && len(alg.lengths) > 1 {
				s.KeyLength = 2048
			}
		}

		if !containsInt(alg.lengths, s.KeyLength) {
			return errInvalidValue(path+".keyLength", strconv.FormatInt(s.KeyLength, 10))
		}
	}

	return nil
}

func containsInt(list []int64, v int64) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}

	return false
}

func keyRR(zone string, k *dnsapi.DnsKey) *dns.DNSKEY {
	flags := uint16(zskFlags)
	if k.Type == keySigning {
		flags = kskFlags
	}

	return &dns.DNSKEY{
		Hdr:   dns.RR_Header{Name: zone, Rrtype: dns.TypeDNSKEY, Class: dns.ClassINET, Ttl: 3600},
		Flags: flags, Protocol: 3, Algorithm: algorithms[k.Algorithm].num, PublicKey: k.PublicKey,
	}
}

func generateKeys(z *dnsapi.ManagedZone, firstID int, now string) ([]*dnsapi.DnsKey, error) {
	specs := z.DnssecConfig.DefaultKeySpecs
	if len(specs) == 0 {
		specs = defaultKeySpecs()
		if err := normalizeKeySpecs(specs); err != nil {
			return nil, err
		}
	}

	keys := make([]*dnsapi.DnsKey, 0, len(specs))

	for i, s := range specs {
		k := &dnsapi.DnsKey{
			Kind: "dns#dnsKey", Id: strconv.Itoa(firstID + i), Type: s.KeyType, Algorithm: s.Algorithm,
			KeyLength: s.KeyLength, IsActive: true, CreationTime: now,
		}
		rr := keyRR(z.DnsName, k)

		if _, err := rr.Generate(int(s.KeyLength)); err != nil {
			return nil, errInvalid("Cannot generate %s key: %v", s.Algorithm, err)
		}

		k.PublicKey, k.KeyTag = rr.PublicKey, int64(rr.KeyTag())
		keys = append(keys, k)
	}

	return keys, nil
}

func withDigests(zone string, k *dnsapi.DnsKey, types []string) (*dnsapi.DnsKey, error) {
	out := clone(k)
	if k.Type != keySigning {
		return out, nil
	}

	if len(types) == 0 {
		types = []string{"sha256"}
	}

	for _, t := range types {
		code, ok := digestTypes[strings.ToLower(t)]
		if !ok {
			return nil, errInvalidValue("digestType", t)
		}

		ds := keyRR(zone, k).ToDS(code)
		if ds == nil {
			return nil, errInvalidValue("digestType", t)
		}

		out.Digests = append(out.Digests, &dnsapi.DnsKeyDigest{Digest: strings.ToLower(ds.Digest), Type: strings.ToLower(t)})
	}

	return out, nil
}

// GetKey returns one DNSSEC key with the requested KSK digests (sha256 by default).
func (b *InMemoryBackend) GetKey(project, zone, id string, digests []string) (*dnsapi.DnsKey, error) {
	b.mu.RLock("GetKey")
	defer b.mu.RUnlock()

	rec, err := b.zoneLocked(project, zone)
	if err != nil {
		return nil, err
	}

	for _, k := range rec.Keys {
		if k.Id == id {
			return withDigests(rec.Zone.DnsName, k, digests)
		}
	}

	return nil, errNotFound("parameters.dnsKeyId", id)
}

// ListKeys returns a page of the zone's DNSSEC keys.
func (b *InMemoryBackend) ListKeys(
	project, zone, token string, limit int, digests []string,
) (page.Page[*dnsapi.DnsKey], error) {
	if err := page.ValidateToken(token); err != nil {
		return page.Page[*dnsapi.DnsKey]{}, errInvalidValue("pageToken", token)
	}

	b.mu.RLock("ListKeys")
	defer b.mu.RUnlock()

	rec, err := b.zoneLocked(project, zone)
	if err != nil {
		return page.Page[*dnsapi.DnsKey]{}, err
	}

	all := make([]*dnsapi.DnsKey, 0, len(rec.Keys))

	for _, k := range rec.Keys {
		dk, derr := withDigests(rec.Zone.DnsName, k, digests)
		if derr != nil {
			return page.Page[*dnsapi.DnsKey]{}, derr
		}

		all = append(all, dk)
	}

	return page.New(all, token, limit, 100), nil
}
