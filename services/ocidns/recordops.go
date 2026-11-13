package ocidns

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/miekg/dns"
)

// GetRecords lists records of a zone, optionally restricted to a domain or a record set.
func (b *InMemoryBackend) GetRecords(r ZoneRef, f RecordFilter, o ListOpts, mustExist bool) (RecordList, tag, error) {
	b.mu.RLock("GetRecords")
	defer b.mu.RUnlock()

	z, err := b.findZone(r)
	if err != nil {
		return RecordList{}, tag{}, err
	}

	list, aerr := b.listRecordsLocked(z, f, o, mustExist)
	if aerr != nil {
		return RecordList{}, tag{}, aerr
	}

	return list, tagOf(z.ID, z.meta), nil
}

// RecordScope restricts a record operation to a domain and record type.
type RecordScope struct {
	Domain string
	Rtype  string
}

func (b *InMemoryBackend) scopedRecords(z *Zone, s RecordScope) RecordCollectionWire {
	recs, _ := b.filterRecords(z, RecordFilter{Domain: s.Domain, Rtype: s.Rtype})
	slices.SortFunc(recs, recordSorters["domain"])

	return RecordCollectionWire{Items: nonNil(recs)}
}

func (b *InMemoryBackend) mutateRecords(
	r ZoneRef, c cond, s RecordScope, name string, fn func(z *Zone) *APIError,
) (RecordCollectionWire, tag, error) {
	b.mu.Lock(name)
	defer b.mu.Unlock()

	z, err := b.findZone(r)
	if err != nil {
		return RecordCollectionWire{}, tag{}, err
	}

	if err = c.check(z.ID, z.meta); err != nil {
		return RecordCollectionWire{}, tag{}, err
	}

	if aerr := fn(z); aerr != nil {
		return RecordCollectionWire{}, tag{}, aerr
	}

	return b.scopedRecords(z, s), tagOf(z.ID, z.meta), nil
}

// PatchRecords applies PATCH operations within the scope.
func (b *InMemoryBackend) PatchRecords(
	r ZoneRef,
	c cond,
	s RecordScope,
	in ItemsIn,
) (RecordCollectionWire, tag, error) {
	return b.mutateRecords(r, c, s, "PatchRecords", func(z *Zone) *APIError {
		return b.patchRecords(z, in.Items, s.Domain, s.Rtype)
	})
}

// UpdateRecords replaces the records within the scope.
func (b *InMemoryBackend) UpdateRecords(
	r ZoneRef,
	c cond,
	s RecordScope,
	in ItemsIn,
) (RecordCollectionWire, tag, error) {
	return b.mutateRecords(r, c, s, "UpdateRecords", func(z *Zone) *APIError {
		return b.replaceRecords(z, in.Items, s.Domain, s.Rtype)
	})
}

// DeleteRecords removes the user records of a domain or of one of its record sets.
func (b *InMemoryBackend) DeleteRecords(r ZoneRef, c cond, s RecordScope) error {
	b.mu.Lock("DeleteRecords")
	defer b.mu.Unlock()

	z, err := b.findZone(r)
	if err != nil {
		return err
	}

	if err = c.check(z.ID, z.meta); err != nil {
		return err
	}

	if aerr := b.deleteRecords(z, s.Domain, s.Rtype); aerr != nil {
		return aerr
	}

	return nil
}

// ZoneContent renders a zone in RFC 1035 master file format.
func (b *InMemoryBackend) ZoneContent(r ZoneRef) (string, tag, error) {
	b.mu.RLock("ZoneContent")
	defer b.mu.RUnlock()

	z, err := b.findZone(r)
	if err != nil {
		return "", tag{}, err
	}

	recs := b.allRecords(z)
	sort.SliceStable(recs, func(i, j int) bool { return recordSorters["domain"](recs[i], recs[j]) < 0 })

	var sb strings.Builder

	fmt.Fprintf(&sb, "$ORIGIN %s.\n", z.Name)

	for _, rec := range recs {
		fmt.Fprintf(&sb, "%s.\t%d\tIN\t%s\t%s\n", rec.Domain, rec.TTL, rec.Rtype, rec.Rdata)
	}

	return sb.String(), tagOf(z.ID, z.meta), nil
}

// CreateZoneFromFile creates a PRIMARY zone from an RFC 1035 master file.
func (b *InMemoryBackend) CreateZoneFromFile(compartment, scope, viewID, text, token string) (ZoneWire, tag, error) {
	recs, name, perr := parseZoneFile(text)
	if perr != nil {
		return ZoneWire{}, tag{}, perr
	}

	b.mu.Lock("CreateZoneFromFile")
	defer b.mu.Unlock()

	if id, ok := b.retry.Lookup("CreateZoneFromFile", token, b.nowUTC()); ok {
		z, live := b.zones[id]
		if !live {
			return ZoneWire{}, tag{}, errInvalidatedToken()
		}

		return b.zoneWire(z), tagOf(z.ID, z.meta), nil
	}

	in := CreateZoneIn{Name: name, CompartmentID: compartment, Scope: scope, ViewID: viewID}
	if err := b.validateZoneIn(&in); err != nil {
		return ZoneWire{}, tag{}, err
	}

	z := &Zone{Name: in.Name, Scope: in.Scope}
	next := map[string]*RRSet{}
	managed := b.managed(&Zone{Name: in.Name, Scope: in.Scope, Version: 1})

	for _, rr := range recs {
		h := rr.Header()
		owner, ok := normName(h.Name, true)

		if !ok || !inZone(in.Name, owner) {
			return ZoneWire{}, tag{}, errParam("record " + h.Name + " is not within zone " + in.Name)
		}

		ttl := int(h.Ttl)
		rtype := dns.TypeToString[h.Rrtype]
		rdata := strings.TrimPrefix(rr.String(), h.String())
		s := recSpec{domain: owner, rtype: rtype, rdata: rdata, ttl: ttl, hasTTL: true}

		if z.isManaged(owner, rtype) {
			if rtype != rtypeSOA && !managedHas(managed, s) {
				return ZoneWire{}, tag{}, errParam("the apex NS records are managed by the service")
			}

			continue
		}

		if !supportedRtypes[rtype] {
			return ZoneWire{}, tag{}, errParam("record type " + rtype + " is not supported")
		}

		z.addRecord(next, s)
	}

	if err := z.checkExclusive(next); err != nil {
		return ZoneWire{}, tag{}, err
	}

	created := b.newZone(in)
	created.Records = next

	for _, rs := range next {
		rs.Version = created.Version
	}

	return b.zoneWire(created), tagOf(created.ID, created.meta), nil
}

func parseZoneFile(text string) ([]dns.RR, string, *APIError) {
	zp := dns.NewZoneParser(strings.NewReader(text), "", "")

	var (
		recs []dns.RR
		name string
	)

	for rr, ok := zp.Next(); ok; rr, ok = zp.Next() {
		recs = append(recs, rr)

		if soa, isSOA := rr.(*dns.SOA); isSOA && name == "" {
			name = soa.Hdr.Name
		}
	}

	if err := zp.Err(); err != nil {
		return nil, "", errParam("the zone file could not be parsed: " + err.Error())
	}

	if name == "" {
		return nil, "", errParam("the zone file must contain an SOA record naming the zone")
	}

	n, ok := normName(name, false)
	if !ok {
		return nil, "", errParam("the zone file names an invalid zone")
	}

	return recs, n, nil
}
