package gcpdns

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/miekg/dns"
	dnsapi "google.golang.org/api/dns/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	changePending = "pending"
	changeDone    = "done"
)

func itemPath(base, kind string, i int) string {
	if base != rrsetPathCh {
		return base
	}

	return fmt.Sprintf("%s.%s[%d]", base, kind, i)
}

func dataSize(sets ...[]*dnsapi.ResourceRecordSet) int {
	n := 0

	for _, list := range sets {
		for _, rr := range list {
			for _, d := range rr.Rrdatas {
				n += len(d)
			}
		}
	}

	return n
}

func displayName(rr *dnsapi.ResourceRecordSet) string { return rr.Name + " (" + rr.Type + ")" }

func parseSOA(rr *dnsapi.ResourceRecordSet) *dns.SOA {
	if rr == nil || len(rr.Rrdatas) != 1 {
		return nil
	}

	parsed, err := dns.NewRR(fmt.Sprintf("%s %d IN SOA %s", rr.Name, rr.Ttl, rr.Rrdatas[0]))
	if err != nil {
		return nil
	}

	soa, _ := parsed.(*dns.SOA)

	return soa
}

func soaData(s *dns.SOA) string {
	return fmt.Sprintf("%s %s %d %d %d %d %d", s.Ns, s.Mbox, s.Serial, s.Refresh, s.Retry, s.Expire, s.Minttl)
}

func cnameConflict(work map[string]*dnsapi.ResourceRecordSet, names map[string]bool) string {
	for k, rr := range work {
		if !names[rr.Name] || rr.Type != typeCNAME {
			continue
		}

		for k2, other := range work {
			if k2 != k && other.Name == rr.Name {
				return rr.Name
			}
		}
	}

	return ""
}

func (b *InMemoryBackend) stageDeletions(
	in *dnsapi.Change, work map[string]*dnsapi.ResourceRecordSet, path string,
) ([]*dnsapi.ResourceRecordSet, error) {
	out := make([]*dnsapi.ResourceRecordSet, 0, len(in.Deletions))

	for i, d := range in.Deletions {
		if d == nil {
			return nil, errRequired(itemPath(path, "deletions", i))
		}

		nd := normRRSet(d)
		p := itemPath(path, "deletions", i)

		cur, ok := work[rrKey(nd.Name, nd.Type)]
		if !ok {
			return nil, errNotFound(p, displayName(nd))
		}

		if !sameRRSet(nd, cur) {
			return nil, errPrecondition(p)
		}

		delete(work, rrKey(nd.Name, nd.Type))
		out = append(out, clone(cur))
	}

	return out, nil
}

func (b *InMemoryBackend) stageAdditions(
	rec *zoneRec, in *dnsapi.Change, work map[string]*dnsapi.ResourceRecordSet, path string,
) ([]*dnsapi.ResourceRecordSet, error) {
	out := make([]*dnsapi.ResourceRecordSet, 0, len(in.Additions))

	for i, a := range in.Additions {
		if a == nil {
			return nil, errRequired(itemPath(path, "additions", i))
		}

		na := normRRSet(a)
		p := itemPath(path, "additions", i)

		if err := validateRRSet(rec.Zone, na, p); err != nil {
			return nil, err
		}

		if _, exists := work[rrKey(na.Name, na.Type)]; exists {
			return nil, errExists(p, displayName(na))
		}

		work[rrKey(na.Name, na.Type)] = na
		out = append(out, clone(na))
	}

	return out, nil
}

// applyChange validates and applies in atomically; the caller holds the write lock.
func (b *InMemoryBackend) applyChange(rec *zoneRec, in *dnsapi.Change, path string) (*dnsapi.Change, error) {
	if len(in.Additions) == 0 && len(in.Deletions) == 0 {
		return nil, errInvalid("Invalid value for '%s': a change needs at least one addition or deletion", path)
	}

	if len(in.Additions) > maxChangeOps || len(in.Deletions) > maxChangeOps {
		return nil, errQuota("Quota 'RRSET_ADDITIONS_PER_CHANGE' exceeded.")
	}

	if dataSize(in.Additions, in.Deletions) > maxChangeLen {
		return nil, errQuota("Quota 'TOTAL_RRDATA_SIZE_PER_CHANGE' exceeded.")
	}

	work := make(map[string]*dnsapi.ResourceRecordSet, len(rec.RRSets))
	maps.Copy(work, rec.RRSets)

	dels, err := b.stageDeletions(in, work, path)
	if err != nil {
		return nil, err
	}

	adds, err := b.stageAdditions(rec, in, work, path)
	if err != nil {
		return nil, err
	}

	touched := map[string]bool{}
	for _, rr := range append(slices.Clone(adds), dels...) {
		touched[rr.Name] = true
	}

	if name := cnameConflict(work, touched); name != "" {
		return nil, errInvalid(
			"Invalid value for '%s': a CNAME cannot coexist with other record types at '%s'", path, name)
	}

	if adds, dels, err = b.finishSOA(rec, work, adds, dels, path); err != nil {
		return nil, err
	}

	if len(work) > maxRRSets {
		return nil, errQuota("Quota 'RRSETS_PER_MANAGED_ZONE' exceeded.")
	}

	rec.RRSets = work

	ch := &dnsapi.Change{
		Kind: "dns#change", Id: strconv.Itoa(len(rec.Changes)), StartTime: b.stamp(), Status: changePending,
		IsServing: true, Additions: adds, Deletions: dels,
	}
	rec.Changes = append(rec.Changes, ch)

	return clone(ch), nil
}

// finishSOA keeps the apex SOA and NS present and bumps the SOA serial unless the change replaced the SOA.
func (b *InMemoryBackend) finishSOA(
	rec *zoneRec, work map[string]*dnsapi.ResourceRecordSet, adds, dels []*dnsapi.ResourceRecordSet, path string,
) ([]*dnsapi.ResourceRecordSet, []*dnsapi.ResourceRecordSet, error) {
	apex := rec.Zone.DnsName
	soaKey, nsKey := rrKey(apex, typeSOA), rrKey(apex, typeNS)

	if work[soaKey] == nil || work[nsKey] == nil {
		return nil, nil, errInvalid(
			"Invalid value for '%s': the SOA and NS records at the zone apex cannot be deleted",
			path,
		)
	}

	oldSOA := rec.RRSets[soaKey]
	newSOA := work[soaKey]

	if newSOA != oldSOA {
		o, n := parseSOA(oldSOA), parseSOA(newSOA)
		if o != nil && n != nil && n.Serial <= o.Serial {
			return nil, nil, errInvalid("Invalid value for '%s': the SOA serial must increase", path)
		}

		return adds, dels, nil
	}

	soa := parseSOA(oldSOA)
	if soa == nil {
		return adds, dels, nil
	}

	bumped := clone(oldSOA)
	soa.Serial++
	bumped.Rrdatas = []string{soaData(soa)}
	work[soaKey] = bumped

	return append(adds, clone(bumped)), append(dels, clone(oldSOA)), nil
}

// CreateChange applies a change atomically; it is returned pending and reads as done afterwards.
func (b *InMemoryBackend) CreateChange(project, zone string, in *dnsapi.Change) (*dnsapi.Change, error) {
	b.mu.Lock("CreateChange")
	defer b.mu.Unlock()

	rec, err := b.zoneLocked(project, zone)
	if err != nil {
		return nil, err
	}

	return b.applyChange(rec, in, rrsetPathCh)
}

func settle(rec *zoneRec) {
	for _, c := range rec.Changes {
		c.Status = changeDone
	}
}

// GetChange returns one change.
func (b *InMemoryBackend) GetChange(project, zone, id string) (*dnsapi.Change, error) {
	b.mu.Lock("GetChange")
	defer b.mu.Unlock()

	rec, err := b.zoneLocked(project, zone)
	if err != nil {
		return nil, err
	}

	i, convErr := strconv.Atoi(id)
	if convErr != nil || i < 0 || i >= len(rec.Changes) {
		return nil, errNotFound("parameters.changeId", id)
	}

	settle(rec)

	return clone(rec.Changes[i]), nil
}

// ListChanges returns a page of changes ordered by sequence.
func (b *InMemoryBackend) ListChanges(
	project, zone, sortBy, sortOrder, token string, limit int,
) (page.Page[*dnsapi.Change], error) {
	var none page.Page[*dnsapi.Change]

	if sortBy != "" && sortBy != "changeSequence" {
		return none, errInvalidValue("sortBy", sortBy)
	}

	if sortOrder != "" && !strings.EqualFold(sortOrder, "ascending") && !strings.EqualFold(sortOrder, "descending") {
		return none, errInvalidValue("sortOrder", sortOrder)
	}

	if err := page.ValidateToken(token); err != nil {
		return none, errInvalidValue("pageToken", token)
	}

	b.mu.Lock("ListChanges")
	defer b.mu.Unlock()

	rec, err := b.zoneLocked(project, zone)
	if err != nil {
		return none, err
	}

	settle(rec)

	all := cloneAll(rec.Changes)

	if strings.EqualFold(sortOrder, "descending") {
		slices.Reverse(all)
	}

	return page.New(all, token, limit, pageSize), nil
}

// GetOperation returns one zone operation.
func (b *InMemoryBackend) GetOperation(project, zone, id string) (*dnsapi.Operation, error) {
	b.mu.RLock("GetOperation")
	defer b.mu.RUnlock()

	rec, err := b.zoneLocked(project, zone)
	if err != nil {
		return nil, err
	}

	for _, op := range rec.Ops {
		if op.Id == id {
			return clone(op), nil
		}
	}

	return nil, errNotFound("parameters.operation", id)
}

// ListOperations returns a page of zone operations ordered by start time or id.
func (b *InMemoryBackend) ListOperations(
	project, zone, sortBy, token string, limit int,
) (page.Page[*dnsapi.Operation], error) {
	var none page.Page[*dnsapi.Operation]

	if sortBy != "" && sortBy != "startTime" && sortBy != "id" {
		return none, errInvalidValue("sortBy", sortBy)
	}

	if err := page.ValidateToken(token); err != nil {
		return none, errInvalidValue("pageToken", token)
	}

	b.mu.RLock("ListOperations")
	defer b.mu.RUnlock()

	rec, err := b.zoneLocked(project, zone)
	if err != nil {
		return none, err
	}

	all := cloneAll(rec.Ops)
	if sortBy == "id" {
		slices.SortStableFunc(all, func(a, c *dnsapi.Operation) int { return strings.Compare(a.Id, c.Id) })
	}

	return page.New(all, token, limit, pageSize), nil
}
