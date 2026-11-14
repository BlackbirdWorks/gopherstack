package gcpresourcemanager

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	kindTagHolds    = "tagHolds"
	maxHoldText     = 200
	maxHoldPage     = 300
	defaultHoldPage = 100
)

type holdRec struct {
	Created  time.Time `json:"created"`
	ID       string    `json:"id"`
	Parent   string    `json:"parent"`
	Holder   string    `json:"holder"`
	Origin   string    `json:"origin,omitempty"`
	HelpLink string    `json:"helpLink,omitempty"`
}

func (h *holdRec) view() *resourcemanagerpb.TagHold {
	return &resourcemanagerpb.TagHold{
		Name:   h.Parent + "/" + kindTagHolds + "/" + h.ID,
		Holder: h.Holder, Origin: h.Origin, HelpLink: h.HelpLink, CreateTime: ts(h.Created),
	}
}

// CreateTagHold records a use of a tag value that no tag binding captures.
func (b *InMemoryBackend) CreateTagHold(in *resourcemanagerpb.CreateTagHoldRequest) (*longrunningpb.Operation, error) {
	hold := in.GetTagHold()
	if hold.GetHolder() == "" {
		return nil, errInvalid("tag_hold.holder must be specified.")
	}

	if len(hold.GetHolder()) >= maxHoldText || len(hold.GetOrigin()) >= maxHoldText {
		return nil, errInvalid("tag_hold.holder and tag_hold.origin must be less than %d characters.", maxHoldText)
	}

	b.mu.Lock("CreateTagHold")
	defer b.mu.Unlock()

	b.purgeLocked()

	v, err := b.tagValueLocked(in.GetParent())
	if err != nil {
		return nil, err
	}

	for _, h := range b.holds {
		if h.Parent == in.GetParent() && h.Holder == hold.GetHolder() && h.Origin == hold.GetOrigin() {
			return nil, errExists("Requested entity already exists: a tag hold for holder %s and origin %q.",
				hold.GetHolder(), hold.GetOrigin())
		}
	}

	id := uniqueDigits(tagIDDigits, func(id string) bool {
		_, taken := b.holds[v.ID+"/"+id]

		return taken
	})
	rec := &holdRec{
		ID:     id,
		Parent: kindTagValues + "/" + v.ID, Holder: hold.GetHolder(), Origin: hold.GetOrigin(),
		HelpLink: hold.GetHelpLink(), Created: b.now(),
	}

	if !in.GetValidateOnly() {
		b.holds[v.ID+"/"+rec.ID] = rec
	}

	return b.opLocked("ch", &resourcemanagerpb.CreateTagHoldMetadata{}, rec.view())
}

// DeleteTagHold removes a tag hold.
func (b *InMemoryBackend) DeleteTagHold(in *resourcemanagerpb.DeleteTagHoldRequest) (*longrunningpb.Operation, error) {
	b.mu.Lock("DeleteTagHold")
	defer b.mu.Unlock()

	b.purgeLocked()

	key, ok := holdKey(in.GetName())
	if !ok {
		return nil, errInvalid("Invalid tag hold name %q: use tagValues/ID/tagHolds/ID.", in.GetName())
	}

	rec, found := b.holds[key]
	if !found {
		return nil, errDenied("resourcemanager.tagHolds.delete", in.GetName())
	}

	if !in.GetValidateOnly() {
		delete(b.holds, key)
	}

	return b.opLocked("dh", &resourcemanagerpb.DeleteTagHoldMetadata{}, rec.view())
}

func holdKey(name string) (string, bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != kindTagValues || parts[2] != kindTagHolds || parts[1] == "" || parts[3] == "" {
		return "", false
	}

	return parts[1] + "/" + parts[3], true
}

// ListTagHolds lists the holds of a tag value; the filter takes holder and origin terms joined by AND.
func (b *InMemoryBackend) ListTagHolds(in *resourcemanagerpb.ListTagHoldsRequest) (
	*resourcemanagerpb.ListTagHoldsResponse, error,
) {
	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	filter, err := parseHoldFilter(in.GetFilter())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("ListTagHolds")
	defer b.mu.Unlock()

	b.purgeLocked()

	if _, err = b.tagValueLocked(in.GetParent()); err != nil {
		return nil, err
	}

	var hits []*holdRec

	for _, h := range b.holds {
		if h.Parent == in.GetParent() && filter.matches(h) {
			hits = append(hits, h)
		}
	}

	slices.SortFunc(hits, func(a, c *holdRec) int { return cmp.Compare(a.ID, c.ID) })

	views := make([]*resourcemanagerpb.TagHold, 0, len(hits))
	for _, h := range hits {
		views = append(views, h.view())
	}

	size := min(int(in.GetPageSize()), maxHoldPage)

	pg := page.New(views, in.GetPageToken(), size, defaultHoldPage)

	return &resourcemanagerpb.ListTagHoldsResponse{TagHolds: pg.Data, NextPageToken: pg.Next}, nil
}

type holdFilter struct {
	holder *string
	origin *string
}

// parseHoldFilter reads `holder = X [AND origin = Y]` in either order, as the ListTagHoldsRequest.filter comment shows.
func parseHoldFilter(filter string) (holdFilter, error) {
	var f holdFilter

	if strings.TrimSpace(filter) == "" {
		return f, nil
	}

	for term := range strings.SplitSeq(filter, " AND ") {
		field, value, ok := strings.Cut(term, "=")
		if !ok {
			return f, errInvalid("Invalid filter %q: use holder = VALUE and origin = VALUE terms.", filter)
		}

		v := strings.Trim(strings.TrimSpace(value), `"`)

		switch strings.TrimSpace(field) {
		case "holder":
			f.holder = &v
		case "origin":
			f.origin = &v
		default:
			return f, errInvalid("Invalid filter %q: only holder and origin can be filtered.", filter)
		}
	}

	return f, nil
}

func (f holdFilter) matches(h *holdRec) bool {
	return (f.holder == nil || h.Holder == *f.holder) && (f.origin == nil || h.Origin == *f.origin)
}

func (b *InMemoryBackend) holdsOfLocked(tagValueID string) bool {
	for k := range b.holds {
		if strings.HasPrefix(k, tagValueID+"/") {
			return true
		}
	}

	return false
}
