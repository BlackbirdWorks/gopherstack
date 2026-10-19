package connect

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/google/uuid"
)

const (
	phoneTollFree   = "TOLL_FREE"
	phoneDID        = "DID"
	poolPerAreaCode = 100
	poolBase        = 5550100
	maxSearchPage   = 10
	statusClaimed   = "CLAIMED"
)

type countrySpec struct {
	dial      string
	didAreas  []string
	tollAreas []string
}

var poolCountries = map[string]countrySpec{ //nolint:gochecknoglobals // immutable pool definition
	"US": {
		"1",
		[]string{"206", "212", "415", "512", "617", "702"},
		[]string{"800", "833", "844", "855", "866", "877", "888"},
	},
	"CA": {"1", []string{"416", "514", "604"}, []string{"800", "833", "844", "855", "866", "877", "888"}},
	"GB": {"44", []string{"20", "121", "161"}, []string{"800"}},
	"DE": {"49", []string{"30", "69", "89"}, []string{"800"}},
	"AU": {"61", []string{"2", "3", "7"}, []string{"1800"}},
	"JP": {"81", []string{"3", "6"}, []string{"120"}},
	"FR": {"33", []string{"1", "4"}, []string{"800"}},
}

type poolEntry struct{ country, kind string }

// phonePool lists every claimable number in a stable order, keyed by E.164 number.
var phonePool = sync.OnceValues(func() (map[string]poolEntry, []string) { //nolint:gochecknoglobals // immutable pool
	byNumber := map[string]poolEntry{}

	var ordered []string

	for _, cc := range slices.Sorted(mapKeys(poolCountries)) {
		spec := poolCountries[cc]

		for _, kind := range []string{phoneDID, phoneTollFree} {
			areas := spec.didAreas
			if kind == phoneTollFree {
				areas = spec.tollAreas
			}

			for _, area := range areas {
				for i := range poolPerAreaCode {
					n := fmt.Sprintf("+%s%s%d", spec.dial, area, poolBase+i)
					byNumber[n] = poolEntry{country: cc, kind: kind}
					ordered = append(ordered, n)
				}
			}
		}
	}

	return byNumber, ordered
})

func mapKeys[K comparable, V any](m map[K]V) func(yield func(K) bool) {
	return func(yield func(K) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

// ClaimPhoneNumberInput is the ClaimPhoneNumber request.
type ClaimPhoneNumberInput struct {
	Tags                   map[string]string
	PhoneNumber            string
	TargetArn              string
	InstanceID             string `json:"InstanceId"`
	PhoneNumberDescription string
	ClientToken            string
}

// SearchAvailablePhoneNumbersInput is the SearchAvailablePhoneNumbers request.
type SearchAvailablePhoneNumbersInput struct {
	PhoneNumberCountryCode string
	PhoneNumberType        string
	PhoneNumberPrefix      string
	TargetArn              string
	InstanceID             string `json:"InstanceId"`
	NextToken              string
	MaxResults             int32
}

// UpdatePhoneNumberInput is the UpdatePhoneNumber and UpdatePhoneNumberMetadata request.
type UpdatePhoneNumberInput struct {
	TargetArn              string
	InstanceID             string `json:"InstanceId"`
	PhoneNumberDescription *string
	ClientToken            string
	PhoneNumberID          string `json:"-"          path:"PhoneNumberId"`
}

// PhoneFlowInput is the (Dis)AssociatePhoneNumberContactFlow request.
type PhoneFlowInput struct {
	ContactFlowID   string `json:"ContactFlowId"`
	InstanceID      string `json:"InstanceId"`
	InstanceIDQuery string `json:"-"             query:"instanceId"`
	PhoneNumberID   string `json:"-"                                path:"PhoneNumberId"`
}

// ListPhoneNumbersV2Input is the ListPhoneNumbersV2 request.
type ListPhoneNumbersV2Input struct {
	TargetArn               string
	InstanceID              string `json:"InstanceId"`
	PhoneNumberPrefix       string
	NextToken               string
	PhoneNumberCountryCodes []string
	PhoneNumberTypes        []string
	MaxResults              int32
}

func (b *Backend) claimedNumbersLocked() map[string]bool {
	claimed := map[string]bool{}
	for _, p := range b.phoneNumbers.All() {
		claimed[p.Phone.PhoneNumber] = true
	}

	return claimed
}

// SearchAvailablePhoneNumbers lists unclaimed numbers of the pool.
func (b *Backend) SearchAvailablePhoneNumbers(in *SearchAvailablePhoneNumbersInput) (*searchNumbersOut, error) {
	if _, ok := poolCountries[in.PhoneNumberCountryCode]; !ok {
		return nil, invalidParamErr("PhoneNumberCountryCode %q has no numbers available", in.PhoneNumberCountryCode)
	}

	if err := oneOf("PhoneNumberType", in.PhoneNumberType, phoneTollFree, phoneDID); err != nil {
		return nil, err
	}

	if in.MaxResults < 0 || in.MaxResults > maxSearchPage {
		return nil, invalidParamErr("MaxResults must be between 1 and %d", maxSearchPage)
	}

	b.mu.RLock("SearchAvailablePhoneNumbers")
	defer b.mu.RUnlock()

	if err := b.checkPhoneTargetLocked(in.TargetArn, in.InstanceID); err != nil {
		return nil, err
	}

	byNumber, ordered := phonePool()
	claimed := b.claimedNumbersLocked()

	var avail []AvailableNumberSummary

	for _, n := range ordered {
		e := byNumber[n]
		if e.country == in.PhoneNumberCountryCode && e.kind == in.PhoneNumberType && !claimed[n] &&
			strings.HasPrefix(n, in.PhoneNumberPrefix) {
			avail = append(
				avail,
				AvailableNumberSummary{PhoneNumber: n, PhoneNumberCountryCode: e.country, PhoneNumberType: e.kind},
			)
		}
	}

	p, err := restroutePaginate(avail, in.NextToken, in.MaxResults, maxSearchPage, maxSearchPage)
	if err != nil {
		return nil, err
	}

	return &searchNumbersOut{AvailableNumbersList: orEmpty(p.Data), NextToken: p.Next}, nil
}

func (b *Backend) checkPhoneTargetLocked(targetARN, instanceRef string) error {
	ref := targetARN
	if ref == "" {
		ref = instanceRef
	}

	if ref == "" {
		return invalidParamErr("TargetArn or InstanceId is required")
	}

	_, err := b.instanceLocked(ref)

	return err
}

// ClaimPhoneNumber claims a number of the pool for an instance.
func (b *Backend) ClaimPhoneNumber(ctx context.Context, in *ClaimPhoneNumberInput) (*phoneOut, error) {
	if err := firstError(
		checkDescription(in.PhoneNumberDescription),
		checkTags(in.Tags),
	); err != nil {
		return nil, err
	}

	b.mu.Lock("ClaimPhoneNumber")
	defer b.mu.Unlock()

	r, err := b.instanceByTargetLocked(in.TargetArn, in.InstanceID)
	if err != nil {
		return nil, err
	}

	byNumber, _ := phonePool()

	entry, ok := byNumber[in.PhoneNumber]
	if !ok {
		return nil, invalidParamErr("PhoneNumber %q is not available to claim", in.PhoneNumber)
	}

	if b.claimedNumbersLocked()[in.PhoneNumber] {
		return nil, inUseErr("PhoneNumber %q is already claimed", in.PhoneNumber)
	}

	id := uuid.NewString()
	rec := &phoneNumberRec{InstanceID: r.Instance.ID, Phone: ClaimedPhoneNumberSummary{
		PhoneNumberStatus: &PhoneNumberStatus{
			Status: statusClaimed,
		},
		Tags:                   cloneTags(in.Tags),
		InstanceID:             r.Instance.ID,
		PhoneNumber:            in.PhoneNumber,
		PhoneNumberArn:         b.buildARN(ctx, "phone-number/"+id),
		PhoneNumberCountryCode: entry.country,
		PhoneNumberDescription: in.PhoneNumberDescription,
		PhoneNumberID:          id,
		PhoneNumberType:        entry.kind,
		TargetArn:              r.Instance.Arn,
	}}
	b.phoneNumbers.Put(rec)

	return &phoneOut{PhoneNumberArn: rec.Phone.PhoneNumberArn, PhoneNumberID: id}, nil
}

func (b *Backend) instanceByTargetLocked(targetARN, instanceRef string) (*instanceRec, error) {
	if err := b.checkPhoneTargetLocked(targetARN, instanceRef); err != nil {
		return nil, err
	}

	ref := targetARN
	if ref == "" {
		ref = instanceRef
	}

	return b.instanceLocked(ref)
}

func (b *Backend) phoneLocked(id string) (*phoneNumberRec, error) {
	if p, ok := b.phoneNumbers.Get(id); ok {
		return p, nil
	}

	return nil, notFoundErr("PhoneNumber", id)
}

// DescribePhoneNumber returns a claimed number.
func (b *Backend) DescribePhoneNumber(id string) (*ClaimedPhoneNumberSummary, error) {
	b.mu.RLock("DescribePhoneNumber")
	defer b.mu.RUnlock()

	p, err := b.phoneLocked(id)
	if err != nil {
		return nil, err
	}

	out := p.Phone
	out.Tags = cloneTags(out.Tags)

	return &out, nil
}

// UpdatePhoneNumber moves a claimed number to another instance, dropping its flow association.
func (b *Backend) UpdatePhoneNumber(in *UpdatePhoneNumberInput) (*phoneOut, error) {
	b.mu.Lock("UpdatePhoneNumber")
	defer b.mu.Unlock()

	p, err := b.phoneLocked(in.PhoneNumberID)
	if err != nil {
		return nil, err
	}

	r, err := b.instanceByTargetLocked(in.TargetArn, in.InstanceID)
	if err != nil {
		return nil, err
	}

	if r.Instance.ID != p.InstanceID {
		p.FlowID = ""
	}

	p.InstanceID, p.Phone.InstanceID, p.Phone.TargetArn = r.Instance.ID, r.Instance.ID, r.Instance.Arn

	return &phoneOut{PhoneNumberArn: p.Phone.PhoneNumberArn, PhoneNumberID: p.Phone.PhoneNumberID}, nil
}

// UpdatePhoneNumberMetadata changes the description of a claimed number.
func (b *Backend) UpdatePhoneNumberMetadata(in *UpdatePhoneNumberInput) (*empty, error) {
	if in.PhoneNumberDescription == nil {
		return nil, invalidParamErr("PhoneNumberDescription is required")
	}

	if err := checkDescription(*in.PhoneNumberDescription); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdatePhoneNumberMetadata")
	defer b.mu.Unlock()

	p, err := b.phoneLocked(in.PhoneNumberID)
	if err != nil {
		return nil, err
	}

	p.Phone.PhoneNumberDescription = *in.PhoneNumberDescription

	return &empty{}, nil
}

// ReleasePhoneNumber returns a number to the pool.
func (b *Backend) ReleasePhoneNumber(id string) (*empty, error) {
	b.mu.Lock("ReleasePhoneNumber")
	defer b.mu.Unlock()

	p, err := b.phoneLocked(id)
	if err != nil {
		return nil, err
	}

	for _, q := range b.queues.in(p.InstanceID) {
		if c := q.Queue.OutboundCallerConfig; c != nil && c.OutboundCallerIDNumberID == id {
			return nil, inUseErr("PhoneNumber %q is the caller ID of queue %q", id, q.Queue.Name)
		}
	}

	b.phoneNumbers.Delete(id)

	return &empty{}, nil
}

func (b *Backend) filteredPhones(instanceID string, types, countries []string, prefix string) []*phoneNumberRec {
	var out []*phoneNumberRec

	for _, p := range b.phoneNumbers.All() {
		switch {
		case instanceID != "" && p.InstanceID != instanceID:
		case len(types) > 0 && !slices.Contains(types, p.Phone.PhoneNumberType):
		case len(countries) > 0 && !slices.Contains(countries, p.Phone.PhoneNumberCountryCode):
		case !strings.HasPrefix(p.Phone.PhoneNumber, prefix):
		default:
			out = append(out, p)
		}
	}

	slices.SortFunc(
		out,
		func(a, c *phoneNumberRec) int { return strings.Compare(a.Phone.PhoneNumber, c.Phone.PhoneNumber) },
	)

	return out
}

// ListPhoneNumbers pages through the numbers claimed by one instance.
func (b *Backend) ListPhoneNumbers(
	inst string,
	types, countries []string,
	token string,
	maxResults int32,
) ([]PhoneNumberSummary, string, error) {
	b.mu.RLock("ListPhoneNumbers")

	r, err := b.instanceLocked(inst)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []PhoneNumberSummary
	for _, p := range b.filteredPhones(r.Instance.ID, types, countries, "") {
		all = append(all, PhoneNumberSummary{
			Arn: p.Phone.PhoneNumberArn, ID: p.Phone.PhoneNumberID, PhoneNumber: p.Phone.PhoneNumber,
			PhoneNumberCountryCode: p.Phone.PhoneNumberCountryCode, PhoneNumberType: p.Phone.PhoneNumberType,
		})
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

// ListPhoneNumbersV2 pages through claimed numbers, optionally scoped to a target.
func (b *Backend) ListPhoneNumbersV2(in *ListPhoneNumbersV2Input) (*listPhonesOut, error) {
	b.mu.RLock("ListPhoneNumbersV2")

	scope := ""

	if in.TargetArn != "" || in.InstanceID != "" {
		r, err := b.instanceByTargetLocked(in.TargetArn, in.InstanceID)
		if err != nil {
			b.mu.RUnlock()

			return nil, err
		}

		scope = r.Instance.ID
	}

	var all []ListPhoneNumbersSummary

	for _, p := range b.filteredPhones(scope, in.PhoneNumberTypes, in.PhoneNumberCountryCodes, in.PhoneNumberPrefix) {
		all = append(all, ListPhoneNumbersSummary{
			InstanceID:             p.Phone.InstanceID,
			PhoneNumber:            p.Phone.PhoneNumber,
			PhoneNumberArn:         p.Phone.PhoneNumberArn,
			PhoneNumberCountryCode: p.Phone.PhoneNumberCountryCode,
			PhoneNumberDescription: p.Phone.PhoneNumberDescription,
			PhoneNumberID:          p.Phone.PhoneNumberID,
			PhoneNumberType:        p.Phone.PhoneNumberType,
			TargetArn:              p.Phone.TargetArn,
		})
	}

	b.mu.RUnlock()

	items, next, err := paged(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listPhonesOut{ListPhoneNumbersSummaryList: orEmpty(items), NextToken: next}, nil
}

// AssociatePhoneNumberContactFlow routes calls to a number through a contact flow.
func (b *Backend) AssociatePhoneNumberContactFlow(in *PhoneFlowInput) (*empty, error) {
	b.mu.Lock("AssociatePhoneNumberContactFlow")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	p, err := b.phoneLocked(in.PhoneNumberID)
	if err != nil {
		return nil, err
	}

	if p.InstanceID != r.Instance.ID {
		return nil, invalidParamErr("PhoneNumber %q is not claimed by instance %q", in.PhoneNumberID, in.InstanceID)
	}

	if !b.flows.has(r.Instance.ID, in.ContactFlowID) {
		return nil, notFoundErr("ContactFlow", in.ContactFlowID)
	}

	p.FlowID = in.ContactFlowID

	return &empty{}, nil
}

// DisassociatePhoneNumberContactFlow removes the flow of a number.
func (b *Backend) DisassociatePhoneNumberContactFlow(in *PhoneFlowInput) (*empty, error) {
	b.mu.Lock("DisassociatePhoneNumberContactFlow")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceIDQuery)
	if err != nil {
		return nil, err
	}

	p, err := b.phoneLocked(in.PhoneNumberID)
	if err != nil {
		return nil, err
	}

	if p.InstanceID != r.Instance.ID {
		return nil, invalidParamErr(
			"PhoneNumber %q is not claimed by instance %q",
			in.PhoneNumberID,
			in.InstanceIDQuery,
		)
	}

	p.FlowID = ""

	return &empty{}, nil
}
