package servicequotas

import (
	"cmp"
	"context"
	"slices"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	requestIDHexLen = 32
	caseIDBase      = 170000000000
)

// RequestView is a quota increase request with the quota it targets.
type RequestView struct {
	Service *ServiceDef
	Def     *QuotaDef
	ARN     string
	Request Request
}

// settleLocked advances every request and report whose dwell time has elapsed.
func (b *InMemoryBackend) settleLocked() {
	now := b.now()
	dwell := b.dwell()

	reqs := b.requests.All()
	slices.SortFunc(reqs, func(a, c *Request) int { return cmp.Compare(a.Seq, c.Seq) })

	for _, r := range reqs {
		if r.Status == statusPending && r.CaseAllowed && !now.Before(r.Created.Add(dwell)) {
			b.openCaseLocked(r, r.Created.Add(dwell))
		}

		if r.Status == statusCaseOpened && !now.Before(r.CaseOpenedAt.Add(dwell)) {
			b.approveLocked(r, r.CaseOpenedAt.Add(dwell))
		}
	}

	b.settleReportsLocked(now, dwell)
}

func (b *InMemoryBackend) openCaseLocked(r *Request, at time.Time) {
	r.Status = statusCaseOpened
	r.CaseID = strconv.FormatInt(caseIDBase+r.Seq, 10)
	r.CaseOpenedAt = at
	r.LastUpdated = at
}

func (b *InMemoryBackend) approveLocked(r *Request, at time.Time) {
	r.LastUpdated = at

	if _, def, ok := b.catalog.Quota(r.ServiceCode, r.QuotaCode); ok && def.Max > 0 && r.DesiredValue > def.Max {
		r.Status = statusDenied

		return
	}

	r.Status = statusApproved

	s := scope{account: r.Account, region: r.Region}
	key := appliedKey(s, r.ServiceCode, r.QuotaCode)

	b.applied.Put(&AppliedValue{
		Key: key, Account: r.Account, Region: r.Region,
		ServiceCode: r.ServiceCode, QuotaCode: r.QuotaCode, Value: r.DesiredValue,
	})
}

func (b *InMemoryBackend) requestViewLocked(r *Request) RequestView {
	svc, def, _ := b.catalog.Quota(r.ServiceCode, r.QuotaCode)

	return RequestView{
		Service: svc, Def: def, Request: *r,
		ARN: scope{account: r.Account, region: r.Region}.quotaARN(r.ServiceCode, r.QuotaCode),
	}
}

func requesterOf(ctx context.Context, account string) string {
	if a := awsmeta.CallerArn(ctx); a != "" {
		return a
	}

	return arn.Build("iam", "", account, "root")
}

// RequestIncrease files a quota increase request.
func (b *InMemoryBackend) RequestIncrease(
	ctx context.Context, serviceCode, quotaCode string, desired *float64, contextID string, caseAllowed *bool,
) (RequestView, error) {
	if desired == nil {
		return RequestView{}, fail(ErrIllegalArgument, "DesiredValue is required")
	}

	if err := checkContext(contextID); err != nil {
		return RequestView{}, err
	}

	b.mu.Lock("RequestServiceQuotaIncrease")
	defer b.mu.Unlock()

	b.settleLocked()

	_, def, err := b.lookupQuotaLocked(serviceCode, quotaCode)
	if err != nil {
		return RequestView{}, err
	}

	s := b.scopeOf(ctx)

	if !def.Adjustable {
		return RequestView{}, fail(ErrIllegalArgument, "The quota %q is not adjustable", quotaCode)
	}

	if cur, _ := b.appliedValueLocked(s, serviceCode, def); *desired <= cur {
		return RequestView{}, fail(ErrIllegalArgument,
			"DesiredValue %v must be greater than the current value %v", *desired, cur)
	}

	if b.openRequestLocked(s, serviceCode, quotaCode) != nil {
		return RequestView{}, fail(ErrResourceAlreadyExists,
			"A request to increase quota %q is already in progress", quotaCode)
	}

	now := b.now()
	r := &Request{
		ID:           awsid.NewN("", requestIDHexLen),
		Account:      s.account,
		Region:       s.region,
		ServiceCode:  serviceCode,
		QuotaCode:    quotaCode,
		DesiredValue: *desired,
		Status:       statusPending,
		Requester:    requesterOf(ctx, s.account),
		Created:      now,
		LastUpdated:  now,
		CaseAllowed:  caseAllowed == nil || *caseAllowed,
		Seq:          b.nextSeqLocked(),
	}
	b.requests.Put(r)

	return b.requestViewLocked(r), nil
}

func (b *InMemoryBackend) openRequestLocked(s scope, serviceCode, quotaCode string) *Request {
	for _, r := range b.requests.All() {
		if r.Account == s.account && r.Region == s.region && r.ServiceCode == serviceCode &&
			r.QuotaCode == quotaCode && (r.Status == statusPending || r.Status == statusCaseOpened) {
			return r
		}
	}

	return nil
}

// CreateSupportCase opens a support case for a pending request.
func (b *InMemoryBackend) CreateSupportCase(ctx context.Context, requestID string) error {
	b.mu.Lock("CreateSupportCase")
	defer b.mu.Unlock()

	b.settleLocked()

	r, err := b.requestLocked(b.scopeOf(ctx), requestID)
	if err != nil {
		return err
	}

	if r.Status != statusPending {
		return fail(
			ErrInvalidResourceState,
			"A support case can only be created for a request in the %s status",
			statusPending,
		)
	}

	b.openCaseLocked(r, b.now())

	return nil
}

func (b *InMemoryBackend) requestLocked(s scope, id string) (*Request, error) {
	r, ok := b.requests.Get(id)
	if !ok || r.Account != s.account || r.Region != s.region {
		return nil, fail(ErrNoSuchResource, "The request %q was not found", id)
	}

	return r, nil
}

// GetRequest returns one request.
func (b *InMemoryBackend) GetRequest(ctx context.Context, id string) (RequestView, error) {
	if id == "" {
		return RequestView{}, fail(ErrIllegalArgument, "RequestId is required")
	}

	b.mu.Lock("GetRequestedServiceQuotaChange")
	defer b.mu.Unlock()

	b.settleLocked()

	r, err := b.requestLocked(b.scopeOf(ctx), id)
	if err != nil {
		return RequestView{}, err
	}

	return b.requestViewLocked(r), nil
}

// RequestFilter narrows a request history listing.
type RequestFilter struct {
	ServiceCode string
	QuotaCode   string
	Status      string
	Level       string
}

var requestStatuses = []string{ //nolint:gochecknoglobals // immutable enum
	statusPending, statusCaseOpened, statusApproved, statusDenied, "CASE_CLOSED", statusNotApproved, "INVALID_REQUEST",
}

// ListRequests returns the request history, oldest first.
func (b *InMemoryBackend) ListRequests(
	ctx context.Context, f RequestFilter, token string, maxResults *int32,
) (page.Page[RequestView], error) {
	if f.Status != "" && !slices.Contains(requestStatuses, f.Status) {
		return page.Page[RequestView]{}, fail(ErrIllegalArgument, "Status %q is not valid", f.Status)
	}

	if err := validateLevel(f.Level); err != nil {
		return page.Page[RequestView]{}, err
	}

	b.mu.Lock("ListRequestedServiceQuotaChangeHistory")
	defer b.mu.Unlock()

	b.settleLocked()

	if err := b.checkFilterTargetLocked(f); err != nil {
		return page.Page[RequestView]{}, err
	}

	s := b.scopeOf(ctx)
	rows := make([]*Request, 0)

	for _, r := range b.requests.All() {
		if r.Account == s.account && r.Region == s.region && f.Level != levelResource && f.matches(r) {
			rows = append(rows, r)
		}
	}

	slices.SortFunc(rows, func(a, c *Request) int { return cmp.Compare(a.Seq, c.Seq) })

	views := make([]RequestView, len(rows))
	for i, r := range rows {
		views[i] = b.requestViewLocked(r)
	}

	return paginate(views, token, maxResults)
}

func (f RequestFilter) matches(r *Request) bool {
	return (f.ServiceCode == "" || r.ServiceCode == f.ServiceCode) &&
		(f.QuotaCode == "" || r.QuotaCode == f.QuotaCode) &&
		(f.Status == "" || r.Status == f.Status)
}

func (b *InMemoryBackend) checkFilterTargetLocked(f RequestFilter) error {
	if f.ServiceCode != "" {
		if _, ok := b.catalog.Service(f.ServiceCode); !ok {
			return fail(ErrNoSuchResource, "The service %q was not found", f.ServiceCode)
		}
	}

	if f.QuotaCode != "" {
		if _, _, err := b.lookupQuotaLocked(f.ServiceCode, f.QuotaCode); err != nil {
			return err
		}
	}

	return nil
}
