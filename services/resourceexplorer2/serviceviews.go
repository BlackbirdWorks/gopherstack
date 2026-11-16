package resourceexplorer2

import (
	"cmp"
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxServiceViewsPerAccount = 50
	maxStreamingPerAccount    = 50
	maxServicePrincipalLen    = 256
	serviceViewResourcePrefix = "service-view/"
)

// ServiceView is a view an AWS service owns on behalf of an account in one Region.
type ServiceView struct {
	ARN                string   `json:"arn"`
	Name               string   `json:"name"`
	AccountID          string   `json:"accountID"`
	Region             string   `json:"region"`
	FilterString       string   `json:"filterString,omitempty"`
	ScopeType          string   `json:"scopeType,omitempty"`
	StreamingService   string   `json:"streamingService,omitempty"`
	RecorderName       string   `json:"recorderName,omitempty"`
	RecorderType       string   `json:"recorderType,omitempty"`
	RecorderPrincipal  string   `json:"recorderPrincipal,omitempty"`
	IncludedProperties []string `json:"includedProperties,omitempty"`
}

// StreamingAccess records that a service principal may stream an account's index data.
type StreamingAccess struct {
	CreatedAt        time.Time `json:"createdAt"`
	Key              string    `json:"key"`
	AccountID        string    `json:"accountID"`
	ServicePrincipal string    `json:"servicePrincipal"`
}

// ServiceLinkedRecorderWire is the ServiceLinkedRecorderInfo shape.
type ServiceLinkedRecorderWire struct {
	RecorderName     string `json:"RecorderName,omitempty"`
	RecorderType     string `json:"RecorderType,omitempty"`
	ServicePrincipal string `json:"ServicePrincipal,omitempty"`
}

// ServiceViewWire is the ServiceView shape.
type ServiceViewWire struct {
	Filters                   *SearchFilter              `json:"Filters,omitempty"`
	ServiceLinkedRecorder     *ServiceLinkedRecorderWire `json:"ServiceLinkedRecorder,omitempty"`
	ScopeType                 string                     `json:"ScopeType,omitempty"`
	ServiceViewArn            string                     `json:"ServiceViewArn"`
	ServiceViewName           string                     `json:"ServiceViewName,omitempty"`
	StreamingAccessForService string                     `json:"StreamingAccessForService,omitempty"`
	IncludedProperties        []IncludedProperty         `json:"IncludedProperties,omitempty"`
}

// StreamingAccessWire is the StreamingAccessDetails shape.
type StreamingAccessWire struct {
	CreatedAt        time.Time `json:"CreatedAt"`
	ServicePrincipal string    `json:"ServicePrincipal"`
}

func streamingKey(account, principal string) string {
	return strconv.Itoa(len(account)) + ":" + account + principal
}

func (v *ServiceView) wire() ServiceViewWire {
	w := ServiceViewWire{
		ScopeType: v.ScopeType, ServiceViewArn: v.ARN, ServiceViewName: v.Name,
		StreamingAccessForService: v.StreamingService,
	}

	if v.FilterString != "" {
		w.Filters = &SearchFilter{FilterString: v.FilterString}
	}

	for _, p := range v.IncludedProperties {
		w.IncludedProperties = append(w.IncludedProperties, IncludedProperty{Name: p})
	}

	if v.RecorderName != "" || v.RecorderType != "" || v.RecorderPrincipal != "" {
		w.ServiceLinkedRecorder = &ServiceLinkedRecorderWire{
			RecorderName: v.RecorderName, RecorderType: v.RecorderType, ServicePrincipal: v.RecorderPrincipal,
		}
	}

	return w
}

// PutServiceView registers a view an AWS service created for an account; it is the entry point for emulated
// services that own Resource Explorer views. The ARN is minted from the name when v.ARN is empty.
func (b *InMemoryBackend) PutServiceView(v ServiceView) (string, error) {
	if v.Name == "" || v.AccountID == "" || v.Region == "" {
		return "", invalid("a service view needs a name, an account and a Region")
	}

	if _, err := validateFilter(&SearchFilter{FilterString: v.FilterString}); err != nil {
		return "", err
	}

	if _, err := validateProperties(toProps(v.IncludedProperties)); err != nil {
		return "", err
	}

	b.mu.Lock("PutServiceView")
	defer b.mu.Unlock()

	if v.ARN == "" {
		v.ARN = arn.Build(signingName, v.Region, v.AccountID, serviceViewResourcePrefix+v.Name)
	}

	if _, exists := b.serviceViews.Get(
		v.ARN,
	); !exists &&
		b.countServiceViewsLocked(v.AccountID) >= maxServiceViewsPerAccount {
		return "", quotaExceeded("at most %d service views are allowed per account", maxServiceViewsPerAccount)
	}

	cp := v
	b.serviceViews.Put(&cp)

	return v.ARN, nil
}

// GrantStreamingAccess records that service principal may stream the account's Resource Explorer data.
func (b *InMemoryBackend) GrantStreamingAccess(account, principal string) error {
	if account == "" || principal == "" || len(principal) > maxServicePrincipalLen {
		return invalid("streaming access needs an account and a service principal of at most %d characters",
			maxServicePrincipalLen)
	}

	b.mu.Lock("GrantStreamingAccess")
	defer b.mu.Unlock()

	key := streamingKey(account, principal)
	if _, ok := b.streaming.Get(key); ok {
		return nil
	}

	if b.countStreamingLocked(account) >= maxStreamingPerAccount {
		return quotaExceeded("at most %d services may have streaming access per account", maxStreamingPerAccount)
	}

	b.streaming.Put(&StreamingAccess{
		Key: key, AccountID: account, ServicePrincipal: principal, CreatedAt: b.now(),
	})

	return nil
}

// RevokeStreamingAccess removes a grant; it reports whether one existed.
func (b *InMemoryBackend) RevokeStreamingAccess(account, principal string) bool {
	b.mu.Lock("RevokeStreamingAccess")
	defer b.mu.Unlock()

	key := streamingKey(account, principal)
	if !b.streaming.Has(key) {
		return false
	}

	b.streaming.Delete(key)

	return true
}

func toProps(names []string) []IncludedProperty {
	out := make([]IncludedProperty, 0, len(names))
	for _, n := range names {
		out = append(out, IncludedProperty{Name: n})
	}

	return out
}

func (b *InMemoryBackend) countServiceViewsLocked(account string) int {
	n := 0

	for _, v := range b.serviceViews.All() {
		if v.AccountID == account {
			n++
		}
	}

	return n
}

func (b *InMemoryBackend) countStreamingLocked(account string) int {
	n := 0

	for _, s := range b.streaming.All() {
		if s.AccountID == account {
			n++
		}
	}

	return n
}

// GetServiceView returns a service view of the caller's account and Region.
func (b *InMemoryBackend) GetServiceView(ctx context.Context, viewARN string) (*ServiceViewWire, error) {
	if !strings.HasPrefix(viewARN, "arn:") || len(viewARN) > maxARNLen {
		return nil, invalid("ServiceViewArn %q is not a valid ARN", viewARN)
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("GetServiceView")
	defer b.mu.RUnlock()

	v, ok := b.serviceViews.Get(viewARN)
	if !ok || v.AccountID != account || v.Region != region {
		return nil, notFound("service view %s not found", viewARN)
	}

	w := v.wire()

	return &w, nil
}

// ListServiceViews lists the ARNs of the caller's service views in the Region.
func (b *InMemoryBackend) ListServiceViews(
	ctx context.Context,
	token string,
	maxResults int,
) (page.Page[string], error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("ListServiceViews")

	var arns []string

	for _, v := range b.serviceViews.All() {
		if v.AccountID == account && v.Region == region {
			arns = append(arns, v.ARN)
		}
	}

	b.mu.RUnlock()

	slices.Sort(arns)

	return paginate(arns, token, maxResults, maxViewListPage, maxViewListPage)
}

// ListStreamingAccessForServices lists the services granted streaming access to the caller's account.
func (b *InMemoryBackend) ListStreamingAccessForServices(
	ctx context.Context, token string, maxResults int,
) (page.Page[StreamingAccessWire], error) {
	account := b.callerAccount(ctx)

	b.mu.RLock("ListStreamingAccessForServices")

	var all []StreamingAccessWire

	for _, s := range b.streaming.All() {
		if s.AccountID == account {
			all = append(all, StreamingAccessWire{CreatedAt: s.CreatedAt, ServicePrincipal: s.ServicePrincipal})
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(all, func(x, y StreamingAccessWire) int {
		return cmp.Compare(x.ServicePrincipal, y.ServicePrincipal)
	})

	return paginate(all, token, maxResults, maxViewListPage, maxViewListPage)
}
