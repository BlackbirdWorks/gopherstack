package resourceexplorer2

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (b *InMemoryBackend) indexARN(account, region string) string {
	return arn.Build(signingName, region, account, "index/"+uuid.NewString())
}

func (b *InMemoryBackend) stateOf(i *Index) string {
	if i.Transition != "" && b.now().Before(i.SettleAt) {
		return i.Transition
	}

	return i.State
}

func (b *InMemoryBackend) settle(i *Index, transition, final string) {
	now := b.now()
	i.State = final
	i.LastUpdatedAt = now

	if b.dwell > 0 {
		i.Transition = transition
		i.SettleAt = now.Add(b.dwell)

		return
	}

	i.Transition = ""
}

func (b *InMemoryBackend) purgeLocked() {
	for _, i := range b.indexes.All() {
		if i.State == stateDeleted && b.stateOf(i) == stateDeleted {
			b.indexes.Delete(i.ARN)
		}
	}
}

func (b *InMemoryBackend) liveIndexes(account string) []*Index {
	var out []*Index

	for _, i := range b.indexes.All() {
		if i.AccountID == account && b.stateOf(i) != stateDeleted {
			out = append(out, i)
		}
	}

	slices.SortFunc(out, func(x, y *Index) int { return cmp.Compare(x.ARN, y.ARN) })

	return out
}

func (b *InMemoryBackend) indexIn(account, region string) *Index {
	for _, i := range b.liveIndexes(account) {
		if i.Region == region {
			return i
		}
	}

	return nil
}

func (b *InMemoryBackend) aggregatorOf(account string) *Index {
	for _, i := range b.liveIndexes(account) {
		if i.Type == indexTypeAggregator {
			return i
		}
	}

	return nil
}

// replication returns the Regions replicating into an aggregator index, or the aggregator Region of a local one.
func (b *InMemoryBackend) replication(i *Index) ([]string, []string) {
	if i.Type == indexTypeAggregator {
		var from []string

		for _, o := range b.liveIndexes(i.AccountID) {
			if o.ARN != i.ARN {
				from = append(from, o.Region)
			}
		}

		slices.Sort(from)

		return from, nil
	}

	if agg := b.aggregatorOf(i.AccountID); agg != nil {
		return nil, []string{agg.Region}
	}

	return nil, nil
}

func (b *InMemoryBackend) checkAggregatorAllowed(account string) error {
	if agg := b.aggregatorOf(account); agg != nil {
		return conflict("an aggregator index already exists in %s; an account can have only one", agg.Region)
	}

	if st, ok := b.accounts.Get(account); ok && b.now().Before(st.AggregatorBlockedUntil) {
		return conflict(
			"a new aggregator index cannot be created until %s",
			st.AggregatorBlockedUntil.Format(time.RFC3339),
		)
	}

	return nil
}

func (b *InMemoryBackend) blockAggregator(account string) {
	b.accounts.Put(&AccountState{AccountID: account, AggregatorBlockedUntil: b.now().Add(aggregatorLock)})
}

// IndexSummary is the Index shape returned by ListIndexes.
type IndexSummary struct {
	Arn    string `json:"Arn"`
	Region string `json:"Region"`
	Type   string `json:"Type"`
}

// IndexDetail is the GetIndex output.
type IndexDetail struct {
	CreatedAt       time.Time         `json:"CreatedAt"`
	LastUpdatedAt   time.Time         `json:"LastUpdatedAt"`
	Tags            map[string]string `json:"Tags,omitempty"`
	Arn             string            `json:"Arn"`
	State           string            `json:"State"`
	Type            string            `json:"Type"`
	ReplicatingFrom []string          `json:"ReplicatingFrom,omitempty"`
	ReplicatingTo   []string          `json:"ReplicatingTo,omitempty"`
}

// IndexChange is the output of CreateIndex, UpdateIndexType and DeleteIndex.
type IndexChange struct {
	CreatedAt     *time.Time `json:"CreatedAt,omitempty"`
	LastUpdatedAt *time.Time `json:"LastUpdatedAt,omitempty"`
	Arn           string     `json:"Arn"`
	State         string     `json:"State"`
	Type          string     `json:"Type,omitempty"`
}

// CreateIndex creates the LOCAL index of the caller's account in the caller's Region.
func (b *InMemoryBackend) CreateIndex(ctx context.Context, token string, tags map[string]string) (*IndexChange, error) {
	return b.createIndexOfType(ctx, token, indexTypeLocal, tags)
}

func (b *InMemoryBackend) createIndexOfType(
	ctx context.Context, token, typ string, tags map[string]string,
) (*IndexChange, error) {
	if err := validateTags(tags); err != nil {
		return nil, err
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	idx, err := idempotency.Create(b.idem, "CreateIndex", token, idempotency.Fingerprint(tags),
		func(i *Index) string { return i.ARN },
		func(id string) (*Index, error) { return b.indexByARN(id) },
		func() (*Index, error) { return b.createIndex(account, region, typ, tags) },
	)
	if err != nil {
		return nil, wrapIdem(err)
	}

	created := idx.CreatedAt

	return &IndexChange{Arn: idx.ARN, CreatedAt: &created, State: stateCreating}, nil
}

func (b *InMemoryBackend) indexByARN(id string) (*Index, error) {
	b.mu.RLock("indexByARN")
	defer b.mu.RUnlock()

	i, ok := b.indexes.Get(id)
	if !ok {
		return nil, notFound("index %s not found", id)
	}

	return i, nil
}

func (b *InMemoryBackend) createIndex(account, region, typ string, tags map[string]string) (*Index, error) {
	b.mu.Lock("CreateIndex")
	defer b.mu.Unlock()

	b.purgeLocked()

	if b.indexIn(account, region) != nil {
		return nil, conflict("an index already exists in %s", region)
	}

	if typ == indexTypeAggregator {
		if err := b.checkAggregatorAllowed(account); err != nil {
			return nil, err
		}
	}

	now := b.now()
	idx := &Index{
		ARN: b.indexARN(account, region), Region: region, AccountID: account, Type: typ,
		CreatedAt: now, Tags: maps.Clone(tags),
	}
	b.settle(idx, stateCreating, stateActive)
	b.indexes.Put(idx)

	return idx, nil
}

func wrapIdem(err error) error {
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return conflict("%v", err)
	}

	return err
}

// GetIndex returns the caller's index in the caller's Region.
func (b *InMemoryBackend) GetIndex(ctx context.Context) (*IndexDetail, error) {
	b.mu.RLock("GetIndex")
	defer b.mu.RUnlock()

	i := b.indexIn(b.callerAccount(ctx), b.callerRegion(ctx))
	if i == nil {
		return nil, notFound("there is no index in this Region")
	}

	from, to := b.replication(i)

	return &IndexDetail{
		Arn: i.ARN, CreatedAt: i.CreatedAt, LastUpdatedAt: i.LastUpdatedAt, State: b.stateOf(i), Type: i.Type,
		Tags: maps.Clone(i.Tags), ReplicatingFrom: from, ReplicatingTo: to,
	}, nil
}

// UpdateIndexType converts the index between LOCAL and AGGREGATOR.
func (b *InMemoryBackend) UpdateIndexType(ctx context.Context, indexARN, typ string) (*IndexChange, error) {
	if indexARN == "" {
		return nil, invalid("Arn is required")
	}

	if err := validateIndexType(typ); err != nil {
		return nil, err
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.Lock("UpdateIndexType")
	defer b.mu.Unlock()

	b.purgeLocked()

	i := b.indexIn(account, region)
	if i == nil || i.ARN != indexARN {
		return nil, notFound("index %s not found", indexARN)
	}

	if st := b.stateOf(i); st != stateActive {
		return nil, conflict("the index is %s; it must be ACTIVE to change its type", st)
	}

	if i.Type == typ {
		return nil, conflict("the index is already of type %s", typ)
	}

	if typ == indexTypeAggregator {
		if err := b.checkAggregatorAllowed(account); err != nil {
			return nil, err
		}
	} else {
		b.blockAggregator(account)
	}

	i.Type = typ
	b.settle(i, stateUpdating, stateActive)
	updated := i.LastUpdatedAt

	return &IndexChange{Arn: i.ARN, LastUpdatedAt: &updated, State: stateUpdating, Type: typ}, nil
}

// DeleteIndex deletes the index and every view in its Region.
func (b *InMemoryBackend) DeleteIndex(ctx context.Context, indexARN string) (*IndexChange, error) {
	if indexARN == "" {
		return nil, invalid("Arn is required")
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.Lock("DeleteIndex")
	defer b.mu.Unlock()

	b.purgeLocked()

	i := b.indexIn(account, region)
	if i == nil || i.ARN != indexARN {
		return nil, notFound("index %s not found", indexARN)
	}

	if st := b.stateOf(i); st == stateDeleting {
		return nil, conflict("the index is already being deleted")
	}

	if i.Type == indexTypeAggregator {
		b.blockAggregator(account)
	}

	b.removeViewsLocked(account, region)
	b.settle(i, stateDeleting, stateDeleted)
	updated := i.LastUpdatedAt

	return &IndexChange{Arn: i.ARN, LastUpdatedAt: &updated, State: stateDeleting}, nil
}

// ListIndexes lists the caller's indexes across every Region.
func (b *InMemoryBackend) ListIndexes(
	ctx context.Context, regions []string, typ, token string, maxResults int,
) (page.Page[IndexSummary], error) {
	if typ != "" {
		if err := validateIndexType(typ); err != nil {
			return page.Page[IndexSummary]{}, err
		}
	}

	b.mu.RLock("ListIndexes")
	defer b.mu.RUnlock()

	var all []IndexSummary

	for _, i := range b.liveIndexes(b.callerAccount(ctx)) {
		if (typ != "" && i.Type != typ) || (len(regions) > 0 && !slices.Contains(regions, i.Region)) {
			continue
		}

		all = append(all, IndexSummary{Arn: i.ARN, Region: i.Region, Type: i.Type})
	}

	return paginate(all, token, maxResults, maxPage)
}

// MemberIndex is an index of a member account.
type MemberIndex struct {
	AccountID string `json:"AccountId"`
	Arn       string `json:"Arn"`
	Region    string `json:"Region"`
	Type      string `json:"Type"`
}

// ListIndexesForMembers lists the indexes of the given accounts.
func (b *InMemoryBackend) ListIndexesForMembers(
	accounts []string, token string, maxResults int,
) (page.Page[MemberIndex], error) {
	if len(accounts) == 0 || len(accounts) > maxMemberAccounts {
		return page.Page[MemberIndex]{}, invalid(
			"AccountIdList must contain between 1 and %d accounts",
			maxMemberAccounts,
		)
	}

	b.mu.RLock("ListIndexesForMembers")
	defer b.mu.RUnlock()

	var all []MemberIndex

	for _, a := range accounts {
		for _, i := range b.liveIndexes(a) {
			all = append(all, MemberIndex{AccountID: a, Arn: i.ARN, Region: i.Region, Type: i.Type})
		}
	}

	return paginate(all, token, maxResults, maxPage)
}
