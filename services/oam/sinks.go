package oam

import (
	"cmp"
	"context"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxNameLen = 255
	arnFields  = 6
)

var nameRE = regexp.MustCompile(`^[a-zA-Z0-9_.\-]+$`)

func paginate[T any](all []T, token string, maxResults *int32) (page.Page[T], error) {
	limit := defaultPageSize

	if maxResults != nil {
		if *maxResults < 1 || *maxResults > maxPageSize {
			return page.Page[T]{}, fail(ErrInvalidParameter, "MaxResults must be between 1 and %d", maxPageSize)
		}

		limit = int(*maxResults)
	}

	if err := page.ValidateToken(token); err != nil || (token != "" && page.DecodeToken(token) > len(all)) {
		return page.Page[T]{}, fail(ErrInvalidParameter, "The NextToken is invalid")
	}

	return page.New(all, token, limit, defaultPageSize), nil
}

func sinkARN(s scope, id string) string {
	return arn.Build(serviceName, s.region, s.account, "sink/"+id)
}

func cloneSink(s *Sink) *Sink {
	cp := *s
	cp.Tags = maps.Clone(s.Tags)

	return &cp
}

// sinkInScopeLocked resolves a sink ARN or ID within the caller's account and Region.
func (b *InMemoryBackend) sinkInScopeLocked(s scope, identifier string) (*Sink, error) {
	if identifier == "" {
		return nil, fail(ErrMissingParameter, "Identifier is required")
	}

	if strings.HasPrefix(identifier, "arn:") {
		if v, ok := b.sinks.Get(identifier); ok && v.Account == s.account && v.Region == s.region {
			return v, nil
		}
	} else {
		for _, v := range b.sinks.All() {
			if v.ID == identifier && v.Account == s.account && v.Region == s.region {
				return v, nil
			}
		}
	}

	return nil, fail(ErrResourceNotFound, "The sink %q was not found", identifier)
}

// CreateSink creates a sink in the caller's account and Region.
func (b *InMemoryBackend) CreateSink(ctx context.Context, name string, tagMap map[string]string) (*Sink, error) {
	if name == "" {
		return nil, fail(ErrMissingParameter, "Name is required")
	}

	if len(name) > maxNameLen || !nameRE.MatchString(name) {
		return nil, fail(
			ErrInvalidParameter,
			"Name must be 1-%d characters of letters, digits, '_', '.' or '-'",
			maxNameLen,
		)
	}

	if err := validateTags(tagMap); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateSink")
	defer b.mu.Unlock()

	s := b.scopeOf(ctx)
	existing := 0

	for _, v := range b.sinks.All() {
		if v.Account != s.account || v.Region != s.region {
			continue
		}

		if v.Name == name {
			return nil, fail(ErrConflict, "A sink named %q already exists", name)
		}

		existing++
	}

	if existing >= maxSinksPerRegion {
		return nil, fail(
			ErrServiceQuotaExceeded,
			"The number of sinks per Region (%d) has been reached",
			maxSinksPerRegion,
		)
	}

	id := uuid.NewString()
	sink := &Sink{
		ARN: sinkARN(s, id), ID: id, Name: name, Account: s.account, Region: s.region,
		Tags: maps.Clone(tagMap), Seq: b.nextSeqLocked(),
	}
	b.sinks.Put(sink)

	return cloneSink(sink), nil
}

// GetSink returns a sink by ARN or ID.
func (b *InMemoryBackend) GetSink(ctx context.Context, identifier string) (*Sink, error) {
	b.mu.RLock("GetSink")
	defer b.mu.RUnlock()

	v, err := b.sinkInScopeLocked(b.scopeOf(ctx), identifier)
	if err != nil {
		return nil, err
	}

	return cloneSink(v), nil
}

// ListSinks returns the caller's sinks in creation order.
func (b *InMemoryBackend) ListSinks(ctx context.Context, token string, maxResults *int32) (page.Page[*Sink], error) {
	b.mu.RLock("ListSinks")
	defer b.mu.RUnlock()

	s := b.scopeOf(ctx)
	rows := make([]*Sink, 0)

	for _, v := range b.sinks.All() {
		if v.Account == s.account && v.Region == s.region {
			rows = append(rows, cloneSink(v))
		}
	}

	slices.SortFunc(rows, func(a, c *Sink) int { return cmp.Compare(a.Seq, c.Seq) })

	return paginate(rows, token, maxResults)
}

// DeleteSink deletes a sink that has no links.
func (b *InMemoryBackend) DeleteSink(ctx context.Context, identifier string) error {
	b.mu.Lock("DeleteSink")
	defer b.mu.Unlock()

	v, err := b.sinkInScopeLocked(b.scopeOf(ctx), identifier)
	if err != nil {
		return err
	}

	for _, l := range b.links.All() {
		if l.SinkARN == v.ARN {
			return fail(ErrConflict, "The sink still has links attached; delete them first")
		}
	}

	b.sinks.Delete(v.ARN)

	return nil
}

// PutSinkPolicy sets the sink policy.
func (b *InMemoryBackend) PutSinkPolicy(ctx context.Context, identifier, policy string) (*Sink, error) {
	if policy == "" {
		return nil, fail(ErrMissingParameter, "Policy is required")
	}

	if err := validateSinkPolicy(policy); err != nil {
		return nil, err
	}

	b.mu.Lock("PutSinkPolicy")
	defer b.mu.Unlock()

	v, err := b.sinkInScopeLocked(b.scopeOf(ctx), identifier)
	if err != nil {
		return nil, err
	}

	v.Policy, v.HasPolicy = policy, true

	return cloneSink(v), nil
}

// GetSinkPolicy returns the sink and its policy.
func (b *InMemoryBackend) GetSinkPolicy(ctx context.Context, identifier string) (*Sink, error) {
	b.mu.RLock("GetSinkPolicy")
	defer b.mu.RUnlock()

	v, err := b.sinkInScopeLocked(b.scopeOf(ctx), identifier)
	if err != nil {
		return nil, err
	}

	if !v.HasPolicy {
		return nil, fail(ErrResourceNotFound, "The sink %q has no policy", identifier)
	}

	return cloneSink(v), nil
}
