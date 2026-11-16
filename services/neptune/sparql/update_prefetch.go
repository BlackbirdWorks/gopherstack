package sparql

import (
	"context"
	"fmt"
)

// maxPrefetchFetches bounds the remote fetches (LOAD sources plus SERVICE sub-queries) one update may trigger.
const maxPrefetchFetches = 64

var errTooManyFetches = fmt.Errorf("an update may fetch at most %d remote sources", maxPrefetchFetches)

type docResult struct {
	err error
	doc Document
}

type serviceResult struct {
	res *Result
	err error
}

// prefetched holds the remote documents and SERVICE answers an update needs, fetched before the store
// write lock is taken so a slow remote never blocks readers.
type prefetched struct {
	docs     map[string]docResult
	services map[string]serviceResult
}

func serviceKey(endpoint, query string) string { return endpoint + "\x00" + query }

// fetcher counts the remote fetches an update triggers against maxPrefetchFetches.
type fetcher struct {
	p     *prefetched
	opts  UpdateOptions
	count int
}

func (f *fetcher) allowed() bool {
	f.count++

	return f.count <= maxPrefetchFetches
}

func (f *fetcher) load(ctx context.Context, src string) {
	if _, done := f.p.docs[src]; done {
		return
	}

	if !f.allowed() {
		f.p.docs[src] = docResult{err: errTooManyFetches}

		return
	}

	doc, err := fetchDocument(ctx, src, f.opts)
	f.p.docs[src] = docResult{doc: doc, err: err}
}

func (f *fetcher) service(ctx context.Context, endpoint, query string) {
	key := serviceKey(endpoint, query)
	if _, done := f.p.services[key]; done {
		return
	}

	if !f.allowed() {
		f.p.services[key] = serviceResult{err: errTooManyFetches}

		return
	}

	res, err := f.opts.Service(ctx, endpoint, query)
	f.p.services[key] = serviceResult{res: res, err: err}
}

func prefetch(ctx context.Context, u *Update, opts UpdateOptions) *prefetched {
	p := &prefetched{docs: map[string]docResult{}, services: map[string]serviceResult{}}
	f := &fetcher{p: p, opts: opts}

	for i := range u.ops {
		op := &u.ops[i]

		if op.Kind == upLoad {
			f.load(ctx, op.Src)
		}

		if opts.Service == nil {
			continue
		}

		for _, el := range op.services {
			if !el.Name.isVar() {
				f.service(ctx, el.Name.Term.Value, op.Prologue+"SELECT * WHERE "+el.Text)
			}
		}
	}

	return p
}

func (p *prefetched) document(ctx context.Context, src string, opts UpdateOptions) (Document, error) {
	if r, ok := p.docs[src]; ok {
		return r.doc, r.err
	}

	return fetchDocument(ctx, src, opts)
}

// serviceFunc answers SERVICE clauses of an update from the prefetched results; an endpoint chosen by a
// variable is only known during evaluation under the lock, so it is refused rather than fetched there.
func (p *prefetched) serviceFunc(_ context.Context, endpoint, query string) (*Result, error) {
	r, ok := p.services[serviceKey(endpoint, query)]
	if !ok {
		return nil, syntaxf("SERVICE with a variable endpoint is not available in an update WHERE clause")
	}

	return r.res, r.err
}
