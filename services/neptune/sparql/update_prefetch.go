package sparql

import "context"

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

func prefetch(ctx context.Context, u *Update, opts UpdateOptions) *prefetched {
	p := &prefetched{docs: map[string]docResult{}, services: map[string]serviceResult{}}

	for i := range u.ops {
		op := &u.ops[i]

		if op.Kind == upLoad {
			if _, done := p.docs[op.Src]; !done {
				doc, err := fetchDocument(ctx, op.Src, opts)
				p.docs[op.Src] = docResult{doc: doc, err: err}
			}
		}

		if opts.Service == nil {
			continue
		}

		for _, el := range op.services {
			if el.Name.isVar() {
				continue
			}

			query := op.Prologue + "SELECT * WHERE " + el.Text

			key := serviceKey(el.Name.Term.Value, query)
			if _, done := p.services[key]; !done {
				res, err := opts.Service(ctx, el.Name.Term.Value, query)
				p.services[key] = serviceResult{res: res, err: err}
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
