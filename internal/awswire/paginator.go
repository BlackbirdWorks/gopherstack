package awswire

import (
	"context"
	"errors"
)

// ErrNoMorePages is returned by NextPage once the last page was read.
var ErrNoMorePages = errors.New("no more pages available")

// Paginator walks a list operation by its continuation token, as the SDK paginators do.
type Paginator[In, Out any] struct {
	call      func(context.Context, *In) (*Out, error)
	setToken  func(*In, *string)
	nextToken func(*Out) *string
	params    *In
	token     *string
	firstPage bool
}

// NewPaginator pages call over in; setToken stores the continuation token in a request copy and nextToken reads it
// from a response.
func NewPaginator[In, Out any](
	in *In,
	call func(context.Context, *In) (*Out, error),
	setToken func(*In, *string),
	nextToken func(*Out) *string,
) *Paginator[In, Out] {
	return &Paginator[In, Out]{
		params: in, call: call, setToken: setToken, nextToken: nextToken, firstPage: true,
	}
}

// HasMorePages reports whether NextPage has another page to return.
func (p *Paginator[In, Out]) HasMorePages() bool {
	return p.firstPage || (p.token != nil && *p.token != "")
}

// NextPage fetches the next page.
func (p *Paginator[In, Out]) NextPage(ctx context.Context) (*Out, error) {
	if !p.HasMorePages() {
		return nil, ErrNoMorePages
	}

	params := *p.params
	p.setToken(&params, p.token)

	out, err := p.call(ctx, &params)
	if err != nil {
		return nil, err
	}

	p.firstPage = false
	p.token = p.nextToken(out)

	return out, nil
}
