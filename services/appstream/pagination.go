package appstream

import (
	"cmp"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultDescribePageSize = 50
	maxDescribePageSize     = 100
	maxBlockBuilderPage     = 25
)

// pageReq carries the body-bound MaxResults/NextToken members shared by paged Describe/List inputs.
type pageReq struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

// pageOf sorts items by key and returns one page plus its continuation token.
// maxSize caps MaxResults (api_op_DescribeAppBlockBuilders.go:31 documents 25; others are undocumented).
func pageOf[T any](items []T, key func(T) string, p pageReq, maxSize int) ([]T, string, error) {
	if p.MaxResults < 0 || page.ValidateToken(p.NextToken) != nil {
		return nil, "", awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	sorted := slices.Clone(items)
	slices.SortStableFunc(sorted, func(a, b T) int { return cmp.Compare(key(a), key(b)) })

	limit := min(int(p.MaxResults), maxSize)
	pg := page.New(sorted, p.NextToken, limit, min(defaultDescribePageSize, maxSize))

	return pg.Data, pg.Next, nil
}

// pagedResponse pages items and renders them under listKey, adding NextToken when truncated.
func pagedResponse[T any](
	items []T, key func(T) string, p pageReq, listKey string, build func(T) map[string]any,
) (any, error) {
	page, next, err := pageOf(items, key, p, maxDescribePageSize)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(page))
	for _, it := range page {
		resp = append(resp, build(it))
	}

	return withNext(map[string]any{listKey: resp}, next), nil
}

// withNext adds NextToken to a response only when more results remain.
func withNext(out map[string]any, next string) map[string]any {
	if next != "" {
		out["NextToken"] = next
	}

	return out
}
