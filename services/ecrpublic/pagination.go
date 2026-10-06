package ecrpublic

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultDescribePageSize = 100
	maxDescribePageSize     = 1000
)

// paginateDescribe applies maxResults (1-1000, default 100) and nextToken (api_op_DescribeRepositories.go:30-50);
// exclusive marks an explicit name/id list, which the SDK forbids combining with either.
func paginateDescribe[T any](items []T, maxResults int32, nextToken string, exclusive bool) ([]T, string, error) {
	if exclusive && (maxResults != 0 || nextToken != "") {
		return nil, "", fmt.Errorf(
			"%w: maxResults and nextToken cannot be used with an explicit list",
			ErrInvalidParameter,
		)
	}

	if exclusive {
		return items, "", nil
	}

	if maxResults != 0 && (maxResults < 1 || maxResults > maxDescribePageSize) {
		return nil, "", fmt.Errorf("%w: maxResults must be between 1 and %d", ErrInvalidParameter, maxDescribePageSize)
	}

	pg := page.New(items, nextToken, int(maxResults), defaultDescribePageSize)

	return pg.Data, pg.Next, nil
}
