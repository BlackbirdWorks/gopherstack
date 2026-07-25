package codebuild

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultListPageSize mirrors real AWS CodeBuild's documented page cap
// ("if there are more than 100 items in the list, only the first 100 items
// are returned") shared by every List* operation, whether or not the
// operation exposes an explicit maxResults request field.
const defaultListPageSize = 100

const sortOrderDescending = "DESCENDING"

// sortBy values shared by ListProjects/ListFleets/ListReportGroups (the
// three List* operations whose real SDK request accepts a sortBy field).
const (
	sortByCreatedTime      = "CREATED_TIME"
	sortByLastModifiedTime = "LAST_MODIFIED_TIME"
)

// paginateIDs applies nextToken/maxResults/sortOrder pagination to an
// ID/name/ARN slice that the backend already returns in ascending order
// (store.Table.Snapshot's deterministic key order, or an explicit sortBy
// ordering computed by the caller). sortOrder == "DESCENDING" reverses the
// slice before paging; any other value (including "") keeps ascending order.
// maxResults <= 0 falls back to [defaultListPageSize].
func paginateIDs(all []string, nextToken, sortOrder string, maxResults int32) (page.Page[string], error) {
	if err := page.ValidateToken(nextToken); err != nil {
		return page.Page[string]{}, fmt.Errorf("%w: invalid nextToken", ErrValidation)
	}

	ordered := all
	if sortOrder == sortOrderDescending {
		ordered = make([]string, len(all))
		for i, v := range all {
			ordered[len(all)-1-i] = v
		}
	}

	return page.New(ordered, nextToken, int(maxResults), defaultListPageSize), nil
}
