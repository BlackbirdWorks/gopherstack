package resourcegroupstaggingapi

const (
	defaultResourcesPerPage = 100
	maxResourcesPerPage     = 100
)

// resolvePageSize returns the effective page size, capped at maxResourcesPerPage.
func resolvePageSize(perPage *int32) int {
	if perPage == nil || *perPage <= 0 {
		return defaultResourcesPerPage
	}

	return min(int(*perPage), maxResourcesPerPage)
}

// resolveTagsPerPage returns the effective TagsPerPage cap, or 0 when unset (no cap).
// Real AWS uses TagsPerPage to additionally split pages by cumulative tag count instead
// of by resource count alone; see [paginateResources] and [capByTagCount].
func resolveTagsPerPage(tagsPerPage *int32) int {
	if tagsPerPage == nil {
		return 0
	}

	return int(*tagsPerPage)
}

// paginateResources applies cursor-based pagination and returns the current page and the
// next pagination token (nil when there are no more results). When tagsPerPage is
// positive the page is additionally capped by [capByTagCount] so that TagsPerPage --
// otherwise accepted and validated but never affecting output -- actually constrains
// page splits the way real AWS documents it doing.
func paginateResources(all []TaggedResource, token string, pageSize, tagsPerPage int) ([]TaggedResource, *string) {
	start := findTokenStart(all, token)
	page := all[start:]
	truncated := len(page) > pageSize

	if truncated {
		page = page[:pageSize]
	}

	if tagsPerPage > 0 {
		if capped, hitLimit := capByTagCount(page, tagsPerPage); hitLimit {
			page = capped
			truncated = true
		}
	}

	if !truncated {
		return page, nil
	}

	tok := page[len(page)-1].ResourceARN

	return page, &tok
}

// capByTagCount returns the longest prefix of page whose cumulative tag count (each
// resource counting max(1, len(tags)) tags -- matching real AWS's "a resource with no
// tags is counted as having one tag") does not exceed tagsPerPage. At least one resource
// is always kept: GetResources never splits a single resource and its tags across pages,
// so an oversized first resource still gets returned alone. hitLimit reports whether the
// returned prefix is shorter than page, i.e. whether tagsPerPage actually constrained it.
func capByTagCount(page []TaggedResource, tagsPerPage int) ([]TaggedResource, bool) {
	total := 0

	for i, r := range page {
		count := max(len(r.Tags), 1)

		if i > 0 && total+count > tagsPerPage {
			return page[:i], true
		}

		total += count
	}

	return page, false
}

// paginateStrings applies cursor-based pagination over a sorted string slice and returns
// the current page and the next pagination token (nil when there are no more results).
func paginateStrings(all []string, token string, pageSize int) ([]string, *string) {
	start := 0

	if token != "" {
		for i, s := range all {
			if s == token {
				start = i + 1

				break
			}
		}
	}

	page := all[start:]

	if len(page) <= pageSize {
		return page, nil
	}

	page = page[:pageSize]
	tok := page[len(page)-1]

	return page, &tok
}

// findTokenStart returns the index after the resource whose ARN equals token,
// or 0 if the token is empty or not found.
func findTokenStart(all []TaggedResource, token string) int {
	if token == "" {
		return 0
	}

	for i, r := range all {
		if r.ResourceARN == token {
			return i + 1
		}
	}

	return 0
}
