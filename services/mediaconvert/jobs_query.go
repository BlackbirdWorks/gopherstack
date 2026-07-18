package mediaconvert

import (
	"sort"

	"github.com/google/uuid"
)

// StartJobsQuery stores a jobs query and returns a query ID for deferred retrieval.
// Filters use key-value pairs where key is a field name (e.g. "queue", "status")
// and values are the allowed values for that field.
func (b *InMemoryBackend) StartJobsQuery(filterList []map[string]any, maxResults int, order string) (string, error) {
	id := uuid.NewString()

	b.mu.Lock("StartJobsQuery")
	defer b.mu.Unlock()

	b.queries.Put(&jobsQuery{
		queryID:    id,
		filterList: filterList,
		maxResults: maxResults,
		order:      order,
	})

	return id, nil
}

// GetJobsQueryResults returns jobs matching the stored query for the given ID.
// If the queryID is unknown (not from a prior StartJobsQuery call), returns empty.
func (b *InMemoryBackend) GetJobsQueryResults(queryID string) []*Job {
	b.mu.RLock("GetJobsQueryResults")
	defer b.mu.RUnlock()

	q, ok := b.queries.Get(queryID)
	if !ok {
		return []*Job{}
	}

	list := make([]*Job, 0, b.jobs.Len())

	for _, j := range b.jobs.All() {
		if !jobMatchesFilters(j, q.filterList) {
			continue
		}

		list = append(list, cloneJob(j))
	}

	if q.order == orderAscending {
		sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt < list[j].CreatedAt })
	} else {
		sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt > list[j].CreatedAt })
	}

	if q.maxResults > 0 && len(list) > q.maxResults {
		list = list[:q.maxResults]
	}

	return list
}

// jobMatchesFilters returns true if the job satisfies all provided query filters.
// Each filter map must have a "key" and a "values" field; any value match passes.
func jobMatchesFilters(j *Job, filters []map[string]any) bool {
	for _, f := range filters {
		key, _ := f["key"].(string)
		vals, _ := f["values"].([]any)

		var jobVal string

		switch key {
		case "queue", "Queue":
			jobVal = j.Queue
		case "status", "Status":
			jobVal = j.Status
		default:
			continue
		}

		matched := false

		for _, v := range vals {
			if vs, ok := v.(string); ok && vs == jobVal {
				matched = true

				break
			}
		}

		if !matched {
			return false
		}
	}

	return true
}
