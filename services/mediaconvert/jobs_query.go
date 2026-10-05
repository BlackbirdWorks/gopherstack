package mediaconvert

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// StartJobsQuery stores a jobs query and returns a query ID for deferred retrieval.
// Filters use key-value pairs where key is a field name (e.g. "queue", "status")
// and values are the allowed values for that field.
func (b *InMemoryBackend) StartJobsQuery(
	filterList []map[string]any, maxResults int, order, nextToken string,
) (string, error) {
	if err := page.ValidateToken(nextToken); err != nil {
		return "", fmt.Errorf("%w: invalid nextToken", ErrValidation)
	}

	id := uuid.NewString()

	b.mu.Lock("StartJobsQuery")
	defer b.mu.Unlock()

	b.queries.Put(&jobsQuery{
		queryID:    id,
		filterList: filterList,
		maxResults: maxResults,
		offset:     page.DecodeToken(nextToken),
		order:      order,
	})

	return id, nil
}

// GetJobsQueryResults returns jobs matching the stored query for the given ID.
// If the queryID is unknown (not from a prior StartJobsQuery call), returns empty.
func (b *InMemoryBackend) GetJobsQueryResults(queryID string) []*Job {
	jobs, _ := b.GetJobsQueryPage(queryID)

	return jobs
}

// GetJobsQueryPage is GetJobsQueryResults plus the nextToken for the following batch, empty when none remains.
func (b *InMemoryBackend) GetJobsQueryPage(queryID string) ([]*Job, string) {
	b.mu.RLock("GetJobsQueryResults")
	defer b.mu.RUnlock()

	q, ok := b.queries.Get(queryID)
	if !ok {
		return []*Job{}, ""
	}

	list := make([]*Job, 0, b.jobs.Len())

	for _, j := range b.jobs.All() {
		if !jobMatchesFilters(j, q.filterList) {
			continue
		}

		list = append(list, cloneJob(j))
	}

	sortQueryJobs(list, q.order == orderAscending)

	pg := page.New(list, page.EncodeToken(q.offset), q.maxResults, defaultListPageSize)

	return pg.Data, pg.Next
}

// sortQueryJobs orders by CreatedAt with the job ID as tiebreak so paging is stable.
func sortQueryJobs(list []*Job, asc bool) {
	sort.Slice(list, func(i, j int) bool {
		if list[i].CreatedAt != list[j].CreatedAt {
			return (list[i].CreatedAt < list[j].CreatedAt) == asc
		}

		return list[i].ID < list[j].ID
	})
}

// jobMatchesFilters applies each JobsQueryFilter: values within a filter are OR'd, filters are AND'd.
// Keys follow types.JobsQueryFilter.Key; queue accepts a name or ARN, fileInput a partial name.
func jobMatchesFilters(j *Job, filters []map[string]any) bool {
	for _, f := range filters {
		key, _ := f["key"].(string)
		vals, _ := f["values"].([]any)

		if !jobMatchesQueryKey(j, key, vals) {
			return false
		}
	}

	return true
}

func jobMatchesQueryKey(j *Job, key string, vals []any) bool {
	var candidates []string

	switch strings.ToLower(key) {
	case "queue":
		candidates = jobQueueRefs(j)
	case "status":
		candidates = []string{j.Status}
	case "jobengineversionrequested":
		candidates = []string{j.JobEngineVersionRequested}
	case "jobengineversionused":
		candidates = []string{j.JobEngineVersionUsed}
	case "audiocodec":
		candidates = outputCodecs(j, "audioDescriptions")
	case "videocodec":
		candidates = outputCodecs(j, "videoDescription")
	case "fileinput":
		for _, v := range vals {
			if vs, ok := v.(string); ok && jobMatchesInputFile(j, vs) {
				return true
			}
		}

		return false
	default:
		return true
	}

	for _, v := range vals {
		if vs, ok := v.(string); ok && slices.Contains(candidates, vs) {
			return true
		}
	}

	return false
}

// outputCodecs collects codecSettings.codec from every output's descriptions entry.
func outputCodecs(j *Job, descKey string) []string {
	var out []string

	groups, _ := j.Settings["outputGroups"].([]any)
	for _, g := range groups {
		gm, _ := g.(map[string]any)
		outputs, _ := gm["outputs"].([]any)

		for _, o := range outputs {
			om, _ := o.(map[string]any)

			switch d := om[descKey].(type) {
			case []any:
				for _, e := range d {
					out = appendCodec(out, e)
				}
			default:
				out = appendCodec(out, d)
			}
		}
	}

	return out
}

func appendCodec(out []string, desc any) []string {
	dm, _ := desc.(map[string]any)
	cs, _ := dm["codecSettings"].(map[string]any)

	if codec, ok := cs["codec"].(string); ok {
		return append(out, codec)
	}

	return out
}

// jobQueueRefs returns every reference a caller may use for the job's queue: name or ARN.
func jobQueueRefs(j *Job) []string {
	refs := []string{j.Queue, j.QueueArn}
	if i := strings.LastIndex(j.QueueArn, "queues/"); i >= 0 {
		refs = append(refs, j.QueueArn[i+len("queues/"):])
	}

	return refs
}
