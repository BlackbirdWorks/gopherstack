package gcplogging

import (
	"slices"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultPageSize   = 1000
	maxPageSize       = 1000
	day               = 24 * time.Hour
	defaultListWindow = day
	maxResourceNames  = 100
)

// source is one bucket (optionally narrowed by a view filter) that a read covers.
type source struct {
	filter *filter
	bucket string
}

// resolveSources maps resource_names to the buckets they read.
func (b *InMemoryBackend) resolveSources(names []string) ([]source, error) {
	if len(names) == 0 {
		return nil, errInvalid("resource_names must contain at least one resource.")
	}

	if len(names) > maxResourceNames {
		return nil, errInvalid("resource_names exceeds the limit of %d.", maxResourceNames)
	}

	var out []source

	for _, n := range names {
		srcs, err := b.sourcesFor(n)
		if err != nil {
			return nil, err
		}

		out = append(out, srcs...)
	}

	return out, nil
}

func (b *InMemoryBackend) sourcesFor(name string) ([]source, error) {
	if parentRe.MatchString(name) {
		b.seed(name)

		return []source{
			{bucket: bucketName(name, globalLocation, defaultBucketID)},
			{bucket: bucketName(name, globalLocation, requiredBucketID)},
		}, nil
	}

	if m := viewRe.FindStringSubmatch(name); m != nil {
		return b.viewSource(name, m[1])
	}

	if bucketRe.MatchString(name) {
		return []source{{bucket: name}}, nil
	}

	return nil, errInvalid("Invalid resource name [%s]: expected projects/[ID] or a log view name.", name)
}

func (b *InMemoryBackend) viewSource(name, bucket string) ([]source, error) {
	b.seed(parentOf(bucket))
	b.mu.RLock("viewSource")
	defer b.mu.RUnlock()

	v, ok := b.views[name]
	if !ok {
		return nil, errNotFound("Log view [%s] not found.", name)
	}

	if !strings.HasSuffix(name, "/views/"+allLogsViewID) && v.GetFilter() != "" {
		f, err := compileFilter(v.GetFilter())
		if err != nil {
			return nil, err
		}

		return []source{{bucket: bucket, filter: f}}, nil
	}

	return []source{{bucket: bucket}}, nil
}

func orderDesc(orderBy string) (bool, error) {
	fields := strings.Fields(orderBy)
	if len(fields) == 0 {
		return false, nil
	}

	if len(fields) > 2 || fields[0] != "timestamp" {
		return false, errInvalid(
			"Invalid order_by [%s]: only \"timestamp asc\" and \"timestamp desc\" are supported.",
			orderBy,
		)
	}

	if len(fields) == 1 || strings.EqualFold(fields[1], "asc") {
		return false, nil
	}

	if strings.EqualFold(fields[1], "desc") {
		return true, nil
	}

	return false, errInvalid("Invalid order_by [%s].", orderBy)
}

// ListLogEntries returns matching entries ordered by timestamp, paged.
func (b *InMemoryBackend) ListLogEntries(req *loggingpb.ListLogEntriesRequest) ([]*loggingpb.LogEntry, string, error) {
	srcs, err := b.resolveSources(req.GetResourceNames())
	if err != nil {
		return nil, "", err
	}

	f, err := compileFilter(req.GetFilter())
	if err != nil {
		return nil, "", err
	}

	desc, err := orderDesc(req.GetOrderBy())
	if err != nil {
		return nil, "", err
	}

	if err = pageTokenOK(req.GetPageToken()); err != nil {
		return nil, "", err
	}

	all := b.collect(srcs, f)
	sortEntries(all, desc)

	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPageSize)

	return pg.Data, pg.Next, nil
}

func clampPage(n int32) int {
	switch {
	case n < 0:
		return 0
	case int(n) > maxPageSize:
		return maxPageSize
	}

	return int(n)
}

func (b *InMemoryBackend) collect(srcs []source, f *filter) []*loggingpb.LogEntry {
	b.mu.RLock("ListLogEntries")
	defer b.mu.RUnlock()

	now := b.now()
	seen := map[*loggingpb.LogEntry]struct{}{}

	var out []*loggingpb.LogEntry

	for _, s := range srcs {
		bd, ok := b.buckets[s.bucket]
		if !ok || !bd.deleteRequested.IsZero() {
			continue
		}

		for _, e := range bd.entries {
			if _, dup := seen[e]; dup || !visible(bd, e, now, f) {
				continue
			}

			if s.filter != nil && !s.filter.matches(newDoc(e)) {
				continue
			}

			seen[e] = struct{}{}

			out = append(out, e)
		}
	}

	return out
}

// visible applies retention, the default 24h window and the request filter.
func visible(bd *bucketData, e *loggingpb.LogEntry, now time.Time, f *filter) bool {
	days := time.Duration(bd.pb.GetRetentionDays()) * day
	if days > 0 && e.GetReceiveTimestamp().AsTime().Before(now.Add(-days)) {
		return false
	}

	if (f == nil || !f.hasTimestamp) && e.GetTimestamp().AsTime().Before(now.Add(-defaultListWindow)) {
		return false
	}

	return f.matches(newDoc(e))
}

func sortEntries(all []*loggingpb.LogEntry, desc bool) {
	sort.SliceStable(all, func(i, j int) bool {
		a, c := all[i].GetTimestamp().AsTime(), all[j].GetTimestamp().AsTime()
		if desc {
			return a.After(c)
		}

		return a.Before(c)
	})
}

// ListLogs returns the names of logs that hold entries under the given resources.
func (b *InMemoryBackend) ListLogs(req *loggingpb.ListLogsRequest) ([]string, string, error) {
	names := req.GetResourceNames()
	if len(names) == 0 && req.GetParent() != "" {
		names = []string{req.GetParent()}
	}

	if len(names) == 0 {
		return nil, "", errInvalid("parent is required.")
	}

	for _, n := range names {
		if err := checkParent(n); err != nil && !viewRe.MatchString(n) {
			return nil, "", err
		}
	}

	srcs, err := b.resolveSources(names)
	if err != nil {
		return nil, "", err
	}

	if err = pageTokenOK(req.GetPageToken()); err != nil {
		return nil, "", err
	}

	logs := b.logNames(srcs)
	pg := page.New(logs, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPageSize)

	return pg.Data, pg.Next, nil
}

func (b *InMemoryBackend) logNames(srcs []source) []string {
	b.mu.RLock("ListLogs")
	defer b.mu.RUnlock()

	set := map[string]struct{}{}

	for _, s := range srcs {
		if bd, ok := b.buckets[s.bucket]; ok {
			for _, e := range bd.entries {
				set[e.GetLogName()] = struct{}{}
			}
		}
	}

	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}

	slices.Sort(out)

	return out
}

// DeleteLog removes every entry of the log from all buckets of its parent.
func (b *InMemoryBackend) DeleteLog(logName string) error {
	parent, err := splitLogName(logName)
	if err != nil {
		return err
	}

	b.seed(parent)
	b.mu.Lock("DeleteLog")
	defer b.mu.Unlock()

	removed := false

	for name, bd := range b.buckets {
		if parentOf(name) != parent {
			continue
		}

		kept := bd.entries[:0]

		for _, e := range bd.entries {
			if e.GetLogName() == logName {
				delete(bd.keys, entryKey(e))

				removed = true

				continue
			}

			kept = append(kept, e)
		}

		clear(bd.entries[len(kept):])
		bd.entries = kept
	}

	if !removed {
		return errNotFound("Log %s does not exist.", logName)
	}

	return nil
}
