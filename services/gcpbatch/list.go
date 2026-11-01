package gcpbatch

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/batch/apiv1/batchpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	fieldState  = "status.state"
	labelPrefix = "labels."
)

type clause struct{ field, value string }

// parseFilter reads clauses joined by AND: status.state=X or labels.key=value, optionally quoted.
func parseFilter(f string) ([]clause, error) {
	var out []clause

	for raw := range strings.SplitSeq(f, " AND ") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}

		field, val, ok := strings.Cut(raw, "=")
		field, val = strings.TrimSpace(field), strings.Trim(strings.TrimSpace(val), `"`)

		if !ok || (field != fieldState && !strings.HasPrefix(field, labelPrefix)) {
			return nil, errInvalid("Invalid filter %q: use status.state=STATE or labels.KEY=VALUE joined by AND.", f)
		}

		if field == fieldState {
			if _, known := batchpb.JobStatus_State_value[val]; !known {
				return nil, errInvalid("Invalid filter %q: unknown state %q.", f, val)
			}
		}

		out = append(out, clause{field, val})
	}

	return out, nil
}

func (c clause) match(j *batchpb.Job) bool {
	if c.field == fieldState {
		return j.GetStatus().GetState().String() == c.value
	}

	v, ok := j.GetLabels()[strings.TrimPrefix(c.field, labelPrefix)]

	return ok && v == c.value
}

func sortJobs(items []*batchpb.Job, orderBy string) error {
	field, desc := "name", false

	if f := strings.Fields(orderBy); len(f) > 0 {
		field = f[0]

		switch {
		case len(f) == 1:
		case len(f) == 2 && strings.EqualFold(f[1], "desc"):
			desc = true
		case len(f) == 2 && strings.EqualFold(f[1], "asc"):
		default:
			return errInvalid("Invalid order_by %q.", orderBy)
		}
	}

	var key func(x *batchpb.Job) time.Time

	switch field {
	case "name":
	case "create_time":
		key = func(x *batchpb.Job) time.Time { return x.GetCreateTime().AsTime() }
	case "update_time":
		key = func(x *batchpb.Job) time.Time { return x.GetUpdateTime().AsTime() }
	default:
		return errInvalid("Cannot order by %q: use name, create_time or update_time.", field)
	}

	slices.SortStableFunc(items, func(x, y *batchpb.Job) int {
		c := cmp.Compare(x.GetName(), y.GetName())
		if key != nil {
			c = key(x).Compare(key(y))
		}

		if desc {
			return -c
		}

		return c
	})

	return nil
}

// ListJobs lists the jobs of a location, or of every location for "-".
func (b *InMemoryBackend) ListJobs(in *batchpb.ListJobsRequest) (*batchpb.ListJobsResponse, error) {
	project, location, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	clauses, err := parseFilter(in.GetFilter())
	if err != nil {
		return nil, err
	}

	prefix := locationName(project, location) + "/" + kindJobs + "/"
	if location == anyLocation {
		prefix = "projects/" + project + "/locations/"
	}

	b.mu.Lock("ListJobs")
	b.advanceLocked()

	var all []*batchpb.Job

	for n, r := range b.jobs {
		if !strings.HasPrefix(n, prefix) || (location == anyLocation && !strings.Contains(n, "/"+kindJobs+"/")) {
			continue
		}

		v := r.view(b.now())
		if slices.ContainsFunc(clauses, func(c clause) bool { return !c.match(v) }) {
			continue
		}

		all = append(all, v)
	}

	b.mu.Unlock()

	if err = sortJobs(all, in.GetOrderBy()); err != nil {
		return nil, err
	}

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &batchpb.ListJobsResponse{Jobs: pg.Data, NextPageToken: pg.Next}, nil
}
