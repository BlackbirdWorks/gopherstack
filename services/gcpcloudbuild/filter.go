package gcpcloudbuild

import (
	"regexp"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
)

const fieldCreateTime = "create_time"

var filterTerm = regexp.MustCompile(
	`^\s*([a-z_.]+)\s*(=|!=|>=|<=|>|<)\s*(?:"([^"]*)"|'([^']*)'|(\S+))\s*$`,
)

var filterSplit = regexp.MustCompile(`(?i)\s+AND\s+`)

type buildPredicate func(*cloudbuildpb.Build) bool

// parseBuildFilter compiles a ListBuilds filter: AND-joined "field op value" terms over status, id,
// build_id, trigger_id, project_id, tags and the create/start/finish times.
func parseBuildFilter(f string) (buildPredicate, error) {
	if strings.TrimSpace(f) == "" {
		return func(*cloudbuildpb.Build) bool { return true }, nil
	}

	var preds []buildPredicate

	for _, term := range filterSplit.Split(f, -1) {
		m := filterTerm.FindStringSubmatch(term)
		if m == nil {
			return nil, errInvalid("Invalid filter %q: unsupported expression.", term)
		}

		val := m[3] + m[4] + m[5]

		p, err := compileTerm(m[1], m[2], val)
		if err != nil {
			return nil, err
		}

		preds = append(preds, p)
	}

	return func(b *cloudbuildpb.Build) bool {
		for _, p := range preds {
			if !p(b) {
				return false
			}
		}

		return true
	}, nil
}

func compileTerm(field, op, val string) (buildPredicate, error) {
	switch field {
	case "status":
		if _, ok := cloudbuildpb.Build_Status_value[val]; !ok {
			return nil, errInvalid("Invalid filter: unknown status %q.", val)
		}

		return stringTerm(
			op,
			val,
			func(b *cloudbuildpb.Build) string { return b.GetStatus().String() },
		)
	case "id", "build_id":
		return stringTerm(op, val, func(b *cloudbuildpb.Build) string { return b.GetId() })
	case "trigger_id", "build_trigger_id":
		return stringTerm(
			op,
			val,
			func(b *cloudbuildpb.Build) string { return b.GetBuildTriggerId() },
		)
	case "project_id":
		return stringTerm(op, val, func(b *cloudbuildpb.Build) string { return b.GetProjectId() })
	case "tags":
		return tagTerm(op, val)
	case fieldCreateTime, "start_time", "finish_time":
		return timeTerm(field, op, val)
	}

	return nil, errInvalid("Invalid filter: unsupported field %q.", field)
}

func stringTerm(op, val string, get func(*cloudbuildpb.Build) string) (buildPredicate, error) {
	switch op {
	case "=":
		return func(b *cloudbuildpb.Build) bool { return get(b) == val }, nil
	case "!=":
		return func(b *cloudbuildpb.Build) bool { return get(b) != val }, nil
	}

	return nil, errInvalid("Invalid filter: operator %q is not valid for strings.", op)
}

func tagTerm(op, val string) (buildPredicate, error) {
	has := func(b *cloudbuildpb.Build) bool {
		return slices.Contains(b.GetTags(), val)
	}

	switch op {
	case "=":
		return has, nil
	case "!=":
		return func(b *cloudbuildpb.Build) bool { return !has(b) }, nil
	}

	return nil, errInvalid("Invalid filter: operator %q is not valid for tags.", op)
}

func timeTerm(field, op, val string) (buildPredicate, error) {
	want, err := time.Parse(time.RFC3339, val)
	if err != nil {
		return nil, errInvalid("Invalid filter: %q is not an RFC 3339 timestamp.", val)
	}

	get := func(b *cloudbuildpb.Build) (time.Time, bool) {
		ts := b.GetCreateTime()

		switch field {
		case "start_time":
			ts = b.GetStartTime()
		case "finish_time":
			ts = b.GetFinishTime()
		}

		return ts.AsTime(), ts != nil
	}

	return func(b *cloudbuildpb.Build) bool {
		got, ok := get(b)

		return ok && compareTime(op, got, want)
	}, nil
}

func compareTime(op string, got, want time.Time) bool {
	switch op {
	case "=":
		return got.Equal(want)
	case "!=":
		return !got.Equal(want)
	case ">":
		return got.After(want)
	case "<":
		return got.Before(want)
	case ">=":
		return !got.Before(want)
	default:
		return !got.After(want)
	}
}
