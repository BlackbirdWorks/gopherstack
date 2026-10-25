package gcpworkflows

import (
	"regexp"
	"strings"
	"time"

	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
)

const (
	fieldExecID   = "executionID"
	fieldStart    = "startTime"
	fieldEnd      = "endTime"
	fieldState    = "state"
	fieldRevision = "workflowRevisionID"
	fieldDuration = "duration"
	opEqual       = "="
	opNotEqual    = "!="
	dirDescending = "desc"
)

var andRe = regexp.MustCompile(`\s+AND\s+`)

var filterTermRe = regexp.MustCompile(`^\s*(label\.[A-Za-z0-9_-]+|[A-Za-z]+)\s*(=|!=|>=|<=|>|<)\s*"?([^"]*?)"?\s*$`)

type execTerm struct {
	field string
	op    string
	value string
}

// parseExecFilter understands AND-joined comparisons on executionID, state, workflowRevisionID, label.* and,
// for startTime and endTime, RFC 3339 ordering comparisons.
func parseExecFilter(filter string) (func(*executionspb.Execution) bool, error) {
	if strings.TrimSpace(filter) == "" {
		return func(*executionspb.Execution) bool { return true }, nil
	}

	var terms []execTerm

	for _, part := range andRe.Split(filter, -1) {
		m := filterTermRe.FindStringSubmatch(part)
		if m == nil {
			return nil, errInvalid("filter term %q is not supported.", strings.TrimSpace(part))
		}

		t := execTerm{field: m[1], op: m[2], value: m[3]}
		if err := checkTerm(t); err != nil {
			return nil, err
		}

		terms = append(terms, t)
	}

	return func(e *executionspb.Execution) bool {
		for _, t := range terms {
			if !t.match(e) {
				return false
			}
		}

		return true
	}, nil
}

func checkTerm(t execTerm) error {
	switch {
	case strings.HasPrefix(t.field, "label."):
		return nil
	case t.field == fieldState:
		if _, ok := executionspb.Execution_State_value[t.value]; !ok {
			return errInvalid("filter state %q is not a valid execution state.", t.value)
		}
	case t.field == fieldExecID || t.field == fieldRevision:
		if t.op != "=" && t.op != "!=" {
			return errInvalid("filter field %q only supports = and !=.", t.field)
		}
	case t.field == fieldStart || t.field == fieldEnd:
		if _, err := time.Parse(time.RFC3339Nano, t.value); err != nil {
			return errInvalid("filter %s value %q is not an RFC 3339 timestamp.", t.field, t.value)
		}
	default:
		return errInvalid("filter field %q is not supported.", t.field)
	}

	return nil
}

func (t execTerm) match(e *executionspb.Execution) bool {
	var got string

	switch {
	case strings.HasPrefix(t.field, "label."):
		got = e.GetLabels()[strings.TrimPrefix(t.field, "label.")]
	case t.field == fieldExecID:
		got = lastSegment(e.GetName())
	case t.field == fieldState:
		got = e.GetState().String()
	case t.field == fieldRevision:
		got = e.GetWorkflowRevisionId()
	case t.field == fieldStart:
		return cmpTime(e.GetStartTime().AsTime(), t)
	default:
		if e.GetEndTime() == nil {
			return false
		}

		return cmpTime(e.GetEndTime().AsTime(), t)
	}

	if t.op == "!=" {
		return got != t.value
	}

	return got == t.value
}

func cmpTime(got time.Time, t execTerm) bool {
	want, _ := time.Parse(time.RFC3339Nano, t.value)

	switch t.op {
	case "=":
		return got.Equal(want)
	case "!=":
		return !got.Equal(want)
	case ">":
		return got.After(want)
	case ">=":
		return !got.Before(want)
	case "<":
		return got.Before(want)
	default:
		return !got.After(want)
	}
}
