package azurearm

import (
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const logicRunType = namespaceMicrosoftLogic + "/" + logicWorkflowsType + "/runs"

func (w *logicWorkflow) correlation(client, fallback string) map[string]any {
	if client == "" {
		client = fallback
	}

	return map[string]any{logicKClientTrackingID: client}
}

func (w *logicWorkflow) runBody(r *logicRunRec, base string) map[string]any {
	trig := map[string]any{
		fieldName:       r.Trigger.Name,
		logicKStartTime: logicTime(r.Trigger.Start),
		logicKEndTime:   logicTime(r.Trigger.End),
		logicKScheduledTime: logicTime(
			r.Trigger.Scheduled,
		),
		logicKStatus:      r.Trigger.Status,
		logicKTrackingID:  r.Trigger.Tracking,
		logicKCorrelation: w.correlation(r.ClientID, r.Name),
	}

	prefix := "/workflows/" + w.FlowID + "/runs/" + r.Name + "/contents/"

	if r.Trigger.Inputs.Set {
		trig["inputsLink"] = w.contentLink(base, prefix+"TriggerInputs", r.Trigger.Inputs.Value)
	}

	if r.Trigger.Outputs.Set {
		trig["outputsLink"] = w.contentLink(base, prefix+"TriggerOutputs", r.Trigger.Outputs.Value)
	}

	props := map[string]any{
		"waitEndTime":     logicTime(r.Start),
		logicKStartTime:   logicTime(r.Start),
		logicKStatus:      r.Status,
		"correlationId":   r.Correlation,
		logicKCorrelation: w.correlation(r.ClientID, r.Name),
		logicKWorkflow:    w.versionRef(r),
		"trigger":         trig,
	}

	if !r.End.IsZero() && r.Status != logicStRunning {
		props[logicKEndTime] = logicTime(r.End)
	}

	if r.Code != "" {
		props[logicKCode] = r.Code
	}

	if r.Error != nil {
		props[logicKError] = r.Error
	}

	return map[string]any{
		logicFieldID: w.armID() + "/runs/" + r.Name, fieldName: r.Name, fieldType: logicRunType, fieldProperties: props,
	}
}

func (w *logicWorkflow) versionRef(r *logicRunRec) map[string]any {
	return logicRef(
		w.armID()+"/versions/"+r.Version,
		r.Version,
		namespaceMicrosoftLogic+"/"+logicWorkflowsType+"/versions",
	)
}

func (w *logicWorkflow) actionBody(r *logicRunRec, a *logicActionRec, base string) map[string]any {
	props := map[string]any{
		logicKStartTime:  logicTime(a.Start),
		logicKStatus:     a.Status,
		logicKTrackingID: a.Tracking,
		logicKCorrelation: map[string]any{
			"actionTrackingId":     a.Tracking,
			logicKClientTrackingID: orStr(r.ClientID, r.Name),
		},
	}

	if !a.End.IsZero() && a.Status != logicStRunning {
		props[logicKEndTime] = logicTime(a.End)
	}

	if a.Code != "" {
		props[logicKCode] = a.Code
	}

	if a.Error != nil {
		props[logicKError] = a.Error
	}

	prefix := "/workflows/" + w.FlowID + "/runs/" + r.Name + "/actions/" + a.Name + "/contents/"

	if a.Inputs.Set {
		props["inputsLink"] = w.contentLink(base, prefix+"ActionInputs", a.Inputs.Value)
	}

	if a.Outputs.Set {
		props["outputsLink"] = w.contentLink(base, prefix+"ActionOutputs", a.Outputs.Value)
	}

	return map[string]any{
		logicFieldID: w.armID() + "/runs/" + r.Name + "/actions/" + a.Name, fieldName: a.Name,
		fieldType: logicRunType + "/actions", fieldProperties: props,
	}
}

func orStr(a, b string) string {
	if a != "" {
		return a
	}

	return b
}

func (w *logicWorkflow) historyBody(h *logicHistory, base string) map[string]any {
	props := map[string]any{
		logicKStartTime: logicTime(
			h.Start,
		),
		logicKEndTime:       logicTime(h.End),
		logicKScheduledTime: logicTime(h.Scheduled),
		logicKStatus:        h.Status,
		"fired":             h.Fired,
		logicKTrackingID:    h.Tracking,
		logicKCorrelation:   map[string]any{logicKClientTrackingID: h.Name},
	}

	if h.Code != "" {
		props[logicKCode] = h.Code
	}

	if h.Error != nil {
		props[logicKError] = h.Error
	}

	if h.Run != "" {
		props["run"] = logicRef(w.armID()+"/runs/"+h.Run, h.Run, logicRunType)
	}

	prefix := "/workflows/" + w.FlowID + "/triggers/" + h.Trigger + "/histories/" + h.Name + "/contents/"

	if h.Inputs.Set {
		props["inputsLink"] = w.contentLink(base, prefix+"TriggerInputs", h.Inputs.Value)
	}

	if h.Outputs.Set {
		props["outputsLink"] = w.contentLink(base, prefix+"TriggerOutputs", h.Outputs.Value)
	}

	return map[string]any{
		logicFieldID: w.armID() + "/triggers/" + h.Trigger + "/histories/" + h.Name, fieldName: h.Name,
		fieldType: namespaceMicrosoftLogic + "/" + logicWorkflowsType + "/triggers/histories", fieldProperties: props,
	}
}

type logicCond struct {
	field string
	op    string
	value string
}

// parseLogicFilter reads the $filter subset the service documents: <field> eq|gt|ge|lt|le '<value>' joined by and.
func parseLogicFilter(s string) ([]logicCond, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}

	var conds []logicCond

	for part := range strings.SplitSeq(s, " and ") {
		f := strings.Fields(strings.TrimSpace(part))
		if len(f) < filterMinParts {
			return nil, logicBadRequest("InvalidFilter", "The $filter expression '%s' is not valid.", s)
		}

		op := strings.ToLower(f[1])
		switch op {
		case "eq", "gt", "ge", "lt", "le":
		default:
			return nil, logicBadRequest("InvalidFilter", "The $filter operator '%s' is not supported.", f[1])
		}

		conds = append(conds, logicCond{
			field: strings.ToLower(f[0]), op: op, value: strings.Trim(strings.Join(f[2:], " "), "'"),
		})
	}

	return conds, nil
}

type logicFilterable struct {
	start  time.Time
	end    time.Time
	status string
}

func (c logicCond) matches(f logicFilterable) bool {
	switch c.field {
	case logicKStatus, logicKState:
		return strings.EqualFold(f.status, c.value)
	case "starttime", "endtime":
		ts, err := parseLogicTime(c.value)
		if err != nil {
			return false
		}

		t := f.start
		if c.field == "endtime" {
			t = f.end
		}

		switch c.op {
		case "gt":
			return t.After(ts.t)
		case "ge":
			return !t.Before(ts.t)
		case "lt":
			return t.Before(ts.t)
		case "le":
			return !t.After(ts.t)
		}

		return t.Equal(ts.t)
	}

	return true
}

func filterMatches(conds []logicCond, f logicFilterable) bool {
	for _, c := range conds {
		if !c.matches(f) {
			return false
		}
	}

	return true
}

// logicQuery carries the list query parameters.
type logicQuery struct {
	token  string
	filter []logicCond
	top    int
}

func pageLogic[T any](all []T, q logicQuery) page.Page[T] {
	top := q.top
	if top <= 0 {
		top = logicDefaultTop
	}

	return page.New(all, q.token, min(top, logicMaxTop), logicDefaultTop)
}

func sortedRuns(w *logicWorkflow, q logicQuery) []*logicRunRec {
	var out []*logicRunRec

	for _, r := range slices.Backward(w.Runs) {
		if filterMatches(q.filter, logicFilterable{start: r.Start, end: r.End, status: r.Status}) {
			out = append(out, r)
		}
	}

	sort.SliceStable(out, func(i, j int) bool { return out[i].Start.After(out[j].Start) })

	return out
}
