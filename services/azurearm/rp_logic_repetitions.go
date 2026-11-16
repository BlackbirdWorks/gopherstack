package azurearm

import (
	"fmt"
	"sort"
	"strings"
)

const logicSegRepetitions = "repetitions"

func repetitionName(idx []logicRepIndex) string {
	var b strings.Builder

	for _, i := range idx {
		fmt.Fprintf(&b, "%06d", i.Item)
	}

	return b.String()
}

func frameIndexes(e *logicEnv) []logicRepIndex {
	var rev []logicRepIndex

	for cur := e; cur != nil; cur = cur.parent {
		if cur.frame != nil {
			rev = append(rev, logicRepIndex{Scope: cur.frame.name, Item: cur.frame.index})
		}
	}

	out := make([]logicRepIndex, len(rev))
	for i, v := range rev {
		out[len(rev)-1-i] = v
	}

	return out
}

// recordRepetitions stores each loop iteration and every action record inside it on the run.
func (rs *logicRunState) recordRepetitions(inner map[string]any, iters []*logicEnv) {
	rs.p.mu.Lock("recordRepetitions")
	defer rs.p.mu.Unlock()

	for _, e := range iters {
		if e == nil || e.frame == nil {
			continue
		}

		idx := frameIndexes(e)
		name := repetitionName(idx)
		scope := &logicActionRec{Name: e.frame.name, Status: logicStSkipped, Code: logicCodeSkipped}

		names := make([]string, 0, len(e.local))
		for n := range e.local {
			if _, ok := inner[n]; ok {
				names = append(names, n)
			}
		}

		sort.Strings(names)

		for _, n := range names {
			rec := e.local[n]
			mergeAggregate(scope, rec)
			rs.run.Repetitions = append(rs.run.Repetitions, &logicRepRec{
				Action: n, Name: name, Indexes: idx, Rec: *rec,
			})
		}

		scope.Tracking = e.frame.name + "/" + name
		rs.run.Repetitions = append(rs.run.Repetitions, &logicRepRec{
			Action: e.frame.name, Name: name, Indexes: idx, Rec: *scope, Scope: true,
		})
	}
}

func (w *logicWorkflow) repetitionBody(r *logicRunRec, rep *logicRepRec, base string) map[string]any {
	seg, typ := logicSegRepetitions, logicSegRepetitions
	if rep.Scope {
		seg, typ = "scopeRepetitions", "scopeRepetitions"
	}

	prefix := "/workflows/" + w.FlowID + "/runs/" + r.Name + "/actions/" + rep.Action + "/" + seg + "/" + rep.Name +
		"/contents/"
	props := w.actionProps(r, &rep.Rec, base, prefix)

	idx := make([]map[string]any, 0, len(rep.Indexes))
	for _, i := range rep.Indexes {
		idx = append(idx, map[string]any{"scopeName": i.Scope, "itemIndex": i.Item})
	}

	props["repetitionIndexes"] = idx

	return map[string]any{
		logicFieldID: w.armID() + "/runs/" + r.Name + "/actions/" + rep.Action + "/" + seg + "/" + rep.Name,
		fieldName:    rep.Name, fieldType: logicRunType + "/actions/" + typ, fieldProperties: props,
	}
}

func (w *logicWorkflow) repetitionsOf(r *logicRunRec, action string, scope bool) ([]*logicRepRec, error) {
	_, known := r.Actions[action]

	var out []*logicRepRec

	for _, rep := range r.Repetitions {
		if rep.Action == action && rep.Scope == scope {
			out = append(out, rep)
		}

		if rep.Action == action {
			known = true
		}
	}

	if !known {
		return nil, ErrLogicActionNotFound
	}

	return out, nil
}

func (h *Handler) logicListRepetitions(q logicReq, scope bool) error {
	return h.logicList(q, func(w *logicWorkflow, lq logicQuery) ([]map[string]any, error) {
		r, ok := findRun(w, q.rest[1])
		if !ok {
			return nil, ErrLogicRunNotFound
		}

		reps, err := w.repetitionsOf(r, q.rest[3], scope)
		if err != nil {
			return nil, err
		}

		out := []map[string]any{}

		for _, rep := range reps {
			f := logicFilterable{start: rep.Rec.Start, end: rep.Rec.End, status: rep.Rec.Status}
			if filterMatches(lq.filter, f) {
				out = append(out, w.repetitionBody(r, rep, q.base))
			}
		}

		return out, nil
	})
}

func (h *Handler) logicGetRepetition(q logicReq, scope bool) error {
	out, err := q.lp.read(q.ref, func(w *logicWorkflow) (any, error) {
		r, ok := findRun(w, q.rest[1])
		if !ok {
			return nil, ErrLogicRunNotFound
		}

		reps, rerr := w.repetitionsOf(r, q.rest[3], scope)
		if rerr != nil {
			return nil, rerr
		}

		for _, rep := range reps {
			if rep.Name == q.rest[5] {
				return w.repetitionBody(r, rep, q.base), nil
			}
		}

		return nil, ErrLogicRepNotFound
	})

	return h.logicReply(q, out, err)
}

func (w *logicWorkflow) repetitionContent(run, action, repetition, kind string) (logicContent, bool) {
	for _, r := range w.Runs {
		if r.Name != run {
			continue
		}

		for _, rep := range r.Repetitions {
			if rep.Action == action && rep.Name == repetition && !rep.Scope {
				return pickContent("ActionInputs", "ActionOutputs", kind, rep.Rec.Inputs, rep.Rec.Outputs)
			}
		}
	}

	return logicContent{}, false
}
