package azurearm

import (
	"net/http"
	"slices"
	"sort"
	"strings"
)

func (h *Handler) logicListVersions(q logicReq) error {
	return h.logicList(q, func(w *logicWorkflow, _ logicQuery) ([]map[string]any, error) {
		out := make([]map[string]any, 0, len(w.Versions))
		for _, v := range slices.Backward(w.Versions) {
			out = append(out, q.lp.versionBody(w, v, q.base))
		}

		return out, nil
	})
}

func (h *Handler) logicGetVersion(q logicReq) error {
	out, err := q.lp.read(q.ref, func(w *logicWorkflow) (any, error) {
		for _, v := range w.Versions {
			if strings.EqualFold(v.Version, q.rest[1]) {
				return q.lp.versionBody(w, v, q.base), nil
			}
		}

		return nil, ErrLogicVersionNotFound
	})

	return h.logicReply(q, out, err)
}

func (h *Handler) logicListTriggers(q logicReq) error {
	return h.logicList(q, func(w *logicWorkflow, lq logicQuery) ([]map[string]any, error) {
		out := []map[string]any{}

		for _, n := range sortedTriggerNames(w) {
			b := q.lp.triggerBody(w, n)
			props, _ := objectOf(b[fieldProperties])

			if filterMatches(lq.filter, logicFilterable{status: stringOf(props[logicKState])}) {
				out = append(out, b)
			}
		}

		return out, nil
	})
}

func (h *Handler) logicGetTrigger(q logicReq) error {
	out, err := q.lp.read(q.ref, func(w *logicWorkflow) (any, error) {
		name, ok := matchTriggerName(w, q.rest[1])
		if !ok {
			return nil, ErrLogicTriggerNotFound
		}

		return q.lp.triggerBody(w, name), nil
	})

	return h.logicReply(q, out, err)
}

func matchTriggerName(w *logicWorkflow, name string) (string, bool) {
	if _, ok := w.Triggers[name]; ok {
		return name, true
	}

	for n := range w.Triggers {
		if strings.EqualFold(n, name) {
			return n, true
		}
	}

	return "", false
}

func (h *Handler) logicTriggerWrite(q logicReq, f func(w *logicWorkflow, name string) error) error {
	_, err := q.lp.write(q.ref, func(w *logicWorkflow) (any, error) {
		name, ok := matchTriggerName(w, q.rest[1])
		if !ok {
			return nil, ErrLogicTriggerNotFound
		}

		return nil, f(w, name)
	})
	if err != nil {
		return h.writeAPIError(q.c, err)
	}

	return q.c.NoContent(http.StatusOK)
}

func (h *Handler) logicRunTrigger(q logicReq) error {
	return h.logicTriggerWrite(q, q.lp.runTriggerLocked)
}

func (h *Handler) logicResetTrigger(q logicReq) error {
	return h.logicTriggerWrite(q, q.lp.resetTriggerLocked)
}

func (h *Handler) logicSetTriggerState(q logicReq) error {
	body, err := decodeJSONBody(q.c.Request())
	if err != nil {
		return h.writeAPIError(q.c, err)
	}

	if err = q.lp.setTriggerState(q.ref, q.rest[1], body); err != nil {
		return h.writeAPIError(q.c, err)
	}

	return q.c.NoContent(http.StatusOK)
}

func (h *Handler) logicTriggerSchema(q logicReq) error {
	if !strings.EqualFold(q.rest[3], "json") {
		return h.writeNotFound(q.c)
	}

	out, err := q.lp.triggerSchema(q.ref, q.rest[1])

	return h.logicReply(q, out, err)
}

func (h *Handler) logicListHistories(q logicReq) error {
	return h.logicList(q, func(w *logicWorkflow, lq logicQuery) ([]map[string]any, error) {
		name, ok := matchTriggerName(w, q.rest[1])
		if !ok {
			return nil, ErrLogicTriggerNotFound
		}

		out := []map[string]any{}

		for _, hist := range slices.Backward(w.Histories) {
			if hist.Trigger == name &&
				filterMatches(lq.filter, logicFilterable{start: hist.Start, end: hist.End, status: hist.Status}) {
				out = append(out, w.historyBody(hist, q.base))
			}
		}

		return out, nil
	})
}

func (h *Handler) logicGetHistory(q logicReq) error {
	out, err := q.lp.read(q.ref, func(w *logicWorkflow) (any, error) {
		name, ok := matchTriggerName(w, q.rest[1])
		if !ok {
			return nil, ErrLogicTriggerNotFound
		}

		for _, hist := range w.Histories {
			if hist.Trigger == name && strings.EqualFold(hist.Name, q.rest[3]) {
				return w.historyBody(hist, q.base), nil
			}
		}

		return nil, ErrLogicHistoryNotFound
	})

	return h.logicReply(q, out, err)
}

func (h *Handler) logicListRuns(q logicReq) error {
	return h.logicList(q, func(w *logicWorkflow, lq logicQuery) ([]map[string]any, error) {
		runs := sortedRuns(w, lq)
		out := make([]map[string]any, 0, len(runs))

		for _, r := range runs {
			out = append(out, w.runBody(r, q.base))
		}

		return out, nil
	})
}

func findRun(w *logicWorkflow, name string) (*logicRunRec, bool) {
	for _, r := range w.Runs {
		if strings.EqualFold(r.Name, name) {
			return r, true
		}
	}

	return nil, false
}

func (h *Handler) logicGetRun(q logicReq) error {
	out, err := q.lp.read(q.ref, func(w *logicWorkflow) (any, error) {
		r, ok := findRun(w, q.rest[1])
		if !ok {
			return nil, ErrLogicRunNotFound
		}

		return w.runBody(r, q.base), nil
	})

	return h.logicReply(q, out, err)
}

func (h *Handler) logicCancelRun(q logicReq) error {
	live, err := q.lp.write(q.ref, func(w *logicWorkflow) (any, error) { return q.lp.cancelRunLocked(w, q.rest[1]) })
	if err != nil {
		return h.writeAPIError(q.c, err)
	}

	if l, ok := live.(*logicLive); ok {
		logicCancelRun(l)
	}

	return q.c.NoContent(http.StatusOK)
}

func (h *Handler) logicListActions(q logicReq) error {
	return h.logicList(q, func(w *logicWorkflow, lq logicQuery) ([]map[string]any, error) {
		r, ok := findRun(w, q.rest[1])
		if !ok {
			return nil, ErrLogicRunNotFound
		}

		names := make([]string, 0, len(r.Actions))
		for n := range r.Actions {
			names = append(names, n)
		}

		sort.Slice(names, func(i, j int) bool { return r.Actions[names[i]].Start.Before(r.Actions[names[j]].Start) })

		out := []map[string]any{}

		for _, n := range names {
			a := r.Actions[n]
			if filterMatches(lq.filter, logicFilterable{start: a.Start, end: a.End, status: a.Status}) {
				out = append(out, w.actionBody(r, a, q.base))
			}
		}

		return out, nil
	})
}

func (h *Handler) logicGetAction(q logicReq) error {
	out, err := q.lp.read(q.ref, func(w *logicWorkflow) (any, error) {
		r, ok := findRun(w, q.rest[1])
		if !ok {
			return nil, ErrLogicRunNotFound
		}

		a, ok := r.Actions[q.rest[3]]
		if !ok {
			return nil, ErrLogicActionNotFound
		}

		return w.actionBody(r, a, q.base), nil
	})

	return h.logicReply(q, out, err)
}
