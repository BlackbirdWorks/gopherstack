package ocimonitoring

import (
	"net/http"
	"strings"
)

const itemsKey = "items"

func (h *Handler) createSuppression(q *req) error {
	var in CreateSuppressionIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.RetryToken = q.r.Header.Get(headerRetry)

	v, etag, err := h.Backend.CreateAlarmSuppression(in)

	return reply(q, v, etag, err)
}

func (h *Handler) getSuppression(q *req) error {
	v, etag, err := h.Backend.GetAlarmSuppression(q.rt.params["alarmSuppressionId"])

	return reply(q, v, etag, err)
}

func (h *Handler) deleteSuppression(q *req) error {
	if err := h.Backend.DeleteAlarmSuppression(q.rt.params["alarmSuppressionId"], q.ifMatch()); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) listSuppressions(q *req) error {
	pg, err := h.Backend.ListAlarmSuppressions(SuppressionFilter{
		AlarmID: q.query("alarmId"), Name: q.query("displayName"), State: q.query("lifecycleState"),
		Level: q.query("level"), Compartment: q.query("compartmentId"), TargetType: q.query("targetType"),
		Subtree: strings.EqualFold(q.query("compartmentIdInSubtree"), "true"),
		All:     strings.EqualFold(q.query("isAllSuppressions"), "true"),
	}, q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, map[string]any{itemsKey: pg.Data}, "")
}

func (h *Handler) removeSuppression(q *req) error {
	if err := h.Backend.RemoveAlarmSuppression(q.alarmID(), q.ifMatch()); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) suppressionHistory(q *req) error {
	var in SuppressionHistoryIn
	if len(q.body) > 0 {
		if err := q.decode(&in); err != nil {
			return err
		}
	}

	pg, err := h.Backend.SummarizeAlarmSuppressionHistory(q.alarmID(), in, q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, map[string]any{itemsKey: pg.Data}, "")
}

func (h *Handler) dimensionStates(q *req) error {
	var in DimensionStatesIn
	if len(q.body) > 0 {
		if err := q.decode(&in); err != nil {
			return err
		}
	}

	out, next, err := h.Backend.RetrieveDimensionStates(q.alarmID(), in, q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(next)

	return q.json(http.StatusOK, out, "")
}
