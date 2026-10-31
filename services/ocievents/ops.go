package ocievents

import (
	"net/http"
)

func buildOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateRule: (*Handler).createRule, opGetRule: (*Handler).getRule, opListRules: (*Handler).listRules,
		opUpdateRule: (*Handler).updateRule, opDeleteRule: (*Handler).deleteRule,
		opChangeRuleCompart: (*Handler).changeRuleCompartment,
	}
}

func (h *Handler) createRule(q *req) error {
	var in createRuleIn
	if err := q.decode(&in); err != nil {
		return err
	}

	w, etag, err := h.Backend.CreateRule(in)
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, etag)
}

func (h *Handler) getRule(q *req) error {
	w, etag, err := h.Backend.GetRule(q.ruleID())
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, etag)
}

func (h *Handler) listRules(q *req) error {
	pg, err := h.Backend.ListRules(q.query("compartmentId"), q.query("displayName"), q.query("lifecycleState"),
		q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func (h *Handler) updateRule(q *req) error {
	var in updateRuleIn
	if err := q.decode(&in); err != nil {
		return err
	}

	w, etag, err := h.Backend.UpdateRule(q.ruleID(), q.ifMatch(), in)
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, etag)
}

func (h *Handler) deleteRule(q *req) error {
	if err := h.Backend.DeleteRule(q.ruleID(), q.ifMatch()); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) changeRuleCompartment(q *req) error {
	var in changeCompartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	if err := h.Backend.ChangeRuleCompartment(q.ruleID(), q.ifMatch(), in); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusOK)

	return nil
}
