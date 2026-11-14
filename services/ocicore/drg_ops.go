package ocicore

import (
	"net/http"
	"strconv"
)

func drgAttachmentFilter(q *req) func(Resource) bool {
	return func(r Resource) bool {
		a, ok := r.(*DrgAttachment)
		if !ok {
			return false
		}

		nd := a.NetworkDetails
		typ := q.query("attachmentType")

		return (q.query("drgId") == "" || a.DrgID == q.query("drgId")) &&
			(q.query("drgRouteTableId") == "" || a.DrgRouteTableID == q.query("drgRouteTableId")) &&
			(q.query("networkId") == "" || (nd != nil && nd.ID == q.query("networkId"))) &&
			(typ == "" || typ == "ALL" || attachmentType(a) == typ) &&
			q.query("drgNatPolicyId") == ""
	}
}

func drgChildFilter(q *req) func(Resource) bool {
	drgID, imp := q.query("drgId"), q.query("importDrgRouteDistributionId")

	return func(r Resource) bool {
		switch v := r.(type) {
		case *DrgRouteTable:
			return v.DrgID == drgID && (imp == "" || v.ImportDrgRouteDistributionID == imp)
		case *DrgRouteDistribution:
			return v.DrgID == drgID
		}

		return false
	}
}

// listByParent is listOp for resources scoped by drgId instead of compartmentId.
func listByParent[T Resource](extra func(*req) func(Resource) bool) opFunc {
	return func(h *Handler, q *req) error {
		if q.query("drgId") == "" {
			return errMissing("drgId is required")
		}

		o, err := q.listOpts()
		if err != nil {
			return err
		}

		o.AnyCompartment, o.Match = true, extra(q)

		pg, aerr := list[T](h.Backend, o)
		if aerr != nil {
			return aerr
		}

		q.nextPage(pg.Next)

		return q.json(http.StatusOK, pg.Data, "")
	}
}

func drgEntries() []entry {
	drg, att := idPath("/drgs", "drgId"), idPath("/drgAttachments", "drgAttachmentId")
	tbl, dist := idPath(
		"/drgRouteTables",
		"drgRouteTableId",
	), idPath(
		"/drgRouteDistributions",
		"drgRouteDistributionId",
	)

	return []entry{
		{createOp((*InMemoryBackend).CreateDrg), http.MethodPost, "/drgs", "CreateDrg"},
		{listOp[*Drg](nil), http.MethodGet, "/drgs", "ListDrgs"},
		{getOp[*Drg]("drgId"), http.MethodGet, drg, "GetDrg"},
		{updateOp("drgId", (*InMemoryBackend).UpdateDrg), http.MethodPut, drg, "UpdateDrg"},
		{deleteOp[*Drg]("drgId"), http.MethodDelete, drg, "DeleteDrg"},
		{(*Handler).changeDrgCompartment, http.MethodPost, drg + "/actions/changeCompartment", "ChangeDrgCompartment"},
		{(*Handler).getDrgRedundancy, http.MethodGet, drg + "/redundancyStatus", "GetDrgRedundancyStatus"},
		{(*Handler).getUpgradeStatus, http.MethodGet, drg + "/actions/upgradeStatus", "GetUpgradeStatus"},
		{(*Handler).upgradeDrg, http.MethodPost, drg + "/actions/upgrade", "UpgradeDrg"},
		{
			(*Handler).getAllDrgAttachments,
			http.MethodPost,
			drg + "/actions/getAllDrgAttachments",
			"GetAllDrgAttachments",
		},

		{createOp((*InMemoryBackend).CreateDrgAttachment), http.MethodPost, "/drgAttachments", "CreateDrgAttachment"},
		{listOp[*DrgAttachment](drgAttachmentFilter), http.MethodGet, "/drgAttachments", "ListDrgAttachments"},
		{getOp[*DrgAttachment]("drgAttachmentId"), http.MethodGet, att, "GetDrgAttachment"},
		{
			updateOp("drgAttachmentId", (*InMemoryBackend).UpdateDrgAttachment),
			http.MethodPut,
			att,
			"UpdateDrgAttachment",
		},
		{(*Handler).deleteDrgAttachment, http.MethodDelete, att, "DeleteDrgAttachment"},
		{(*Handler).removeExportDist, http.MethodPost, att + "/actions/removeExportDrgRouteDistribution",
			"RemoveExportDrgRouteDistribution"},

		{createOp((*InMemoryBackend).CreateDrgRouteTable), http.MethodPost, "/drgRouteTables", "CreateDrgRouteTable"},
		{listByParent[*DrgRouteTable](drgChildFilter), http.MethodGet, "/drgRouteTables", "ListDrgRouteTables"},
		{getOp[*DrgRouteTable]("drgRouteTableId"), http.MethodGet, tbl, "GetDrgRouteTable"},
		{
			updateOp("drgRouteTableId", (*InMemoryBackend).UpdateDrgRouteTable),
			http.MethodPut,
			tbl,
			"UpdateDrgRouteTable",
		},
		{deleteOp[*DrgRouteTable]("drgRouteTableId"), http.MethodDelete, tbl, "DeleteDrgRouteTable"},
		{(*Handler).removeImportDist, http.MethodPost, tbl + "/actions/removeImportDrgRouteDistribution",
			"RemoveImportDrgRouteDistribution"},
		{(*Handler).addDrgRules, http.MethodPost, tbl + "/actions/addDrgRouteRules", "AddDrgRouteRules"},
		{(*Handler).updateDrgRules, http.MethodPost, tbl + "/actions/updateDrgRouteRules", "UpdateDrgRouteRules"},
		{(*Handler).removeDrgRules, http.MethodPost, tbl + "/actions/removeDrgRouteRules", "RemoveDrgRouteRules"},
		{(*Handler).listDrgRules, http.MethodGet, tbl + "/drgRouteRules", "ListDrgRouteRules"},

		{createOp((*InMemoryBackend).CreateDrgRouteDistribution), http.MethodPost, "/drgRouteDistributions",
			"CreateDrgRouteDistribution"},
		{listByParent[*DrgRouteDistribution](drgChildFilter), http.MethodGet, "/drgRouteDistributions",
			"ListDrgRouteDistributions"},
		{getOp[*DrgRouteDistribution]("drgRouteDistributionId"), http.MethodGet, dist, "GetDrgRouteDistribution"},
		{updateOp("drgRouteDistributionId", (*InMemoryBackend).UpdateDrgRouteDistribution), http.MethodPut, dist,
			"UpdateDrgRouteDistribution"},
		{deleteOp[*DrgRouteDistribution]("drgRouteDistributionId"), http.MethodDelete, dist,
			"DeleteDrgRouteDistribution"},
		{(*Handler).addDrgStatements, http.MethodPost, dist + "/actions/addDrgRouteDistributionStatements",
			"AddDrgRouteDistributionStatements"},
		{(*Handler).updateDrgStatements, http.MethodPost, dist + "/actions/updateDrgRouteDistributionStatements",
			"UpdateDrgRouteDistributionStatements"},
		{(*Handler).removeDrgStatements, http.MethodPost, dist + "/actions/removeDrgRouteDistributionStatements",
			"RemoveDrgRouteDistributionStatements"},
		{(*Handler).listDrgStatements, http.MethodGet, dist + "/drgRouteDistributionStatements",
			"ListDrgRouteDistributionStatements"},
	}
}

func (h *Handler) changeDrgCompartment(q *req) error {
	var in struct {
		CompartmentID string `json:"compartmentId"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	return noBody(q, h.Backend.ChangeDrgCompartment(q.param("drgId"), in.CompartmentID, q.ifMatch()))
}

func (h *Handler) getDrgRedundancy(q *req) error {
	out, aerr := h.Backend.GetDrgRedundancyStatus(q.param("drgId"))
	if aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, out, "")
}

func (h *Handler) getUpgradeStatus(q *req) error {
	out, aerr := h.Backend.GetUpgradeStatus(q.param("drgId"))
	if aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, out, "")
}

func (h *Handler) upgradeDrg(q *req) error {
	id, aerr := h.Backend.UpgradeDrg(q.param("drgId"))
	if aerr != nil {
		return aerr
	}

	return q.acceptWorkRequest(id)
}

func (h *Handler) getAllDrgAttachments(q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	var cross *bool

	if s := q.query("isCrossTenancy"); s != "" {
		v, perr := strconv.ParseBool(s)
		if perr != nil {
			return errParam("isCrossTenancy must be true or false")
		}

		cross = &v
	}

	pg, aerr := h.Backend.GetAllDrgAttachments(q.param("drgId"), q.query("attachmentType"), o.Page, o.Limit, cross)
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func (h *Handler) deleteDrgAttachment(q *req) error {
	return noBody(q, h.Backend.DeleteDrgAttachment(q.param("drgAttachmentId"), q.ifMatch()))
}

func (h *Handler) removeExportDist(q *req) error {
	v, etag, aerr := h.Backend.RemoveExportDrgRouteDistribution(q.param("drgAttachmentId"), q.ifMatch())

	return send(q, v, etag, aerr)
}

func (h *Handler) removeImportDist(q *req) error {
	v, etag, aerr := h.Backend.RemoveImportDrgRouteDistribution(q.param("drgRouteTableId"), q.ifMatch())

	return send(q, v, etag, aerr)
}

// items writes a bare JSON array body.
func items[T any](q *req, v []T, aerr *APIError) error {
	if aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, v, "")
}

func (h *Handler) addDrgRules(q *req) error {
	var in AddDrgRulesIn
	if err := q.decode(&in); err != nil {
		return err
	}

	out, aerr := h.Backend.AddDrgRouteRules(q.param("drgRouteTableId"), in)

	return items(q, out, aerr)
}

func (h *Handler) updateDrgRules(q *req) error {
	var in UpdateDrgRulesIn
	if err := q.decode(&in); err != nil {
		return err
	}

	out, aerr := h.Backend.UpdateDrgRouteRules(q.param("drgRouteTableId"), in)

	return items(q, out, aerr)
}

func (h *Handler) removeDrgRules(q *req) error {
	var in struct {
		RouteRuleIDs []string `json:"routeRuleIds"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	return noBody(q, h.Backend.RemoveDrgRouteRules(q.param("drgRouteTableId"), in.RouteRuleIDs))
}

func (h *Handler) listDrgRules(q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	pg, aerr := h.Backend.ListDrgRouteRules(q.param("drgRouteTableId"), q.query("routeType"), o.Page, o.Limit)
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func (h *Handler) addDrgStatements(q *req) error {
	var in AddDrgStatementsIn
	if err := q.decode(&in); err != nil {
		return err
	}

	out, aerr := h.Backend.AddDrgRouteDistributionStatements(q.param("drgRouteDistributionId"), in)

	return items(q, out, aerr)
}

func (h *Handler) updateDrgStatements(q *req) error {
	var in UpdateDrgStatementsIn
	if err := q.decode(&in); err != nil {
		return err
	}

	out, aerr := h.Backend.UpdateDrgRouteDistributionStatements(q.param("drgRouteDistributionId"), in)

	return items(q, out, aerr)
}

func (h *Handler) removeDrgStatements(q *req) error {
	var in struct {
		StatementIDs []string `json:"statementIds"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	return noBody(q, h.Backend.RemoveDrgRouteDistributionStatements(q.param("drgRouteDistributionId"), in.StatementIDs))
}

func (h *Handler) listDrgStatements(q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	pg, aerr := h.Backend.ListDrgRouteDistributionStatements(
		q.param("drgRouteDistributionId"), q.query("sortOrder"), o.Page, o.Limit)
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}
