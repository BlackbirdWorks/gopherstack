package ocidns

import (
	"cmp"
	"net/http"
	"strconv"
	"time"
)

func buildOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateZone:          (*Handler).createZone,
		opGetZone:             (*Handler).getZone,
		opListZones:           (*Handler).listZones,
		opUpdateZone:          (*Handler).updateZone,
		opDeleteZone:          (*Handler).deleteZone,
		opChangeZoneComp:      (*Handler).changeZoneCompartment,
		opGetZoneContent:      (*Handler).getZoneContent,
		opCreateZoneFromFile:  (*Handler).createZoneFromFile,
		opListTransferServers: (*Handler).listTransferServers,
		opGetZoneRecords:      (*Handler).getZoneRecords,
		opPatchZoneRecords:    (*Handler).patchRecords,
		opUpdateZoneRecords:   (*Handler).updateRecords,
		opGetDomainRecords:    (*Handler).getDomainRecords,
		opPatchDomainRecords:  (*Handler).patchRecords,
		opUpdateDomainRecords: (*Handler).updateRecords,
		opDeleteDomainRecords: (*Handler).deleteRecords,
		opGetRRSet:            (*Handler).getRRSet,
		opPatchRRSet:          (*Handler).patchRecords,
		opUpdateRRSet:         (*Handler).updateRecords,
		opDeleteRRSet:         (*Handler).deleteRecords,
		opCreateView:          (*Handler).createView,
		opGetView:             (*Handler).getView,
		opListViews:           (*Handler).listViews,
		opUpdateView:          (*Handler).updateView,
		opDeleteView:          (*Handler).deleteView,
		opChangeViewComp:      (*Handler).changeViewCompartment,
		opGetResolver:         (*Handler).getResolver,
		opListResolvers:       (*Handler).listResolvers,
		opUpdateResolver:      (*Handler).updateResolver,
		opChangeResolverComp:  (*Handler).changeResolverCompartment,
		opCreateEndpoint:      (*Handler).createEndpoint,
		opGetEndpoint:         (*Handler).getEndpoint,
		opListEndpoints:       (*Handler).listEndpoints,
		opUpdateEndpoint:      (*Handler).updateEndpoint,
		opDeleteEndpoint:      (*Handler).deleteEndpoint,
		opCreateTsigKey:       (*Handler).createTsigKey,
		opGetTsigKey:          (*Handler).getTsigKey,
		opListTsigKeys:        (*Handler).listTsigKeys,
		opUpdateTsigKey:       (*Handler).updateTsigKey,
		opDeleteTsigKey:       (*Handler).deleteTsigKey,
		opChangeTsigKeyComp:   (*Handler).changeTsigKeyCompartment,
		opCreatePolicy:        (*Handler).createPolicy,
		opGetPolicy:           (*Handler).getPolicy,
		opListPolicies:        (*Handler).listPolicies,
		opUpdatePolicy:        (*Handler).updatePolicy,
		opDeletePolicy:        (*Handler).deletePolicy,
		opChangePolicyComp:    (*Handler).changePolicyCompartment,
		opCreateAttachment:    (*Handler).createAttachment,
		opGetAttachment:       (*Handler).getAttachment,
		opListAttachments:     (*Handler).listAttachments,
		opUpdateAttachment:    (*Handler).updateAttachment,
		opDeleteAttachment:    (*Handler).deleteAttachment,
	}
}

// withBody decodes the request body into In, then runs call.
func withBody[In any](q *req, call func(in In) error) error {
	var in In
	if err := q.decode(&in); err != nil {
		return err
	}

	return call(in)
}

// get writes a 200 response, or 304 when the request's conditional headers match.
func (q *req) get(v any, t tag, err error) error {
	if err != nil {
		return err
	}

	if inm := q.r.Header.Get("If-None-Match"); inm != "" && etagMatches(inm, t.ETag) {
		q.header("etag", t.ETag)
		q.w.WriteHeader(http.StatusNotModified)

		return nil
	}

	if since, perr := http.ParseTime(q.r.Header.Get("If-Modified-Since")); perr == nil &&
		q.r.Header.Get("If-None-Match") == "" && !t.Modified.Truncate(time.Second).After(since) {
		q.header("etag", t.ETag)
		q.w.WriteHeader(http.StatusNotModified)

		return nil
	}

	return q.write(http.StatusOK, v, t)
}

func (q *req) ok(v any, t tag, err error) error {
	if err != nil {
		return err
	}

	return q.write(http.StatusOK, v, t)
}

func (q *req) accepted(v any, t tag, location string, err error) error {
	if err != nil {
		return err
	}

	q.header("location", location)

	return q.write(http.StatusAccepted, v, t)
}

func noContent(q *req, err error) error {
	if err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusNoContent)

	return nil
}

func accepted(q *req, err error) error {
	if err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusAccepted)

	return nil
}

func listOut[T any](q *req, res listResult[T], err error, totals bool) error {
	if err != nil {
		return err
	}

	if res.Next != "" {
		q.header("opc-next-page", res.Next)
	}

	if totals {
		q.header("opc-total-items", strconv.Itoa(res.Total))
	}

	return q.write(http.StatusOK, res.Items, tag{})
}

func (q *req) zoneRef(param string) ZoneRef {
	return ZoneRef{Ref: q.param(param), Scope: q.query("scope"), ViewID: q.query("viewId")}
}

func (h *Handler) createZone(q *req) error {
	return withBody(q, func(in CreateZoneIn) error {
		in.CompartmentID = cmp.Or(in.CompartmentID, q.query("compartmentId"))
		in.Scope = cmp.Or(in.Scope, q.query("scope"))
		in.ViewID = cmp.Or(in.ViewID, q.query("viewId"))

		w, t, err := h.Backend.CreateZone(in)

		return q.accepted(w, t, w.Self, err)
	})
}

func (h *Handler) createZoneFromFile(q *req) error {
	if len(q.body) == 0 {
		return errMissing("A zone file body is required")
	}

	w, t, err := h.Backend.CreateZoneFromFile(q.query("compartmentId"), q.query("scope"), q.query("viewId"),
		string(q.body))

	return q.accepted(w, t, w.Self, err)
}

func (h *Handler) getZone(q *req) error {
	w, t, err := h.Backend.GetZone(q.zoneRef("zoneNameOrId"))

	return q.get(w, t, err)
}

func (h *Handler) getZoneContent(q *req) error {
	text, t, err := h.Backend.ZoneContent(q.zoneRef("zoneNameOrId"))
	if err != nil {
		return err
	}

	q.header("etag", t.ETag)
	q.header("Content-Type", "text/dns")
	q.w.WriteHeader(http.StatusOK)
	_, _ = q.w.Write([]byte(text))

	return nil
}

func (h *Handler) listZones(q *req) error {
	f := ZoneFilter{
		Compart: q.query("compartmentId"), Name: q.query(keyName), Contains: q.query("nameContains"),
		ZoneType: q.query("zoneType"), State: q.query("lifecycleState"), Scope: q.query("scope"),
		ViewID: q.query("viewId"), TsigKeyID: q.query("tsigKeyId"), Dnssec: q.query("dnssecState"),
		Created: q.timeFilter(),
	}
	res, err := h.Backend.ListZones(f, q.listOpts())

	return listOut(q, res, err, true)
}

func (h *Handler) updateZone(q *req) error {
	return withBody(q, func(in UpdateZoneIn) error {
		w, t, err := h.Backend.UpdateZone(q.zoneRef("zoneNameOrId"), q.cond(), in)

		return q.ok(w, t, err)
	})
}

func (h *Handler) deleteZone(q *req) error {
	return noContent(q, h.Backend.DeleteZone(q.zoneRef("zoneNameOrId"), q.cond()))
}

func (h *Handler) changeZoneCompartment(q *req) error {
	return withBody(q, func(in ChangeCompartmentIn) error {
		return accepted(q, h.Backend.ChangeZoneCompartment(q.zoneRef("zoneId"), q.cond(), in))
	})
}

func (h *Handler) listTransferServers(q *req) error {
	res, err := h.Backend.ListZoneTransferServers(q.query("compartmentId"), q.listOpts())

	return listOut(q, res, err, false)
}

func (q *req) recordFilter() RecordFilter {
	return RecordFilter{
		Domain: cmp.Or(q.param("domain"), q.query("domain")), DomainContains: q.query("domainContains"),
		Rtype: cmp.Or(q.param("rtype"), q.query("rtype")), ZoneVersion: q.query("zoneVersion"),
	}
}

func (h *Handler) getRecords(q *req, mustExist bool) error {
	res, t, err := h.Backend.GetRecords(q.zoneRef("zoneNameOrId"), q.recordFilter(), q.listOpts(), mustExist)
	if err != nil {
		return err
	}

	if res.Next != "" {
		q.header("opc-next-page", res.Next)
	}

	q.header("opc-total-items", strconv.Itoa(res.Total))

	return q.get(RecordCollectionWire{Items: nonNil(res.Items)}, t, nil)
}

func (h *Handler) getZoneRecords(q *req) error   { return h.getRecords(q, false) }
func (h *Handler) getDomainRecords(q *req) error { return h.getRecords(q, true) }
func (h *Handler) getRRSet(q *req) error         { return h.getRecords(q, true) }

func (q *req) recordScope() RecordScope {
	return RecordScope{Domain: q.param("domain"), Rtype: q.param("rtype")}
}

func (h *Handler) patchRecords(q *req) error {
	return withBody(q, func(in ItemsIn) error {
		w, t, err := h.Backend.PatchRecords(q.zoneRef("zoneNameOrId"), q.cond(), q.recordScope(), in)

		return q.ok(w, t, err)
	})
}

func (h *Handler) updateRecords(q *req) error {
	return withBody(q, func(in ItemsIn) error {
		w, t, err := h.Backend.UpdateRecords(q.zoneRef("zoneNameOrId"), q.cond(), q.recordScope(), in)

		return q.ok(w, t, err)
	})
}

func (h *Handler) deleteRecords(q *req) error {
	return noContent(q, h.Backend.DeleteRecords(q.zoneRef("zoneNameOrId"), q.cond(), q.recordScope()))
}

func (h *Handler) createView(q *req) error {
	return withBody(q, func(in CreateViewIn) error {
		w, t, err := h.Backend.CreateView(in)

		return q.accepted(w, t, w.Self, err)
	})
}

func (h *Handler) getView(q *req) error {
	w, t, err := h.Backend.GetView(q.param("viewId"))

	return q.get(w, t, err)
}

func (h *Handler) listViews(q *req) error {
	f := ViewFilter{
		Compart: q.query("compartmentId"), Name: q.query(keyDisplayName), ID: q.query("id"),
		State: q.query("lifecycleState"), Scope: q.query("scope"),
	}
	res, err := h.Backend.ListViews(f, q.listOpts())

	return listOut(q, res, err, false)
}

func (h *Handler) updateView(q *req) error {
	return withBody(q, func(in UpdateViewIn) error {
		w, t, err := h.Backend.UpdateView(q.param("viewId"), q.cond(), in)

		return q.ok(w, t, err)
	})
}

func (h *Handler) deleteView(q *req) error {
	return noContent(q, h.Backend.DeleteView(q.param("viewId"), q.cond()))
}

func (h *Handler) changeViewCompartment(q *req) error {
	return withBody(q, func(in ChangeCompartmentIn) error {
		return accepted(q, h.Backend.ChangeViewCompartment(q.param("viewId"), q.cond(), in))
	})
}

func (h *Handler) getResolver(q *req) error {
	w, t, err := h.Backend.GetResolver(q.param("resolverId"))

	return q.get(w, t, err)
}

func (h *Handler) listResolvers(q *req) error {
	f := ResolverFilter{
		Compart: q.query("compartmentId"), Name: q.query(keyDisplayName), ID: q.query("id"),
		State: q.query("lifecycleState"),
	}
	res, err := h.Backend.ListResolvers(f, q.listOpts())

	return listOut(q, res, err, false)
}

func (h *Handler) updateResolver(q *req) error {
	return withBody(q, func(in UpdateResolverIn) error {
		w, t, err := h.Backend.UpdateResolver(q.param("resolverId"), q.cond(), in)

		return q.ok(w, t, err)
	})
}

func (h *Handler) changeResolverCompartment(q *req) error {
	return withBody(q, func(in ChangeCompartmentIn) error {
		return accepted(q, h.Backend.ChangeResolverCompartment(q.param("resolverId"), q.cond(), in))
	})
}

func (h *Handler) createEndpoint(q *req) error {
	return withBody(q, func(in CreateEndpointIn) error {
		w, t, err := h.Backend.CreateEndpoint(q.param("resolverId"), in)

		return q.accepted(w, t, w.Self, err)
	})
}

func (h *Handler) getEndpoint(q *req) error {
	w, t, err := h.Backend.GetEndpoint(q.param("resolverId"), q.param("resolverEndpointName"))

	return q.get(w, t, err)
}

func (h *Handler) listEndpoints(q *req) error {
	res, err := h.Backend.ListEndpoints(q.param("resolverId"), q.query(keyName), q.query("lifecycleState"),
		q.listOpts())

	return listOut(q, res, err, false)
}

func (h *Handler) updateEndpoint(q *req) error {
	return withBody(q, func(in UpdateEndpointIn) error {
		w, t, err := h.Backend.UpdateEndpoint(q.param("resolverId"), q.param("resolverEndpointName"), q.cond(), in)

		return q.ok(w, t, err)
	})
}

func (h *Handler) deleteEndpoint(q *req) error {
	return noContent(q, h.Backend.DeleteEndpoint(q.param("resolverId"), q.param("resolverEndpointName"), q.cond()))
}

func (h *Handler) createTsigKey(q *req) error {
	return withBody(q, func(in CreateTsigKeyIn) error {
		w, t, err := h.Backend.CreateTsigKey(in)

		return q.accepted(w, t, w.Self, err)
	})
}

func (h *Handler) getTsigKey(q *req) error {
	w, t, err := h.Backend.GetTsigKey(q.param("tsigKeyId"))

	return q.get(w, t, err)
}

func (h *Handler) listTsigKeys(q *req) error {
	f := TsigFilter{
		Compart: q.query("compartmentId"), ID: q.query("id"), Name: q.query(keyName), State: q.query("lifecycleState"),
	}
	res, err := h.Backend.ListTsigKeys(f, q.listOpts())

	return listOut(q, res, err, false)
}

func (h *Handler) updateTsigKey(q *req) error {
	return withBody(q, func(in UpdateTsigKeyIn) error {
		w, t, err := h.Backend.UpdateTsigKey(q.param("tsigKeyId"), q.cond(), in)

		return q.ok(w, t, err)
	})
}

func (h *Handler) deleteTsigKey(q *req) error {
	return noContent(q, h.Backend.DeleteTsigKey(q.param("tsigKeyId"), q.cond()))
}

func (h *Handler) changeTsigKeyCompartment(q *req) error {
	return withBody(q, func(in ChangeCompartmentIn) error {
		return accepted(q, h.Backend.ChangeTsigKeyCompartment(q.param("tsigKeyId"), q.cond(), in))
	})
}

func (h *Handler) createPolicy(q *req) error {
	return withBody(q, func(in CreatePolicyIn) error {
		w, t, err := h.Backend.CreateSteeringPolicy(in)

		return q.accepted(w, t, w.Self, err)
	})
}

func (h *Handler) getPolicy(q *req) error {
	w, t, err := h.Backend.GetSteeringPolicy(q.param("steeringPolicyId"))

	return q.get(w, t, err)
}

func (h *Handler) listPolicies(q *req) error {
	f := PolicyFilter{
		Compart: q.query("compartmentId"), ID: q.query("id"), Name: q.query(keyDisplayName),
		Contains: q.query("displayNameContains"), Monitor: q.query("healthCheckMonitorId"),
		Template: q.query("template"), State: q.query("lifecycleState"), Created: q.timeFilter(),
	}
	res, err := h.Backend.ListSteeringPolicies(f, q.listOpts())

	return listOut(q, res, err, true)
}

func (h *Handler) updatePolicy(q *req) error {
	return withBody(q, func(in UpdatePolicyIn) error {
		w, t, err := h.Backend.UpdateSteeringPolicy(q.param("steeringPolicyId"), q.cond(), in)

		return q.ok(w, t, err)
	})
}

func (h *Handler) deletePolicy(q *req) error {
	return noContent(q, h.Backend.DeleteSteeringPolicy(q.param("steeringPolicyId"), q.cond()))
}

func (h *Handler) changePolicyCompartment(q *req) error {
	return withBody(q, func(in ChangeCompartmentIn) error {
		return accepted(q, h.Backend.ChangeSteeringPolicyCompartment(q.param("steeringPolicyId"), q.cond(), in))
	})
}

func (h *Handler) createAttachment(q *req) error {
	return withBody(q, func(in CreateAttachmentIn) error {
		w, t, err := h.Backend.CreateAttachment(in)

		return q.accepted(w, t, w.Self, err)
	})
}

func (h *Handler) getAttachment(q *req) error {
	w, t, err := h.Backend.GetAttachment(q.param("steeringPolicyAttachmentId"))

	return q.get(w, t, err)
}

func (h *Handler) listAttachments(q *req) error {
	f := AttachmentFilter{
		Compart: q.query("compartmentId"), ID: q.query("id"), Name: q.query(keyDisplayName),
		PolicyID: q.query("steeringPolicyId"), ZoneID: q.query("zoneId"), Domain: q.query("domain"),
		Contains: q.query("domainContains"), State: q.query("lifecycleState"), Created: q.timeFilter(),
	}
	res, err := h.Backend.ListAttachments(f, q.listOpts())

	return listOut(q, res, err, true)
}

func (h *Handler) updateAttachment(q *req) error {
	return withBody(q, func(in UpdateAttachmentIn) error {
		w, t, err := h.Backend.UpdateAttachment(q.param("steeringPolicyAttachmentId"), q.cond(), in)

		return q.ok(w, t, err)
	})
}

func (h *Handler) deleteAttachment(q *req) error {
	return noContent(q, h.Backend.DeleteAttachment(q.param("steeringPolicyAttachmentId"), q.cond()))
}
