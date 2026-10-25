package ocifunctions

import (
	"net/http"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func buildOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateApplication: (*Handler).createApplication, opGetApplication: (*Handler).getApplication,
		opListApplications: (*Handler).listApplications, opUpdateApplication: (*Handler).updateApplication,
		opDeleteApplication: (*Handler).deleteApplication, opChangeAppComp: (*Handler).changeApplicationCompartment,
		opCreateFunction: (*Handler).createFunction, opGetFunction: (*Handler).getFunction,
		opListFunctions: (*Handler).listFunctions, opUpdateFunction: (*Handler).updateFunction,
		opDeleteFunction: (*Handler).deleteFunction, opInvokeFunction: (*Handler).invokeFunction,
		opGetWorkRequest: (*Handler).getWorkRequest, opListWorkRequests: (*Handler).listWorkRequests,
		opListWorkReqErrors: (*Handler).listWorkRequestErrors, opListWorkReqLogs: (*Handler).listWorkRequestLogs,
	}
}

func reply[T any](q *req, w T, etag string, err error) error {
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, etag)
}

func listReply[T any](q *req, pg page.Page[T], err error) error {
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

// replyWR answers a create or update with the resource, its etag and the work request id.
func replyWR[T any](q *req, w T, etag, wr string, err error) error {
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)

	return q.json(http.StatusOK, w, etag)
}

func acceptedWR(q *req, wr string, err error) error {
	if err != nil {
		return err
	}

	return q.accepted(wr)
}

const (
	invokeSync     = "sync"
	invokeDetached = "detached"
)

func (h *Handler) invokeFunction(q *req) error {
	if intent := q.r.Header.Get("fn-intent"); intent != "" && !slices.Contains([]string{"httprequest", "cloudevent"}, intent) {
		return errParam("fn-intent must be httprequest or cloudevent")
	}

	kind := strings.ToLower(q.r.Header.Get("fn-invoke-type"))
	if kind != "" && kind != invokeSync && kind != invokeDetached {
		return errParam("fn-invoke-type must be sync or detached")
	}

	out, err := h.Backend.Invoke(q.r.Context(), q.param("functionId"), InvokeIn{
		Header: q.r.Header, URL: q.r.URL.RequestURI(), Body: q.body,
		Detached: kind == invokeDetached, DryRun: strings.EqualFold(q.r.Header.Get("is-dry-run"), "true"),
	})
	if err != nil {
		return err
	}

	for k, vs := range out.Header {
		q.w.Header()[k] = vs
	}

	q.w.WriteHeader(out.Status)
	_, _ = q.w.Write(out.Body)

	return nil
}
