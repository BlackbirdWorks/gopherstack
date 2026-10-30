package azurearm

import (
	"encoding/base64"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	apimOpResultsSegment = "operationresults"
	apimOpStatusRunning  = "InProgress"
	apimOpStatusDone     = "Succeeded"
	apimOpKeep           = time.Hour
	apimOpPathLen        = 2
	apimOpStartTime      = "startTime"
)

// apimOp is one long-running service operation served at .../service/{name}/operationresults/{id}.
type apimOp struct {
	Started time.Time `json:"started"`
	ReadyAt time.Time `json:"readyAt"`
	Key     string    `json:"key"`
	Method  string    `json:"method"`
}

func (o *apimOp) done(now time.Time) bool { return !now.Before(o.ReadyAt) }

// startOp records a long-running operation on the service key and returns its id. The caller holds the write lock.
func (p *APIMProvider) startOp(key, method, location string, ready time.Time) string {
	now := p.now()

	for id, o := range p.ops {
		if o.done(now) && now.Sub(o.ReadyAt) > apimOpKeep {
			delete(p.ops, id)
		}
	}

	id := base64.URLEncoding.EncodeToString([]byte(location + ":" + awsid.New("") + "_Act"))
	p.ops[id] = &apimOp{Started: now, ReadyAt: ready, Key: key, Method: method}

	return id
}

// operationResult answers GET .../operationresults/{id}: 202 while running; on completion 200 with the service
// (PUT/PATCH, so that the Location and Azure-AsyncOperation pollers can share the URL) or an empty body (DELETE).
func (p *APIMProvider) operationResult(req apimReq) (apimResp, error) {
	id := req.Rest[1]

	o, ok := p.ops[id]
	if !ok {
		return apimResp{}, apimNotFound("operation")
	}

	now := p.now()
	started := o.Started.UTC().Format(time.RFC3339Nano)

	if !o.done(now) {
		return apimResp{
			Status: http.StatusAccepted, Retry: true,
			Body: map[string]any{fieldName: id, acrPropStatus: apimOpStatusRunning, apimOpStartTime: started},
		}, nil
	}

	s, ok := p.svcs[o.Key]
	if o.Method == http.MethodDelete || !ok {
		return apimResp{Status: http.StatusOK}, nil
	}

	body := s.body(now)
	body[acrPropStatus] = apimOpStatusDone
	body[apimOpStartTime] = started
	body["endTime"] = o.ReadyAt.UTC().Format(time.RFC3339Nano)

	return apimResp{Status: http.StatusOK, Body: body, Etag: apimEtag(s.Etag)}, nil
}

func isAPIMOperationResult(rest []string) bool {
	return len(rest) == apimOpPathLen && strings.EqualFold(rest[0], apimOpResultsSegment)
}

// setAPIMOperation points Location (and, for PUT/PATCH, Azure-AsyncOperation) at the operation result.
func (h *Handler) setAPIMOperation(c *echo.Context, segs []string, resp apimResp) {
	r := c.Request()
	loc := h.baseURLFor(r) + "/" + strings.Join(segs[:apimMinSegs], "/") + "/" + apimOpResultsSegment + "/" +
		resp.OpID + "?api-version=" + r.URL.Query().Get("api-version")

	hdr := c.Response().Header()
	if resp.OpAsync {
		loc += "&asyncResponse"
		hdr.Set("Azure-Asyncoperation", loc)
	}

	hdr.Set("Location", loc)
}

// asyncWrite answers a creating PUT with 201 and a PATCH with 202 (ApiManagementCreateService.json,
// ApiManagementUpdateServiceDisableTls10.json); both carry Location and Azure-AsyncOperation headers.
func (p *APIMProvider) asyncWrite(s *apimService, method string, created bool) apimResp {
	now := p.now()

	ready := now
	if s.Transition != "" {
		ready = s.ReadyAt
	}

	resp := apimResp{
		Status: http.StatusAccepted, Etag: apimEtag(s.Etag), Retry: true, OpAsync: true,
		OpID: p.startOp(apimKey(s.SubID, s.ResourceGrp, s.Name), method, s.Location, ready),
	}

	if created {
		resp.Status, resp.Body = http.StatusCreated, s.createdBody(now)
	}

	return resp
}
