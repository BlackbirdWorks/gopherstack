package azurearm

import (
	"context"
	"strings"
	"time"
)

const (
	logicMaxRequests     = 100
	logicMaxReqBodyStore = 256 << 10
	logicSegRequestHist  = "requestHistories"
	logicReqBodyKind     = "ResponseBody"
)

// logicRequestRec is one outbound HTTP call the engine made for an action (an attempt, retry or poll).
type logicRequestRec struct {
	Start       time.Time         `json:"start"`
	End         time.Time         `json:"end"`
	Body        any               `json:"body,omitempty"`
	ReqHeaders  map[string]string `json:"reqHeaders,omitempty"`
	RespHeaders map[string]string `json:"respHeaders,omitempty"`
	Name        string            `json:"name"`
	Method      string            `json:"method"`
	URI         string            `json:"uri"`
	BodySize    int64             `json:"bodySize,omitempty"`
	Status      int               `json:"status,omitempty"`
	HasBody     bool              `json:"hasBody,omitempty"`
	Responded   bool              `json:"responded,omitempty"`
}

func secretHeader(k string) bool {
	switch strings.ToLower(k) {
	case "authorization", "proxy-authorization", "cookie", "set-cookie":
		return true
	}

	return false
}

func redactHeaders(h map[string]string) {
	for k := range h {
		if secretHeader(k) {
			delete(h, k)
		}
	}
}

func (rs *logicRunState) currentRec(env *logicEnv, name string) *logicActionRec {
	rs.p.mu.RLock("currentRec")
	defer rs.p.mu.RUnlock()

	return env.local[name]
}

// send performs one outbound call and records it on the action.
func (rs *logicRunState) send(ctx context.Context, req *logicHTTPRequest) (*logicHTTPResponse, error) {
	start := rs.clock().UTC()
	resp, err := rs.p.sendHTTP(ctx, req)

	if req.rec != nil {
		rs.recordRequest(req, resp, start)
	}

	return resp, err
}

func (rs *logicRunState) recordRequest(req *logicHTTPRequest, resp *logicHTTPResponse, start time.Time) {
	rq := &logicRequestRec{
		Start: start, End: rs.clock().UTC(), Method: req.method, URI: req.url,
		ReqHeaders: map[string]string{},
	}

	for k, v := range req.header {
		rq.ReqHeaders[k] = strings.Join(v, ", ")
	}

	redactHeaders(rq.ReqHeaders)

	if resp != nil {
		rq.Responded, rq.Status, rq.RespHeaders = true, resp.status, map[string]string{}

		for k, v := range resp.headers {
			rq.RespHeaders[k] = logicString(v)
		}

		redactHeaders(rq.RespHeaders)

		if resp.body != nil {
			rq.BodySize = bodySize(resp.body)
			if rq.BodySize <= logicMaxReqBodyStore {
				rq.Body, rq.HasBody = resp.body, true
			}
		}
	}

	rs.p.mu.Lock("recordRequest")
	defer rs.p.mu.Unlock()

	if len(req.rec.Requests) >= logicMaxRequests {
		return
	}

	rq.Name = rs.p.newVersionID(start)
	req.rec.Requests = append(req.rec.Requests, rq)
}

func bodySize(v any) int64 {
	if s, ok := v.(string); ok {
		return int64(len(s))
	}

	return marshalLogicSize(v)
}

func (w *logicWorkflow) requestBody(a *logicActionRec, rq *logicRequestRec, rel, base string) map[string]any {
	props := map[string]any{logicKStartTime: logicTime(rq.Start), logicKEndTime: logicTime(rq.End)}

	req := map[string]any{"method": rq.Method}
	if !a.Inputs.Hidden {
		req["uri"] = rq.URI
		req[logicKHeaders] = orEmptyHeaders(rq.ReqHeaders)
	}

	props["request"] = req

	if rq.Responded {
		resp := map[string]any{"statusCode": int64(rq.Status)}

		if !a.Outputs.Hidden {
			resp[logicKHeaders] = orEmptyHeaders(rq.RespHeaders)

			if rq.HasBody {
				resp["bodyLink"] = w.contentLink(base, "/workflows/"+w.FlowID+rel+"/requestHistories/"+rq.Name+
					"/contents/"+logicReqBodyKind, rq.Body)
			}
		}

		props["response"] = resp
	}

	return map[string]any{
		logicFieldID: w.armID() + rel + "/requestHistories/" + rq.Name,
		fieldName:    rq.Name, fieldType: logicRunType + "/actions/" + logicSegRequestHist, fieldProperties: props,
	}
}

func orEmptyHeaders(h map[string]string) map[string]any {
	out := make(map[string]any, len(h))
	for k, v := range h {
		out[k] = v
	}

	return out
}

// requestTarget resolves the action record a request history route addresses: an action of the run, or one
// repetition of it when rep is set.
func (w *logicWorkflow) requestTarget(runName, action, rep string) (*logicActionRec, string, error) {
	r, ok := findRun(w, runName)
	if !ok {
		return nil, "", ErrLogicRunNotFound
	}

	scope := "/runs/" + r.Name + "/actions/" + action

	if rep == "" {
		a, found := r.Actions[action]
		if !found {
			return nil, "", ErrLogicActionNotFound
		}

		return a, scope, nil
	}

	reps, err := w.repetitionsOf(r, action, false)
	if err != nil {
		return nil, "", err
	}

	for _, x := range reps {
		if x.Name == rep {
			return &x.Rec, scope + "/repetitions/" + rep, nil
		}
	}

	return nil, "", ErrLogicRepNotFound
}

func (h *Handler) logicListRequests(q logicReq, rep string) error {
	return h.logicList(q, func(w *logicWorkflow, _ logicQuery) ([]map[string]any, error) {
		a, rel, err := w.requestTarget(q.rest[1], q.rest[3], rep)
		if err != nil {
			return nil, err
		}

		out := make([]map[string]any, 0, len(a.Requests))
		for _, rq := range a.Requests {
			out = append(out, w.requestBody(a, rq, rel, q.base))
		}

		return out, nil
	})
}

func (h *Handler) logicGetRequest(q logicReq, rep string) error {
	name := q.rest[len(q.rest)-1]

	out, err := q.lp.read(q.ref, func(w *logicWorkflow) (any, error) {
		a, rel, err := w.requestTarget(q.rest[1], q.rest[3], rep)
		if err != nil {
			return nil, err
		}

		for _, rq := range a.Requests {
			if strings.EqualFold(rq.Name, name) {
				return w.requestBody(a, rq, rel, q.base), nil
			}
		}

		return nil, ErrLogicRequestNotFound
	})

	return h.logicReply(q, out, err)
}

// requestContent resolves a request history body link; segs end in .../requestHistories/{name}/contents/{kind}.
func (w *logicWorkflow) requestContent(segs []string) (logicContent, bool) {
	n := len(segs)
	if segs[n-1] != logicReqBodyKind {
		return logicContent{}, false
	}

	rep := ""
	if n == contentRequestRepSegs {
		rep = segs[6]
	}

	a, _, err := w.requestTarget(segs[2], segs[4], rep)
	if err != nil || a.Outputs.Hidden {
		return logicContent{}, false
	}

	for _, rq := range a.Requests {
		if rq.Name == segs[n-3] && rq.HasBody {
			return logicContent{Value: rq.Body, Set: true}, true
		}
	}

	return logicContent{}, false
}
