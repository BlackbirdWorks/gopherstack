package cosmosdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	headerPreTrigger  = "X-Ms-Documentdb-Pre-Trigger-Include"
	headerPostTrigger = "X-Ms-Documentdb-Post-Trigger-Include"
	headerScriptLog   = "X-Ms-Documentdb-Script-Enable-Logging"
	headerScriptLogs  = "X-Ms-Documentdb-Script-Log-Results"

	maxScriptLogBytes = 1024
	opExecuteSproc    = "ExecuteStoredProcedure"
)

// scriptKindInfo names a script kind's list key and operation noun.
type scriptKindInfo struct {
	listKey string
	noun    string
}

func scriptInfoOf(kind scriptKind) scriptKindInfo {
	switch kind {
	case kindTrigger:
		return scriptKindInfo{listKey: "Triggers", noun: "Trigger"}
	case kindUDF:
		return scriptKindInfo{listKey: "UserDefinedFunctions", noun: "UserDefinedFunction"}
	default:
		return scriptKindInfo{listKey: "StoredProcedures", noun: "StoredProcedure"}
	}
}

// scriptKindOf maps a script resource kind to its scriptKind and whether it names one item.
func scriptKindOf(kind resourceKind) (scriptKind, bool, bool) {
	switch kind {
	case resourceSprocs:
		return kindSproc, false, true
	case resourceSprocItem:
		return kindSproc, true, true
	case resourceTriggers:
		return kindTrigger, false, true
	case resourceTriggerItem:
		return kindTrigger, true, true
	case resourceUDFs:
		return kindUDF, false, true
	case resourceUDFItem:
		return kindUDF, true, true
	default:
		return "", false, false
	}
}

func scriptSupportedOperations() []string {
	kinds := [...]scriptKind{kindSproc, kindTrigger, kindUDF}
	ops := make([]string, 0, 1+6*len(kinds))
	ops = append(ops, opExecuteSproc)

	for _, k := range kinds {
		n := scriptInfoOf(k).noun
		ops = append(ops, "Create"+n, "Get"+n, "Replace"+n, "Delete"+n, "List"+n+"s", "Query"+n+"s")
	}

	return ops
}

func scriptOperationFor(kind resourceKind, r *http.Request) string {
	sk, isItem, ok := scriptKindOf(kind)
	if !ok {
		return unknownOperation
	}

	n := scriptInfoOf(sk).noun

	if isItem {
		return scriptItemOperationFor(r.Method, sk, n)
	}

	switch r.Method {
	case http.MethodPost:
		if isQueryRequest(r) {
			return "Query" + n + "s"
		}

		return "Create" + n
	case http.MethodGet:
		return "List" + n + "s"
	default:
		return unknownOperation
	}
}

func scriptItemOperationFor(method string, kind scriptKind, noun string) string {
	switch method {
	case http.MethodGet:
		return "Get" + noun
	case http.MethodPut:
		return "Replace" + noun
	case http.MethodDelete:
		return "Delete" + noun
	case http.MethodPost:
		if kind == kindSproc {
			return opExecuteSproc
		}
	}

	return unknownOperation
}

type scriptBackend interface {
	CreateScript(dbID, collID string, kind scriptKind, spec ScriptSpec) (ScriptInfo, error)
	GetScript(dbID, collID string, kind scriptKind, id string) (ScriptInfo, error)
	ListScripts(dbID, collID string, kind scriptKind) ([]ScriptInfo, error)
	ReplaceScript(dbID, collID string, kind scriptKind, spec ScriptSpec) (ScriptInfo, error)
	DeleteScript(dbID, collID string, kind scriptKind, id string) error
	ExecuteSproc(dbID, collID, id, partitionKey string, args []any) (ScriptOutput, error)
}

type scriptBody struct {
	ID               string `json:"id"`
	Body             string `json:"body"`
	TriggerType      string `json:"triggerType"`
	TriggerOperation string `json:"triggerOperation"`
}

func (s scriptBody) spec() ScriptSpec { return ScriptSpec(s) }

func scriptWire(kind scriptKind, info ScriptInfo) map[string]any {
	m := map[string]any{
		"id": info.ID, "body": info.Body, sysPropRID: info.RID, sysPropSelf: info.Self,
		sysPropETag: info.ETag, sysPropTS: info.Timestamp.Unix(),
	}
	if kind == kindTrigger {
		m["triggerType"], m["triggerOperation"] = info.TriggerType, info.TriggerOperation
	}

	return m
}

func (h *Handler) methodNotAllowed(c *echo.Context) error {
	return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowed",
		"The resource doesn't support the specified HTTP verb.")
}

func (h *Handler) handleScripts(c *echo.Context, kind scriptKind, isItem bool, dbID, collID, id string) error {
	sb, ok := h.be(c).(scriptBackend)
	if !ok {
		return h.writeError(
			c,
			http.StatusMethodNotAllowed,
			"MethodNotAllowed",
			"The backend has no server-side scripts.",
		)
	}

	if isItem {
		return h.handleScriptItem(c, sb, kind, dbID, collID, id)
	}

	switch c.Request().Method {
	case http.MethodPost:
		if isQueryRequest(c.Request()) {
			return h.queryScripts(c, sb, kind, dbID, collID)
		}

		return h.writeScript(c, sb, kind, dbID, collID, "", http.StatusCreated)
	case http.MethodGet:
		return h.listScripts(c, sb, kind, dbID, collID)
	default:
		return h.methodNotAllowed(c)
	}
}

func (h *Handler) handleScriptItem(c *echo.Context, sb scriptBackend, kind scriptKind, dbID, collID, id string) error {
	switch c.Request().Method {
	case http.MethodGet:
		info, err := sb.GetScript(dbID, collID, kind, id)
		if err != nil {
			return h.writeDocumentError(c, err)
		}

		return h.writeScriptInfo(c, kind, info, http.StatusOK)
	case http.MethodPut:
		return h.writeScript(c, sb, kind, dbID, collID, id, http.StatusOK)
	case http.MethodDelete:
		if err := sb.DeleteScript(dbID, collID, kind, id); err != nil {
			return h.writeDocumentError(c, err)
		}

		setCharge(c, ruMetadata)

		return c.NoContent(http.StatusNoContent)
	case http.MethodPost:
		if kind == kindSproc {
			return h.executeSproc(c, sb, dbID, collID, id)
		}

		return h.methodNotAllowed(c)
	default:
		return h.methodNotAllowed(c)
	}
}

// writeScript creates (id == "") or replaces a script from the request body.
func (h *Handler) writeScript(
	c *echo.Context,
	sb scriptBackend,
	kind scriptKind,
	dbID, collID, id string,
	status int,
) error {
	raw, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	var req scriptBody
	if jsonErr := json.Unmarshal(raw, &req); jsonErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "The input is not valid JSON.")
	}

	var info ScriptInfo

	switch {
	case id == "":
		info, err = sb.CreateScript(dbID, collID, kind, req.spec())
	case req.ID != id:
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "The script id does not match the request path.")
	default:
		info, err = sb.ReplaceScript(dbID, collID, kind, req.spec())
	}

	if err != nil {
		return h.writeScriptError(c, err)
	}

	return h.writeScriptInfo(c, kind, info, status)
}

func (h *Handler) writeScriptError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrScriptAlreadyExists):
		return h.writeError(c, http.StatusConflict, "Conflict", "Resource with specified id already exists.")
	case errors.Is(err, ErrInvalidScript):
		return h.writeError(c, http.StatusBadRequest, "BadRequest", err.Error())
	default:
		return h.writeDocumentError(c, err)
	}
}

func (h *Handler) writeScriptInfo(c *echo.Context, kind scriptKind, info ScriptInfo, status int) error {
	c.Response().Header().Set("ETag", info.ETag)
	setCharge(c, ruMetadata)

	return h.writeJSON(c, status, scriptWire(kind, info))
}

func (h *Handler) scriptBodies(sb scriptBackend, kind scriptKind, dbID, collID string) ([]map[string]any, error) {
	infos, err := sb.ListScripts(dbID, collID, kind)
	if err != nil {
		return nil, err
	}

	bodies := make([]map[string]any, 0, len(infos))
	for _, info := range infos {
		bodies = append(bodies, scriptWire(kind, info))
	}

	return bodies, nil
}

func (h *Handler) listScripts(c *echo.Context, sb scriptBackend, kind scriptKind, dbID, collID string) error {
	token, limit, reqErr := pageRequest(c.Request())
	if reqErr != nil {
		return h.writeContinuationError(c)
	}

	bodies, err := h.scriptBodies(sb, kind, dbID, collID)
	if err != nil {
		return h.writeDocumentError(c, err)
	}

	pg := paginate(bodies, token, limit)
	setPageHeaders(c, len(pg.Data), pg.Next)
	setCharge(c, ruMetadata)

	return h.writeJSON(c, http.StatusOK, map[string]any{
		sysPropRID: "", scriptInfoOf(kind).listKey: pg.Data, sysPropCount: len(pg.Data),
	})
}

func (h *Handler) queryScripts(c *echo.Context, sb scriptBackend, kind scriptKind, dbID, collID string) error {
	bodies, err := h.scriptBodies(sb, kind, dbID, collID)
	if err != nil {
		return h.writeDocumentError(c, err)
	}

	return h.queryResources(c, scriptInfoOf(kind).listKey, bodies)
}

// executeSproc runs POST .../sprocs/{id}: the body is a JSON array of arguments.
func (h *Handler) executeSproc(c *echo.Context, sb scriptBackend, dbID, collID, id string) error {
	r := c.Request()

	pk, pkErr := partitionKeyFromHeader(r)
	if pkErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", pkErr.Error())
	}

	raw, err := httputils.ReadBody(r)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	args, argErr := sprocArgs(raw)
	if argErr != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequest",
			"The stored procedure parameters must be a JSON array.",
		)
	}

	out, execErr := sb.ExecuteSproc(dbID, collID, id, pk, args)
	if execErr != nil {
		return h.writeDocumentError(c, execErr)
	}

	setCharge(c, ruReadBase+ruWriteBase*float64(out.Writes))

	if strings.EqualFold(r.Header.Get(headerScriptLog), "true") {
		c.Response().Header().Set(headerScriptLogs, scriptLogHeader(out.Logs))
	}

	if !out.HasBody {
		return c.Blob(http.StatusOK, "application/json", nil)
	}

	return h.writeJSON(c, http.StatusOK, out.Body)
}

func sprocArgs(raw []byte) ([]any, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}

	var args []any

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	if err := dec.Decode(&args); err != nil || rejectTrailingJSON(dec) != nil {
		return nil, ErrInvalidDocument
	}

	return args, nil
}

func scriptLogHeader(logs []string) string {
	enc := url.QueryEscape(strings.Join(logs, "\n"))
	if len(enc) > maxScriptLogBytes {
		enc = enc[:maxScriptLogBytes]
	}

	return enc
}

// requestedTriggers are the trigger names a write asked for.
type requestedTriggers struct {
	pre  []string
	post []string
}

func (t requestedTriggers) requested() bool { return len(t.pre)+len(t.post) > 0 }

func triggerRequest(r *http.Request) requestedTriggers {
	return requestedTriggers{
		pre:  triggerNames(r.Header.Get(headerPreTrigger)),
		post: triggerNames(r.Header.Get(headerPostTrigger)),
	}
}

type triggerRunner interface {
	runTriggered(dbID, collID string, op docOp) (docOpResult, error)
}

// runWithTriggers executes a document write with its pre/post triggers and writes the response.
func (h *Handler) runWithTriggers(c *echo.Context, dbID, collID string, op docOp) error {
	runner, ok := h.be(c).(triggerRunner)
	if !ok {
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowed", "The backend has no triggers.")
	}

	res, err := runner.runTriggered(dbID, collID, op)
	if err != nil {
		return h.writeDocumentError(c, err)
	}

	if len(res.logs) > 0 && strings.EqualFold(c.Request().Header.Get(headerScriptLog), "true") {
		c.Response().Header().Set(headerScriptLogs, scriptLogHeader(res.logs))
	}

	if res.deleted {
		setCharge(c, ruDelete)

		return c.NoContent(http.StatusNoContent)
	}

	c.Response().Header().Set("ETag", res.info.ETag)

	status := http.StatusOK
	if res.created {
		status = http.StatusCreated
	}

	var payload any = h.encodeDocument(res.info)
	if res.hasBody {
		payload = res.body
	}

	return h.writeJSONCharged(c, status, payload, h.writeCharger(dbID, collID, res.info.Body, true))
}
