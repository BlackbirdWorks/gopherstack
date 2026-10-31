package cosmosdb

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/dop251/goja"
)

// Positions of the options argument in replaceDocument(link, doc, options) and deleteDocument(link, options).
const (
	replaceOptionsArg = 2
	deleteOptionsArg  = 1
)

// scriptEnv is the server-side API surface (getContext) a stored procedure or trigger runs against: one
// transaction, one partition.
type scriptEnv struct {
	reqObj    goja.Value
	respObj   goja.Value
	vm        *goja.Runtime
	tx        *containerTx
	cbOptions map[string]any
	scope     string
	logs      []string
	reqSet    bool
	respSet   bool
}

func newScriptEnv(vm *goja.Runtime, tx *containerTx, scope string) *scriptEnv {
	e := &scriptEnv{vm: vm, tx: tx, scope: scope}
	e.install()

	return e
}

func (e *scriptEnv) install() {
	ctx := e.vm.NewObject()
	_ = ctx.Set("getCollection", func() goja.Value { return e.collectionObject() })
	_ = ctx.Set("getResponse", func() goja.Value { return e.responseObject() })
	_ = ctx.Set("getRequest", func() goja.Value { return e.requestObject() })
	_ = e.vm.Set("getContext", func() goja.Value { return ctx })

	console := e.vm.NewObject()
	_ = console.Set("log", func(call goja.FunctionCall) goja.Value {
		parts := make([]string, len(call.Arguments))
		for i, a := range call.Arguments {
			parts[i] = a.String()
		}

		e.logs = append(e.logs, strings.Join(parts, " "))

		return goja.Undefined()
	})
	_ = e.vm.Set("console", console)
}

func (e *scriptEnv) selfLink() string {
	return "dbs/" + e.tx.dbID + "/colls/" + e.tx.collID + "/"
}

func (e *scriptEnv) responseObject() goja.Value {
	o := e.vm.NewObject()
	_ = o.Set("setBody", func(v goja.Value) { e.respObj, e.respSet = v, true })
	_ = o.Set("getBody", func() goja.Value {
		if e.respObj == nil {
			return goja.Undefined()
		}

		return e.respObj
	})

	return o
}

func (e *scriptEnv) requestObject() goja.Value {
	o := e.vm.NewObject()
	_ = o.Set("setBody", func(v goja.Value) { e.reqObj, e.reqSet = v, true })
	_ = o.Set("getBody", func() goja.Value {
		if e.reqObj == nil {
			return goja.Undefined()
		}

		return e.reqObj
	})

	return o
}

func (e *scriptEnv) collectionObject() goja.Value {
	o := e.vm.NewObject()
	_ = o.Set("getSelfLink", func() string { return e.selfLink() })
	_ = o.Set("getAltLink", func() string { return e.selfLink() })
	_ = o.Set("createDocument", e.method(e.createDocument(false)))
	_ = o.Set("upsertDocument", e.method(e.createDocument(true)))
	_ = o.Set("readDocument", e.method(e.readDocument))
	_ = o.Set("replaceDocument", e.method(e.replaceDocument))
	_ = o.Set("deleteDocument", e.method(e.deleteDocument))
	_ = o.Set("queryDocuments", e.method(e.queryDocuments))

	return o
}

// scriptOp is one collection operation: it gets the non-callback arguments and returns the value passed to the
// callback.
type scriptOp func(args []goja.Value) (any, error)

// method wraps op with the Cosmos calling convention: a trailing callback receives (err, result, options) and the
// call returns true; without a callback a failure is thrown.
func (e *scriptEnv) method(op scriptOp) func(goja.FunctionCall) goja.Value {
	return func(call goja.FunctionCall) goja.Value {
		args := call.Arguments

		var cb goja.Callable
		if n := len(args); n > 0 {
			if fn, ok := goja.AssertFunction(args[n-1]); ok {
				cb, args = fn, args[:n-1]
			}
		}

		res, err := op(args)
		if err != nil && cb == nil {
			panic(e.errorValue(err))
		}

		if cb != nil {
			e.invoke(cb, res, err)
		}

		return e.vm.ToValue(true)
	}
}

func (e *scriptEnv) invoke(cb goja.Callable, res any, err error) {
	errVal, resVal := goja.Undefined(), goja.Undefined()
	if err != nil {
		errVal = e.errorValue(err)
	} else {
		resVal = toJS(e.vm, res)
	}

	opts := e.vm.NewObject()
	for k, v := range e.cbOptions {
		_ = opts.Set(k, v)
	}

	e.cbOptions = nil

	if _, callErr := cb(goja.Undefined(), errVal, resVal, opts); callErr != nil {
		panic(callErr)
	}
}

// errorValue builds the JS Error a failed operation reports, carrying the HTTP status as `number`.
func (e *scriptEnv) errorValue(err error) goja.Value {
	obj, ctorErr := e.vm.New(e.vm.Get("Error"), e.vm.ToValue(err.Error()))
	if ctorErr != nil {
		return e.vm.ToValue(err.Error())
	}

	_ = obj.Set("number", opStatus(err))

	return obj
}

func opStatus(err error) int {
	if errors.Is(err, ErrScriptTimeout) {
		return http.StatusRequestTimeout
	}

	return statusForError(err)
}

func argAt(args []goja.Value, i int) goja.Value {
	if i < len(args) {
		return args[i]
	}

	return goja.Undefined()
}

func (e *scriptEnv) bodyArg(args []goja.Value, i int) (map[string]any, error) {
	m, ok := jsonValue(argAt(args, i)).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: the document must be an object", ErrInvalidDocument)
	}

	return m, nil
}

// optionETag reads options.etag from the argument at i.
func optionETag(args []goja.Value, i int) string {
	obj, ok := argAt(args, i).(*goja.Object)
	if !ok {
		return ""
	}

	if v := obj.Get("etag"); v != nil && !goja.IsUndefined(v) {
		return v.String()
	}

	return ""
}

// docID extracts the id from a dbs/{db}/colls/{coll}/docs/{id} link of this transaction's collection.
func (e *scriptEnv) docID(link string) (string, error) {
	parts := strings.Split(strings.Trim(link, "/"), "/")
	if len(parts) != segCountDocumentItem || parts[0] != dbsSegment || parts[2] != collsSegment ||
		parts[4] != docsSegment {
		return "", fmt.Errorf("%w: %q is not a document link", ErrInvalidDocument, link)
	}

	if parts[1] != e.tx.dbID || parts[3] != e.tx.collID {
		return "", fmt.Errorf("%w: %q is outside the collection the script runs in", ErrInvalidDocument, link)
	}

	return parts[5], nil
}

func (e *scriptEnv) createDocument(upsert bool) scriptOp {
	return func(args []goja.Value) (any, error) {
		body, err := e.bodyArg(args, 1)
		if err != nil {
			return nil, err
		}

		info, _, err := e.tx.createDoc(e.scope, body, upsert)
		if err != nil {
			return nil, err
		}

		return documentAsMap(info), nil
	}
}

func (e *scriptEnv) readDocument(args []goja.Value) (any, error) {
	id, err := e.docID(argAt(args, 0).String())
	if err != nil {
		return nil, err
	}

	info, err := e.tx.readDoc(e.scope, id)
	if err != nil {
		return nil, err
	}

	return documentAsMap(info), nil
}

func (e *scriptEnv) replaceDocument(args []goja.Value) (any, error) {
	id, err := e.docID(argAt(args, 0).String())
	if err != nil {
		return nil, err
	}

	body, err := e.bodyArg(args, 1)
	if err != nil {
		return nil, err
	}

	info, err := e.tx.replaceDoc(e.scope, id, body, optionETag(args, replaceOptionsArg))
	if err != nil {
		return nil, err
	}

	return documentAsMap(info), nil
}

func (e *scriptEnv) deleteDocument(args []goja.Value) (any, error) {
	id, err := e.docID(argAt(args, 0).String())
	if err != nil {
		return nil, err
	}

	return undef{}, e.tx.deleteDoc(e.scope, id, optionETag(args, deleteOptionsArg))
}

func (e *scriptEnv) queryDocuments(args []goja.Value) (any, error) {
	query, params, err := queryArg(argAt(args, 1))
	if err != nil {
		return nil, err
	}

	opts := feedOptions(argAt(args, queryOptionsArg))

	stmt, err := e.tx.b.parseCached(query)
	if err != nil {
		return nil, err
	}

	scan := QueryOptions{AllowScan: opts.enableScan}
	if idxErr := checkQueryIndexing(stmt, e.tx.c.IndexingPolicy, scan, e.tx.b.computedExprs(e.tx.c)); idxErr != nil {
		return nil, idxErr
	}

	res, err := e.tx.b.queryLocked(e.tx.c, e.tx.dbID, e.tx.collID, query, params, e.scope)
	if err != nil {
		return nil, err
	}

	return e.page(res.Rows, opts)
}

// Server-side FeedOptions (azure-cosmosdb-js-server, Collection.queryDocuments): pageSize defaults to 100,
// continuation resumes an enumeration, enableScan lets a query the indexing policy cannot serve run anyway.
const (
	queryOptionsArg     = 2
	defaultScriptPage   = 100
	continuationOptions = "continuation"
)

type scriptFeedOptions struct {
	continuation string
	pageSize     int
	enableScan   bool
}

func feedOptions(v goja.Value) scriptFeedOptions {
	out := scriptFeedOptions{pageSize: defaultScriptPage}

	obj, ok := v.(*goja.Object)
	if !ok {
		return out
	}

	if n := obj.Get("pageSize"); n != nil && !goja.IsUndefined(n) && n.ToInteger() > 0 {
		out.pageSize = int(n.ToInteger())
	}

	if c := obj.Get(continuationOptions); c != nil && !goja.IsUndefined(c) && !goja.IsNull(c) {
		out.continuation = c.String()
	}

	if s := obj.Get("enableScan"); s != nil && !goja.IsUndefined(s) {
		out.enableScan = s.ToBoolean()
	}

	return out
}

// page returns one page of rows and records the continuation the callback receives in its options argument.
func (e *scriptEnv) page(rows []any, opts scriptFeedOptions) ([]any, error) {
	start := 0

	if opts.continuation != "" {
		n, err := strconv.Atoi(opts.continuation)
		if err != nil || n < 0 || n > len(rows) {
			return nil, fmt.Errorf("%w: invalid continuation token", ErrInvalidDocument)
		}

		start = n
	}

	end := min(start+opts.pageSize, len(rows))
	e.cbOptions = nil

	if end < len(rows) {
		e.cbOptions = map[string]any{continuationOptions: strconv.Itoa(end)}
	}

	return rows[start:end], nil
}

// queryArg accepts a query string or a {query, parameters} object.
func queryArg(v goja.Value) (string, []QueryParameter, error) {
	switch t := jsonValue(v).(type) {
	case string:
		return t, nil, nil
	case map[string]any:
		query, _ := t["query"].(string)

		return query, queryParams(t["parameters"]), nil
	default:
		return "", nil, fmt.Errorf("%w: a query string or {query, parameters} object is required", ErrQueryParse)
	}
}

func queryParams(raw any) []QueryParameter {
	list, _ := raw.([]any)
	out := make([]QueryParameter, 0, len(list))

	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		name, _ := m["name"].(string)
		out = append(out, QueryParameter{Name: name, Value: m["value"]})
	}

	return out
}
