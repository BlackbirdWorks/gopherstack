package appsync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
)

const (
	lambdaOpBatchInvoke = "BatchInvoke"
	lambdaInvokeEvent   = "Event"
	lambdaInvokeSync    = "RequestResponse"
)

type batchOutcome struct {
	val any
	err error
}

type batchEntry struct {
	cc      *callCtx
	args    map[string]any
	mu      mappingUnit
	key     string
	payload any
}

// batchFields lists the fields of set, descending into fragments.
func batchFields(set ast.SelectionSet) []*ast.Field {
	var out []*ast.Field

	for _, sel := range set {
		switch s := sel.(type) {
		case *ast.Field:
			out = append(out, s)
		case *ast.InlineFragment:
			out = append(out, batchFields(s.SelectionSet)...)
		case *ast.FragmentSpread:
			if s.Definition != nil {
				out = append(out, batchFields(s.Definition.SelectionSet)...)
			}
		}
	}

	return out
}

func (e *execEnv) batchable(r *Resolver) bool {
	if e == nil || r == nil || r.Kind == resolverKindPipeline {
		return false
	}

	ds := e.datasources[r.DataSourceName]
	if ds == nil || ds.Type != DataSourceTypeLambda {
		return false
	}

	return r.MaxBatchSize > 0 ||
		strings.Contains(r.RequestMappingTemplate, lambdaOpBatchInvoke) || strings.Contains(r.Code, lambdaOpBatchInvoke)
}

// prebatch resolves BatchInvoke Lambda fields of every list item together,
// one Lambda call per maxBatchSize items, and stores the per-item outcomes
// under their response path for resolveFieldValue.
func (e *execEnv) prebatch(ctx context.Context, items []any, set ast.SelectionSet, path []any) {
	if e == nil {
		return
	}

	for _, f := range batchFields(set) {
		if f.ObjectDefinition == nil {
			continue
		}

		r := e.resolvers[resolverKey(f.ObjectDefinition.Name, f.Name)]
		if e.batchable(r) {
			e.runBatch(ctx, f, r, items, path)
		}
	}
}

func (e *execEnv) runBatch(ctx context.Context, f *ast.Field, r *Resolver, items []any, path []any) {
	if e.backend.lambdaFn == nil {
		return
	}

	var (
		entries []batchEntry
		invType = lambdaInvokeSync
		out     = map[string]batchOutcome{}
	)

	for i, it := range items {
		src, ok := it.(map[string]any)
		if !ok {
			continue
		}

		cc := e.newCallCtx(f.ObjectDefinition.Name, f, false)
		ent := batchEntry{
			cc: cc, args: extractArguments(f, e.variables), mu: resolverMappingUnit(r, src),
			key: pathString(appendPath(appendPath(path, i), f.Alias)),
		}
		ent.mu.Call = cc

		payload, itype, outcome, state := batchRequest(ent.mu, ent.args)

		switch state {
		case batchSkip:
			return
		case batchDone:
			out[ent.key] = outcome

			continue
		}

		if itype == lambdaInvokeEvent {
			invType = lambdaInvokeEvent
		}

		ent.payload = payload
		entries = append(entries, ent)
	}

	size := max(int(r.MaxBatchSize), 1)
	ds := e.datasources[r.DataSourceName]

	for start := 0; start < len(entries); start += size {
		chunk := entries[start:min(start+size, len(entries))]
		e.invokeBatch(ctx, ds, invType, chunk, out)
	}

	if e.batched == nil {
		e.batched = map[string]batchOutcome{}
	}

	for _, ent := range entries {
		e.ext = append(e.ext, ent.cc.extensionCalls()...)
	}

	for k, v := range out {
		e.batched[k] = v
	}
}

const (
	batchReady = iota
	batchDone
	batchSkip
)

// batchRequest evaluates one item's request mapping. batchSkip means the
// resolver does not use BatchInvoke and must resolve item by item.
func batchRequest(mu mappingUnit, args map[string]any) (any, string, batchOutcome, int) {
	value, ok, err := evalRequestMapping(mu, args, nil)
	if err != nil {
		if early, isEarly := evalEarly(err); isEarly {
			return nil, "", batchOutcome{val: early}, batchDone
		}

		return nil, "", batchOutcome{err: err}, batchDone
	}

	if !ok {
		return defaultLambdaEvent(mu, args), lambdaInvokeSync, batchOutcome{}, batchReady
	}

	m, isMap := value.(map[string]any)
	if !isMap || m["operation"] != lambdaOpBatchInvoke {
		return nil, "", batchOutcome{}, batchSkip
	}

	itype, _ := m["invocationType"].(string)

	return m["payload"], itype, batchOutcome{}, batchReady
}

func (e *execEnv) invokeBatch(
	ctx context.Context, ds *DataSource, invType string, chunk []batchEntry, out map[string]batchOutcome,
) {
	fail := func(err error) {
		for _, ent := range chunk {
			out[ent.key] = batchOutcome{err: err}
		}
	}

	if ds.LambdaConfig == nil {
		fail(ErrLambdaMissingConfig)

		return
	}

	payloads := make([]any, len(chunk))
	for i, ent := range chunk {
		payloads[i] = ent.payload
	}

	body, err := json.Marshal(payloads)
	if err != nil {
		fail(err)

		return
	}

	raw, _, err := e.backend.lambdaFn.InvokeFunction(
		withRegion(ctx, e.backend.region), ds.LambdaConfig.LambdaFunctionARN, invType, body,
	)
	if err != nil {
		fail(fmt.Errorf("lambda invocation failed: %w", err))

		return
	}

	var results []any

	if invType == lambdaInvokeEvent {
		results = make([]any, len(chunk))
	} else if err = json.Unmarshal(raw, &results); err != nil || len(results) != len(chunk) {
		fail(fmt.Errorf("%w: BatchInvoke response must be a list of %d items", ErrValidation, len(chunk)))

		return
	}

	for i, ent := range chunk {
		out[ent.key] = batchItemOutcome(ent, results[i])
	}
}

// batchItemOutcome maps one Lambda batch element to the field result. Without
// a response mapping the element is {data, errorMessage, errorType}.
func batchItemOutcome(ent batchEntry, elem any) batchOutcome {
	if ent.mu.Code != "" || ent.mu.ResponseTemplate != "" {
		v, err := evalResponseMapping(ent.mu, ent.args, elem)

		return batchOutcome{val: v, err: err}
	}

	m, ok := elem.(map[string]any)
	if !ok {
		return batchOutcome{val: elem}
	}

	if msg := m["errorMessage"]; msg != nil {
		typ, _ := m["errorType"].(string)

		return batchOutcome{err: &resolverError{Message: fmt.Sprint(msg), Type: typ, Data: m["data"]}}
	}

	return batchOutcome{val: m["data"]}
}
