package appsync

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"maps"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// Conflict detection and resolution (AppSync developer guide, "Conflict detection and resolution").
const (
	conflictDetectionVersion  = "VERSION"
	conflictDetectionNone     = "NONE"
	conflictHandlerOptimistic = "OPTIMISTIC_CONCURRENCY"
	conflictHandlerAutomerge  = "AUTOMERGE"
	conflictHandlerLambda     = "LAMBDA"
	conflictHandlerNone       = "NONE"

	actionResolve = "RESOLVE"
	actionReject  = "REJECT"
	actionRemove  = "REMOVE"

	errCodeConflictError = "ConflictError"

	maxMergeDepth = 32
)

// conflictEnv carries what a versioned write needs to resolve a version mismatch.
type conflictEnv struct {
	sync       *SyncConfig
	lambda     LambdaInvoker
	identity   any
	args       map[string]any
	region     string
	parentType string
	field      string
}

const jsCtxKeyIdentity = "identity"

func (e conflictEnv) detects() bool {
	return e.sync == nil || e.sync.ConflictDetection != conflictDetectionNone
}

func (e conflictEnv) handler() string {
	if e.sync == nil {
		return conflictHandlerOptimistic
	}

	switch e.sync.ConflictHandler {
	case conflictHandlerAutomerge, conflictHandlerLambda:
		return e.sync.ConflictHandler
	}

	return conflictHandlerOptimistic
}

func validateSyncConfig(c *SyncConfig) error {
	if c == nil {
		return nil
	}

	switch c.ConflictDetection {
	case "", conflictDetectionVersion, conflictDetectionNone:
	default:
		return fmt.Errorf("%w: invalid conflictDetection %q", ErrValidation, c.ConflictDetection)
	}

	switch c.ConflictHandler {
	case "", conflictHandlerOptimistic, conflictHandlerAutomerge, conflictHandlerLambda, conflictHandlerNone:
	default:
		return fmt.Errorf("%w: invalid conflictHandler %q", ErrValidation, c.ConflictHandler)
	}

	if c.ConflictHandler == conflictHandlerLambda &&
		(c.LambdaConflictHandlerConfig == nil || c.LambdaConflictHandlerConfig.LambdaConflictHandlerARN == "") {
		return fmt.Errorf("%w: conflictHandler LAMBDA requires lambdaConflictHandlerConfig", ErrValidation)
	}

	return nil
}

func conflictFailure(msg string) error {
	return &DynamoDBError{Code: errCodeConflictError, Message: msg}
}

func isMetadataAttr(name string) bool {
	switch name {
	case attrVersion, attrLastChanged, attrDeleted, attrTTL:
		return true
	}

	return false
}

func withoutMetadata(item map[string]any) map[string]any {
	out := make(map[string]any, len(item))

	for k, v := range item {
		if !isMetadataAttr(k) {
			out[k] = v
		}
	}

	return out
}

func isTombstone(item map[string]any) bool {
	d, _ := item[attrDeleted].(map[string]any)
	b, _ := d[ddbTagBool].(bool)

	return b
}

func attrParts(v any) (string, any, bool) {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return "", nil, false
	}

	for tag, val := range m {
		return tag, val, true
	}

	return "", nil, false
}

// automerge merges incoming into existing by the Automerge rules: existing scalars win, lists concatenate,
// sets union, maps recurse, fields only in incoming (or null in existing) are added, omitted fields stay.
func automerge(existing, incoming map[string]any, depth int) map[string]any {
	out := maps.Clone(existing)
	if out == nil {
		out = make(map[string]any, len(incoming))
	}

	for k, in := range incoming {
		cur, ok := existing[k]
		if !ok {
			out[k] = in

			continue
		}

		if tag, _, isAttr := attrParts(cur); isAttr && tag == ddbTagNull {
			out[k] = in

			continue
		}

		out[k] = mergeAttr(cur, in, depth)
	}

	return out
}

func mergeAttr(cur, in any, depth int) any {
	ctag, cval, cok := attrParts(cur)
	itag, ival, iok := attrParts(in)

	if !cok || !iok || ctag != itag {
		return cur
	}

	switch ctag {
	case "L":
		return map[string]any{"L": append(slices.Clone(listOf(cval)), listOf(ival)...)}
	case "SS", "NS", "BS":
		return map[string]any{ctag: unionSet(cval, ival)}
	case "M":
		cm, cIsMap := cval.(map[string]any)
		im, iIsMap := ival.(map[string]any)

		if !cIsMap || !iIsMap || depth >= maxMergeDepth {
			return cur
		}

		return map[string]any{"M": automerge(cm, im, depth+1)}
	}

	return cur
}

func setMembers(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))

		for _, e := range t {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}

		return out
	}

	return nil
}

func unionSet(a, b any) []any {
	seen := map[string]struct{}{}
	out := []any{}

	for _, s := range append(slices.Clone(setMembers(a)), setMembers(b)...) {
		if _, dup := seen[s]; dup {
			continue
		}

		seen[s] = struct{}{}

		out = append(out, s)
	}

	return out
}

type lambdaResolution struct {
	Item   map[string]any `json:"item"`
	Action string         `json:"action"`
}

func (vs versionedSource) lambdaResolve(
	ctx context.Context, op string, existing, newItem map[string]any,
) (lambdaResolution, error) {
	var res lambdaResolution

	env := vs.conflict
	if env.lambda == nil {
		return res, conflictFailure("no Lambda invoker is configured for the conflict handler")
	}

	payload, err := json.Marshal(map[string]any{
		"newItem":      ddbPlainItem(newItem),
		"existingItem": ddbPlainItem(existing),
		"arguments":    env.args,
		"resolver": map[string]any{
			"tableName": vs.cfg.TableName, "awsRegion": env.region, "parentType": env.parentType, "field": env.field,
		},
		jsCtxKeyIdentity: env.identity,
	}, jsonx.V1())
	if err != nil {
		return res, conflictFailure("the conflict handler payload is not serializable: " + err.Error())
	}

	out, _, err := env.lambda.InvokeFunction(
		withRegion(ctx, env.region), env.sync.LambdaConflictHandlerConfig.LambdaConflictHandlerARN,
		lambdaInvokeSync, payload,
	)
	if err != nil {
		return res, conflictFailure("cannot invoke the Lambda conflict handler: " + err.Error())
	}

	if err = json.Unmarshal(out, &res, jsonx.V1()); err != nil {
		return res, conflictFailure("the Lambda conflict handler returned a malformed response")
	}

	valid := res.Action == actionReject
	if op == ddbOpDeleteItem {
		valid = valid || res.Action == actionRemove
	} else {
		valid = valid || res.Action == actionResolve && res.Item != nil
	}

	if !valid {
		return res, conflictFailure(
			fmt.Sprintf("the Lambda conflict handler returned action %q, invalid for %s", res.Action, op))
	}

	return res, nil
}

// previewWrite returns the images of the rejected write: the item as the mutation would leave it, and for
// UpdateItem the fields the update itself sets.
func (vs versionedSource) previewWrite(
	op string,
	req, existing map[string]any,
) (map[string]any, map[string]any, error) {
	if op != ddbOpUpdateItem {
		put := ddbPutItem(req)

		return put, put, nil
	}

	update := exprOf(req["update"])

	preview, err := vs.ddb.ApplyUpdateRaw(existing, update)
	if err != nil {
		return nil, nil, err
	}

	delta, err := vs.ddb.ApplyUpdateRaw(maps.Clone(mapOf(req["key"])), update)
	if err != nil {
		return nil, nil, err
	}

	return preview, delta, nil
}

// resolveConflict returns the item to store in place of a write whose _version did not match, or the error
// (ConflictUnhandled, ConflictError) the conflict handler produced. For DeleteItem a nil error means the delete
// may proceed.
func (vs versionedSource) resolveConflict(
	ctx context.Context, op string, req, existing map[string]any,
) (map[string]any, error) {
	handler := vs.conflict.handler()
	if op == ddbOpDeleteItem && handler != conflictHandlerLambda || handler == conflictHandlerOptimistic {
		return nil, conflictError(existing)
	}

	preview, delta, err := vs.previewWrite(op, req, existing)
	if err != nil {
		return nil, conflictFailure("cannot compute the mutation preview: " + err.Error())
	}

	if handler == conflictHandlerAutomerge {
		base := existing
		if isTombstone(existing) {
			base = mapOf(req["key"])
		}

		return automerge(withoutMetadata(base), withoutMetadata(delta), 0), nil
	}

	res, err := vs.lambdaResolve(ctx, op, existing, preview)
	if err != nil {
		return nil, err
	}

	switch res.Action {
	case actionRemove:
		return map[string]any{}, nil
	case actionResolve:
		item := make(map[string]any, len(res.Item))

		for k, v := range res.Item {
			if !isMetadataAttr(k) {
				item[k] = ddbTyped(v)
			}
		}

		maps.Copy(item, mapOf(req["key"]))

		return item, nil
	}

	return nil, conflictError(existing)
}
