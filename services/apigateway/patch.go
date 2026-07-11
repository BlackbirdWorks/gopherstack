package apigateway

import (
	"encoding/json"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// patch.go implements AWS API Gateway's PATCH-operation semantics beyond what
// a flat "replace one top-level field" model (the old normalizePatchBody) can
// express. Every Update* REST operation in this service (UpdateRestApi,
// UpdateStage, UpdateAccount, UpdateUsagePlan, UpdateGatewayResponse, ...)
// shares the same wire shape: a PATCH request body of
// {"patchOperations":[{"op":...,"path":...,"value":...,"from":...}, ...]}
// (aws-sdk-go-v2/service/apigateway/types.PatchOperation), or a bare RFC-6902
// array. Two things about that wire shape are easy to get wrong and were
// previously wrong here:
//
//  1. PatchOperation.Value is *always* a JSON string on the wire — the real
//     SDK serializer (awsRestjson1_serializeDocumentPatchOperation) calls
//     String() unconditionally — even when the target field is a bool or a
//     number (e.g. {"op":"replace","path":"/tracingEnabled","value":"true"}).
//     Copying that raw JSON string byte-for-byte into a field that unmarshals
//     into a Go bool/int (e.g. UpdateStageInput.TracingEnabled *bool) fails
//     with a JSON type-mismatch error. patchValueString + the per-field
//     coercion below convert the string payload to the JSON literal the
//     target field actually needs.
//  2. Many real-world patches don't target a top-level field at all — they
//     target one entry of a map/list, or a nested struct field, e.g.
//     "/variables/apiKey" (one stage variable), "/binaryMediaTypes/image~1png"
//     (one binary media type, JSON-Pointer-escaped), "/throttle/rateLimit"
//     (one field of the account's/usage plan's ThrottleSettings),
//     "/apiStages" (usage-plan API stage membership, add/remove, value
//     "restApiId:stage"), or per-method stage settings addressed as
//     "/{resourcePath}/{httpMethod}/{category}/{property}" with NO
//     "methodSettings" path prefix (resourcePath JSON-Pointer-escaped, or "*"
//     for the wildcard default — e.g. "/~1pets/GET/throttling/burstLimit" or
//     "/*/*/logging/loglevel"). The previous flatten took the *entire*
//     remaining path verbatim as a single flat field name (e.g.
//     "variables/apiKey"), which never matches any Update*Input json tag, so
//     the edit was silently dropped. This also affected canary-deployment
//     promotion, which AWS models as a "copy" op
//     ({"op":"copy","from":"/canarySettings/deploymentId","path":"/deploymentId"})
//     that the old flatten didn't implement at all — "copy" isn't "add" or
//     "replace" so it was unconditionally skipped, alongside "remove".
//
// Resolving (1) and (2) requires reading the CURRENT resource (to merge one
// map/struct entry into its existing siblings — the Update* backend methods
// replace a map/struct field wholesale when it's provided), so this logic
// needs Handler.Backend access and lives here rather than in a pure body ->
// body helper.
//
// NOTE: the per-route method-settings property paths
// (stageMethodSettingProperty below) are a best-effort mapping from AWS's
// "Method Settings Property of a Stage" patch-operations reference. Unlike
// the struct field names above (verified against aws-sdk-go-v2/service/
// apigateway/types), the SDK does not expose these dotted-path strings as a
// typed enum to check against — PatchOperation.Path is just a free string.
// Flag it if a live wire capture disagrees with the exact property spelling.

// PATCH operation verbs (aws-sdk-go-v2/service/apigateway/types.Op) and the
// scalar-kind tags used by patchFieldKind/coerceTopLevelPatchValue.
const (
	patchOpAdd     = "add"
	patchOpReplace = "replace"
	patchOpRemove  = "remove"
	patchOpCopy    = "copy"

	patchKindBool = "bool"
	patchKindInt  = "int"

	// patchPathSegs2 is the segment count of a two-level PATCH path such as
	// "/variables/{name}" or "/throttle/rateLimit" (field + one sub-key).
	patchPathSegs2 = 2
)

// patchOp is a single PATCH operation from a PATCH request body, matching
// aws-sdk-go-v2/service/apigateway/types.PatchOperation's wire shape:
// {"op":"replace","path":"/description","value":"foo"}, or, for the "copy"
// op used e.g. to promote a canary deployment,
// {"op":"copy","from":"/canarySettings/deploymentId","path":"/deploymentId"}.
type patchOp struct {
	Op    string          `json:"op"`
	Path  string          `json:"path"`
	From  string          `json:"from,omitempty"`
	Value json.RawMessage `json:"value"`
}

// parsePatchDocument detects and parses a PATCH request body: either a bare
// RFC-6902-flavored array ([{"op":...}]) or a {"patchOperations":[...]}
// wrapper (the real AWS wire shape), which may carry sibling fields alongside
// "patchOperations" (rest). ok is false when body is neither shape, in which
// case the caller should treat body as an already-flat field object.
func parsePatchDocument(body []byte) ([]patchOp, map[string]json.RawMessage, bool) {
	if len(body) == 0 {
		return nil, nil, false
	}

	switch body[0] {
	case '[':
		var arr []patchOp
		if err := json.Unmarshal(body, &arr); err != nil || len(arr) == 0 || arr[0].Op == "" {
			return nil, nil, false
		}

		return arr, nil, true
	case '{':
		var wrapper struct {
			PatchOperations []patchOp `json:"patchOperations"`
		}
		if err := json.Unmarshal(body, &wrapper); err != nil || len(wrapper.PatchOperations) == 0 {
			return nil, nil, false
		}

		var siblings map[string]json.RawMessage

		_ = json.Unmarshal(body, &siblings)
		delete(siblings, "patchOperations")

		return wrapper.PatchOperations, siblings, true
	default:
		return nil, nil, false
	}
}

// patchFieldKind classifies known non-string top-level PATCH field names by
// their real Go/JSON type, so patchValueString's string payload is coerced to
// a matching JSON literal (see the package doc above) instead of being copied
// in verbatim as a JSON string that the target field can't unmarshal.
//
//nolint:gochecknoglobals // read-only lookup table initialized once at startup
var patchFieldKind = map[string]string{
	"tracingEnabled":               patchKindBool,
	"cacheClusterEnabled":          patchKindBool,
	"minimumCompressionSize":       patchKindInt,
	"enabled":                      patchKindBool, // ApiKey.enabled
	"validateRequestBody":          patchKindBool,
	"validateRequestParameters":    patchKindBool,
	"apiKeyRequired":               patchKindBool,
	"authorizerResultTtlInSeconds": patchKindInt,
}

// coerceTopLevelPatchValue converts a top-level PATCH value (always a JSON
// string on the wire, see patchValueString) into the JSON literal the target
// field's Go type needs, for the handful of non-string fields patched at the
// top level. Fields absent from patchFieldKind are assumed to be strings (the
// common case, and already correctly a JSON string on the wire) and passed
// through unchanged.
func coerceTopLevelPatchValue(field string, raw json.RawMessage) json.RawMessage {
	kind, known := patchFieldKind[field]
	if !known {
		return raw
	}

	s, ok := patchValueString(raw)
	if !ok {
		return raw
	}

	switch kind {
	case patchKindBool:
		if b, parseErr := strconv.ParseBool(s); parseErr == nil {
			if encoded, marshalErr := json.Marshal(b); marshalErr == nil {
				return encoded
			}
		}
	case patchKindInt:
		if n, parseErr := strconv.Atoi(s); parseErr == nil {
			if encoded, marshalErr := json.Marshal(n); marshalErr == nil {
				return encoded
			}
		}
	}

	return raw
}

// patchValueString decodes a PATCH operation's Value into a Go string. Value
// is always transmitted as a JSON string on the wire, even for boolean or
// numeric targets (see the package doc above), so every call site that needs
// the underlying scalar goes through this and then parses it with strconv.
// ok is false when Value is empty/absent or not a JSON string.
func patchValueString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}

	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}

	return s, true
}

// jsonPointerUnescape decodes a single RFC 6901 JSON Pointer path segment
// ("~1" -> "/", then "~0" -> "~"; that order matters so "~01" decodes to "~1"
// and not "/").
func jsonPointerUnescape(seg string) string {
	seg = strings.ReplaceAll(seg, "~1", "/")

	return strings.ReplaceAll(seg, "~0", "~")
}

// patchPathSegments splits a JSON-Pointer path into its still-escaped raw
// segments, dropping the leading empty segment produced by the leading "/".
func patchPathSegments(path string) []string {
	trimmed := strings.TrimPrefix(path, "/")
	if trimmed == "" {
		return nil
	}

	return strings.Split(trimmed, "/")
}

// setJSONValue marshals value and stores it under field in out. Errors are
// swallowed (defensive only): every call site below passes a well-formed
// built-in Go value that always marshals successfully.
func setJSONValue(out map[string]json.RawMessage, field string, value any) {
	raw, err := json.Marshal(value)
	if err != nil {
		return
	}

	out[field] = raw
}

// cloneStringMap returns a non-nil shallow copy of m. A nil result would be
// indistinguishable, to the Update* backend methods' "field != nil means the
// caller provided it" checks, from the field being entirely absent from the
// patch — so a merge that removes the last entry must still produce a
// non-nil empty map for the removal to actually take effect.
func cloneStringMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}

// applyStructuredPatch resolves a PATCH request body into the flat JSON
// object that the target action's Update*Input struct unmarshals from,
// correctly handling per-entry map/list edits, nested struct-field edits,
// per-route stage method settings, "remove", and "copy" — none of which the
// old single-field flatten supported (see package doc). Non-patch bodies
// (plain field objects, GET/DELETE's synthesized "{}", etc.) pass through
// unchanged.
func (h *Handler) applyStructuredPatch(action string, pathParams map[string]string, body []byte) []byte {
	ops, rest, isPatch := parsePatchDocument(body)
	if !isPatch {
		return body
	}

	out := make(map[string]json.RawMessage, len(rest)+len(ops))
	maps.Copy(out, rest)

	for _, op := range ops {
		if h.applyResourcePatchOp(action, pathParams, op, out) {
			continue
		}

		applyTopLevelPatchOp(op, out)
	}

	raw, err := json.Marshal(out)
	if err != nil {
		return body
	}

	return raw
}

// applyTopLevelPatchOp is the fallback for every path/action combination the
// resource-specific resolvers below don't recognize: a plain single-segment
// "add"/"replace" of one field, with type coercion for the small set of
// known non-string fields (see patchFieldKind). "remove" and multi-segment
// paths that no resolver claimed are left as no-ops, matching the previous
// (silent-drop) behavior, since correctly clearing a bare scalar field would
// need every Update*Input field to distinguish "not provided" from "reset to
// zero value" — a much larger refactor across every resource's Update
// backend method than this pass's PATCH-semantics fix. Tracked as a follow-up.
func applyTopLevelPatchOp(op patchOp, out map[string]json.RawMessage) {
	if op.Op != patchOpReplace && op.Op != patchOpAdd {
		return
	}

	field := strings.TrimPrefix(op.Path, "/")
	if field == "" || strings.Contains(field, "/") {
		return
	}

	out[field] = coerceTopLevelPatchValue(field, op.Value)
}

// applyResourcePatchOp dispatches to the per-action patch resolver for
// actions with map/struct/list PATCH targets that need current backend state
// to merge correctly. Returns true when the op was fully handled (including
// "handled as an intentional no-op", e.g. an unresolvable "copy" source) so
// the caller must not also apply the generic top-level fallback.
func (h *Handler) applyResourcePatchOp(
	action string, pathParams map[string]string, op patchOp, out map[string]json.RawMessage,
) bool {
	segs := patchPathSegments(op.Path)
	if len(segs) == 0 {
		return false
	}

	switch action {
	case opUpdateStage:
		return h.applyStagePatchOp(pathParams, op, segs, out)
	case opUpdateRestAPI:
		return h.applyRestAPIPatchOp(pathParams, op, segs, out)
	case opUpdateAccount:
		return h.applyAccountPatchOp(op, segs, out)
	case opUpdateUsagePlan:
		return h.applyUsagePlanPatchOp(pathParams, op, segs, out)
	case opUpdateGatewayResponse:
		return h.applyGatewayResponsePatchOp(pathParams, op, segs, out)
	default:
		return false
	}
}

// applyStagePatchOp handles UpdateStage PATCH ops that a flat top-level
// replace cannot express: canary-deployment promotion (a "copy" op), single
// stage-variable edits, single canarySettings/accessLogSettings sub-fields,
// and per-route method settings.
func (h *Handler) applyStagePatchOp(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) bool {
	if op.Op == patchOpCopy && op.Path == "/deploymentId" && op.From == "/canarySettings/deploymentId" {
		stage, err := h.Backend.GetStage(pathParams[keyRestAPIID], pathParams[keyStageName])
		if err != nil || stage.CanarySettings == nil {
			return true
		}

		setJSONValue(out, "deploymentId", stage.CanarySettings.DeploymentID)

		return true
	}

	if len(segs) >= 3 && (segs[0] == "*" || strings.HasPrefix(segs[0], "~1")) {
		return h.applyStageMethodSettingPatch(pathParams, op, segs, out)
	}

	if len(segs) != patchPathSegs2 {
		return false
	}

	switch segs[0] {
	case "variables":
		return h.applyStageVariablePatch(pathParams, op, segs[1], out)
	case "canarySettings":
		return h.applyStageCanaryPatch(pathParams, op, segs[1], out)
	case "accessLogSettings":
		return h.applyStageAccessLogPatch(pathParams, op, segs[1], out)
	default:
		return false
	}
}

// applyStageVariablePatch adds/replaces/removes a single stage variable
// ("/variables/{name}"), merging with the stage's existing variables (a
// wholesale replace would otherwise silently drop every other variable).
func (h *Handler) applyStageVariablePatch(
	pathParams map[string]string, op patchOp, rawName string, out map[string]json.RawMessage,
) bool {
	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return false
	}

	stage, err := h.Backend.GetStage(pathParams[keyRestAPIID], pathParams[keyStageName])
	if err != nil {
		return true
	}

	vars := cloneStringMap(stage.Variables)
	name := jsonPointerUnescape(rawName)

	if op.Op == patchOpRemove {
		delete(vars, name)
	} else if v, ok := patchValueString(op.Value); ok {
		vars[name] = v
	}

	setJSONValue(out, "variables", vars)

	return true
}

// applyStageCanaryPatch replaces a single CanarySettings sub-field
// ("/canarySettings/{deploymentId,percentTraffic,useStageCache}"), merging
// with the stage's existing canary settings.
func (h *Handler) applyStageCanaryPatch(
	pathParams map[string]string, op patchOp, prop string, out map[string]json.RawMessage,
) bool {
	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return false
	}

	stage, err := h.Backend.GetStage(pathParams[keyRestAPIID], pathParams[keyStageName])
	if err != nil {
		return true
	}

	cur := CanarySettings{}
	if stage.CanarySettings != nil {
		cur = *stage.CanarySettings
	}

	val, _ := patchValueString(op.Value)
	remove := op.Op == patchOpRemove

	switch prop {
	case "deploymentId":
		if remove {
			cur.DeploymentID = ""
		} else {
			cur.DeploymentID = val
		}
	case "percentTraffic":
		if remove {
			cur.PercentTraffic = 0
		} else if f, parseErr := strconv.ParseFloat(val, 64); parseErr == nil {
			cur.PercentTraffic = f
		}
	case "useStageCache":
		cur.UseStageCache = !remove && parseBoolLenient(val)
	default:
		return false
	}

	setJSONValue(out, "canarySettings", &cur)

	return true
}

// applyStageAccessLogPatch replaces a single AccessLogSettings sub-field
// ("/accessLogSettings/{destinationArn,format}"), merging with the stage's
// existing access log settings.
func (h *Handler) applyStageAccessLogPatch(
	pathParams map[string]string, op patchOp, prop string, out map[string]json.RawMessage,
) bool {
	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return false
	}

	stage, err := h.Backend.GetStage(pathParams[keyRestAPIID], pathParams[keyStageName])
	if err != nil {
		return true
	}

	cur := AccessLogSettings{}
	if stage.AccessLogSettings != nil {
		cur = *stage.AccessLogSettings
	}

	val, _ := patchValueString(op.Value)
	if op.Op == patchOpRemove {
		val = ""
	}

	switch prop {
	case "destinationArn":
		cur.DestinationARN = val
	case "format":
		cur.Format = val
	default:
		return false
	}

	setJSONValue(out, "accessLogSettings", &cur)

	return true
}

// stageMethodSettingProperty maps a per-route method-settings PATCH property
// path (the segments after "{resourcePath}/{httpMethod}", joined with "/") to
// the MethodSetting field it targets. See the package doc's note on the
// accuracy of these property strings.
//
//nolint:gochecknoglobals // read-only lookup table initialized once at startup
var stageMethodSettingProperty = map[string]func(ms *MethodSetting, value string, remove bool){
	"logging/loglevel": func(ms *MethodSetting, v string, remove bool) {
		if remove {
			ms.LoggingLevel = ""

			return
		}

		ms.LoggingLevel = v
	},
	"logging/dataTrace": func(ms *MethodSetting, v string, remove bool) {
		ms.DataTraceEnabled = !remove && parseBoolLenient(v)
	},
	"metrics/enabled": func(ms *MethodSetting, v string, remove bool) {
		ms.MetricsEnabled = !remove && parseBoolLenient(v)
	},
	"throttling/burstLimit": func(ms *MethodSetting, v string, remove bool) {
		if remove {
			ms.ThrottlingBurstLimit = 0

			return
		}

		if n, err := strconv.Atoi(v); err == nil {
			ms.ThrottlingBurstLimit = n
		}
	},
	"throttling/rateLimit": func(ms *MethodSetting, v string, remove bool) {
		if remove {
			ms.ThrottlingRateLimit = 0

			return
		}

		if f, err := strconv.ParseFloat(v, 64); err == nil {
			ms.ThrottlingRateLimit = f
		}
	},
	"caching/enabled": func(ms *MethodSetting, v string, remove bool) {
		ms.CachingEnabled = !remove && parseBoolLenient(v)
	},
	"caching/ttlInSeconds": func(ms *MethodSetting, v string, remove bool) {
		if remove {
			ms.CacheTTLInSeconds = 0

			return
		}

		if n, err := strconv.Atoi(v); err == nil {
			ms.CacheTTLInSeconds = n
		}
	},
	"caching/requireAuthorizationForCacheControl": func(ms *MethodSetting, v string, remove bool) {
		ms.RequireAuthorizationForCacheControl = !remove && parseBoolLenient(v)
	},
}

// parseBoolLenient parses v as a bool, defaulting to false on a malformed
// value rather than erroring the whole PATCH request over one bad flag.
func parseBoolLenient(v string) bool {
	b, _ := strconv.ParseBool(v)

	return b
}

// applyStageMethodSettingPatch handles per-route stage method settings,
// addressed as "/{resourcePath}/{httpMethod}/{category}/{property}" with NO
// "methodSettings" path prefix — this is how AWS actually addresses them
// (e.g. "/~1pets/GET/throttling/burstLimit" or "/*/*/logging/loglevel") —
// merged with the stage's existing per-route overrides.
func (h *Handler) applyStageMethodSettingPatch(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) bool {
	apply, known := stageMethodSettingProperty[strings.Join(segs[2:], "/")]
	if !known || (op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove) {
		return false
	}

	stage, err := h.Backend.GetStage(pathParams[keyRestAPIID], pathParams[keyStageName])
	if err != nil {
		return true
	}

	settings := make(map[string]MethodSetting, len(stage.MethodSettings)+1)
	maps.Copy(settings, stage.MethodSettings)

	routeKey := jsonPointerUnescape(segs[0]) + "/" + segs[1]
	ms := settings[routeKey]

	val, _ := patchValueString(op.Value)
	apply(&ms, val, op.Op == patchOpRemove)
	settings[routeKey] = ms

	setJSONValue(out, "methodSettings", settings)

	return true
}

// applyRestAPIPatchOp handles UpdateRestApi's binary-media-type membership
// edits ("/binaryMediaTypes/{escaped-media-type}", add/remove), merging with
// the API's existing binary media types (a wholesale replace would otherwise
// silently drop every other configured type).
func (h *Handler) applyRestAPIPatchOp(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) bool {
	if len(segs) != patchPathSegs2 || segs[0] != "binaryMediaTypes" {
		return false
	}

	if op.Op != patchOpAdd && op.Op != patchOpRemove {
		return false
	}

	api, err := h.Backend.GetRestAPI(pathParams[keyRestAPIID])
	if err != nil {
		return true
	}

	mediaType := jsonPointerUnescape(segs[1])
	types := slices.Clone(api.BinaryMediaTypes)

	if op.Op == patchOpAdd {
		if !slices.Contains(types, mediaType) {
			types = append(types, mediaType)
		}
	} else {
		types = slices.DeleteFunc(types, func(t string) bool { return t == mediaType })
	}

	if types == nil {
		types = []string{}
	}

	setJSONValue(out, "binaryMediaTypes", types)

	return true
}

// applyAccountPatchOp handles UpdateAccount's nested ThrottleSettings edits
// ("/throttle/{rateLimit,burstLimit}"), merging with the account's existing
// throttle settings. "/cloudwatchRoleArn" is a plain top-level string field
// and is handled by the generic fallback (applyTopLevelPatchOp).
func (h *Handler) applyAccountPatchOp(op patchOp, segs []string, out map[string]json.RawMessage) bool {
	if len(segs) != patchPathSegs2 || segs[0] != "throttle" {
		return false
	}

	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return false
	}

	acct, err := h.Backend.GetAccount()
	if err != nil {
		return true
	}

	cur := ThrottleSettings{}
	if acct.ThrottleSettings != nil {
		cur = *acct.ThrottleSettings
	}

	val, _ := patchValueString(op.Value)
	remove := op.Op == patchOpRemove

	switch segs[1] {
	case "rateLimit":
		if remove {
			cur.RateLimit = 0
		} else if f, parseErr := strconv.ParseFloat(val, 64); parseErr == nil {
			cur.RateLimit = f
		}
	case "burstLimit":
		if remove {
			cur.BurstLimit = 0
		} else if n, parseErr := strconv.Atoi(val); parseErr == nil {
			cur.BurstLimit = n
		}
	default:
		return false
	}

	setJSONValue(out, "throttleSettings", &cur)

	return true
}

// applyUsagePlanPatchOp handles UpdateUsagePlan's API-stage membership edits:
// AWS addresses these with a single-segment path ("/apiStages"), add/remove,
// where Value is the string "{restApiId}:{stage}" (not a nested path) — see
// the AWS "Usage Plan Update Operation" patch-operations reference.
func (h *Handler) applyUsagePlanPatchOp(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) bool {
	if len(segs) != 1 || segs[0] != "apiStages" {
		return false
	}

	if op.Op != patchOpAdd && op.Op != patchOpRemove {
		return false
	}

	val, ok := patchValueString(op.Value)
	if !ok {
		return true
	}

	restAPIID, stage, cut := strings.Cut(val, ":")
	if !cut {
		return true
	}

	plan, err := h.Backend.GetUsagePlan(pathParams[keyUsagePlanID])
	if err != nil {
		return true
	}

	matches := func(a APIStageAssociation) bool { return a.RestAPIID == restAPIID && a.Stage == stage }
	stages := slices.Clone(plan.APIStages)

	if op.Op == patchOpAdd {
		if !slices.ContainsFunc(stages, matches) {
			stages = append(stages, APIStageAssociation{RestAPIID: restAPIID, Stage: stage})
		}
	} else {
		stages = slices.DeleteFunc(stages, matches)
	}

	if stages == nil {
		stages = []APIStageAssociation{}
	}

	setJSONValue(out, "apiStages", stages)

	return true
}

// applyGatewayResponsePatchOp handles UpdateGatewayResponse's per-key
// responseParameters/responseTemplates edits
// ("/responseParameters/{key}", "/responseTemplates/{key}"), merging with the
// gateway response's existing entries (falling back to AWS's implicit
// default response when none has been customized yet).
func (h *Handler) applyGatewayResponsePatchOp(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) bool {
	if len(segs) != patchPathSegs2 || (segs[0] != "responseParameters" && segs[0] != "responseTemplates") {
		return false
	}

	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return false
	}

	gr, err := h.Backend.GetGatewayResponse(pathParams[keyRestAPIID], pathParams[keyResponseType])
	if err != nil {
		return true
	}

	key := jsonPointerUnescape(segs[1])

	var m map[string]string
	if segs[0] == "responseParameters" {
		m = cloneStringMap(gr.ResponseParameters)
	} else {
		m = cloneStringMap(gr.ResponseTemplates)
	}

	if op.Op == patchOpRemove {
		delete(m, key)
	} else if v, ok := patchValueString(op.Value); ok {
		m[key] = v
	}

	setJSONValue(out, segs[0], m)

	return true
}
