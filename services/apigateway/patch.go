package apigateway

import (
	"encoding/json"
	"fmt"
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

	// fieldRequestParameters and fieldResponseParameters name the
	// Method/Integration/MethodResponse/IntegrationResponse request- and
	// response-parameter map fields shared by several per-key PATCH resolvers
	// below.
	fieldRequestParameters  = "requestParameters"
	fieldResponseParameters = "responseParameters"
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
	"disableExecuteApiEndpoint":    patchKindBool, // RestApi.disableExecuteApiEndpoint
	"timeoutInMillis":              patchKindInt,  // Integration.timeoutInMillis
}

// removableTopLevelScalar lists, per action, the single-segment top-level
// scalar PATCH fields where AWS documents op:remove as supported
// (patch-operations.html) and this service's Update*Input struct has been
// migrated to a *string to make "explicitly cleared" distinguishable from
// "not touched by this PATCH" (see e.g. UpdateRestAPIInput.Description's doc
// comment). Fields not listed here still silently no-op on "remove" — the
// broader gap (every OTHER Update*Input uses a bare zero-value-means-absent
// string/int) is tracked in PARITY.md and is NOT fixed by this table; only
// entries actually present here have a working "remove".
//
//nolint:gochecknoglobals // read-only lookup table initialized once at startup
var removableTopLevelScalar = map[string]map[string]bool{
	opUpdateRestAPI:    {"description": true},
	opUpdateAuthorizer: {"identitySource": true},
	opUpdateDomainName: {
		"certificateArn": true, "certificateName": true,
		"regionalCertificateArn": true, "regionalCertificateName": true,
		"ownershipVerificationCertificateArn": true,
	},
	opUpdateUsagePlan: {"productCode": true},
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

// patchIgnorableErr reports whether err is non-nil, for the handful of spots
// in this file that deliberately swallow an error rather than propagate it:
// a backend lookup needed to merge a PATCH op into current state failing
// (e.g. the target was deleted concurrently — the outer Update* backend call
// re-resolves the same identifier right after and returns the authoritative
// NotFoundException, so this lookup's error would be redundant), or the
// defensive json.Marshal(out) in applyStructuredPatch falling back to the
// original body. Factored out so that intent, rather than a bare "err !=
// nil" the nilerr linter can't tell from an accidental swallow, is visible at
// every call site.
func patchIgnorableErr(err error) bool {
	return err != nil
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

// stagedValue decodes the JSON already staged in out[field] into a T, when
// present. Several resolvers below (per-route stage method settings,
// usage-plan API-stage membership/throttle) merge one entry into a
// map/slice-valued field that a single PATCH request's patchOperations array
// can legitimately touch more than once (e.g. setting both throttle/rateLimit
// and throttle/burstLimit for the same route in one request). Without this,
// each op would independently re-derive its starting point from the
// backend's pre-PATCH state and overwrite out[field] wholesale, silently
// discarding whatever an earlier op in the SAME request had already staged
// there. ok is false when field isn't staged yet or fails to decode, in
// which case the caller must fall back to reading current backend state.
func stagedValue[T any](out map[string]json.RawMessage, field string) (T, bool) {
	var v T

	raw, present := out[field]
	if !present {
		return v, false
	}

	if err := json.Unmarshal(raw, &v); err != nil {
		return v, false
	}

	return v, true
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

// cloneBoolMap is cloneStringMap's map[string]bool counterpart, used for
// Method.RequestParameters and MethodResponse.ResponseParameters (both
// AWS-modeled as presence flags rather than string values).
func cloneBoolMap(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
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
func (h *Handler) applyStructuredPatch(action string, pathParams map[string]string, body []byte) ([]byte, error) {
	ops, rest, isPatch := parsePatchDocument(body)
	if !isPatch {
		return body, nil
	}

	out := make(map[string]json.RawMessage, len(rest)+len(ops))
	maps.Copy(out, rest)

	for _, op := range ops {
		handled, err := h.applyResourcePatchOp(action, pathParams, op, out)
		if err != nil {
			return nil, err
		}

		if handled {
			continue
		}

		applyTopLevelPatchOp(action, op, out)
	}

	raw, err := json.Marshal(out)
	if patchIgnorableErr(err) {
		return body, nil
	}

	return raw, nil
}

// unsupportedPatchOp rejects an op/path combination that patch-operations.html
// documents as "Not supported" for this path, instead of silently applying or
// dropping it (see package doc: silently accepting a patch that changes nothing
// is the bug this file fixes).
func unsupportedPatchOp(path, op string) error {
	return fmt.Errorf("%w: unsupported PATCH op %q for path %q", ErrInvalidParameter, op, path)
}

// unmodeledPatchPath rejects a PATCH path that patch-operations.html documents as
// AWS-supported but that this backend does not (yet) track as real state, rather
// than silently accepting a patch that changes nothing.
func unmodeledPatchPath(action, path string) error {
	return fmt.Errorf("%w: %s does not support PATCH path %q in this emulator", ErrInvalidParameter, action, path)
}

// applyTopLevelPatchOp is the fallback for every path/action combination the
// resource-specific resolvers below don't recognize: a plain single-segment
// "add"/"replace" of one field, with type coercion for the small set of
// known non-string fields (see patchFieldKind). "remove" is applied (as an
// explicit zero-value write) only for the handful of action+field pairs in
// removableTopLevelScalar whose Update*Input field is a pointer able to
// represent "explicitly cleared"; every other field's "remove" is still a
// no-op, matching the previous (silent-drop) behavior, since correctly
// clearing a bare scalar field needs its Update*Input field to distinguish
// "not provided" from "reset to zero value" — a much larger refactor across
// every resource's Update backend method than this pass's PATCH-semantics
// fix covers in full. Tracked as a follow-up (see PARITY.md gaps).
func applyTopLevelPatchOp(action string, op patchOp, out map[string]json.RawMessage) {
	field := strings.TrimPrefix(op.Path, "/")
	if field == "" || strings.Contains(field, "/") {
		return
	}

	switch op.Op {
	case patchOpReplace, patchOpAdd:
		out[field] = coerceTopLevelPatchValue(field, op.Value)
	case patchOpRemove:
		if removableTopLevelScalar[action][field] {
			out[field] = json.RawMessage(`""`)
		}
	}
}

// resourcePatchResolver is the shared signature for every per-action PATCH
// resolver dispatched by resourcePatchResolvers. Returns (true, nil) when the
// op was fully handled (including "handled as an intentional no-op", e.g. an
// unresolvable "copy" source), (true, err) when the op/path/value combination
// is one this action must reject, or (false, nil) to fall through to the
// generic top-level fallback.
type resourcePatchResolver func(
	h *Handler, pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) (bool, error)

// resourcePatchResolvers maps each action with map/struct/list/rejectable
// PATCH targets to its resolver. A map keeps applyResourcePatchOp a flat
// lookup instead of a growing switch, so adding actions here doesn't raise
// its cyclomatic complexity.
//
//nolint:gochecknoglobals // read-only dispatch table initialized once at startup
var resourcePatchResolvers = map[string]resourcePatchResolver{
	opUpdateStage:               (*Handler).applyStagePatchOp,
	opUpdateRestAPI:             (*Handler).applyRestAPIPatchOp,
	opUpdateAccount:             (*Handler).applyAccountPatchOp,
	opUpdateUsagePlan:           (*Handler).applyUsagePlanPatchOp,
	opUpdateGatewayResponse:     (*Handler).applyGatewayResponsePatchOp,
	opUpdateAPIKey:              (*Handler).applyAPIKeyPatchOp,
	opUpdateDomainName:          (*Handler).applyDomainNamePatchOp,
	opUpdateResource:            (*Handler).applyResourceEntityPatchOp,
	opUpdateMethod:              (*Handler).applyMethodPatchOp,
	opUpdateIntegration:         (*Handler).applyIntegrationPatchOp,
	opUpdateIntegrationResponse: (*Handler).applyIntegrationResponsePatchOp,
	opUpdateMethodResponse:      (*Handler).applyMethodResponsePatchOp,
	opUpdateBasePathMapping:     (*Handler).applyBasePathMappingPatchOp,
	opUpdateAuthorizer:          (*Handler).applyAuthorizerPatchOp,
}

// applyResourcePatchOp dispatches to the per-action patch resolver for
// actions with map/struct/list/rejectable PATCH targets that need current
// backend state (or an explicit AWS-documented rejection) that a flat
// single-field replace cannot express. See resourcePatchResolver's doc
// comment for the return-value contract.
func (h *Handler) applyResourcePatchOp(
	action string, pathParams map[string]string, op patchOp, out map[string]json.RawMessage,
) (bool, error) {
	segs := patchPathSegments(op.Path)
	if len(segs) == 0 {
		return false, nil
	}

	resolver, ok := resourcePatchResolvers[action]
	if !ok {
		return false, nil
	}

	return resolver(h, pathParams, op, segs, out)
}

// applyStagePatchOp handles UpdateStage PATCH ops that a flat top-level
// replace cannot express: canary-deployment promotion (a "copy" op), single
// stage-variable edits, single canarySettings/accessLogSettings sub-fields,
// and per-route method settings.
func (h *Handler) applyStagePatchOp(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) (bool, error) {
	if op.Op == patchOpCopy && op.Path == "/deploymentId" && op.From == "/canarySettings/deploymentId" {
		stage, err := h.Backend.GetStage(pathParams[keyRestAPIID], pathParams[keyStageName])
		if patchIgnorableErr(err) || stage.CanarySettings == nil {
			return true, nil
		}

		setJSONValue(out, "deploymentId", stage.CanarySettings.DeploymentID)

		return true, nil
	}

	if len(segs) >= 3 && (segs[0] == "*" || strings.HasPrefix(segs[0], "~1")) {
		return h.applyStageMethodSettingPatch(pathParams, op, segs, out), nil
	}

	if len(segs) != patchPathSegs2 {
		return false, nil
	}

	switch segs[0] {
	case "variables":
		return h.applyStageVariablePatch(pathParams, op, segs[1], out), nil
	case "canarySettings":
		return h.applyStageCanaryPatch(pathParams, op, segs[1], out), nil
	case "accessLogSettings":
		return h.applyStageAccessLogPatch(pathParams, op, segs[1], out), nil
	default:
		return false, nil
	}
}

// applyStageVariablePatch adds/replaces/removes a single stage variable
// ("/variables/{name}"), merging with the stage's existing variables (a
// wholesale replace would otherwise silently drop every other variable), and
// with whatever an earlier op in the SAME request already staged into
// out["variables"] (see stagedValue's doc comment).
func (h *Handler) applyStageVariablePatch(
	pathParams map[string]string, op patchOp, rawName string, out map[string]json.RawMessage,
) bool {
	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return false
	}

	vars, ok := stagedValue[map[string]string](out, "variables")
	if !ok {
		stage, err := h.Backend.GetStage(pathParams[keyRestAPIID], pathParams[keyStageName])
		if err != nil {
			return true
		}

		vars = stage.Variables
	}

	vars = cloneStringMap(vars)
	name := jsonPointerUnescape(rawName)

	if op.Op == patchOpRemove {
		delete(vars, name)
	} else if v, valOK := patchValueString(op.Value); valOK {
		vars[name] = v
	}

	setJSONValue(out, "variables", vars)

	return true
}

// applyStageCanaryPatch replaces a single CanarySettings sub-field
// ("/canarySettings/{deploymentId,percentTraffic,useStageCache}"), merging
// with the stage's existing canary settings and whatever an earlier op in the
// SAME request already staged into out["canarySettings"].
func (h *Handler) applyStageCanaryPatch(
	pathParams map[string]string, op patchOp, prop string, out map[string]json.RawMessage,
) bool {
	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return false
	}

	cur, ok := stagedValue[CanarySettings](out, "canarySettings")
	if !ok {
		stage, err := h.Backend.GetStage(pathParams[keyRestAPIID], pathParams[keyStageName])
		if err != nil {
			return true
		}

		if stage.CanarySettings != nil {
			cur = *stage.CanarySettings
		}
	}

	val, _ := patchValueString(op.Value)
	remove := op.Op == patchOpRemove

	if !applyStageCanaryProp(&cur, prop, val, remove) {
		return false
	}

	setJSONValue(out, "canarySettings", &cur)

	return true
}

// applyStageCanaryProp merges one CanarySettings sub-field (see
// applyStageCanaryPatch's doc comment for the four supported property names)
// into cur in place. Returns false when prop isn't a recognized canary
// property, matching applyResourcePatchOp's "unhandled -> fall through"
// contract.
func applyStageCanaryProp(cur *CanarySettings, prop, val string, remove bool) bool {
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
	case "stageVariableOverrides":
		// AWS documents only op:replace as supported for this path
		// (patch-operations.html), with a whole-map replacement rather than a
		// per-key path (unlike "/variables", which has a documented "/variables/*"
		// per-key wildcard row alongside its whole-map row -- no such wildcard
		// row exists for stageVariableOverrides). Value is, per the standard
		// PatchOperation wire shape, a JSON string -- here that string's
		// contents are themselves a JSON-encoded {name: value} object.
		if !remove {
			var overrides map[string]string
			if json.Unmarshal([]byte(val), &overrides) == nil {
				cur.StageVariableOverrides = overrides
			}
		}
	default:
		return false
	}

	return true
}

// applyStageAccessLogPatch replaces a single AccessLogSettings sub-field
// ("/accessLogSettings/{destinationArn,format}"), merging with the stage's
// existing access log settings and whatever an earlier op in the SAME
// request already staged into out["accessLogSettings"].
func (h *Handler) applyStageAccessLogPatch(
	pathParams map[string]string, op patchOp, prop string, out map[string]json.RawMessage,
) bool {
	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return false
	}

	cur, ok := stagedValue[AccessLogSettings](out, "accessLogSettings")
	if !ok {
		stage, err := h.Backend.GetStage(pathParams[keyRestAPIID], pathParams[keyStageName])
		if err != nil {
			return true
		}

		if stage.AccessLogSettings != nil {
			cur = *stage.AccessLogSettings
		}
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
	"caching/dataEncrypted": func(ms *MethodSetting, v string, remove bool) {
		ms.CacheDataEncrypted = !remove && parseBoolLenient(v)
	},
	"caching/unauthorizedCacheControlHeaderStrategy": func(ms *MethodSetting, v string, remove bool) {
		if remove {
			ms.UnauthorizedCacheControlHeaderStrategy = ""

			return
		}

		ms.UnauthorizedCacheControlHeaderStrategy = v
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

	settings, ok := stagedValue[map[string]MethodSetting](out, "methodSettings")
	if !ok {
		stage, err := h.Backend.GetStage(pathParams[keyRestAPIID], pathParams[keyStageName])
		if err != nil {
			return true
		}

		settings = make(map[string]MethodSetting, len(stage.MethodSettings)+1)
		maps.Copy(settings, stage.MethodSettings)
	}

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
// silently drop every other configured type) and whatever an earlier op in
// the SAME request already staged into out["binaryMediaTypes"].
func (h *Handler) applyRestAPIPatchOp(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) (bool, error) {
	if len(segs) != patchPathSegs2 || segs[0] != "binaryMediaTypes" {
		return false, nil
	}

	if op.Op != patchOpAdd && op.Op != patchOpRemove {
		return false, nil
	}

	types, ok := stagedValue[[]string](out, "binaryMediaTypes")
	if !ok {
		api, err := h.Backend.GetRestAPI(pathParams[keyRestAPIID])
		if patchIgnorableErr(err) {
			return true, nil
		}

		types = api.BinaryMediaTypes
	}

	mediaType := jsonPointerUnescape(segs[1])
	types = slices.Clone(types)

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

	return true, nil
}

// accountFeatureUsagePlans is the one feature patch-operations.html documents
// as blocked from removal (UpdateAccount "/features" row: "op:remove
// Supported, but not for the UsagePlans feature").
const accountFeatureUsagePlans = "UsagePlans"

// applyAccountPatchOp handles UpdateAccount's nested ThrottleSettings edits
// ("/throttle/{rateLimit,burstLimit}") and "/features" add/remove list
// membership. "/cloudwatchRoleArn" is a plain top-level string field and is
// handled by the generic fallback (applyTopLevelPatchOp).
func (h *Handler) applyAccountPatchOp(
	_ map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) (bool, error) {
	if len(segs) == 1 && segs[0] == "features" {
		return h.applyAccountFeaturesPatch(op, out)
	}

	if len(segs) != patchPathSegs2 || segs[0] != "throttle" {
		return false, nil
	}

	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return false, nil
	}

	cur, ok := stagedValue[ThrottleSettings](out, "throttleSettings")
	if !ok {
		acct, err := h.Backend.GetAccount()
		if patchIgnorableErr(err) {
			return true, nil
		}

		if acct.ThrottleSettings != nil {
			cur = *acct.ThrottleSettings
		}
	}

	val, _ := patchValueString(op.Value)

	if !applyAccountThrottleProp(&cur, segs[1], val, op.Op == patchOpRemove) {
		return false, nil
	}

	setJSONValue(out, "throttleSettings", &cur)

	return true, nil
}

// applyAccountThrottleProp merges one ThrottleSettings sub-field
// ("rateLimit" or "burstLimit") into cur in place. Returns false when prop
// isn't recognized, matching applyResourcePatchOp's "unhandled -> fall
// through" contract.
func applyAccountThrottleProp(cur *ThrottleSettings, prop, val string, remove bool) bool {
	switch prop {
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

	return true
}

// applyAccountFeaturesPatch adds/removes one entry of the account's Features
// list, merging with the account's existing features (a wholesale replace
// would otherwise silently drop every other feature) and whatever an earlier
// op in the SAME request already staged into out["features"]. Rejects
// removing accountFeatureUsagePlans, matching patch-operations.html's
// documented exception, and any op other than add/remove (replace is "Not
// supported").
func (h *Handler) applyAccountFeaturesPatch(op patchOp, out map[string]json.RawMessage) (bool, error) {
	if op.Op != patchOpAdd && op.Op != patchOpRemove {
		return true, unsupportedPatchOp("/features", op.Op)
	}

	val, ok := patchValueString(op.Value)
	if !ok {
		return true, nil
	}

	if op.Op == patchOpRemove && val == accountFeatureUsagePlans {
		return true, unsupportedPatchOp("/features", op.Op)
	}

	features, ok := stagedValue[[]string](out, "features")
	if !ok {
		acct, err := h.Backend.GetAccount()
		if patchIgnorableErr(err) {
			return true, nil
		}

		features = acct.Features
	}

	features = slices.Clone(features)

	if op.Op == patchOpAdd {
		if !slices.Contains(features, val) {
			features = append(features, val)
		}
	} else {
		features = slices.DeleteFunc(features, func(f string) bool { return f == val })
	}

	if features == nil {
		features = []string{}
	}

	setJSONValue(out, "features", features)

	return true, nil
}

// currentUsagePlanAPIStages returns the APIStages already staged in
// out["apiStages"] (from an earlier op in this same PATCH request, see
// stagedValue), falling back to the plan's current backend state when no
// earlier op in this request has touched apiStages yet. ok is false only
// when the plan itself can't be resolved.
func (h *Handler) currentUsagePlanAPIStages(
	pathParams map[string]string, out map[string]json.RawMessage,
) ([]APIStageAssociation, bool) {
	if staged, ok := stagedValue[[]APIStageAssociation](out, "apiStages"); ok {
		return staged, true
	}

	plan, err := h.Backend.GetUsagePlan(pathParams[keyUsagePlanID])
	if err != nil {
		return nil, false
	}

	return plan.APIStages, true
}

// applyUsagePlanPatchOp handles UpdateUsagePlan's API-stage membership edits
// ("/apiStages", add/remove, Value "{restApiId}:{stage}") and per-route
// throttle overrides within one API stage
// ("/apiStages/{restApiId}:{stage}/throttle/...", see
// applyUsagePlanThrottlePatch) — see the AWS "UpdateUsagePlan" patch-operations
// reference.
func (h *Handler) applyUsagePlanPatchOp(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) (bool, error) {
	if len(segs) == 1 && segs[0] == "apiStages" {
		return h.applyUsagePlanAPIStageMembershipPatch(pathParams, op, out), nil
	}

	if len(segs) >= patchPathSegs2+1 && segs[0] == "apiStages" && segs[2] == "throttle" {
		return h.applyUsagePlanThrottlePatch(pathParams, op, segs, out), nil
	}

	return false, nil
}

// applyUsagePlanAPIStageMembershipPatch handles the whole-apiStage add/remove
// case ("/apiStages", Value "{restApiId}:{stage}").
func (h *Handler) applyUsagePlanAPIStageMembershipPatch(
	pathParams map[string]string, op patchOp, out map[string]json.RawMessage,
) bool {
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

	stages, ok := h.currentUsagePlanAPIStages(pathParams, out)
	if !ok {
		return true
	}

	matches := func(a APIStageAssociation) bool { return a.RestAPIID == restAPIID && a.Stage == stage }
	stages = slices.Clone(stages)

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

// usagePlanThrottlePathMinSegs is the segment count of
// "/apiStages/{id:stage}/throttle/{resourcePath}/{httpMethod}" (the shortest
// per-route throttle path, supporting only "remove" of the whole entry).
// Adding "/rateLimit" or "/burstLimit" (usagePlanThrottlePathMinSegs+1)
// supports add/replace of that one field.
const usagePlanThrottlePathMinSegs = 5

// applyUsagePlanThrottlePatch handles per-route throttle overrides within one
// usage-plan API stage:
// "/apiStages/{restApiId}:{stage}/throttle/{resourcePath}/{httpMethod}"
// (remove the whole ThrottleSettings entry) or with a trailing
// "/rateLimit"/"/burstLimit" segment (add/replace that one field) — see the
// AWS "UpdateUsagePlan" patch-operations reference. resourcePath follows the
// same JSON-Pointer-escaped convention as Stage's per-route method settings
// (applyStageMethodSettingPatch); the merged map key matches the
// "{httpMethod} {resourcePath}" convention already used by
// APIStageAssociation.Throttle elsewhere in this service (see usage.go's
// effectiveThrottle and CreateUsagePlan's test fixtures).
func (h *Handler) applyUsagePlanThrottlePatch(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) bool {
	if len(segs) < usagePlanThrottlePathMinSegs || len(segs) > usagePlanThrottlePathMinSegs+1 {
		return false
	}

	restAPIID, stageName, cut := strings.Cut(segs[1], ":")
	if !cut {
		return true
	}

	current, ok := h.currentUsagePlanAPIStages(pathParams, out)
	if !ok {
		return true
	}

	stages := slices.Clone(current)
	idx := slices.IndexFunc(stages, func(a APIStageAssociation) bool {
		return a.RestAPIID == restAPIID && a.Stage == stageName
	})
	if idx < 0 {
		return true
	}

	routeKey := segs[4] + " " + jsonPointerUnescape(segs[3])
	throttle := make(map[string]*ThrottleSettings, len(stages[idx].Throttle)+1)
	maps.Copy(throttle, stages[idx].Throttle)

	if !h.mergeUsagePlanThrottleEntry(op, segs, routeKey, throttle) {
		return false
	}

	stages[idx].Throttle = throttle
	setJSONValue(out, "apiStages", stages)

	return true
}

// mergeUsagePlanThrottleEntry applies one add/replace/remove op to throttle's
// routeKey entry in place. Returns false when op.Op/segs don't match a
// supported combination (whole-entry remove at 5 segments, or a
// rateLimit/burstLimit add/replace at 6 segments), matching
// applyResourcePatchOp's "unhandled -> fall through" contract.
func (h *Handler) mergeUsagePlanThrottleEntry(
	op patchOp, segs []string, routeKey string, throttle map[string]*ThrottleSettings,
) bool {
	if len(segs) == usagePlanThrottlePathMinSegs {
		if op.Op != patchOpRemove {
			return false
		}

		delete(throttle, routeKey)

		return true
	}

	if op.Op != patchOpAdd && op.Op != patchOpReplace {
		return false
	}

	val, ok := patchValueString(op.Value)
	if !ok {
		return true
	}

	cur := ThrottleSettings{}
	if existing := throttle[routeKey]; existing != nil {
		cur = *existing
	}

	switch segs[5] {
	case "rateLimit":
		f, parseErr := strconv.ParseFloat(val, 64)
		if parseErr != nil {
			return true
		}

		cur.RateLimit = f
	case "burstLimit":
		n, parseErr := strconv.Atoi(val)
		if parseErr != nil {
			return true
		}

		cur.BurstLimit = n
	default:
		return false
	}

	throttle[routeKey] = &cur

	return true
}

// applyGatewayResponsePatchOp handles UpdateGatewayResponse's per-key
// responseParameters/responseTemplates edits
// ("/responseParameters/{key}", "/responseTemplates/{key}"), merging with the
// gateway response's existing entries (falling back to AWS's implicit
// default response when none has been customized yet) and whatever an
// earlier op in the SAME request already staged into out[segs[0]].
func (h *Handler) applyGatewayResponsePatchOp(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) (bool, error) {
	if len(segs) != patchPathSegs2 || (segs[0] != fieldResponseParameters && segs[0] != "responseTemplates") {
		return false, nil
	}

	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return false, nil
	}

	m, ok := stagedValue[map[string]string](out, segs[0])
	if !ok {
		gr, err := h.Backend.GetGatewayResponse(pathParams[keyRestAPIID], pathParams[keyResponseType])
		if patchIgnorableErr(err) {
			return true, nil
		}

		if segs[0] == fieldResponseParameters {
			m = gr.ResponseParameters
		} else {
			m = gr.ResponseTemplates
		}
	}

	key := jsonPointerUnescape(segs[1])
	m = cloneStringMap(m)

	if op.Op == patchOpRemove {
		delete(m, key)
	} else if v, valOK := patchValueString(op.Value); valOK {
		m[key] = v
	}

	setJSONValue(out, segs[0], m)

	return true, nil
}

// applyAPIKeyPatchOp handles UpdateApiKey's "/stages" add/remove (AWS
// "DEPRECATED FOR USAGE PLANS" but still real and wire-modeled -- see
// APIKey.StageKeys's doc comment), where Value is the string
// "{restApiId}/{stageName}" (types.ApiKey.StageKeys / GetApiKey's
// stageKeys response field format), merged with the key's existing
// StageKeys (a wholesale replace would otherwise silently drop every other
// associated stage).
func (h *Handler) applyAPIKeyPatchOp(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) (bool, error) {
	if len(segs) != 1 || segs[0] != "stages" {
		return false, nil
	}

	if op.Op != patchOpAdd && op.Op != patchOpRemove {
		return false, nil
	}

	val, ok := patchValueString(op.Value)
	if !ok {
		return true, nil
	}

	var stageKeys []string
	if staged, stagedOK := stagedValue[[]string](out, "stageKeys"); stagedOK {
		stageKeys = slices.Clone(staged)
	} else {
		key, err := h.Backend.GetAPIKey(pathParams[keyAPIKeyID])
		if patchIgnorableErr(err) {
			return true, nil
		}

		stageKeys = slices.Clone(key.StageKeys)
	}

	if op.Op == patchOpAdd {
		if !slices.Contains(stageKeys, val) {
			stageKeys = append(stageKeys, val)
		}
	} else {
		stageKeys = slices.DeleteFunc(stageKeys, func(s string) bool { return s == val })
	}

	if stageKeys == nil {
		stageKeys = []string{}
	}

	setJSONValue(out, "stageKeys", stageKeys)

	return true, nil
}

// applyDomainNamePatchOp handles UpdateDomainName's nested PATCH paths
// ("/endpointConfiguration/types", "/endpointConfiguration/ipAddressType",
// "/mutualTlsAuthentication/{truststoreUri,truststoreVersion}" — see
// patch-operations.html's UpdateDomainName table). Before this, applyResourcePatchOp
// had no case for opUpdateDomainName at all, so every nested DomainName PATCH
// path silently no-opped via applyTopLevelPatchOp's path-contains-"/" guard.
func (h *Handler) applyDomainNamePatchOp(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) (bool, error) {
	if len(segs) != patchPathSegs2 {
		return false, nil
	}

	switch segs[0] {
	case "endpointConfiguration":
		return h.applyDomainNameEndpointConfigPatch(pathParams, op, segs[1], out), nil
	case "mutualTlsAuthentication":
		return h.applyDomainNameMTLSPatch(pathParams, op, segs[1], out), nil
	default:
		return false, nil
	}
}

// applyDomainNameEndpointConfigPatch handles "/endpointConfiguration/types"
// (add/remove one endpoint type) and "/endpointConfiguration/ipAddressType"
// (replace), merging with the domain name's existing endpoint configuration
// and whatever an earlier op in the SAME request already staged into
// out["endpointConfiguration"].
func (h *Handler) applyDomainNameEndpointConfigPatch(
	pathParams map[string]string, op patchOp, prop string, out map[string]json.RawMessage,
) bool {
	cur, ok := stagedValue[EndpointConfiguration](out, "endpointConfiguration")
	if !ok {
		dn, err := h.Backend.GetDomainName(pathParams[keyDomainName])
		if err != nil {
			return true
		}

		if dn.EndpointConfiguration != nil {
			cur = *dn.EndpointConfiguration
		}
	}

	val, _ := patchValueString(op.Value)

	switch prop {
	case "types":
		if op.Op != patchOpAdd && op.Op != patchOpRemove {
			return false
		}

		types := slices.Clone(cur.Types)
		if op.Op == patchOpAdd {
			if !slices.Contains(types, val) {
				types = append(types, val)
			}
		} else {
			types = slices.DeleteFunc(types, func(t string) bool { return t == val })
		}

		cur.Types = types
	case "ipAddressType":
		if op.Op != patchOpReplace {
			return false
		}

		cur.IPAddressType = val
	default:
		return false
	}

	setJSONValue(out, "endpointConfiguration", &cur)

	return true
}

// applyDomainNameMTLSPatch handles a single MutualTLSAuthentication sub-field
// ("/mutualTlsAuthentication/{truststoreUri,truststoreVersion}"), merging
// with the domain name's existing mTLS configuration and whatever an earlier
// op in the SAME request already staged into out["mutualTlsAuthentication"].
func (h *Handler) applyDomainNameMTLSPatch(
	pathParams map[string]string, op patchOp, prop string, out map[string]json.RawMessage,
) bool {
	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return false
	}

	cur, ok := stagedValue[MutualTLSAuthentication](out, "mutualTlsAuthentication")
	if !ok {
		dn, err := h.Backend.GetDomainName(pathParams[keyDomainName])
		if err != nil {
			return true
		}

		if dn.MutualTLSAuthentication != nil {
			cur = *dn.MutualTLSAuthentication
		}
	}

	val, _ := patchValueString(op.Value)
	if op.Op == patchOpRemove {
		val = ""
	}

	switch prop {
	case "truststoreUri":
		cur.TruststoreURI = val
	case "truststoreVersion":
		cur.TruststoreVersion = val
	default:
		return false
	}

	setJSONValue(out, "mutualTlsAuthentication", &cur)

	return true
}

// applyResourceEntityPatchOp handles UpdateResource's "/parentId" (move a
// resource to a new parent, recomputing its subtree's Path — see
// InMemoryBackend.UpdateResource). "/pathPart" is a plain top-level string
// field and is left to the generic fallback (applyTopLevelPatchOp).
// patch-operations.html's UpdateResource table documents both paths as
// replace-only.
func (h *Handler) applyResourceEntityPatchOp(
	_ map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) (bool, error) {
	if len(segs) != 1 || segs[0] != "parentId" {
		return false, nil
	}

	if op.Op != patchOpReplace {
		return true, unsupportedPatchOp("/parentId", op.Op)
	}

	out["parentId"] = op.Value

	return true, nil
}

// applyBasePathMappingPatchOp resolves UpdateBasePathMapping's PATCH paths.
// AWS's own docs disagree on how "/basepath" is cased: patch-operations.html
// spells it lowercase ("/basepath", also "/restapiId" — note the capital I
// even there), while the AWS CLI reference's own worked example (docs.aws.
// amazon.com/cli/latest/reference/apigateway/update-base-path-mapping.html,
// also mirrored in AWS's Doc SDK Examples code-library page) uses
// path='/basePath' and returns a populated "basePath" in its shown output —
// both spellings are accepted here rather than picking one.
//
// Renaming the base path itself needs a dedicated resolver, not just a case
// alias: UpdateBasePathMappingInput's "basePath" field does DOUBLE DUTY as
// both the REQUIRED identity used to look up which mapping to update (from
// the URL path segment) and, naively, the field a "/basepath" patch would
// target — but pathParams is merged into the body via injectJSONFieldAPIGW
// AFTER applyStructuredPatch runs (handler.go) and unconditionally overwrites
// "basePath" with the URL's OLD value, clobbering anything staged here
// regardless of casing. So both spellings stage the new value under
// "newBasePath" instead, a field private to this backend's
// UpdateBasePathMappingInput with no equivalent on the real AWS wire (every
// real client only ever sends the rename through patchOperations), which
// InMemoryBackend.UpdateBasePathMapping applies as an actual key rename.
//
// "/restapiId" is aliased explicitly to RestAPIID's "restApiId" json tag
// rather than relied on via json.Unmarshal's incidental case-insensitive
// field match (which happens to also save it, since RestAPIID isn't
// pathParams-clobbered — but relying on that would be an accident, not a
// decision). "/restApiId" and "/stage" are spelled identically in every AWS
// source and already work via the generic fallback (applyTopLevelPatchOp).
func (h *Handler) applyBasePathMappingPatchOp(
	_ map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) (bool, error) {
	if len(segs) != 1 {
		return false, nil
	}

	if op.Op != patchOpReplace && op.Op != patchOpAdd {
		return false, nil
	}

	switch segs[0] {
	case "basepath", "basePath":
		out["newBasePath"] = coerceTopLevelPatchValue("newBasePath", op.Value)
	case "restapiId":
		out["restApiId"] = coerceTopLevelPatchValue("restApiId", op.Value)
	default:
		return false, nil
	}

	return true, nil
}

// applyAuthorizerPatchOp handles UpdateAuthorizer's "/providerARNs" add/remove
// list-membership edits (patch-operations.html: UpdateAuthorizer table
// documents add/remove as supported, replace as not supported), merging with
// the authorizer's existing ProviderARNs (a wholesale replace would otherwise
// silently drop every other ARN) and whatever an earlier op in the SAME
// request already staged into out["providerARNs"]. Before this, the path fell
// through to applyTopLevelPatchOp, which wrote the raw Value JSON string
// straight into a field UpdateAuthorizerInput types as []string —
// json.Unmarshal then failed the WHOLE PATCH request with a 500 (same bug
// class as UpdateIntegration's /cacheKeyParameters).
func (h *Handler) applyAuthorizerPatchOp(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) (bool, error) {
	if len(segs) != 1 || segs[0] != "providerARNs" {
		return false, nil
	}

	if op.Op != patchOpAdd && op.Op != patchOpRemove {
		return true, unsupportedPatchOp("/providerARNs", op.Op)
	}

	val, ok := patchValueString(op.Value)
	if !ok {
		return true, nil
	}

	arns, ok := stagedValue[[]string](out, "providerARNs")
	if !ok {
		auth, err := h.Backend.GetAuthorizer(pathParams[keyRestAPIID], pathParams[keyAuthorizerID])
		if patchIgnorableErr(err) {
			return true, nil
		}

		arns = auth.ProviderARNs
	}

	arns = slices.Clone(arns)

	if op.Op == patchOpAdd {
		if !slices.Contains(arns, val) {
			arns = append(arns, val)
		}
	} else {
		arns = slices.DeleteFunc(arns, func(a string) bool { return a == val })
	}

	if arns == nil {
		arns = []string{}
	}

	setJSONValue(out, "providerARNs", arns)

	return true, nil
}

// applyMethodPatchOp handles UpdateMethod's per-key "/requestParameters/{name}"
// and "/requestModels/{content-type}" map edits. "/authorizationScopes" is
// AWS-documented (patch-operations.html) as add/remove-supported but Method
// has no AuthorizationScopes field in this backend, so it is rejected rather
// than silently accepted (see package doc). The remaining top-level scalars
// (authorizationType, authorizerId, apiKeyRequired, operationName,
// requestValidatorId) are plain replace-only fields left to the generic
// fallback.
func (h *Handler) applyMethodPatchOp(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) (bool, error) {
	if len(segs) == 1 && segs[0] == "authorizationScopes" {
		return true, unmodeledPatchPath(opUpdateMethod, op.Path)
	}

	if len(segs) != patchPathSegs2 {
		return false, nil
	}

	switch segs[0] {
	case fieldRequestParameters:
		return h.applyMethodRequestParameterPatch(pathParams, op, segs[1], out)
	case "requestModels":
		return h.applyMethodRequestModelPatch(pathParams, op, segs[1], out)
	default:
		return false, nil
	}
}

// applyMethodRequestParameterPatch adds/replaces/removes a single method
// request-parameter flag ("/requestParameters/{name}"), merging with the
// method's existing RequestParameters (a wholesale replace would otherwise
// silently drop every other parameter) and whatever an earlier op in the SAME
// request already staged into out["requestParameters"].
func (h *Handler) applyMethodRequestParameterPatch(
	pathParams map[string]string, op patchOp, rawName string, out map[string]json.RawMessage,
) (bool, error) {
	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return true, unsupportedPatchOp("/requestParameters", op.Op)
	}

	params, ok := stagedValue[map[string]bool](out, fieldRequestParameters)
	if !ok {
		m, err := h.Backend.GetMethod(pathParams[keyRestAPIID], pathParams[keyResourceID], pathParams[keyHTTPMethod])
		if patchIgnorableErr(err) {
			return true, nil
		}

		params = m.RequestParameters
	}

	params = cloneBoolMap(params)
	name := jsonPointerUnescape(rawName)

	if op.Op == patchOpRemove {
		delete(params, name)
	} else {
		val, _ := patchValueString(op.Value)
		params[name] = parseBoolLenient(val)
	}

	setJSONValue(out, fieldRequestParameters, params)

	return true, nil
}

// applyMethodRequestModelPatch adds/replaces/removes a single method request
// model mapping ("/requestModels/{content-type}"), merging with the method's
// existing RequestModels and whatever an earlier op in the SAME request
// already staged into out["requestModels"].
func (h *Handler) applyMethodRequestModelPatch(
	pathParams map[string]string, op patchOp, rawContentType string, out map[string]json.RawMessage,
) (bool, error) {
	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return true, unsupportedPatchOp("/requestModels", op.Op)
	}

	models, ok := stagedValue[map[string]string](out, "requestModels")
	if !ok {
		m, err := h.Backend.GetMethod(pathParams[keyRestAPIID], pathParams[keyResourceID], pathParams[keyHTTPMethod])
		if patchIgnorableErr(err) {
			return true, nil
		}

		models = m.RequestModels
	}

	models = cloneStringMap(models)
	contentType := jsonPointerUnescape(rawContentType)

	if op.Op == patchOpRemove {
		delete(models, contentType)
	} else if v, valOK := patchValueString(op.Value); valOK {
		models[contentType] = v
	}

	setJSONValue(out, "requestModels", models)

	return true, nil
}

// applyIntegrationPatchOp handles UpdateIntegration's PATCH paths that a flat
// top-level replace cannot express: "/cacheKeyParameters" (list membership),
// the per-key "/requestParameters/{name}" and "/requestTemplates/{content-type}"
// map edits, and three paths patch-operations.html documents that this
// backend does not model: "/type" (documented "Not supported" for every op),
// "/integrationTarget", "/responseTransferMode", and
// "/tlsConfig/insecureSkipVerification" (rejected rather than silently
// accepted — see package doc). The remaining top-level scalars (cacheNamespace,
// connectionId, connectionType, contentHandling, httpMethod, passthroughBehavior,
// uri, timeoutInMillis) are plain replace-only fields left to the generic fallback.
func (h *Handler) applyIntegrationPatchOp(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) (bool, error) {
	if len(segs) == 1 {
		return h.applyIntegrationTopLevelPatchOp(pathParams, op, segs[0], out)
	}

	if len(segs) != patchPathSegs2 {
		return false, nil
	}

	switch segs[0] {
	case fieldRequestParameters:
		return h.applyIntegrationMapPatch(pathParams, op, fieldRequestParameters, segs[1], out)
	case "requestTemplates":
		return h.applyIntegrationMapPatch(pathParams, op, "requestTemplates", segs[1], out)
	case "tlsConfig":
		return true, unmodeledPatchPath(opUpdateIntegration, op.Path)
	default:
		return false, nil
	}
}

// applyIntegrationTopLevelPatchOp handles UpdateIntegration's single-segment
// PATCH paths that need explicit rejection or list-membership merging rather
// than the generic scalar fallback.
func (h *Handler) applyIntegrationTopLevelPatchOp(
	pathParams map[string]string, op patchOp, field string, out map[string]json.RawMessage,
) (bool, error) {
	switch field {
	case "type":
		return true, unsupportedPatchOp("/type", op.Op)
	case "integrationTarget", "responseTransferMode":
		return true, unmodeledPatchPath(opUpdateIntegration, op.Path)
	case "cacheKeyParameters":
		return h.applyIntegrationCacheKeyParametersPatch(pathParams, op, out)
	default:
		return false, nil
	}
}

// applyIntegrationCacheKeyParametersPatch adds/removes one cache-key
// parameter ("/cacheKeyParameters", Value the parameter string), merging with
// the integration's existing CacheKeyParameters. patch-operations.html lists
// op:replace as supported alongside add/remove; since PatchOperation.Value is a
// single string (not an array) there is no documented way for one "replace"
// op to set the whole list at once, so it is treated as an idempotent add,
// matching this file's other single-segment list-membership paths
// (UpdateApiKey's "/stages", UpdateRestApi's "/binaryMediaTypes").
func (h *Handler) applyIntegrationCacheKeyParametersPatch(
	pathParams map[string]string, op patchOp, out map[string]json.RawMessage,
) (bool, error) {
	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return true, unsupportedPatchOp("/cacheKeyParameters", op.Op)
	}

	val, ok := patchValueString(op.Value)
	if !ok {
		return true, nil
	}

	params, ok := stagedValue[[]string](out, "cacheKeyParameters")
	if !ok {
		integ, err := h.Backend.GetIntegration(
			pathParams[keyRestAPIID], pathParams[keyResourceID], pathParams[keyHTTPMethod],
		)
		if patchIgnorableErr(err) {
			return true, nil
		}

		params = integ.CacheKeyParameters
	}

	params = slices.Clone(params)

	if op.Op == patchOpRemove {
		params = slices.DeleteFunc(params, func(p string) bool { return p == val })
	} else if !slices.Contains(params, val) {
		params = append(params, val)
	}

	if params == nil {
		params = []string{}
	}

	setJSONValue(out, "cacheKeyParameters", params)

	return true, nil
}

// applyIntegrationMapPatch adds/replaces/removes a single entry of
// UpdateIntegration's "requestParameters" or "requestTemplates" map, merging
// with the integration's existing map and whatever an earlier op in the SAME
// request already staged into out[field].
func (h *Handler) applyIntegrationMapPatch(
	pathParams map[string]string, op patchOp, field, rawKey string, out map[string]json.RawMessage,
) (bool, error) {
	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return true, unsupportedPatchOp("/"+field, op.Op)
	}

	m, ok := stagedValue[map[string]string](out, field)
	if !ok {
		integ, err := h.Backend.GetIntegration(
			pathParams[keyRestAPIID], pathParams[keyResourceID], pathParams[keyHTTPMethod],
		)
		if patchIgnorableErr(err) {
			return true, nil
		}

		if field == fieldRequestParameters {
			m = integ.RequestParameters
		} else {
			m = integ.RequestTemplates
		}
	}

	m = cloneStringMap(m)
	key := jsonPointerUnescape(rawKey)

	if op.Op == patchOpRemove {
		delete(m, key)
	} else if v, valOK := patchValueString(op.Value); valOK {
		m[key] = v
	}

	setJSONValue(out, field, m)

	return true, nil
}

// applyIntegrationResponsePatchOp handles UpdateIntegrationResponse's per-key
// "/responseParameters/{name}" and "/responseTemplates/{content-type}" map
// edits. "/contentHandling" and "/selectionPattern" are plain top-level
// replace-only fields left to the generic fallback.
func (h *Handler) applyIntegrationResponsePatchOp(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) (bool, error) {
	if len(segs) != patchPathSegs2 {
		return false, nil
	}

	switch segs[0] {
	case fieldResponseParameters:
		return h.applyIntegrationResponseMapPatch(pathParams, op, fieldResponseParameters, segs[1], out)
	case "responseTemplates":
		return h.applyIntegrationResponseMapPatch(pathParams, op, "responseTemplates", segs[1], out)
	default:
		return false, nil
	}
}

// applyIntegrationResponseMapPatch adds/replaces/removes a single entry of
// UpdateIntegrationResponse's "responseParameters" or "responseTemplates" map,
// merging with the integration response's existing map and whatever an
// earlier op in the SAME request already staged into out[field].
func (h *Handler) applyIntegrationResponseMapPatch(
	pathParams map[string]string, op patchOp, field, rawKey string, out map[string]json.RawMessage,
) (bool, error) {
	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return true, unsupportedPatchOp("/"+field, op.Op)
	}

	m, ok := stagedValue[map[string]string](out, field)
	if !ok {
		ir, err := h.Backend.GetIntegrationResponse(
			pathParams[keyRestAPIID], pathParams[keyResourceID], pathParams[keyHTTPMethod], pathParams[keyStatusCode],
		)
		if patchIgnorableErr(err) {
			return true, nil
		}

		if field == fieldResponseParameters {
			m = ir.ResponseParameters
		} else {
			m = ir.ResponseTemplates
		}
	}

	m = cloneStringMap(m)
	key := jsonPointerUnescape(rawKey)

	if op.Op == patchOpRemove {
		delete(m, key)
	} else if v, valOK := patchValueString(op.Value); valOK {
		m[key] = v
	}

	setJSONValue(out, field, m)

	return true, nil
}

// applyMethodResponsePatchOp handles UpdateMethodResponse's per-key
// "/responseModels/{content-type}" and "/responseParameters/{name}" map edits.
func (h *Handler) applyMethodResponsePatchOp(
	pathParams map[string]string, op patchOp, segs []string, out map[string]json.RawMessage,
) (bool, error) {
	if len(segs) != patchPathSegs2 {
		return false, nil
	}

	switch segs[0] {
	case "responseModels":
		return h.applyMethodResponseModelPatch(pathParams, op, segs[1], out)
	case fieldResponseParameters:
		return h.applyMethodResponseParameterPatch(pathParams, op, segs[1], out)
	default:
		return false, nil
	}
}

// applyMethodResponseModelPatch adds/replaces/removes a single method
// response model mapping ("/responseModels/{content-type}"), merging with the
// method response's existing ResponseModels and whatever an earlier op in the
// SAME request already staged into out["responseModels"].
func (h *Handler) applyMethodResponseModelPatch(
	pathParams map[string]string, op patchOp, rawContentType string, out map[string]json.RawMessage,
) (bool, error) {
	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return true, unsupportedPatchOp("/responseModels", op.Op)
	}

	models, ok := stagedValue[map[string]string](out, "responseModels")
	if !ok {
		mr, err := h.Backend.GetMethodResponse(
			pathParams[keyRestAPIID], pathParams[keyResourceID], pathParams[keyHTTPMethod], pathParams[keyStatusCode],
		)
		if patchIgnorableErr(err) {
			return true, nil
		}

		models = mr.ResponseModels
	}

	models = cloneStringMap(models)
	contentType := jsonPointerUnescape(rawContentType)

	if op.Op == patchOpRemove {
		delete(models, contentType)
	} else if v, valOK := patchValueString(op.Value); valOK {
		models[contentType] = v
	}

	setJSONValue(out, "responseModels", models)

	return true, nil
}

// applyMethodResponseParameterPatch adds/replaces/removes a single method
// response-parameter flag ("/responseParameters/{name}"), merging with the
// method response's existing ResponseParameters and whatever an earlier op in
// the SAME request already staged into out["responseParameters"].
func (h *Handler) applyMethodResponseParameterPatch(
	pathParams map[string]string, op patchOp, rawName string, out map[string]json.RawMessage,
) (bool, error) {
	if op.Op != patchOpAdd && op.Op != patchOpReplace && op.Op != patchOpRemove {
		return true, unsupportedPatchOp("/responseParameters", op.Op)
	}

	params, ok := stagedValue[map[string]bool](out, fieldResponseParameters)
	if !ok {
		mr, err := h.Backend.GetMethodResponse(
			pathParams[keyRestAPIID], pathParams[keyResourceID], pathParams[keyHTTPMethod], pathParams[keyStatusCode],
		)
		if patchIgnorableErr(err) {
			return true, nil
		}

		params = mr.ResponseParameters
	}

	params = cloneBoolMap(params)
	name := jsonPointerUnescape(rawName)

	if op.Op == patchOpRemove {
		delete(params, name)
	} else {
		val, _ := patchValueString(op.Value)
		params[name] = parseBoolLenient(val)
	}

	setJSONValue(out, fieldResponseParameters, params)

	return true, nil
}
