package opensearch

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// openSearchServerlessTargetPrefix is the real opensearchserverless@v1.34.4
// JSON-RPC 1.0 X-Amz-Target prefix (serializers.go: every op's
// awsAwsjson10_serializeOp<Op>.HandleSerialize calls
// httpBindingEncoder.SetHeader("X-Amz-Target").String("OpenSearchServerless.<Op>")
// and POSTs to "/" -- zero URI template, unlike this package's own
// REST-JSON classic-OpenSearch protocol under openSearchPathPrefixes). See
// gopherstack-92ft: before this file, no real client of any of the 19 AOSS
// ops could reach this Handler at all.
const openSearchServerlessTargetPrefix = "OpenSearchServerless."

// openSearchServerlessJSONContentType is AOSS's real wire content type
// (opensearchserverless@v1.34.4 serializers.go's protocol.go sets
// "application/x-amz-json-1.0" for every request/response).
const openSearchServerlessJSONContentType = "application/x-amz-json-1.0"

// jsonKeyPolicyTypeJR is the "type" discriminator field shared by every
// AccessPolicy/SecurityPolicy/SecurityConfig JSON-RPC request and response.
const jsonKeyPolicyTypeJR = "type"

// serverlessJSONRPCOpFunc is a real-transport handler for one AOSS op. It
// receives the JSON-RPC request body already decoded to a generic map --
// unlike the fabricated REST path, the resource identifier always travels
// in the body, never a URL segment -- and returns the response body to
// marshal under the real wire's top-level key(s).
type serverlessJSONRPCOpFunc func(map[string]any) (map[string]any, error)

// handleServerlessJSONRPC serves a real AOSS request (POST /, X-Amz-Target:
// OpenSearchServerless.<op>). It reuses the SAME backend calls
// handleServerlessRoutes (the fabricated REST path under
// openSearchServerlessPath) already uses for every op it can -- both paths
// are kept; see gopherstack-92ft for why the REST path is unreachable by
// any real client and is not being removed here.
func (h *Handler) handleServerlessJSONRPC(c *echo.Context, op string) error {
	if c.Request().Method != http.MethodPost {
		return awserr.Write(c, awserr.ProtocolJSON10, awserr.APIError{
			Code:       "UnknownOperationException",
			Message:    "method not allowed",
			HTTPStatus: http.StatusMethodNotAllowed,
		})
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return awserr.Write(c, awserr.ProtocolJSON10, serverlessInternalError())
	}

	var input map[string]any
	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
			return awserr.Write(c, awserr.ProtocolJSON10, awserr.APIError{
				Code:       "ValidationException",
				Message:    "invalid JSON",
				HTTPStatus: http.StatusBadRequest,
			})
		}
	}
	if input == nil {
		input = map[string]any{}
	}

	fn, ok := h.serverlessJSONRPCOps()[op]
	if !ok {
		return awserr.Write(c, awserr.ProtocolJSON10, awserr.APIError{
			Code:       "UnknownOperationException",
			Message:    fmt.Sprintf("operation %q not implemented", op),
			HTTPStatus: http.StatusBadRequest,
		})
	}

	out, opErr := fn(input)
	if opErr != nil {
		apiErr := awserr.Classify(opErr, serverlessErrorTable(), serverlessInternalError())

		return awserr.Write(c, awserr.ProtocolJSON10, apiErr)
	}

	payload, marshalErr := json.Marshal(out)
	if marshalErr != nil {
		return awserr.Write(c, awserr.ProtocolJSON10, serverlessInternalError())
	}

	c.Response().Header().Set("Content-Type", openSearchServerlessJSONContentType)

	return c.JSONBlob(http.StatusOK, payload)
}

func serverlessInternalError() awserr.APIError {
	return awserr.APIError{
		Code:       "InternalServerException",
		Message:    "internal error",
		HTTPStatus: http.StatusInternalServerError,
	}
}

// serverlessErrorTable maps this package's serverless backend sentinels to
// their real AOSS exception codes (opensearchserverless@v1.34.4
// types/errors.go's exception set: ConflictException,
// InternalServerException, OcuLimitExceededException,
// ResourceNotFoundException, ServiceQuotaExceededException,
// ValidationException -- notably NOT "ResourceAlreadyExistsException",
// which is what ErrApplicationAlreadyExists's message string says and what
// the fabricated REST path (out of scope to fix here) still returns; the
// real already-exists exception is ConflictException).
func serverlessErrorTable() map[error]awserr.APIError {
	return map[error]awserr.APIError{
		ErrInvalidParameter:         {Code: "ValidationException", HTTPStatus: http.StatusBadRequest},
		ErrApplicationNotFound:      {Code: "ResourceNotFoundException", HTTPStatus: http.StatusNotFound},
		ErrApplicationAlreadyExists: {Code: "ConflictException", HTTPStatus: http.StatusConflict},
		// DeleteServerlessCollection (serverless.go) reuses the shared
		// domain-not-found sentinel; without this entry it fell through to
		// serverlessInternalError() (500 InternalServerException) instead of
		// the real AOSS 404 ResourceNotFoundException.
		ErrDomainNotFound: {Code: "ResourceNotFoundException", HTTPStatus: http.StatusNotFound},
	}
}

// serverlessJSONRPCOps returns the dispatch table for all 19 real AOSS ops
// this Handler advertises (serverlessOperations() in handler_operations.go).
func (h *Handler) serverlessJSONRPCOps() map[string]serverlessJSONRPCOpFunc {
	return map[string]serverlessJSONRPCOpFunc{
		"BatchGetCollection":   h.jrBatchGetCollection,
		"CreateAccessPolicy":   h.jrCreateAccessPolicy,
		"CreateCollection":     h.jrCreateCollection,
		"CreateSecurityConfig": h.jrCreateSecurityConfig,
		"CreateSecurityPolicy": h.jrCreateSecurityPolicy,
		"DeleteAccessPolicy":   h.jrDeleteAccessPolicy,
		"DeleteCollection":     h.jrDeleteCollection,
		"DeleteSecurityConfig": h.jrDeleteSecurityConfig,
		"DeleteSecurityPolicy": h.jrDeleteSecurityPolicy,
		"GetAccessPolicy":      h.jrGetAccessPolicy,
		"GetSecurityConfig":    h.jrGetSecurityConfig,
		"GetSecurityPolicy":    h.jrGetSecurityPolicy,
		"ListAccessPolicies":   h.jrListAccessPolicies,
		"ListCollections":      h.jrListCollections,
		"ListSecurityConfigs":  h.jrListSecurityConfigs,
		"ListSecurityPolicies": h.jrListSecurityPolicies,
		"UpdateAccessPolicy":   h.jrUpdateAccessPolicy,
		"UpdateSecurityConfig": h.jrUpdateSecurityConfig,
		"UpdateSecurityPolicy": h.jrUpdateSecurityPolicy,
	}
}

// --- Collections ---

func (h *Handler) jrBatchGetCollection(input map[string]any) (map[string]any, error) {
	ids := strSliceJR(input, "ids")
	names := strSliceJR(input, "names")

	colls := h.Backend.BatchGetServerlessCollections(ids, names)
	if colls == nil {
		colls = []*ServerlessCollection{}
	}

	return map[string]any{"collectionDetails": colls}, nil
}

func (h *Handler) jrCreateCollection(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	typ, _ := input[jsonKeyPolicyTypeJR].(string)
	desc, _ := input["description"].(string)
	tags := tagListToMapJR(input["tags"])

	var kmsKeyArn string
	if enc, ok := input["encryptionConfig"].(map[string]any); ok {
		kmsKeyArn, _ = enc["kmsKeyArn"].(string)
	}

	coll, err := h.Backend.CreateServerlessCollection(name, typ, desc, kmsKeyArn, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{"createCollectionDetail": coll}, nil
}

func (h *Handler) jrDeleteCollection(input map[string]any) (map[string]any, error) {
	id, _ := input["id"].(string)

	coll, err := h.Backend.DeleteServerlessCollection(id)
	if err != nil {
		return nil, err
	}

	return map[string]any{"deleteCollectionDetail": coll}, nil
}

func (h *Handler) jrListCollections(_ map[string]any) (map[string]any, error) {
	colls := h.Backend.BatchGetServerlessCollections(nil, nil)
	if colls == nil {
		colls = []*ServerlessCollection{}
	}

	return map[string]any{"collectionSummaries": colls}, nil
}

// --- Access policies ---

func (h *Handler) jrCreateAccessPolicy(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	typ, _ := input[jsonKeyPolicyTypeJR].(string)
	desc, _ := input["description"].(string)
	policy, _ := input["policy"].(string)

	result, err := h.accessPolicyCRUD().create(typ, name, desc, policy)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyAccessPolicyDetail: policyDetailJR(result)}, nil
}

func (h *Handler) jrGetAccessPolicy(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	typ, _ := input[jsonKeyPolicyTypeJR].(string)

	result, err := h.accessPolicyCRUD().get(typ, name)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyAccessPolicyDetail: policyDetailJR(result)}, nil
}

func (h *Handler) jrUpdateAccessPolicy(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	typ, _ := input[jsonKeyPolicyTypeJR].(string)
	desc, _ := input["description"].(string)
	policy, _ := input["policy"].(string)
	ver, _ := input["policyVersion"].(string)

	result, err := h.accessPolicyCRUD().update(typ, name, desc, policy, ver)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyAccessPolicyDetail: policyDetailJR(result)}, nil
}

func (h *Handler) jrDeleteAccessPolicy(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	typ, _ := input[jsonKeyPolicyTypeJR].(string)

	if err := h.accessPolicyCRUD().deleteByName(typ, name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) jrListAccessPolicies(input map[string]any) (map[string]any, error) {
	typ, _ := input[jsonKeyPolicyTypeJR].(string)

	return map[string]any{"accessPolicySummaries": h.accessPolicyCRUD().list(typ)}, nil
}

// --- Security policies ---
//
// The real AOSS API models a single SecurityPolicy resource discriminated
// by a required "type" field (encryption|network) -- there is no separate
// Encryption/Network operation family (see serverlessOperations' doc
// comment in handler_operations.go, which already corrects the reported op
// NAMES; this corrects the wire response KEYS too: the real
// CreateSecurityPolicy/GetSecurityPolicy/etc. all wrap their result under
// "securityPolicyDetail"/"securityPolicySummaries", never
// "encryptionPolicyDetail"/"networkPolicyDetail" -- verified against
// deserializers.go's awsAwsjson10_deserializeOpDocumentCreateSecurityPolicyOutput
// etc.). "type" is client-side required for every one of these ops
// (validators.go), so it is always present.
//
// GetServerlessNetworkPolicy/UpdateServerlessNetworkPolicy have no backend
// implementation -- only Create/List/Delete exist for network policies
// (services/opensearch/serverless.go), a pre-existing gap unrelated to
// transport (the fabricated REST path has the identical gap: no GET-by-name
// or PUT route under networksecuritypolicies/{name}). type=="network" Get
// and Update return ErrInvalidParameter rather than silently succeeding.
func (h *Handler) serverlessSecurityPolicyCRUD(policyType string) serverlessPolicyCRUD {
	if policyType == slPolicyTypeNetwork {
		return serverlessPolicyCRUD{
			create: func(pt, name, desc, policy string) (any, error) {
				return h.Backend.CreateServerlessNetworkPolicy(pt, name, desc, policy)
			},
			list: func(pt string) any {
				nps := h.Backend.ListServerlessNetworkPolicies(pt)
				if nps == nil {
					nps = []*ServerlessNetworkPolicy{}
				}

				return nps
			},
			get: func(_, name string) (any, error) {
				return nil, fmt.Errorf(
					"%w: network security policy %s: retrieval not supported", ErrInvalidParameter, name,
				)
			},
			update: func(_, name, _, _, _ string) (any, error) {
				return nil, fmt.Errorf(
					"%w: network security policy %s: update not supported", ErrInvalidParameter, name,
				)
			},
			deleteByName: h.Backend.DeleteServerlessNetworkPolicy,
		}
	}

	return h.encryptionPolicyCRUD()
}

func (h *Handler) jrCreateSecurityPolicy(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	typ, _ := input[jsonKeyPolicyTypeJR].(string)
	desc, _ := input["description"].(string)
	policy, _ := input["policy"].(string)

	result, err := h.serverlessSecurityPolicyCRUD(typ).create(typ, name, desc, policy)
	if err != nil {
		return nil, err
	}

	return map[string]any{keySecurityPolicyDetail: policyDetailJR(result)}, nil
}

func (h *Handler) jrGetSecurityPolicy(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	typ, _ := input[jsonKeyPolicyTypeJR].(string)

	result, err := h.serverlessSecurityPolicyCRUD(typ).get(typ, name)
	if err != nil {
		return nil, err
	}

	return map[string]any{keySecurityPolicyDetail: policyDetailJR(result)}, nil
}

func (h *Handler) jrUpdateSecurityPolicy(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	typ, _ := input[jsonKeyPolicyTypeJR].(string)
	desc, _ := input["description"].(string)
	policy, _ := input["policy"].(string)
	ver, _ := input["policyVersion"].(string)

	result, err := h.serverlessSecurityPolicyCRUD(typ).update(typ, name, desc, policy, ver)
	if err != nil {
		return nil, err
	}

	return map[string]any{keySecurityPolicyDetail: policyDetailJR(result)}, nil
}

func (h *Handler) jrDeleteSecurityPolicy(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	typ, _ := input[jsonKeyPolicyTypeJR].(string)

	if err := h.serverlessSecurityPolicyCRUD(typ).deleteByName(typ, name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) jrListSecurityPolicies(input map[string]any) (map[string]any, error) {
	typ, _ := input[jsonKeyPolicyTypeJR].(string)

	return map[string]any{"securityPolicySummaries": h.serverlessSecurityPolicyCRUD(typ).list(typ)}, nil
}

// --- Security configs ---

func (h *Handler) jrCreateSecurityConfig(input map[string]any) (map[string]any, error) {
	typ, _ := input[jsonKeyPolicyTypeJR].(string)
	desc, _ := input["description"].(string)
	saml := decodeSAMLOptionsJR(input["samlOptions"])

	sc, err := h.Backend.CreateServerlessSecurityConfig(typ, desc, saml)
	if err != nil {
		return nil, err
	}

	return map[string]any{keySecurityConfigDetail: sc}, nil
}

func (h *Handler) jrGetSecurityConfig(input map[string]any) (map[string]any, error) {
	id, _ := input["id"].(string)

	sc, err := h.Backend.GetServerlessSecurityConfig(id)
	if err != nil {
		return nil, err
	}

	return map[string]any{keySecurityConfigDetail: sc}, nil
}

func (h *Handler) jrUpdateSecurityConfig(input map[string]any) (map[string]any, error) {
	id, _ := input["id"].(string)
	desc, _ := input["description"].(string)
	ver, _ := input["configVersion"].(string)
	saml := decodeSAMLOptionsJR(input["samlOptions"])

	sc, err := h.Backend.UpdateServerlessSecurityConfig(id, desc, ver, saml)
	if err != nil {
		return nil, err
	}

	return map[string]any{keySecurityConfigDetail: sc}, nil
}

func (h *Handler) jrDeleteSecurityConfig(input map[string]any) (map[string]any, error) {
	id, _ := input["id"].(string)

	if err := h.Backend.DeleteServerlessSecurityConfig(id); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) jrListSecurityConfigs(input map[string]any) (map[string]any, error) {
	typ, _ := input[jsonKeyPolicyTypeJR].(string)

	scs := h.Backend.ListServerlessSecurityConfigs(typ)
	if scs == nil {
		scs = []*ServerlessSecurityConfig{}
	}

	return map[string]any{"securityConfigSummaries": scs}, nil
}

// --- Wire decode helpers ---
//
// strSliceJR/tagListToMapJR/decodeSAMLOptionsJR parse the REAL AOSS JSON-RPC
// body shapes (verified against opensearchserverless@v1.34.4 serializers.go):
// tags is a list of {"key","value"} objects, not the map the fabricated
// REST path's request structs assume -- these do NOT reuse those REST
// structs because that shape is wrong for the real wire.

func strSliceJR(input map[string]any, key string) []string {
	raw, ok := input[key].([]any)
	if !ok {
		return nil
	}

	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, isStr := v.(string); isStr {
			out = append(out, s)
		}
	}

	return out
}

func tagListToMapJR(raw any) map[string]string {
	list, ok := raw.([]any)
	if !ok {
		return nil
	}

	out := make(map[string]string, len(list))
	for _, item := range list {
		entry, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		k, _ := entry["key"].(string)
		v, _ := entry["value"].(string)
		if k != "" {
			out[k] = v
		}
	}

	return out
}

// policyLikeJR is the field set shared, name-for-name, by
// ServerlessAccessPolicy/ServerlessEncryptionPolicy/ServerlessNetworkPolicy.
type policyLikeJR struct {
	Description      string
	Name             string
	Policy           string
	PolicyVersion    string
	Type             string
	CreatedDate      float64
	LastModifiedDate float64
}

// policyDetailJR converts an access/security policy backend result (any of
// the three structurally-identical policy types) into the real AOSS
// AccessPolicyDetail/SecurityPolicyDetail wire shape. The real "policy"
// field is a smithy document -- an embedded JSON value, not a JSON string
// (verified against deserializers.go's
// awsAwsjson10_deserializeDocumentAccessPolicyDetail: its "policy" case
// calls awsAwsjson10_deserializeDocumentDocument on the ALREADY-DECODED
// value, i.e. the wire nests real JSON there, never a quoted string) -- so
// this wraps the stored policy text in json.RawMessage rather than letting
// it marshal as a Go string field would (which would double-encode it).
func policyDetailJR(v any) map[string]any {
	pl := toPolicyLikeJR(v)

	m := map[string]any{
		jsonKeyAppName:      pl.Name,
		jsonKeyPolicyTypeJR: pl.Type,
		"policyVersion":     pl.PolicyVersion,
		"createdDate":       pl.CreatedDate,
		"lastModifiedDate":  pl.LastModifiedDate,
	}
	if pl.Description != "" {
		m["description"] = pl.Description
	}
	if pl.Policy != "" {
		m["policy"] = json.RawMessage(pl.Policy)
	}

	return m
}

func toPolicyLikeJR(v any) policyLikeJR {
	switch p := v.(type) {
	case *ServerlessAccessPolicy:
		return policyLikeJR{
			p.Description, p.Name, p.Policy, p.PolicyVersion, p.Type, p.CreatedDate, p.LastModifiedDate,
		}
	case *ServerlessEncryptionPolicy:
		return policyLikeJR{
			p.Description, p.Name, p.Policy, p.PolicyVersion, p.Type, p.CreatedDate, p.LastModifiedDate,
		}
	case *ServerlessNetworkPolicy:
		return policyLikeJR{
			p.Description, p.Name, p.Policy, p.PolicyVersion, p.Type, p.CreatedDate, p.LastModifiedDate,
		}
	default:
		return policyLikeJR{}
	}
}

func decodeSAMLOptionsJR(raw any) *ServerlessSAMLOptions {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil
	}

	b, err := json.Marshal(m)
	if err != nil {
		return nil
	}

	var opts ServerlessSAMLOptions
	if unmarshalErr := json.Unmarshal(b, &opts); unmarshalErr != nil {
		return nil
	}

	return &opts
}
