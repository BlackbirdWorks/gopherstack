package s3

import (
	"bytes"
	"encoding/json"
)

// validateBucketPolicyDocument checks a PutBucketPolicy request body against
// the IAM/S3 resource-policy JSON grammar documented at
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_grammar.html:
//
//	policy     = { <version_block?>, <id_block?>, <statement_block> }
//	<version_block>   = "Version" : ("2008-10-17" | "2012-10-17")
//	<statement_block> = "Statement" : [ <statement>, ... ]   // a single object is also valid
//	<statement> = { <sid_block?>, <principal_block?>, <effect_block>,
//	                <action_block>, <resource_block>, <condition_block?> }
//	<effect_block>    = "Effect" : ("Allow" | "Deny")
//	<principal_block> = ("Principal" | "NotPrincipal") : ...
//	<action_block>    = ("Action" | "NotAction") : ...
//	<resource_block>  = ("Resource" | "NotResource") : ...
//
// Bucket policies are resource-based policies, so (unlike identity-based IAM
// policies) the principal_block is REQUIRED on every statement — real S3
// rejects a statement with no Principal/NotPrincipal with
// "MalformedPolicy: Missing required field Principal cannot be empty!"
// (a well-documented real-world PutBucketPolicy error; see e.g.
// https://github.com/cn-terraform/terraform-aws-logs-s3-bucket/issues/4).
//
// This performs shape validation only (presence/type of the required
// elements) — it does not resolve/validate ARN syntax inside Resource, IAM
// principal identifiers, or action-name namespaces, which real S3 checks via
// deeper policy-engine logic outside the scope of shape validation.
//
// Returns nil if the document is well-formed; otherwise a *ErrorResponse
// with Code "MalformedPolicy" and a message describing the first shape
// violation found (matching real S3's fail-fast-on-first-error behavior).
func validateBucketPolicyDocument(body []byte) *ErrorResponse {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		// json.Valid() already gated non-JSON bodies before this is called;
		// this branch covers valid-JSON-but-not-an-object bodies (e.g. a bare
		// JSON array, string, or number), which real S3 also rejects.
		return &ErrorResponse{
			Code:    errMalformedPolicy,
			Message: "Policy document must be a JSON object.",
		}
	}

	if versionErr := validatePolicyVersion(top); versionErr != nil {
		return versionErr
	}

	stmtRaw, ok := top["Statement"]
	if !ok {
		return &ErrorResponse{Code: errMalformedPolicy, Message: "Missing required field Statement"}
	}

	statements, statementsErr := decodePolicyStatements(stmtRaw)
	if statementsErr != nil {
		return statementsErr
	}

	if len(statements) == 0 {
		return &ErrorResponse{Code: errMalformedPolicy, Message: "Missing required field Statement"}
	}

	for _, stmt := range statements {
		if shapeErr := validatePolicyStatementShape(stmt); shapeErr != nil {
			return shapeErr
		}
	}

	return nil
}

// validatePolicyVersion validates the optional top-level Version element. AWS
// accepts a policy with no Version at all (version_block is marked `?` in the
// grammar), but if present it must be exactly "2008-10-17" or "2012-10-17".
func validatePolicyVersion(top map[string]json.RawMessage) *ErrorResponse {
	raw, ok := top["Version"]
	if !ok {
		return nil
	}

	var version string
	if err := json.Unmarshal(raw, &version); err != nil {
		return &ErrorResponse{
			Code:    errMalformedPolicy,
			Message: "The policy must contain a valid version string.",
		}
	}

	if version != "2008-10-17" && version != "2012-10-17" {
		return &ErrorResponse{
			Code:    errMalformedPolicy,
			Message: "The policy must contain a valid version string.",
		}
	}

	return nil
}

// decodePolicyStatements decodes the "Statement" value, which per the IAM
// grammar convention may be either a single statement object or a JSON array
// of statement objects.
func decodePolicyStatements(raw json.RawMessage) ([]map[string]json.RawMessage, *ErrorResponse) {
	trimmed := bytes.TrimSpace(raw)
	shapeErr := &ErrorResponse{
		Code:    errMalformedPolicy,
		Message: "Statement is not well-formed",
	}

	if len(trimmed) == 0 {
		return nil, shapeErr
	}

	if trimmed[0] == '[' {
		var arr []map[string]json.RawMessage
		if err := json.Unmarshal(trimmed, &arr); err != nil {
			return nil, shapeErr
		}

		return arr, nil
	}

	var single map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &single); err != nil {
		return nil, shapeErr
	}

	return []map[string]json.RawMessage{single}, nil
}

// validatePolicyStatementShape validates a single statement object against
// the required <statement> grammar elements for a resource-based (bucket)
// policy: Effect, Principal/NotPrincipal, Action/NotAction, and
// Resource/NotResource.
func validatePolicyStatementShape(stmt map[string]json.RawMessage) *ErrorResponse {
	if effectErr := validatePolicyEffect(stmt); effectErr != nil {
		return effectErr
	}

	if _, hasPrincipal := stmt["Principal"]; !hasPrincipal {
		if _, hasNotPrincipal := stmt["NotPrincipal"]; !hasNotPrincipal {
			return &ErrorResponse{
				Code:    errMalformedPolicy,
				Message: "Missing required field Principal cannot be empty!",
			}
		}
	}

	if _, hasAction := stmt["Action"]; !hasAction {
		if _, hasNotAction := stmt["NotAction"]; !hasNotAction {
			return &ErrorResponse{Code: errMalformedPolicy, Message: "Missing required field Action"}
		}
	}

	if _, hasResource := stmt["Resource"]; !hasResource {
		if _, hasNotResource := stmt["NotResource"]; !hasNotResource {
			return &ErrorResponse{Code: errMalformedPolicy, Message: "Missing required field Resource"}
		}
	}

	return nil
}

// validatePolicyEffect validates the required Effect element of a statement.
func validatePolicyEffect(stmt map[string]json.RawMessage) *ErrorResponse {
	effectRaw, ok := stmt["Effect"]
	if !ok {
		return &ErrorResponse{Code: errMalformedPolicy, Message: "Missing required field Effect"}
	}

	var effect string
	if err := json.Unmarshal(effectRaw, &effect); err != nil || (effect != "Allow" && effect != "Deny") {
		return &ErrorResponse{
			Code:    errMalformedPolicy,
			Message: "Invalid effect: " + string(effectRaw),
		}
	}

	return nil
}
