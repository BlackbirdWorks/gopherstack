package iam

import (
	"encoding/json"
	"fmt"
	"strings"
)

// IAM policy-grammar validation.
//
// AWS does not merely require that a policy document parse as JSON: it enforces
// the IAM policy grammar and returns MalformedPolicyDocument when a structurally
// valid JSON document violates that grammar. The pre-existing checks in this
// package only ran json.Valid, so documents such as
//
//	{"Statement":[{"Effect":"Permit","Action":"s3:*","Resource":"*"}]}
//	{"Version":"2012-10-17"}                        (no Statement)
//	{"Statement":[{"Effect":"Allow","Resource":"*"}]} (no Action/NotAction)
//
// were silently accepted, diverging from real AWS and LocalStack behaviour.
//
// validateIdentityPolicyDocument implements the subset of the grammar that AWS
// enforces synchronously at Create/Put time for identity-based policies
// (managed policies, policy versions, and inline user/role/group policies):
//
//   - The document must be a JSON object.
//   - Version, if present, must be "2012-10-17" or "2008-10-17".
//   - Statement is required and must be a single statement object or a
//     non-empty array of statement objects.
//   - Each statement's Effect must be exactly "Allow" or "Deny".
//   - Each statement must specify exactly one of Action / NotAction.
//   - Each statement must specify exactly one of Resource / NotResource.
//   - Identity policies must not contain Principal / NotPrincipal (those belong
//     to resource-based and trust policies only).
//
// The validator is deliberately lenient about things AWS validates lazily or
// not at all here (e.g. action-name spelling, ARN well-formedness of every
// resource), to avoid rejecting documents that real AWS accepts.

// isSupportedPolicyVersion reports whether v is one of the two Version values
// AWS accepts in a policy document.
func isSupportedPolicyVersion(v string) bool {
	return v == "2012-10-17" || v == "2008-10-17"
}

// rawStatement is a statement decoded with field presence preserved so that
// "absent" can be distinguished from "present but empty".
type rawStatement struct {
	Effect       *string         `json:"Effect"`
	Action       json.RawMessage `json:"Action"`
	NotAction    json.RawMessage `json:"NotAction"`
	Resource     json.RawMessage `json:"Resource"`
	NotResource  json.RawMessage `json:"NotResource"`
	Principal    json.RawMessage `json:"Principal"`
	NotPrincipal json.RawMessage `json:"NotPrincipal"`
}

// validateIdentityPolicyDocument validates an identity-based policy document
// against the IAM policy grammar. An empty document is treated as valid (the
// caller may have an empty default). It returns an error wrapping
// ErrMalformedPolicyDocument on any grammar violation.
func validateIdentityPolicyDocument(policyDocument string) error {
	if strings.TrimSpace(policyDocument) == "" {
		return nil
	}

	// Top level must be a JSON object.
	var top map[string]json.RawMessage
	if err := json.Unmarshal([]byte(policyDocument), &top); err != nil {
		return fmt.Errorf("%w: policy document must be a JSON object", ErrMalformedPolicyDocument)
	}

	if err := validatePolicyVersion(top["Version"]); err != nil {
		return err
	}

	rawStmt, ok := top["Statement"]
	if !ok {
		return fmt.Errorf("%w: policy document must contain a Statement element", ErrMalformedPolicyDocument)
	}

	stmts, err := decodeStatements(rawStmt)
	if err != nil {
		return err
	}

	if len(stmts) == 0 {
		return fmt.Errorf("%w: Statement must contain at least one statement", ErrMalformedPolicyDocument)
	}

	for i, st := range stmts {
		if vErr := validateIdentityStatement(i, st); vErr != nil {
			return vErr
		}
	}

	return nil
}

// validatePolicyVersion checks the optional Version element.
func validatePolicyVersion(raw json.RawMessage) error {
	if raw == nil {
		return nil
	}

	var version string
	if err := json.Unmarshal(raw, &version); err != nil {
		return fmt.Errorf("%w: Version must be a string", ErrMalformedPolicyDocument)
	}

	if !isSupportedPolicyVersion(version) {
		return fmt.Errorf(
			"%w: unsupported policy Version %q; expected 2012-10-17 or 2008-10-17",
			ErrMalformedPolicyDocument, version,
		)
	}

	return nil
}

// decodeStatements accepts either a single statement object or an array of
// statement objects, matching AWS's tolerance of both shapes.
func decodeStatements(raw json.RawMessage) ([]rawStatement, error) {
	trimmed := strings.TrimSpace(string(raw))
	if strings.HasPrefix(trimmed, "[") {
		var arr []rawStatement
		if err := json.Unmarshal(raw, &arr); err != nil {
			return nil, fmt.Errorf("%w: Statement array is malformed", ErrMalformedPolicyDocument)
		}

		return arr, nil
	}

	var single rawStatement
	if err := json.Unmarshal(raw, &single); err != nil {
		return nil, fmt.Errorf("%w: Statement must be an object or array of objects", ErrMalformedPolicyDocument)
	}

	return []rawStatement{single}, nil
}

// validateIdentityStatement enforces the per-statement grammar for identity
// policies.
func validateIdentityStatement(idx int, st rawStatement) error {
	if st.Effect == nil {
		return fmt.Errorf("%w: statement[%d] is missing the required Effect element", ErrMalformedPolicyDocument, idx)
	}

	if *st.Effect != "Allow" && *st.Effect != "Deny" {
		return fmt.Errorf(
			"%w: statement[%d] Effect %q must be exactly \"Allow\" or \"Deny\"",
			ErrMalformedPolicyDocument, idx, *st.Effect,
		)
	}

	hasAction := len(st.Action) > 0
	hasNotAction := len(st.NotAction) > 0

	switch {
	case !hasAction && !hasNotAction:
		return fmt.Errorf("%w: statement[%d] must specify Action or NotAction", ErrMalformedPolicyDocument, idx)
	case hasAction && hasNotAction:
		return fmt.Errorf(
			"%w: statement[%d] must not specify both Action and NotAction",
			ErrMalformedPolicyDocument, idx,
		)
	}

	hasResource := len(st.Resource) > 0
	hasNotResource := len(st.NotResource) > 0

	switch {
	case !hasResource && !hasNotResource:
		return fmt.Errorf("%w: statement[%d] must specify Resource or NotResource", ErrMalformedPolicyDocument, idx)
	case hasResource && hasNotResource:
		return fmt.Errorf(
			"%w: statement[%d] must not specify both Resource and NotResource",
			ErrMalformedPolicyDocument, idx,
		)
	}

	// Identity-based policies may not carry a Principal — that is exclusive to
	// resource-based and trust policies. AWS rejects such documents.
	if len(st.Principal) > 0 || len(st.NotPrincipal) > 0 {
		return fmt.Errorf(
			"%w: statement[%d] specifies Principal, which is not allowed in an identity-based policy",
			ErrMalformedPolicyDocument, idx,
		)
	}

	return nil
}
