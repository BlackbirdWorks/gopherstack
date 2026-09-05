package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func extractFixture(t *testing.T, src string) []candidate {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "fixture.go"), []byte(src), 0o600))

	got, err := extractCandidates(dir, dir)
	require.NoError(t, err)

	return got
}

func codesOf(cands []candidate) []string {
	out := make([]string, 0, len(cands))
	for _, c := range cands {
		out = append(out, c.Code)
	}

	return out
}

// TestExtractCandidates_Positive covers each of the four handler.go
// mechanisms plus the two narrower extensions this tool's brief and
// sink.go's own doc comments were built from, using real pre-fix snippets
// (services/ecs at fa0e68c21^) and the real shapes read from
// services/iam and services/cloudformation's handler.go.
func TestExtractCandidates_Positive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want string
	}{
		{
			// services/ecs/errors.go (fa0e68c21^): ecs's own mechanism.
			name: "awserr.New sentinel",
			src: `package ecs

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var ErrClusterAlreadyExists = awserr.New("ClusterAlreadyExistsException", awserr.ErrAlreadyExists)
`,
			want: "ClusterAlreadyExistsException",
		},
		{
			// services/lambda/errors.go: lambda's own mechanism -- the
			// sentinel's message IS the code.
			name: "stdlib errors.New sentinel",
			src: `package lambda

import "errors"

var ErrFunctionNotFound = errors.New("ResourceNotFoundException")
`,
			want: "ResourceNotFoundException",
		},
		{
			// services/cloudformation/handler_hooks.go: a bare literal at
			// a xmlError call site, caught via sink.go's registry because
			// xmlError's own body writes its code param into
			// xmlErrBody{Code: code}.
			name: "sink call argument",
			src: `package cloudformation

func (h *Handler) xmlError(c *echo.Context, code, message string) error {
	type xmlErrBody struct {
		Code    string
		Message string
	}

	return enc.Encode(xmlErrBody{Code: code, Message: message})
}

func (h *Handler) handleHookResult() error {
	return h.xmlError(c, "HookResultNotFound", err.Error())
}
`,
			want: "HookResultNotFound",
		},
		{
			// services/iam/handler.go: iamErrorMapping's positional table
			// shape, resolved through the struct's own declared field
			// order and a one-hop package const.
			name: "positional mapping table",
			src: `package iam

const codeNoSuchEntity = "NoSuchEntity"

type iamErrorMapping struct {
	err    error
	code   string
	status int
}

var iamErrorMappings = []iamErrorMapping{
	{ErrUserNotFound, codeNoSuchEntity, http.StatusNotFound},
}
`,
			want: "NoSuchEntity",
		},
		{
			// services/ecs/handler.go: the map[string]string{keyTypeField:
			// ...} shape, resolved one hop through keyTypeField's own
			// "__type" value.
			name: "map keyed by resolved wire-key const",
			src: `package ecs

const keyTypeField = "__type"

func x() {
	_ = map[string]string{keyTypeField: "UnknownOperationException", "message": "x"}
}
`,
			want: "UnknownOperationException",
		},
		{
			// services/cloudformation/handler_stack_refactors.go: a
			// code-shaped literal assigned to a code-named local.
			name: "code-named local assignment",
			src: `package cloudformation

func f(err error) string {
	code := "ValidationError"
	if errors.Is(err, ErrStackRefactorNotFound) {
		code = "StackRefactorNotFoundException"
	}

	return code
}
`,
			want: "StackRefactorNotFoundException",
		},
		{
			// services/cloudformation/handler_stack_sets.go: a return
			// statement inside a function named like an error-code
			// classifier.
			name: "return in error-classifier function",
			src: `package cloudformation

func stackInstancesErrorCode(err error) string {
	if errors.Is(err, ErrStackSetNotFound) {
		return "StackSetNotFoundException"
	}

	return "ValidationError"
}
`,
			want: "StackSetNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := extractFixture(t, tt.src)
			assert.Contains(t, codesOf(got), tt.want)
		})
	}
}

// TestExtractCandidates_Negative covers the false-positive classes found
// and fixed during this tool's own calibration pass (see sink.go and
// extract.go's doc comments for each) plus the shapes the code-shape
// filter alone must reject.
func TestExtractCandidates_Negative(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		bad  string
	}{
		{
			// services/acm's DNS challenge record: a bare "Type" field on
			// a struct with no "Error" in its name is not a code field.
			name: "type field on non-error struct",
			src: `package acm

type DNSRecord struct {
	Type  string
	Value string
}

func f() DNSRecord {
	return DNSRecord{Type: "CNAME", Value: "x"}
}
`,
			bad: "CNAME",
		},
		{
			// services/textract's Money type: a bare "Code" field on a
			// struct with no "Error" in its name is not a code field
			// either.
			name: "code field on non-error struct",
			src: `package textract

type Money struct {
	Code string
}

func f() Money {
	return Money{Code: "USD"}
}
`,
			bad: "USD",
		},
		{
			// services/autoscaling and services/docdb's own
			// autoscalingError{Code: code, Type: "Sender"}: a Type
			// sibling is suppressed once a Code field is present in the
			// same literal, since it is the Query-protocol fault role,
			// not a second code.
			name: "type sibling suppressed by code field",
			src: `package autoscaling

type autoscalingError struct {
	Code    string
	Message string
	Type    string
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	return c.XML(autoscalingError{Code: code, Message: message, Type: "Sender"})
}
`,
			bad: "Sender",
		},
		{
			// services/amplify's handleBackendError(ctx, c, "CreateApp",
			// err): an action-name argument to an "...Error"-suffixed
			// call that never writes its parameter into a code-labeled
			// field is never a sink.
			name: "unclassified error-suffixed call is not a sink",
			src: `package amplify

func (h *Handler) handleBackendError(ctx context.Context, c *echo.Context, action string, err error) error {
	log.Error("backend error", "action", action, "err", err)
	return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
}

func (h *Handler) createApp(ctx context.Context, c *echo.Context) error {
	return h.handleBackendError(ctx, c, "CreateApp", err)
}
`,
			bad: "CreateApp",
		},
		{
			// services/ce and services/comprehend: a var name that merely
			// contains "code" as a substring (not the error-code naming
			// convention) is not a code variable.
			name: "currency code var is not an error code var",
			src: `package ce

const handlerCurrencyCode = "USD"
`,
			bad: "USD",
		},
		{
			// services/quicksight and services/securityhub's own
			// keyErrorCode = "ErrorCode": a key/field-prefixed constant
			// names a wire KEY, not a code value.
			name: "key-prefixed const is a wire key name, not a code",
			src: `package quicksight

const keyErrorCode = "ErrorCode"
`,
			bad: "ErrorCode",
		},
		{
			name: "human message is not code-shaped",
			src: `package cloudformation

func f(c *echo.Context) error {
	return h.xmlError(c, "ValidationError", "StackName is required")
}
`,
			bad: "StackName is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := extractFixture(t, tt.src)
			assert.NotContains(t, codesOf(got), tt.bad)
		})
	}
}

// TestExtractCandidates_ElidedCompositeType covers the fill this tool's
// own calibration pass needed: an elided composite-literal type inside a
// slice-of-struct table (services/networkmanager and services/xray's own
// shape) must still resolve to the outer slice's element type.
func TestExtractCandidates_ElidedCompositeType(t *testing.T) {
	t.Parallel()

	src := `package networkmanager

type CoreNetworkPolicyError struct {
	ErrorCode string
	Message   string
	Path      string
}

func f() []CoreNetworkPolicyError {
	return []CoreNetworkPolicyError{
		{ErrorCode: "InvalidPolicyDocument", Message: "bad", Path: "/"},
	}
}
`

	got := extractFixture(t, src)
	assert.Contains(t, codesOf(got), "InvalidPolicyDocument")
}
